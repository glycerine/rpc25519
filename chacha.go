package rpc25519

import (
	"crypto/aes"
	"crypto/cipher"
	cryrand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/cloudflare/circl/cipher/ascon"
	"golang.org/x/crypto/chacha20poly1305"
)

var _ = fmt.Printf

// blabber holds stream encryption/decryption facilities.
//
// It is typically used for encrypting net.Conn connections.
//
// A blabber uses the ChaCha20-Poly1305 AEAD which works
// with 12 byte nonces. ChaCha20 is the stream cipher.
// Poly1305 is the message authentication code.
//
// Reference for XChaCha and its 24 bytes nonce:
// "Extending the Salsa20 nonce" by Daniel J. Bernstein.
// https://cr.yp.to/snuffle/xsalsa-20110204.pdf
//
// A note on nonce selection:
//
// Since our secret key is derived from a random
// ephemeral elliptic Diffie-Hellman handshake
// combined with the pre-shared-key, the
// only real danger of re-using a nonce
// for this key comes from the client and
// server picking the same nonce.
// Nonces are chosen randomly from
// cyprto/rand input.
// To avoid any chance of these two colliding, we
// set the two high bits on the server to 0b10,
// and the two high bits on the client to 0b00.
//
// Each sides increments their nonce
// by one after each write. A 94-bit
// integer (ChaCha's 12 byte nonce minus
// 2 bits) is not going to overflow
// while the Earth exists, and even then
// there is one extra bit of safety.
//
// The name blabber? Well... what comes
// out is just blah, blah, blah.
type blabber struct {
	encrypt    bool
	maxMsgSize int

	conn uConn // can be net.Conn

	enc *encoder
	dec *decoder
}

// encoder organizes the encryption of messages
// passing through a net.Conn like connection.
// Users write or sendMessage() to an encoder.
//
// encrypted message structure is:
//
//	8 bytes of *mlen* message length: big endian
//
// then the following *mlen* bytes are
//
//	12/24 bytes of nonce, if ChaCha20 or XChaCah20 is used.
//	xx bytes of 1:1 cyphertext (same length as plaintext) } these two are output
//	16 bytes of poly1305 authentication tag.              } by the e.aead.Seal() call.
//
// encoder uses a workspace to avoid allocation.
type encoder struct {
	key  []byte      // must be 32 bytes == chacha20poly1305.KeySize (256 bits)
	aead cipher.AEAD // (X)ChaCha20-Poly1305, needs 256-bit key

	initialNonce   []byte
	writeNonce     []byte
	noncesize      int
	overhead       int // also know as tag size
	lastNonceSeqno uint64

	mut  sync.Mutex
	work *workspace
}

// decoder organizes the decryption of messages
// passing through a net.Conn like connection.
// users read from a decoder / call readMessage().
type decoder struct {
	key  []byte
	aead cipher.AEAD

	noncesize int
	overhead  int

	mut  sync.Mutex
	work *workspace
}

// newBlabber: at the moment it gets setup to do both read
// and write every time, even though, because there is only
// one workspace and we don't want that workspace to be
// shared between the readLoop and the writeLoop, only
// one half of its facility will ever get used in each
// instance. That's okay. The symmetry makes it simple
// to maintain.
//
// Latest: use ASCON 128a inside, so inner tunnel can
// differ from outer. Is about 2x faster than ChaChan20.
func newBlabber(name string, key [32]byte, conn uConn, encrypt bool, maxMsgSize int, isServer bool) *blabber {

	var err error
	var aeadEnc, aeadDec cipher.AEAD

	// options for inner cipher:
	useAscon128a := true
	useAesGCM := false
	if useAesGCM {
		block, err := aes.NewCipher(key[:])
		panicOn(err)
		aeadEnc, err = cipher.NewGCM(block)
		panicOn(err)
		aeadDec = aeadEnc

	} else if useAscon128a {
		aeadEnc, err = ascon.New(key[:16], ascon.Ascon128a)
		panicOn(err)

		aeadDec, err = ascon.New(key[:16], ascon.Ascon128a)
		panicOn(err)
	} else {
		// changed to ChaCha20 instead of XChaCha20 since it should be faster.
		aeadEnc, err = chacha20poly1305.New(key[:])
		panicOn(err)

		aeadDec, err = chacha20poly1305.New(key[:])
		panicOn(err)
	}

	nsz := aeadDec.NonceSize()

	// nonces: start random, then alter each time.
	// The use of random starting nonce means even if we
	// have multple clients (or both clients?) on say
	// a multicast channel, we are still not going
	// to re-use the same nonce (probabilistically neglible chance).
	writeNonce := make([]byte, nsz) // 24 bytes for XChaCha20, 12 bytes for ChaCha20.
	_, err = cryrand.Read(writeNonce)
	panicOn(err)

	// Insure client and server nonces are different by at least 1 bit,
	// even if we use the 0 starting nonce or the same on each side.
	// So: the 2 high bits of the Nonce start at 0 on the client,
	// and at 1 on the server.
	// (This is little endian to make increment faster.)
	writeNonce[nsz-1] &= 63 // clear highest 2 bits
	if isServer {
		writeNonce[nsz-1] |= 127 // set high bit on server only.
	}

	initialNonce := make([]byte, nsz)
	copy(initialNonce, writeNonce)

	enc := &encoder{
		key:          key[:],
		aead:         aeadEnc,
		initialNonce: initialNonce,
		writeNonce:   writeNonce,
		noncesize:    nsz,
		overhead:     aeadEnc.Overhead(),
		work:         newWorkspace(name+"_enc", maxMsgSize),
	}
	dec := &decoder{
		key:       key[:],
		aead:      aeadDec,
		noncesize: nsz,
		overhead:  aeadEnc.Overhead(),
		work:      newWorkspace(name+"_dec", maxMsgSize),
	}

	return &blabber{
		conn:       conn,
		maxMsgSize: maxMsgSize,
		encrypt:    encrypt,

		enc: enc,
		dec: dec,
	}
}

func (blab *blabber) readMessage(conn uConn, timeout *time.Duration) (msg *Message, err error) {
	if !blab.encrypt {
		return blab.dec.work.readMessage(conn, timeout)
	}
	return blab.dec.readMessage(conn, timeout)
}

func (blab *blabber) sendMessage(conn uConn, msg *Message, timeout *time.Duration) error {
	if !blab.encrypt {
		return blab.enc.work.sendMessage(conn, msg, timeout)
	}
	return blab.enc.sendMessage(conn, msg, timeout)
}

func NewChaCha20CryptoRandKey() []byte {
	key := make([]byte, chacha20poly1305.KeySize) // 32 bytes
	if _, err := cryrand.Read(key); err != nil {
		panic(err)
	}
	return key
}

// incrementNonce safely increments the nonce. We
// do little endian (least significant bit first)
// to make this faster and avoid bounds checking.
func incrementNonce(nonce []byte) {
	for i := range nonce {
		nonce[i]++
		if nonce[i] != 0 {
			return
		}
	}
	// overflow. will never happen.
	// *And* we would like this function to get inlined,
	// so we aren't going to do anything here.
}

// moveToNextNonce must insure never to repeat a e.writeNonce value.
func (e *encoder) moveToNextNonce() {
	e.lastNonceSeqno++ // just for reference, not actually used atm.
	incrementNonce(e.writeNonce)

	//xorNonceWithNextNonceSeqno(e.initialNonce, e.lastNonceSeqno, e.writeNonce)
}

// xorNonceWithUint64 XORs a nonce with a 64-bit integer.
func xorNonceWithNextNonceSeqno(input []byte, seqno uint64, output []byte) {

	// Convert the uint64 num to bytes
	var numBytes [8]byte
	binary.BigEndian.PutUint64(numBytes[:], seqno)
	//vv("%v -> numBytes = '%x'", seqno, numBytes)

	nw := copy(output, input)
	if nw != len(input) {
		panic("output too small")
	}

	//vv("output starting: '%x'", output)
	// XOR the input with the numBytes
	for i := 0; i < 8; i++ {
		output[i] ^= numBytes[i]
	}
	//vv("output ending: '%x'", output)
}

// Commit to the encryption key? This will
// re-write our authentication tag in order
// to only allow the key we used when encrypting.
// This is desirable.
// See https://eprint.iacr.org/2024/1382.pdf
// "Universal Context Commitment without Ciphertext Expansion"
// and
// https://github.com/samuel-lucas6/pact-go/
const commitWithPACT = true

// Write encrypts data and writes it to the underlying stream.
// This is what a client actively does when they write to net.Conn.
// Write(plaintext []byte) (n int, err error)
func (e *encoder) sendMessage(conn uConn, msg *Message, timeout *time.Duration) error {

	// encryption
	e.mut.Lock()
	defer e.mut.Unlock()

	// defer func() {
	// 	r := recover()
	// 	if r != nil {
	// 		alwaysPrintf("encoder.sendMessage recovers from panic: '%v'", r)
	// 		panic(r)
	// 	}
	// }()

	// serialize message to bytes
	bytesMsg, err := msg.AsGreenpack(e.work.buf[8+e.noncesize : cap(e.work.buf)])
	if err != nil {
		return err
	}

	if len(bytesMsg) > maxMessage {
		// We don't want to go over because client will just drop it,
		// thinking it an encrypted vs unencrypted mix up.
		return ErrTooLong
	}

	sz := len(bytesMsg) + e.noncesize + e.overhead

	binary.BigEndian.PutUint64(e.work.buf[:8], uint64(sz))
	assocData := e.work.buf[:8]

	buf := e.work.buf

	// Encrypt the data (prepends the nonce? nope need to do so ourselves)

	// write the nonce
	copy(buf[8:8+e.noncesize], e.writeNonce)

	// encrypt. notice we get to re-use the plain text buf for the encrypted output.
	// So ideally, no allocation necessary.
	sealOut := e.aead.Seal(buf[8+e.noncesize:8+e.noncesize], buf[8:8+e.noncesize], buf[8+e.noncesize:8+e.noncesize+len(bytesMsg)], assocData)

	if commitWithPACT {
		tag := sealOut[len(sealOut)-e.overhead:]
		pactEncryptTag(e.key, assocData, e.writeNonce, tag)
	}

	// Update the nonce: ONLY AFTER using it above in Seal!
	e.moveToNextNonce()
	//e.xorNonceWithNextNonceSeqno(e.writeNonce, e.lastNonceSeqno)

	// Write the 8 bytes of msglen + the nonce + encrypted data with authentication tag.
	return writeFull(conn, buf[:8+e.noncesize+len(sealOut)], timeout)
}

// Read decrypts data from the underlying stream.
// When a client actively reads from a net.Conn they are doing this.
// Read(plain []byte) (n int, err error) {
func (d *decoder) readMessage(conn uConn, timeout *time.Duration) (msg *Message, err error) {

	d.mut.Lock()
	defer d.mut.Unlock()

	// Read the first 8 bytes for the Message length
	_, err = readFull(conn, d.work.readLenMessageBytes, timeout)
	if err != nil {
		//vv("err = '%v'", err) // Application error 0x0 (remote): server shutdown
		return
	}
	messageLen := binary.BigEndian.Uint64(d.work.readLenMessageBytes)

	// Read the message based on the messageLen
	if messageLen > maxMessage {
		// probably an encrypted client against an unencrypted server
		return nil, ErrTooLong
	}

	buf := d.work.buf

	// Read the encrypted data
	encrypted := buf[:messageLen]
	_, err = readFull(conn, encrypted, timeout)
	if err != nil {
		return
	}

	// Decrypt the data

	// if the "autheticated associated data" of lenBy got messed with, do we detect it? yep!
	// lenBy[3]++
	// error: chacha20poly1305: message authentication failed

	assocData := d.work.readLenMessageBytes // length of message should be authentic too.
	nonce := encrypted[:d.noncesize]

	if commitWithPACT {
		tag := encrypted[len(encrypted)-d.overhead:]
		pactDecryptTag(d.key, assocData, nonce, tag)
	}

	message, err := d.aead.Open(nil, nonce, encrypted[d.noncesize:], assocData)
	if err != nil {
		//panic(fmt.Sprintf("decrypt failed: '%v'", err))
		alwaysPrintf("decryption failure on '%v' readMessage: '%v'", d.work.name, err)
		return nil, err
	}

	return MessageFromGreenpack(message)
}

// utility for doing 100 hashes
func hashAlotSha256(input []byte) [32]byte {
	var res [32]byte
	hashprev := input
	for i := 0; i < 100; i++ {
		res = sha256.Sum256(hashprev)
		hashprev = res[:]
	}
	return res
}
