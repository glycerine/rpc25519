package rpc25519

import (
	"crypto/cipher"
	cryrand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log"
	"sync"
	"time"

	"golang.org/x/crypto/chacha20poly1305"
)

var _ = fmt.Printf

// blabber holds stream encryption/decryption facilities.
//
// It is typically used for encrypting net.Conn connections.
//
// A blabber uses the XChaCha20-Poly1305 AEAD which works
// with 24 byte nonces. XChaCha20 is the stream cipher.
// Poly1305 is the message authentication code.
//
// Reference: "Extending the Salsa20 nonce" by Daniel J. Bernstein.
// https://cr.yp.to/snuffle/xsalsa-20110204.pdf
//
// A note on nonce selection:
//
// Since our secret key is derived from a random
// ephemeral elliptic Diffie-Hellman handshake
// combined with the pre-shared-key, the
// only real danger of re-using a nonce
// comes from the client and
// server picking the same nonce. To avoid
// any chance of these two colliding, we
// choose the initial nonce on the server
// randomly from those with the high nibble set
// to 16 (binary 0100xxxx), while the
// client chooses randomly from those with
// the high nibble set to 0 (binary 0000xxxx)
//
// Then each increments the initial (random) nonce
// by one after each use. Even if the client
// picked 0 as its inital nonce (the lowest possible),
// and the server randomly picked the
// highest possible nonce,
// the incrementing by one
// still has 3<<184 increments
// before nonce reuse, which is ~ 1e55 increments.
//
// Even at the astounding rate of one
// increment for each bit in 128 GB per
// *nanosecond*, in order to wrap
// and collide would require 1e28 years.
// This is long, long after the sun has
// destroyed the Earth in ~ 8e9 years. So
// on our timescale, its never going to happen.
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
//	24 bytes of nonce
//	xx bytes of 1:1 cyphertext (same length as plaintext) } these two are output
//	16 bytes of poly1305 authentication tag.              } by the e.aead.Seal() call.
//
// encoder uses a workspace to avoid allocation.
type encoder struct {
	key  []byte      // must be 32 bytes == chacha20poly1305.KeySize (256 bits)
	aead cipher.AEAD // XChaCha20-Poly1305, needs 256-bit key

	writeNonce []byte
	noncesize  int
	overhead   int

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
func newBlabber(key [32]byte, conn uConn, encrypt bool, maxMsgSize int, isServer bool) *blabber {

	aeadEnc, err := chacha20poly1305.NewX(key[:])
	panicOn(err)

	aeadDec, err := chacha20poly1305.NewX(key[:])
	panicOn(err)

	// See discussion of nonce planning above.
	writeNonce := make([]byte, aeadEnc.NonceSize()) // 24 bytes
	writeNewCryRandomNonce(writeNonce)

	// little endian to make increment faster.
	writeNonce[23] &= 15 // clear highest 4 bits
	if isServer {
		writeNonce[23] |= 64 // set 2nd highest bit
	}

	enc := &encoder{
		key:        key[:],
		aead:       aeadEnc,
		writeNonce: writeNonce,
		noncesize:  aeadEnc.NonceSize(),
		overhead:   aeadEnc.Overhead(),
		work:       newWorkspace(maxMsgSize),
	}
	dec := &decoder{
		key:       key[:],
		aead:      aeadDec,
		noncesize: aeadEnc.NonceSize(),
		overhead:  aeadEnc.Overhead(),
		work:      newWorkspace(maxMsgSize),
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

func NewXChaCha20CryptoRandKey() []byte {
	key := make([]byte, chacha20poly1305.KeySize) // 32 bytes
	if _, err := cryrand.Read(key); err != nil {
		log.Fatal(err)
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
	// overflow. will never happen with XChaCha20.
	// *And* we would like this function to get inlined,
	// so we aren't going to do anything here.
}

func writeNewCryRandomNonce(nonce []byte) {
	_, err := cryrand.Read(nonce)

	// docs: cryrand.Read "is a helper function that
	// calls Reader.Read using io.ReadFull.
	// On return, n == len(b) if and only if err == nil."
	panicOn(err) // system borked if we cannot read.
}

// Write encrypts data and writes it to the underlying stream.
// This is what a client actively does when they write to net.Conn.
// Write(plaintext []byte) (n int, err error)
func (e *encoder) sendMessage(conn uConn, msg *Message, timeout *time.Duration) error {

	// encryption
	e.mut.Lock()
	defer e.mut.Unlock()

	defer func() {
		r := recover()
		if r != nil {
			vv("encoder.sendMessage recovers from panic: '%v'", r)
			panic(r)
		}
	}()

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

	// Update the nonce: ONLY AFTER using it above in Seal!
	incrementNonce(e.writeNonce)

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
	err = readFull(conn, d.work.readLenMessageBytes, timeout)
	if err != nil {
		//vv("err = '%v'", err) // Application error 0x0 (remote): server shutdown
		return
	}
	messageLen := int(binary.BigEndian.Uint64(d.work.readLenMessageBytes))

	// Read the message based on the messageLen
	if messageLen > maxMessage {
		// probably an encrypted client against an unencrypted server
		return nil, ErrTooLong
	}

	buf := d.work.buf

	// Read the encrypted data
	encrypted := buf[:messageLen]
	err = readFull(conn, encrypted, timeout)

	// Decrypt the data

	// if the "autheticated associated data" of lenBy got messed with, do we detect it? yep!
	// lenBy[3]++
	// error: chacha20poly1305: message authentication failed

	assocData := d.work.readLenMessageBytes // length of message should be authentic too.
	nonce := encrypted[:d.noncesize]

	message, err := d.aead.Open(nil, nonce, encrypted[d.noncesize:], assocData)
	panicOn(err)
	if err != nil {
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
