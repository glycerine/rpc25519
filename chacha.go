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

// blabber holds stream encryption/decryption facilities.
//
// It uses the XChaCha20-Poly1305 AEAD which works well
// with random 24 byte nonces. XChaCha20 is the stream cipher.
// Poly1305 is the message authentication code.
//
// What comes out is just blah, blah, blah.
type blabber struct {
	encrypt    bool
	maxMsgSize int

	conn uConn // can be net.Conn

	enc *encoder
	dec *decoder
}

// users write to an encoder
type encoder struct {
	key  []byte      // must be 32 bytes == chacha20poly1305.KeySize (256 bits)
	aead cipher.AEAD // XChaCha20-Poly1305, needs 256-bit key

	writeNonce []byte
	noncesize  int
	overhead   int

	mut  sync.Mutex
	work *workspace
}

// users read from a decoder
type decoder struct {
	key  []byte
	aead cipher.AEAD

	noncesize int
	overhead  int

	mut  sync.Mutex
	work *workspace
}

func newBlabber(key [32]byte, conn uConn, encrypt bool, maxMsgSize int) *blabber {

	aeadEnc, err := chacha20poly1305.NewX(key[:])
	panicOn(err)

	aeadDec, err := chacha20poly1305.NewX(key[:])
	panicOn(err)

	// Use random nonces, since XChaCha20 supports them
	// without collision risk, and
	// it is much less dangerous than accidentally re-using a nonce.
	//
	// See "Extending the Salsa20 nonce" by Daniel J. Bernstein.
	// https://cr.yp.to/snuffle/xsalsa-20110204.pdf
	//
	writeNonce := make([]byte, aeadEnc.NonceSize()) // 24 bytes
	_, err = cryrand.Read(writeNonce)
	panicOn(err)

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

func newXChaCha20CryptoRandKey() []byte {
	key := make([]byte, chacha20poly1305.KeySize) // 32 bytes
	if _, err := cryrand.Read(key); err != nil {
		log.Fatal(err)
	}
	return key
}

// incrementNonce safely increments the nonce.
func incrementNonce(nonce []byte) error {
	for i := len(nonce) - 1; i >= 0; i-- {
		nonce[i]++
		if nonce[i] != 0 {
			return nil
		}
	}
	// overflow, start at random point.
	return writeNewCryRandomNonce(nonce)
}

func writeNewCryRandomNonce(nonce []byte) error {
	_, err := cryrand.Read(nonce)
	return err
}

// Write encrypts data and writes it to the underlying stream.
// This is what a client actively does when they write to net.Conn.
// Write(plaintext []byte) (n int, err error)
func (e *encoder) sendMessage(conn uConn, msg *Message, timeout *time.Duration) error {

	// encryption
	e.mut.Lock()
	defer e.mut.Unlock()

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
	vv("pre-encryption is sz=%v, being written as first 8-bytes; bytesMsg=%v", sz, len(bytesMsg))

	vv("plaintext = '%x'", bytesMsg)

	binary.BigEndian.PutUint64(e.work.buf[:8], uint64(sz))
	assocData := e.work.buf[:8]
	_ = assocData
	buf := e.work.buf

	// Encrypt the data (prepends the nonce? nope need to do so ourselves)

	// write the nonce
	copy(buf[8:8+e.noncesize], e.writeNonce)
	vv("nonce is '%x'", e.writeNonce)

	// encrypt. notice we get to re-use the plain text buf for the encrypted output.
	// So ideally, no allocation necessary.

	sealOut := e.aead.Seal(buf[8+e.noncesize:8+e.noncesize], e.writeNonce, buf[8+e.noncesize:8+e.noncesize+len(bytesMsg)], assocData)

	if e.noncesize+len(sealOut) != sz {
		panic(fmt.Sprintf("e.noncesize+len(sealOut)=%v != sz(%v)", e.noncesize+len(sealOut), sz))
	}

	// Update the nonce: ONLY AFTER using it above in Seal!
	// random is better tha incrementing, and the same speed.
	// Much less chance of losing security by having a nonce re-used.
	err = writeNewCryRandomNonce(e.writeNonce)
	panicOn(err) // really should never fail unless whole system is borked.

	ns := len(sealOut)
	vv("sealOut len = %v : '%x'", ns, sealOut)

	// DEBUG TODO REMOVE
	message2, err := e.aead.Open(nil, buf[8:8+e.noncesize], sealOut, assocData)
	panicOn(err)
	vv("was able to Open after Seal, plaintext back='%x'", message2)

	vv("seal got first 60 bytes: '%x'", sealOut[:60])
	vv("buf is first 60 bytes: '%x'", buf[:60])

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
		return
	}
	messageLen := int(binary.BigEndian.Uint64(d.work.readLenMessageBytes))

	vv("readMessage() sees messageLen = %v (first 8-bytes): '%x'", messageLen, d.work.readLenMessageBytes)

	// Read the message based on the messageLen
	if messageLen > maxMessage {
		// probably an encrypted client against an unencrypted server
		return nil, ErrTooLong
	}

	buf := d.work.buf

	/* should never be needed.
	// only grow buf if we have to
	buf = buf[0:cap(buf)]
	if len(buf) < messageLen {
		d.work.buf = append(d.work.buf, make([]byte, messageLen-len(buf))...)
		buf = d.work.buf
	}
	*/

	// Read the encrypted data
	encrypted := buf[:messageLen]
	err = readFull(conn, encrypted, timeout)

	// Decrypt the data

	// if the "autheticated associated data" of lenBy got messed with, do we detect it? yep!
	// lenBy[3]++
	// error: chacha20poly1305: message authentication failed

	assocData := d.work.readLenMessageBytes // length of message should be authentic too.
	nonce := encrypted[:d.noncesize]
	vv("nonce = '%x'", nonce)
	vv("pre decryption, encrypted = len %v: '%x'", len(encrypted[d.noncesize:]), encrypted[d.noncesize:])

	message, err := d.aead.Open(nil, nonce, encrypted[d.noncesize:], assocData)
	panicOn(err)
	if err != nil {
		return nil, err
	}

	return MessageFromGreenpack(message)
}

func HashAlotSha256(input []byte) [32]byte {
	var res [32]byte
	hashprev := input
	for i := 0; i < 100; i++ {
		res = sha256.Sum256(hashprev)
		hashprev = res[:]
		//fmt.Printf("hashprev = '%v'\n", string(hashprev))
	}
	return res
}
