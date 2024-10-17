package main

import (
	"crypto/cipher"
	cryrand "crypto/rand"
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

func newBlabber(key []byte, conn uConn, encrypt bool, maxMsgSize int) *blabber {

	aeadEnc, err := chacha20poly1305.NewX(key)
	panicOn(err)

	aeadDec, err := chacha20poly1305.NewX(key)
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
		key:        key,
		aead:       aeadEnc,
		writeNonce: writeNonce,
		noncesize:  aeadEnc.NonceSize(),
		overhead:   aeadEnc.Overhead(),
		work:       newWorkspace(maxMsgSize),
	}
	dec := &decoder{
		key:       key,
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

func (blab *blabber) readMessage(timeout *time.Duration) (msg []byte, err error) {
	if !blab.encrypt {
		return blab.dec.work.readMessage(blab.conn, timeout)
	}
	return blab.dec.readMessage(blab.conn, timeout)
}

func (blab *blabber) sendMessage(bytesMsg []byte, timeout *time.Duration) error {
	if !blab.encrypt {
		return blab.enc.work.sendMessage(blab.conn, bytesMsg, timeout)
	}
	return blab.enc.sendMessage(blab.conn, bytesMsg, timeout)
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
func (e *encoder) sendMessage(conn uConn, bytesMsg []byte, timeout *time.Duration) error {

	// encryption
	e.mut.Lock()
	defer e.mut.Unlock()

	sz := len(bytesMsg) + e.noncesize + e.overhead
	binary.BigEndian.PutUint64(e.work.writeLenMessageBytes, uint64(sz))

	// Write Message length
	if err := writeFull(conn, e.work.writeLenMessageBytes, timeout); err != nil {
		return err
	}
	assocData := e.work.writeLenMessageBytes

	// grow buf if need be
	buf := e.work.buf
	buf = buf[0:cap(buf)]
	if len(buf) < sz {
		e.work.buf = append(e.work.buf, make([]byte, sz-len(buf))...)
		buf = e.work.buf
	}

	// Encrypt the data (prepends the nonce? nope need to do so ourselves)
	copy(buf[:e.noncesize], e.writeNonce)
	noncePlusEncrypted := e.aead.Seal(buf[:e.noncesize], e.writeNonce, bytesMsg, assocData)
	// verify size assumption was correct
	if len(noncePlusEncrypted) != int(sz) {
		panic(fmt.Sprintf("noncePlusEncrypted(%v) != sz(%v): our associated data in lenBy is wrong!",
			len(noncePlusEncrypted), sz))
	}

	// Update the nonce. random is better tha incrementing, and the same speed.
	// Much less chance of losing security by having a nonce re-used.
	err := writeNewCryRandomNonce(e.writeNonce)
	panicOn(err) // really should never fail unless whole system is borked.

	// Write the encrypted data
	return writeFull(conn, noncePlusEncrypted, timeout)
}

// Read decrypts data from the underlying stream.
// When a client actively reads from a net.Conn they are doing this.
// Read(plain []byte) (n int, err error) {
func (d *decoder) readMessage(conn uConn, timeout *time.Duration) (msg []byte, err error) {

	d.mut.Lock()
	defer d.mut.Unlock()

	// Read the first 8 bytes for the Message length
	err = readFull(conn, d.work.readLenMessageBytes, timeout)
	if err != nil {
		return
	}
	messageLen := int(binary.BigEndian.Uint64(d.work.readLenMessageBytes))

	// Read the message based on the messageLen
	if messageLen > maxMessage {
		// probably an encrypted client against an unencrypted server
		return nil, ErrTooLong
	}

	// only grow buf if we have to
	buf := d.work.buf
	buf = buf[0:cap(buf)]
	if len(buf) < messageLen {
		d.work.buf = append(d.work.buf, make([]byte, messageLen-len(buf))...)
		buf = d.work.buf
	}

	// Read the encrypted data
	encrypted := buf[:messageLen]
	err = readFull(conn, encrypted, timeout)

	// Decrypt the data

	// if the "autheticated associated data" of lenBy got messed with, do we detect it? yep!
	// lenBy[3]++
	// error: chacha20poly1305: message authentication failed

	assocData := d.work.readLenMessageBytes // length of message should be authentic too.
	return d.aead.Open(nil, encrypted[:d.noncesize], encrypted[d.noncesize:], assocData)
}
