package main

import (
	//"bytes"
	"crypto/cipher"
	cryrand "crypto/rand"
	"encoding/binary"
	//"errors"
	"fmt"
	"io"
	"time"
	//"io"
	"log"
	"sync"

	"golang.org/x/crypto/chacha20poly1305"
	//"crypto/sha256"
	//nacl "golang.org/x/crypto/nacl/secretbox"
)

const LEN_XCHACHA20_NONCE_BYTES = 24

// blabber holds stream encryption/decryption facilities.
// What comes out is just blah, blah, blah.
type blabber struct {
	encrypt    bool
	maxMsgSize int

	enc *encoder
	dec *decoder
}

// users write to an encoder
type encoder struct {
	key  []byte      // must be 32 bytes == chacha20poly1305.KeySize
	aead cipher.AEAD // XChaCha20-Poly1305, needs 256-bit key

	conn io.Writer // can be net.Conn

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

	conn io.Reader // can be net.Conn

	noncesize int
	overhead  int

	mut  sync.Mutex
	work *workspace
}

func newBlabber(key []byte, rw io.ReadWriter, encrypt bool, maxMsgSize int) *blabber {

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
		conn:       rw,
		key:        key,
		aead:       aeadEnc,
		writeNonce: writeNonce,
		noncesize:  aeadEnc.NonceSize(),
		overhead:   aeadEnc.Overhead(),
		work:       newWorkspace(maxMsgSize),
	}
	dec := &decoder{
		conn:      rw,
		key:       key,
		aead:      aeadDec,
		noncesize: aeadEnc.NonceSize(),
		overhead:  aeadEnc.Overhead(),
		work:      newWorkspace(maxMsgSize),
	}

	return &blabber{
		maxMsgSize: maxMsgSize,
		encrypt:    encrypt,

		enc: enc,
		dec: dec,
	}
}

func (blab *blabber) readMessage(conn uConn, timeout *time.Duration) (msg []byte, err error) {
	if !blab.encrypt {
		return blab.dec.work.readMessage(conn, timeout)
	}
	return blab.dec.readMessage(conn, timeout)
}

func (blab *blabber) sendMessage(conn uConn, bytesMsg []byte, timeout *time.Duration) error {
	if !blab.encrypt {
		return blab.enc.work.sendMessage(conn, bytesMsg, timeout)
	}
	return blab.enc.sendMessage(conn, bytesMsg, timeout)
}

func generateXChaChaNonce24(nonce *[LEN_XCHACHA20_NONCE_BYTES]byte) {
	_, err := cryrand.Read(nonce[:])
	if err != nil {
		panic(err)
	}
}

func NewXChaCha20CryptoRandKey() []byte {
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
	//return errors.New("nonce overflow")
}

func writeNewCryRandomNonce(nonce []byte) error {
	_, err := cryrand.Read(nonce)
	return err
}

/*
// net.Conn does this, so client does not really need it.
func (e *encoder) Read(ciph []byte) (n int, err error) {
	e.mut.Lock()
	defer e.mut.Unlock()

	return e.ciph.Read(ciph)
}
*/

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

	// update the nonce. random is better tha incrementing;
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

/*
// Write ciphertext to be decoded into the decoder.
// net.Conn will do this for us, when we read from decoder, so not really needed.
func (d *decoder) Write(ciphertext []byte) (n int, err error) {
	// write into d.ciph to accumulate
	d.mut.Lock()
	defer d.mut.Unlock()

	// lazily wait until we are read from to decode,
	// because we might not have it all?
	return d.ciph.Write(ciphertext)
}
*/

/*
// ReadFrom reads plaintext data from src, encrypts it, and writes encrypted data to the underlying writer.
func (e *encoder) ReadFrom(src io.Reader) (n int64, err error) {
	// Buffer size can be adjusted based on expected data sizes.
	bufferSize := 32 * 1024 // 32KB buffer
	buffer := make([]byte, bufferSize)

	for {
		readBytes, readErr := src.Read(buffer)
		if readBytes > 0 {
			// Encrypt the data
			encrypted := e.aead.Seal(nil, e.writeNonce, buffer[:readBytes], nil)

			// Increment the nonce
			e.mut.Lock()
			err = incrementNonce(e.writeNonce)
			e.mut.Unlock()
			if err != nil {
				return n, err
			}

			// Write the length prefix
			err = binary.Write(&e.ciph, binary.BigEndian, uint32(len(encrypted)))
			if err != nil {
				return n, err
			}

			// Write the encrypted data
			written, writeErr := e.ciph.Write(encrypted)
			if writeErr != nil {
				return n, writeErr
			}

			if written != len(encrypted) {
				return n, io.ErrShortWrite
			}

			n += int64(readBytes)
		}

		if readErr != nil {
			if readErr == io.EOF {
				return n, nil
			}
			return n, readErr
		}
	}
}
*/

func encryptChaCha20Poly1305(plaintext, key []byte) ([]byte, error) {

	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, err
	}

	// Select a random nonce, and leave capacity for the ciphertext.
	nonce := make([]byte, aead.NonceSize(), aead.NonceSize()+len(plaintext)+aead.Overhead())
	if _, err := cryrand.Read(nonce); err != nil {
		panic(err)
	}

	// Encrypt the message and append the ciphertext to the nonce.
	ciphertext := aead.Seal(nonce, nonce, plaintext, nil)
	return ciphertext, nil
}

func decryptChaCha20Poly1305(encryptedMsg, key []byte) ([]byte, error) {
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, err
	}

	if len(encryptedMsg) < aead.NonceSize() {
		panic("ciphertext too short")
	}

	nonceSize := chacha20poly1305.NonceSizeX
	if len(encryptedMsg) < nonceSize {
		return nil, err
	}

	if chacha20poly1305.NonceSizeX != aead.NonceSize() {
		panic(fmt.Sprintf("chacha20poly1305.NonceSizeX(%v) != aead.NonceSize(%v)", chacha20poly1305.NonceSizeX, aead.NonceSize()))
	}

	// Split nonce and ciphertext.
	nonce, ciphertext := encryptedMsg[:aead.NonceSize()], encryptedMsg[aead.NonceSize():]

	plaintext, err := aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}

	return plaintext, nil
}

/*
func main() {
	key := make([]byte, chacha20poly1305.KeySize)
	if _, err := cryrand.Read(key); err != nil {
		log.Fatal(err)
	}

	plaintext := []byte("Secure Message: gophers, gophers, everywhere!")
	ciphertext, err := encryptChaCha20Poly1305(plaintext, key)
	if err != nil {
		log.Fatal(err)
	}

	decrypted, err := decryptChaCha20Poly1305(ciphertext, key)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Decrypted Text: %s", decrypted)
}
*/
