package main

import (
	//"bytes"
	"crypto/cipher"
	cryrand "crypto/rand"
	"encoding/binary"
	//"errors"
	"fmt"
	"io"
	//"io"
	"log"
	"sync"

	"golang.org/x/crypto/chacha20poly1305"
	//"crypto/sha256"
	//nacl "golang.org/x/crypto/nacl/secretbox"
)

const (
	maxMessage = 2 * 1024 * 1024 * 1024 // 2GB max message size, prevents TLS clients from talking to TCP servers.
)

var ErrTooLong = fmt.Errorf("message message too long:  over 1MB; encrypted client vs an un-encrypted server?")

const LEN_XCHACHA20_NONCE_BYTES = 24

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

// users write to an encoder
type Encoder struct {
	key  []byte      // must be 32 bytes == chacha20poly1305.KeySize
	aead cipher.AEAD // XChaCha20-Poly1305, needs 256-bit key

	ciph io.Writer
	//ciph bytes.Buffer // stored encrypted for the reader

	writeNonce []byte
	noncesize  int
	overhead   int

	mut sync.Mutex
}

// users read from a decoder
type Decoder struct {
	key  []byte
	aead cipher.AEAD

	ciph io.Reader // can be net.Conn

	plain []byte // extra []byte, decrypted but not yet read; read from here first.

	noncesize int
	overhead  int

	mut sync.Mutex
}

func NewEncoderDecoderPair(key []byte, rw io.ReadWriter) (enc *Encoder, dec *Decoder) {
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

	enc = &Encoder{
		ciph:       rw,
		key:        key,
		aead:       aeadEnc,
		writeNonce: writeNonce,
		noncesize:  aeadEnc.NonceSize(),
		overhead:   aeadEnc.Overhead(),
	}
	dec = &Decoder{
		ciph:      rw,
		key:       key,
		aead:      aeadDec,
		noncesize: aeadEnc.NonceSize(),
		overhead:  aeadEnc.Overhead(),
	}
	return
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
func (e *Encoder) Read(ciph []byte) (n int, err error) {
	e.mut.Lock()
	defer e.mut.Unlock()

	return e.ciph.Read(ciph)
}
*/

// Write encrypts data and writes it to the underlying stream.
// This is what a client actively does when they write to net.Conn.
func (e *Encoder) Write(plaintext []byte) (n int, err error) {
	// encryption
	e.mut.Lock()
	defer e.mut.Unlock()

	// Encrypt the data (prepends the nonce? nope need to do space)
	var sz uint32 = uint32(e.noncesize + len(plaintext) + e.overhead)
	var lenBy [4]byte
	nl, lerr := binary.Encode(lenBy[:], binary.BigEndian, sz)
	panicOn(lerr)
	if nl != 4 {
		panic("short write")
	}
	space := make([]byte, e.noncesize, sz)
	copy(space, e.writeNonce)
	noncePlusEncrypted := e.aead.Seal(space, e.writeNonce, plaintext, lenBy[:])
	// verify size assumption was correct
	if len(noncePlusEncrypted) != int(sz) {
		panic(fmt.Sprintf("noncePlusEncrypted(%v) != sz(%v): our associated data in lenBy is wrong!",
			len(noncePlusEncrypted), sz))
	}

	// update the nonce. random is better tha incrementing;
	err = writeNewCryRandomNonce(e.writeNonce)
	if err != nil {
		return 0, err
	}

	// Write the length prefix
	err = binary.Write(e.ciph, binary.BigEndian, uint32(len(noncePlusEncrypted)))
	if err != nil {
		//e.ciph.Reset()
		return 0, err
	}

	// Write the encrypted data
	_, err = e.ciph.Write(noncePlusEncrypted)
	if err != nil {
		return 0, err
	}

	return len(plaintext), nil
}

// Read decrypts data from the underlying stream.
// When a client actively reads from a net.Conn they are doing this.
func (d *Decoder) Read(plain []byte) (n int, err error) {
	d.mut.Lock()
	defer d.mut.Unlock()

	//goal := len(plain)
	//	for n < goal {

	if len(d.plain) >= len(plain) {
		n += copy(plain, d.plain)
		d.plain = d.plain[n:]
		return
	}
	// INVAR: len(d.plain) < len(plain)
	if len(d.plain) > 0 {
		// accumulate into n as we write more to plain.
		n += copy(plain, d.plain)
		d.plain = d.plain[:0]
		plain = plain[n:]
	}

	// Read the length prefix (uint32)
	var length uint32
	lenBy := make([]byte, 4)
	var nr int
	nr, err = d.ciph.Read(lenBy)
	//vv("nr=%v, err =%v", nr, err)
	tot := nr
	for tot < 4 {
		if err != nil {
			return
		}
		nr, err = d.ciph.Read(lenBy[nr:])
		tot += nr
	}
	var used int
	used, err = binary.Decode(lenBy, binary.BigEndian, &length)
	panicOn(err)
	if used != len(lenBy) {
		panic("short read")
	}
	//err = binary.Read(d.ciph, binary.BigEndian, &length)
	if err != nil {
		return
	}
	if length > maxMessage {
		return n, ErrTooLong
	}

	// Read the encrypted data
	var nfull int
	encrypted := make([]byte, length)
	nfull, err = io.ReadFull(d.ciph, encrypted)
	if err != nil {
		return
	}
	_ = nfull
	//vv("past ReadFull, nfull=%v", nfull)

	// Decrypt the data
	var decrypted []byte

	// if the "autheticated data" of lenBy got messed with, do we detect it? yep!
	// lenBy[3]++
	// error: chacha20poly1305: message authentication failed

	decrypted, err = d.aead.Open(nil, encrypted[:d.noncesize], encrypted[d.noncesize:], lenBy)
	if err != nil {
		return
	}
	//vv("decoder.read decrypted data ok: '%v'", string(decrypted))

	// Copy decrypted data to plain
	more := copy(plain, decrypted)
	plain = plain[more:]
	n += more
	//vv("copy to plain, more=%v; now len = %v", more, len(plain))
	if len(decrypted) > more {
		d.plain = append(d.plain, decrypted[more:]...)
	}
	if len(plain) == 0 {
		return
	}
	//	}
	return
}

/*
// Write ciphertext to be decoded into the Decoder.
// net.Conn will do this for us, when we read from decoder, so not really needed.
func (d *Decoder) Write(ciphertext []byte) (n int, err error) {
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
func (e *Encoder) ReadFrom(src io.Reader) (n int64, err error) {
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
