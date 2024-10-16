package main

import (
	"bytes"
	"crypto/cipher"
	cryrand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	//"io"
	"log"
	"sync"

	"golang.org/x/crypto/chacha20poly1305"
)

func NewCryptoRandKey() []byte {
	key := make([]byte, chacha20poly1305.KeySize)
	if _, err := cryrand.Read(key); err != nil {
		log.Fatal(err)
	}
	return key
}

type Encoder struct {
	key  []byte       // must be 32 bytes == chacha20poly1305.KeySize
	aead cipher.AEAD  // XChaCha20-Poly1305, needs 256-bit key
	ciph bytes.Buffer // stored encrypted for the reader

	writeNonce []byte
	noncesize  int
	overhead   int

	mut sync.Mutex
}

type Decoder struct {
	key  []byte
	aead cipher.AEAD
	ciph bytes.Buffer // written until read is needed

	plain []byte // extra decrypted not yet read; read from here first.

	noncesize int
	overhead  int

	mut sync.Mutex
}

func NewEncoderDecoderPair(key []byte) (enc *Encoder, dec *Decoder) {
	aeadEnc, err := chacha20poly1305.NewX(key)
	panicOn(err)

	aeadDec, err := chacha20poly1305.NewX(key)
	panicOn(err)

	// Initialize nonce with random data
	writeNonce := make([]byte, aeadEnc.NonceSize())
	_, err = cryrand.Read(writeNonce)
	panicOn(err)

	enc = &Encoder{
		key:        key,
		aead:       aeadEnc,
		writeNonce: writeNonce,
		noncesize:  aeadEnc.NonceSize(),
		overhead:   aeadEnc.Overhead(),
	}
	dec = &Decoder{
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
	return errors.New("nonce overflow")
}

func (e *Encoder) Read(ciph []byte) (n int, err error) {
	e.mut.Lock()
	defer e.mut.Unlock()

	return e.ciph.Read(ciph)
}

// Write encrypts data and writes it to the underlying stream.
func (e *Encoder) Write(plaintext []byte) (n int, err error) {
	// encryption
	e.mut.Lock()
	defer e.mut.Unlock()

	// Encrypt the data (prepends the nonce? nope need to do space)
	space := make([]byte, e.noncesize, e.noncesize+len(plaintext)+e.overhead)
	copy(space, e.writeNonce)
	noncePlusEncrypted := e.aead.Seal(space, e.writeNonce, plaintext, nil)

	// Increment the nonce: hold mut if not already.
	err = incrementNonce(e.writeNonce)
	if err != nil {
		return 0, err
	}

	// Write the length prefix
	err = binary.Write(&e.ciph, binary.BigEndian, uint32(len(noncePlusEncrypted)))
	if err != nil {
		e.ciph.Reset()
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
func (d *Decoder) Read(plain []byte) (n int, err error) {
	d.mut.Lock()
	defer d.mut.Unlock()

	goal := len(plain)
	for n < goal {

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
		err = binary.Read(&d.ciph, binary.BigEndian, &length)
		if err != nil {
			return
		}

		// Read the encrypted data
		encrypted := make([]byte, length)
		_, err = io.ReadFull(&d.ciph, encrypted)
		if err != nil {
			return
		}

		// Decrypt the data
		var decrypted []byte
		decrypted, err = d.aead.Open(nil, encrypted[:d.noncesize], encrypted[d.noncesize:], nil)
		if err != nil {
			return
		}

		// Copy decrypted data to plain
		more := copy(plain, decrypted)
		plain = plain[more:]
		n += more
		if len(decrypted) > more {
			d.plain = append(d.plain, decrypted[more:]...)
		}
		if len(plain) == 0 {
			return
		}
	}
	return
}

// Write ciphertext to be decoded into the Decoder.
func (d *Decoder) Write(ciphertext []byte) (n int, err error) {
	// write into d.ciph to accumulate
	d.mut.Lock()
	defer d.mut.Unlock()

	// lazily wait until we are read from to decode,
	// because we might not have it all?
	return d.ciph.Write(ciphertext)
}

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

/*
// WriteTo reads encrypted data from the underlying reader, decrypts it, and writes plaintext data to dst.
func (d *Decoder) WriteTo(dst io.Writer) (n int64, err error) {
	for {
		// Read the length prefix
		var length uint32
		err = binary.Read(&d.ciph, binary.BigEndian, &length)
		if err != nil {
			if err == io.EOF {
				return n, nil
			}
			return n, err
		}

		// Read the encrypted data
		encrypted := make([]byte, length)
		_, err = io.ReadFull(&d.ciph, encrypted)
		if err != nil {
			return n, err
		}

		// Decrypt the data
		decrypted, err := d.aead.Open(nil, d.readNonce, encrypted, nil)
		if err != nil {
			return n, err
		}

		// Increment the nonce
		d.mut.Lock()
		err = incrementNonce(d.readNonce)
		d.mut.Unlock()
		if err != nil {
			return n, err
		}

		// Write decrypted data to dst
		written, writeErr := dst.Write(decrypted)
		if writeErr != nil {
			return n, writeErr
		}

		if written != len(decrypted) {
			return n, io.ErrShortWrite
		}

		n += int64(written)
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
