package main

import (
	"bytes"
	"crypto/cipher"
	cryrand "crypto/rand"
	"fmt"
	//"io"
	"log"

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
	key    []byte       // must be 32 bytes == chacha20poly1305.KeySize
	aead   cipher.AEAD  // XChaCha20-Poly1305, needs 256-bit key
	cipher bytes.Buffer // stored encrypted for the reader
}

type Decoder struct {
	key   []byte
	aead  cipher.AEAD
	plain bytes.Buffer // stored decrypted for the reader
}

func NewEncoderDecoderPair(key []byte) (enc *Encoder, dec *Decoder) {
	aeadEnc, err := chacha20poly1305.NewX(key)
	panicOn(err)

	aeadDec, err := chacha20poly1305.NewX(key)
	panicOn(err)

	enc = &Encoder{
		key:  key,
		aead: aeadEnc,
	}
	dec = &Decoder{
		key:  key,
		aead: aeadDec,
	}
	return
}

func (e *Encoder) Read(cipher []byte) (n int, err error) {
	return e.cipher.Read(cipher)
}

func (e *Encoder) Write(plaintext []byte) (n int, err error) {
	// encryption
	cipher, err := encryptChaCha20Poly1305(plaintext, e.key)
	if err != nil {
		return 0, err
	}
	nw, err := e.cipher.Write(cipher)
	panicOn(err)
	if nw != len(cipher) {
		panic("short write")
	}
	return len(plaintext), nil
}

func (d *Decoder) Read(plain []byte) (n int, err error) {
	return d.plain.Read(plain)
}

func (d *Decoder) Write(ciphertext []byte) (n int, err error) {
	// decryption
	plain, err := decryptChaCha20Poly1305(ciphertext, d.key)
	if err != nil {
		return 0, err
	}
	nw, err := d.plain.Write(plain)
	panicOn(err)
	if nw != len(plain) {
		panic("short write")
	}
	return len(ciphertext), nil
}

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
