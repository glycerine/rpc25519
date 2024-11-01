package main

import (
	cryrand "crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"

	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/curve25519"
	"golang.org/x/crypto/hkdf"
)

func encrypt(recipientPublicKey [32]byte, plaintext []byte) (ephemeralPublicKey [32]byte, ciphertext []byte, err error) {
	// Generate ephemeral private key
	var ephemeralPrivateKey [32]byte
	_, err = cryrand.Read(ephemeralPrivateKey[:])
	if err != nil {
		return
	}

	// Compute ephemeral public key
	curve25519.ScalarBaseMult(&ephemeralPublicKey, &ephemeralPrivateKey)

	// Compute shared secret
	var sharedSecret [32]byte
	curve25519.ScalarMult(&sharedSecret, &ephemeralPrivateKey, &recipientPublicKey)

	// Derive symmetric key using HKDF with SHA-256
	hkdf := hkdf.New(sha256.New, sharedSecret[:], nil, nil)
	symmetricKey := make([]byte, chacha20poly1305.KeySize)
	_, err = io.ReadFull(hkdf, symmetricKey)
	if err != nil {
		return
	}

	// Encrypt the plaintext using ChaCha20-Poly1305
	aead, err := chacha20poly1305.New(symmetricKey)
	if err != nil {
		return
	}
	nonce := make([]byte, aead.NonceSize())
	_, err = cryrand.Read(nonce)
	if err != nil {
		return
	}
	ciphertext = aead.Seal(nonce, nonce, plaintext, nil)
	return
}

func decrypt(recipientPrivateKey [32]byte, ephemeralPublicKey [32]byte, ciphertext []byte) (plaintext []byte, err error) {
	// Compute shared secret
	var sharedSecret [32]byte
	curve25519.ScalarMult(&sharedSecret, &recipientPrivateKey, &ephemeralPublicKey)

	// Derive symmetric key using HKDF with SHA-256
	hkdf := hkdf.New(sha256.New, sharedSecret[:], nil, nil)
	symmetricKey := make([]byte, chacha20poly1305.KeySize)
	_, err = io.ReadFull(hkdf, symmetricKey)
	if err != nil {
		return
	}

	// Decrypt the ciphertext using ChaCha20-Poly1305
	aead, err := chacha20poly1305.New(symmetricKey)
	if err != nil {
		return
	}
	if len(ciphertext) < aead.NonceSize() {
		err = fmt.Errorf("ciphertext too short")
		return
	}
	nonce, ct := ciphertext[:aead.NonceSize()], ciphertext[aead.NonceSize():]
	plaintext, err = aead.Open(nil, nonce, ct, nil)
	return
}

func main() {
	// Recipient generates a key pair
	var recipientPrivateKey, recipientPublicKey [32]byte
	_, err := cryrand.Read(recipientPrivateKey[:])
	if err != nil {
		panic(err)
	}
	curve25519.ScalarBaseMult(&recipientPublicKey, &recipientPrivateKey)

	// Message to encrypt
	message := []byte("Hello, this is a secret message.")

	// Sender encrypts the message
	ephemeralPublicKey, ciphertext, err := encrypt(recipientPublicKey, message)
	if err != nil {
		panic(err)
	}

	// Recipient decrypts the message
	decryptedMessage, err := decrypt(recipientPrivateKey, ephemeralPublicKey, ciphertext)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Decrypted message: %s\n", decryptedMessage)
}
