package main

import (
	//"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"golang.org/x/crypto/curve25519"
	"golang.org/x/crypto/hkdf"
	"io"
	"net"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// Generate ephemeral X25519 key pair
	clientPrivateKey, clientPublicKey, err := generateX25519KeyPair()
	if err != nil {
		panic(err)
	}

	// Read the server's public key
	serverPublicKey := make([]byte, 32)
	_, err = io.ReadFull(conn, serverPublicKey)
	if err != nil {
		panic(err)
	}

	// Send the client's public key to the server
	_, err = conn.Write(clientPublicKey[:])
	if err != nil {
		panic(err)
	}

	// Compute the shared secret
	sharedSecret, err := curve25519.X25519(clientPrivateKey[:], serverPublicKey)
	if err != nil {
		panic(err)
	}

	// Define the same pre-shared key (PSK) used by the server
	psk := []byte("mysecurepre-sharedkeythatis256bits!") // Example 32-byte PSK

	// Derive the final symmetric key using HKDF
	key := deriveKey(sharedSecret, psk)

	// Print the symmetric key (for demonstration purposes)
	fmt.Printf("Client derived symmetric key: %x\n", key[:])
}

func generateX25519KeyPair() (privateKey, publicKey [32]byte, err error) {
	_, err = rand.Read(privateKey[:])
	if err != nil {
		return privateKey, publicKey, err
	}

	privateKey[0] &= 248
	privateKey[31] &= 127
	privateKey[31] |= 64

	publicKeyBytes, err := curve25519.X25519(privateKey[:], curve25519.Basepoint)
	if err != nil {
		return privateKey, publicKey, err
	}

	copy(publicKey[:], publicKeyBytes)

	return privateKey, publicKey, nil
}

func deriveKey(sharedSecret, psk []byte) [32]byte {
	// Use HKDF with SHA-256, mixing in the pre-shared key
	hkdf := hkdf.New(sha256.New, sharedSecret, psk, nil)

	var finalKey [32]byte
	_, err := io.ReadFull(hkdf, finalKey[:])
	if err != nil {
		panic(err)
	}

	return finalKey
}

/*
package main

import (
	cryrand "crypto/rand"
	"crypto/sha256"
	"fmt"
	"golang.org/x/crypto/curve25519"
	"io"
	"net"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// Generate ephemeral X25519 key pair
	clientPrivateKey, clientPublicKey, err := generateX25519KeyPair()
	if err != nil {
		panic(err)
	}

	// Read the server's public key
	serverPublicKey := make([]byte, 32)
	_, err = io.ReadFull(conn, serverPublicKey)
	if err != nil {
		panic(err)
	}

	// Send the client's public key to the server
	_, err = conn.Write(clientPublicKey[:])
	if err != nil {
		panic(err)
	}

	// Compute the shared secret
	sharedSecret, err := curve25519.X25519(clientPrivateKey[:], serverPublicKey)
	if err != nil {
		panic(err)
	}

	// Derive a symmetric key from the shared secret (using SHA-256 as a simple KDF)
	key := sha256.Sum256(sharedSecret)

	// Print the symmetric key (for demonstration purposes)
	fmt.Printf("Client derived symmetric key: %x\n", key[:])
}

// generateX25519KeyPair generates an ephemeral X25519 key pair.
func generateX25519KeyPair() (privateKey, publicKey [32]byte, err error) {
	// Generate a 32-byte random private key
	_, err = cryrand.Read(privateKey[:])
	if err != nil {
		return privateKey, publicKey, err
	}

	// Apply clamping as required by the X25519 spec (clamp private key)
	privateKey[0] &= 248  // Clear the last 3 bits
	privateKey[31] &= 127 // Clear the last bit
	privateKey[31] |= 64  // Set the second to last bit

	// Derive the public key from the private key using curve25519
	publicKeyBytes, err := curve25519.X25519(privateKey[:], curve25519.Basepoint)
	if err != nil {
		return privateKey, publicKey, err
	}

	// Convert the []byte result to a [32]byte array
	copy(publicKey[:], publicKeyBytes)

	return privateKey, publicKey, nil
}
*/
