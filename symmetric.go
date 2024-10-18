package rpc25519

import (
	cryrand "crypto/rand"
	"crypto/sha256"
	"golang.org/x/crypto/curve25519"
	"golang.org/x/crypto/hkdf"
	"io"
)

func symmetricServerHandshake(conn uConn, psk [32]byte) (sharedSecretRandomSymmetricKey [32]byte, err error) {
	//vv("top of symmetricServerHandshake")

	// Generate ephemeral X25519 key pair
	serverPrivateKey, serverPublicKey, err := generateX25519KeyPair()
	if err != nil {
		panic(err)
	}

	// Read the client's public key. Server *must* read first, since
	// QUIC streams are only established when the client writes.
	clientPublicKey := make([]byte, 32)
	_, err = io.ReadFull(conn, clientPublicKey)
	if err != nil {
		panic(err)
	}

	// Send the public key to the client
	_, err = conn.Write(serverPublicKey[:])
	if err != nil {
		panic(err)
	}

	// Compute the shared secret
	sharedSecret, err := curve25519.X25519(serverPrivateKey[:], clientPublicKey)
	if err != nil {
		panic(err)
	}

	// Derive the final symmetric key using HKDF
	var ssec [32]byte
	n := copy(ssec[:], sharedSecret)
	if n != 32 {
		panic("sharedSecret must be 32 bytes")
	}

	key := deriveSymmetricKeyFromBaseSymmetricAndSharedRandomSecret(ssec, psk)

	// Print the symmetric key (for demonstration purposes)
	//fmt.Printf("Server derived symmetric key: %x\n", key[:])

	return key, nil
}

func generateX25519KeyPair() (privateKey, publicKey [32]byte, err error) {
	_, err = cryrand.Read(privateKey[:])
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

func deriveSymmetricKeyFromBaseSymmetricAndSharedRandomSecret(sharedSecret, psk [32]byte) [32]byte {
	// Use HKDF with SHA-256, mixing in the pre-shared key
	hkdf := hkdf.New(sha256.New, sharedSecret[:], psk[:], nil)

	var finalKey [32]byte
	_, err := io.ReadFull(hkdf, finalKey[:])
	if err != nil {
		panic(err)
	}

	return finalKey
}

func symmetricClientHandshake(conn uConn, psk [32]byte) (sharedSecretRandomSymmetricKey [32]byte, err error) {
	//vv("top of symmetricClientHandshake")

	// Generate ephemeral X25519 key pair
	clientPrivateKey, clientPublicKey, err := generateX25519KeyPair()
	if err != nil {
		panic(err)
	}

	// Send the client's public key to the server. Client must write first (for QUIC).
	_, err = conn.Write(clientPublicKey[:])
	if err != nil {
		panic(err)
	}

	// Read the server's public key
	serverPublicKey := make([]byte, 32)
	_, err = io.ReadFull(conn, serverPublicKey)
	if err != nil {
		panic(err)
	}

	// Compute the shared secret
	sharedSecret, err := curve25519.X25519(clientPrivateKey[:], serverPublicKey)
	if err != nil {
		panic(err)
	}

	// Derive the final symmetric key using HKDF
	var ssec [32]byte
	n := copy(ssec[:], sharedSecret)
	if n != 32 {
		panic("sharedSecret must be 32 bytes")
	}
	key := deriveSymmetricKeyFromBaseSymmetricAndSharedRandomSecret(ssec, psk)

	// Print the symmetric key (for demonstration purposes)
	//fmt.Printf("Client derived symmetric key: %x\n", key[:])
	return key, nil
}
