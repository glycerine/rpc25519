package rpc25519

import (
	cryrand "crypto/rand"
	"crypto/sha256"
	"fmt"
	"golang.org/x/crypto/curve25519"
	"golang.org/x/crypto/hkdf"
	"io"
)

// Intent of symmetric.go: provide "inside" layer of
// encryption for post-quantum resistance.
//
// Like Wireguard, we do not use the symmetric
// pre-shared-key (PSK) from disk directly for encryption.
//
// Instead we do an elliptic curve Diffie-Hellman handshake
// first to get a shared random secret for this session.
// TLS-v1.3 does the same. This provides forward secrecy
// and a per-session (shared) random secret for the symmetric
// encryption that follows.
//
// The difference with the PSK is: we then strong-hash[1] together
// the per-session random secret with the PSK from
// disk to derive the symmetric encryption session
// key for this session (QUIC stream/TLS conn).
// Wireguard does the same but in the "outer" session;
// it has no "inner" session.
//
// Each of our QUIC streams or TLS connections (or TCP sessions)
// will do this second handshake if pre-shared-keys are enabled.
// We do this handshake as the very first communication
// inside the already encrypted stream, before any other
// messages are exchanged.
//
// This provides the 2nd layer with post-quantum resistance,
// even if the newer post-quantum resistance options
// for the outer TLS-v1.3 (that came in go1.23; see below)
// are not used.
//
// It can also provide post-quantum resistance for the TCP
// alone option, but this is still vulnerable to active
// replay/DoS attacks. For details of such attacks, see for example the
// discussion under "DoS Mitigation" in the
// Wireguard docs: https://www.wireguard.com/protocol/
//
// We cannot, therefore, recommend that you use TCP alone --
// even if secured by the ephemeral ECDH handshake with PSK --
// unless your application's security model can afford
// to ignore active attackers and denial-of-service attacks.
// This may be the case if you are already inside a VPN,
// for instance. Then opting for triple encryption
// (VPN + TLS + 2nd symmetric) may be overkill. If
// you already have pre-shared keys and means to
// rotate them, then under this scenario opting for just
// two layers (VPN + TCP with PSK symmetric encryption)
// might make sense. However consider the limitations of
// your VPN here[2], as it will be doing the heavy
// lifting/providing most of the security.
//
// [1] Using the "HMAC-based Extract-and-Expand Key Derivation
//     Function (HKDF) as defined in RFC 5869" as implemented
//     in https://pkg.go.dev/golang.org/x/crypto/hkdf
//     Its docs:
//     "HKDF is a cryptographic key derivation function (KDF)
//      with the goal of expanding limited input keying material
//      into one or more cryptographically strong secret keys."
//
// [2] for example: https://www.wireguard.com/known-limitations/
//
// Note: In go1.23 there actually is some post-quantum resistance
// for the TLS handshake built in by default:
/*
https://groups.google.com/g/golang-dev/c/-hmSqJm03V0/m/MYGjVWUzCgAJ
Aug 28, 2024, 3:26:47 AM
Russ Cox wrote:

A year ago Filippo wrote back to you saying:

> We're experimenting with Kyber, as KEMs are the most
> urgent concern to protect against collect-now-decrypt-later
> attacks. It's unlikely we'll expose an API in the
> standard library before NIST produces a final specification,
> but we might enable draft hybrid key exchanges in
> crypto/tls in the meantime, maybe behind a GOEXPERIMENT flag.

That happened. Go 1.23, released a couple weeks ago, includes
a production Kyber implementation that is enabled by default
in crypto/tls. Quoting the release notes:

> The experimental post-quantum key exchange mechanism
> X25519Kyber768Draft00 is now enabled by default when
> Config.CurvePreferences is nil. The default can be
> reverted by adding tlskyber=0 to the GODEBUG
> environment variable.

Best,
Russ
*/

var _ = fmt.Printf

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

	nr := 0
	nr, err = cryrand.Read(privateKey[:])
	panicOn(err)
	if nr != 32 {
		panic("short read from cryrand.Read()")
	}

	// https://cr.yp.to/ecdh.html
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

	// Use HKDF with SHA-256, mixing in the pre-shared key.
	// Note that the psk plays the role of the "salt" in this,
	// but still must be kept secret in our application.
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
