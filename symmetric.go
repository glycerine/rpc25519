package rpc25519

import (
	"crypto/ed25519"
	cryrand "crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"fmt"
	"io"
	"time"

	"github.com/glycerine/greenpack/msgp"
	"github.com/glycerine/rpc25519/selfcert"
	"golang.org/x/crypto/curve25519"
	"golang.org/x/crypto/hkdf"
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
// your VPN carefully[2], as needs to help with replay/DoS
// attacks since TCP + PSK alone is weak there.
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
Aug 28, 2024, 3:26:47 AM
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
*/

var _ = fmt.Printf

// For forward privacy, the ephemeral ECDH handshake
// serverPrivateKey generated here
// is deliberately forgotten and not returned.
func symmetricServerHandshake(conn uConn, psk [32]byte) (sharedRandomSecret [32]byte, cliEphemPub, srvEphemPub []byte, err0 error) {
	//vv("top of symmetricServerHandshake")

	// Generate ephemeral X25519 key pair
	serverPrivateKey, serverPublicKey, err := generateX25519KeyPair()
	if err != nil {
		panic(err)
	}

	// set return value
	srvEphemPub = serverPublicKey[:]

	// Read the client's public key. Server *must* read first, since
	// QUIC streams are only established when the client writes.
	clientPublicKey := make([]byte, 32)
	_, err = io.ReadFull(conn, clientPublicKey)
	if err != nil {
		panic(err)
	}
	cliEphemPub = clientPublicKey

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

	sharedRandomSecret = deriveSymmetricKeyFromBaseSymmetricAndSharedRandomSecret(ssec, psk)

	// Print the symmetric key (for demonstration purposes)
	//fmt.Printf("Server derived symmetric key: %x\n", sharedRandomSecret[:])

	return
}

//go:generate greenpack

// VerifiedHandshake lets us verify that our CA has signed
// the SigningCert, and that the SigningCert has in turn signed
// the EphemPubKey. The EphemPubKey is X25519 not ed25519,
// but that is the right thing for doing the ephemeral
// elliptic curve Diffie-Hellman handshake that gives us
// a shared secret to mix with our pre-shared key.
type VerifiedHandshake struct {
	EphemPubKey      []byte `zid:"0"`
	SignatureOfEphem []byte `zid:"1"`
	SigningCert      []byte `zid:"2"`
}

// symmetricServerVerifiedHandshake
// is a version of the above that also checks the counterparty's
// cert was signed by our shared CA.
//
// It signs the ephemeral public key, sends our static certificate, and verifies the
// client's certificate received in the same way.
func symmetricServerVerifiedHandshake(
	conn uConn,
	psk [32]byte,
	creds *selfcert.Creds,
) (sharedRandomSecret [32]byte, cliEphemPub, srvEphemPub []byte, err0 error) {

	// Generate ephemeral X25519 key pair
	serverEphemPrivateKey, serverEphemPublicKey, err := generateX25519KeyPair()
	if err != nil {
		err0 = fmt.Errorf("failed to generate server X25519 key pair: %v", err)
		return
	}

	// set return value
	srvEphemPub = serverEphemPublicKey[:]

	// Sign the server's ephemeral public key with the static Ed25519 private key
	signature := ed25519.Sign(creds.NodePrivateKey, srvEphemPub)

	// Marshal the server's certificate
	//serverCertBytes := serverCert.Raw

	// server shake
	sshake := VerifiedHandshake{
		EphemPubKey:      srvEphemPub,
		SignatureOfEphem: signature,
		SigningCert:      creds.NodeCert.Raw,
	}

	err = msgp.Encode(conn, &sshake)
	if err != nil {
		panic(err)
		err0 = fmt.Errorf("at server, could not encode our server side handshake: '%v'", err)
		return
	}

	// client shake
	var cshake VerifiedHandshake
	err = msgp.Decode(conn, &cshake)
	if err != nil {
		panic(err)
		err0 = fmt.Errorf("at server, could not decode from client the client handshake: '%v'", err)
		return
	}

	// Parse the client's certificate
	clientCert, err := x509.ParseCertificate(cshake.SigningCert)
	if err != nil {
		err0 = fmt.Errorf("failed to parse client certificate: %v", err)
		return
	}

	// Verify the client's certificate against the CA
	opts := x509.VerifyOptions{
		Roots:         creds.CACertPool,
		CurrentTime:   time.Now(),
		Intermediates: x509.NewCertPool(),
	}
	if _, err = clientCert.Verify(opts); err != nil {
		err0 = fmt.Errorf("client certificate verification failed: %v", err)
		return
	}

	// Extract the client's static Ed25519 public key from the certificate
	clientStaticPubKey, ok := clientCert.PublicKey.(ed25519.PublicKey)
	if !ok {
		err0 = fmt.Errorf("client certificate does not contain an Ed25519 public key")
		return
	}

	// Verify the client's signature on their ephemeral public key
	if !ed25519.Verify(clientStaticPubKey, cshake.EphemPubKey, cshake.SignatureOfEphem) {
		err0 = fmt.Errorf("client's ephemeral public key signature verification failed")
		return
	}

	// set return value
	cliEphemPub = cshake.EphemPubKey

	// Compute the shared secret
	sharedSecret, err := curve25519.X25519(serverEphemPrivateKey[:], cshake.EphemPubKey)
	if err != nil {
		panic(err)
	}

	// Derive the final symmetric key using HKDF
	var ssec [32]byte
	n := copy(ssec[:], sharedSecret)
	if n != 32 {
		panic("sharedSecret must be 32 bytes")
	}

	sharedRandomSecret = deriveSymmetricKeyFromBaseSymmetricAndSharedRandomSecret(ssec, psk)

	// Print the symmetric key (for demonstration purposes)
	//fmt.Printf("(verified) Server derived symmetric key: %x\n", sharedRandomSecret[:])

	return
}

func computeSharedSecret(serverPrivateKey, clientPublicKey []byte) (sharedSecret []byte, err error) {

	sharedSecret, err = curve25519.X25519(serverPrivateKey, clientPublicKey)
	if err != nil {
		panic(err)
	}
	return
}

func generateX25519KeyPair() (privateKey, publicKey [32]byte, err error) {

	_, err = cryrand.Read(privateKey[:])
	panicOn(err)

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

// For forward privacy, the ephemeral ECDH handshake
// clientPrivateKey generated here
// is deliberately forgotten and not returned.
func symmetricClientHandshake(conn uConn, psk [32]byte) (sharedRandomSecret [32]byte, cliEphemPub, srvEphemPub []byte, err0 error) {
	//vv("top of symmetricClientHandshake")

	// Generate ephemeral X25519 key pair
	clientPrivateKey, clientPublicKey, err := generateX25519KeyPair()
	if err != nil {
		panic(err)
	}

	// set return value
	cliEphemPub = clientPublicKey[:]

	// Send the client's public key to the server. Client must write first (for QUIC).
	_, err = conn.Write(clientPublicKey[:])
	if err != nil {
		panic(err)
	}

	// Read the server's public key
	serverPublicKey := make([]byte, 32)
	_, err = io.ReadFull(conn, serverPublicKey)
	if err != nil {
		err0 = fmt.Errorf("symmetricClientHandshake error: could not read handshake: '%v'", err)
		return
	}
	srvEphemPub = serverPublicKey

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
	sharedRandomSecret = deriveSymmetricKeyFromBaseSymmetricAndSharedRandomSecret(ssec, psk)

	// Print the symmetric key (for demonstration purposes)
	//fmt.Printf("Client derived symmetric key: %x\n", sharedRandomSecret[:])
	return
}

// verified version of the above
func symmetricClientVerifiedHandshake(
	conn uConn,
	psk [32]byte,
	creds *selfcert.Creds,
) (sharedRandomSecret [32]byte, cliEphemPub, srvEphemPub []byte, err0 error) {

	//vv("top of symmetricClientVerifiedHandshake")

	// Generate ephemeral X25519 key pair
	clientEphemPrivateKey, clientEphemPublicKey, err := generateX25519KeyPair()
	if err != nil {
		panic(err)
	}

	// set return value
	cliEphemPub = clientEphemPublicKey[:]

	// Send the client's public key to the server. Client must write first (for QUIC).

	// Sign the client's ephemeral public key with the static Ed25519 private key
	signature := ed25519.Sign(creds.NodePrivateKey, cliEphemPub)

	// Marshal the server's certificate
	//clientCertBytes := clientCert.Raw

	cshake := VerifiedHandshake{
		EphemPubKey:      cliEphemPub,
		SignatureOfEphem: signature,
		SigningCert:      creds.NodeCert.Raw,
	}

	err = msgp.Encode(conn, &cshake)
	if err != nil {
		panic(err)
		err0 = fmt.Errorf("at client, could not encode our client side handshake: '%v'", err)
		return
	}

	var sshake VerifiedHandshake
	err = msgp.Decode(conn, &sshake)
	if err != nil {
		panic(err)
		err0 = fmt.Errorf("at client, could not decode from server the server handshake: '%v'", err)
		return
	}

	// Parse the server's certificate
	serverCert, err := x509.ParseCertificate(sshake.SigningCert)
	if err != nil {
		panic(err)
		err0 = fmt.Errorf("failed to parse servers certificate: '%v'", err)
		return
	}

	// Verify the server's certificate against the CA
	opts := x509.VerifyOptions{
		Roots:         creds.CACertPool,
		CurrentTime:   time.Now(),
		Intermediates: x509.NewCertPool(),
	}
	if _, err = serverCert.Verify(opts); err != nil {
		err0 = fmt.Errorf("server certificate verification failed: '%v'", err)
		return
	}

	// Extract the server's static Ed25519 public key from the certificate
	serverStaticPubKey, ok := serverCert.PublicKey.(ed25519.PublicKey)
	if !ok {
		err0 = fmt.Errorf("server certificate does not contain an Ed25519 public key")
		return
	}

	// Verify the server's signature on their ephemeral public key
	if !ed25519.Verify(serverStaticPubKey, sshake.EphemPubKey, sshake.SignatureOfEphem) {
		err0 = fmt.Errorf("server's ephemeral public key signature verification failed")
		return
	}

	// set return value
	srvEphemPub = sshake.EphemPubKey

	// Compute the shared secret
	sharedSecret, err := curve25519.X25519(clientEphemPrivateKey[:], sshake.EphemPubKey)
	if err != nil {
		panic(err)
	}

	// Derive the final symmetric key using HKDF
	var ssec [32]byte
	n := copy(ssec[:], sharedSecret)
	if n != 32 {
		panic("sharedSecret must be 32 bytes")
	}
	sharedRandomSecret = deriveSymmetricKeyFromBaseSymmetricAndSharedRandomSecret(ssec, psk)

	// Print the symmetric key (for demonstration purposes)
	//fmt.Printf("(verified) Client derived symmetric key: %x\n", sharedRandomSecret[:])
	return
}
