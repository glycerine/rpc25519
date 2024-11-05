package rpc25519

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ed25519"
	cryrand "crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/glycerine/greenpack/msgp"
	"github.com/glycerine/rpc25519/selfcert"
	"golang.org/x/crypto/chacha20poly1305"
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
// denial-of-service (DoS) attacks. For details of such
// attacks, see for example the
// discussion under "DoS Mitigation" in the
// Wireguard docs: https://www.wireguard.com/protocol/
//
// We cannot, therefore, recommend that you use TCP alone --
// even if secured by the ephemeral ECDH handshake with PSK --
// _unless_ your application's security model can afford
// to ignore active attackers and denial-of-service attacks.
// This may be the case if you are already inside a VPN,
// for instance. Then opting for triple encryption
// (VPN + TLS + 2nd symmetric) may be overkill. If
// you already have pre-shared keys and means to
// rotate them, then under this scenario opting for just
// two layers (VPN + TCP with PSK symmetric encryption)
// might make sense. However consider the limitations of
// your VPN carefully[2], as needs to help with active DoS
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

// Note: you probably want the symmetricServerVerifiedHandshake()
// function below, as it prevents man-in-the-middle attacks.
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
		err0 = fmt.Errorf("could not read client public key from conn: '%v'", err)
		return
	}
	cliEphemPub = clientPublicKey

	if !isValidX25519PublicKey(clientPublicKey) {
		err0 = fmt.Errorf("we read an invalid clientPublicKey: '%x'", clientPublicKey)
		return
	}

	// Send the public key to the client
	_, err = conn.Write(serverPublicKey[:])
	if err != nil {
		err0 = fmt.Errorf("symmetricServerHandshake could not write to conn: '%v'", err)
		return
	}

	// Compute the shared secret
	sharedSecret, err := curve25519.X25519(serverPrivateKey[:], clientPublicKey)
	if err != nil {
		panic(err)
	}

	// Derive the final symmetric key using HKDF
	sharedRandomSecret = deriveSymmetricKeyFromBaseSymmetricAndSharedRandomSecret(sharedSecret, psk[:])

	// Print the symmetric key (for demonstration purposes)
	//fmt.Printf("Server derived symmetric key: %x\n", sharedRandomSecret[:])

	return
}

//go:generate greenpack -unexported

// verifiedHandshake lets us verify that our CA has signed
// the SigningCert, and that the SigningCert has in turn signed
// the EphemPubKey. The EphemPubKey is X25519 not ed25519,
// but that is the right thing for doing the ephemeral
// elliptic curve Diffie-Hellman handshake that gives us
// a shared secret to mix with our pre-shared key.
type verifiedHandshake struct {
	EphemPubKey      []byte    `zid:"0"`
	SignatureOfEphem []byte    `zid:"1"`
	SigningCert      []byte    `zid:"2"`
	SenderSentAt     time.Time `zid:"3"`
}

type clientEncryptedHandshake struct {
	CliEphemPubKey0 []byte `zid:"0"` // client's ephemeral public key 0
	EncPayload      []byte `zid:"1"` // payload: verifiedHandshake, encrypted with client EphemPubKey.
}

type serverEncryptedHandshake struct {
	SrvEphemPubKey0 []byte `zid:"0"` // server's ephemeral public key 0
	EncPayload      []byte `zid:"1"` // payload: caboose, encrypted with dervied symmetric key.
}

// caboose may be sent (only on server response,
// encrypted with symm key); if useCaboose is true.
//
// The caboose is here to match what TLS does if we are being
// used without an outer wrapper: to immediately
// use the symmetric encryption key to verify the
// previously received handshake and thereby to allow the
// client detect and reject replay/active attacks.
//
// It is only sent by the server after the
// server's verifiedHandshake response. It is
// only verified by the client
// so that we don't need another round trip. Everything
// is encrypted anyway with the symmetric key after
// the handshake, and authenticated with that key
// since AEAD is used, so this is
// largely redundant, but does allow
// the client to detect and drop a faulty
// connection faster than waiting for
// future communication to fail to decrypt.
type caboose struct {
	ClientAuthTag     []byte    `zid:"0"` // 32 bytes
	ClientEphemPubKey []byte    `zid:"1"` // 32 bytes
	ClientSigOfEphem  []byte    `zid:"2"` // 64 bytes
	ClientSigningCert []byte    `zid:"3"` // 540 bytes
	ClientSentAt      time.Time `zid:"4"` // 12 bytes

	ServerAuthTag     []byte    `zid:"5"` // 32 bytes
	ServerEphemPubKey []byte    `zid:"6"` // 32 bytes
	ServerSigOfEphem  []byte    `zid:"7"` // 64 bytes
	ServerSigningCert []byte    `zid:"8"` // 540 bytes
	ServerSentAt      time.Time `zid:"9"` // 12 bytes

	// client must rotate to this new public key from server.
	ServerNewPub []byte `zid:"10"` // 32 bytes
}

const useCaboose = true

const maxCabooseBytes = 1750

// VerifiedHandshake when encoded to greenpack
// must be under this length in bytes.
// This allows us to reject binary/bad handshake
// messages. It prevents DDOS/Denial of service
// attacks, because we cannot be tricked into allocating
// more than this.
const maxHandshakeBytes = 914

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
) (sharedRandomSecret [32]byte, cliEphemPub, srvEphemPub []byte, clientStaticPubKey ed25519.PublicKey, err0 error) {

	//vv("server creds = '%#v'", creds)

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

	// Read the client's public key/handshake. Server *must* read first, since
	// QUIC streams are only established when the client writes.
	var cshake verifiedHandshake

	timeout := time.Second * 10
	cshakeTag, err := readLenThenShakeTag(conn, &cshake, &timeout)
	if err != nil {
		err0 = fmt.Errorf("at server could not decode from client the client handshake: '%v'", err)
		return
	}

	if !isValidX25519PublicKey(cshake.EphemPubKey) {
		err0 = fmt.Errorf("we read an invalid client cshake ephem public key: '%x'", cshake.EphemPubKey)
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
		//vv("server sees bad client cert")
		err0 = fmt.Errorf("server cannot verify client cert: %v", err)
		return
	}

	// Extract the client's static Ed25519 public key from the certificate
	var ok bool
	clientStaticPubKey, ok = clientCert.PublicKey.(ed25519.PublicKey)
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

	// since client checked out so far, send our empheral key to handshake.
	// Otherwise we don't even bother generating the network traffic.

	// server shake
	sshake := verifiedHandshake{
		EphemPubKey:      srvEphemPub,
		SignatureOfEphem: signature,
		SigningCert:      creds.NodeCert.Raw,
		SenderSentAt:     time.Now(),
	}
	//vv("len SigningCert = %v", len(sshake.SigningCert)) // 540

	sshakeTag, err := sendLenThenShakeTag(conn, &sshake, &timeout)
	if err != nil {
		err0 = fmt.Errorf("at server, could not encode/send our server side handshake: '%v'", err)
		return
	}

	// Compute the shared secret
	sharedSecret, err := curve25519.X25519(serverEphemPrivateKey[:], cshake.EphemPubKey)
	if err != nil {
		panic(err)
	}

	// Derive the final symmetric key using HKDF
	sharedRandomSecret = deriveSymmetricKeyFromBaseSymmetricAndSharedRandomSecret(sharedSecret, psk[:])

	// Print the symmetric key (for demonstration purposes)
	//fmt.Printf("(verified) Server derived symmetric key: %x\n", sharedRandomSecret[:])

	if useCaboose {
		// Generate new, 2nd, ephemeral X25519 key pair
		serverEphemPrivateKey2, serverEphemPublicKey2, err := generateX25519KeyPair()
		if err != nil {
			err0 = fmt.Errorf("failed to generate server X25519 key pair: %v", err)
			return
		}

		cab := &caboose{
			ClientAuthTag:     cshakeTag,
			ServerAuthTag:     sshakeTag,
			ClientEphemPubKey: cshake.EphemPubKey,
			ServerEphemPubKey: sshake.EphemPubKey,
			ClientSentAt:      cshake.SenderSentAt,
			ServerSentAt:      sshake.SenderSentAt,
			ClientSigningCert: cshake.SigningCert,
			ServerSigningCert: sshake.SigningCert,
			ClientSigOfEphem:  cshake.SignatureOfEphem,
			ServerSigOfEphem:  sshake.SignatureOfEphem,
			ServerNewPub:      serverEphemPublicKey2[:],
		}
		err0 = sendCrypticCaboose(conn, cab, sharedRandomSecret[:], &timeout)
		if err0 != nil {
			return
		}

		// Compute the new shared secret, based on the serverEphemPrivateKey2
		sharedSecret2, err := curve25519.X25519(serverEphemPrivateKey2[:], cshake.EphemPubKey)
		if err != nil {
			panic(err)
		}
		_ = sharedSecret2
	}

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

func deriveSymmetricKeyFromBaseSymmetricAndSharedRandomSecret(sharedSecret, psk []byte) [32]byte {

	if len(sharedSecret) != 32 {
		panic("sharedSecret must be len 32")
	}

	if len(psk) != 32 {
		panic("psk must be len 32")
	}

	// Use HKDF with SHA-256, mixing in the pre-shared key.
	// Note that the psk plays the role of the "salt" in this,
	// but still must be kept secret in our application.
	hkdf := hkdf.New(sha256.New, sharedSecret, psk, nil)

	var finalKey [32]byte
	_, err := io.ReadFull(hkdf, finalKey[:])
	if err != nil {
		panic(err)
	}

	return finalKey
}

// Note: you probably want the symmetricClientVerifiedHandshake()
// function below, as it prevents man-in-the-middle attacks.
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
		err0 = fmt.Errorf("symmetricClientHandshake error: could not write client eph pub key: '%v'", err)
		return
	}

	// Read the server's public key
	serverPublicKey := make([]byte, 32)
	_, err = io.ReadFull(conn, serverPublicKey)
	if err != nil {
		err0 = fmt.Errorf("symmetricClientHandshake error: could not read handshake: '%v'", err)
		return
	}
	srvEphemPub = serverPublicKey

	if !isValidX25519PublicKey(serverPublicKey) {
		err0 = fmt.Errorf("we read an invalid serverPublicKey: '%x'", serverPublicKey)
		return
	}

	// Compute the shared secret
	sharedSecret, err := curve25519.X25519(clientPrivateKey[:], serverPublicKey)
	if err != nil {
		panic(err)
	}

	// Derive the final symmetric key using HKDF
	sharedRandomSecret = deriveSymmetricKeyFromBaseSymmetricAndSharedRandomSecret(sharedSecret, psk[:])

	// Print the symmetric key (for demonstration purposes)
	//fmt.Printf("Client derived symmetric key: %x\n", sharedRandomSecret[:])
	return
}

// verified version of the above
func symmetricClientVerifiedHandshake(
	conn uConn,
	psk [32]byte,
	creds *selfcert.Creds,
) (sharedRandomSecret [32]byte, cliEphemPub, srvEphemPub []byte, serverStaticPubKey ed25519.PublicKey, err0 error) {

	//vv("top of symmetricClientVerifiedHandshake")

	//vv("client creds = '%#v'", creds)

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

	cshake := verifiedHandshake{
		EphemPubKey:      cliEphemPub,
		SignatureOfEphem: signature,
		SigningCert:      creds.NodeCert.Raw,
		SenderSentAt:     time.Now(),
	}

	// client sending first to open quic stream (if using quic).
	timeout := time.Second * 10
	var cshakeTag []byte
	cshakeTag, err0 = sendLenThenShakeTag(conn, &cshake, &timeout)
	if err0 != nil {
		return
	}

	var sshake verifiedHandshake
	var sshakeTag []byte
	sshakeTag, err = readLenThenShakeTag(conn, &sshake, &timeout)
	if err != nil {
		err0 = fmt.Errorf("at client, could not decode from server the server handshake: '%v'", err)
		return
	}

	if !isValidX25519PublicKey(sshake.EphemPubKey) {
		err0 = fmt.Errorf("we read an invalid server sshake public key: '%x'", sshake.EphemPubKey)
		return
	}

	// Parse the server's certificate
	serverStaticCert, err := x509.ParseCertificate(sshake.SigningCert)
	if err != nil {
		err0 = fmt.Errorf("failed to parse servers certificate: '%v'", err)
		return
	}

	// Verify the server's certificate against the CA
	opts := x509.VerifyOptions{
		Roots:         creds.CACertPool,
		CurrentTime:   time.Now(),
		Intermediates: x509.NewCertPool(),
	}
	if _, err = serverStaticCert.Verify(opts); err != nil {
		//vv("client sees bad server signing")
		err0 = fmt.Errorf("client could not verify servers static cert: '%v'", err)
		return
	}
	//vv("client sees ok server signing")

	// Extract the server's static Ed25519 public key from the certificate
	var ok bool
	serverStaticPubKey, ok = serverStaticCert.PublicKey.(ed25519.PublicKey)
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
	sharedRandomSecret = deriveSymmetricKeyFromBaseSymmetricAndSharedRandomSecret(sharedSecret, psk[:])

	if useCaboose {
		cab := &caboose{}
		err0 = readCrypticCaboose(conn, cab, sharedRandomSecret[:], &timeout)
		if err0 != nil {
			return
		}
		// the legit server put the fields as:
		expected := &caboose{
			ClientAuthTag:     cshakeTag,
			ServerAuthTag:     sshakeTag,
			ClientEphemPubKey: cshake.EphemPubKey,
			ServerEphemPubKey: sshake.EphemPubKey,
			ClientSentAt:      cshake.SenderSentAt,
			ServerSentAt:      sshake.SenderSentAt,
			ClientSigningCert: cshake.SigningCert,
			ServerSigningCert: sshake.SigningCert,
			ClientSigOfEphem:  cshake.SignatureOfEphem,
			ServerSigOfEphem:  sshake.SignatureOfEphem,
		}
		if !cab.Equal(expected) {
			err0 = fmt.Errorf("caboose mismatch: got '%#v'; \n\n versus expected '%#v'", cab, expected)
			//vv("err0 = '%v'", err0)
			return
		}
		//vv("caboose was as expected")
	}

	// Print the symmetric key (for demonstration purposes)
	//fmt.Printf("(verified) Client derived symmetric key: %x\n", sharedRandomSecret[:])
	return
}

const authTagLen = 32

func sendLenThenShakeTag(conn uConn, shake *verifiedHandshake, timeout *time.Duration) (authTag []byte, err error) {

	var buf bytes.Buffer
	err = msgp.Encode(&buf, shake)
	if err != nil {
		return nil, fmt.Errorf("sendLenThenShake could not Encode: '%v'", err)
	}

	shakeBytes := buf.Bytes()
	n := len(shakeBytes)
	allby := make([]byte, 8, 8+n+authTagLen) // 8 for length, the handshake, 32 bytes for auth tag based on sha256.

	//vv("allby was length %v -- to set our max length for handshake", 8+n+authTagLen) // 777, 775.

	if 8+n+authTagLen > maxHandshakeBytes {
		panic(fmt.Sprintf("internal error, need to raise maxHandshakeBytes? our "+
			"handshake %v bytes is bigger than maxHandshakeBytes = %v", 8+n+authTagLen, maxHandshakeBytes))
	}

	// write in this order.
	// 8 bytes of n = the length of the greenpack (cannot be over maxHandshakeBytes).
	// n bytes of greenpack
	// 32 bytes of hmac tag computer over the n bytes, with key the first 8 bytes (those encoding n).

	binary.BigEndian.PutUint64(allby[:8], uint64(n))
	allby = append(allby, shakeBytes...)

	// compute an hmac. Use the length bytes as the key
	// to be sure the data is the expected length.
	// The len is all we've got at this point, so
	// early (first thing) in the handshake.
	authTag = computeHMAC(shakeBytes, allby[:8])
	allby = append(allby, authTag...)

	// Write
	err = writeFull(conn, allby, timeout)
	if err != nil {
		return nil, fmt.Errorf("sendLenThenShakeTagcould not write to conn: '%v'", err)
	}

	return authTag, nil
}

// fills in shake as the major aim.
// verifies the hmac tag, keeps length of handshake sane, under maxHandshakeBytes.
func readLenThenShakeTag(conn uConn, shake *verifiedHandshake, timeout *time.Duration) (tag []byte, err error) {

	// Read the first 8 bytes for the Message length
	var lenby [8]byte
	if err := readFull(conn, lenby[:], timeout); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint64(lenby[:])

	// Read the message based on the messageLen
	if n > maxHandshakeBytes {
		// probably an encrypted client against an unencrypted server
		return nil, ErrTooLong
	}

	shakeAndTagBytes := make([]byte, n+authTagLen)
	if err := readFull(conn, shakeAndTagBytes, timeout); err != nil {
		return nil, fmt.Errorf("readLenThenShakeTag error reading from conn: '%v'", err)
	}

	conveyedTag := shakeAndTagBytes[n:]
	shakeBytes := shakeAndTagBytes[:n]
	computedTag := computeHMAC(shakeBytes, lenby[:])

	if !bytes.Equal(conveyedTag, computedTag) {
		return nil, fmt.Errorf("auth tag did not match on handshake")
	}
	tag = conveyedTag

	// after authentication, then msgp decode.
	err = msgp.Decode(bytes.NewBuffer(shakeBytes), shake)
	if err != nil {
		return nil, fmt.Errorf("could not msgp.Decode() handshake: '%v'", err)
	}

	return
}

// isValidX25519PublicKey checks if the provided public key is a valid X25519 public key.
// According to RFC 7748, all 32-byte strings are accepted as valid
// public keys, and the scalar multiplication function processes them securely.
func isValidX25519PublicKey(pubKey []byte) (valid bool) {
	// Check that the public key is exactly 32 bytes long
	if len(pubKey) != 32 {
		return false
	}

	// Check that the public key is not all zeros
	var zero [32]byte
	if bytes.Equal(pubKey, zero[:]) {
		return false
	}

	// No further validation is necessary for X25519
	return true
}

func sendCrypticCaboose(conn uConn, cab *caboose, symkey []byte, timeout *time.Duration) error {

	if len(symkey) != 32 {
		panic("symkey must be 32 bytes")
	}

	var bb bytes.Buffer
	err := msgp.Encode(&bb, cab)
	if err != nil {
		return fmt.Errorf("sendCrypticCaboose could not Encode: '%v'", err)
	}

	bytesMsg := bb.Bytes()
	if len(bytesMsg) > maxCabooseBytes {
		panic(fmt.Sprintf("internal error, need to raise maxCabooseBytes? our "+
			"caboose %v bytes is bigger than maxCabooseBytes = %v", len(bytesMsg), maxCabooseBytes))
	}

	block, err := aes.NewCipher(symkey)
	panicOn(err)
	aeadEnc, err := cipher.NewGCM(block)
	panicOn(err)

	noncesize := aeadEnc.NonceSize()
	overhead := aeadEnc.Overhead()

	n := len(bytesMsg)
	sz := n + noncesize + overhead // does not include the 8 bytes len prefix.
	buf := make([]byte, 8+sz)
	nw := copy(buf[8+noncesize:8+noncesize+n], bytesMsg)
	if nw != n {
		panic("logic error above") // assert our copy was correct.
	}

	// write order/contents of buf:
	// 8 bytes len ; sz = noncesize + n bytes cipher/plain text + authtag overhead (everything that follows).
	// noncesize bytes of nonce
	// len(bytesMsg) cyphertext
	// authtag (overhead) bytes.

	binary.BigEndian.PutUint64(buf[:8], uint64(sz))
	assocData := buf[:8]

	nonce := buf[8 : 8+noncesize]
	_, err = cryrand.Read(nonce)
	panicOn(err)

	// Seal(dst, nonce, plaintext, associatedData) does not prepend the nonce,
	// thus we must ourselves arrange that.
	sealOut := aeadEnc.Seal(
		buf[8+noncesize:8+noncesize],               // destination to be appended to;
		buf[8:8+noncesize],                         // nonce
		buf[8+noncesize:8+noncesize+len(bytesMsg)], // plaintext (will be overwritten with cyphertext)
		assocData)

	// assert all bytes present and accounted for.
	if len(sealOut)+8+noncesize != len(buf) {
		panic("internal miscalc; sealOut len != len(buf)")
	}
	return writeFull(conn, buf, timeout)
}

// fill in cab
func readCrypticCaboose(conn uConn, cab *caboose, symkey []byte, timeout *time.Duration) error {

	if len(symkey) != 32 {
		panic("symkey must be 32 bytes")
	}

	// Read the first 8 bytes for the Message length
	var lenby [8]byte
	if err := readFull(conn, lenby[:], timeout); err != nil {
		return err
	}
	n := binary.BigEndian.Uint64(lenby[:])

	if n > maxCabooseBytes {
		// probably an encrypted client against an unencrypted server
		return fmt.Errorf("Caboose too big: %v bytes indicated, but maxCabooseBytes = %v", n, maxCabooseBytes)
	}
	// Read the message based on the messageLen

	block, err := aes.NewCipher(symkey)
	panicOn(err)
	aeadDec, err := cipher.NewGCM(block)
	panicOn(err)

	noncesize := aeadDec.NonceSize()
	//overhead := aeadDec.Overhead()

	// Read the encrypted data
	encrypted := make([]byte, n)
	err = readFull(conn, encrypted, timeout)
	if err != nil {
		return err
	}

	// Decrypt the data
	assocData := lenby[:] // length of message should be authentic too.
	nonce := encrypted[:noncesize]

	message, err := aeadDec.Open(nil, nonce, encrypted[noncesize:], assocData)
	if err != nil {
		alwaysPrintf("decryption failure at readCrypticCaboose : '%v'", err)
		return err
	}

	err = msgp.Decode(bytes.NewBuffer(message), cab)
	if err != nil {
		return fmt.Errorf("could not msgp.Decode() crypticCaboose: '%v'", err)
	}

	return nil
}

func (a *caboose) Equal(b *caboose) bool {
	if !bytes.Equal(a.ClientAuthTag, b.ClientAuthTag) {
		return false
	}
	if !bytes.Equal(a.ServerAuthTag, b.ServerAuthTag) {
		return false
	}
	if !bytes.Equal(a.ServerEphemPubKey, b.ServerEphemPubKey) {
		return false
	}
	if !bytes.Equal(a.ClientEphemPubKey, b.ClientEphemPubKey) {
		return false
	}
	if !a.ClientSentAt.Equal(b.ClientSentAt) {
		return false
	}
	if !a.ServerSentAt.Equal(b.ServerSentAt) {
		return false
	}
	if !bytes.Equal(a.ClientSigningCert, b.ClientSigningCert) {
		return false
	}
	if !bytes.Equal(a.ServerSigningCert, b.ServerSigningCert) {
		return false
	}
	if !bytes.Equal(a.ClientSigOfEphem, b.ClientSigOfEphem) {
		return false
	}
	if !bytes.Equal(a.ServerSigOfEphem, b.ServerSigOfEphem) {
		return false
	}
	return true
}

func encryptWithPubKey(
	recipientPublicKey [32]byte,
	plaintext []byte,
	scratch []byte,

) (ephemeralPublicKey [32]byte, ciphertext []byte, err0 error) {

	// Generate ephemeral X25519 key pair
	var ephemeralPrivateKey [32]byte
	ephemeralPrivateKey, ephemeralPublicKey, err0 = generateX25519KeyPair()
	panicOn(err0)

	// Compute shared secret

	// func X25519(scalar, point []byte) ([]byte, error)
	//   X25519 returns the result of the scalar
	//   multiplication (scalar * point), according to RFC 7748, Section 5.
	//   scalar, point and the return value are slices of 32 bytes.
	//   scalar can be generated at random, for example with
	//   crypto/rand. point should be either Basepoint or the
	//   output of another X25519 call.

	sharedSecret, err := curve25519.X25519(ephemeralPrivateKey[:], recipientPublicKey[:])
	panicOn(err)

	// Derive symmetric key using HKDF with SHA-256
	hkdf := hkdf.New(sha256.New, sharedSecret[:], nil, nil)
	symmetricKey := make([]byte, chacha20poly1305.KeySize)
	_, err0 = io.ReadFull(hkdf, symmetricKey)
	panicOn(err0)

	// Encrypt the plaintext using ChaCha20-Poly1305
	aead, err := chacha20poly1305.New(symmetricKey)
	panicOn(err)

	nonce := make([]byte, aead.NonceSize())
	_, err = cryrand.Read(nonce)
	panicOn(err)
	scratch = append(scratch, nonce...)

	ciphertext = aead.Seal(scratch, nonce, plaintext, nil)
	return
}

func decryptWithPrivKey(
	recipientPrivateKey [32]byte,
	ephemeralPublicKey [32]byte,
	ciphertext []byte,
	scratch []byte,

) (plaintext []byte, err0 error) {

	// Compute shared secret
	sharedSecret, err := curve25519.X25519(recipientPrivateKey[:], ephemeralPublicKey[:])
	panicOn(err)

	// Derive symmetric key using HKDF with SHA-256
	hkdf := hkdf.New(sha256.New, sharedSecret[:], nil, nil)
	symmetricKey := make([]byte, chacha20poly1305.KeySize)
	_, err = io.ReadFull(hkdf, symmetricKey)
	panicOn(err)

	// Decrypt the ciphertext using ChaCha20-Poly1305
	aead, err := chacha20poly1305.New(symmetricKey)
	panicOn(err)
	if len(ciphertext) < aead.NonceSize() {
		err0 = fmt.Errorf("decryptWithPrivKey(): ciphertext too short")
		return
	}
	nonce, ct := ciphertext[:aead.NonceSize()], ciphertext[aead.NonceSize():]
	return aead.Open(scratch, nonce, ct, nil)
}

// handshakeRecord shows what was exchanged and verified
// between client and server.
type handshakeRecord struct {
	Cshake             *verifiedHandshake `zid:"0"`
	Sshake             *verifiedHandshake `zid:"1"`
	SharedRandomSecret []byte             `zid:"2"`
}

// encrypt more with the public/private keys.
func symmetricServerVerifiedHandshake2(
	conn uConn,
	psk [32]byte,
	creds *selfcert.Creds,
) (r *handshakeRecord, err0 error) {

	r = &handshakeRecord{}

	var cliEphemPub, srvEphemPub []byte
	_ = cliEphemPub
	var clientStaticPubKey ed25519.PublicKey

	//vv("server creds = '%#v'", creds)

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

	// Read the client's public key/handshake. Server *must* read first, since
	// QUIC streams are only established when the client writes.
	var cshake verifiedHandshake

	timeout := time.Second * 10
	cshakeTag, err := readLenThenShakeTag(conn, &cshake, &timeout)
	if err != nil {
		err0 = fmt.Errorf("at server could not decode from client the client handshake: '%v'", err)
		return
	}

	if !isValidX25519PublicKey(cshake.EphemPubKey) {
		err0 = fmt.Errorf("we read an invalid client cshake ephem public key: '%x'", cshake.EphemPubKey)
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
		//vv("server sees bad client cert")
		err0 = fmt.Errorf("server cannot verify client cert: %v", err)
		return
	}

	// Extract the client's static Ed25519 public key from the certificate
	var ok bool
	clientStaticPubKey, ok = clientCert.PublicKey.(ed25519.PublicKey)
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

	// since client checked out so far, send our empheral key to handshake.
	// Otherwise we don't even bother generating the network traffic.

	// server shake
	sshake := verifiedHandshake{
		EphemPubKey:      srvEphemPub,
		SignatureOfEphem: signature,
		SigningCert:      creds.NodeCert.Raw,
		SenderSentAt:     time.Now(),
	}
	//vv("len SigningCert = %v", len(sshake.SigningCert)) // 540

	sshakeTag, err := sendLenThenShakeTag(conn, &sshake, &timeout)
	if err != nil {
		err0 = fmt.Errorf("at server, could not encode/send our server side handshake: '%v'", err)
		return
	}

	// Compute the shared secret
	sharedSecret, err := curve25519.X25519(serverEphemPrivateKey[:], cshake.EphemPubKey)
	if err != nil {
		panic(err)
	}

	// Derive the final symmetric key using HKDF
	sharedRandomSecret := deriveSymmetricKeyFromBaseSymmetricAndSharedRandomSecret(sharedSecret, psk[:])
	r.SharedRandomSecret = sharedRandomSecret[:]

	// Print the symmetric key (for demonstration purposes)
	//fmt.Printf("(verified) Server derived symmetric key: %x\n", sharedRandomSecret[:])

	if useCaboose {
		// Generate new, 2nd, ephemeral X25519 key pair
		serverEphemPrivateKey2, serverEphemPublicKey2, err := generateX25519KeyPair()
		if err != nil {
			err0 = fmt.Errorf("failed to generate server X25519 key pair: %v", err)
			return
		}

		cab := &caboose{
			ClientAuthTag:     cshakeTag,
			ServerAuthTag:     sshakeTag,
			ClientEphemPubKey: cshake.EphemPubKey,
			ServerEphemPubKey: sshake.EphemPubKey,
			ClientSentAt:      cshake.SenderSentAt,
			ServerSentAt:      sshake.SenderSentAt,
			ClientSigningCert: cshake.SigningCert,
			ServerSigningCert: sshake.SigningCert,
			ClientSigOfEphem:  cshake.SignatureOfEphem,
			ServerSigOfEphem:  sshake.SignatureOfEphem,
			ServerNewPub:      serverEphemPublicKey2[:],
		}
		err0 = sendCrypticCaboose(conn, cab, sharedRandomSecret[:], &timeout)
		if err0 != nil {
			return
		}

		// Compute the new shared secret, based on the serverEphemPrivateKey2
		sharedSecret2, err := curve25519.X25519(serverEphemPrivateKey2[:], cshake.EphemPubKey)
		if err != nil {
			panic(err)
		}
		_ = sharedSecret2
	}

	return
}

func symmetricClientVerifiedHandshake2(
	conn uConn,
	psk [32]byte,
	creds *selfcert.Creds,
) (r *handshakeRecord, err0 error) {

	r = &handshakeRecord{}

	var cliEphemPub, srvEphemPub []byte
	_ = srvEphemPub
	var serverStaticPubKey ed25519.PublicKey

	//vv("top of symmetricClientVerifiedHandshake")

	//vv("client creds = '%#v'", creds)

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

	cshake := verifiedHandshake{
		EphemPubKey:      cliEphemPub,
		SignatureOfEphem: signature,
		SigningCert:      creds.NodeCert.Raw,
		SenderSentAt:     time.Now(),
	}

	// client sending first to open quic stream (if using quic).
	timeout := time.Second * 10
	var cshakeTag []byte
	cshakeTag, err0 = sendLenThenShakeTag(conn, &cshake, &timeout)
	if err0 != nil {
		return
	}

	var sshake verifiedHandshake
	var sshakeTag []byte
	sshakeTag, err = readLenThenShakeTag(conn, &sshake, &timeout)
	if err != nil {
		err0 = fmt.Errorf("at client, could not decode from server the server handshake: '%v'", err)
		return
	}

	if !isValidX25519PublicKey(sshake.EphemPubKey) {
		err0 = fmt.Errorf("we read an invalid server sshake public key: '%x'", sshake.EphemPubKey)
		return
	}

	// Parse the server's certificate
	serverStaticCert, err := x509.ParseCertificate(sshake.SigningCert)
	if err != nil {
		err0 = fmt.Errorf("failed to parse servers certificate: '%v'", err)
		return
	}

	// Verify the server's certificate against the CA
	opts := x509.VerifyOptions{
		Roots:         creds.CACertPool,
		CurrentTime:   time.Now(),
		Intermediates: x509.NewCertPool(),
	}
	if _, err = serverStaticCert.Verify(opts); err != nil {
		//vv("client sees bad server signing")
		err0 = fmt.Errorf("client could not verify servers static cert: '%v'", err)
		return
	}
	//vv("client sees ok server signing")

	// Extract the server's static Ed25519 public key from the certificate
	var ok bool
	serverStaticPubKey, ok = serverStaticCert.PublicKey.(ed25519.PublicKey)
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
	sharedRandomSecret := deriveSymmetricKeyFromBaseSymmetricAndSharedRandomSecret(sharedSecret, psk[:])
	r.SharedRandomSecret = sharedRandomSecret[:]

	if useCaboose {
		cab := &caboose{}
		err0 = readCrypticCaboose(conn, cab, sharedRandomSecret[:], &timeout)
		if err0 != nil {
			return
		}
		// the legit server put the fields as:
		expected := &caboose{
			ClientAuthTag:     cshakeTag,
			ServerAuthTag:     sshakeTag,
			ClientEphemPubKey: cshake.EphemPubKey,
			ServerEphemPubKey: sshake.EphemPubKey,
			ClientSentAt:      cshake.SenderSentAt,
			ServerSentAt:      sshake.SenderSentAt,
			ClientSigningCert: cshake.SigningCert,
			ServerSigningCert: sshake.SigningCert,
			ClientSigOfEphem:  cshake.SignatureOfEphem,
			ServerSigOfEphem:  sshake.SignatureOfEphem,
		}
		if !cab.Equal(expected) {
			err0 = fmt.Errorf("caboose mismatch: got '%#v'; \n\n versus expected '%#v'", cab, expected)
			//vv("err0 = '%v'", err0)
			return
		}
		//vv("caboose was as expected")
	}

	// Print the symmetric key (for demonstration purposes)
	//fmt.Printf("(verified) Client derived symmetric key: %x\n", sharedRandomSecret[:])
	return
}
