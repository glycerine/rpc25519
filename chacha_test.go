package rpc25519

import (
	"bytes"
	cryrand "crypto/rand"
	"fmt"
	"testing"
	"time"

	cv "github.com/glycerine/goconvey/convey"
)

func Test020_nonce_sequence_not_reused(t *testing.T) {

	cv.Convey("the blabber encoder nonces should be different after each use, "+
		"so a nonce is never re-used, esp between client and server", t, func() {
		var key [32]byte

		bcli := newBlabber("test", key, nil, true, 1024, false, UseCompressionAlgo)
		bsrv := newBlabber("test", key, nil, true, 1024, true, UseCompressionAlgo)
		n := 1100

		const nonceSize = 16

		m := make(map[[nonceSize]byte]int)

		var last [nonceSize]byte
		if len(last) != bcli.enc.noncesize {
			panic(fmt.Sprintf("need to update this test, nonce size is no longer %v, but rather %v", nonceSize, bcli.enc.noncesize))
		}
		var i int
		add := func(by []byte, offset int) {
			copy(last[:], by)
			prev, dup := m[last]
			if dup {
				panic(fmt.Sprintf("saw duplicated nonce '%x' at i=%v, "+
					"must never happen. prev seen at %v", last[:], i, prev))
			}
			m[last] = i + offset
		}

		add(bcli.enc.writeNonce, 0)
		//vv("first = '%x'", last[:])
		add(bsrv.enc.writeNonce, 1e6)
		//vv("second = '%x'", last[:])
		for i = 0; i < n; i++ {
			bcli.enc.moveToNextNonce()
			bsrv.enc.moveToNextNonce()

			add(bcli.enc.writeNonce, 0)

			//vv("last[%v] = '%x'", i, last[:])
			add(bsrv.enc.writeNonce, 1e6)

			//vv("last[%v] = '%x'", i, last[:])
		}
		vv("checked up to %v-1 for collision in the first (cli+server) %v values.", n, len(m))
		cv.So(true, cv.ShouldBeTrue)
	})
}

type fakeConn struct {
	bytes.Buffer
}

func (f *fakeConn) SetWriteDeadline(t time.Time) error {
	return nil
}

func (f *fakeConn) SetReadDeadline(t time.Time) error {
	return nil
}

func Test021_caboose_encrypt_decrypt(t *testing.T) {

	cv.Convey("sendCrypticCaboose and readCrypticCaboose are inverses of each other", t, func() {

		symkey := make([]byte, 32)
		_, err := cryrand.Read(symkey)
		panicOn(err)

		cshakeTag := make([]byte, 32)
		_, err = cryrand.Read(cshakeTag)
		panicOn(err)
		sshakeTag := make([]byte, 32)
		_, err = cryrand.Read(sshakeTag)
		panicOn(err)

		clientSigningCert := make([]byte, 540)
		serverSigningCert := make([]byte, 540)
		_, err = cryrand.Read(clientSigningCert)
		panicOn(err)
		_, err = cryrand.Read(serverSigningCert)
		panicOn(err)

		// treat the private key like a public key, just to quickly
		// generate some random keys.
		key1, key2, err := generateX25519KeyPair()
		panicOn(err)
		now := time.Now()
		f := &fakeConn{}
		cab := &caboose{
			ClientAuthTag:     cshakeTag,
			ServerAuthTag:     sshakeTag,
			ClientEphemPubKey: key1[:],
			ServerEphemPubKey: key2[:],
			ClientSentAt:      now,
			ServerSentAt:      now.Add(time.Second),
			ClientSigningCert: clientSigningCert,
			ServerSigningCert: serverSigningCert,
		}
		panicOn(sendCrypticCaboose(f, cab, symkey, nil))

		//vv("f says len is '%x'", f.Bytes()[:8])
		//vv("f has '%x'", f.Bytes())

		var cab1 caboose
		panicOn(readCrypticCaboose(f, &cab1, symkey, nil))

		if !cab.Equal(&cab1) {
			panic("not inverses: cab1 != cab")
		}
	})
}

func Test022_encryptWithPubKey(t *testing.T) {

	cv.Convey("encryptWithPubKey and decryptWithPrivKey are inverses of each other", t, func() {

		plaintext := make([]byte, 100)
		_, err := cryrand.Read(plaintext)
		panicOn(err)

		plaintext = []byte("hello there")

		recipientPrivateKey, recipientPublicKey, err := generateX25519KeyPair()
		panicOn(err)

		// Sender encrypts the message
		ephemeralPublicKey, ciphertext, err := encryptWithPubKey(recipientPublicKey, plaintext)
		if err != nil {
			panic(err)
		}

		// Recipient decrypts the message
		decryptedMessage, err := decryptWithPrivKey(recipientPrivateKey, ephemeralPublicKey, ciphertext)
		if err != nil {
			panic(err)
		}

		vv("plaintext = '%s'", plaintext)
		vv("ciphertext = '%x'", ciphertext)
		vv("decryptedMessage = '%s'", decryptedMessage)

		cv.So(!bytes.Equal(ciphertext, plaintext), cv.ShouldBeTrue)
		cv.So(bytes.Equal(plaintext, decryptedMessage), cv.ShouldBeTrue)
	})
}
