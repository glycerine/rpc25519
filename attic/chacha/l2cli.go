package main

// layer2 client

import (
	"crypto/aes"
	"crypto/cipher"
	cryrand "crypto/rand"
	mathrandv2 "math/rand/v2"
	//"fmt"
	"bytes"
	//"encoding/binary"
	"io"
	"log"
	"net"
	"os"
	"time"
)

var _ = mathrandv2.NewChaCha8

func main() {
	path := "psk.hex"
	key, err := os.ReadFile(path)
	panicOn(err)

	vv("client key = '%x'", key)

	// Connect to the server
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()
	log.Println("Connected to server")

	encrypt := true
	blab := newBlabber(key, conn, encrypt, maxMessage)

	// send a big message
	//	by, err := os.ReadFile("big")
	//	panicOn(err)

	by := make([]byte, maxMessage-64) // 2GB - 44, our max message size minus 44 bytes of overhead+nonce+msgLen 4 bytes
	t0 := time.Now()

	trueRandom := false
	if trueRandom {
		_, err = cryrand.Read(by)
		panicOn(err)
		vv("elap %v to generate 2GB cryrand data: %v", time.Since(t0), len(by))

	} else {
		const chacha8 = false
		if chacha8 {

			var seed [32]byte
			_, err = cryrand.Read(seed[:])
			panicOn(err)

			cc := mathrandv2.NewChaCha8(seed)
			_, err = cc.Read(by)
			panicOn(err)
			vv("elap %v to generate 2GB ChaCha8 data: %v", time.Since(t0), len(by))

		} else {
			keysz := 32

			key := make([]byte, keysz)
			_, err = io.ReadFull(cryrand.Reader, key)
			panicOn(err)

			nonce := make([]byte, 12)
			_, err = io.ReadFull(cryrand.Reader, nonce)
			panicOn(err)

			block, err := aes.NewCipher(key)
			panicOn(err)

			aesgcm, err := cipher.NewGCM(block)
			panicOn(err)

			ciphertext := aesgcm.Seal(by[:0], nonce, by, nil)
			by = ciphertext
			vv("elap %v to generate AES-GCM-%v: %v", time.Since(t0), keysz*8, len(by))
		}
	}
	// elap 4.998854362s to generate 2GB cryrand data => 400 MB/sec.

	t1 := time.Now()

	err = blab.sendMessage(by, nil)
	panicOn(err)
	vv("wrote big: %v message, took: %v", len(by), time.Since(t1))

	// Read response
	var msg []byte

	t2 := time.Now()
	n := 0
	msg, err = blab.readMessage(nil)
	panicOn(err)
	n = len(msg)
	if err == nil {
		vv("got response to big of len %v in %v", n, time.Since(t2))
	}
	if err != nil {
		if err != io.EOF {
			log.Fatalf("Read error: %v", err)
		}
	}
	// On localhost, over TCP.
	// on linux amd rhyzen total roundtrip time 5.860731946s => 349 MB/second to do
	// encryption on cli, decryption on srv, encryption on srv, decryption on cli.
	//
	// So one-way bandwidth of 698 MB/sec to encrypt and decrypt and transfer through kernel loopback.
	//
	// On mac intel i7 2.3GHz, total roundtrip time 14.823133954s; So 138*2 = 276 MB/sec one-way enc+dec bandwidth.
	//   but cryrand generation is 3x faster, huh!
	//
	// compared to linux unencrypted: 1.400682196s roundtrip. So 4.2x slower when encrypted.
	// compared to intel i7 mac unencrypted: 2s roundtrip, so 7x slower when encrypted.
	vv("compared equal: %v, total roundtrip time %v (not including data gen)", bytes.Equal(by, msg), time.Since(t1))
}
