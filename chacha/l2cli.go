package main

// layer2 client

import (
	cryrand "crypto/rand"
	//"fmt"
	"bytes"
	//"encoding/binary"
	"io"
	"log"
	"net"
	"os"
	"time"
)

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

	by := make([]byte, maxMessage-44) // 2GB - 44, our max message size minus 44 bytes of overhead+nonce+msgLen 4 bytes
	t0 := time.Now()
	_, err = cryrand.Read(by)
	panicOn(err)
	// elap 4.998854362s to generate 2GB cryrand data => 400 MB/sec.
	vv("elap %v to generate 2GB cryrand data: %v", time.Since(t0), len(by))

	t1 := time.Now()

	err = blab.sendMessage(conn, by, nil)
	panicOn(err)
	vv("wrote big: %v message, took: %v", len(by), time.Since(t1))

	// Read response
	var msg []byte

	t2 := time.Now()
	n := 0
	msg, err = blab.readMessage(conn, nil)
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
