package main

// layer2 client

import (
	cryrand "crypto/rand"
	//"fmt"
	"bytes"
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

	enc, dec := NewEncoderDecoderPair(key, conn)

	// Send messages
	messages := []string{"Hello, Server!", "How are you?", "Goodbye!"}
	for _, msg := range messages {
		_, err := enc.Write([]byte(msg))
		if err != nil {
			log.Fatalf("Write error: %v", err)
		}
		log.Printf("Sent: %s", msg)

		// Read response
		buffer := make([]byte, 4096)
		n, err := dec.Read(buffer)
		if err != nil {
			if err != io.EOF {
				log.Fatalf("Read error: %v", err)
			}
			break
		}
		response := string(buffer[:n])
		log.Printf("Received: %s", response)

		//time.Sleep(1 * time.Second)
	}

	// send a big message
	//	by, err := os.ReadFile("big")
	//	panicOn(err)

	by := make([]byte, 2*1024*1024*1024-44) // 2GB, our max message size. minus 44 bytes of overhead+nonce+msgLen 4 bytes
	t0 := time.Now()
	_, err = cryrand.Read(by)
	panicOn(err)
	// elap 4.998854362s to generate 2GB cryrand data => 400 MB/sec.
	vv("elap %v to generate 2GB cryrand data", time.Since(t0))

	t1 := time.Now()
	_, err = enc.Write(by)
	panicOn(err)
	vv("wrote big: %v message, took: %v", len(by), time.Since(t1))

	// Read response
	buffer := make([]byte, 2*1024*1024*1024)
	t2 := time.Now()
	n, err := dec.Read(buffer)
	if err == nil {
		vv("got response to big of len %v in %v", n, time.Since(t2))
	}
	if err != nil {
		if err != io.EOF {
			log.Fatalf("Read error: %v", err)
		}
	}
	// On localhost, over TCP.
	// on linux amd rhyzen total roundtrip time 7.099127628s => 281 MB/second to do
	// encryption on cli, decryption on srv, encryption on srv, decryption on cli.
	//
	// So-one way bandwidth of 562 MB/sec to encrypt and then decrypt.
	//
	// On mac intel i7 2.3GHz, total roundtrip time 48.117771412. So 83 MB/sec one-way enc+dec bandwidth.
	//
	vv("compared equal: %v, total roundtrip time %v (not including data gen)", bytes.Equal(by, buffer[:n]), time.Since(t1))
}
