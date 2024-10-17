package main

// layer2 server

import (
	//cryrand "crypto/rand"
	//"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
)

func main() {
	path := "psk.hex"
	var key []byte
	if !fileExists(path) {
		// Define a shared secret key (32 bytes for AES-256-GCM)
		key := NewXChaCha20CryptoRandKey()
		fd, err := os.Create(path)
		panicOn(err)
		n, err := fd.Write(key)
		panicOn(err)
		if n != len(key) {
			panic("short write")
		}
		fd.Close()
	} else {
		var err error
		key, err = os.ReadFile(path)
		panicOn(err)
	}

	vv("server key = '%x'", key)
	if len(key) != 32 {
		panic("could not load key")
	}

	// Start the server
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	defer ln.Close()
	log.Println("Server listening on :8080")

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}
		log.Printf("Accepted connection from %v", conn.RemoteAddr())

		go handleConnection(conn, key)
	}
}

func handleConnection(conn net.Conn, key []byte) {
	defer conn.Close()

	encrypt := false
	blab := newBlabber(key, conn, encrypt, maxMessage)

	var err error

	//vv("top of handle connection")
	var msg []byte
	for {
		n := 0
		msg, err = blab.readMessage(conn, nil)
		if err == io.EOF {
			continue
		}
		panicOn(err)
		n = len(msg)

		//vv("dec.Read: n=%v, err='%v'; msg='%v'", n, err, string(buffer[:n]))
		if err != nil {
			if err != io.EOF {
				log.Printf("Read error: %v", err)
			}
			break
		}

		if n < 100 {
			vv("Received: %s", string(msg))
		} else {
			vv("Received msg of len %v", n)
		}

		//vv("about to echo")
		// Echo back the message
		var response []byte
		if n < 100 {
			response = []byte(fmt.Sprintf("Echo: %s", string(msg)))
		} else {
			response = msg
		}

		err = blab.sendMessage(conn, response, nil)
		panicOn(err)

		vv("server: echo %v suceeded", len(response))
	}
}
