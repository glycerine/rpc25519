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

	skipCrypt := true
	var err error
	enc, dec := NewEncoderDecoderPair(key, conn)
	//vv("top of handle connection")
	buf := make([]byte, 3*1024*1024*1024)
	var msg []byte
	for {
		n := 0
		if skipCrypt {

			msg, err = receiveMessage(conn, buf, nil)
			if err == io.EOF {
				continue
			}
			panicOn(err)
			n = len(msg)
		} else {
			n, err = dec.Read(buf)
			msg = buf[:n]
		}
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

		nw := 0
		if skipCrypt {
			err = sendMessage(conn, response, nil)
			panicOn(err)
			nw = len(response)
		} else {
			nw, err = enc.Write(response)
		}
		if nw == len(response) {
			vv("server: echo %v suceeded", nw)
			continue
		}
		vv("send / enc.Write got err = '%v', nw=%v out of %v", err, nw, len(response))
		if err != nil {
			log.Printf("Write error: %v", err)
			break
		}
	}
}
