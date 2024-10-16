package main

// layer2 client

import (
	//cryrand "crypto/rand"
	//"fmt"
	"io"
	"log"
	"net"
	"os"
	// "time"
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
}
