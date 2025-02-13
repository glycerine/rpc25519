// Server
package main

import (
	"fmt"
	"log"
	"net"
	"os"
)

func main() {
	socketPath := "/tmp/example.sock"

	// Cleanup existing socket file
	os.Remove(socketPath)

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			//log.Fatal(err)
			os.Exit(1)
		}
		go handleConnection(conn, listener)
	}
}

func handleConnection(conn net.Conn, lsn net.Listener) {
	defer conn.Close()
	// Handle connection like any other net.Conn

	buf := make([]byte, 100)
	n, _ := conn.Read(buf)
	if n > 0 {
		fmt.Printf("udsrv sees '%v'\n", string(buf[:n]))
	}
	lsn.Close()
}
