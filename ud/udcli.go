// Client
package main

import (
	"fmt"
	"log"
	"net"
)

func main() {
	conn, err := net.Dial("unix", "/tmp/example.sock")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	msg := "hello unix domain socket!"
	fmt.Fprintf(conn, "%v", msg)
	fmt.Printf("cli sent msg = '%v'\n", msg)
}
