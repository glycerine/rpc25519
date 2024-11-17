package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"runtime"
	"strings"
	"time"
)

var (
	duration    = int64(0)
	count       = int64(0)
	connections = int64(0)
	//responseDelay = time.Millisecond // Wait before next send.
)

func service(conn net.Conn) {

	buff := make([]byte, 364)
	var i uint64
	var by [8]byte

	for i = 0; i < 1e8; i++ {

		by = toBytes(i)
		copy(buff[:8], by[:])

		// send sequential numbers to client, starting at 0.
		if _, err := conn.Write(buff); err != nil {
			panic(err)
		}
		runtime.Gosched()
	}
}

func listener(ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			panic(err)
		}
		go service(c)
	}
}

func toBytes(n uint64) (r [8]byte) {
	binary.BigEndian.PutUint64(r[:], n)
	return
}

func fromBytes(by []byte) uint64 {
	return binary.BigEndian.Uint64(by)
}

func client(id int) {
	conn, err := net.Dial("tcp", "127.0.0.1:5555")
	if err != nil {
		panic(err)
	}

	buff := make([]byte, 364)
	var i uint64
	timeout := time.Millisecond * 100

	for i = 0; i < 1e8; i++ {

		// Read with 100 msec timeouts, as if we also
		// need to periodically check for other events, e.g. shutdown, pause, etc.
		// On darwin, at go1.23.3, the read timeouts occassionally
		// causes us to lose TCP data. Estimated around every 1 in 500K
		// reads when under load. We could see the data packets being sent
		// to the client in Wireshark, but the client would never see them.
		for {
			err := readFull(conn, buff, &timeout)
			if err != nil {
				r := err.Error()
				if strings.Contains(r, "i/o timeout") || strings.Contains(r, "deadline exceeded") {
					continue // normal, expected, timeout
				}
				panic(err)
			}
			// INVAR: err == nil, so we can exit the repeated-read loop.
			break
		}
		j := fromBytes(buff[:8])
		if i != j {
			panic(fmt.Sprintf("next expected is i=%v, but we got j=%v", i, j))
		}
		if i%10000 == 0 {
			fmt.Printf("at i = %v\n", i)
		}
	}
}

var zeroTime = time.Time{}

// readFull reads exactly len(buf) bytes from conn
func readFull(conn net.Conn, buf []byte, timeout *time.Duration) error {

	if timeout != nil && *timeout > 0 {
		conn.SetReadDeadline(time.Now().Add(*timeout))
	} else {
		// do not let previous deadlines contaminate this one.
		conn.SetReadDeadline(zeroTime)
	}

	need := len(buf)
	total := 0
	for total < len(buf) {
		n, err := conn.Read(buf[total:])
		total += n
		if total == need {
			// probably just EOF
			if err != nil {
				panic(err)
			}
			return nil
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func startClients() {
	for i := 0; i < 50; i++ {
		go client(i)

		time.Sleep(time.Millisecond * 2)
	}
}

func main() {
	ln, err := net.Listen("tcp", "127.0.0.1:5555")
	if err != nil {
		panic(err)
	}
	go listener(ln)

	startClients()
	select {}
}
