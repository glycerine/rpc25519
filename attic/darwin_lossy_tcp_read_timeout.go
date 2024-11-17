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
	timeout := time.Millisecond

	t0 := time.Now()
	defer func() {
		fmt.Printf("elapsed since starting: %v\n", time.Since(t0))
	}()
	for i = 0; i < 1e8; i++ {

		// The repeated-read loop. Here we
		// read with timeouts; as if we also
		// need to periodically check for other events, e.g. shutdown, pause, etc.
		//
		// When we do so, we observe data loss under load.
		// Originally the library that revealed this issue used
		// 100 msec timeouts, and multiple 50 client processes.
		// Under this reproducer when we put everything in
		// one process for ease of repro, we cranked it down
		// to 1 msec to make it happen faster and more reliably.
		//
		// On darwin, at go1.23.3, the read timeouts occassionally
		// causes us to lose TCP data. Estimated around every 1 in 500K (maybe)
		// reads when under load. We could see the TCP data packets being sent
		// to the client in Wireshark, but the Go client would never get
		// delivery of the response to their RPC call.
		// MacOS Sonoma 14.0 on darwin/amd64.
		//
		// I was expecting to see a skipped number and not zero (to match
		// the original library's experience of not getting an RPC
		// reply), but in this compressed reproducer, observing
		// getting a zero instead of the next expected number
		// still qualifies as a bug. This may be as close as we can
		// easily get with this little code (the original library is much bigger).
		for {
			err := readFull(conn, buff, &timeout)
			if err != nil {
				//fmt.Printf("err = '%v'\n", err)
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
			panic(fmt.Sprintf("expected next number: %v, but we got %v", i, j))
		}
		if i%100000 == 0 {
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

/*
sample output 1
...
at i = 200000
elapsed since starting: 15.691497196s
panic: expected next number: 203963, but we got 0

goroutine 111 [running]:
main.client(0x0?)
	/Users/jaten/trash/darwin_lossy_tcp_read_timeout.go:98 +0x2fa
created by main.startClients in goroutine 1
	/Users/jaten/trash/darwin_lossy_tcp_read_timeout.go:139 +0x3d
exit status 2

Compilation exited abnormally with code 1 at Sat Nov 16 21:28:33


sample output 2
...
at i = 400000
elapsed since starting: 35.007915405s
panic: expected next number: 420721, but we got 0

goroutine 68 [running]:
main.client(0x0?)
	/Users/jaten/trash/darwin_lossy_tcp_read_timeout.go:98 +0x2fa
created by main.startClients in goroutine 1
	/Users/jaten/trash/darwin_lossy_tcp_read_timeout.go:139 +0x3d
exit status 2

Compilation exited abnormally with code 1 at Sat Nov 16 21:29:57

sample output 3

at i = 100000
elapsed since starting: 12.453088565s
panic: expected next number: 148173, but we got 0

goroutine 125 [running]:
main.client(0x0?)
	/Users/jaten/trash/darwin_lossy_tcp_read_timeout.go:105 +0x2fa
created by main.startClients in goroutine 1
	/Users/jaten/trash/darwin_lossy_tcp_read_timeout.go:146 +0x3d
exit status 2

Compilation exited abnormally with code 1 at Sat Nov 16 21:37:24

sample output 4

...
at i = 800000
elapsed since starting: 1m10.034763283s
panic: expected next number: 829960, but we got 0

goroutine 130 [running]:
main.client(0x0?)
	/Users/jaten/trash/darwin_lossy_tcp_read_timeout.go:112 +0x2fa
created by main.startClients in goroutine 1
	/Users/jaten/trash/darwin_lossy_tcp_read_timeout.go:153 +0x3d
exit status 2

Compilation exited abnormally with code 1 at Sat Nov 16 21:41:49

*/
