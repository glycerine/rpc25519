package main

import (
	"strconv"
	//cryrand "crypto/rand"
	"encoding/binary"
	"fmt"
	mathrand2 "math/rand/v2"
	"net"
	"runtime"
	"strings"
	"time"
)

func init() {
	var seed [32]byte
	chacha8rand = mathrand2.NewChaCha8(seed)
}

var chacha8rand *mathrand2.ChaCha8

// const buffSize = 364 // crasher size
const buffSize = 16

func service(conn net.Conn) {

	cliGoroNumBytes := make([]byte, 8)
	err := readFull(conn, cliGoroNumBytes, nil)
	if err != nil {
		panic(err)
	}
	cliGoroNumber := fromBytes(cliGoroNumBytes)

	buff := make([]byte, buffSize)

	// fill with random bytes to see if that changes
	// our wrong receive from 0 to something else. Yes! It does.
	// We are getting back an incorrectly offset buffer! See
	// the sample output below.
	chacha8rand.Read(buff)

	var i uint64
	var by [8]byte

	for i = 0; i < 1e8; i++ {

		by = toBytes(i)
		copy(buff[:8], by[:])

		if i == 0 {
			//j := fromBytes(buff[8:16])
			fmt.Printf("buff = '%x'  (goroutine %v)\n", buff, cliGoroNumber)
		}

		// verify it
		check := fromBytes(buff[:8])
		if check != i {
			panic("did not encode i into buff!")
		}

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

	myGoroNumAsBytes := toBytes(uint64(GoroNumber()))
	_, err = conn.Write(myGoroNumAsBytes[:])
	if err != nil {
		panic(err)
	}

	buff := make([]byte, buffSize)
	var i, j uint64
	timeout := time.Millisecond

	t0 := time.Now()
	defer func() {
		fmt.Printf("elapsed since starting: %v\n", time.Since(t0))
	}()
	crashNext := false
	var unexpected uint64
	_ = unexpected
	for i = 0; i < 1e8; i++ {

		// The repeated-read loop. Here we
		// read with timeouts; as if we also
		// need to periodically check for other events, e.g. shutdown, pause, etc.
		//
		// When we do so, we observe data loss.
		for {
			err := readFull(conn, buff, &timeout)
			if err != nil {
				//fmt.Printf("err = '%v'; current i=%v; prev j=%v\n", err, i, j)
				r := err.Error()
				if strings.Contains(r, "i/o timeout") || strings.Contains(r, "deadline exceeded") {
					continue // normal, expected, timeout
				}
				panic(err)
			}
			// INVAR: err == nil, so we can exit the repeated-read loop.
			break
		}
		j = fromBytes(buff[:8])

		if crashNext {
			//errConn := connCheck(conn)
			writable := canWrite(conn) // always seeing true.
			panic(fmt.Sprintf("crashNext true: prev unexpected was: %v; on the one after we see j = %v, while i (after increment at top of loop) is now = %v; canWrite = %v; buff='%x'; last 8 bytes decoded as uint64: '%v'", unexpected, j, i, writable, buff, fromBytes(buff[8:16])))
		}

		if i != j {
			//errConn := connCheck(conn)
			//panic(fmt.Sprintf("expected next number: %v, but we got %v;  errConn='%v'\n", i, j, errConn))
			fmt.Printf("expected next number: %v, but we got %v; buff = '%x' (buff[8:16] decoded as uint64: '%v')\n", i, j, buff, fromBytes(buff[8:16]))
			unexpected = j
			crashNext = true
			runtime.Gosched()
		}
		//		if i%100000 == 0 {
		//			fmt.Printf("at i = %v\n", i)
		//		}
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

func canWrite(c net.Conn) bool {
	_, err := c.Write([]byte("OK"))
	if err != nil {
		return false
	}
	return true
}

func panicOn(err error) {
	if err != nil {
		panic(err)
	}
}

// GoroNumber returns the calling goroutine's number.
func GoroNumber() int {
	buf := make([]byte, 48)
	nw := runtime.Stack(buf, false) // false => just us, no other goro.
	buf = buf[:nw]

	// prefix "goroutine " is len 10.
	i := 10
	for buf[i] != ' ' && i < 30 {
		i++
	}
	n, err := strconv.Atoi(string(buf[10:i]))
	panicOn(err)
	return n
}

/*
sample output 1 (on go version go1.23.3 darwin/amd64)

-*- mode: compilation; default-directory: "~/trash/tmp/" -*-
Compilation started at Sun Nov 17 22:34:34

go run darwin_lossy_tcp_read_timeout.go
buff = '00000000000000001a6f419ec627c76b'  (goroutine 20)
buff = '0000000000000000a46add6a48d89474'  (goroutine 22)
buff = '00000000000000004cf4929ef54f635d'  (goroutine 33)
buff = '0000000000000000dcb8a99468ef7de3'  (goroutine 65)
buff = '000000000000000043cea3e828a15c70'  (goroutine 66)
buff = '0000000000000000e3eb28cb670f0e97'  (goroutine 67)
buff = '0000000000000000d8d1001f787c1704'  (goroutine 68)
buff = '000000000000000068b5c68785829264'  (goroutine 81)
buff = '000000000000000055b970d3ecbf14bd'  (goroutine 82)
buff = '0000000000000000b7dafc64deb68410'  (goroutine 83)
buff = '0000000000000000d3e59bdef423e884'  (goroutine 84)
buff = '0000000000000000314a42a39f8abf25'  (goroutine 85)
buff = '0000000000000000314a42a39f8abf25'  (goroutine 87)
buff = '0000000000000000da86b0011deb21bb'  (goroutine 88)
buff = '0000000000000000387bfdb84b56e5d8'  (goroutine 89)
buff = '00000000000000000e0cfe42821e68aa'  (goroutine 86)
buff = '0000000000000000837fefe5279cfc9e'  (goroutine 90)
buff = '00000000000000001e4cb12690cafb7a'  (goroutine 93)
buff = '0000000000000000a94911c891eeb225'  (goroutine 91)
buff = '0000000000000000ecc97fc250c8ac13'  (goroutine 92)
buff = '00000000000000003e01c3d906865c6f'  (goroutine 94)
buff = '0000000000000000aacf9b68a5267c6c'  (goroutine 96)
buff = '00000000000000006eccf1bba9c5284e'  (goroutine 129)
buff = '00000000000000005ff4c8fd651e175a'  (goroutine 130)
buff = '000000000000000021473e89362f469b'  (goroutine 95)
buff = '0000000000000000d50f51f96e722347'  (goroutine 131)
buff = '0000000000000000a1ced239de959f76'  (goroutine 132)
buff = '00000000000000000c72ee7e53331554'  (goroutine 133)
buff = '000000000000000033f157282afa440d'  (goroutine 134)
buff = '0000000000000000e61d1fb237cde886'  (goroutine 135)
buff = '0000000000000000c8c839492da3b333'  (goroutine 136)
buff = '00000000000000005e4890521e53b60d'  (goroutine 137)
buff = '0000000000000000628d82cf7ecd4691'  (goroutine 138)
buff = '0000000000000000e8b3595252b39038'  (goroutine 139)
buff = '0000000000000000177ed7038108641d'  (goroutine 140)
buff = '0000000000000000e685815248a7a297'  (goroutine 144)
buff = '000000000000000059f50c2bf3db897f'  (goroutine 142)
buff = '00000000000000008e6ace6ba25365a2'  (goroutine 143)
buff = '0000000000000000e29f79c7ead1f149'  (goroutine 141)
buff = '000000000000000008f3fe1ad87a97e8'  (goroutine 145)
buff = '0000000000000000fb144320b847203f'  (goroutine 146)
buff = '0000000000000000dd2ec5603046950c'  (goroutine 149)
buff = '000000000000000078a7fccc2a97681f'  (goroutine 147)
buff = '00000000000000002ac9312fdf823181'  (goroutine 151) <<<< NOTICE! goro 151 is panic-ing
buff = '00000000000000002fa4883dfaeedcef'  (goroutine 153)
buff = '000000000000000044374e8b34b4eb1b'  (goroutine 152)
buff = '0000000000000000643cff2836d178e8'  (goroutine 148)
buff = '00000000000000007d294126d3a9fccc'  (goroutine 150)
expected next number: 2438, but we got 3083049501594890625; buff = '2ac9312fdf8231810000000000000987' (buff[8:16] decoded as uint64: '2439')
elapsed since starting: 499.613109ms
panic: crashNext true: prev unexpected was: 3083049501594890625; on the one after we see j = 3083049501594890625, while i (after increment at top of loop) is now = 2439; errConn ='<nil>'; canWrite = true; buff='2ac9312fdf8231810000000000000988'; last 8 bytes decoded as uint64: '2440'

goroutine 151 [running]:
main.client(0x0?)
	/Users/jaten/trash/tmp/darwin_lossy_tcp_read_timeout.go:164 +0x599
created by main.startClients in goroutine 1
	/Users/jaten/trash/tmp/darwin_lossy_tcp_read_timeout.go:214 +0x3d
exit status 2

Compilation exited abnormally with code 1 at Sun Nov 17 22:34:35

My commentary:

When we shrink the message size down from 264 to just 16 bytes,
then we can get a glimpse into what is happening when we get a
wrong read on the client end. The above shows us a one word
(8 byte) shift:

goroutine 151 observed after read into its buff this set of bytes,
shown as two words:
'2ac9312fdf823181 0000000000000987'

but we expected to get these two words:
'0000000000000986 2ac9312fdf823181'

so the faulty read has shifted the read from the underlying
TCP buffer incorrrectly forward by 8 bytes (one word on amd64).

I have also seen more than an 8 byte shift in a
run. See sample output 3 below for a 12 byte shift example.

sample output 2: This show the same issue as above. We happen
to get to see 4 client reads being incorrect before the
panic took down the process. (also go version go1.23.3 darwin/amd64)

-*- mode: compilation; default-directory: "~/trash/tmp/" -*-
Compilation started at Sun Nov 17 22:45:20

go run darwin_lossy_tcp_read_timeout.go
buff = '00000000000000001a6f419ec627c76b'  (goroutine 8)
buff = '0000000000000000a46add6a48d89474'  (goroutine 17)
buff = '00000000000000004cf4929ef54f635d'  (goroutine 10)
buff = '0000000000000000dcb8a99468ef7de3'  (goroutine 49)
buff = '000000000000000043cea3e828a15c70'  (goroutine 65)
buff = '0000000000000000e3eb28cb670f0e97'  (goroutine 66)
buff = '0000000000000000d8d1001f787c1704'  (goroutine 68)
buff = '000000000000000068b5c68785829264'  (goroutine 69)
buff = '000000000000000055b970d3ecbf14bd'  (goroutine 71)
buff = '0000000000000000b7dafc64deb68410'  (goroutine 73)
buff = '0000000000000000d3e59bdef423e884'  (goroutine 72)
buff = '0000000000000000aadc8f829e0ce35c'  (goroutine 75)
buff = '0000000000000000a775a4810dd20a80'  (goroutine 76)
buff = '0000000000000000df31b5282bbc3d9a'  (goroutine 77)
buff = '000000000000000070aa5fa26ca01c38'  (goroutine 78)
buff = '0000000000000000ba340af96279bf01'  (goroutine 79)
buff = '00000000000000000e427b01e9fa49a3'  (goroutine 80)
buff = '0000000000000000a0fe81b34c7f8050'  (goroutine 81)
buff = '000000000000000014bdb20f9675ae25'  (goroutine 82)
buff = '000000000000000000d0e937a152a4fd'  (goroutine 83)
buff = '000000000000000037491e5f0ae4ba1c'  (goroutine 85)
buff = '00000000000000005fc087cb70df5b51'  (goroutine 86)
buff = '0000000000000000429173d13425349a'  (goroutine 87)
buff = '0000000000000000eeb74f5a77e95d3b'  (goroutine 88)
buff = '000000000000000073acc6c3565825a0'  (goroutine 84)
buff = '0000000000000000d72b59b8227d4506'  (goroutine 90)
buff = '0000000000000000dcf76aa0aa1a4dc4'  (goroutine 94)
buff = '0000000000000000fb679aac0dc09769'  (goroutine 95)
buff = '0000000000000000938a75b111a52d46'  (goroutine 96)
buff = '0000000000000000bf5ab58c8e61eb90'  (goroutine 91)
buff = '0000000000000000bb25d77bb39f40c5'  (goroutine 92)
buff = '00000000000000005382898c849e5abe'  (goroutine 113)
buff = '00000000000000006d06240c1e08b578'  (goroutine 93)
buff = '00000000000000003e83149a5f2feb67'  (goroutine 89)
buff = '0000000000000000499af66bed112e78'  (goroutine 114)
buff = '000000000000000009e2007b87270bb1'  (goroutine 115)
buff = '000000000000000060eeddc708e2d75f'  (goroutine 116)
buff = '00000000000000005332d537b95eef80'  (goroutine 127)
buff = '0000000000000000c2cd89d8a4799eef'  (goroutine 121)
buff = '00000000000000000929d71336fdf5b0'  (goroutine 128)
buff = '000000000000000064ec0f9968bbaa21'  (goroutine 126)
buff = '0000000000000000f70e90b8f3003857'  (goroutine 120)
buff = '0000000000000000bdc400b69b983b9e'  (goroutine 117)
buff = '000000000000000015f7619aaa157a1f'  (goroutine 118)
buff = '000000000000000012f238a56d597369'  (goroutine 119)
buff = '00000000000000009bc20ccbc82cea86'  (goroutine 145)
buff = '00000000000000005c17c3998763199d'  (goroutine 148)
buff = '0000000000000000fe568512a242b356'  (goroutine 146)
buff = '00000000000000003976e4ef048cbe3d'  (goroutine 147)
buff = '00000000000000004bf44cc939358b5e'  (goroutine 149)
expected next number: 2700, but we got 6984763919015532383; buff = '60eeddc708e2d75f0000000000000a8d' (buff[8:16] decoded as uint64: '2701')
expected next number: 2430, but we got 13674055152898161566; buff = 'bdc400b69b983b9e000000000000097f' (buff[8:16] decoded as uint64: '2431')
expected next number: 4974, but we got 12311873272484258652; buff = 'aadc8f829e0ce35c000000000000136f' (buff[8:16] decoded as uint64: '4975')
expected next number: 4429, but we got 8335255552625485216; buff = '73acc6c3565825a0000000000000114e' (buff[8:16] decoded as uint64: '4430')
elapsed since starting: 464.649704ms
panic: crashNext true: prev unexpected was: 6984763919015532383; on the one after we see j = 6984763919015532383, while i (after increment at top of loop) is now = 2701; errConn ='<nil>'; canWrite = true; buff='60eeddc708e2d75f0000000000000a8e'; last 8 bytes decoded as uint64: '2702'

goroutine 116 [running]:
main.client(0xc000204028?)
	/Users/jaten/trash/tmp/darwin_lossy_tcp_read_timeout.go:139 +0x599
created by main.startClients in goroutine 1
	/Users/jaten/trash/tmp/darwin_lossy_tcp_read_timeout.go:189 +0x3d
exit status 2

Compilation exited abnormally with code 1 at Sun Nov 17 22:45:21

------
sample output 3: shows an off-by 12 bytes, rather than an off-by 8 bytes:

-*- mode: compilation; default-directory: "~/go/src/github.com/glycerine/rpc25519/attic/" -*-
Compilation started at Sun Nov 17 22:49:49

go run darwin_word_shift.go
buff = '00000000000000001a6f419ec627c76b'  (goroutine 20)
buff = '0000000000000000a46add6a48d89474'  (goroutine 33)
buff = '00000000000000004cf4929ef54f635d'  (goroutine 49)
buff = '0000000000000000dcb8a99468ef7de3'  (goroutine 50)
buff = '000000000000000043cea3e828a15c70'  (goroutine 51)
buff = '0000000000000000e3eb28cb670f0e97'  (goroutine 53)
buff = '0000000000000000d8d1001f787c1704'  (goroutine 52)
buff = '000000000000000068b5c68785829264'  (goroutine 56)
buff = '000000000000000055b970d3ecbf14bd'  (goroutine 57)
buff = '0000000000000000d3e59bdef423e884'  (goroutine 58)
buff = '0000000000000000b7dafc64deb68410'  (goroutine 59)
buff = '0000000000000000aadc8f829e0ce35c'  (goroutine 60)
buff = '0000000000000000a775a4810dd20a80'  (goroutine 54)
buff = '0000000000000000df31b5282bbc3d9a'  (goroutine 55)
buff = '000000000000000070aa5fa26ca01c38'  (goroutine 61) <<<<<< 6ca01c38 occurs here.
buff = '0000000000000000ba340af96279bf01'  (goroutine 63)
buff = '00000000000000000e427b01e9fa49a3'  (goroutine 99)
buff = '0000000000000000a0fe81b34c7f8050'  (goroutine 64)
buff = '000000000000000014bdb20f9675ae25'  (goroutine 98)
buff = '000000000000000000d0e937a152a4fd'  (goroutine 97)
buff = '000000000000000037491e5f0ae4ba1c'  (goroutine 62)
buff = '00000000000000005fc087cb70df5b51'  (goroutine 106)
buff = '0000000000000000429173d13425349a'  (goroutine 100)
buff = '0000000000000000eeb74f5a77e95d3b'  (goroutine 101)
buff = '000000000000000073acc6c3565825a0'  (goroutine 102)
buff = '0000000000000000d72b59b8227d4506'  (goroutine 103)
buff = '0000000000000000bf5ab58c8e61eb90'  (goroutine 104)
buff = '0000000000000000dcf76aa0aa1a4dc4'  (goroutine 105)
buff = '0000000000000000fb679aac0dc09769'  (goroutine 107)
buff = '0000000000000000938a75b111a52d46'  (goroutine 108)
buff = '0000000000000000bb25d77bb39f40c5'  (goroutine 109)
buff = '00000000000000005382898c849e5abe'  (goroutine 110)
buff = '00000000000000003e83149a5f2feb67'  (goroutine 111)
buff = '00000000000000006d06240c1e08b578'  (goroutine 112)
buff = '0000000000000000499af66bed112e78'  (goroutine 129)
buff = '000000000000000009e2007b87270bb1'  (goroutine 130)
buff = '000000000000000060eeddc708e2d75f'  (goroutine 131)
buff = '00000000000000005332d537b95eef80'  (goroutine 132)
buff = '000000000000000012f238a56d597369'  (goroutine 133)
buff = '0000000000000000c2cd89d8a4799eef'  (goroutine 137)
buff = '00000000000000000929d71336fdf5b0'  (goroutine 136)
buff = '000000000000000064ec0f9968bbaa21'  (goroutine 134)
buff = '0000000000000000f70e90b8f3003857'  (goroutine 135)
buff = '0000000000000000bdc400b69b983b9e'  (goroutine 148)
buff = '000000000000000015f7619aaa157a1f'  (goroutine 138)
buff = '00000000000000009bc20ccbc82cea86'  (goroutine 139)
buff = '00000000000000005c17c3998763199d'  (goroutine 149)
buff = '0000000000000000fe568512a242b356'  (goroutine 151)
buff = '00000000000000003976e4ef048cbe3d'  (goroutine 150)
buff = '00000000000000004bf44cc939358b5e'  (goroutine 152)
expected next number: 3345, but we got 7827287179213668352; buff = '6ca01c380000000000000d1270aa5fa2' (buff[8:16] decoded as uint64: '14372850786210')
elapsed since starting: 503.292711ms
panic: crashNext true: prev unexpected was: 7827287179213668352; on the one after we see j = 7827287179213668352, while i (after increment at top of loop) is now = 3346; errConn ='<nil>'; canWrite = true; buff='6ca01c380000000000000d1370aa5fa2'; last 8 bytes decoded as uint64: '14377145753506'

goroutine 61 [running]:
main.client(0xc000094038?)
	/Users/jaten/go/src/github.com/glycerine/rpc25519/attic/darwin_word_shift.go:139 +0x599
created by main.startClients in goroutine 1
	/Users/jaten/go/src/github.com/glycerine/rpc25519/attic/darwin_word_shift.go:189 +0x3d
exit status 2

Compilation exited abnormally with code 1 at Sun Nov 17 22:49:50

*/

/* on windows, go version go1.23.2 windows/amd64

jaten@DESKTOP-689SS63 ~/go/src/github.com/glycerine/rpc25519/attic (master) $ go run darwin_word_shift.go 
buff = '00000000000000001a6f419ec627c76b'  (goroutine 21)
buff = '0000000000000000a46add6a48d89474'  (goroutine 22)
buff = '00000000000000004cf4929ef54f635d'  (goroutine 36)
buff = '0000000000000000dcb8a99468ef7de3'  (goroutine 50)
buff = '000000000000000043cea3e828a15c70'  (goroutine 38)
buff = '0000000000000000e3eb28cb670f0e97'  (goroutine 83)
buff = '0000000000000000d8d1001f787c1704'  (goroutine 51)
buff = '000000000000000068b5c68785829264'  (goroutine 85)
buff = '000000000000000055b970d3ecbf14bd'  (goroutine 84)
buff = '0000000000000000b7dafc64deb68410'  (goroutine 23)
buff = '0000000000000000d3e59bdef423e884'  (goroutine 39)
buff = '0000000000000000aadc8f829e0ce35c'  (goroutine 114)
buff = '0000000000000000a775a4810dd20a80'  (goroutine 25)
buff = '0000000000000000df31b5282bbc3d9a'  (goroutine 26)
buff = '000000000000000070aa5fa26ca01c38'  (goroutine 163)
buff = '0000000000000000ba340af96279bf01'  (goroutine 27)
buff = '00000000000000000e427b01e9fa49a3'  (goroutine 162)
buff = '0000000000000000a0fe81b34c7f8050'  (goroutine 30)
buff = '000000000000000014bdb20f9675ae25'  (goroutine 29)
buff = '000000000000000000d0e937a152a4fd'  (goroutine 146)
buff = '000000000000000037491e5f0ae4ba1c'  (goroutine 28)
buff = '00000000000000005fc087cb70df5b51'  (goroutine 31)
buff = '0000000000000000429173d13425349a'  (goroutine 33)
buff = '0000000000000000eeb74f5a77e95d3b'  (goroutine 182)
buff = '000000000000000073acc6c3565825a0'  (goroutine 183)
buff = '0000000000000000d72b59b8227d4506'  (goroutine 179)
buff = '0000000000000000bf5ab58c8e61eb90'  (goroutine 178)
buff = '0000000000000000dcf76aa0aa1a4dc4'  (goroutine 181)
buff = '0000000000000000fb679aac0dc09769'  (goroutine 185)
buff = '0000000000000000938a75b111a52d46'  (goroutine 180)
buff = '0000000000000000bb25d77bb39f40c5'  (goroutine 188)
buff = '00000000000000005382898c849e5abe'  (goroutine 184)
buff = '00000000000000003e83149a5f2feb67'  (goroutine 186)
buff = '00000000000000006d06240c1e08b578'  (goroutine 194)
buff = '0000000000000000499af66bed112e78'  (goroutine 187)
buff = '000000000000000009e2007b87270bb1'  (goroutine 195) <<<<<<<<<<<
buff = '000000000000000060eeddc708e2d75f'  (goroutine 189)
buff = '00000000000000005332d537b95eef80'  (goroutine 196)
buff = '000000000000000012f238a56d597369'  (goroutine 191)
buff = '0000000000000000c2cd89d8a4799eef'  (goroutine 193)
buff = '00000000000000000929d71336fdf5b0'  (goroutine 190)
buff = '000000000000000064ec0f9968bbaa21'  (goroutine 192)
buff = '0000000000000000f70e90b8f3003857'  (goroutine 199)
buff = '0000000000000000bdc400b69b983b9e'  (goroutine 201)
buff = '000000000000000015f7619aaa157a1f'  (goroutine 198)
buff = '00000000000000009bc20ccbc82cea86'  (goroutine 197)
buff = '00000000000000005c17c3998763199d'  (goroutine 200)
buff = '0000000000000000fe568512a242b356'  (goroutine 202)
buff = '00000000000000003976e4ef048cbe3d'  (goroutine 204)
buff = '00000000000000004bf44cc939358b5e'  (goroutine 203)
expected next number: 16309, but we got 13045206287355684619; buff = 'b509e2007b87270bb10000000000003f' (buff[8:16] decoded as uint64: '12754194144713244735')
elapsed since starting: 3.3483095s
panic: crashNext true: prev unexpected was: 13045206287355684619; on the one after we see j = 13117263881393612555, while i (after increment at top of loop) is now = 16310; canWrite = true; buff='b609e2007b87270bb10000000000003f'; last 8 bytes decoded as uint64: '12754194144713244735'

goroutine 195 [running]:
main.client(0x0?)
	C:/Users/jaten/go/src/github.com/glycerine/rpc25519/attic/darwin_word_shift.go:136 +0x534
created by main.startClients in goroutine 1
	C:/Users/jaten/go/src/github.com/glycerine/rpc25519/attic/darwin_word_shift.go:186 +0x3d
exit status 2
jaten@DESKTOP-689SS63 ~/go/src/github.com/glycerine/rpc25519/attic (master) $

*/
