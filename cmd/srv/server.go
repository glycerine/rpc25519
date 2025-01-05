package main

import (
	//"bytes"
	"flag"
	"fmt"
	//"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	//"strings"
	"sync/atomic"
	"syscall"
	"time"

	_ "net/http/pprof" // for web based profiling while running

	"github.com/glycerine/rpc25519"
)

var quiet *bool

var calls int64

func noticeControlC() {
	t0 := time.Now()
	sigChan := make(chan os.Signal, 1)
	go func() {
		for _ = range sigChan {
			n := atomic.LoadInt64(&calls)
			elap := time.Since(t0)
			fmt.Printf("\n\nserver %v for calls seen: %v  => %v calls/second.\n", elap, n, float64(n)/float64(int64(elap)/1e9))
			os.Exit(0)
		}
	}()
	signal.Notify(sigChan, syscall.SIGINT)
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile) // Add Lshortfile for short file names

	noticeControlC()

	var addr = flag.String("s", "0.0.0.0:8443", "server address to bind and listen on")
	var tcp = flag.Bool("tcp", false, "use TCP instead of the default TLS")
	var skipVerify = flag.Bool("skip-verify", false, "do not require client certs be from our CA, nor remember client certs in a known_client_keys file for later lockdown")

	var useName = flag.String("k", "node", "specifies name of keypairs to use (certs/name.crt and certs/name.key); instead of the default certs/node.crt and certs/node.key for the server.")
	var certPath = flag.String("certs", "", "use this path for certs; instead of the local ./certs/ directory.")

	var quic = flag.Bool("q", false, "use QUIC instead of TCP/TLS")

	var profile = flag.String("prof", "", "host:port to start web profiler on. host can be empty for all localhost interfaces")

	var psk = flag.String("psk", "", "path to pre-shared key file")

	var seconds = flag.Int("sec", 0, "run for this many seconds")

	var max = flag.Int("max", 0, "set runtime.GOMAXPROCS to this value.")

	quiet = flag.Bool("quiet", false, "for profiling, do not log answer")

	var readto = flag.Duration("read", 0, "timeout on reads")

	var readfile = flag.Bool("readfile", false, "listen for files to write to disk; client should run -sendfile")

	flag.Parse()

	if *max > 0 {
		runtime.GOMAXPROCS(*max)
	}

	if *profile != "" {
		fmt.Printf("webprofile starting at '%v'...\n", *profile)
		go func() {
			http.ListenAndServe(*profile, nil)
		}()
	}

	cfg := rpc25519.NewConfig()
	cfg.ServerAddr = *addr // "0.0.0.0:8443"
	cfg.TCPonly_no_TLS = *tcp
	cfg.UseQUIC = *quic
	cfg.SkipVerifyKeys = *skipVerify
	cfg.ServerKeyPairName = *useName
	cfg.CertPath = *certPath
	//cfg.ServerSendKeepAlive = time.Second * 5
	cfg.PreSharedKeyPath = *psk
	cfg.ReadTimeout = *readto

	srv := rpc25519.NewServer("srv", cfg)
	defer srv.Close()

	if *readfile {
		streamer := rpc25519.NewServerSideUploadState()
		// use the example.go example.
		srv.RegisterUploadReaderFunc(streamer.ReceiveFileInParts)
	} else {
		srv.Register2Func(customEcho)
	}

	serverAddr, err := srv.Start()
	if err != nil {
		panic(fmt.Sprintf("could not start rpc25519.Server with config = '%#v'; err='%v'", cfg, err))
	}

	log.Printf("rpc25519.server Start() returned serverAddr = '%v'", serverAddr)
	if *seconds > 0 {
		t0 := time.Now()
		<-time.After(time.Second * time.Duration(*seconds))
		//fmt.Printf("\nAfter %v seconds, server calls seen: %v\n", *seconds, atomic.LoadInt64(&calls))

		n := atomic.LoadInt64(&calls)
		elap := time.Since(t0)
		fmt.Printf("\n\nserver %v for calls seen: %v  => %v calls/second.\n", elap, n, float64(n)/float64(int64(elap)/1e9))

	} else {
		select {}
	}
}

// echo implements rpc25519.TwoWayFunc
func customEcho(req, reply *rpc25519.Message) error {
	if !*quiet {
		log.Printf("server customEcho called, Seqno=%v, msg='%v'", req.HDR.Seqno, string(req.JobSerz))
	}
	atomic.AddInt64(&calls, 1)
	//vv("callback to echo: with msg='%#v'", in)
	reply.JobSerz = append(req.JobSerz, []byte(fmt.Sprintf("\n with time customEcho sees this: '%v'", time.Now()))...)
	return nil
}

/* old
func receiveFiles(req, reply *rpc25519.Message) error {
	t0 := time.Now()
	log.Printf("server receiveFile called, Subject='%v'; To='%v'", req.HDR.Subject, req.HDR.To)
	if !strings.HasPrefix(req.HDR.Subject, "receiveFile:") {
		return nil
	}
	prefix := "receiveFile:"
	fname := req.HDR.Subject[len(prefix):]
	if fname == "" {
		return nil
	}
	w, err := os.Create(fname)
	if err != nil {
		panic(err)
	}
	_, err = io.Copy(w, bytes.NewBuffer(req.JobSerz))
	if err != nil {
		panic(err)
	}
	log.Printf("saved data to file='%v'", fname)
	elap := t0.Sub(req.HDR.Created)
	mb := float64(len(req.JobSerz)) / float64(1<<20)
	rate := mb / (float64(elap) / float64(time.Second))
	reply.JobSerz = []byte(fmt.Sprintf("got upcall at '%v' => elap = %v (while mb=%v) => %v MB/sec", t0, elap, mb, rate))

	return nil
}
*/
