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

	"github.com/glycerine/ipaddr"
	"github.com/glycerine/rpc25519"
	rsync "github.com/glycerine/rpc25519/jsync"
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
			if n > 0 {
				fmt.Printf("\n\nserver elapsed: %v for calls seen: %v  => %v calls/second.\n", elap, n, float64(n)/float64(int64(elap)/1e9))
			}
			os.Exit(0)
		}
	}()
	signal.Notify(sigChan, syscall.SIGINT)
}

func main() {

	rpc25519.Exit1IfVersionReq()

	fmt.Printf("%v", rpc25519.GetCodeVersion("srv"))

	log.SetFlags(log.LstdFlags | log.Lshortfile) // Add Lshortfile for short file names

	noticeControlC()

	certdir := rpc25519.GetCertsDir()
	//cadir := rpc25519.GetPrivateCertificateAuthDir()

	hostIP := ipaddr.GetExternalIP() // e.g. 100.x.x.x
	_ = hostIP
	//vv("hostIP = '%v'", hostIP)

	//scheme, ip, port, isUnspecified, isIPv6, err := ipaddr.ParseURLAddress(hostIP)
	//panicOn(err)
	//vv("server defaults to binding: scheme='%v', ip='%v', port=%v, isUnspecified='%v', isIPv6='%v'", scheme, ip, port, isUnspecified, isIPv6)

	//var addr = flag.String("s", hostIP+":8443", "server address to bind and listen on")
	var addr = flag.String("s", "0.0.0.0:8443", "server address to bind and listen on")
	var tcp = flag.Bool("tcp", false, "use TCP instead of the default TLS")
	var skipVerify = flag.Bool("skip-verify", false, "do not require client certs be from our CA, nor remember client certs in a known_client_keys file for later lockdown")

	var useName = flag.String("k", "node", "specifies name of keypairs to use (certs/name.crt and certs/name.key); instead of the default certs/node.crt and certs/node.key for the server.")
	var certPath = flag.String("certs", certdir, "use this path for certs; instead of the local ./certs/ directory.")

	var quic = flag.Bool("q", false, "use QUIC instead of TCP/TLS")

	var profile = flag.String("prof", "", "host:port to start web profiler on. host can be empty for all localhost interfaces")

	var psk = flag.String("psk", "", "path to pre-shared key file")

	var seconds = flag.Int("sec", 0, "run for this many seconds")

	var max = flag.Int("max", 0, "set runtime.GOMAXPROCS to this value.")

	quiet = flag.Bool("quiet", false, "for profiling, do not log answer")

	var readfile = flag.Bool("readfile", false, "listen for files to write to disk; client should run -sendfile")

	var servefile = flag.Bool("serve", false, "serve downloads; client should run -download")

	var doRsyncServer = flag.Bool("rsync", true, "act as an rsync reader/receiver of files; cli -rsync will send us the diffs of a file. We report what chunks we need to update a file beforehand.")

	var echo = flag.Bool("echo", false, "bistream echo everything")

	var compressOff = flag.Bool("big", false, "turn off sending compression.")
	var compressAlgo = flag.String("press", "s2", "select sending compression algorithm; one of: s2, lz4, zstd:01, zstd:03, zstd:07, zstd:11")

	var verboseDebugCompress = flag.Bool("v", false, "verbose debugging compression settings per message")

	var serialNotParallel = flag.Bool("serial", false, "serial single threaded file chunking, rather than parallel. Mostly for benchmarking")

	flag.Parse()

	rsync.SetParallelChunking(!*serialNotParallel)

	if *verboseDebugCompress {
		rpc25519.DebugVerboseCompress = true
	}

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
	cfg.CompressionOff = *compressOff
	cfg.CompressAlgo = *compressAlgo

	srv := rpc25519.NewServer("srv", cfg)
	defer srv.Close()

	if *echo {
		streamerName := "echoBistreamFunc"
		srv.RegisterBistreamFunc(streamerName, rpc25519.EchoBistreamFunc)
	}

	if *servefile {
		// serve downloads
		downloaderName := "__downloaderName" // must match client.go:163
		ssss := rpc25519.NewServerSendsDownloadState()
		srv.RegisterServerSendsDownloadFunc(downloaderName, ssss.ServerSendsDownload)

	}

	if *readfile {
		streamer := rpc25519.NewServerSideUploadState()
		// use the example.go example.
		srv.RegisterUploadReaderFunc("__fileUploader", streamer.ReceiveFileInParts)
	} else {
		serviceName := "customEcho"
		srv.Register2Func(serviceName, customEcho)
	}
	//vv("cfg.ServerAddr='%v'", cfg.ServerAddr)
	serverAddr, err := srv.Start()
	if err != nil {
		panic(fmt.Sprintf("could not start rpc25519.Server with config = '%#v'; err='%v'", cfg, err))
	}

	log.Printf("rpc25519.server Start() returned serverAddr = '%v'", serverAddr)
	fmt.Printf("compression: %v; compressionAlgo: '%v'\n",
		!*compressOff, *compressAlgo)

	if *doRsyncServer {
		reqs := make(chan *rsync.RequestToSyncPath)
		fmt.Printf("starting rsync_server\n")
		lpb, ctx, canc, err := rsync.RunRsyncService(cfg, srv, "rsync_server", false, reqs)

		panicOn(err)
		defer lpb.Close()
		defer canc()
		_ = ctx

		select {}
		return
	}

	if *seconds > 0 {
		t0 := time.Now()
		<-time.After(time.Second * time.Duration(*seconds))
		n := atomic.LoadInt64(&calls)
		elap := time.Since(t0)
		if n > 0 {
			fmt.Printf("\n\nserver %v for calls seen: %v  => %v calls/second.\n", elap, n, float64(n)/float64(int64(elap)/1e9))
		}
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
