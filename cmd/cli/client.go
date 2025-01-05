package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	filepath "path/filepath"
	"time"

	tdigest "github.com/caio/go-tdigest"
	"github.com/glycerine/rpc25519"
)

var td *tdigest.TDigest

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile) // Add Lshortfile for short file names

	var dest = flag.String("s", "127.0.0.1:8443", "server address to send echo request to.")
	var remoteDefault = flag.Bool("r", false, "ping the default test remote at 192.168.254.151")
	var tcp = flag.Bool("tcp", false, "use TCP instead of the default TLS")
	var skipVerify = flag.Bool("skip-verify", false, "skip verify-ing that server certs are in-use and authorized by our CA; only possible with TLS.")
	var useName = flag.String("k", "", "specifies name of keypairs to use (certs/name.crt and certs/name.key); instead of the default certs/client.crt and certs/client.key")
	var certPath = flag.String("certs", "", "use this path on the lived filesystem for certs; instead of the embedded certs/ from build-time.")

	var quic = flag.Bool("q", false, "use QUIC instead of TCP/TLS")
	var hang = flag.Bool("hang", false, "hang at the end, to see if keep-alives are working.")
	var psk = flag.String("psk", "", "path to pre-shared key file")
	var clientHostPort = flag.String("hostport", ":0", "client will use use this host and port (port can be 0) to dial from.")
	var n = flag.Int("n", 1, "number of calls to make")
	var quiet = flag.Bool("quiet", false, "operate quietly")

	var wait = flag.Duration("wait", 10*time.Second, "time to wait for call to complete")

	var sendfile = flag.String("sendfile", "", "path to file to send")

	flag.Parse()

	if *remoteDefault {
		*dest = "192.168.254.151:8443"
	}

	// A tdigest compress setting of 100 gives 1000x compression,
	// about 8KB for 1e6 float64 samples; and retains good accuracy
	// at the tails of the distribution.
	var err error
	td, err = tdigest.New(tdigest.Compression(100))
	panicOn(err)

	cfg := rpc25519.NewConfig()
	cfg.ClientDialToHostPort = *dest // "127.0.0.1:8443",
	cfg.TCPonly_no_TLS = *tcp
	cfg.UseQUIC = *quic
	cfg.SkipVerifyKeys = *skipVerify
	cfg.ClientKeyPairName = *useName
	cfg.CertPath = *certPath
	cfg.PreSharedKeyPath = *psk
	cfg.ClientHostPort = *clientHostPort

	cli, err := rpc25519.NewClient("cli", cfg)
	if err != nil {
		log.Printf("bad client config: '%v'\n", err)
		os.Exit(1)
	}
	err = cli.Start()
	if err != nil {
		log.Printf("client could not connect: '%v'\n", err)
		os.Exit(1)
	}
	defer cli.Close()
	vv("client connected from local addr='%v'", cli.LocalAddr())

	if *sendfile != "" {
		t0 := time.Now()

		path := *sendfile
		if !fileExists(path) {
			panic(fmt.Sprintf("drat! cli -sendfile path '%v' not found", path))
		}
		by, err := os.ReadFile(path)
		if err != nil {
			panic(fmt.Sprintf("error reading path '%v': '%v'", path, err))
		}

		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()
		var strm *rpc25519.Uploader

		maxMessage := 64*1024*1024 - 80
		for i := 0; len(by) > 0; i++ {

			var part []byte
			if len(by) < maxMessage {
				part = by
				by = nil
			} else {
				part = by[:maxMessage]
				by = by[maxMessage:]
			}
			if i == 0 {
				req := rpc25519.NewMessage()
				req.JobSerz = part
				req.HDR.Subject = "receiveFile:" + filepath.Base(path) // client looks for this

				strm, err = cli.UploadBegin(ctx, req)
				panicOn(err)
				continue
			}
			streamMsg := rpc25519.NewMessage()
			streamMsg.JobSerz = part
			err = strm.UploadMore(ctx, streamMsg, len(by) == 0)
			panicOn(err)
		}

		select {
		case reply := <-strm.ReadCh:
			report := string(reply.JobSerz)
			vv("reply.HDR: '%v' with JobSerz: '%v'", reply.HDR.String(), report)
			fmt.Printf("round trip time for upload: '%v'\n", time.Since(t0))
		case <-time.After(time.Minute):
			panic("should have gotten a reply from the server finishing the stream.")
		}
		return
	} // if sendfile

	if *n > 1 {
		alwaysPrintf("about to do n = %v calls.\n", *n)
	}
	var reply *rpc25519.Message
	var i int
	slowest := -1.0
	nextSlowest := -1.0
	defer func() {
		q999 := td.Quantile(0.999)
		q99 := td.Quantile(0.99)
		q50 := td.Quantile(0.50)
		alwaysPrintf("client did %v calls.  err = '%v' \nslowest= %v nanosec\nnextSlowest= %v nanosec\nq999_= %v nanosec\nq99_= %v nanosec\nq50_= %v nanosec\n", i, err, slowest, nextSlowest, q999, q99, q50)
	}()

	var elaps []time.Duration

	req := rpc25519.NewMessage()
	req.JobSerz = []byte("client says hello and requests this be echoed back with a timestamp!")

	for i = 0; i < *n; i++ {
		//reply, err = cli.SendAndGetReply(req, nil)
		t0 := time.Now()
		reply, err = cli.SendAndGetReplyWithTimeout(*wait, req)
		e := time.Since(t0)
		elaps = append(elaps, e)
		elap := float64(e)
		errTd := td.Add(elap) // nanoseconds
		panicOn(errTd)
		if elap > slowest {
			nextSlowest = slowest
			slowest = elap
		}
		if err != nil {

			var sum time.Duration
			for i, e := range elaps {
				if i < 10 || i > len(elaps)-5 {
					fmt.Printf("%v : %v\n", i, e)
				}
				sum += e
			}
			fmt.Printf("\n ========= sum = %v \n", sum)
			panicOn(err)
		}
	}

	if !*quiet {
		vv("client sees reply (Seqno=%v) = '%v'", reply.HDR.Seqno, string(reply.JobSerz))
	}
	if *hang {
		log.Printf("client hanging to see if keep-alives happen...")
		for {
			select {
			case <-time.After(time.Second * 35):
				if cli.IsDown() {
					log.Printf("client is down")
				} else {
					log.Printf("client is up")
				}
			}
		}
	}
}
