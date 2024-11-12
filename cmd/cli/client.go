package main

import (
	//"fmt"
	"flag"
	"log"
	"os"
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

	flag.Parse()

	if *remoteDefault {
		*dest = "192.168.254.151:8443"
	}

	// compress of 100 still gives 1000x compression,
	// about 8KB for 1e6 samples; good accuracy at tails
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
	log.Printf("client connected from local addr='%v'\n", cli.LocalAddr())

	req := rpc25519.NewMessage()
	req.JobSerz = []byte("client says hello and requests this be echoed back with a timestamp!")

	if *n > 1 {
		log.Printf("about to do n = %v calls.\n", *n)
	}
	var reply *rpc25519.Message
	var i int
	slowest := -1.0
	defer func() {
		q999 := td.Quantile(0.999)
		q99 := td.Quantile(0.99)
		q50 := td.Quantile(0.50)
		log.Printf("client did %v calls.  err = '%v' slowest='%v nanosec'; q999='%v nanoseconds'; q99='%v nanoseconds'; q50='%v nanoseconds'\n", i, err, slowest, q999, q99, q50)
	}()
	for i = 0; i < *n; i++ {
		//reply, err = cli.SendAndGetReply(req, nil)
		t0 := time.Now()
		reply, err = cli.SendAndGetReplyWithTimeout(*wait, req)
		if err != nil {
			panic(err)
		}
		elap := float64(time.Since(t0))
		err = td.Add(elap) // nanoseconds
		panicOn(err)
		if elap > slowest {
			slowest = elap
		}
	}

	if !*quiet {
		log.Printf("client sees reply (Seqno=%v) = '%v'\n", reply.HDR.Seqno, string(reply.JobSerz))
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

func panicOn(err error) {
	if err != nil {
		panic(err)
	}
}
