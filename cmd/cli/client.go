package main

import (
	"flag"
	"fmt"
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

	if *n > 1 {
		vv("about to do n = %v calls.\n", *n)
	}
	var reply *rpc25519.Message
	var i int
	slowest := -1.0
	nextSlowest := -1.0
	defer func() {
		q999 := td.Quantile(0.999)
		q99 := td.Quantile(0.99)
		q50 := td.Quantile(0.50)
		vv("client did %v calls.  err = '%v' \nslowest= %v nanosec\nnextSlowest= %v nanosec\nq999_= %v nanosec\nq99_= %v nanosec\nq50_= %v nanosec\n", i, err, slowest, nextSlowest, q999, q99, q50)
	}()

	var elaps []time.Duration

	req := rpc25519.NewMessage()
	req.JobSerz = []byte("client says hello and requests this be echoed back with a timestamp!")

	for i = 0; i < *n; i++ {
		//reply, err = cli.SendAndGetReply(req, nil)
		t0 := time.Now()
		reply, err = cli.SendAndGetReplyWithTimeout(*wait, req)

		/*		if err == nil && i == 0 {
					// verify that our diagnostics actually print on the server
					diag := rpc25519.NewMessage()
					diag.HDR.Typ = rpc25519.CallDebugWasSeen
					diag.HDR.Subject = req.HDR.CallID
					panicOn(cli.OneWaySend(diag, nil))
					vv("sent diagnostic for first good call, callID = '%v'", req.HDR.CallID)
				}
		*/

		e := time.Since(t0)
		elaps = append(elaps, e)
		elap := float64(e)
		errTd := td.Add(elap) // nanoseconds
		panicOn(errTd)
		if elap > slowest {
			nextSlowest = slowest
			slowest = elap
		}
		// only now panic on timeout, so our 10 sec / 20 sec timeout is the slowest.
		if err != nil {
			/*
				vv("client stop sending on err = '%v', elap = '%v'. req = '%v'; history=", err, elap, req.HDR.String())
				vv("try re-sending!")

				t1 := time.Now()
				reply, err = cli.SendAndGetReplyWithTimeout(*wait, req)
				vv("re-send completed in %v", time.Since(t1))
			*/

			vv("client stopping on err='%v'; sending diagnostic for CallID='%v'", err, req.HDR.CallID)
			// send diagnostic: did it get there before? yes.
			// did we get a response?
			cli.MutDebug.Lock()
			reply, ok := cli.AllReply[req.HDR.CallID]
			cli.MutDebug.Unlock()
			if ok {
				vv("client got a reply after %v, but it was not matched. reply = '%v'", reply.HDR.LocalRecvTm.Sub(t0), reply.HDR.String())
			} else {
				vv("client has not seen a reply with req.HDR.CallID = '%v'", req.HDR.CallID)
			}

			diag := rpc25519.NewMessage()
			diag.HDR.Typ = rpc25519.CallDebugWasSeen
			diag.HDR.Subject = req.HDR.CallID
			panicOn(cli.OneWaySend(diag, nil))
			//time.Sleep(5 * time.Minute)
			break

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
