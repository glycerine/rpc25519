package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	filepath "path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	tdigest "github.com/caio/go-tdigest"
	"github.com/glycerine/loquet"
	"github.com/glycerine/rpc25519"
	myblake3 "github.com/glycerine/rpc25519/hash"
	"github.com/glycerine/rpc25519/progress"
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

	var echofile = flag.String("echofile", "", "path to file to send to bistreaming server, which will echo it back to us")

	flag.Parse()

	if *remoteDefault {
		*dest = "192.168.254.151:8443"
	}

	if *dest != "" {
		// provide a default port if not given, of :8443
		host, port, err := net.SplitHostPort(*dest)
		_ = port
		if err != nil && strings.Contains(err.Error(), "missing port in address") {
			*dest += ":8443"
			vv("using default dest port: %v", *dest)
		} else {
			if port == "0" {
				*dest += host + ":8443"
				vv("defaulting to port 8443, as in: %v", *dest)
			}
		}
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
	fmt.Println()

	doBistream := false
	var bistream *rpc25519.Bistreamer
	var wg sync.WaitGroup
	bistreamerName := "echoBistreamFunc"

	// down/upload can be 9x different, measure both.
	meterDownQuietCh := make(chan bool, 2)
	meterDownQuietCh <- true
	uploadDone := loquet.NewChan[bool](nil)

	if *echofile != "" {
		doBistream = true

		path := *echofile
		fi, err := os.Stat(path)
		panicOn(err)
		meterDown := progress.NewTransferStats(fi.Size(), "[down]"+filepath.Base(path))

		// Approach:
		// have the echofile implementation do the upload for us,
		// while we do the download in a separate goroutine.

		vv("cli echofile requested for file '%v'", *echofile)

		*sendfile = *echofile

		downloadFile := *echofile + ".echoed"

		bistream, err = cli.NewBistreamer(bistreamerName)
		panicOn(err)
		defer bistream.Close()

		s := rpc25519.NewPerCallID_FileToDiskState(bistream.CallID())
		s.OverrideFilename = downloadFile
		//vv("bistream.CallID() = '%v'", bistream.CallID())

		wg.Add(1)
		go func() {
			defer wg.Done()

			meterDownQuiet := true
			lastUpdate := time.Now()
			netread := 0 // net count of bytes read off the network.
			whenUploadDone := uploadDone.WhenClosed()
			for {
				select {
				case <-whenUploadDone:
					vv("upload done, down has read: %v bytes", netread)
					whenUploadDone = nil
				case meterDownQuiet = <-meterDownQuietCh:
				case req := <-bistream.ReadDownloadsCh:
					//vv("cli bistream downloadsCh sees %v", req.String())

					netread += len(req.JobSerz)

					if req.HDR.Typ == rpc25519.CallRPCReply {
						//vv("cli bistream downloadsCh sees CallRPCReply, exiting goro")
						return
					}
					if time.Since(lastUpdate) > time.Second {
						meterDown.DoProgressWithSpeed(int64(netread), meterDownQuiet, req.HDR.StreamPart)
						lastUpdate = time.Now()
					}

					last := (req.HDR.Typ == rpc25519.CallDownloadEnd)
					err = s.WriteOneMsgToFile(req, "echoclientgot", last)

					if err != nil {
						panic(err)
						return
					}
					if last {
						//totSum := "blake3-" + cristalbase64.URLEncoding.EncodeToString(s.Blake3hash.Sum(nil))
						////vv("ReceiveFileInParts sees last set!")
						//vv("bytesWrit=%v; \nserver totSum='%v'", s.BytesWrit, totSum)

						elap := time.Since(s.T0)
						mb := float64(s.BytesWrit) / float64(1<<20)
						seconds := (float64(elap) / float64(time.Second))
						rate := mb / seconds
						_ = rate

						fmt.Println()
						fmt.Printf("total time for echo: '%v'\n", time.Since(s.T0))
						fmt.Printf("file size: %v bytes.\n", formatUnder(int(s.BytesWrit)))

					} // end if last
				} //end select
			} // end for seenCount
		}()

	} // end if echofile

	if *sendfile != "" {
		t0 := time.Now()

		path := *sendfile
		if !fileExists(path) {
			panic(fmt.Sprintf("drat! cli -sendfile path '%v' not found", path))
		}
		fi, err := os.Stat(path)
		panicOn(err)
		meterUp := progress.NewTransferStats(fi.Size(), "[up]"+filepath.Base(path))

		r, err := os.Open(path)
		if err != nil {
			panic(fmt.Sprintf("error reading path '%v': '%v'", path, err))
		}

		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()

		// wait to initialize the Uploader until we
		// actually have data to send.
		var strm *rpc25519.Uploader

		blake3hash := myblake3.NewBlake3()

		// much smoother progress display waiting for 1MB rather than 64MB
		maxMessage := 1024 * 1024
		//maxMessage := rpc25519.UserMaxPayload
		//maxMessage := 1024
		buf := make([]byte, maxMessage)
		var tot int

		req := rpc25519.NewMessage()
		req.HDR.Created = time.Now()
		var lastUpdate time.Time

		// check for errors
		var checkForErrors = func() *rpc25519.Message {
			if strm != nil {
				//vv("checking strm (sendfile) chan for error: %p; callID='%v'", strm.ErrorCh, strm.CallID())
				select {
				case errMsg := <-strm.ErrorCh:
					vv("error for sendfile: '%v'", err)
					return errMsg
				default:
				}
			}
			if bistream != nil {
				//vv("checking bistream (echofile) chan for error: %p; callID='%v'", bistream.ErrorCh, bistream.CallID())
				select {
				case errMsg := <-bistream.ErrorCh:
					vv("error from echofile: '%v'", err)
					return errMsg
				default:
				}
			}
			return nil
		}

		i := 0
	upload:
		for i = 0; true; i++ {

			if err := checkForErrors(); err != nil {
				alwaysPrintf("error: '%v'", err.String())
				return
			}

			nr, err1 := r.Read(buf)
			//vv("on read i=%v, got nr=%v, (maxMessage=%v), err='%v'", i, nr, maxMessage, err1)

			send := buf[:nr] // can be empty
			tot += nr
			sumstring := myblake3.Blake3OfBytesString(send)
			//vv("i=%v, len=%v, sumstring = '%v'", i, nr, sumstring)
			blake3hash.Write(send)

			if i == 0 {
				// must copy!
				req.JobSerz = append([]byte{}, send...)
				filename := filepath.Base(path)
				req.HDR.Args = map[string]string{"readFile": filename, "blake3": sumstring}

				if doBistream {
					//vv("client about to do bistream.Begin(); req = '%v'", req.String())
					err = bistream.Begin(ctx, req)
				} else {
					strm, err = cli.UploadBegin(ctx, "__fileUploader", req)
				}
				panicOn(err)
				if err1 == io.EOF {
					uploadDone.Close()
					break upload
				}
				panicOn(err1)
				continue
			}

			streamMsg := rpc25519.NewMessage()
			// must copy!
			streamMsg.JobSerz = append([]byte{}, send...)
			streamMsg.HDR.Args = map[string]string{"blake3": sumstring}
			if doBistream {
				err = bistream.UploadMore(ctx, streamMsg, err1 == io.EOF)
			} else {
				err = strm.UploadMore(ctx, streamMsg, err1 == io.EOF)
			}
			// likely just "shutting down", so ask for details.
			if err != nil {
				err2msg := checkForErrors()
				if err2msg != nil {
					alwaysPrintf("err: '%v'", err)
					alwaysPrintf("err2: '%v'", err2msg.String())
				}
				panicOn(err)
				return
			}

			if time.Since(lastUpdate) > time.Second {
				meterUp.DoProgressWithSpeed(int64(tot), false, int64(i))
				lastUpdate = time.Now()
			}

			if err1 == io.EOF {
				uploadDone.Close()
				break upload
			}
			panicOn(err1)

		} // end for i
		nparts := i

		meterUp.DoProgressWithSpeed(int64(tot), false, int64(i))
		clientTotSum := blake3hash.SumString()

		fmt.Println()

		reportUploadTime := true
		if reportUploadTime {
			elap := time.Since(t0)
			mb := float64(tot) / float64(1<<20)
			seconds := (float64(elap) / float64(time.Second))
			rate := mb / seconds

			alwaysPrintf("upload part of echo done elapsed: %v \n we "+
				"uploaded tot = %v bytes (=> %0.6f MB/sec) in %v parts",
				elap, tot, rate, nparts)
		}
		if doBistream {
			fmt.Println()
			meterDownQuietCh <- false // show the download progress

			//vv("bistream about to wait")
			wg.Wait()

			reportRoundTripTime := true
			if reportRoundTripTime {
				elap := time.Since(t0)
				mb := float64(2*tot) / float64(1<<20)
				seconds := (float64(elap) / float64(time.Second))
				rate := mb / seconds

				alwaysPrintf("round-trip echo done elapsed: %v; we "+
					"transferred 2*tot = %v bytes (=> %0.6f MB/sec)", elap, 2*tot, rate)
			}

			//vv("past Wait, client upload tot-sum='%v'", clientTotSum)
			// the goro consumed the CallRPCReply to know when to
			// exit, and we get here because when the goro exists, the
			// wait is done. so there won't be any more bistream messages coming.
			return // all done, return from main().

		} else {
			select {
			case errMsg := <-strm.ErrorCh:
				alwaysPrintf("errMsg: '%v'", errMsg.String())
				return
			case reply := <-strm.ReadCh:
				if false {
					report := string(reply.JobSerz)
					vv("reply.HDR: '%v'", reply.HDR.String())
					vv("with JobSerz: '%v'", report)
				}
				fmt.Printf("total time for upload: '%v'\n", time.Since(t0))
				fmt.Printf("file size: %v bytes.\n", formatUnder(tot)) // , clientTotSum)
				serverTotSum := reply.HDR.Args["serverTotalBlake3sum"]

				if clientTotSum == serverTotSum {
					//vv("GOOD! server and client blake3 checksums are the same!\n serverTotSum='%v'\n clientTotsum='%v'", serverTotSum, clientTotSum)
				} else {
					vv("PROBLEM! server and client blake3 checksums do not match!\n serverTotSum='%v'\n clientTotsum='%v'", serverTotSum, clientTotSum)
				}
			case <-time.After(time.Minute):
				panic("should have gotten a reply from the server finishing the stream.")
			}
			return
		}
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

func formatUnder(n int) string {
	// Convert to string first
	str := strconv.FormatInt(int64(n), 10)

	// Handle numbers less than 1000
	if len(str) <= 3 {
		return str
	}

	// Work from right to left, adding underscores
	var result []byte
	for i := len(str) - 1; i >= 0; i-- {
		if (len(str)-1-i)%3 == 0 && i != len(str)-1 {
			result = append([]byte{'_'}, result...)
		}
		result = append([]byte{str[i]}, result...)
	}

	return string(result)
}
