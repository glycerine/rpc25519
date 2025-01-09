package rpc25519

import (
	"context"
	"fmt"
	"io"
	"sync"
	//"net/http"
	"os"
	filepath "path/filepath"
	"strconv"
	//"strings"
	//"sync"
	"testing"
	"time"

	//tdigest "github.com/caio/go-tdigest"
	"github.com/glycerine/loquet"
	myblake3 "github.com/glycerine/rpc25519/hash"

	cv "github.com/glycerine/goconvey/convey"
	"github.com/glycerine/rpc25519/progress"
)

func Test300_upload_streaming_test_of_large_file(t *testing.T) {

	cv.Convey("before we add compression, we want to test the cli -sendfile (upload) against he srv -readfile, the corresponding server side operation. Test300 is for upload of a file (multiple parts bigger than our max Message size.", t, func() {

		// set up server and client.

		// start with server
		cfg := NewConfig()
		cfg.ServerAddr = "127.0.0.1:0"

		srv := NewServer("srv_test300", cfg)
		serverAddr, err := srv.Start()
		panicOn(err)
		defer srv.Close()

		streamer := NewServerSideUploadState()
		// use the example.go example.
		fileUploaderServiceName := "__fileUploader"
		srv.RegisterUploadReaderFunc(fileUploaderServiceName,
			streamer.ReceiveFileInParts)

		// client

		cfg.ClientDialToHostPort = serverAddr.String()
		cli, err := NewClient("cli_test300", cfg)
		panicOn(err)
		err = cli.Start()
		panicOn(err)

		defer cli.Close()

		// end standard setup of server and client.

		// upload a blob from testdata/ directory.

		path := "testdata/blob977k"
		pathOut := "blob977k.servergot"
		os.Remove(pathOut)

		if !fileExists(path) {
			panic(fmt.Sprintf("drat! cli -sendfile path '%v' not found", path))
		}
		fi, err := os.Stat(path)
		panicOn(err)
		pathsize := fi.Size()
		meterUp := progress.NewTransferStats(pathsize, "[up]"+filepath.Base(path))

		t0 := time.Now()

		r, err := os.Open(path)
		if err != nil {
			panic(fmt.Sprintf("error reading path '%v': '%v'", path, err))
		}

		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()

		// wait to initialize the Uploader until we
		// actually have data to send.
		var strm *Uploader

		blake3hash := myblake3.NewBlake3()

		// much smoother progress display waiting for 1MB rather than 64MB
		//maxMessage := 1024 * 1024
		//maxMessage := UserMaxPayload

		// chunk smaller than we would for production,
		// so our testdata/blob can be small,
		// while we still test multiple chunk streaming.
		maxMessage := 1024
		buf := make([]byte, maxMessage)
		var tot int

		req := NewMessage()
		req.HDR.Created = time.Now()
		req.HDR.Args["pathsize"] = fmt.Sprintf("%v", pathsize)
		var lastUpdate time.Time

		// check for errors
		var checkForErrors = func() *Message {
			if strm != nil {
				//vv("checking strm (sendfile) chan for error: %p; callID='%v'", strm.ErrorCh, strm.CallID())
				select {
				case errMsg := <-strm.ErrorCh:
					vv("error for sendfile: '%v'", err)
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
				panic(err)
				break upload
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

				strm, err = cli.UploadBegin(ctx, fileUploaderServiceName, req)

				panicOn(err)
				if err1 == io.EOF {
					break upload
				}
				panicOn(err1)
				continue
			}

			streamMsg := NewMessage()
			// must copy!
			streamMsg.JobSerz = append([]byte{}, send...)
			streamMsg.HDR.Args = map[string]string{"blake3": sumstring}

			err = strm.UploadMore(ctx, streamMsg, err1 == io.EOF)

			// likely just "shutting down", so ask for details.
			if err != nil {
				err2msg := checkForErrors()
				if err2msg != nil {
					alwaysPrintf("err: '%v'", err)
					alwaysPrintf("err2: '%v'", err2msg.String())
				}
				panicOn(err)
				break upload
			}

			if time.Since(lastUpdate) > time.Second {
				meterUp.DoProgressWithSpeed(int64(tot), false, int64(i))
				lastUpdate = time.Now()
			}

			if err1 == io.EOF {
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
		select {
		case errMsg := <-strm.ErrorCh:
			alwaysPrintf("errMsg: '%v'", errMsg.String())
			panic(errMsg)
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
				vv("GOOD! server and client blake3 checksums are the same!\n serverTotSum='%v'\n clientTotsum='%v'", serverTotSum, clientTotSum)
				cv.So(true, cv.ShouldBeTrue)
			} else {
				vv("PROBLEM! server and client blake3 checksums do not match!\n serverTotSum='%v'\n clientTotsum='%v'", serverTotSum, clientTotSum)
				panic("checksum mismatch!")
			}
		case <-time.After(time.Minute):
			panic("should have gotten a reply from the server finishing the stream.")
		}

		// final result: did we get the file uploaded the same?
		diff := compareFilesDiffLen(path, pathOut)
		cv.So(diff, cv.ShouldEqual, 0)
	})
}

func Test301_download_streaming_test(t *testing.T) {

	cv.Convey("before we add compression, test cli -download vs srv -serve are the corresponding server side operations. Test301 is for download of a file (multiple parts)", t, func() {

		// set up server and client.

		// start with server
		cfg := NewConfig()
		cfg.ServerAddr = "127.0.0.1:0"

		srv := NewServer("srv_test301", cfg)
		serverAddr, err := srv.Start()
		panicOn(err)
		defer srv.Close()

		// serve downloads
		downloaderName := "__downloaderName" // must match client.go:163
		ssss := NewServerSendsDownloadState()
		srv.RegisterServerSendsDownloadFunc(downloaderName, ssss.ServerSendsDownload)

		// client

		cfg.ClientDialToHostPort = serverAddr.String()
		cli, err := NewClient("cli_test301", cfg)
		panicOn(err)
		err = cli.Start()
		panicOn(err)

		defer cli.Close()

		// end standard setup of server and client.

		// download a blob from testdata/ directory.

		path := "testdata/blob977k"

		downloadFile := path + ".downloaded"

		observedOutfile := path + ".downloaded.download_client_got"
		os.Remove(downloadFile)
		os.Remove(observedOutfile)

		t0 := time.Now()

		var meterDown *progress.TransferStats

		ctx := context.Background()
		downloader, err := cli.NewDownloader(ctx, downloaderName)
		panicOn(err)
		defer downloader.Close()

		s := NewPerCallID_FileToDiskState(downloader.CallID())
		s.OverrideFilename = downloadFile
		//vv("downloader.CallID() = '%v'", downloader.CallID())

		meterDownQuiet := false
		lastUpdate := time.Now()
		netread := 0 // net count of bytes read off the network.

		err = downloader.BeginDownload(ctx, path)
		panicOn(err)
		maxPayloadSeen := 0

	partsloop:
		for part := int64(0); true; part++ {
			select {
			case <-ctx.Done():
				vv("download aborting: context cancelled")
				break partsloop

			case req := <-downloader.ReadDownloadsCh:
				//vv("cli downloader downloadsCh sees %v", req.String())
				sz0 := len(req.JobSerz)
				netread += sz0
				if sz0 > maxPayloadSeen {
					maxPayloadSeen = sz0
				}

				if req.HDR.Typ == CallRPCReply {
					vv("cli downloader downloadsCh sees CallRPCReply, exiting")
					break partsloop
				}

				if req.HDR.StreamPart != part {
					panic(fmt.Sprintf("%v = req.HDR.StreamPart != part = %v",
						req.HDR.StreamPart, part))
				}
				if part == 0 {
					sz, ok := req.HDR.Args["pathsize"]
					if !ok {
						panic("server did not send of the pathsize in the first part!")
					}
					szi, err := strconv.Atoi(sz)
					pathsize := int64(szi)
					panicOn(err)

					vv("got 1st download part: %v bytes after %v; can meter total %v bytes:", sz0, time.Since(t0), formatUnder(int(pathsize)))

					meterDown = progress.NewTransferStats(
						pathsize, "[dn]"+filepath.Base(path))
				}

				if time.Since(lastUpdate) > time.Second {
					meterDown.DoProgressWithSpeed(int64(netread), meterDownQuiet, req.HDR.StreamPart)
					lastUpdate = time.Now()
				}

				last := (req.HDR.Typ == CallDownloadEnd)

				// this writes our req.JobSerz to disk.
				err = s.WriteOneMsgToFile(req, "download_client_got", last)

				if err != nil {
					panic(err)
					break partsloop
				}
				if last {
					if int(s.BytesWrit) != netread {
						panic(fmt.Sprintf("%v = s.BytesWrit != netread = %v", s.BytesWrit, netread))
					}
					//totSum := "blake3-" + cristalbase64.URLEncoding.EncodeToString(s.Blake3hash.Sum(nil))
					////vv("ReceiveFileInParts sees last set!")
					//vv("bytesWrit=%v; \nserver totSum='%v'", s.BytesWrit, totSum)

					elap := time.Since(s.T0)
					mb := float64(s.BytesWrit) / float64(1<<20)
					seconds := (float64(elap) / float64(time.Second))
					rate := mb / seconds
					_ = rate

					fmt.Println()
					fmt.Printf("total time for download: '%v'\n", time.Since(s.T0))
					fmt.Printf("file size: %v bytes.\n", formatUnder(int(s.BytesWrit)))
					fmt.Printf("rate:  %0.6f MB/sec. Used %v parts of max size %v bytes\n", rate, part+1, maxPayloadSeen)

				} // end if last
			} //end select
		} // end for part loop

		diff := compareFilesDiffLen(path, observedOutfile)
		cv.So(diff, cv.ShouldEqual, 0)

	})
}

func Test302_bistreaming_test_simultaneous_upload_and_download(t *testing.T) {

	cv.Convey("before we add compression, test bistreaming: cli -echofile vs srv -echo;. Test302 is for bistreaming (simultaneous upload and download of files bigger than max Message size)", t, func() {

		// set up server and client.

		// start with server
		cfg := NewConfig()
		cfg.ServerAddr = "127.0.0.1:0"

		srv := NewServer("srv_test301", cfg)
		serverAddr, err := srv.Start()
		panicOn(err)
		defer srv.Close()

		// serve bistreams
		streamerName := "echoBistreamFunc"

		// EchoBistreamFunc in example.go has most of the implementation.
		srv.RegisterBistreamFunc(streamerName, EchoBistreamFunc)

		// client

		cfg.ClientDialToHostPort = serverAddr.String()
		cli, err := NewClient("cli_test301", cfg)
		panicOn(err)
		err = cli.Start()
		panicOn(err)

		defer cli.Close()

		// end standard setup of server and client.

		// download a blob from testdata/ directory.

		//path := "testdata/blob977k"
		path := "testdata/blob3m"
		observedOutfile := path + ".echoed.echoclientgot"
		os.Remove(observedOutfile)

		t0 := time.Now()

		var bistream *Bistreamer
		var wg sync.WaitGroup
		bistreamerName := "echoBistreamFunc"
		meterDownQuietCh := make(chan bool, 2)
		meterDownQuietCh <- true
		uploadDone := loquet.NewChan[bool](nil)

		var meterDown *progress.TransferStats

		//if doBistream {

		fi, err := os.Stat(path)
		panicOn(err)
		meterDown = progress.NewTransferStats(fi.Size(), "[dn]"+filepath.Base(path))

		// Approach:
		// have the echofile implementation do the upload for us,
		// while we do the download in a separate goroutine.

		downloadFile := path + ".echoed"

		bistream, err = cli.NewBistreamer(bistreamerName)
		panicOn(err)
		defer bistream.Close()

		s := NewPerCallID_FileToDiskState(bistream.CallID())
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
					sz := len(req.JobSerz)
					if netread == 0 {
						vv("downloaded %v bytes after %v", sz, time.Since(t0))
					}
					netread += sz

					if req.HDR.Typ == CallRPCReply {
						//vv("cli bistream downloadsCh sees CallRPCReply, exiting goro")
						return
					}
					if time.Since(lastUpdate) > time.Second {
						meterDown.DoProgressWithSpeed(int64(netread), meterDownQuiet, req.HDR.StreamPart)
						lastUpdate = time.Now()
					}

					last := (req.HDR.Typ == CallDownloadEnd)
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

		//} // end if bistream

		// sendfile part: upload simultaneously
		//if sendfile != "" {

		if !fileExists(path) {
			panic(fmt.Sprintf("drat! cli -sendfile path '%v' not found", path))
		}
		fi, err = os.Stat(path)
		panicOn(err)
		pathsize := fi.Size()
		meterUp := progress.NewTransferStats(pathsize, "[up]"+filepath.Base(path))

		r, err := os.Open(path)
		if err != nil {
			panic(fmt.Sprintf("error reading path '%v': '%v'", path, err))
		}

		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()

		// wait to initialize the Uploader until we
		// actually have data to send.
		var strm *Uploader

		blake3hash := myblake3.NewBlake3()

		// much smoother progress display waiting for 1MB rather than 64MB
		maxMessage := 1024 * 1024
		//maxMessage := UserMaxPayload
		//maxMessage := 1024
		buf := make([]byte, maxMessage)
		var tot int

		req := NewMessage()
		req.HDR.Created = time.Now()
		req.HDR.Args["pathsize"] = fmt.Sprintf("%v", pathsize)
		var lastUpdate time.Time

		// check for errors
		var checkForErrors = func() *Message {
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

				//vv("client about to do bistream.Begin(); req = '%v'", req.String())
				err = bistream.Begin(ctx, req)
				panicOn(err)

				if err1 == io.EOF {
					uploadDone.Close()
					break upload
				}
				panicOn(err1)
				continue
			}

			streamMsg := NewMessage()
			// must copy!
			streamMsg.JobSerz = append([]byte{}, send...)
			streamMsg.HDR.Args = map[string]string{"blake3": sumstring}

			err = bistream.UploadMore(ctx, streamMsg, err1 == io.EOF)

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
		//clientTotSum := blake3hash.SumString()

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
		//return // all done, return from main().

		//} // if sendfile

		diff := compareFilesDiffLen(path, observedOutfile)
		cv.So(diff, cv.ShouldEqual, 0)

	})
}
