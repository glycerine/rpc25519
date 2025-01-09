package rpc25519

import (
	"context"
	"fmt"
	"io"
	//"net/http"
	"os"
	filepath "path/filepath"
	//"strconv"
	//"strings"
	//"sync"
	"testing"
	"time"

	//tdigest "github.com/caio/go-tdigest"
	//"github.com/glycerine/loquet"
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
		defer func() {
			diff := compareFilesDiffLen(path, pathOut)
			cv.So(diff, cv.ShouldEqual, 0)
		}()
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

				strm, err = cli.UploadBegin(ctx, fileUploaderServiceName, req)

				panicOn(err)
				if err1 == io.EOF {
					//uploadDone.Close()
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
				return
			}

			if time.Since(lastUpdate) > time.Second {
				meterUp.DoProgressWithSpeed(int64(tot), false, int64(i))
				lastUpdate = time.Now()
			}

			if err1 == io.EOF {
				//uploadDone.Close()
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
				cv.So(true, cv.ShouldBeTrue)
			} else {
				vv("PROBLEM! server and client blake3 checksums do not match!\n serverTotSum='%v'\n clientTotsum='%v'", serverTotSum, clientTotSum)
				panic("checksum mismatch!")
			}
		case <-time.After(time.Minute):
			panic("should have gotten a reply from the server finishing the stream.")
		}
		return
	})
}

func Test301_streaming_test_of_download(t *testing.T) {

	cv.Convey("before we add compression, we want to take the cli -sendfile, cli -echofile, and cli -download commands and turn them into test(s) that verify quickly that they are all still working. The srv -readfile, srv -echo, and srv -serve are the corresponding server side operations. Test301 is for download of a file (multiple parts)", t, func() {
	})
}

func Test302_streaming_test_of_bistream(t *testing.T) {

	cv.Convey("before we add compression, we want to take the cli -sendfile, cli -echofile, and cli -download commands and turn them into test(s) that verify quickly that they are all still working. The srv -readfile, srv -echo, and srv -serve are the corresponding server side operations. Test302 is for bistreaming (simultaneous upload and download of files bigger than max Message size)", t, func() {

	})
}
