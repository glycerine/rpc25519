package rsync

import (
	"bytes"
	cryrand "crypto/rand"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	cv "github.com/glycerine/goconvey/convey"
	rpc "github.com/glycerine/rpc25519"
)

func Test201_rsync_style_chunking_and_hash_generation(t *testing.T) {

	cv.Convey("SummarizeFileInCDCHashes() should generate CDC FastCDC and/or UltraCDC hashes for a file. This exercises the CDC chunkers and does a basic sanity check that they are actually working, but not much more.", t, func() {

		host := "localhost"
		path := "../testdata/blob977k"

		data, err := os.ReadFile(path)
		panicOn(err)

		var modTime time.Time

		// SummarizeFile... rather than SummarizeBytes...
		// so we can manually confirm owner name is present. Yes.
		a, achunks, err := SummarizeFileInCDCHashes(host, path)

		cv.So(a.FileOwner != "", cv.ShouldBeTrue)

		vv("scan of file gave chunks: '%v'", achunks)
		cv.So(len(achunks.Chunks), cv.ShouldEqual, 24) // blob977k

		// now alter the data by prepending 2 bytes
		data2 := append([]byte{0x24, 0xff}, data...)
		_, bchunks, err := SummarizeBytesInCDCHashes(host, path+".prepend2bytes", data2, modTime)
		panicOn(err)

		onlyA, onlyB, both := Diff(achunks, bchunks)

		cv.So(len(onlyA), cv.ShouldEqual, 1)
		cv.So(len(onlyB), cv.ShouldEqual, 1)
		cv.So(len(both), cv.ShouldEqual, 23)

		// lets try putting 2 bytes at the end instead:
		data3 := append(data, []byte{0xf3, 0xee}...)
		_, bchunks, err = SummarizeBytesInCDCHashes(host, path+".postpend2bytes", data3, modTime)
		panicOn(err)

		onlyA, onlyB, both = Diff(achunks, bchunks)

		cv.So(len(onlyA), cv.ShouldEqual, 1)
		cv.So(len(onlyB), cv.ShouldEqual, 1)
		cv.So(len(both), cv.ShouldEqual, 23)

	})
}

func Test210_client_gets_new_file_over_rsync_twice(t *testing.T) {

	cv.Convey("using our rsync-like-protocol, the client, lacking a file, should be able to fetch it from the server. The second time fetching the same should be very fast because of chunking and hash comparisons in the rsync-like protocol", t, func() {

		// create a test file
		N := 1
		remotePath := fmt.Sprintf("cry%vmb", N)
		testfd, err := os.Create(remotePath)
		panicOn(err)
		slc := make([]byte, 1<<20) // 1 MB slice

		// random or zeros?
		if false {
			cryrand.Read(slc)
		}
		for range N {
			_, err = testfd.Write(slc)
			panicOn(err)
		}
		testfd.Close()
		vv("created N = %v MB test file in remotePath='%v'.", N, remotePath)

		// modify "local" target path so we don't overwrite our
		// source file when testing in one directory
		localPath := remotePath + ".local_rsync_out"
		vv("localPath = '%v'", localPath)

		// set up a server and a client.

		cfg := rpc.NewConfig()
		cfg.TCPonly_no_TLS = true

		cfg.CompressionOff = true

		cfg.ServerAddr = "127.0.0.1:0"
		srv := rpc.NewServer("srv_rsync_test210", cfg)

		serverAddr, err := srv.Start()
		panicOn(err)
		defer srv.Close()

		vv("server Start() returned serverAddr = '%v'", serverAddr)

		//srv.RegisterBistreamFunc("RsyncServerSide", srv.RsyncServerSide)

		srvRsyncNode := &RsyncNode{}
		panicOn(srv.Register(srvRsyncNode))

		cfg.ClientDialToHostPort = serverAddr.String()
		cli, err := rpc.NewClient("cli_rsync_test210", cfg)
		panicOn(err)
		err = cli.Start()
		panicOn(err)

		defer cli.Close()

		host := cli.LocalAddr()
		step0request, err := RsyncCliWantsToReadRemotePath(host, remotePath) // request, step 1
		panicOn(err)

		senderOV := &RsyncStep1_SenderOverview{} // response, step 1
		err = cli.Call("RsyncNode.Step1_SenderOverview", step0request, senderOV, nil)

		vv("got senderOV = '%#v'", senderOV)

		// A regular client would report to user that the file is not avail;
		// or the error from remote. We just panic in this test.
		if senderOV.ErrString != "" {
			panic(senderOV.ErrString)
		}
		if senderOV.SenderPath != remotePath {
			panic(fmt.Sprintf("'%v' = SenderPath != remotePath = '%v'",
				senderOV.SenderPath, remotePath))
		}
		if senderOV.SenderLenBytes == 0 {
			panic(fmt.Sprintf("remote file is 0 bytes: '%v'", remotePath))
		}

		// summarize our local file contents (even if empty)
		localPrecis, local, err := SummarizeFileInCDCHashes(host, localPath)
		panicOn(err)

		// step2 request: get diffs from what we have.
		readerAckOV := &RsyncStep2_ReaderAcksOverview{
			ReaderMatchesSenderAllGood: false,
			SenderPath:                 remotePath,
			ReaderPrecis:               localPrecis,
			ReaderChunks:               local,
		}

		senderDeltas := &RsyncStep3A_SenderProvidesData{} // response

		err = cli.Call("RsyncNode.Step3_SenderProvidesData", readerAckOV, senderDeltas, nil)
		panicOn(err) // reading body msgp: attempted to decode type "ext" with method for "map"

		vv("senderDeltas = '%v'", senderDeltas)

		plan := senderDeltas.SenderChunks // the plan follow remote template, our target.
		vv("plan = '%v'", plan)
		//local is our origin or starting point.
		localMap := getCryMap(local) // pre-index them for the update.

		// had to do a full file transfer for missing file.
		cv.So(plan.DataPresent(), cv.ShouldEqual, 1048576)
		cv.So(plan.FileSize, cv.ShouldEqual, 1048576)

		err = UpdateLocalWithRemoteDiffs(local.Path, localMap, plan)
		panicOn(err)

		if !fileExists(local.Path) {
			panic("file should have been written locally now!")
		}
		difflen := compareFilesDiffLen(local.Path, remotePath)
		cv.So(difflen, cv.ShouldEqual, 0)

		// ==============================
		// ==============================
		//
		// now repeat a second time, and we should get
		// no Data segments transfered.
		//
		// ==============================
		// ==============================

		vv("========>  second time! now no data expected")

		// update the localState, as if we didn't know it already.
		localPrecis, local, err = SummarizeFileInCDCHashes(host, localPath)
		panicOn(err)

		// step2 request: get diffs from what we have.
		readerAckOV = &RsyncStep2_ReaderAcksOverview{
			ReaderMatchesSenderAllGood: false,
			SenderPath:                 remotePath,
			ReaderPrecis:               localPrecis,
			ReaderChunks:               local,
		}

		senderDeltas = &RsyncStep3A_SenderProvidesData{} // response

		err = cli.Call("RsyncNode.Step3_SenderProvidesData", readerAckOV, senderDeltas, nil)
		panicOn(err) // reading body msgp: attempted to decode type "ext" with method for "map"

		//vv("senderDeltas = '%v'", senderDeltas)

		plan = senderDeltas.SenderChunks // the plan follow remote template, our target.
		cv.So(plan.DataPresent(), cv.ShouldEqual, 0)

		// ==============================
		// ==============================
		//
		// third time: pre-pend 2 bytes, and
		// tell server we want them to sync
		// to us.
		//
		// ==============================
		// ==============================
		cur, err := os.ReadFile(localPath)
		panicOn(err)

		pre2path := remotePath + ".pre2"
		pre2, err := os.Create(pre2path)
		panicOn(err)

		_, err = pre2.Write([]byte{0x77, 0x88})
		panicOn(err)
		_, err = io.Copy(pre2, bytes.NewBuffer(cur))
		panicOn(err)

		// new pre2path file is ready, summarize
		// it and push it to the remotePath.

		localPrecis2, local2, err := SummarizeFileInCDCHashes(host, pre2path)
		panicOn(err)

		pushMe := &RsyncStep3A_SenderProvidesData{
			SenderPath:   remotePath,
			SenderPrecis: localPrecis2,
			SenderChunks: local2,
		}
		_ = pushMe
	})
}
