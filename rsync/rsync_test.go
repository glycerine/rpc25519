package rsync

import (
	cryrand "crypto/rand"
	"fmt"
	"os"
	"testing"
	"time"

	cv "github.com/glycerine/goconvey/convey"
	rpc "github.com/glycerine/rpc25519"
)

func Test201_rsync_style_hash_generation(t *testing.T) {

	cv.Convey("rsync.go SummarizeFileInCDCHashes() should generate CDC FastCDC and/or UltraCDC hashes for a file", t, func() {
		host := "localhost"
		path := "../testdata/blob977k"

		data, err := os.ReadFile(path)
		panicOn(err)

		var modTime time.Time
		// SummarizeFile... rather than SummarizeBytes...
		// so we can manually confirm owner name is present. Yes.
		h, err := SummarizeFileInCDCHashes(host, path)
		//h, err := SummarizeBytesInCDCHashes(host, path, data, modTime)

		cv.So(h.FileOwner != "", cv.ShouldBeTrue)
		_ = h
		//vv("scan of file gave: hashes '%v'", h.String())
		//cv.So(h.NumChunks, cv.ShouldEqual, 16) // blob977k

		// now alter the data by prepending 2 bytes
		data2 := append([]byte{0x24, 0xff}, data...)
		h2, err := SummarizeBytesInCDCHashes(host, path+".prepend2bytes", data2, modTime)
		panicOn(err)
		_ = h2
		/*
			diffs2 := h2.Diff(h)
			//vv("diffs2 = '%s'", diffs2)
			//cv.So(h2.NumChunks, cv.ShouldEqual, 16)
			//cv.So(len(h2.Chunks), cv.ShouldEqual, 16)
			cv.So(len(diffs2.OnlyA), cv.ShouldEqual, 1)
			cv.So(len(diffs2.OnlyB), cv.ShouldEqual, 1)
			//cv.So(len(diffs2.Both), cv.ShouldEqual, 15)
			cv.So(diffs2.OnlyA[0].ChunkNumber, cv.ShouldEqual, 0)
			cv.So(diffs2.OnlyB[0].ChunkNumber, cv.ShouldEqual, 0)

			// lets try putting 2 bytes at the end instead:
			data3 := append(data, []byte{0xf3, 0xee}...)
			h3, err := SummarizeBytesInCDCHashes(host, path+".postpend2bytes", data3, modTime)
			panicOn(err)

			diffs3 := h3.Diff(h)
			//vv("diffs3 = '%s'", diffs3)
			//cv.So(h3.NumChunks, cv.ShouldEqual, 16)
			//cv.So(len(h3.Chunks), cv.ShouldEqual, 16)
			cv.So(len(diffs3.OnlyA), cv.ShouldEqual, 1)
			cv.So(len(diffs3.OnlyB), cv.ShouldEqual, 1)
			//cv.So(len(diffs3.Both), cv.ShouldEqual, 15)
			//cv.So(diffs3.OnlyA[0].ChunkNumber, cv.ShouldEqual, 15)
			//cv.So(diffs3.OnlyB[0].ChunkNumber, cv.ShouldEqual, 15)
		*/
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
		if true {
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
		localState, err := SummarizeFileInCDCHashes(host, localPath)
		panicOn(err)

		// step2 request: get diffs from what we have.
		readerAckOV := &RsyncStep2_ReaderAcksOverview{
			ReaderMatchesSenderAllGood: false,
			SenderPath:                 remotePath,
			ReaderHashes:               localState,
		}

		senderDeltas := &RsyncStep3A_SenderProvidesData{} // response

		err = cli.Call("RsyncNode.Step3_SenderProvidesData", readerAckOV, senderDeltas, nil)
		panicOn(err) // reading body msgp: attempted to decode type "ext" with method for "map"

		vv("senderDeltas = '%v'", senderDeltas)

		plan := senderDeltas.Chunks // the plan follow remote template, our target.
		vv("plan = '%v'", plan)
		local := localState.Chunks   // our origin or starting point.
		localMap := getCryMap(local) // pre-index them for the update.

		err = UpdateLocalWithRemoteDiffs(local.Path, localMap, plan)
		panicOn(err)

		if !fileExists(local.Path) {
			panic("file should have been written locally now!")
		}
		difflen := compareFilesDiffLen(local.Path, remotePath)
		cv.So(difflen, cv.ShouldEqual, 0)

	})
}
