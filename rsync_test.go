package rpc25519

import (
	"fmt"
	"os"
	"testing"
	"time"

	cv "github.com/glycerine/goconvey/convey"
)

func Test201_rsync_style_hash_generation(t *testing.T) {

	cv.Convey("rsync.go SummarizeFileInCDCHashes() should generate CDC FastCDC and/or UltraCDC hashes for a file", t, func() {
		host := "localhost"
		path := "testdata/blob977k"

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
	})
}

func Test210_client_sends_file_over_rsync(t *testing.T) {

	cv.Convey("using our rsync-like-protocol, the client should be able to send a file to the server and only end up sending the small parts that have changed.", t, func() {

		// set up a server and a client.

		cfg := NewConfig()
		cfg.TCPonly_no_TLS = false

		cfg.CompressionOff = true

		cfg.ServerAddr = "127.0.0.1:0"
		srv := NewServer("srv_rsync_test210", cfg)

		serverAddr, err := srv.Start()
		panicOn(err)
		defer srv.Close()

		vv("server Start() returned serverAddr = '%v'", serverAddr)

		//srv.RegisterBistreamFunc("RsyncServerSide", srv.RsyncServerSide)

		srvRsyncNode := &RsyncNode{}
		panicOn(srv.Register(srvRsyncNode))

		cfg.ClientDialToHostPort = serverAddr.String()
		cli, err := NewClient("cli_rsync_test210", cfg)
		panicOn(err)
		err = cli.Start()
		panicOn(err)

		defer cli.Close()

		host := cli.LocalAddr()
		path := "/Users/jaten/go/src/github.com/glycerine/rpc25519/cry100mb"
		step0request, err := RsyncCliWantsToReadRemotePath(host, path) // request, step 1
		panicOn(err)

		senderOV := &RsyncStep1_SenderOverview{} // response, step 1
		err = cli.Call("RsyncNode.Step1_SenderOverview", step0request, senderOV, nil)

		vv("got senderOV = '%#v'", senderOV)

		if senderOV.ErrString != "" {
			panic(senderOV.ErrString)
		}
		if senderOV.SenderLenBytes == 0 {
			panic(fmt.Sprintf("remote file is 0 bytes: '%v'", path))
		}

		var rsyncHashes *RsyncHashes
		if fileExists(path) {
			rsyncHashes, err = SummarizeFileInCDCHashes(host, path)
			panicOn(err)
		}

		// request:
		readerAckOV := &RsyncStep2_ReaderAcksOverview{
			ReaderMatchesSenderAllGood: false,
			SenderPath:                 senderOV.SenderPath,
			ReaderHashes:               rsyncHashes,
		}

		senderDeltas := &RsyncStep3A_SenderProvidesDeltas{} // response

		err = cli.Call("RsyncNode.Step3_SenderProvidesDelta", readerAckOV, senderDeltas, nil)
		panicOn(err) // reading body msgp: attempted to decode type "ext" with method for "map"

		vv("senderDeltas = '%#v'", senderDeltas)
	})
}
