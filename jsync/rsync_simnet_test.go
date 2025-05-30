package jsync

import (
	"bytes"
	//cryrand "crypto/rand"
	"fmt"
	"io"
	mathrand2 "math/rand/v2"
	"os"
	"testing"
	"time"

	cv "github.com/glycerine/goconvey/convey"
	rpc "github.com/glycerine/rpc25519"
	//"github.com/glycerine/rpc25519/hash"
)

func Test710_client_gets_new_file_over_rsync_twice(t *testing.T) {

	bubbleOrNot(func() {

		cv.Convey("using our rsync-like-protocol, the client, lacking a file, should be able to fetch it from the server. The second time fetching the same should be very fast because of chunking and hash comparisons in the rsync-like protocol", t, func() {

			// create a test file
			N := 1
			remotePath := fmt.Sprintf("charand%vmb", N)
			testfd, err := os.Create(remotePath)
			panicOn(err)
			slc := make([]byte, 1<<20) // 1 MB slice

			// deterministic pseudo-random numbers as data.
			var seed [32]byte
			seed[1] = 2
			generator := mathrand2.NewChaCha8(seed)

			// random or zeros?
			allZeros := false
			if allZeros {
				// slc is already ready with all 0.
			} else {
				generator.Read(slc)
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

			// delete any old leftover test file from before.
			os.Remove(localPath)

			// set up a server and a client.

			cfg := rpc.NewConfig()
			cfg.TCPonly_no_TLS = true
			cfg.CompressionOff = true
			cfg.UseSimNet = true

			cfg.ServerAddr = "127.0.0.1:0"
			srv := rpc.NewServer("srv_rsync_test210", cfg)

			serverAddr, err := srv.Start()
			panicOn(err)
			defer srv.Close()

			vv("server Start() returned serverAddr = '%v'", serverAddr)

			if faketime {
				simnet := cfg.GetSimnet()
				defer simnet.Close()
			}

			//srv.RegisterBistreamFunc("RsyncServerSide", srv.RsyncServerSide)

			srvRsyncNode := &RsyncNode{}
			panicOn(srv.Register(srvRsyncNode))

			cfg.ClientDialToHostPort = serverAddr.String()
			cli, err := rpc.NewClient("cli_rsync_test710", cfg)
			panicOn(err)
			err = cli.Start()
			panicOn(err)

			defer cli.Close()

			// summarize our local file contents (empty here, but in general).
			host := "localhost"
			//localPrecis, local, err := SummarizeFileInCDCHashes(host, localPath, true, true)
			localPrecis, local, err := GetHashesOneByOne(host, localPath)
			panicOn(err)

			// get diffs from what we have. We send a light
			// request (one without Data attached, just hashes);
			// but since we send to RequestLatest, we'll get back
			// a Data heavy payload; possibly requiring
			// a stream.
			light := &LightRequest{
				SenderPath:   remotePath,
				ReaderPrecis: localPrecis,
				ReaderChunks: local,
			}

			senderDeltas := &HeavyPlan{} // response

			err = cli.Call("RsyncNode.RequestLatest", light, senderDeltas, nil)
			panicOn(err) // reading body msgp: attempted to decode type "ext" with method for "map"

			//vv("senderDeltas = '%v'", senderDeltas)

			plan := senderDeltas.SenderPlan // the plan follow remote template, our target.
			//vv("plan = '%v'", plan)
			//local is our origin or starting point.
			localMap := getCryMap(local) // pre-index them for the update.

			// had to do a full file transfer for missing file.
			cv.So(plan.DataPresent(), cv.ShouldEqual, 1048576)
			cv.So(plan.FileSize, cv.ShouldEqual, 1048576)

			err = UpdateLocalWithRemoteDiffs(local.Path, localMap, plan, senderDeltas.SenderPrecis)
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
			//localPrecis, local, err = SummarizeFileInCDCHashes(host, localPath, true, true)
			localPrecis, local, err = GetHashesOneByOne(host, localPath)
			panicOn(err)

			clearLocal := local.CloneWithClearData()

			//  get diffs from what we have.
			light = &LightRequest{
				SenderPath:   remotePath,
				ReaderPrecis: localPrecis,
				ReaderChunks: clearLocal,
			}

			senderDeltas = &HeavyPlan{} // response

			err = cli.Call("RsyncNode.RequestLatest", light, senderDeltas, nil)
			panicOn(err) // reading body msgp: attempted to decode type "ext" with method for "map"

			//vv("senderDeltas = '%v'", senderDeltas)

			plan = senderDeltas.SenderPlan // the plan follow remote template, our target.
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

			// the data is attached to the local2 Chunks .Data element.
			// We had to read it in, so might as well keep it until we
			// know we want to discard it, which the GetPlan() below will do
			// if we tell it too.
			localPrecis2, local2, err := SummarizeFileInCDCHashes(host, pre2path, true, true)
			panicOn(err)

			// generate a plan to update the remote server, based on
			// the diff that we just made.

			bs := NewBlobStore()

			plan.ClearData()
			cv.So(plan.DataPresent(), cv.ShouldEqual, 0)
			remoteWantsUpdate := plan

			dropPlanData := true
			plan2 := bs.GetPlanToUpdateFromGoal(remoteWantsUpdate, local2, dropPlanData, false)

			// verify minimal changes being sent
			if allZeros {
				cv.So(plan2.DataChunkCount(), cv.ShouldEqual, 2)
				cv.So(plan2.DataPresent(), cv.ShouldEqual, 147458)

				vv("out of %v chunks, in a %v byte long file, these were updated: '%v'",
					len(plan2.Chunks), plan2.FileSize, plan2.DataFilter())

			} else {
				// random
				cv.So(plan2.DataChunkCount(), cv.ShouldEqual, 1)

				vv("out of %v chunks, in a %v byte long file, these were updated: '%v'",
					len(plan2.Chunks), plan2.FileSize, plan2.DataFilter())

				// this varies because the data is random:
				//cv.So(plan2.DataPresent(), cv.ShouldEqual, 11796)
			}

			pushMe := &HeavyPlan{
				SenderPath:   remotePath,
				SenderPrecis: localPrecis2,
				SenderPlan:   plan2,
			}
			_ = pushMe

			gotBack := &HeavyPlan{} // they might update us too... :) ignore for now.
			err = cli.Call("RsyncNode.AcceptHeavy", pushMe, gotBack, nil)
			panicOn(err)

			// confirm it happened.
			difflen = compareFilesDiffLen(pre2path, remotePath)
			cv.So(difflen, cv.ShouldEqual, 0)
		})
	})
}

// efficient on big files with small deltas

func Test777_big_files_with_small_changes(t *testing.T) {
	//return

	bubbleOrNot(func() {

		cv.Convey("using our rsync-like-protocol, rectifying a small diff in a big file should be efficient. Let the local have a small difference, and sync it to the remote 'template'", t, func() {

			// template, match to this:
			//remotePath := "Ubuntu_24.04_VB_LinuxVMImages.COM.vdi"

			// the first ~ 1MB of Ub
			//remotePath := "repro.orig.1098290"
			remotePath := "10mb.ub" // has 27 out of 646. why not just 2?
			//remotePath := "cry.1098290" // compare/contrast

			if !fileExists(remotePath) {
				vv("warning: skipping test b/c '%v' test file does not exist.", remotePath)
				return
			}

			// smaller file while looking at hashes directly.
			//remotePath := "cry2mb"
			//remotePath := "cry8mb"
			//remotePath := "cry16mb"
			vv("template (goal) remotePath='%v'", remotePath)

			localPath := remotePath + ".local"
			vv("adjust local to be like remote: localPath = '%v'", localPath)

			// after update, leave final local in:
			localPathFinal := remotePath + ".final"

			// delete any old leftover test file from before.
			os.Remove(localPath)
			os.Remove(localPathFinal)

			in, err := os.Open(remotePath)
			panicOn(err)

			out, err := os.Create(localPath)
			panicOn(err)
			fmt.Fprintf(out, "hello world!")
			_, err = io.Copy(out, in)
			panicOn(err)
			out.Close()
			in.Close()

			// set up a server and a client.

			cfg := rpc.NewConfig()
			cfg.TCPonly_no_TLS = true
			cfg.CompressionOff = true
			cfg.UseSimNet = true

			cfg.ServerAddr = "127.0.0.1:0"
			srv := rpc.NewServer("srv_rsync_test777", cfg)

			serverAddr, err := srv.Start()
			panicOn(err)
			defer srv.Close()

			if faketime {
				simnet := cfg.GetSimnet()
				defer simnet.Close()
			}

			// about 4 seconds to copy.
			vv("copy done. server Start() returned serverAddr = '%v'", serverAddr)

			srvRsyncNode := &RsyncNode{}
			panicOn(srv.Register(srvRsyncNode))

			cfg.ClientDialToHostPort = serverAddr.String()
			cli, err := rpc.NewClient("cli_rsync_test777", cfg)
			panicOn(err)
			err = cli.Start()
			panicOn(err)

			defer cli.Close()

			// summarize our local file contents (empty here, but in general).
			host := "localhost"
			_ = host

			t0 := time.Now()

			//wantsChunks := true
			//keepData := false

			parallel := true

			var localPrecis *FilePrecis
			var wantsUpdate *Chunks

			// taker does
			if parallel {
				fmt.Printf("first ChunkFile: \n")
				localPrecis, wantsUpdate, err = ChunkFile(localPath)
				panicOn(err)
				// 2.5 sec.
			} else {
				localPrecis, wantsUpdate, err = GetHashesOneByOne(host, localPath)
				//localPrecis, wantsUpdate, err = SummarizeFileInCDCHashes(host, localPath, wantsChunks, keepData)
				panicOn(err)
				// 14.335789s
			}
			if false {
				// debug
				_, debugser, _ := GetHashesOneByOne(host, localPath) // debug todo remove
				vv("for reference, here are the serial cuts: ")
				showEachSegment(0, debugser.Chunks)
				vv("why are the parallel chunks differenent?: ")
				showEachSegment(0, wantsUpdate.Chunks)
			}
			vv("elap first SummarizeFileInCDCHashes = '%v'", time.Since(t0))
			_ = localPrecis

			localMap := getCryMap(wantsUpdate) // pre-index them for the update.

			t2 := time.Now()

			//goalPrecis, templateChunks, err := GetHashesOneByOne(rpc.Hostname, remotePath) // no data, just chunks. read data directly from file below.

			var goalPrecis *FilePrecis
			var templateChunks *Chunks

			if parallel {
				fmt.Printf("second ChunkFile: \n")
				goalPrecis, templateChunks, err = ChunkFile(remotePath)
				// 2.4 sec.
			} else {
				goalPrecis, templateChunks, err = GetHashesOneByOne(host, remotePath)
				//goalPrecis, templateChunks, err = SummarizeFileInCDCHashes(host, remotePath, wantsChunks, keepData)
				// 11.1s, or 13.34s, so long!
			}

			vv("templateChunks done after %v", time.Since(t2))

			_ = goalPrecis

			const dropPlanData = true // ignored when usePlaceHolders is true.
			const usePlaceHolders = true

			// new: placeholderPlan has a single data byte in Chunk.Data
			// to flag us to read the actual data from disk and then
			// send it over the wire. This helps keep memory footprint low.

			t3 := time.Now()
			bs := NewBlobStore() // make persistent state, at some point.
			oneByteMarkedPlan := bs.GetPlanToUpdateFromGoal(wantsUpdate, templateChunks, dropPlanData, usePlaceHolders)

			if oneByteMarkedPlan.DataChunkCount() != 2 {
				//t.Fatalf("oneByteMarkedPlan.DataChunkCount() = %v, why not 2 ??", oneByteMarkedPlan.DataChunkCount())
			}

			// 360ms. plan.DataChunkCount 2 out of 664047; DataPresent() = 75_740 bytes
			// parallel: 27486 count, arg.
			vv("elap to GetPlanToUpdateFromGoal = '%v'; plan.DataChunkCount()= %v out of %v;  oneByteMarkedPlan.DataPresent() = %v bytes", time.Since(t3), oneByteMarkedPlan.DataChunkCount(), len(oneByteMarkedPlan.Chunks), oneByteMarkedPlan.DataPresent())

			// get rid of the 1 byte place holders; fill in
			// with live data

			// from giver.go:801

			bytesFromDisk := 0
			t4 := time.Now()
			fd, err := os.Open(remotePath)
			panicOn(err)
			n := len(oneByteMarkedPlan.Chunks)
			for i := 0; i < n; i++ {
				//if i%10000 == 0 {
				//vv("on chunk %v of of %v", i, n)
				//}
				next := oneByteMarkedPlan.Chunks[i]
				if len(next.Data) > 0 {
					// we have our 1 byte flag.
					// need to read it from file
					_, err := fd.Seek(int64(next.Beg), 0)
					panicOn(err)

					amt := next.Endx - next.Beg
					next.Data = make([]byte, amt)
					_, err = io.ReadFull(fd, next.Data)
					panicOn(err)
					bytesFromDisk += amt
				}
			}
			//      75_740 bytes with traditional single threaded chunking.
			// 325_033_867 with parallel, initially. arg!
			// 325_033_867 with min 1 byte chunk; no change. (4.7% of orig 6.7GB)
			//  14_264_136 bytes with 1<<24 or 16MB segments (0.2% of orig)
			//   3_964_395 bytes with 1<<26 or 64MB segments per goro. (0.04% of orig)
			//  because each segment the first and last is messed up, of course.
			vv("bytesFromDisk = %v bytes, deltas from remote template file (want this to be as small as possible). elap = %v", bytesFromDisk, time.Since(t4))

			plan := oneByteMarkedPlan
			// see the
			// case OpRsync_HeavyDiffChunksEnclosed
			// handling in taker.go

			t5 := time.Now()
			//err = UpdateLocalWithRemoteDiffs(localPath, localMap, plan, goalPrecis)

			err = UpdateLocalFileWithRemoteDiffs(localPathFinal, localPath, localMap, plan, goalPrecis)
			panicOn(err)

			// localPathFinal has the file made to match remotePath.
			// normally we would now rename localPathFinal onto localPath,
			// and be done.

			// 7.94s
			vv("elap to UpdateLocalWithRemoteDiffs = '%v'", time.Since(t5))

			difflen := compareFilesDiffLen(localPathFinal, remotePath)
			cv.So(difflen, cv.ShouldEqual, 0)
		})
	})
}

func Test788_rle_zeros_encoded(t *testing.T) {

	bubbleOrNot(func() {

		cv.Convey("using our rsync-like-protocol, all zero files should be efficiently run-length-encoded. Can we get an all zero 10MB file down to one chunk?", t, func() {

			remotePath := "10mb.zeros"
			vv("template (goal) remotePath='%v'", remotePath)

			testfd, err := os.Create(remotePath)
			panicOn(err)
			slc := make([]byte, 1<<20) // 1 MB slice
			N := 10
			for range N {
				_, err = testfd.Write(slc)
				panicOn(err)
			}
			testfd.Close()

			localPath := remotePath + ".local"
			vv("adjust local to be like remote: localPath = '%v'", localPath)

			// after update, leave final local in:
			localPathFinal := remotePath + ".final"

			// delete any old leftover test file from before.
			os.Remove(localPath)
			os.Remove(localPathFinal)

			in, err := os.Open(remotePath)
			panicOn(err)

			out, err := os.Create(localPath)
			panicOn(err)
			fmt.Fprintf(out, "hello world!")
			_, err = io.Copy(out, in)
			panicOn(err)
			out.Close()
			in.Close()

			// set up a server and a client.

			cfg := rpc.NewConfig()
			cfg.TCPonly_no_TLS = true
			cfg.CompressionOff = true
			cfg.UseSimNet = true

			cfg.ServerAddr = "127.0.0.1:0"
			srv := rpc.NewServer("srv_rsync_test788", cfg)

			serverAddr, err := srv.Start()
			panicOn(err)
			defer srv.Close()

			if faketime {
				simnet := cfg.GetSimnet()
				defer simnet.Close()
			}

			// about 4 seconds to copy.
			vv("copy done. server Start() returned serverAddr = '%v'", serverAddr)

			srvRsyncNode := &RsyncNode{}
			panicOn(srv.Register(srvRsyncNode))

			cfg.ClientDialToHostPort = serverAddr.String()
			cli, err := rpc.NewClient("cli_rsync_test788", cfg)
			panicOn(err)
			err = cli.Start()
			panicOn(err)

			defer cli.Close()

			// summarize our local file contents (empty here, but in general).
			host := "localhost"
			_ = host

			t0 := time.Now()

			//wantsChunks := true
			//keepData := false

			parallel := true

			var localPrecis *FilePrecis
			var wantsUpdate *Chunks

			// only parallel has the RLE0; impl to start.
			if parallel {
				fmt.Printf("first ChunkFile: \n")
				localPrecis, wantsUpdate, err = ChunkFile(localPath)
				panicOn(err)
				// 2.5 sec.
			} else {
				localPrecis, wantsUpdate, err = GetHashesOneByOne(host, localPath)
				//localPrecis, wantsUpdate, err = SummarizeFileInCDCHashes(host, localPath, wantsChunks, keepData)
				panicOn(err)
				// 14.335789s
			}
			//if false {
			//	// debug
			//	_, debugser, _ := GetHashesOneByOne(host, localPath) // debug todo remove
			//	vv("for reference, here are the serial cuts: ")
			//	showEachSegment(0, debugser.Chunks)

			vv("parallel chunks: ")
			showEachSegment(0, wantsUpdate.Chunks)

			nchunk := len(wantsUpdate.Chunks)
			if nchunk != 2 {
				t.Fatalf("ideally we can compress all 10MB of zeros to 2 chunks... with coalescing; not %v", nchunk) // not 73
			}

			//}
			vv("elap first SummarizeFileInCDCHashes = '%v'", time.Since(t0))
			_ = localPrecis

			localMap := getCryMap(wantsUpdate) // pre-index them for the update.

			t2 := time.Now()

			//goalPrecis, templateChunks, err := GetHashesOneByOne(rpc.Hostname, remotePath) // no data, just chunks. read data directly from file below.

			var goalPrecis *FilePrecis
			var templateChunks *Chunks

			if parallel {
				fmt.Printf("second ChunkFile: \n")
				goalPrecis, templateChunks, err = ChunkFile(remotePath)
				// 2.4 sec.
			} else {
				goalPrecis, templateChunks, err = GetHashesOneByOne(host, remotePath)
				//goalPrecis, templateChunks, err = SummarizeFileInCDCHashes(host, remotePath, wantsChunks, keepData)
				// 11.1s, or 13.34s, so long!
			}

			vv("templateChunks done after %v", time.Since(t2))

			_ = goalPrecis

			const dropPlanData = true // ignored when usePlaceHolders is true.
			const usePlaceHolders = true

			// new: placeholderPlan has a single data byte in Chunk.Data
			// to flag us to read the actual data from disk and then
			// send it over the wire. This helps keep memory footprint low.

			t3 := time.Now()
			bs := NewBlobStore() // make persistent state, at some point.
			oneByteMarkedPlan := bs.GetPlanToUpdateFromGoal(wantsUpdate, templateChunks, dropPlanData, usePlaceHolders)

			//if oneByteMarkedPlan.DataChunkCount() != 2 {
			//	t.Fatalf("oneByteMarkedPlan.DataChunkCount() = %v, why not 2 ??", oneByteMarkedPlan.DataChunkCount())
			//}

			// 360ms. plan.DataChunkCount 2 out of 664047; DataPresent() = 75_740 bytes
			// parallel: 27486 count, arg.
			vv("elap to GetPlanToUpdateFromGoal = '%v'; plan.DataChunkCount()= %v out of %v;  oneByteMarkedPlan.DataPresent() = %v bytes", time.Since(t3), oneByteMarkedPlan.DataChunkCount(), len(oneByteMarkedPlan.Chunks), oneByteMarkedPlan.DataPresent())

			// get rid of the 1 byte place holders; fill in
			// with live data

			// from giver.go:801

			bytesFromDisk := 0
			t4 := time.Now()
			fd, err := os.Open(remotePath)
			panicOn(err)
			n := len(oneByteMarkedPlan.Chunks)
			for i := 0; i < n; i++ {
				//if i%10000 == 0 {
				//vv("on chunk %v of of %v", i, n)
				//}
				next := oneByteMarkedPlan.Chunks[i]
				if len(next.Data) > 0 {
					// we have our 1 byte flag.
					// need to read it from file
					_, err := fd.Seek(int64(next.Beg), 0)
					panicOn(err)

					amt := next.Endx - next.Beg
					next.Data = make([]byte, amt)
					_, err = io.ReadFull(fd, next.Data)
					panicOn(err)
					bytesFromDisk += amt
				}
			}
			vv("bytesFromDisk = %v bytes, deltas from remote template file (want this to be as small as possible). elap = %v", bytesFromDisk, time.Since(t4))

			plan := oneByteMarkedPlan
			// see the
			// case OpRsync_HeavyDiffChunksEnclosed
			// handling in taker.go

			t5 := time.Now()
			//err = UpdateLocalWithRemoteDiffs(localPath, localMap, plan, goalPrecis)

			err = UpdateLocalFileWithRemoteDiffs(localPathFinal, localPath, localMap, plan, goalPrecis)
			panicOn(err)

			// localPathFinal has the file made to match remotePath.
			// normally we would now rename localPathFinal onto localPath,
			// and be done.

			vv("elap to UpdateLocalWithRemoteDiffs = '%v'", time.Since(t5))

			difflen := compareFilesDiffLen(localPathFinal, remotePath)
			cv.So(difflen, cv.ShouldEqual, 0)
		})
	})
}
