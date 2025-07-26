package jsync

import (
	"bytes"
	//cryrand "crypto/rand"
	"fmt"
	"io"
	mathrand2 "math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	cv "github.com/glycerine/goconvey/convey"
	rpc "github.com/glycerine/rpc25519"
	//"github.com/glycerine/rpc25519/hash"

	"github.com/glycerine/rpc25519/jsync/sparsified"
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
		a, achunks, err := SummarizeFileInCDCHashes(host, path, true)

		cv.So(a.FileOwner != "", cv.ShouldBeTrue)

		//vv("scan of file gave chunks: '%v'", achunks)
		//cv.So(len(achunks.Chunks), cv.ShouldEqual, 24) // blob977k / FastCDC
		//cv.So(len(achunks.Chunks), cv.ShouldEqual, 16) // blob977k / UltraCDC

		// now alter the data by prepending 2 bytes
		data2 := append([]byte{0x24, 0xff}, data...)
		path2 := path + ".prepend2bytes"
		fd2, err2 := os.Create(path2)
		panicOn(err2)
		defer fd2.Close()
		defer os.Remove(path2)
		_, err = fd2.Write(data2)
		panicOn(err)
		fd2.Sync()
		fi2, err2 := fd2.Stat()
		panicOn(err2)
		//_, bchunks, err := SummarizeBytesInCDCHashes(host, path2, fd2, modTime, true, int64(len(data2)))
		_, bchunks, err := SummarizeBytesInCDCHashes(host, path2, fd2, modTime, true, fi2)
		panicOn(err)

		onlyA, onlyB, both := Diff(achunks, bchunks)
		cv.So(len(onlyA), cv.ShouldEqual, 1)
		cv.So(len(onlyB), cv.ShouldEqual, 1)
		_ = both
		//cv.So(len(both), cv.ShouldEqual, 23) // FastCDC
		//cv.So(len(both), cv.ShouldEqual, 15) // UltraCDC

		// lets try putting 2 bytes at the end instead:
		data3 := append(data, []byte{0xf3, 0xee}...)
		path3 := path + ".postpend2bytes"
		fd3, err3 := os.Create(path3)
		panicOn(err3)
		defer fd3.Close()
		defer os.Remove(path3)
		_, err = fd3.Write(data3)
		panicOn(err)
		fd3.Sync()
		fi3, err3 := fd3.Stat()
		_, bchunks, err = SummarizeBytesInCDCHashes(host, path3, fd3, modTime, true, fi3)
		panicOn(err)

		onlyA, onlyB, both = Diff(achunks, bchunks)

		cv.So(len(onlyA), cv.ShouldEqual, 1)
		cv.So(len(onlyB), cv.ShouldEqual, 1)
		//cv.So(len(both), cv.ShouldEqual, 23) // FastCDC
		//cv.So(len(both), cv.ShouldEqual, 15) // UltraCDC

	})
}

func Test210_client_gets_new_file_over_rsync_twice(t *testing.T) {

	cv.Convey("using our rsync-like-protocol, the client, lacking a file, should be able to fetch it from the server. The second time fetching the same should be very fast because of chunking and hash comparisons in the rsync-like protocol", t, func() {

		for j := range 2 {

			// random or zeros?
			var allZeros bool
			switch j {
			case 0:
				allZeros = true
			default:
				allZeros = false // random data
			}

			// create a test file
			N := 1
			remotePath := fmt.Sprintf("charand%vmb", N)
			testfd, err := os.Create(remotePath)
			panicOn(err)
			slc := make([]byte, 1<<19) // 0.5 MB slice

			// deterministic pseudo-random numbers as data.
			var seed [32]byte
			seed[1] = 2
			generator := mathrand2.NewChaCha8(seed)

			if allZeros {
				// slc is already ready with all 0.
			} else {
				generator.Read(slc)
				vv("first 10 of slc random = '%v'", string(slc[:10]))
			}
			for range N {
				_, err = testfd.Write(slc)
				panicOn(err)
			}
			// add a big sparse hole at the end of the remote path.
			err = testfd.Truncate(1 << 20)
			panicOn(err)

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
			// 1048576 -> 538512 b/c much more aggressive RLE0;
			// why are linux and darwin different?
			if allZeros {
				cv.So(plan.DataPresent(), cv.ShouldEqual, 0)
			} else {
				if runtime.GOOS == "darwin" {
					cv.So(plan.DataPresent(), cv.ShouldEqual, 538512) // darwin
				} else {
					cv.So(plan.DataPresent(), cv.ShouldEqual, 524288) // linux
				}
			}
			//cv.So(plan.DataPresent(), cv.ShouldEqual, 1048576)
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
			// why darwin and linux different?
			if runtime.GOOS == "darwin" {
				cv.So(plan.DataPresent(), cv.ShouldEqual, 0) // darwin
			} else {
				if allZeros {
					cv.So(plan.DataPresent(), cv.ShouldEqual, 0) // linux
				} else {
					cv.So(plan.DataPresent(), cv.ShouldEqual, 2160) // linux
				}
			}

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
			localPrecis2, local2, err := SummarizeFileInCDCHashes(host, pre2path, true)
			panicOn(err)

			// generate a plan to update the remote server, based on
			// the diff that we just made.

			bs := NewBlobStore()

			plan.ClearData()
			cv.So(plan.DataPresent(), cv.ShouldEqual, 0)
			remoteWantsUpdate := plan

			dropPlanData := true
			plan2 := bs.GetPlanToUpdateFromGoal(remoteWantsUpdate, local2, dropPlanData, false)

			//vv("plan2 = '%v'", plan2)
			// verify minimal changes being sent
			if allZeros {
				if runtime.GOOS == "darwin" {
					cv.So(plan2.DataChunkCount(), cv.ShouldEqual, 1)
					cv.So(plan2.DataPresent(), cv.ShouldEqual, 16384)
				} else {
					cv.So(plan2.DataChunkCount(), cv.ShouldEqual, 1)
					//cv.So(plan2.DataChunkCount(), cv.ShouldEqual, 2)
					cv.So(plan2.DataPresent(), cv.ShouldEqual, 16384)
					//cv.So(plan2.DataPresent(), cv.ShouldEqual, 147458)
				}

				vv("out of %v chunks, in a %v byte long file, these were updated: '%v'",
					len(plan2.Chunks), plan2.FileSize, plan2.DataFilter())

			} else {
				// random
				if runtime.GOOS == "darwin" {
					cv.So(plan2.DataChunkCount(), cv.ShouldEqual, 1) // darwin
				} else {
					cv.So(plan2.DataChunkCount(), cv.ShouldEqual, 2) // linux
				}

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

		} // end j over allZeros/not.
	})
}

func Test300_incremental_chunker_matches_batch(t *testing.T) {

	cv.Convey("the new incremental GetHashesOneByOne should return the same chunks as the earlier batch version SummarizeFileInCDCHashes", t, func() {

		host := ""
		path := "test300.dat"

		// create a test file
		//N := 1000 // green at N=1000 with ultracdc; takes 3 sec. for all zeros. hangs! on random with -race, race detector is really slow.

		// incremental one-at-a-time is actually a bit faster than batch.
		//rsync_test.go:327 2025-01-23 16:26:39.56 -0600 CST n0 = 89001; took 2.8s
		//rsync_test.go:332 2025-01-23 16:26:41.465 -0600 CST n1 = 89001; took 1.9s

		//rsync_test.go:337 2025-01-23 18:40:58.579 -0600 CST n1 = 89001; took 2.45s
		//rsync_test.go:343 2025-01-23 18:41:01.381 -0600 CST n0 = 89001; took 2.80s
		//rsync.go:1114 2025-01-23 18:41:01.381 -0600 CST using bufsz = 1073741824
		//rsync_test.go:350 2025-01-23 18:41:03.718 -0600 CST n2 = 89001; took 2.33s

		//N := 200 // 200 takes 13s, or 6.5 seconds each. maybe just takes awhile?
		N := 20
		// 400 takes 12.6 sec each for 400MB random file on ultracdc. under -race

		// with N := 2000
		// one at a time, gets better pipelining overlap probably:
		//rsync_test.go:354 2025-01-23 20:27:05.581 -0600 CST n2 = 178001; took 3.81s
		// read whole file at once:
		//rsync_test.go:365 2025-01-23 20:27:15.265 -0600 CST n0 = 178001; took 5.53s
		// 2025-July-12: update: after re-writing to use data 1M buffer,
		// whole file at once is faster: 2.921026401s
		// vs one-at-a-time: 3.163196976s

		testfd, err := os.Create(path)
		panicOn(err)
		slc := make([]byte, 1<<20) // 1 MB slice

		// deterministic pseudo-random numbers as data.
		var seed [32]byte
		seed[1] = 2
		generator := mathrand2.NewChaCha8(seed)

		// random or zeros?
		//allZeros := true
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
		vv("created N = %v MB test file in remotePath='%v'.", N, path)
		defer os.Remove(path) // cleanup test.

		// lightly is only a little faster than reading the whole file at once.
		// rsync_test.go:333 2025-01-23 18:12:11.436 -0600 CST n1 = 71201; took 24.318241895s
		// rsync_test.go:339 2025-01-23 18:12:40.268 -0600 CST n0 = 71201; took 28.831323864s

		show := func(chunks *Chunks, label string, n int) {
			fmt.Printf("\n%v\n", label)
			for i := range n {
				chnk := chunks.Chunks[i]
				beg := chnk.Beg
				endx := chnk.Endx
				fmt.Printf("[%03d] Beg: %v,  Endx: %v\n", i, beg, endx)
			}
		}

		// verify precis alone matches the others
		precisAlone, err := GetPrecis(host, path)

		vv("starting on one-at-a-time...")
		t1 := time.Now()
		precis1, chunks1, err := GetHashesOneByOne(host, path)
		panicOn(err)
		n1 := len(chunks1.Chunks)
		vv("one-at-a-time n2 = %v; took %v", n1, time.Since(t1))

		if !precisAlone.Equal(precis1) {
			panic(fmt.Sprintf("precisAlone (%v) != precis1 (%v)", precisAlone, precis1))
		}

		t0 := time.Now()
		precis0, chunks0, err := SummarizeFileInCDCHashes(host, path, false)
		panicOn(err)
		n0 := len(chunks0.Chunks)
		vv("full-file at once n0 = %v; took %v", n0, time.Since(t0))

		if !precisAlone.Equal(precis0) {
			panic(fmt.Sprintf("precisAlone (%v) != precis0 (%v)", precisAlone, precis0))
		}

		var x string
		for i, chnk := range chunks0.Chunks {
			beg := chnk.Beg
			endx := chnk.Endx

			beg1 := chunks1.Chunks[i].Beg
			if beg1 != beg {
				x = fmt.Sprintf("<< vs incr.Beg: %v ; incr Beg DIFFERS! on i = %v", beg1, i)
				vv(x)
				show(chunks0, "chunks0   full file, Cutpoints on it.", i+1)
				show(chunks1, "chunks1  one at a time.", i+1)
				panic("beg mismatch")
			}
			endx1 := chunks1.Chunks[i].Endx
			if endx1 != endx {
				x += fmt.Sprintf(" << vs incr.Endx: %v ; incr Endx DIFFERS! on i = %v.  chunks0 has '%v'; chunks1 has '%v'\n", endx1, i, chunks0.Chunks[i], chunks1.Chunks[i])
				vv(x)
				panic("endx mismatch")
			}
		}
		if n1 != n0 {
			t.Fatalf("error: Lightly got %v, but batch got %v", n1, n0)
		}

		_ = precis0
		_ = precis1
		cv.So(precis0.FileCry, cv.ShouldEqual, precis1.FileCry)

		vv("precis1 = '%#v'", precis1)
		vv("precis0 = '%#v'", precis0)
		cv.So(precis0, cv.ShouldResemble, precis1)
	})
}

// GetHashes WAS returning a different (smaller/truncated) set of
// hashes it seems. test more rigourously. Now fixed.
func Test302_incremental_chunker_matches_batch_bigger(t *testing.T) {

	return // we don't want to check in path, very big file for testing.

	//cv.Convey("the new incremental GetHashesOneByOne should return the same chunks as the earlier batch version SummarizeFileInCDCHashes", t, func() {

	host := ""
	//path := "Ubuntu_24.04_VB_LinuxVMImages.COM.vdi.rog.long"
	path := "Ubuntu_24.04_VB_LinuxVMImages.COM.vdi"
	//path := "Ubuntu_24.04_VB_LinuxVMImages.COM.vdi.aorus.short"

	show := func(chunks *Chunks, label string, i int) {
		fmt.Printf("\n%v\n", label)
		//for i := range n {
		chnk := chunks.Chunks[i]
		beg := chnk.Beg
		endx := chnk.Endx
		fmt.Printf("[%03d] Beg: %v,  Endx: %v  Cry: %v\n", i, beg, endx, chnk.Cry)
		//}
	}

	// verify precis alone matches the others
	precisAlone, err := GetPrecis(host, path)

	vv("starting on one-at-a-time...")
	t1 := time.Now()
	precis1, chunks1, err := GetHashesOneByOne(host, path)
	panicOn(err)
	n1 := len(chunks1.Chunks)
	vv("one-at-a-time n2 = %v; took %v", n1, time.Since(t1))

	t0 := time.Now()
	precis0, chunks0, err := SummarizeFileInCDCHashes(host, path, false)
	panicOn(err)
	n0 := len(chunks0.Chunks)
	vv("full-file at once n0 = %v; took %v", n0, time.Since(t0))

	if !precisAlone.Equal(precis0) {
		panic(fmt.Sprintf("precisAlone (%v) != precis0 (%v)", precisAlone, precis0))
	}

	if !precisAlone.Equal(precis1) {
		panic(fmt.Sprintf("precisAlone (%v) != precis1 (%v)", precisAlone, precis1))
	}

	var x string
	// j is index for chunks1; i is index for chunks0
	j := 0
	unwrittenCount := 0
	dataSame := 0
	dataSameBytes := 0
loopi:
	for i := 0; i < len(chunks0.Chunks); i++ {
		chnk := chunks0.Chunks[i]
		beg := chnk.Beg
		endx := chnk.Endx

		if chnk.Cry == "UNWRIT;" {
			unwrittenCount++
			j++
			continue
		}

		if j >= len(chunks1.Chunks) {
			vv("end of available j over chunks1")
			break
		}
		chnk1 := chunks1.Chunks[j]
		beg1 := chnk1.Beg
		if beg1 != beg {
			x = fmt.Sprintf("<< vs incr.Beg: %v ; incr Beg DIFFERS! on i = %v", beg1, i)
			vv(x)
			show(chunks0, "chunks0   full file, Cutpoints on it.", i)
			show(chunks1, "chunks1  one at a time.", j)
			panic("beg mismatch")
		}
		endx1 := chunks1.Chunks[i].Endx
		if endx1 != endx {
			all0, all0max := allZeroChunk(chunks1.Chunks[i], path, chnk)
			if endx1 < endx && all0max {
				// skip past all the zero chunks and keep comparing
				j0 := j
				for k := j + 1; k < len(chunks1.Chunks); k++ {
					if chunks1.Chunks[k].Endx < endx {
						j = k
					} else {
						vv("jumping j past the RLE0 %v -> %v", j0, j)
						continue loopi
					}
				}
			}
			isAllZero1 := fmt.Sprintf("isAllZero1 = %v", all0)

			x += fmt.Sprintf(" << vs incr.Endx: %v ; incr Endx DIFFERS! on i = %v.  chunks0 has '%v'; chunks1 has '%v'\n %v", endx1, i, chunks0.Chunks[i], chunks1.Chunks[i], isAllZero1)
			vv(x)
			panic("endx mismatch")
		}
		// INVAR: beg and endx match
		if chnk.Cry == chnk1.Cry {
			dataSame++
			dataSameBytes += int(endx - beg)
		}
		j++
	}
	if n1 != n0-unwrittenCount {
		t.Fatalf("error: Lightly got %v, but batch got %v", n1, n0)
	}
	vv("dataSame = %v; dataSameBytes = %v out of %v", dataSame, dataSameBytes, precis0.FileSize)

	_ = precis0
	_ = precis1
	//cv.So(precis0.FileCry, cv.ShouldEqual, precis1.FileCry)
	if precis0.FileCry != precis1.FileCry {
		vv("precis0 = '%#v'", precis0)
		vv("precis1 = '%#v'", precis1)
		panic(fmt.Sprintf("precis0.FileCry != precis1.FileCry"))
	}

	if false {
		vv("precis1 = '%#v'", precis1)
		vv("precis0 = '%#v'", precis0)
		cv.So(precis0, cv.ShouldResemble, precis1)
	}
	// })
}

// efficient on big files with small deltas

func Test377_big_files_with_small_changes(t *testing.T) {
	//return
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

		cfg.ServerAddr = "127.0.0.1:0"
		srv := rpc.NewServer("srv_rsync_test777", cfg)

		serverAddr, err := srv.Start()
		panicOn(err)
		defer srv.Close()

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

		var bytesFromDisk int64
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

		err = UpdateLocalFileWithRemoteDiffs_TestHelper(localPathFinal, localPath, localMap, plan, goalPrecis)
		panicOn(err)

		// localPathFinal has the file made to match remotePath.
		// normally we would now rename localPathFinal onto localPath,
		// and be done.

		// 7.94s
		vv("elap to UpdateLocalWithRemoteDiffs = '%v'", time.Since(t5))

		difflen := compareFilesDiffLen(localPathFinal, remotePath)
		cv.So(difflen, cv.ShouldEqual, 0)
	})
}

func Test888_rle_zeros_encoded(t *testing.T) {

	//cv.Convey("using our rsync-like-protocol, all zero files should be efficiently run-length-encoded. Can we get an all zero 10MB file down to one chunk?", t, func() {

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

	cfg.ServerAddr = "127.0.0.1:0"
	srv := rpc.NewServer("srv_rsync_test888", cfg)

	serverAddr, err := srv.Start()
	panicOn(err)
	defer srv.Close()

	// about 4 seconds to copy.
	vv("copy done. server Start() returned serverAddr = '%v'", serverAddr)

	srvRsyncNode := &RsyncNode{}
	panicOn(srv.Register(srvRsyncNode))

	cfg.ClientDialToHostPort = serverAddr.String()
	cli, err := rpc.NewClient("cli_rsync_test888", cfg)
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

	//parallel := true // green, was using
	parallel := false

	var localPrecis *FilePrecis
	var wantsUpdate *Chunks

	// only parallel has the RLE0; impl to start.
	if parallel {
		fmt.Printf("first ChunkFile: \n")
		localPrecis, wantsUpdate, err = ChunkFile(localPath)
		panicOn(err)
		// 2.5 sec.
	} else {
		//localPrecis, wantsUpdate, err = GetHashesOneByOne(host, localPath)
		localPrecis, wantsUpdate, err = SummarizeFileInCDCHashes(host, localPath, false)
		panicOn(err)
		// 14.335789s
	}
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
		//goalPrecis, templateChunks, err = GetHashesOneByOne(host, remotePath)
		goalPrecis, templateChunks, err = SummarizeFileInCDCHashes(host, remotePath, false)
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

	var bytesFromDisk int64
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

	err = UpdateLocalFileWithRemoteDiffs_TestHelper(localPathFinal, localPath, localMap, plan, goalPrecis)
	panicOn(err)

	// localPathFinal has the file made to match remotePath.
	// normally we would now rename localPathFinal onto localPath,
	// and be done.

	vv("elap to UpdateLocalWithRemoteDiffs = '%v'", time.Since(t5))

	difflen := compareFilesDiffLen(localPathFinal, remotePath)
	if difflen != 0 {
		panic(fmt.Sprintf("expected difflen(%v) to be 0.", difflen))
	}
	// })
}

// debug/test print helper
func allZeroChunk(chnk1 *Chunk, path string, chnk *Chunk) (all0, all0max bool) {
	fd, err := os.Open(path)
	panicOn(err)
	defer fd.Close()

	_, err = fd.Seek(chnk1.Beg, 0)
	panicOn(err)
	sz1 := chnk1.Endx - chnk1.Beg
	sz0 := chnk.Endx - chnk.Beg
	sz := max(sz0, sz1)
	slc := make([]byte, sz)
	_, err = io.ReadFull(fd, slc)
	panicOn(err)
	all0 = allZero(slc[:sz1])
	all0max = allZero(slc[:sz])
	vv("allZeroChunk %v for size %v chnk1.Beg(%v):chnk1.Endx(%v) of path '%v'; all0max = %v for the max sz (chnk is %v:%v)", all0, sz1, chnk1.Beg, chnk1.Endx, path, all0max, chnk.Beg, chnk.Endx)
	return
}

// 203 check returned precis is right on SummarizeBytesInCDCHashes on
// empty file, non-existance file, fully-sparse file (0 stat size)
// or fully pre-allocated but otherwise empty files.
func Test203_rsync_SummarizeBytesInCDCHashes_on_size_0_four_cases(t *testing.T) {

	cv.Convey("SummarizeBytesInCDCHashes() should generate a precis even in these 4 corner cases: when there is no file, the file is regular but empty, a fully sparse big file with 0 bytes, and a statsize 0 file that has pre-allocated a bunch of space", t, func() {

		// set up two isolated test dirs, so we can
		// be sure exactly what is changing by inspection.

		cwd, err := os.Getwd()
		panicOn(err)
		_ = cwd

		localDir := cwd + sep + "local_cli_dir_test203/"
		//remoteDir := cwd + sep + "remote_srv_dir_test203/"
		// remove any old leftover test files first.
		os.RemoveAll(localDir)
		//os.RemoveAll(remoteDir)
		os.MkdirAll(localDir, 0755)
		//os.MkdirAll(remoteDir, 0755)

		// write the sparse and pre-alloc test suite to localDir
		sparsified.MustGenerateSparseTestFiles(localDir, 0, 0)

		list, err := filepath.Glob(localDir + "/*")
		panicOn(err)

		//vv("done with MustGenerateSparseTestFiles, list = '%#v'", list)

		//vv("list = '%#v'", list)
		host := "localhost"
		const keepDataNO = false
		for i, path := range list {
			_ = i

			fi, err := os.Stat(path)
			panicOn(err)
			statsz, modTime := fi.Size(), fi.ModTime()
			_ = statsz

			fd, err := os.Open(path)
			panicOn(err)
			precis, chunks, err := SummarizeBytesInCDCHashes(host, path, fd, modTime, keepDataNO, fi) // int64(len(data2)))
			panicOn(err)
			_ = chunks
			if precis == nil {
				panic(fmt.Sprintf("i=%v had nil precis for path '%v'", i, path))
			}
			vv("on i=%v, got precis = '%v'", i, precis)
		}

		// test non-existent file
		path := "test203_file_path_that_does_not_exist"
		os.Remove(path)
		fi, _ := os.Stat(path)
		if fi != nil {
			panic(fmt.Sprintf("expected nil fi back from os.Stat(nonexistent_path); got = '%#v'", fi))
		}
		precis, chunks, err := SummarizeBytesInCDCHashes(host, path, nil, time.Time{}, keepDataNO, fi) // int64(len(data2)))
		_ = chunks
		panicOn(err)
		if precis == nil {
			panic(fmt.Sprintf("non-existent file got nil precis back. for path '%v'", path))
		}
		if !precis.PathAbsent {
			panic(fmt.Sprintf("non-existent file must have precis.PathAbsent flag set! for path '%v'", path))
		}
		// currently getting a precis for a non-existent file.
		// Is that really what we want?
		//vv("on non-existent file, got precis = '%v'", precis)
		/*
			rsync_test.go:1107 [goID 20] 2025-07-26 06:56:10.935 -0500 CDT on non-existent file, got precis = '{
			    "CallID": "",
			    "IsFromSender": false,
			    "Created": "0001-01-01T00:00:00Z",
			    "Host": "localhost",
			    "Path": "test203_file_path_that_does_not_exist",
			    "ModTime": "0001-01-01T00:00:00Z",
			    "FileSize": 0,
			    "FileMode": 0,
			    "FileOwner": "",
			    "FileOwnerID": 0,
			    "FileGroup": "",
			    "FileGroupID": 0,
			    "FileMeta": null,
			    "HashName": "blake3.33B",
			    "FileCry": "blake3.33B-rxNJufX5oaagQE3qNtzJSZvLJcmtwRK3zJqTyuQfMmLg",
			    "IsSparse": false,
			    "PreAllocUnwritBeg": 0,
			    "PreAllocUnwritEndx": 0,
			    "ChunkerName": "fastcdc-plakar-go-cdc-chunkers",
			    "CDC_Config": {
			        "MinSize": 4096,
			        "TargetSize": 8192,
			        "MaxSize": 16384
			    }
			}
		*/
	})
}

// parallelChunking uses ChunkFile(), so check same sparse file cases as 203.
func Test204_rsync_ChunkFile_on_size_0_four_cases(t *testing.T) {

	//cv.Convey("SummarizeBytesInCDCHashes() should generate a precis even in these 4 corner cases: when there is no file, the file is regular but empty, a fully sparse big file with 0 bytes, and a statsize 0 file that has pre-allocated a bunch of space", t, func() {

	// set up two isolated test dirs, so we can
	// be sure exactly what is changing by inspection.

	cwd, err := os.Getwd()
	panicOn(err)
	_ = cwd

	localDir := cwd + sep + "local_cli_dir_test204/"
	//remoteDir := cwd + sep + "remote_srv_dir_test204/"
	// remove any old leftover test files first.
	os.RemoveAll(localDir)
	//os.RemoveAll(remoteDir)
	os.MkdirAll(localDir, 0755)
	//os.MkdirAll(remoteDir, 0755)

	// write the sparse and pre-alloc test suite to localDir
	sparsified.MustGenerateSparseTestFiles(localDir, 0, 0)

	list, err := filepath.Glob(localDir + "/*")
	panicOn(err)

	//vv("done with MustGenerateSparseTestFiles, list = '%#v'", list)

	//vv("list = '%#v'", list)
	//host := "localhost"
	const keepDataNO = false
	for i, path := range list {
		_ = i

		//fi, err := os.Stat(path)
		//panicOn(err)
		//statsz, modTime := fi.Size(), fi.ModTime()
		//_ = statsz

		//fd, err := os.Open(path)
		//panicOn(err)
		//precis, chunks, err := SummarizeBytesInCDCHashes(host, path, fd, modTime, keepDataNO, fi) // int64(len(data2)))

		precis, chunks, err := ChunkFile(path)
		panicOn(err)
		_ = chunks
		if precis == nil {
			panic(fmt.Sprintf("i=%v had nil precis for path '%v'", i, path))
		}
		vv("on i=%v, got precis = '%v'", i, precis)
	}

	// test non-existent file
	path := "test204_file_path_that_does_not_exist"
	os.Remove(path)
	fi, _ := os.Stat(path)
	if fi != nil {
		panic(fmt.Sprintf("expected nil fi back from os.Stat(nonexistent_path); got = '%#v'", fi))
	}
	//precis, chunks, err := SummarizeBytesInCDCHashes(host, path, nil, time.Time{}, keepDataNO, fi) // int64(len(data2)))
	precis, chunks, err := ChunkFile(path)
	_ = chunks
	panicOn(err)
	if precis == nil {
		panic(fmt.Sprintf("non-existent file got nil precis back. for path '%v'", path))
	}
	if !precis.PathAbsent {
		panic(fmt.Sprintf("non-existent file must have precis.PathAbsent flag set! for path '%v'", path))
	}
	// currently getting a precis for a non-existent file.
	// Is that really what we want?
	//vv("on non-existent file, got precis = '%v'", precis)

}

// !parallelChunking uses GetHashesOneByOne, so check same sparse file cases as 203.
func Test205_rsync_GetHashesOneByOne_on_size_0_four_cases(t *testing.T) {

	//cv.Convey("GetHashesOneByOne() should generate a precis even in these 4 corner cases: when there is no file, the file is regular but empty, a fully sparse big file with 0 bytes, and a statsize 0 file that has pre-allocated a bunch of space", t, func() {

	// set up two isolated test dirs, so we can
	// be sure exactly what is changing by inspection.

	cwd, err := os.Getwd()
	panicOn(err)
	_ = cwd

	localDir := cwd + sep + "local_cli_dir_test205/"
	//remoteDir := cwd + sep + "remote_srv_dir_test205/"
	// remove any old leftover test files first.
	os.RemoveAll(localDir)
	//os.RemoveAll(remoteDir)
	os.MkdirAll(localDir, 0755)
	//os.MkdirAll(remoteDir, 0755)

	// write the sparse and pre-alloc test suite to localDir
	sparsified.MustGenerateSparseTestFiles(localDir, 0, 0)

	list, err := filepath.Glob(localDir + "/*")
	panicOn(err)

	//vv("done with MustGenerateSparseTestFiles, list = '%#v'", list)

	//vv("list = '%#v'", list)
	host := "localhost"
	const keepDataNO = false
	for i, path := range list {
		_ = i

		//fi, err := os.Stat(path)
		//panicOn(err)
		//statsz, modTime := fi.Size(), fi.ModTime()
		//_ = statsz

		//fd, err := os.Open(path)
		//panicOn(err)

		//precis, chunks, err := SummarizeBytesInCDCHashes(host, path, fd, modTime, keepDataNO, fi) // int64(len(data2)))
		precis, chunks, err := GetHashesOneByOne(host, path)
		panicOn(err)
		_ = chunks
		if precis == nil {
			panic(fmt.Sprintf("i=%v had nil precis for path '%v'", i, path))
		}
		vv("on i=%v, got precis = '%v'", i, precis)
	}

	// test non-existent file
	path := "test205_file_path_that_does_not_exist"
	os.Remove(path)
	fi, _ := os.Stat(path)
	if fi != nil {
		panic(fmt.Sprintf("expected nil fi back from os.Stat(nonexistent_path); got = '%#v'", fi))
	}
	//precis, chunks, err := SummarizeBytesInCDCHashes(host, path, nil, time.Time{}, keepDataNO, fi) // int64(len(data2)))
	precis, chunks, err := GetHashesOneByOne(host, path)
	_ = chunks
	panicOn(err)
	if precis == nil {
		panic(fmt.Sprintf("non-existent file got nil precis back. for path '%v'", path))
	}
	if !precis.PathAbsent {
		panic(fmt.Sprintf("non-existent file must have precis.PathAbsent flag set! for path '%v'", path))
	}
	// currently getting a precis for a non-existent file.
	// Is that really what we want?
	//vv("on non-existent file, got precis = '%v'", precis)

}

// same test as 203-205 but for SummarizeFileInCDCHashes
func Test206_rsync_SummarizeFileInCDCHashes_on_size_0_four_cases(t *testing.T) {

	//cv.Convey("SummarizeFileInCDCHashes() should generate a precis even in these 4 corner cases: when there is no file, the file is regular but empty, a fully sparse big file with 0 bytes, and a statsize 0 file that has pre-allocated a bunch of space", t, func() {

	// set up two isolated test dirs, so we can
	// be sure exactly what is changing by inspection.

	cwd, err := os.Getwd()
	panicOn(err)
	_ = cwd

	localDir := cwd + sep + "local_cli_dir_test206/"
	//remoteDir := cwd + sep + "remote_srv_dir_test206/"
	// remove any old leftover test files first.
	os.RemoveAll(localDir)
	//os.RemoveAll(remoteDir)
	os.MkdirAll(localDir, 0755)
	//os.MkdirAll(remoteDir, 0755)

	// write the sparse and pre-alloc test suite to localDir
	sparsified.MustGenerateSparseTestFiles(localDir, 0, 0)

	list, err := filepath.Glob(localDir + "/*")
	panicOn(err)

	//vv("done with MustGenerateSparseTestFiles, list = '%#v'", list)

	//vv("list = '%#v'", list)
	host := "localhost"
	const keepDataNO = false
	for i, path := range list {
		_ = i

		//fi, err := os.Stat(path)
		//panicOn(err)
		//statsz, modTime := fi.Size(), fi.ModTime()
		//_ = statsz

		//fd, err := os.Open(path)
		//panicOn(err)

		precis, chunks, err := SummarizeFileInCDCHashes(host, path, keepDataNO)
		panicOn(err)
		_ = chunks
		if precis == nil {
			panic(fmt.Sprintf("i=%v had nil precis for path '%v'", i, path))
		}
		vv("on i=%v, got precis = '%v'", i, precis)
	}

	// test non-existent file
	path := "test206_file_path_that_does_not_exist"
	os.Remove(path)
	fi, _ := os.Stat(path)
	if fi != nil {
		panic(fmt.Sprintf("expected nil fi back from os.Stat(nonexistent_path); got = '%#v'", fi))
	}
	precis, chunks, err := SummarizeFileInCDCHashes(host, path, keepDataNO)
	_ = chunks
	panicOn(err)
	if precis == nil {
		panic(fmt.Sprintf("non-existent file got nil precis back. for path '%v'", path))
	}
	if !precis.PathAbsent {
		panic(fmt.Sprintf("non-existent file must have precis.PathAbsent flag set! for path '%v'", path))
	}
	// currently getting a precis for a non-existent file.
	// Is that really what we want?
	//vv("on non-existent file, got precis = '%v'", precis)

}
