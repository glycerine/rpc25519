package jsync

import (
	//"bufio"
	"context"
	"fmt"
	myblake3 "github.com/glycerine/rpc25519/hash"
	//"github.com/glycerine/rpc25519/progress"
	//"golang.org/x/sys/unix"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	//"sync"
	"time"

	"github.com/glycerine/blake3"
	"github.com/glycerine/idem"
	rpc "github.com/glycerine/rpc25519"
	"github.com/glycerine/rpc25519/jsync/sparsified"
)

const useRLE0 = true

var ErrNeedDirTaker = fmt.Errorf("DirTaker needed: giver has directory where taker has file")

// Taker handes receiving updated data ("taking it")
// from the wire and writing it, in turn, to disk.
//
// When the demo client "cli" is invoked with "cli -pull",
// then the local service will run Taker.
// The remote service will run Giver in that case.
//
// When client "cli" does (push, the default),
// then the local service will run Giver.
// The remote service will run Taker in that case.
//
// See the RsyncService.Start() method for
// more detail.
//
// The syncReq pointer in this call will be set when
// the Taker is local to the initating goroutine.
// It will be nil when Taker is remote.
//
// The returned err0 is used inside this func in a defer.
// If syncReq is present (as will be the case
// when Taker is being run locally not remotely),
// then the err0.Error() string will be reported
// in syncReq.Errs. This communicates back error
// information, for example, to the cmd/cli/client.go
// code. Also in the code here the ckt will be
// Closed with this same err0, propagating the
// error throught the Circuit.Halt.ReqStop
// shutdown tree.
func (s *SyncService) Taker(ctx0 context.Context, ckt *rpc.Circuit, myPeer *rpc.LocalPeer, syncReq *RequestToSyncPath) (err0 error) {

	//vv("SyncService.Taker top")

	name := myPeer.PeerServiceName
	_ = name // used when logging is on.

	bt := &byteTracker{}

	defer func(syncReq *RequestToSyncPath) {
		//vv("%v: (ckt '%v') defer running! finishing Taker; syncReq=%p; err0='%v'", name, ckt.Name, syncReq, err0)
		//vv("bt = '%#v'", bt)

		// only close Done for local (client, typically) if we were started locally.
		if syncReq != nil {
			syncReq.BytesRead = int64(bt.bread)
			syncReq.BytesSent = int64(bt.bsend)
			//syncReq.RemoteBytesTransferred = ?
			if err0 != nil {
				//vv("setting (%p) syncReq.Errs = '%v'", syncReq, err0.Error())
				syncReq.Errs = err0.Error()
			}
			syncReq.Done.Close() // only AFTER setting Errs, please!
		}
		//vv("taker defer ckt.Close(err0='%v')", err0)
		ckt.Close(err0)

		// suppress context cancelled shutdowns
		if r := recover(); r != nil {
			//vv("taker sees panic: '%v'", r)
			switch x := r.(type) {
			case error:
				xerr := x.Error()
				if strings.Contains(xerr, "connection reset") ||
					strings.Contains(xerr, "use of closed network connection") ||
					strings.Contains(xerr, "broken pipe") {
					// ok
					return
				}
			}
			if r != rpc.ErrContextCancelled && r != rpc.ErrHaltRequested {
				alwaysPrintf("taker sees abnormal shutdown panic: '%v'", r)
				panic(r)
			} else {
				//vv("Taker suppressing rpc.ErrContextCancelled or ErrHaltRequested, this is normal shutdown.")
			}
		}
	}(syncReq)

	done0 := ctx0.Done()
	done := ckt.Context.Done()
	t0 := time.Now()
	lastUpdate := t0
	_ = lastUpdate

	var disk *FileToDiskState

	var goalPrecis *FilePrecis
	var local *Chunks
	var plan *Chunks
	var senderPlan *SenderPlan
	var localMap map[string]*Chunk

	var localPathToWrite string
	var localPathToRead string
	var localPathToReadBlake3sum string

	var origVersFd *os.File

	// working buffer to read local file chunks into.
	buf := make([]byte, rpc.UserMaxPayload+10_000)
	//var newversBufio *bufio.Writer // comment for sparse
	var newversFd *os.File
	var newversFdUnwritPreallocDone bool
	var tmp string

	j := 0 // index to new version, how much we have written.
	h := blake3.New(64, nil)

	if syncReq != nil {
		// local taker, remote giver. part of pull.

		// Start already sent this syncReq to the remote giver.
		// This syncReq was assembled by the cmd/cli/client.go
		// for instance, and already included the Chunks/FilePrecis.
		// They will get back to us. Tell us what we need, if anything.
		// for instance
		// OpRsync_TellTakerToDelete
		// OpRsync_ToTakerMetaUpdateAtLeast (in future)
		// ... or any of the rest of the usual

		local = syncReq.Chunks
		if local != nil {
			localMap = getCryMap(local) // pre-index them for the update.
			//vv("local Chunks: have DataPresent()= '%v'", local.DataPresent())
		} else {
			// We don't send to giver OpRsync_LazyTakerWantsToPull here
			// b/c it is redundant with the send in service.go:373
		}
		localPathToWrite = syncReq.TakerPath
		localPathToRead = syncReq.TakerPath

		// Dir sync requests will set TakerTempDir
		// to have us write into a whole separate
		// directory tree before a more or less atomic
		// move of it all into place. We should write
		// where they ask us to, and skip the rename
		// at the end.
		if syncReq.TakerTempDir != "" {
			//vv("TakerTempDir = '%v', so localPathToWrite '%v' => '%v'",
			//	syncReq.TakerTempDir, localPathToWrite,
			//	filepath.Join(syncReq.TakerTempDir, syncReq.TakerPath))

			localPathToWrite = filepath.Join(
				syncReq.TakerTempDir, syncReq.TakerPath)
		} else {
			localPathToWrite = filepath.Join(
				syncReq.TopTakerDirFinal, syncReq.TakerPath)
		}
		if syncReq.TopTakerDirFinal != "" {
			localPathToRead = filepath.Join(
				syncReq.TopTakerDirFinal, syncReq.TakerPath)
		}
		//vv("localPathToRead = '%v'", localPathToRead)
		//vv("localPathToWrite = '%v'", localPathToWrite)

		if dirExists(localPathToRead) {
			return fmt.Errorf("error in Taker: localPathToRead cannot be an existing directory: '%v'; use DirTaker functionality.", localPathToRead)
		}

		// prep for reading data
		var err error
		if fileExists(localPathToRead) {
			origVersFd, err = os.OpenFile(localPathToRead, os.O_RDONLY, 0)
			panicOn(err)
			defer origVersFd.Close()
		}
	}

	//vv("taker about to start taker for loop.")
	// this is the Taker side
takerForSelectLoop:
	for {
		select {
		case frag := <-ckt.Reads:
			//vv("%v: (ckt %v) (Taker) ckt.Reads sees frag:'%s'", name, ckt.Name, frag)
			_ = frag
			switch frag.FragOp {

			case OpRsync_ToTakerDratGiverFileIsNowDir: // 40
				// ugh. need to tell caller to run DirTaker
				// after deleting their local TakerPath file to
				// make room for the giver's directory.
				return ErrNeedDirTaker

			case OpRsync_AckBackFIN_ToTaker:
				//vv("%v: (ckt '%v') (Taker) sees OpRsync_AckBackFIN_ToTaker. returning.", name, ckt.Name)
				return

			case OpRsync_LazyTakerNoLuck_ChunksRequired:
				//vv("%v: (ckt '%v') (Taker) sees OpRsync_LazyTakerNoLuck_ChunksRequired.", name, ckt.Name)
				// should we just be overwriting syncReq ? TODO!

				syncReq2 := &RequestToSyncPath{
					TakerTempDir:     syncReq.TakerTempDir,
					TopTakerDirFinal: syncReq.TopTakerDirFinal,
				}
				_, err := syncReq2.UnmarshalMsg(frag.Payload)
				panicOn(err)
				bt.bread += len(frag.Payload)
				if syncReq2.TakerPath != syncReq.TakerPath ||
					syncReq2.SyncFromHostCID != syncReq.SyncFromHostCID ||
					syncReq2.SyncFromHostCID != rpc.HostCID {

					panic(fmt.Sprintf("sanity check failed. "+
						"On OpRsync_LazyTakerNoLuck_ChunksRequired: "+
						"(%v is syncReq2.TakerPath) != "+
						"(syncReq.TakerPath which is %v) or HostCID changed.",
						syncReq2.TakerPath, syncReq.TakerPath))
				} else {
					//vv("lazy taker no luck: chunks required. syncReq2.TakerPath = '%v'; GiverPath='%v'", syncReq2.TakerPath, syncReq2.GiverPath)
				}

				// can we avoid sending a file if it was just
				// a mod time mismatch?
				b3sumGiver, ok := frag.GetUserArg("giverFullFileBlake3sum")
				if !ok {
					panic("why no giverFullFileBlake3sum ?")
				}
				sumTaker, _, err := blake3.HashFile(localPathToRead)
				panicOn(err)
				b3sumTaker := myblake3.RawSumBytesToString(sumTaker)
				if b3sumTaker == b3sumGiver {
					//vv("good: b3sumTaker == b3sumGiver: setting syncReq.ModTime = '%v'", syncReq2.GiverModTime)
					// hard link it.
					if localPathToWrite != localPathToRead {
						//vv("hard linking 7 '%v' <- '%v'",	localPathToRead, localPathToWrite)
						panicOn(os.Link(localPathToRead, localPathToWrite))
					}
					// just adjust mod time and fin.
					err = os.Chtimes(localPathToWrite, time.Time{}, syncReq2.GiverModTime)
					panicOn(err)
					// update mode too? not sure if we have it avail
					if syncReq2.GiverFileMode != 0 {
						err = os.Chmod(localPathToWrite, fs.FileMode(syncReq2.GiverFileMode))
						panicOn(err)
					}
					s.ackBackFINToGiver(ckt, frag)
					continue
				}

				if fileExists(syncReq2.TakerPath) {
					//vv("taker is setting syncReq2.MoreChunksComming = true")
					syncReq2.MoreChunksComming = true
				}
				// have to set an empty Chunks too.
				syncReq2.Chunks = &Chunks{
					Path: syncReq2.TakerPath,
					//FileCry: // not known yet. overlap the net/disk so leave blank
				}

				bts, err := syncReq2.MarshalMsg(nil)
				panicOn(err)
				beginAgain := myPeer.NewFragment()
				beginAgain.FragSubject = frag.FragSubject
				beginAgain.FragOp = OpRsync_RequestRemoteToGive // 12
				beginAgain.Payload = bts
				err = ckt.SendOneWay(beginAgain, 0, 0)
				panicOn(err)
				bt.bsend += len(bts)

				var takerChunks *Chunks
				if parallelChunking {
					_, takerChunks, err = ChunkFile(syncReq2.TakerPath)

				} else {
					_, takerChunks, err = GetHashesOneByOne(rpc.Hostname,
						syncReq2.TakerPath)
				}
				panicOn(err)

				if len(takerChunks.Chunks) == 0 {
					panic(fmt.Sprintf("how can takerChunks be len 0 here?: '%#v'", takerChunks))
				}

				err = s.packAndSendChunksLimitedSize(
					takerChunks,
					frag.FragSubject,
					OpRsync_RequestRemoteToGive_ChunksLast,
					OpRsync_RequestRemoteToGive_ChunksMore,
					ckt,
					bt,
					syncReq2.TakerPath,
					syncReq, // for the progress meter, send orig
				)
				panicOn(err)

				// overlap network and disk by indexing while we wait.
				localMap = getCryMap(takerChunks)

				frag = nil // GC early
				// no, cannot reply to client if we do this: syncReq = syncReq2
				continue

			case OpRsync_FileSizeModTimeMatch:
				//vv("%v: (ckt '%v') (Taker) sees OpRsync_FileSizeModTimeMatch. sending ack back FIN", name, ckt.Name)

				if localPathToWrite != localPathToRead {
					//vv("hard linking 2 '%v' <- '%v'", localPathToRead, localPathToWrite)
					panicOn(os.Link(localPathToRead, localPathToWrite))
				}

				s.ackBackFINToGiver(ckt, frag)
				if syncReq != nil {
					syncReq.SizeModTimeMatch = true
				}
				frag = nil
				continue // wait for FIN

			case OpRsync_TellTakerToDelete: // part of pull
				//vv("%v: (ckt %v) (Taker) sees OpRsync_TellTakerToDelete. deleting '%v'", name, ckt.Name, syncReq.TakerPath)
				if syncReq.DryRun {
					alwaysPrintf("dry: would remove '%v', since OpRsync_TellTakerToDelete", syncReq.TakerPath)
				} else {
					err := os.Remove(syncReq.TakerPath)
					panicOn(err)
				}
				s.ackBackFINToGiver(ckt, frag)
				frag = nil
				continue // wait for FIN

			case OpRsync_ToTakerMetaUpdateAtLeast:
				precis := &FilePrecis{}
				_, err := precis.UnmarshalMsg(frag.Payload)
				panicOn(err)
				bt.bread += len(frag.Payload)

				//vv("%v: (ckt %v) (Taker) sees OpRsync_ToTakerMetaUpdateAtLeast. updating mode/modTime on '%v' to '%v'", name, ckt.Name, syncReq.TakerPath, precis.ModTime)

				if localPathToWrite != localPathToRead {
					if syncReq.DryRun {
						alwaysPrintf("dry: would hard linking 6 '%v' <- '%v'",
							localPathToRead, localPathToWrite)
					} else {
						panicOn(os.Link(localPathToRead, localPathToWrite))
					}
				} else {
					// an empty file might need to be created;
					// its data hash will be the same as a non-existant file.
					if !fileExists(localPathToWrite) {
						if syncReq.DryRun {
							alwaysPrintf("dry: would create empty file '%v'",
								localPathToWrite)
						} else {
							fd, err := os.Create(localPathToWrite)
							panicOn(err)
							fd.Close()
						}
					}
				}

				//path := syncReq.TakerPath
				//path := localPathToRead
				mode := precis.FileMode
				if mode == 0 {
					mode = 0600
				}
				err = os.Chmod(localPathToWrite, fs.FileMode(mode))
				panicOn(err)

				if !precis.ModTime.IsZero() {
					err = os.Chtimes(localPathToWrite, time.Time{}, precis.ModTime)
					panicOn(err)
				}

				s.ackBackFINToGiver(ckt, frag)
				frag = nil
				continue // wait for FIN.

			case OpRsync_ToGiverNeedFullFile2:
				panic("OpRsync_ToGiverNeedFullFile2 not expected in Taker!")

			case OpRsync_HeavyDiffChunksEnclosed, OpRsync_HeavyDiffChunksLast:
				//vv("stream of heavy diffs arriving! : %v", frag.String())

				chunks := &Chunks{}
				_, err := chunks.UnmarshalMsg(frag.Payload)
				panicOn(err)
				bt.bread += len(frag.Payload)

				// this is UpdateLocalWithRemoteDiffs() in pieces,
				// incrementally as Chunks arrive

				// stream through a bufio to disk, rather than holding all in memory.
				if newversFd == nil {

					rnd := cryRandBytesBase64(18)
					tmp = localPathToWrite + "_accept_plan_tmp_" + rnd

					// Dir sync requests will set TakerTempDir
					// to have us write into a whole separate
					// directory tree before a more or less atomic
					// move of it all into place. We should write
					// where they ask us to, and skip the rename
					// at the end.
					if syncReq.TakerTempDir != "" {
						//vv("since syncReq.TakerTempDir is set, '%v'. we keep tmp == localPathToWrite: '%v'", syncReq.TakerTempDir, localPathToWrite)
						tmp = localPathToWrite
					}
					//else {
					//	tmp = filepath.Join(syncReq.TopTakerDirFinal, tmp)
					//}
					newversFd, err = os.Create(tmp)
					panicOn(err)

					vv("make sparse if possible! truncating to %v our tmp file '%v' on newversFd", syncReq.GiverFileSize, tmp)
					err = newversFd.Truncate(int64(syncReq.GiverFileSize))
					panicOn(err)
					//newversFd.Sync() // does this help keep the sparseness? nope.

					// just do one prealloc if any of the space is unwrit.
					if chunks.PreAllocUnwritEndx > 0 {
						vv("PreAllocUnwritEndx(%v) > 0 on path '%v'", chunks.PreAllocUnwritEndx, newversFd.Name()) // now seen in test 220
						_, err = newversFd.Seek(0, 0)
						panicOn(err)
						// always start from 0, since otherwise
						// APFS complains.
						_, err = sparsified.Fallocate(newversFd, sparsified.FALLOC_FL_KEEP_SIZE, 0, chunks.PreAllocUnwritEndx) // seen in 220 test, making this call: sparse_darwin.go:300 2025-07-16 23:14:30.896 -0500 CDT top of fallocate(mode = 43, off = 0, length = 67108864) for path '/Users/jaten/rpc25519/jsync/remote_srv_dir_test220/testZZZ.outpath.04.sparsefile_accept_plan_tmp_RzcPeYhKUg6hx0Tftk1lkRPB'
						if err != nil {
							// try not to fail just because disk is fragmented or no pre-allocation support. Just warn.
							alwaysPrintf("warning: could not pre-allocate space same as origin for path (tmp='%v'; final='%v') of size bytes: %v; err = '%v'. Likely filesystem does not support pre-allocation, or target disk is too fragmented.", tmp, localPathToWrite, formatUnder(chunks.PreAllocUnwritEndx), err)
						}
						newversFdUnwritPreallocDone = true
					}

					// for debugging sparse hole issues:
					//spans, err := sparsified.FindSparseRegions(newversFd)
					//panicOn(err)
					//vv("debug sparse spans after first Truncate = '%v'", spans)

					//vv("taker created file '%v'", tmp)
					//newversBufio = bufio.NewWriterSize(newversFd, rpc.UserMaxPayload)
					// remember to Flush and Close!
					//defer newversBufio.Flush() // must be first
					defer newversFd.Close()

					// prep local file too, for seeking to chunks.
					if origVersFd == nil {
						if localPathToRead == "" {
							panic("localPathToRead must have been set!")
						}
						if fileExists(localPathToRead) {
							origVersFd, err = os.Open(localPathToRead)
							panicOn(err)
							defer origVersFd.Close()
						}
					}
					t0 = time.Now()
					lastUpdate = time.Now()
				} // end if newversFD == nil

				// compute the full file hash/checksum as we go

				// remote gives the plan of what to create
				for _, chunk := range chunks.Chunks {

					if len(chunk.Data) == 0 {
						//vv("len(chunk.Data) == 0 => the data is local, or RLE0; .Cry = '%v'", chunk.Cry)

						if chunk.Cry == "RLE0;" {
							span, wings := sparsified.AlignedSparseSpan(int64(chunk.Beg), int64(chunk.Endx))

							// 3 steps to write a sparse hole full
							// of logically 0. Each optional is optional:
							// a) actual data 0, up to a 4k alignment point (pre-wing).
							// b) the hole, a span >= 4k, 4k aligned.
							// c) actuall data 0, trailing the hole (the post-wing).
							if wings != nil && wings.Pre != nil {
								n := wings.Pre.Endx - wings.Pre.Beg
								wb, err := newversFd.Write(zeros4k[:n])
								panicOn(err)
								j += wb
								h.Write(zeros4k[:n])
							}
							if span != nil {
								startPos := curpos(newversFd)
								n := span.Endx - span.Beg
								vv("applying sparse span of len %v; curpos = %v", n, startPos)
								ns := n / 4096  // len(zeros4k)
								rem := n % 4096 // len(zeros4k)
								//_, err = newversFd.Seek(n, 1) // 1=> relative to current offset
								_, err = newversFd.Seek(span.Endx, 0)
								panicOn(err)
								//vv("after Seek(n=%v,1): curpos = %v", n, curpos(newversFd))
								vv("after Seek(span.Endx=%v,0): curpos = %v", span.Endx, curpos(newversFd))
								// hasher update only. not to disk.
								for range ns {
									//wb, err := newversFd.Write(zeros4k)
									//panicOn(err)
									j += len(zeros4k)
									h.Write(zeros4k)
								}
								if rem != 0 {
									panic(fmt.Sprintf("span must be 4k aligned: rem=%v", rem))
								}
								// probably won't work here if we
								// write anything else after it and
								// end up with a file still under
								// 32MB on APFS; e.g. below
								// the PunchBelowBytes threshold.
								// But this does help in some
								// cases on APFS. We just do it always
								// now to do the best sparseness we can.
								// if syncReq.GiverFileSize < sparsified.PunchBelowBytes {
								_, err = sparsified.Fallocate(newversFd, sparsified.FALLOC_FL_PUNCH_HOLE, startPos, n)
								panicOn(err)
								//}

							}
							if wings != nil && wings.Post != nil {
								n := wings.Post.Endx - wings.Post.Beg
								wb, err := newversFd.Write(zeros4k[:n])
								panicOn(err)
								j += wb
								h.Write(zeros4k[:n])
							}
						} else if chunk.Cry == "UNWRIT;" {
							// moved to above now: not working though.

							// try to do only once, and hopefully
							// get above working so pre-alloc
							// happens before other writes.
							if !newversFdUnwritPreallocDone {
								newversFdUnwritPreallocDone = true
								_, err = newversFd.Seek(chunk.Beg, 0)
								panicOn(err)
								// always start from 0, since otherwise
								// APFS complains.
								sz := chunk.Endx - chunk.Beg
								vv("about to Fallocate beg=%v, sz=%v, path='%v'", chunk.Beg, sz, newversFd.Name())
								_, err = sparsified.Fallocate(newversFd, sparsified.FALLOC_FL_KEEP_SIZE, chunk.Beg, sz)
								if err != nil {
									// try not to fail just because disk is fragmented or no pre-allocation support. Just warn.
									alwaysPrintf("warning: could not pre-allocate space same as origin for path (tmp='%v'; final='%v') of size bytes: %v; err = '%v'. Likely filesystem does not support pre-allocation, or target disk is too fragmented.", tmp, localPathToWrite, formatUnder(sz), err)
								}
							}
						} else {
							lc, ok := localMap[chunk.Cry]
							if !ok {
								panic(fmt.Sprintf("rsync algo failed, "+
									"the needed data is not "+
									"available locally: '%v'; len(localMap)=%v",
									chunk, len(localMap)))
							}
							// data is typically nil!
							// localMap should have only hashes.
							// so this is just getting an empty slice.
							// Is this always true?
							data := lc.Data
							if origVersFd != nil {
								//vv("read from original on disk, for chunk '%v'", chunk)
								beg := int64(lc.Beg)
								newOffset, err := origVersFd.Seek(beg, 0)
								panicOn(err)
								if newOffset != beg {
									panic(fmt.Sprintf("huh? could not seek to %v in file '%v'", lc.Beg, syncReq.TakerPath))
								}
								data = buf[:lc.Len()]
								_, err = io.ReadFull(origVersFd, data)
								panicOn(err)
							}

							//wb, err := newversBufio.Write(data)
							wb, err := newversFd.Write(data)
							panicOn(err)

							j += wb
							if wb != len(data) {
								panic("short write?!?!")
							}
							// sanity check the local chunk as a precaution.
							if int64(wb) != lc.Endx-lc.Beg {
								panic(fmt.Sprintf("lc.Endx = %v, lc.Beg = %v, but "+
									"lc.Data len = %v", lc.Endx, lc.Beg, wb))
							} // panic: lc.Endx = 2992124, lc.Beg = 2914998, but lc.Data len = 0
							h.Write(data) // update checksum
						} // end else not RLE0; and not UNWRIT;

						//vv("number sparse holes seen = %v", len(sparse))
					} else {
						// INVAR: len(chunk.Data) > 0
						//vv("the data is not local, len(chunk.Data) = %v > 0; writing at %v", len(chunk.Data), curpos(newversFd))

						//wb, err := newversBufio.Write(chunk.Data)
						wb, err := newversFd.Write(chunk.Data)
						panicOn(err)

						j += wb
						if wb != len(chunk.Data) {
							panic("short write!?!!")
						}
						// sanity check the local chunk as a precaution.
						if int64(wb) != chunk.Endx-chunk.Beg {
							panic(fmt.Sprintf("lc.Endx = %v, lc.Beg = %v, but "+
								"lc.Data len = %v", chunk.Endx, chunk.Beg, wb))
						}
						h.Write(chunk.Data)
					}

					if time.Since(lastUpdate) > time.Millisecond*100 {
						lastUpdate = time.Now()
						s.reportProgress(syncReq, syncReq.TakerPath, syncReq.GiverFileSize, int64(chunk.Endx), t0)
					}
				} // end for chunk over chunks.Chunks
				// chunk goes out of scope, so chunk.Data should get GC-ed.
				chunks = nil // GC early.

				if frag.FragOp != OpRsync_HeavyDiffChunksLast {
					frag = nil // GC early
					continue takerForSelectLoop
				}
				// INVAR: we have seen all the chunks
				// but we still use the meta info on frag below.
				sum := myblake3.SumToString(h)
				if sum != plan.FileCry {
					err = fmt.Errorf("checksum mismatch error! reconstructed='%v'; expected='%v'; plan path = ''%v'", sum, plan.FileCry, plan.Path)
					panic(err)
				}

				//newversBufio.Flush() // must be before newversFd.Close()

				// vv("total number sparse holes seen = %v", len(totsparse))
				// for _, span := range totsparse {
				// 	sz := span.Endx - span.Beg
				// 	_, err = sparsified.Fallocate(newversFd, sparsified.FALLOC_FL_PUNCH_HOLE, span.Beg, sz)
				// 	panicOn(err)
				// }

				// debug, is it sparse before we rename it?
				// only with the hole punching above.
				//newversFd.Sync() // ARG! this turns sparse into non-sparse
				//spans, err := sparsified.FindSparseRegions(newversFd)
				//panicOn(err)
				//vv("debug sparse spans just before newversFd.Close() = '%v'", spans)

				newversFd.Close()

				// if TakerTempDir is set we are
				// already writing into a full
				// temp directory structure that
				// will, as a whole, get renamed when
				// all is ready.
				if syncReq.TakerTempDir == "" {
					err = os.Rename(tmp, localPathToWrite)
					panicOn(err)
					vv("synced to disk: localPathToWrite='%v' -> renamed to '%v'", tmp, localPathToWrite)

					// debug
					//renamedFd, err := os.Open(localPathToWrite)
					//panicOn(err)
					//spans, err := sparsified.FindSparseRegions(renamedFd)
					//panicOn(err)
					//vv("debug sparse spans after rename to '%v' => '%v'", localPathToWrite, spans)
					//renamedFd.Close()

				} else {
					// need to hard link it.
					if localPathToWrite != localPathToRead {
						if !fileExists(localPathToWrite) {
							vv("hard linking 3 '%v' <- '%v'", localPathToRead, localPathToWrite)
							panicOn(os.Link(localPathToRead, localPathToWrite))
						}
					}
				}

				vv("restore mode, modtime on localPathToWrite='%v'", localPathToWrite)
				mode := goalPrecis.FileMode
				if mode == 0 {
					// unknown mode or new file, give sane default
					mode = 0600
				}
				err = os.Chmod(localPathToWrite, fs.FileMode(mode))
				panicOn(err)

				err = os.Chtimes(localPathToWrite, time.Time{}, goalPrecis.ModTime)
				panicOn(err)

				//vv("ack back file fully received! set Chtimes -> goalPrecis.ModTime='%v'", goalPrecis.ModTime)
				// needed? we'll probably be racing against shut down here.
				ackAll := myPeer.NewFragment()
				ackAll.FragSubject = frag.FragSubject
				ackAll.FragOp = OpRsync_FileAllReadAckToGiver
				ackAll.FragPart = int64(bt.bsend + bt.bread)
				bt.bsend += ackAll.Msgsize()

				err = ckt.SendOneWay(ackAll, 0, 0)
				panicOn(err)
				frag = nil
				// wait for ack back FIN

			case OpRsync_SenderPlanEnclosed:
				//vv("OpRsync_SenderPlanEnclosed: stream of heavy diffs arriving! : %v", frag.String())
				//vv("OpRsync_SenderPlanEnclosed: localPathToWrite='%v'", localPathToWrite)
				senderPlan = &SenderPlan{} // response
				_, err := senderPlan.UnmarshalMsg(frag.Payload)
				panicOn(err)
				bt.bread += len(frag.Payload)

				if senderPlan.FileIsDeleted {
					//vv("senderPlan.FileIsDeleted true, deleting path '%v'", localPathToWrite)
					if syncReq.DryRun {
						alwaysPrintf("dry: would remove '%v' since senderPlan.FileIsDeleted", localPathToWrite)
					} else {
						_ = os.Remove(localPathToWrite)
					}
					s.ackBackFINToGiver(ckt, frag)
					frag = nil
					continue // wait for other side to close
				}

				// plan has no actual Data, just ranges+hashes of the goal file.
				// actually now it does not even have chunks; they were
				// too big and redundant anyway.
				plan = senderPlan.SenderChunksNoSlice
				goalPrecis = senderPlan.SenderPrecis

				//vv("plan = '%v'", plan)

				if plan.FileSize == 0 { // ? && syncReq.TakerTempDir == "" ??
					//vv("plan.FileSize == 0 => truncate to zero localPathToWrite='%v'", localPathToWrite)
					err = truncateFileToZero(localPathToWrite)
					panicOn(err)

					// also need to set the time/mode
					// restore mode, modtime
					mode := goalPrecis.FileMode
					if mode == 0 {
						mode = 0600
					}
					err = os.Chmod(localPathToWrite, fs.FileMode(mode))
					panicOn(err)

					err = os.Chtimes(localPathToWrite, time.Time{}, goalPrecis.ModTime)
					panicOn(err)

					if localPathToWrite != localPathToRead {
						//vv("hard linking 4 '%v' <- '%v'", localPathToRead, localPathToWrite)
						panicOn(os.Link(localPathToRead, localPathToWrite))
					}

					//vv("ack back all done: file was truncated to 0 bytes.")

					ackAll := myPeer.NewFragment()
					ackAll.FragSubject = frag.FragSubject
					ackAll.FragOp = OpRsync_FileAllReadAckToGiver
					ackAll.FragPart = int64(bt.bsend + bt.bread)
					bt.bsend += ackAll.Msgsize()
					err = ckt.SendOneWay(ackAll, 0, 0)
					panicOn(err)

					frag = nil
					continue // wait for FIN

				}

			case OpRsync_HereIsFullFileBegin3, OpRsync_HereIsFullFileMore4, OpRsync_HereIsFullFileEnd5:
				if disk == nil {
					//vv("HereIsFullFile: creating disk file localPathToWrite = '%v'", localPathToWrite)
					disk = NewFileToDiskState(localPathToWrite)
					disk.T0 = time.Now()
				}
				isLast := (frag.FragOp == OpRsync_HereIsFullFileEnd5)
				//vv("isLast = %v; frag.FragPart = %v", isLast, frag.FragPart)

				req := ckt.ConvertFragmentToMessage(frag)
				err := disk.WriteOneMsgToFile(req, isLast)

				//vv("%v: (ckt %v) (Taker) disk.WriteOneMsgToFile() -> err = '%v'", name, ckt.Name, err)
				panicOn(err)

				if !isLast {
					frag = nil // gc early.
					req = nil  // gc early.
					continue takerForSelectLoop
				}
				// INVAR: on last, finish up.
				totSum := disk.Blake3hash.SumString()

				clientTotalBlake3sum, ok := frag.GetUserArg("clientTotalBlake3sum")
				//vv("responder rsyncd ReceiveFileInParts sees last set!"+
				//	" clientTotalBlake3sum='%v'", clientTotalBlake3sum)
				//vv("bytesWrit=%v; \nserver totSum='%v'", disk.BytesWrit, totSum)
				if ok && clientTotalBlake3sum != totSum {
					panic("blake3 checksums disagree!")
				}

				elap := time.Since(disk.T0)
				mb := float64(disk.BytesWrit) / float64(1<<20)
				seconds := (float64(elap) / float64(time.Second))
				rate := mb / seconds

				// match the mode/mod time of the source.
				if !syncReq.TakerStartsEmpty {
					mode := syncReq.GiverFileMode
					if mode == 0 {
						mode = 0600
					}
					err = os.Chmod(localPathToWrite, fs.FileMode(mode))
					panicOn(err)

					err = os.Chtimes(localPathToWrite, time.Time{}, syncReq.GiverModTime)
					panicOn(err)
				} else {
					// try to use what the remote told us.
					modeString, ok := frag.GetUserArg("mode")
					if ok {
						mode, err := strconv.ParseUint(modeString, 10, 32)
						err = os.Chmod(localPathToWrite, fs.FileMode(mode))
						panicOn(err)
					}
					modTimeString, ok := frag.GetUserArg("modTime")
					if ok {
						modTime, err := time.Parse(time.RFC3339Nano, modTimeString)
						panicOn(err)
						err = os.Chtimes(localPathToWrite, time.Time{}, modTime)
						panicOn(err)
					}
				}

				ackAll := myPeer.NewFragment()
				ackAll.FragSubject = frag.FragSubject
				ackAll.FragOp = OpRsync_FileAllReadAckToGiver
				ackAll.FragPart = int64(bt.bsend + bt.bread)
				ackAll.SetUserArg("serverTotalBlake3sum", totSum)
				ackAll.SetUserArg("clientTotalBlake3sum", clientTotalBlake3sum)

				// finally reply to the original caller.
				ackAll.Payload = []byte(fmt.Sprintf("got upcall at '%v' => "+
					"elap = %v\n (while mb=%v) => %v MB/sec. ; \n bytesWrit=%v;",
					disk.T0, elap, mb, rate, disk.BytesWrit))

				bt.bsend += ackAll.Msgsize()
				err = ckt.SendOneWay(ackAll, 0, 0)
				frag = nil
				ackAll = nil
				continue // wait for fin ack back.

			case OpRsync_RequestRemoteToTake, OpRsync_ToGiverSizeMatchButCheckHashAck:

				if frag.FragOp == OpRsync_ToGiverSizeMatchButCheckHashAck {
					b3sumGiver, ok := frag.GetUserArg("giverFullFileBlake3sum")
					if !ok {
						panic("must have set giverFullFileBlake3sum user arg")
					}
					b3sumTaker, ok := frag.GetUserArg("takerFullFileBlake3sum")
					if !ok {
						panic("must have set takerFullFileBlake3sum user arg")
					}
					if b3sumTaker == b3sumGiver {
						//vv("contents same, just modtime needs update: '%v'",
						//	localPathToWrite)

						// hard link it
						if localPathToWrite != localPathToRead {
							//vv("hard linking 8 '%v' <- '%v'", localPathToRead, localPathToWrite)
							panicOn(os.Link(localPathToRead, localPathToWrite))
						}
						err := os.Chtimes(localPathToWrite, time.Time{},
							syncReq.GiverModTime)
						panicOn(err)
						s.ackBackFINToGiver(ckt, frag)
						frag = nil
						continue
					}
					//vv("drat: modTime update will not suffice for localPathToWrite = '%v'; on OpRsync_ToGiverSizeMatchButCheckHashAck", localPathToWrite)
				}

				// then syncReq is already set, just pick up where
				// we left off.
				if syncReq == nil {
					syncReq = &RequestToSyncPath{}
					_, err := syncReq.UnmarshalMsg(frag.Payload)
					panicOn(err)
					syncReq.Done = idem.NewIdemCloseChan()
					bt.bread += len(frag.Payload)

					//vv("OpRsync_RequestRemoteToTake sees: \nsyncReq.TakerPath=: '%v';\nTakerTempDir = '%v';\nGiverPath='%v'\nGiverDirAbs='%v';\nTopTakerDirFinal='%v'", syncReq.TakerPath, syncReq.TakerTempDir, syncReq.GiverPath, syncReq.GiverDirAbs, syncReq.TopTakerDirFinal)

					localPathToWrite = syncReq.TakerPath
					localPathToRead = syncReq.TakerPath

				}

				if syncReq.TakerTempDir != "" {
					localPathToWrite = filepath.Join(
						syncReq.TakerTempDir,
						syncReq.TakerPath)
					//vv("see TakerTempDir='%v', setting localPathToWrite = '%v'", syncReq.TakerTempDir, localPathToWrite)
				}
				if syncReq.TopTakerDirFinal != "" {
					localPathToRead = filepath.Join(
						syncReq.TopTakerDirFinal,
						syncReq.TakerPath)
				}

				if syncReq.GiverScanFlags&ScanFlagIsSymLink != 0 {
					//vv("syncReq is for a symlink")
					s.takeSymlink(syncReq, localPathToWrite)
					s.ackBackFINToGiver(ckt, frag)
					frag = nil
					continue
				}
				//vv("syncReq is not for a symlink: '%#v'", syncReq)

				// use Lstat so we can over-write symlinks,
				// without mistaking them for directories.
				fi, err := os.Lstat(localPathToRead)
				readPathIsSymlink := (err == nil) && fi.Mode()&fs.ModeSymlink != 0

				if readPathIsSymlink {
					panic(fmt.Sprintf("what to do if localPathToRead is a symlink? '%v'", localPathToRead))
				}

				existsFile := (err == nil) && !fi.IsDir()
				if err == nil && fi.IsDir() {

					// this means we are replacing a directory
					// with a file. That's okay (under dirtaker).
					// Just ignore the existing directory and
					// write the new file into the new tmp dir write image.

					// if we are just a single file over writing a dir,
					// then complain.
					if localPathToWrite == localPathToRead {
						panic(fmt.Errorf("error in Taker OpRsync_RequestRemoteToTake: syncReq.TakerPath cannot be an existing directory: localPathToRead='%v'\n\n syncReq = '%#v'", localPathToRead, syncReq))
					}
				}

				if existsFile { // fileExists(localPathToRead) {
					//vv("path '%v' already exists! let's see if we need to rsync diffs or not at all!", syncReq.TakerPath)

					// are we on the same host? avoid overwritting self with self!
					cwd, err := os.Getwd()
					panicOn(err)
					absCwd, err := filepath.Abs(cwd)
					panicOn(err)

					if syncReq.SyncFromHostCID == rpc.HostCID &&
						syncReq.GiverDirAbs == absCwd &&
						syncReq.GiverPath == syncReq.TakerPath {

						skip := myPeer.NewFragment()
						skip.FragSubject = frag.FragSubject
						skip.Typ = rpc.CallPeerError
						skip.Err = fmt.Sprintf("same host and dir detected! cowardly refusing to overwrite path with itself: path='%v'; on '%v' / Hostname '%v'", syncReq.TakerPath, syncReq.ToRemoteNetAddr, rpc.Hostname)
						//vv(skip.Err)
						bt.bsend += skip.Msgsize()
						err = ckt.SendOneWay(skip, 0, 0)
						panicOn(err)

						frag = nil
						skip = nil
						continue // wait for giver to close on error
					}

					fi, err := os.Stat(localPathToRead)
					panicOn(err)
					sz, mod, mode := fi.Size(), fi.ModTime(), uint32(fi.Mode())
					if syncReq.GiverFileSize == sz && syncReq.GiverModTime.Equal(mod) {
						//vv("size + modtime match. nothing to do, tell Giver.")

						s.contentsMatch(syncReq, ckt, frag, mode,
							localPathToRead, localPathToWrite, bt)
						// all done with this file. still wait for FIN
						// for consistency
						s.ackBackFINToGiver(ckt, frag)
						frag = nil
						continue
					}
					//vv("syncReq.GiverFileSize(%v) vs sz(%v) && syncReq.GiverModTime(%v) vs mod(%v))", syncReq.GiverFileSize, sz, syncReq.GiverModTime, mod)

					// we have some differences--at least the modtime.

					if syncReq.GiverFileSize == sz && localPathToReadBlake3sum == "" {
						// First time here, because otherwise
						// when localPathToReadBlake3sum != "" we know
						// we have a checksum mismatch even though the
						// file size may be the same.

						// Oh nice: same *size* file. So
						// start by just doing a fast blake3.HashFile
						// of the whole file; maybe we can still not
						// send data...
						sum, _, err := blake3.HashFile(localPathToRead)
						panicOn(err)
						b3sum := myblake3.RawSumBytesToString(sum)
						localPathToReadBlake3sum = b3sum

						definitelySameContent := false
						definitelyNotSameContent := false

						if syncReq.GiverFullFileBlake3 != "" {
							if syncReq.GiverFullFileBlake3 == b3sum {
								definitelySameContent = true
							} else {
								definitelyNotSameContent = true
							}
						} else if syncReq.Precis != nil &&
							syncReq.Precis.FileCry != "" {
							if syncReq.Precis.FileCry == b3sum {
								definitelySameContent = true
							} else {
								definitelyNotSameContent = true
							}
						}
						if definitelySameContent {
							// we have the file contents!
							// just update the mod time.

							// link in file, tell giver we are cool.
							s.contentsMatch(syncReq, ckt, frag, mode,
								localPathToRead, localPathToWrite, bt)
							// do this after the hard link is made:
							err = os.Chtimes(localPathToWrite, time.Time{},
								syncReq.GiverModTime)
							panicOn(err)
							// all done with this file. still wait for FIN
							// for consistency
							s.ackBackFINToGiver(ckt, frag)
							frag = nil
							continue

						} else if !definitelyNotSameContent {
							// Didn't have full file hash available.
							// Request the file hash first.
							// Since content chunking is slow, this
							// might save us alot of work.
							check := myPeer.NewFragment()
							check.FragOp = OpRsync_ToGiverSizeMatchButCheckHash
							check.SetUserArg("takerFullFileBlake3sum", b3sum)
							check.FragSubject = frag.FragSubject
							bt.bsend += check.Msgsize()

							err = ckt.SendOneWay(check, 0, 0)
							panicOn(err)

							// giver will send
							// OpRsync_ToGiverSizeMatchButCheckHashAck with
							// SetUserArg("giverFullFileBlake3sum", b3sumGiver)
							continue

						} else {
							// different content, proceed below
							// to GetHashesOneByOne
						}
					}
					// end if syncReq.GiverFileSize == sz &&
					//    localPathToReadBlake3sum == ""
				} // end if exists file

				// now we use this for even non-existant files,
				// in order to get RLE0 chunking benefits.

				var precis *FilePrecis
				const wantChunks = true
				const keepData = false

				if parallelChunking {
					//vv("calling ChunkFile")
					precis, local, err = ChunkFile(localPathToRead)
				} else {
					//vv("calling GetHashesOneByOne")
					precis, local, err = GetHashesOneByOne(rpc.Hostname,
						localPathToRead)
				}
				panicOn(err)

				if len(local.Chunks) == 0 {
					// empty or non-existant file
				}

				light := LightRequest{
					SenderPath:   syncReq.TakerPath,
					ReaderPrecis: precis,
					//ReaderChunks: local, // too large from a 3GB file. Send after.
					ReaderChunks: &Chunks{
						Path:     local.Path,
						FileSize: local.FileSize,
						FileCry:  local.FileCry,
					},
				}

				//vv("precis = '%v'", precis)
				bts, err := light.MarshalMsg(nil)
				panicOn(err)

				pre := myPeer.NewFragment()
				pre.FragSubject = frag.FragSubject
				pre.FragOp = OpRsync_LightRequestEnclosed
				pre.Payload = bts
				err = ckt.SendOneWay(pre, 0, 0)
				panicOn(err)
				bt.bsend += len(bts)

				if precis.FileSize > 0 {
					// send local in Chunks, else we will get
					// ErrTooLarg sending all of local/ReaderChunks
					// ? should this be packAndSendChunksJustInTime ?
					err0 = s.packAndSendChunksLimitedSize(
						local,
						frag.FragSubject,
						OpRsync_RequestRemoteToGive_ChunksLast,
						OpRsync_RequestRemoteToGive_ChunksMore,
						ckt,
						bt,
						light.SenderPath,
						syncReq,
					)
					panicOn(err0)
				}

				// while waiting for data...
				localMap = getCryMap(local) // pre-index them for the update.
				frag = nil
				continue // wait for next data fragment

				// We used to just send the whole file if taker
				// did not have it; but now we
				// still do the chunking to take advantage
				// of RLE0 compresion, which can be substantial.
				// comment out since RLE0 now always desired.
				// if !existsFile && !useRLE0 {
				// 	// not present

				// 	//vv("not present: must request the "+
				// 	//	"full file for syncReq.TakerPath='%v'",
				// 	//	syncReq.TakerPath)

				// 	fullReq := myPeer.NewFragment()
				// 	fullReq.FragOp = OpRsync_ToGiverNeedFullFile2

				// 	bt.bsend += fullReq.Msgsize()
				// 	err := ckt.SendOneWay(fullReq, 0, 0)
				// 	panicOn(err)
				// 	frag = nil
				// 	fullReq = nil
				// 	continue
				// }
			} // end switch FragOp

		case fragerr := <-ckt.Errors:
			//vv("%v: (ckt '%v') Taker fragerr = '%v'", name, ckt.Name, fragerr)
			_ = fragerr

			if fragerr != nil {
				// do we want to set syncReq.Err? No, the defer will do it.
				if fragerr.Err != "" {
					return fmt.Errorf("%v", fragerr.Err)
				}
				panic(fmt.Sprintf("unhandled ckt.Errors in taker: '%v'", fragerr))
			}
			panic(fmt.Sprintf("what does a nil fragerr from ckt.Errors mean!?!?"+
				" when do we see it? (%v: (ckt '%v') Taker fragerr = '%v'",
				name, ckt.Name, fragerr))
			return
		case <-done:
			//vv("%v: (ckt '%v') done! cause: '%v'", name, ckt.Name, context.Cause(ckt.Context))
			return
		case <-done0:
			////vv("%v: (ckt '%v') done0! reason: '%v'", name, ckt.Name, context.Cause(ctx0))
			return
			//case <-s.halt.ReqStop.Chan:
			//zz("%v: (ckt '%v') top func halt.ReqStop seen", name, ckt.Name)
			//	return
		case <-ckt.Halt.ReqStop.Chan:
			//vv("%v: (ckt '%v') (Taker) ckt halt requested.", name, ckt.Name)
			return
		}
	}

}

func (s *SyncService) contentsMatch(syncReq *RequestToSyncPath, ckt *rpc.Circuit, frag *rpc.Fragment, mode uint32, localPathToRead, localPathToWrite string, bt *byteTracker) {
	// but do match mode too
	if mode != 0 && syncReq.GiverFileMode != mode && syncReq.GiverFileMode != 0 {
		err := os.Chmod(localPathToWrite, fs.FileMode(syncReq.GiverFileMode))
		panicOn(err)
	}

	if localPathToWrite != localPathToRead {
		//vv("hard linking 1 '%v' <- '%v'",
		//	localPathToRead, localPathToWrite)
		panicOn(os.Link(localPathToRead, localPathToWrite))
	}

	ack := ckt.LpbFrom.NewFragment()
	ack.FragSubject = frag.FragSubject
	ack.FragOp = OpRsync_FileSizeModTimeMatch

	bt.bsend += ack.Msgsize()
	err := ckt.SendOneWay(ack, 0, 0)
	panicOn(err)
}

func (s *SyncService) takeSymlink(syncReq *RequestToSyncPath, localPathToWrite string) {

	targ := syncReq.GiverSymLinkTarget
	//vv("installing symlink '%v' -> '%v'; syncReq.GiverModTime = '%v'", localPathToWrite, targ, syncReq.GiverModTime.Format(time.RFC3339Nano))
	if syncReq.DryRun {
		alwaysPrintf("dry: would remove '%v' and put symlink to '%v' there", localPathToWrite, targ)
		return
	}

	os.Remove(localPathToWrite)
	err := os.Symlink(targ, localPathToWrite)
	panicOn(err)

	//
	// "Lutimes sets the access and modification times tv
	//  on path. If path refers to a symlink, it is not
	//  dereferenced and the timestamps are set on the
	//  symlink. If tv is nil, the access and modification
	//  times are set to the current time. Otherwise tv
	//  must contain exactly 2 elements, with access time
	//  as the first element and modification time as the
	//  second element."
	//
	updateLinkModTime(localPathToWrite, syncReq.GiverModTime)
}

// get current position in file fd.
func curpos(fd *os.File) int64 {
	pos, err := fd.Seek(0, io.SeekCurrent)
	panicOn(err)
	return int64(pos)
}
