package jsync

import (
	"bufio"
	"context"
	"fmt"
	myblake3 "github.com/glycerine/rpc25519/hash"
	//"github.com/glycerine/rpc25519/progress"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	//"sync"
	"time"

	"github.com/glycerine/idem"
	rpc "github.com/glycerine/rpc25519"
	"lukechampine.com/blake3"
)

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

	////vv("SyncService.Taker top")

	name := myPeer.PeerServiceName
	_ = name // used when logging is on.

	bt := &byteTracker{}

	defer func(syncReq *RequestToSyncPath) {
		vv("%v: (ckt '%v') defer running! finishing Taker; syncReq=%p; err0='%v'", name, ckt.Name, syncReq, err0)
		////vv("bt = '%#v'", bt)

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
		ckt.Close(err0)

		// suppress context cancelled shutdowns
		if r := recover(); r != nil {
			switch x := r.(type) {
			case error:
				if strings.Contains(x.Error(), "connection reset") {
					// ok
					return
				}
			}
			if r != rpc.ErrContextCancelled && r != rpc.ErrHaltRequested {
				panic(r)
			} else {
				//vv("Taker suppressing rpc.ErrContextCancelled or ErrHaltRequested, this is normal shutdown.")
			}
		}
	}(syncReq)

	done0 := ctx0.Done()
	done := ckt.Context.Done()

	var disk *FileToDiskState

	var goalPrecis *FilePrecis
	var local *Chunks
	var plan *Chunks
	var senderPlan *SenderPlan
	var localMap map[string]*Chunk
	var localPathToWrite string
	var origVersFd *os.File

	// working buffer to read local file chunks into.
	buf := make([]byte, rpc.UserMaxPayload+10_000)
	var newversBufio *bufio.Writer
	var newversFd *os.File
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
			////vv("local Chunks: have DataPresent()= '%v'", local.DataPresent())
		} else {
			/* // this is redundant with the send in service.go:373
			// client did not give us Chunks, hoping for an
			// efficient file size + modTime win.
			// send to giver OpRsync_LazyTakerWantsToPull
			// to check just those meta data.
			//vv("client gave no Chunks on pull, sending OpRsync_LazyTakerWantsToPull")
			lazy := rpc.NewFragment()
			lazy.FragOp = OpRsync_LazyTakerWantsToPull
			data, err := syncReq.MarshalMsg(nil)
			panicOn(err)
			lazy.Payload = data
			err = ckt.SendOneWay(lazy, 0)
			panicOn(err)
			*/
		}
		localPathToWrite = syncReq.TakerPath
		if dirExists(localPathToWrite) {
			return fmt.Errorf("error in Taker: syncReq.TakerPath cannot be an existing directory: '%v'", localPathToWrite)
		}

		// prep for reading data
		var err error
		if fileExists(syncReq.TakerPath) {
			origVersFd, err = os.Open(syncReq.TakerPath)
			panicOn(err)
			defer origVersFd.Close()
		}
	}

	// this is the Taker side
takerForSelectLoop:
	for {
		select {
		case frag := <-ckt.Reads:
			//vv("%v: (ckt %v) (Taker) ckt.Reads sees frag:'%s'", name, ckt.Name, frag)
			_ = frag
			switch frag.FragOp {

			///////////////// begin dir sync stuff

			case OpRsync_DirSyncBeginToTaker:
				vv("%v: (ckt '%v') (Taker) sees OpRsync_DirSyncBeginToTaker.", name, ckt.Name)
				// we should: setup a top tempdir and tell the
				// giver the path so they can send new files into that path.

				// reply with OpRsync_DirSyncBeginReplyFromTaker
				// and the new top tempdir path, even if
				// we initiated and they already know the path; just repeat it
				// for simplicity/reusing the flow.

			case OpRsync_DirSyncEndToTaker:
				vv("%v: (ckt '%v') (Taker) sees OpRsync_DirSyncEndToTaker", name, ckt.Name)
				// we (taker) can rename the temp top dir/replace any old top dir.

				// reply with OpRsync_DirSyncEndAckFromTaker, wait for FIN.

			case OpRsync_GiverSendsTopDirListing, OpRsync_GiverSendsTopDirListingMore, OpRsync_GiverSendsTopDirListingEnd:
				vv("%v: (ckt '%v') (Taker) sees %v.", rpc.FragOpDecode(frag.FragOp), name, ckt.Name)
				// Getting this means here is the starting dir tree from giver.
				// or, to me (taker), here is more dir listing
				// or, to me (taker), here is end dir listing

				// reply to OpRsync_GiverSendsTopDirListingEnd
				// with OpRsync_TakerReadyForDirContents

			///////////////// end dir sync stuff

			case OpRsync_AckBackFIN_ToTaker:
				////vv("%v: (ckt '%v') (Taker) sees OpRsync_AckBackFIN_ToTaker. returning.", name, ckt.Name)
				return

			case OpRsync_LazyTakerNoLuck_ChunksRequired:
				//vv("%v: (ckt '%v') (Taker) sees OpRsync_LazyTakerNoLuck_ChunksRequired.", name, ckt.Name)
				// should we just be overwriting syncReq ?
				syncReq2 := &RequestToSyncPath{}
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
					////vv("lazy taker no luck: good. syncReq2.TakerPath = '%v'; GiverPath='%v'", syncReq2.TakerPath, syncReq2.GiverPath)
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
				beginAgain := rpc.NewFragment()
				beginAgain.FragSubject = frag.FragSubject
				beginAgain.FragOp = OpRsync_RequestRemoteToGive // 12
				beginAgain.Payload = bts
				err = ckt.SendOneWay(beginAgain, 0)
				panicOn(err)

				_, takerChunks, err := GetHashesOneByOne(rpc.Hostname, syncReq2.TakerPath)
				panicOn(err)

				err = s.packAndSendChunksLimitedSize(
					takerChunks,
					frag.FragSubject,
					OpRsync_RequestRemoteToGive_ChunksLast,
					OpRsync_RequestRemoteToGive_ChunksMore,
					ckt,
					bt,
					syncReq2.TakerPath,
				)
				panicOn(err)

				// overlap network and disk by indexing while we wait.
				localMap = getCryMap(takerChunks)

				frag = nil // GC early
				// no, cannot reply to client if we do this: syncReq = syncReq2
				continue

			case OpRsync_FileSizeModTimeMatch:
				//vv("%v: (ckt '%v') (Taker) sees OpRsync_FileSizeModTimeMatch. sending ack back FIN", name, ckt.Name)
				s.ackBackFINToGiver(ckt, frag)
				if syncReq != nil {
					syncReq.SizeModTimeMatch = true
				}
				frag = nil
				continue // wait for FIN

			case OpRsync_TellTakerToDelete: // part of pull
				//vv("%v: (ckt %v) (Taker) sees OpRsync_TellTakerToDelete. deleting '%v'", name, ckt.Name, syncReq.TakerPath)
				err := os.Remove(syncReq.TakerPath)
				panicOn(err)
				s.ackBackFINToGiver(ckt, frag)
				frag = nil
				continue // wait for FIN

			case OpRsync_ToTakerMetaUpdateAtLeast: // part of pull. not on yet.
				//vv("%v: (ckt %v) (Taker) sees OpRsync_ToTakerMetaUpdateAtLeast. updating mode/modTime on '%v'", name, ckt.Name, syncReq.TakerPath)
				precis := &FilePrecis{}
				_, err := precis.UnmarshalMsg(frag.Payload)
				panicOn(err)

				path := syncReq.TakerPath
				mode := precis.FileMode
				if mode == 0 {
					mode = 0600
				}
				err = os.Chmod(path, fs.FileMode(mode))
				panicOn(err)
				if !precis.ModTime.IsZero() {
					err = os.Chtimes(path, time.Time{}, precis.ModTime)
					panicOn(err)
				}
				s.ackBackFINToGiver(ckt, frag)
				frag = nil
				continue // wait for FIN.

			case OpRsync_ToGiverNeedFullFile2:
				panic("OpRsync_ToGiverNeedFullFile2 not expected in Taker!")

			case OpRsync_HeavyDiffChunksEnclosed, OpRsync_HeavyDiffChunksLast:
				////vv("stream of heavy diffs arriving! : %v", frag.String())

				chunks := &Chunks{}
				_, err := chunks.UnmarshalMsg(frag.Payload)
				panicOn(err)
				bt.bread += len(frag.Payload)

				// this is UpdateLocalWithRemoteDiffs() in pieces,
				// incrementally as Chunks arrive

				// stream through a bufio to disk, rather than holding all in memory.
				if newversFd == nil {

					rnd := cryRandBytesBase64(16)
					tmp = localPathToWrite + "_accept_plan_tmp_" + rnd
					newversFd, err = os.Create(tmp)
					panicOn(err)
					newversBufio = bufio.NewWriterSize(newversFd, rpc.UserMaxPayload)
					// remember to Flush and Close!
					defer newversBufio.Flush() // must be first
					defer newversFd.Close()

					// prep local file too, for seeking to chunks.
					if origVersFd == nil {
						if fileExists(syncReq.TakerPath) {
							origVersFd, err = os.Open(syncReq.TakerPath)
							panicOn(err)
							defer origVersFd.Close()
						}
					}
				}

				// compute the full file hash/checksum as we go

				// remote gives the plan of what to create
				for _, chunk := range chunks.Chunks {

					if len(chunk.Data) == 0 {
						// the data is local
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
							////vv("read from original on disk, for chunk '%v'", chunk)
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

						wb, err := newversBufio.Write(data)
						panicOn(err)

						j += wb
						if wb != len(data) {
							panic("short write?!?!")
						}
						// sanity check the local chunk as a precaution.
						if wb != lc.Endx-lc.Beg {
							panic(fmt.Sprintf("lc.Endx = %v, lc.Beg = %v, but "+
								"lc.Data len = %v", lc.Endx, lc.Beg, wb))
						} // panic: lc.Endx = 2992124, lc.Beg = 2914998, but lc.Data len = 0
						h.Write(data) // update checksum
					} else {
						// INVAR: len(chunk.Data) > 0
						wb, err := newversBufio.Write(chunk.Data)
						panicOn(err)

						j += wb
						if wb != len(chunk.Data) {
							panic("short write!?!!")
						}
						// sanity check the local chunk as a precaution.
						if wb != chunk.Endx-chunk.Beg {
							panic(fmt.Sprintf("lc.Endx = %v, lc.Beg = %v, but "+
								"lc.Data len = %v", chunk.Endx, chunk.Beg, wb))
						}
						h.Write(chunk.Data)
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
					return err
				}

				newversBufio.Flush() // must be before newversFd.Close()
				newversFd.Close()

				err = os.Rename(tmp, localPathToWrite)
				panicOn(err) // rename _accept_plan_tmp_BNu4UZ796shgiyp_yQLXmg== : no such file or directory
				//vv("synced to disk: localPathToWrite='%v' -> renamed to '%v'", tmp, localPathToWrite)

				// restore mode, modtime
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
				ackAll := rpc.NewFragment()
				ackAll.FragSubject = frag.FragSubject
				ackAll.FragOp = OpRsync_FileAllReadAckToGiver
				ackAll.FragPart = int64(bt.bsend + bt.bread)
				err = ckt.SendOneWay(ackAll, 0)
				panicOn(err)
				frag = nil
				// wait for ack back FIN

			case OpRsync_SenderPlanEnclosed:
				////vv("stream of heavy diffs arriving! : %v", frag.String())

				senderPlan = &SenderPlan{} // response
				_, err := senderPlan.UnmarshalMsg(frag.Payload)
				panicOn(err)
				bt.bread += len(frag.Payload)

				if senderPlan.FileIsDeleted {
					//vv("senderPlan.FileIsDeleted true, deleting path '%v'", localPathToWrite)
					err = os.Remove(localPathToWrite)
					s.ackBackFINToGiver(ckt, frag)
					frag = nil
					continue // wait for other side to close
				}

				// plan has no actual Data, just ranges+hashes of the goal file.
				// actually now it does not even have chunks; they were
				// too big and redundant anyway.
				plan = senderPlan.SenderChunksNoSlice
				goalPrecis = senderPlan.SenderPrecis

				if plan.FileSize == 0 {
					////vv("plan.FileSize == 0 => truncate to zero localPathToWrite='%v'", localPathToWrite)
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

					////vv("ack back all done: file was truncated to 0 bytes.")

					ackAll := rpc.NewFragment()
					ackAll.FragSubject = frag.FragSubject
					ackAll.FragOp = OpRsync_FileAllReadAckToGiver
					ackAll.FragPart = int64(bt.bsend + bt.bread)
					err = ckt.SendOneWay(ackAll, 0)
					panicOn(err)
					frag = nil
					continue // wait for FIN

				}

				// actually we didn't use the plan for chunks anyway;
				// heavy has it all.
				/*if len(plan.Chunks) == 0 {
					panic(fmt.Sprintf("missing plan chunks for non-size-zero file '%v'", localPathToWrite))
				}

				// make sure we have a full plan, not just a partial diff.
				if plan.FileSize != plan.Last().Endx {
					panic("plan was not a full plan for every byte!")
				}
				*/

			case OpRsync_HereIsFullFileBegin3, OpRsync_HereIsFullFileMore4, OpRsync_HereIsFullFileEnd5:
				if disk == nil {
					disk = NewFileToDiskState(localPathToWrite)
					disk.T0 = time.Now()
				}
				isLast := (frag.FragOp == OpRsync_HereIsFullFileEnd5)
				//////vv("isLast = %v; frag.FragPart = %v", isLast, frag.FragPart)

				req := ckt.ConvertFragmentToMessage(frag)
				err := disk.WriteOneMsgToFile(req, isLast)

				////vv("%v: (ckt %v) (Taker) disk.WriteOneMsgToFile() -> err = '%v'", name, ckt.Name, err)
				panicOn(err)
				if err != nil {
					return
				}
				if !isLast {
					frag = nil // gc early.
					req = nil  // gc early.
					continue takerForSelectLoop
				}
				// finish up.
				totSum := disk.Blake3hash.SumString()

				clientTotalBlake3sum, ok := frag.GetUserArg("clientTotalBlake3sum")
				//////vv("responder rsyncd ReceiveFileInParts sees last set!"+
				//	" clientTotalBlake3sum='%v'", clientTotalBlake3sum)
				//////vv("bytesWrit=%v; \nserver totSum='%v'", disk.BytesWrit, totSum)
				if ok && clientTotalBlake3sum != totSum {
					panic("blake3 checksums disagree!")
				}

				elap := time.Since(disk.T0)
				mb := float64(disk.BytesWrit) / float64(1<<20)
				seconds := (float64(elap) / float64(time.Second))
				rate := mb / seconds

				// match the mode/mod time of the source.
				//localPath, _ := frag.GetUserArg("readFile")
				localPath := syncReq.TakerPath
				if dirExists(localPath) {
					return fmt.Errorf("error in HereIsFullFile op: syncReq.TakerPath cannot be an existing directory: '%v'", localPath)
				}

				if !syncReq.TakerStartsEmpty {
					mode := syncReq.FileMode
					if mode == 0 {
						mode = 0600
					}
					err = os.Chmod(localPath, fs.FileMode(mode))
					panicOn(err)

					err = os.Chtimes(localPath, time.Time{}, syncReq.ModTime)
					panicOn(err)
				} else {
					// try to use what the remote told us.
					modeString, ok := frag.GetUserArg("mode")
					if ok {
						mode, err := strconv.ParseUint(modeString, 10, 32)
						err = os.Chmod(localPath, fs.FileMode(mode))
						panicOn(err)
					}
					modTimeString, ok := frag.GetUserArg("modTime")
					if ok {
						modTime, err := time.Parse(time.RFC3339Nano, modTimeString)
						panicOn(err)
						err = os.Chtimes(localPath, time.Time{}, modTime)
						panicOn(err)
					}
				}

				ackAll := rpc.NewFragment()
				ackAll.FragSubject = frag.FragSubject
				ackAll.FragOp = OpRsync_FileAllReadAckToGiver
				ackAll.FragPart = int64(bt.bsend + bt.bread)
				ackAll.SetUserArg("serverTotalBlake3sum", totSum)
				ackAll.SetUserArg("clientTotalBlake3sum", clientTotalBlake3sum)

				// finally reply to the original caller.
				ackAll.Payload = []byte(fmt.Sprintf("got upcall at '%v' => "+
					"elap = %v\n (while mb=%v) => %v MB/sec. ; \n bytesWrit=%v;",
					disk.T0, elap, mb, rate, disk.BytesWrit))

				err = ckt.SendOneWay(ackAll, 0)
				frag = nil
				ackAll = nil
				continue // wait for fin ack back.

			case OpRsync_RequestRemoteToTake:

				if syncReq != nil {
					panic("duplicate OpRsync_RequestRemoteToTake ! already started on file")
				}
				syncReq = &RequestToSyncPath{}
				_, err := syncReq.UnmarshalMsg(frag.Payload)
				panicOn(err)
				syncReq.Done = idem.NewIdemCloseChan()
				bt.bread += len(frag.Payload)

				////vv("server sees RequestToSyncPath: '%#v'", syncReq)

				localPathToWrite = syncReq.TakerPath
				if dirExists(localPathToWrite) {
					return fmt.Errorf("error in Taker OpRsync_RequestRemoteToTake: syncReq.TakerPath cannot be an existing directory: '%v'", localPathToWrite)
				}

				if fileExists(syncReq.TakerPath) {
					////vv("path '%v' already exists! let's see if we need to rsync diffs or not at all!", syncReq.TakerPath)

					// are we on the same host? avoid overwritting self with self!
					cwd, err := os.Getwd()
					panicOn(err)
					absCwd, err := filepath.Abs(cwd)
					panicOn(err)

					if syncReq.SyncFromHostCID == rpc.HostCID &&
						syncReq.AbsDir == absCwd &&
						syncReq.GiverPath == syncReq.TakerPath {

						skip := rpc.NewFragment()
						skip.FragSubject = frag.FragSubject
						skip.Typ = rpc.CallPeerError
						skip.Err = fmt.Sprintf("same host and dir detected! cowardly refusing to overwrite path with itself: path='%v'; on '%v' / Hostname '%v'", syncReq.TakerPath, syncReq.ToRemoteNetAddr, rpc.Hostname)
						////vv(skip.Err)
						err = ckt.SendOneWay(skip, 0)
						panicOn(err)
						// return // all done
						frag = nil
						skip = nil
						continue // wait for giver to close on error
					}

					fi, err := os.Stat(syncReq.TakerPath)
					panicOn(err)
					sz, mod, mode := fi.Size(), fi.ModTime(), uint32(fi.Mode())
					if syncReq.FileSize == sz && syncReq.ModTime.Equal(mod) {
						//vv("size + modtime match. nothing to do, tell Giver.")
						// but do match mode too
						if syncReq.FileMode != mode && syncReq.FileMode != 0 {
							err = os.Chmod(syncReq.TakerPath, fs.FileMode(syncReq.FileMode))
							panicOn(err)
						}
						ack := rpc.NewFragment()
						ack.FragSubject = frag.FragSubject
						ack.FragOp = OpRsync_FileSizeModTimeMatch

						err = ckt.SendOneWay(ack, 0)
						panicOn(err)

						// all done with this file. still wait for FIN
						// for consistency
						s.ackBackFINToGiver(ckt, frag)
						frag = nil
						ack = nil
						continue
					}
					//vv("syncReq.FileSize(%v) vs sz(%v) && syncReq.ModTime(%v) vs mod(%v))", syncReq.FileSize, sz, syncReq.ModTime, mod)

					// we have some differences

					var precis *FilePrecis
					const wantChunks = true
					const keepData = false
					precis, local, err = GetHashesOneByOne(rpc.Hostname, syncReq.TakerPath)
					//precis, local, err = SummarizeFileInCDCHashes(rpc.Hostname, syncReq.TakerPath, wantChunks, keepData)

					panicOn(err)

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

					////vv("precis = '%v'", precis)
					bts, err := light.MarshalMsg(nil)
					panicOn(err)

					pre := rpc.NewFragment()
					pre.FragSubject = frag.FragSubject
					pre.FragOp = OpRsync_LightRequestEnclosed
					pre.Payload = bts
					err = ckt.SendOneWay(pre, 0)
					panicOn(err)
					bt.bsend += len(frag.Payload)

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
					)

					// while waiting for data...
					localMap = getCryMap(local) // pre-index them for the update.
					frag = nil
					continue // wait for next data fragment
				} else {
					// not present: must request the full file.

					//path := syncReq.Path
					fullReq := rpc.NewFragment()
					fullReq.FragOp = OpRsync_ToGiverNeedFullFile2
					err := ckt.SendOneWay(fullReq, 0)
					panicOn(err)
					frag = nil
					fullReq = nil
					continue
				}
			} // end switch FragOp

		case fragerr := <-ckt.Errors:
			//vv("%v: (ckt '%v') Taker fragerr = '%v'", name, ckt.Name, fragerr)
			_ = fragerr

			if fragerr != nil {
				// do we want to set syncReq.Err? No, the defer will do it.
				if fragerr.Err != "" {
					return fmt.Errorf(fragerr.Err)
				}
				panic(fmt.Sprintf("unhandled ckt.Errors in taker: '%v'", fragerr))
			}
			panic(fmt.Sprintf("what does a nil fragerr from ckt.Errors mean!?!?"+
				" when do we see it? (%v: (ckt '%v') Taker fragerr = '%v'",
				name, ckt.Name, fragerr))
			return
		case <-done:
			//////vv("%v: (ckt '%v') done! cause: '%v'", name, ckt.Name, context.Cause(ckt.Context))
			return
		case <-done0:
			////////vv("%v: (ckt '%v') done0! reason: '%v'", name, ckt.Name, context.Cause(ctx0))
			return
			//case <-s.halt.ReqStop.Chan:
			//zz("%v: (ckt '%v') top func halt.ReqStop seen", name, ckt.Name)
			//	return
		case <-ckt.Halt.ReqStop.Chan:
			//////vv("%v: (ckt '%v') (Taker) ckt halt requested.", name, ckt.Name)
			return
		}
	}

}
