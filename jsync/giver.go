package jsync

import (
	"context"
	"fmt"
	myblake3 "github.com/glycerine/rpc25519/hash"
	//"github.com/glycerine/rpc25519/progress"
	"io"
	"os"
	"path/filepath"
	//"strconv"
	"strings"
	//"sync"
	"time"

	//"github.com/glycerine/idem"
	"github.com/glycerine/blake3"
	rpc "github.com/glycerine/rpc25519"
)

// Giver wants to send a local file efficiently over
// the wire to the Taker.
//
// When client does -pull, then the local service will run Taker.
// The remote service will run Giver.
//
// When client does (push, the default), the local service will run Giver.
// The remote service will run Taker in that case.
//
// The syncReq pointer in this call will be set when Giver is local.
// It will be nil when Giver is remote.
func (s *SyncService) Giver(ctx0 context.Context, ckt *rpc.Circuit, myPeer *rpc.LocalPeer, syncReq *RequestToSyncPath) (err0 error) {

	//vv("SyncService.Giver top.")

	name := myPeer.PeerServiceName
	_ = name // used when logging is on.

	bt := &byteTracker{}

	// this is the active side, as we called NewCircuitToPeerURL()
	defer func(syncReq *RequestToSyncPath) {
		//vv("%v: Giver() ckt '%v' defer running: shutting down. bt = '%#v'", name, ckt.Name, bt)

		if syncReq != nil {
			syncReq.BytesRead = int64(bt.bread)
			syncReq.BytesSent = int64(bt.bsend)
			if err0 != nil {
				syncReq.Errs = err0.Error()
			}
			if syncReq.Done != nil {
				syncReq.Done.Close() // only after setting .Errs
			}
		}
		ckt.Close(err0)

		// suppress context cancelled shutdowns, and network errors
		if r := recover(); r != nil {
			//vv("giver sees panic: '%v'", r)
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
				alwaysPrintf("giver sees abnormal shutdown panic: '%v'", r)
				panic(r)
			} else {
				//vv("Giver suppressing ErrContextCancelled or ErrHaltRequested, this is normal shutdown.")
			}
		}
	}(syncReq) // capture original pointer or nil, so we are setting Errs for sure

	done0 := ctx0.Done()
	done := ckt.Context.Done()

	var localPath string
	if syncReq != nil {
		localPath = syncReq.GiverPath
	}

	var light *LightRequest

	for {
		select { // 440 hang hung here

		case frag0 := <-ckt.Reads:
			//vv("%v: (ckt '%v') (Giver) saw read frag0:'%v'", name, ckt.Name, frag0)

			//vv("frag0 = '%v'", frag0)
			switch frag0.FragOp {

			case OpRsync_ToGiverSizeMatchButCheckHash:
				b3sumTaker, ok := frag0.GetUserArg("takerFullFileBlake3sum")
				if !ok {
					panic("protocol violated: taker must set fullFileBlake3sum")
				}
				if localPath == "" {
					panic("we need localPath here")
				}
				sum, _, err := blake3.HashFile(localPath)
				panicOn(err)
				b3sumGiver := myblake3.RawSumBytesToString(sum)

				syncReq.GiverFullFileBlake3 = b3sumGiver
				syncReq.TakerFullFileBlake3 = b3sumTaker

				if b3sumGiver == b3sumTaker {
					//vv("early checksum finds no transfer needed. good: '%v'", localPath)
					// we are all good. but we cannot FIN out yet
					// because their mod-time needs updating.

					// giver will send OpRsync_ToGiverSizeMatchButCheckHashAck
					// SetUserArg("giverFullFileBlake3sum", b3sumGiver)
				}
				ack := myPeer.NewFragment()
				ack.FragOp = OpRsync_ToGiverSizeMatchButCheckHashAck
				ack.SetUserArg("giverFullFileBlake3sum", b3sumGiver)
				ack.SetUserArg("takerFullFileBlake3sum", b3sumTaker)
				bt.bsend += ack.Msgsize()
				err = ckt.SendOneWay(ack, 0, 0)
				panicOn(err)
				continue

			case OpRsync_LazyTakerWantsToPull: // FragOp 19
				//vv("%v: (ckt '%v') (Giver) sees OpRsync_LazyTakerWantsToPull", name, ckt.Name)
				syncReq = &RequestToSyncPath{}
				_, err0 = syncReq.UnmarshalMsg(frag0.Payload)
				panicOn(err0)
				bt.bread += len(frag0.Payload)

				// dir sync may specify a path relative to
				// GiverDirAbs
				path := syncReq.GiverPath
				//if syncReq.GiverDirAbs != "" {
				//	path = filepath.Join(syncReq.GiverDirAbs, path)
				//}
				fi, err := os.Stat(path)
				if err != nil {
					// file does not exist
					notfound := myPeer.NewFragment()
					notfound.FragSubject = frag0.FragSubject
					notfound.Typ = rpc.CallPeerError
					//notfound.FragOp =
					notfound.Err = fmt.Sprintf("file access error "+
						"for '%v': '%v' on host '%v'",
						syncReq.GiverPath, err.Error(), rpc.Hostname)
					bt.bsend += notfound.Msgsize()
					err = ckt.SendOneWay(notfound, 0, 0)
					panicOn(err)
					continue
				}
				if fi.IsDir() {
					// taker has a file, but giver has a directory
					// of the same name. Tell taker to call back
					// with dirtaker to dirgiver.

					//vv("drat: taker has file '%v' but on giver it is a dir."+
					//	" Have to restart with DirTaker supervising.", path)

					drat := myPeer.NewFragment()
					drat.FragOp = OpRsync_ToTakerDratGiverFileIsNowDir
					drat.FragSubject = frag0.FragSubject
					bt.bsend += drat.Msgsize()
					err = ckt.SendOneWay(drat, 0, 0)
					panicOn(err)
					// wait for ack back fin so we don't shut them
					// down before they can get dirtaker started.
					continue
				}
				sz, mod := fi.Size(), fi.ModTime()
				if syncReq.TakerFileSize == sz && syncReq.TakerModTime.Equal(mod) {
					//vv("giver: OpRsync_LazyTakerWantsToPull: size + modtime match. nothing to do, tell taker. syncReq.GiverPath = '%v'", syncReq.GiverPath)
					// let the taker know they can stop with this file.
					ack := myPeer.NewFragment()
					ack.FragSubject = frag0.FragSubject
					ack.FragOp = OpRsync_FileSizeModTimeMatch

					bt.bsend += ack.Msgsize()
					err = ckt.SendOneWay(ack, 0, 0)
					panicOn(err)
				} else {
					// tell them they must send the chunks... if they want it.
					//vv("giver responding to OpRsync_LazyTakerWantsToPull with OpRsync_LazyTakerNoLuck_ChunksRequired; setting GiverModTime = '%v'", mod)

					syncReq.GiverModTime = mod
					syncReq.GiverFileSize = sz
					syncReq.GiverFileMode = uint32(fi.Mode())

					// but also send them the checksum in case we can
					// avoid sending a big file just b/c of mod time update.
					sum, _, err := blake3.HashFile(path)
					panicOn(err)
					b3sumGiver := myblake3.RawSumBytesToString(sum)
					syncReq.GiverFullFileBlake3 = b3sumGiver

					ack := myPeer.NewFragment()
					ack.FragSubject = frag0.FragSubject
					ack.FragOp = OpRsync_LazyTakerNoLuck_ChunksRequired
					bts, err := syncReq.MarshalMsg(nil)
					panicOn(err)
					ack.Payload = bts
					//ack.Payload = frag0.Payload // send the syncReq back
					ack.SetUserArg("giverFullFileBlake3sum", b3sumGiver)
					bt.bsend += len(ack.Payload)
					err = ckt.SendOneWay(ack, 0, 0)
					panicOn(err)
				}
				frag0 = nil
				continue // wait for FIN or chunks.

			case OpRsync_AckBackFIN_ToGiver:
				//vv("%v: (ckt '%v') (Giver) sees OpRsync_AckBackFIN_ToGiver. returning.", name, ckt.Name)
				return

			case OpRsync_RequestRemoteToGive: // FragOp 12
				// This is the pull entry point.
				// We, the giver, are the "remote" in this case.
				// The other side, the taker, will be the "local".

				// This is also where local dirgiver gets to
				// for individual file sync.

				// Update: our later addition, for massive efficiency, of
				//
				// OpRsync_LazyTakerWantsToPull and
				// OpRsync_LazyTakerNoLuck_ChunksRequired
				//
				// to lazily _not pull_ if a file size + modTime
				// + one network roundtrip tells us we have no need:
				// means that we can now get here when we have
				// already been started for a remote lazy pull.
				//
				// Which means syncReq might not be nil here.
				// We overwrite it anyway. We are still the remote,
				// so this should not impact byte tracking or
				// progress reporting.

				syncReq = &RequestToSyncPath{}
				_, err0 = syncReq.UnmarshalMsg(frag0.Payload)
				panicOn(err0)
				bt.bread += len(frag0.Payload)

				//localPrecis := syncReq.Precis // not used at the moment.
				//localChunks = syncReq.Chunks
				wireChunks := syncReq.Chunks

				// 0. We might need to wait for more chunks.
				if syncReq.MoreChunksComming {
					//vv("syncReq.MoreChunksComming waiting for more...")
					// get the extra fragments with more []*Chunk
					err0 = s.getMoreChunks(ckt, bt, &wireChunks, done, done0, syncReq, OpRsync_RequestRemoteToGive_ChunksLast, OpRsync_RequestRemoteToGive_ChunksMore)
					//err0 = s.getMoreChunks(ckt, bt, &localChunks, done, done0, syncReq, OpRsync_HeavyDiffChunksLast, OpRsync_HeavyDiffChunksEnclosed)
					panicOn(err0)

				} // end if syncReq.MoreChunksComming
				//vv("no more chunks to wait for...")

				// after moreLoop, we get here:

				// 1. if local has nothing, send full stream. stop.
				// BUT! we want to apply RLE0 even in this case.

				// 2. else: scan our "remote path". updated not needed? ack back FIN
				if !s.remoteGiverAreDiffChunksNeeded(syncReq, ckt, bt, frag0) {
					//vv("giver sees no update needed. done. ackBackFINToTaker and (wait for FIN)")
					s.ackBackFINToTaker(ckt, frag0)
					frag0 = nil // GC early.
					continue    // wait for other side to close
				}

				// 3. compute update plan, send plan, then diff chunks.
				//vv("OpRsync_RequestRemoteToGive calling giverSendsPlanAndDataUpdates")
				s.giverSendsPlanAndDataUpdates(wireChunks, ckt, syncReq.GiverPath, bt, frag0, syncReq)
				//vv("done with s.giverSendsPlanAndDataUpdates. done (wait for FIN/ckt shutdown)")
				// wait for FIN or ckt shutdown, to let data get there.
				frag0 = nil // GC early.
				continue

			case OpRsync_LightRequestEnclosed:

				// For reference, here is what LightRequest is:
				// type LightRequest struct {
				// 	SenderPath string
				// 	ReaderPrecis *FilePrecis
				// 	ReaderChunks *Chunks    // coming next separately
				// }

				light = &LightRequest{}
				_, err0 = light.UnmarshalMsg(frag0.Payload)
				panicOn(err0)
				bt.bread += len(frag0.Payload)

				//vv("giver sees OpRsync_LightRequestEnclosed; light.ReaderChunks = '%#v'", light)

				if light.ReaderPrecis.FileSize > 0 {
					// light.ReaderChunks were too big,
					// so now we always get them in packs instead.
					err0 = s.getMoreChunks(ckt, bt, &light.ReaderChunks,
						done, done0, syncReq,
						OpRsync_RequestRemoteToGive_ChunksLast,
						OpRsync_RequestRemoteToGive_ChunksMore)
					panicOn(err0)
				}
				// else nothing will be sent for size 0 file; as
				// there is nothing (no chunks) to send.

				//vv("OpRsync_LightRequestEnclosed calling giverSendsPlanAndDataUpdates")
				s.giverSendsPlanAndDataUpdates(light.ReaderChunks, ckt, localPath, bt, frag0, syncReq)
				// middle of a sequence, certainly do not return.
				frag0 = nil // GC early.
				continue

			case OpRsync_FileSizeModTimeMatch:
				//vv("giver sees OpRsync_FileSizeModTimeMatch")
				if syncReq != nil {
					syncReq.SizeModTimeMatch = true
				}
				// this is also an Op specific ack back.
				//vv("OpRsync_FileSizeModTimeMatch -> ackBackFINToTaker")
				s.ackBackFINToTaker(ckt, frag0) // probably not needed, but exercise it.
				// wait for other side to close on FIN.
				frag0 = nil // GC early.
				continue

			case OpRsync_FileAllReadAckToGiver:
				//vv("Giver sees OpRsync_FileAllReadAckToGiver for '%v'; about to ackBackFINToTaker", syncReq.GiverPath)

				syncReq.GiverFullFileBlake3, _ = frag0.GetUserArg("clientTotalBlake3sum")
				syncReq.TakerFullFileBlake3, _ = frag0.GetUserArg("serverTotalBlake3sum")
				syncReq.RemoteBytesTransferred = frag0.FragPart

				s.ackBackFINToTaker(ckt, frag0)
				// wait for ckt to close on FIN, not: return
				frag0 = nil // GC early.
				continue

				// case OpRsync_ToGiverNeedFullFile2:
				// 	panic("OpRsync_ToGiverNeedFullFile2 no longer used, b/c we want RLE0 compression support")
				// 	// We no long use this (assuming useRLE0 = true).
				// 	// We chunk all files now to get the RLE0 benefits.
			} // end switch frag0.FragOp

		case fragerr := <-ckt.Errors:
			//vv("%v: (ckt %v) (Giver) ckt.Errors sees fragerr:'%s'", name, ckt.Name, fragerr)
			if syncReq != nil {
				syncReq.Errs = fragerr.Err
			}
			return
		case <-done:
			//vv("%v: (ckt '%v') (Giver) ctx.Done seen. cause: '%v'", name, ckt.Name, context.Cause(ckt.Context))
			return
		case <-done0:
			//vv("%v: (ckt '%v') (Giver) ctx.Done seen. cause: '%v'", name, ckt.Name, context.Cause(ctx0))
			return
		case <-ckt.Halt.ReqStop.Chan:
			//vv("%v: (ckt '%v') (Giver) ckt halt requested.", name, ckt.Name)
			return
		}
	}
}

type byteTracker struct {
	bsend int
	bread int
}

func (s *SyncService) giverReportFileNotFound(
	ckt *rpc.Circuit,
	localPath string,
	bt *byteTracker,
	frag0 *rpc.Fragment,

) error {
	splan := SenderPlan{
		FileIsDeleted: true,
		SenderPath:    localPath,
	}

	pf := ckt.LpbFrom.NewFragment()
	pf.FragSubject = frag0.FragSubject
	pf.FragOp = OpRsync_SenderPlanEnclosed

	report := fmt.Sprintf("giver was asked for non-existent file: '%v' not found;\nstack='%v'", localPath, stack())
	pf.Err = report
	//vv(report)

	bts, err := splan.MarshalMsg(nil)
	panicOn(err)
	pf.Payload = bts
	bt.bsend += len(bts)

	err = ckt.SendOneWay(pf, 0, 0)
	panicOn(err)
	//return fmt.Errorf(report)
	// let the other side close us down, so they hear about this,
	// rather than us shutting down before we can send to them.
	return nil
}

func (s *SyncService) giverSendsPlanAndDataUpdates(
	remoteWantsUpdate *Chunks,
	ckt *rpc.Circuit,
	localPath string,
	bt *byteTracker,
	frag0 *rpc.Fragment,
	syncReq *RequestToSyncPath,
) error {

	// handle the case when file is no longer here; or never was.
	if !fileExists(localPath) {
		return s.giverReportFileNotFound(ckt, localPath, bt, frag0)
	}
	t0 := time.Now()
	_ = t0
	//vv("giverSendsPlanAndDataUpdates top: localPath='%v'", localPath)
	//defer func() {
	//	vv("end giverSendsPlanAndDataUpdates. elap = '%v'", time.Since(t0))
	//}()
	//vv("remoteWantsUpdate DataPresent = %v; should be 0", remoteWantsUpdate.DataPresent())

	// reply with these
	// (maybe) OpRsync_ToTakerMetaUpdateAtLeast(14) (that's all)
	//OpRsync_SenderPlanEnclosed(11)
	//OpRsync_HeavyDiffChunksEnclosed(9)
	//OpRsync_HeavyDiffChunksLast(10)

	// local describes the local file in Chunks.
	// We send it to Taker so they know how
	// to assemble the updated file.
	const keepData = false

	t1 := time.Now()
	_ = t1
	var err error
	var goalPrecis *FilePrecis
	var local *Chunks
	if parallelChunking {
		// parallel version
		//vv("begin ChunkFile (parallel)")
		goalPrecis, local, err = ChunkFile(localPath)
	} else {
		// non-parallel version:
		//vv("begin GetHashesOneByOne")
		//goalPrecis, local, err = GetHashesOneByOne(rpc.Hostname, localPath)
		goalPrecis, local, err = SummarizeFileInCDCHashes(rpc.Hostname, localPath, false)
		// no data, just chunks. read data directly from file below.
		//vv("end GetHashesOneByOne. elap = %v", time.Since(t1))
	}
	panicOn(err)
	//vv("end ChunkFile. elap = %v; local = '%v'; \n goalPrecis = '%v'", time.Since(t1), local, goalPrecis)
	// have UNWRIT here; and PreAllocUnwritBytes

	// are the whole file checksums the same? we can
	// avoid sending back a whole lotta chunks of nothing
	// in this case. A file touch will do this/test this.
	//vv("remoteWantsUpdate = %p", remoteWantsUpdate) // nil in test 440
	//vv("goalPrecis = %v", goalPrecis)               // fine 440

	// Avoid short circuiting if we have
	// UNWRIT; pre-allocated spans, so we can
	// try to re-create them on the other side.
	//
	// NB remoteWantsUpdate is nil when file absent.
	if local.PreAllocUnwritEndx == 0 &&
		(remoteWantsUpdate != nil &&
			remoteWantsUpdate.FileCry == goalPrecis.FileCry) { // was segfault 440
		//vv("we can save on sending chunks! remoteWantsUpdate.FileCry == goalPrecis.FileCry = '%v'. sending back OpRsync_ToTakerMetaUpdateAtLeast", remoteWantsUpdate.FileCry)

		updateMeta := ckt.LpbFrom.NewFragment()
		updateMeta.FragOp = OpRsync_ToTakerMetaUpdateAtLeast // 14
		updateMeta.FragSubject = frag0.FragSubject

		bts, err := goalPrecis.MarshalMsg(nil)
		panicOn(err)
		updateMeta.Payload = bts
		err = ckt.SendOneWay(updateMeta, 0, 0)
		panicOn(err)
		bt.bsend += len(bts)
		return nil
	}

	// index our data chunks in a hash map.
	bs := &BlobStore{
		Map: getCryMap(local),
	}

	const dropPlanData = true // only send what they need.
	const usePlaceHolders = true

	if remoteWantsUpdate == nil {
		// absent file on remote, try to get GetPlanToUpdateFromGoal
		// to give us "everything" locally because Chunks.Chunks is empty...
		// this makes 440 dir_test green.
		remoteWantsUpdate = &Chunks{
			Path: syncReq.TakerPath,
		}
	}

	// old: heavyPlan has all the data diff chunks we want to send;
	// right after sending the light plan. By streaming
	// them separately, we keep the message sizes reasonable.
	//
	// new: placeholderPlan has a single data byte in Chunk.Data
	// to flag us to read the actual data from disk and then
	// send it over the wire. This helps keep memory footprint low.
	placeholderPlan := bs.GetPlanToUpdateFromGoal(remoteWantsUpdate, local, dropPlanData, usePlaceHolders)
	//vv("placeholderPlan = '%v'", placeholderPlan)

	chunksWithDataN := placeholderPlan.DataPresent()
	_ = chunksWithDataN
	//vv("placeholderPlan.DataPresent() = '%v'", chunksWithDataN)

	// can we just skip the lightPlan?
	// Mostly! Turns out it did not need any Chunks!
	// We send them afterwards, in packAndSendChunksJustInTime().
	lightPlan := placeholderPlan.CloneWithNoChunks()

	if goalPrecis == nil {
		panic(fmt.Sprintf("why is goalPrecis nil here?? would cause problems over at taker.go:754 and following"))
	}
	splan := SenderPlan{
		SenderPath:          localPath,
		SenderPrecis:        goalPrecis,
		SenderChunksNoSlice: lightPlan,
	}

	pf := ckt.LpbFrom.NewFragment()
	pf.FragSubject = frag0.FragSubject
	pf.FragOp = OpRsync_SenderPlanEnclosed // 11

	bts, err := splan.MarshalMsg(nil)
	panicOn(err)
	pf.Payload = bts
	bt.bsend += len(bts)

	err = ckt.SendOneWay(pf, 0, 0)
	panicOn(err)

	// Now stream the heavy chunks. Since our max message
	// is typically about 1MB, we'll pack lots of
	// chunks into one message. Some of them will
	// have no attached Data, so will be very small.

	// make this a reusable routine so
	// pulls at the Start can use it too, when
	// the local file is very large.

	//	return s.packAndSendChunksLimitedSize(
	return s.packAndSendChunksJustInTime(
		placeholderPlan,
		frag0.FragSubject,
		OpRsync_HeavyDiffChunksLast,     // 10
		OpRsync_HeavyDiffChunksEnclosed, // 9
		ckt,
		bt,
		placeholderPlan.Path,
		syncReq,
		goalPrecis,
		false,
		nil,
	)
}

func (s *SyncService) remoteGiverAreDiffChunksNeeded(
	syncReq *RequestToSyncPath, // from "local" (not actually local)
	ckt *rpc.Circuit,
	bt *byteTracker,
	frag *rpc.Fragment,

) bool {

	t0 := time.Now()
	_ = t0
	//vv("top of remoteGiverAreDiffChunksNeeded()")
	//defer func() {
	//vv("end remoteGiverAreDiffChunksNeeded() elap = %v", time.Since(t0))
	//}()
	if !fileExists(syncReq.GiverPath) {
		//vv("path '%v' does not exist on Giver: tell Taker to delete their file.", syncReq.GiverPath)
		rm := ckt.LpbFrom.NewFragment()
		rm.FragOp = OpRsync_TellTakerToDelete // 13

		bt.bsend += rm.Msgsize()
		err := ckt.SendOneWay(rm, 0, 0)
		panicOn(err)
		return false // all done
	}
	// are we on the same host? avoid overwritting self with self!
	cwd, err := os.Getwd()
	panicOn(err)
	absCwd, err := filepath.Abs(cwd)
	panicOn(err)

	if syncReq.SyncFromHostCID == rpc.HostCID &&
		syncReq.GiverDirAbs == absCwd &&
		syncReq.GiverPath == syncReq.TakerPath {

		skip := ckt.LpbFrom.NewFragment()
		skip.FragSubject = frag.FragSubject
		skip.Typ = rpc.CallPeerError
		skip.Err = fmt.Sprintf("same host and dir detected! cowardly refusing to overwrite path with itself: '%v' on '%v' / Hostname '%v'", syncReq.GiverPath, syncReq.ToRemoteNetAddr, rpc.Hostname)
		//vv(skip.Err)
		bt.bsend += skip.Msgsize()
		err = ckt.SendOneWay(skip, 0, 0)
		panicOn(err)
		return false // all done
	}

	fi, err := os.Stat(syncReq.GiverPath)
	panicOn(err)
	sz, mod, mode := fi.Size(), fi.ModTime(), uint32(fi.Mode())
	if syncReq.TakerFileSize == sz && syncReq.TakerModTime.Equal(mod) {
		//vv("remoteGiverAreDiffChunksNeeded(): size + modtime match. nothing to do, tell taker. syncReq.GiverPath = '%v'", syncReq.GiverPath)

		// but do match mode too... advanced! leave out for now.
		//
		// Rationale: I'm just not sure we want the expense of doing a full file
		// scan just because the mode bits disagree.
		//
		if false { // too advanced for now. start simpler.
			if syncReq.TakerFileMode != mode {
				updateMeta := ckt.LpbFrom.NewFragment()
				updateMeta.FragOp = OpRsync_ToTakerMetaUpdateAtLeast
				updateMeta.FragSubject = frag.FragSubject

				// computes full-file hash incrementally
				precis, err := GetPrecis(rpc.Hostname, syncReq.GiverPath)
				panicOn(err)

				// send them the full file hash?
				bts, err := precis.MarshalMsg(nil)
				panicOn(err)
				updateMeta.Payload = bts
				bt.bsend += len(bts)
				// returning false will ackBackFINToTaker
				return false // nothing more needed
			}
		}
		// let the taker know they can stop with this file.
		ack := ckt.LpbFrom.NewFragment()
		ack.FragSubject = frag.FragSubject
		ack.FragOp = OpRsync_FileSizeModTimeMatch

		bt.bsend += ack.Msgsize()
		err = ckt.SendOneWay(ack, 0, 0)
		panicOn(err)
		return false // all done with this file.
	} else {
		//vv("forced to update, syncReq.FileSize(%v) != sz(%v) || syncReq.ModTime(%v) != mod = %v", syncReq.FileSize, sz, syncReq.ModTime, mod)
	}

	// we have some differences that need diff chunks
	return true
}

func (s *SyncService) packAndSendChunksLimitedSize(
	heavyPlan *Chunks,
	subject string,
	opLast int,
	opMore int,
	ckt *rpc.Circuit,
	bt *byteTracker,
	path string,
	syncReq *RequestToSyncPath,

) (err error) {

	// called by both taker and giver. But only seen on dir taker.
	//vv("top of packAndSendChunksLimitedSize; n = %v", len(heavyPlan.Chunks))
	//defer vv("end of packAndSendChunksLimitedSize")

	// pack up to max bytes of Chunks into a message.
	max := rpc.UserMaxPayload - 10_000

	t0 := time.Now()
	_ = t0
	n := len(heavyPlan.Chunks)
	//nBytesTot := heavyPlan.DataPresent()
	nBytesTot := heavyPlan.Chunks[n-1].Endx
	_ = nBytesTot
	var tot int64
	_ = tot
	last := false

	//for i, chunk := range heavyPlan.Chunks
	for i := 0; i < n; {
		f := ckt.LpbFrom.NewFragment()
		f.FragSubject = subject

		// use FragPart to give the
		// index of first in the pack.
		// Let the reader verify they are reading
		// properly in sync.
		f.FragPart = int64(i)
		var pack []*Chunk

		have := 0 // how many bytes in the pack

		// use CloneNoData to not leave any memory
		// allocations for .Data sitting around in
		// heavyPlan after this func returns.
		next := heavyPlan.Chunks[i]
		uses := next.Msgsize()
		tot = next.Endx

		for have+uses < max {
			pack = append(pack, next)
			have += uses
			i++
			if i >= n {
				last = true
				break
			}
			next = heavyPlan.Chunks[i]
			uses = next.Msgsize()
			tot = next.Endx
		}
		if last {
			f.FragOp = opLast // OpRsync_HeavyDiffChunksLast
		} else {
			f.FragOp = opMore // OpRsync_HeavyDiffChunksEnclosed
		}
		chnks := &Chunks{
			Chunks: pack,
			Path:   path,
		}
		bts, err := chnks.MarshalMsg(nil)
		panicOn(err)
		f.Payload = bts
		err = ckt.SendOneWay(f, 0, 0)
		panicOn(err)
		//vv("packAndSendChunksLimitedSize sent f = '%v'", f.String())
		bt.bsend += len(bts)

		// taker progress: this is just the plan, about 280 msec, not
		// worth confusing with the heavy chunks so comment out.
		//s.reportProgress(syncReq, filepath.Base(path), int64(nBytesTot), int64(tot), t0)
	}
	return nil
}

// Trying to keep memory footprint low, this
// version of packAndSend uses a light plan instead of heavy,
// with 1 byte .Data marks to know where to read from disk
// just it time to ship over the network. If a Chunk has
// the one byte .Data mark, we read seek it in the
// source file and send it over the wire.
func (s *SyncService) packAndSendChunksJustInTime(
	oneByteMarkedPlan *Chunks,
	subject string,
	opLast int,
	opMore int,
	ckt *rpc.Circuit,
	bt *byteTracker,
	path string,
	syncReq *RequestToSyncPath,
	goalPrecis *FilePrecis,
	sendDataWithoutOneByteMarkers bool,
	lastFragCallBack func(f *rpc.Fragment, blake3hash *myblake3.Blake3),
) (err error) {

	//vv("top of packAndSendChunksJustInTime; oneByteMarkedPlan.DataPresent = %v; len(oneByteMarkedPlan.Chunks) = %v", oneByteMarkedPlan.DataPresent(), len(oneByteMarkedPlan.Chunks))
	//vv("top of packAndSendChunksJustInTime; oneByteMarkedPlan.DataPresent = %v; len(oneByteMarkedPlan.Chunks) = %v; oneByteMarkedPlan.Chunks = '%v'", oneByteMarkedPlan.DataPresent(), len(oneByteMarkedPlan.Chunks), oneByteMarkedPlan.String())

	t0 := time.Now()
	var bytesFromDisk int64
	var tot int64 // total accounted for; either sent or not needed to send.

	//defer func() {
	//vv("end of packAndSendChunksJustInTime; oneByteMarkedPlan.DataPresent = %v; bytesFromDisk = %v", oneByteMarkedPlan.DataPresent(), bytesFromDisk)
	//}()

	var blake3hash *myblake3.Blake3
	if lastFragCallBack != nil {
		blake3hash = myblake3.NewBlake3()
	}

	fd, err := os.Open(path)
	panicOn(err)

	// pack up to max bytes of Chunks into a message.
	max := rpc.UserMaxPayload - 10_000

	n := len(oneByteMarkedPlan.Chunks)
	last := false

	//for i, chunk := range oneByteMarkedPlan.Chunks
	for i := 0; i < n; {
		f := ckt.LpbFrom.NewFragment()
		f.FragSubject = subject

		// use FragPart to give the
		// index of first in the pack.
		// Let the reader verify they are reading
		// properly in sync.
		f.FragPart = int64(i)
		var pack []*Chunk

		have := 0 // how many bytes in the pack

		// use CloneNoData to not leave any memory
		// allocations for .Data sitting around in
		// oneByteMarkedPlan after this func returns.
		next := oneByteMarkedPlan.Chunks[i]
		letgo := next.CloneNoData() // let go of memory after we return.
		tot = next.Endx

		// "just-in-time" data delivery, to
		// lower the memory footprint.
		if len(next.Data) > 0 { // do we have our 1 byte flag?
			if next.Cry == "RLE0;" {
				panic("RLE0 should never have Data!?!")
			}
			// need to read it from file
			_, err := fd.Seek(int64(next.Beg), 0)
			panicOn(err)

			amt := next.Endx - next.Beg
			letgo.Data = make([]byte, amt)
			_, err = io.ReadFull(fd, letgo.Data)
			panicOn(err)
			bytesFromDisk += amt
		}
		// else letgo is ready to go already.

		uses := letgo.Msgsize()

		for have+uses < max {
			pack = append(pack, letgo)
			have += uses
			i++
			if i >= n {
				last = true
				break
			}
			next = oneByteMarkedPlan.Chunks[i]
			letgo = next.CloneNoData()
			tot = next.Endx

			if (sendDataWithoutOneByteMarkers &&
				next.Cry != "RLE0;" &&
				next.Cry != "UNWRIT;") ||
				len(next.Data) > 0 { // is our 1 marker byte there?

				// fill in letgo.Data
				_, err := fd.Seek(int64(next.Beg), 0)
				panicOn(err)

				amt := next.Endx - next.Beg
				letgo.Data = make([]byte, amt)
				_, err = io.ReadFull(fd, letgo.Data)
				panicOn(err)
				bytesFromDisk += amt

				if lastFragCallBack != nil {
					blake3hash.Write(letgo.Data)
				}
			} else {
				// RLE0; runs might need to be hashed.
				if lastFragCallBack != nil {
					if next.Cry == "RLE0;" {
						// need to add zeros to the hash
						left := next.Endx - next.Beg
						for left > 0 {
							var lim int64 = 4096
							if left < 4096 {
								lim = left
							}
							blake3hash.Write(zeros4k[:lim])
							left -= lim
						}
					}
				}
			}
			uses = letgo.Msgsize()
		}
		if last {
			f.FragOp = opLast // OpRsync_HeavyDiffChunksLast
			if lastFragCallBack != nil {
				lastFragCallBack(f, blake3hash)
			}
		} else {
			f.FragOp = opMore // OpRsync_HeavyDiffChunksEnclosed
		}
		chnks := &Chunks{
			Chunks: pack,
			Path:   path,
		}
		//		if i == 0 {
		// try on all! since only one (first detected) will go through.
		chnks.PreAllocUnwritBeg = oneByteMarkedPlan.PreAllocUnwritBeg
		chnks.PreAllocUnwritEndx = oneByteMarkedPlan.PreAllocUnwritEndx
		//		}
		bts, err := chnks.MarshalMsg(nil)
		panicOn(err)
		f.Payload = bts
		err = ckt.SendOneWay(f, 0, 0)
		panicOn(err)
		bt.bsend += len(bts)

		s.reportProgress(syncReq, path, int64(goalPrecis.FileSize), int64(tot), t0)
	} // end for i
	return nil
}

func (s *SyncService) getMoreChunks(
	ckt *rpc.Circuit,
	bt *byteTracker,
	localChunks **Chunks,
	done, done0 <-chan struct{},
	syncReq *RequestToSyncPath,
	opLast int,
	opMore int,
) (err0 error) {

moreLoop:
	for {
		select {
		case fragX := <-ckt.Reads:

			switch fragX.FragOp {
			case opLast:
				//vv("getMoreChunks sees opLast '%v'", rpc.FragOpDecode(opLast))
				// a) match paths for sanity;
				// b) append to localChunks;
				// c) can continue with 1. below
				x := &Chunks{}
				_, err := x.UnmarshalMsg(fragX.Payload)
				panicOn(err)
				bt.bread += len(fragX.Payload)

				lc := (*localChunks)
				n := len(lc.Chunks)
				if n > 0 {
					if lc.Chunks[n-1].Endx != x.Chunks[0].Beg {
						panic(fmt.Sprintf("not contiguous! at n=%v; "+
							"%v = lc.Chunks[n-1].Endx != x.Chunks[0].Beg = %v",
							n, lc.Chunks[n-1].Endx, x.Chunks[0].Beg))
					}
				}

				(*localChunks).Chunks = append(
					(*localChunks).Chunks, x.Chunks...)

				break moreLoop

			case opMore:
				//vv("getMoreChunks sees opMore '%v'", rpc.FragOpDecode(opMore))
				// a) match paths for sanity;
				// b) append to localChunks;
				// c) still have to wait for opLast
				// (e.g. OpRsync_RequestRemoteToGive_ChunksLast)
				// so just continue to loop

				x := &Chunks{}
				_, err := x.UnmarshalMsg(fragX.Payload)
				panicOn(err)
				bt.bread += len(fragX.Payload)

				// if x.Path != (*localChunks).Path {
				// 	panic(fmt.Sprintf("are we mixing up"+
				// 		"chunks from two files?? x.Path='%v'"+
				// 		" but localChunks.Path='%v'",
				// 		x.Path, (*localChunks).Path))
				// }

				(*localChunks).Chunks = append(
					(*localChunks).Chunks, x.Chunks...)

			default:
				panic(fmt.Sprintf("unexpected: fragX: '%v", fragX))
			}
			fragX = nil // GC early.
			continue

			// and all the regular shutdown stuff.
		case fragerr := <-ckt.Errors:
			//vv("%v: (ckt %v) (Giver) ckt.Errors sees fragerr:'%s'", s.ServiceName, ckt.Name, fragerr)
			_ = fragerr
			if syncReq != nil {
				syncReq.Errs = fragerr.Err
			}
			return
		case <-done:
			//vv("%v: (ckt '%v') (Giver) ctx.Done seen. cause: '%v'", name, ckt.Name, context.Cause(ckt.Context))
			return
		case <-done0:
			//vv("%v: (ckt '%v') (Giver) ctx.Done seen. cause: '%v'", name, ckt.Name, context.Cause(ctx0))
			return
		case <-ckt.Halt.ReqStop.Chan:
			//vv("%v: (ckt '%v') (Giver) ckt halt requested.", name, ckt.Name)
			return
		}
	}
	return nil
}
