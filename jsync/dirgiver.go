package jsync

import (
	"context"
	"fmt"
	"os"
	"strings"
	//"sync"
	//"time"

	//"github.com/glycerine/rpc25519/progress"
	//"github.com/glycerine/idem"
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
func (s *SyncService) DirGiver(ctx0 context.Context, ckt *rpc.Circuit, myPeer *rpc.LocalPeer, syncReq *RequestToSyncPath) (err0 error) {

	////vv("SyncService.Giver top.")

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

	//var localChunks *Chunks

	//var goalPrecis *FilePrecis
	//var remoteWantsUpdate *Chunks
	var light *LightRequest

	for {
		select {

		case frag0 := <-ckt.Reads:
			//vv("%v: (ckt '%v') (Giver) saw read frag0:'%v'", name, ckt.Name, frag0)

			////vv("frag0 = '%v'", frag0)
			switch frag0.FragOp {

			///////////// begin dir sync stuff
			case OpRsync_TakerRequestsDirSyncBegin:
				vv("%v: (ckt '%v') (Giver) sees OpRsync_TakerRequestsDirSyncBegin.", name, ckt.Name)

				// taker gives us their top dir temp dir to write paths into.
				// send my (takers) temp new top dir for paths to go into.
				// be sure to setup the new temp dir as separately as possible,
				// to avoid overlapping dir transfers having crosstalk.
				// we tell giver, please send me 22,26/27/28

				// reply with: 22 OpRsync_DirSyncBeginToTaker repeating the
				// path they already sent us, so we can join/reuse the flow.
				// We will also send OpRsync_DirSyncBeginToTaker in
				// service Start() if we are local giver
				// and are starting a push to remote.

				// after sending 22
				// then
				// send 26/27/28 immediately next
				// OpRsync_GiverSendsTopDirListing
				// OpRsync_GiverSendsTopDirListingMore
				// OpRsync_GiverSendsTopDirListingEnd

				// and wait for OpRsync_TakerReadyForDirContents

			case OpRsync_TakerReadyForDirContents:
				vv("%v: (ckt '%v') (Giver) sees OpRsync_TakerReadyForDirContents", name, ckt.Name)

				// we (giver) now do individual file syncs (newly deleted files can be simply not transferred on the taker side to the new dir!) ... -> at end, giver -> DirSyncEndToTaker

				// at end, send OpRsync_DirSyncEndToTaker,
				// wait for OpRsync_DirSyncEndAckFromTaker.

			case OpRsync_DirSyncEndAckFromTaker:
				vv("%v: (ckt '%v') (Giver) sees OpRsync_DirSyncEndAckFromTaker", name, ckt.Name)
				// shut down all dir sync stuff, send FIN.

				s.ackBackFINToTaker(ckt, frag0)
				frag0 = nil // GC early.
				continue    // wait for other side to close

				///////////// end dir sync stuff

			case OpRsync_LazyTakerWantsToPull: // FragOp 19
				//vv("%v: (ckt '%v') (Giver) sees OpRsync_LazyTakerWantsToPull", name, ckt.Name)
				syncReq = &RequestToSyncPath{}
				_, err0 = syncReq.UnmarshalMsg(frag0.Payload)
				panicOn(err0)
				bt.bread += len(frag0.Payload)

				fi, err := os.Stat(syncReq.GiverPath)
				if err != nil {
					// file does not exist
					notfound := rpc.NewFragment()
					notfound.FragSubject = frag0.FragSubject
					notfound.Typ = rpc.CallPeerError
					//notfound.FragOp =
					notfound.Err = fmt.Sprintf("file access error "+
						"for '%v': '%v' on host '%v'",
						syncReq.GiverPath, err.Error(), rpc.Hostname)
					err = ckt.SendOneWay(notfound, 0)
					panicOn(err)
					continue
				}
				sz, mod := fi.Size(), fi.ModTime()
				if syncReq.FileSize == sz && syncReq.ModTime.Equal(mod) {
					//vv("giver: OpRsync_LazyTakerWantsToPull: size + modtime match. nothing to do, tell taker. syncReq.GiverPath = '%v'", syncReq.GiverPath)
					// let the taker know they can stop with this file.
					ack := rpc.NewFragment()
					ack.FragSubject = frag0.FragSubject
					ack.FragOp = OpRsync_FileSizeModTimeMatch

					err = ckt.SendOneWay(ack, 0)
					panicOn(err)
				} else {
					// tell them they must send the chunks... if they want it.
					//vv("giver responding to OpRsync_LazyTakerWantsToPull with OpRsync_LazyTakerNoLuck_ChunksRequired")
					ack := rpc.NewFragment()
					ack.FragSubject = frag0.FragSubject
					ack.FragOp = OpRsync_LazyTakerNoLuck_ChunksRequired
					ack.Payload = frag0.Payload // send the syncReq back
					err = ckt.SendOneWay(ack, 0)
					bt.bsend += len(frag0.Payload)
					panicOn(err)
				}
				frag0 = nil
				continue // wait for FIN or chunks.

			case OpRsync_AckBackFIN_ToGiver:
				//vv("%v: (ckt '%v') (Giver) sees OpRsync_AckBackFIN_ToGiver. returning.", name, ckt.Name)
				return

			case OpRsync_RequestRemoteToGive: // FragOp 12
				// This is the pull entry point.
				// 2nd implemented: pull is newer.

				// we, the giver, are the "remote" in this case.
				// The other side, the taker, will be the "local".
				//

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
				// Old/was: assert syncReq was nil as passed in to our Giver() call.
				//if syncReq != nil {
				//	panic(fmt.Sprintf("syncReq should have been nil! OpRsync_RequestRemoteToGive seen with already set syncReq! Is there an operation sequence already in motion?"))
				//}

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
					if err0 != nil {
						return
					}

				} // end if syncReq.MoreChunksComming
				//vv("nore more chunks to wait for...")

				// after moreLoop, we get here:

				// 1. if local has nothing, send full stream. stop.
				if syncReq.FileSize == 0 {
					err0 = s.giverSendsWholeFile(syncReq.GiverPath, syncReq.TakerPath, ckt, bt, frag0)

					//vv("giver sent whole file. done (wait for FIN)")
					frag0 = nil // GC early.
					continue    // wait for FIN ack back.
				}

				// 2. else: scan our "remote path". updated not needed? ack back FIN
				if !s.remoteGiverAreDiffChunksNeeded(syncReq, ckt, bt, frag0) {
					//vv("giver sees no update needed. done (wait for FIN)")
					s.ackBackFINToTaker(ckt, frag0)
					frag0 = nil // GC early.
					continue    // wait for other side to close
				}

				// 3. compute update plan, send plan, then diff chunks.
				//vv("OpRsync_RequestRemoteToGive calling giverSendsPlanAndDataUpdates")
				s.giverSendsPlanAndDataUpdates(wireChunks, ckt, syncReq.GiverPath, bt, frag0)
				//vv("done with s.giverSendsPlanAndDataUpdates. done (wait for FIN/ckt shutdown)")
				// wait for FIN or ckt shutdown, to let data get there.
				frag0 = nil // GC early.
				continue

			case OpRsync_LightRequestEnclosed:

				// type LightRequest struct {
				// 	SenderPath string `zid:"0"`
				// 	ReaderPrecis *FilePrecis `zid:"1"`
				// 	ReaderChunks *Chunks     `zid:"2"` // coming next separately
				// }

				light = &LightRequest{}
				_, err0 = light.UnmarshalMsg(frag0.Payload)
				panicOn(err0)
				bt.bread += len(frag0.Payload)

				// light.ReaderChunks were too big,
				// get them in packs instead.
				err0 = s.getMoreChunks(ckt, bt, &light.ReaderChunks,
					done, done0, syncReq,
					OpRsync_RequestRemoteToGive_ChunksLast,
					OpRsync_RequestRemoteToGive_ChunksMore)

				if err0 != nil {
					return
				}

				//vv("OpRsync_LightRequestEnclosed calling giverSendsPlanAndDataUpdates")
				s.giverSendsPlanAndDataUpdates(light.ReaderChunks, ckt, localPath, bt, frag0)
				// middle of a sequence, certainly do not return.
				frag0 = nil // GC early.
				continue

			case OpRsync_FileSizeModTimeMatch:
				////vv("giver sees OpRsync_FileSizeModTimeMatch")
				if syncReq != nil {
					syncReq.SizeModTimeMatch = true
				}
				// this is also an Op specific ack back.
				s.ackBackFINToTaker(ckt, frag0) // probably not needed, but exercise it.
				// wait for other side to close on FIN.
				frag0 = nil // GC early.
				continue

			case OpRsync_FileAllReadAckToGiver:
				//vv("Giver sees OpRsync_FileAllReadAckToGiver, closing syncReq.Done")

				syncReq.FullFileInitSideBlake3, _ = frag0.GetUserArg("clientTotalBlake3sum")
				syncReq.FullFileRespondSideBlake3, _ = frag0.GetUserArg("serverTotalBlake3sum")
				syncReq.RemoteBytesTransferred = frag0.FragPart

				// this is an Op specific ack back. just finish.
				s.ackBackFINToTaker(ckt, frag0)
				// wait for ckt to close on FIN, not: return
				frag0 = nil // GC early.
				continue

			case OpRsync_ToGiverNeedFullFile2:
				// this is the upload streaming protocol. We send the data.

				err0 = s.giverSendsWholeFile(syncReq.GiverPath, syncReq.TakerPath, ckt, bt, frag0)

				// wait for FIN ack back.
				frag0 = nil // GC early.
				continue
			} // end switch frag0.FragOp

		case fragerr := <-ckt.Errors:
			//vv("%v: (ckt %v) (Giver) ckt.Errors sees fragerr:'%s'", name, ckt.Name, fragerr)
			_ = fragerr
			// 	err := ckt.SendOneWay(frag, 0)
			// 	panicOn(err)
			if syncReq != nil {
				syncReq.Errs = fragerr.Err
			}
			return
		case <-done:
			////vv("%v: (ckt '%v') (Giver) ctx.Done seen. cause: '%v'", name, ckt.Name, context.Cause(ckt.Context))
			return
		case <-done0:
			////vv("%v: (ckt '%v') (Giver) ctx.Done seen. cause: '%v'", name, ckt.Name, context.Cause(ctx0))
			return
		case <-ckt.Halt.ReqStop.Chan:
			////vv("%v: (ckt '%v') (Giver) ckt halt requested.", name, ckt.Name)
			return
		}
	}
}
