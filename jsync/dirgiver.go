package jsync

import (
	"context"
	//"fmt"
	//"os"
	//"strings"
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
func (s *SyncService) DirGiver(ctx0 context.Context, ckt *rpc.Circuit, myPeer *rpc.LocalPeer, syncReq *RequestToSyncPath, frag0 *rpc.Fragment, bt *byteTracker) (err0 error) {

	vv("SyncService.DirGiver top.")

	name := myPeer.PeerServiceName
	_ = name // used when logging is on.

	done0 := ctx0.Done()
	done := ckt.Context.Done()

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
				// wait for FIN?

				///////////// end dir sync stuff

			case OpRsync_AckBackFIN_ToGiver:
				//vv("%v: (ckt '%v') (Giver) sees OpRsync_AckBackFIN_ToGiver. returning.", name, ckt.Name)
				return

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
