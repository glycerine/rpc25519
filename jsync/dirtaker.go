package jsync

import (
	//"bufio"
	"context"
	"fmt"
	//myblake3 "github.com/glycerine/rpc25519/hash"
	//"github.com/glycerine/rpc25519/progress"
	//"io"
	//"io/fs"
	//"os"
	//"path/filepath"
	//"strconv"
	//"strings"
	//"sync"
	//"time"

	//"github.com/glycerine/idem"
	rpc "github.com/glycerine/rpc25519"
	//"lukechampine.com/blake3"
)

// DirTaker is the directory top-level sync
// coorinator from the Taker side.
func (s *SyncService) DirTaker(ctx0 context.Context, ckt *rpc.Circuit, myPeer *rpc.LocalPeer, syncDirReq *RequestToSyncDir, frag *rpc.Fragment, bt *byteTracker) (err0 error) {

	vv("SyncService.DirTaker top")

	name := myPeer.PeerServiceName
	_ = name // used when logging is on.

	done0 := ctx0.Done()
	done := ckt.Context.Done()

	// this is the DirTaker side
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
