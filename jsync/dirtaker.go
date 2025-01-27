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
	"strings"
	//"sync"
	//"time"

	//"github.com/glycerine/idem"
	rpc "github.com/glycerine/rpc25519"
	//"lukechampine.com/blake3"
)

// DirTaker is the directory top-level sync
// coordinator from the Taker side.
func (s *SyncService) DirTaker(ctx0 context.Context, ckt *rpc.Circuit, myPeer *rpc.LocalPeer, reqDir *RequestToSyncDir) (err0 error) {

	vv("SyncService.DirTaker top; we are local if reqDir = %p != nil", reqDir)

	name := myPeer.PeerServiceName
	_ = name // used when logging is on.

	done0 := ctx0.Done()
	done := ckt.Context.Done()
	bt := &byteTracker{}

	weAreRemoteTaker := (reqDir == nil)

	if reqDir != nil {
		if reqDir.TopTakerDirTemp == "" {
			panic("reqDir.TopTakerDirTemp should have been created/filled in!")
		}
		if !dirExists(reqDir.TopTakerDirTemp) {
			panic(fmt.Sprintf("why does not exist reqDir.TopTakerDirTemp = '%v' ???", reqDir.TopTakerDirTemp))
		}
	}

	defer func(reqDir *RequestToSyncDir) {
		vv("%v: (ckt '%v') defer running! finishing DirTaker; reqDir=%p; err0='%v'", name, ckt.Name, reqDir, err0)
		////vv("bt = '%#v'", bt)

		// only close Done for local (client, typically) if we were started locally.
		if reqDir != nil {
			reqDir.SR.BytesRead = int64(bt.bread)
			reqDir.SR.BytesSent = int64(bt.bsend)
			//reqDir.RemoteBytesTransferred = ?
			if err0 != nil {
				//vv("setting (%p) reqDir.Errs = '%v'", reqDir, err0.Error())
				reqDir.SR.Errs = err0.Error()
			}
			reqDir.SR.Done.Close() // only AFTER setting Errs, please!
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
				//vv("DirTaker suppressing rpc.ErrContextCancelled or ErrHaltRequested, this is normal shutdown.")
			}
		}
	}(reqDir)

	// this is the DirTaker side
	for {
		select {
		case frag := <-ckt.Reads:
			//vv("%v: (ckt %v) (DirTaker) ckt.Reads sees frag:'%s'", name, ckt.Name, frag)
			_ = frag
			switch frag.FragOp {

			///////////////// begin dir sync stuff

			case OpRsync_DirSyncBeginToTaker: // 22
				vv("%v: (ckt '%v') (DirTaker) sees OpRsync_DirSyncBeginToTaker.", name, ckt.Name)
				// we should: setup a top tempdir and tell the
				// giver the path so they can send new files into that path.

				// reply with 23 OpRsync_DirSyncBeginReplyFromTaker
				// and the new top tempdir path, even if
				// we initiated and they already know the path; just repeat it
				// for simplicity/reusing the flow.

				var err error
				reqDir2 := &RequestToSyncDir{}
				_, err = reqDir2.UnmarshalMsg(frag.Payload)
				panicOn(err)
				bt.bread += len(frag.Payload)
				if reqDir == nil {
					// already set above: weAreRemoteTaker = true
					reqDir = reqDir2
				} else {
					// we are the local taker. We created the
					// temp target dir in service Start(). The
					// remote giver should be echo-ing back what
					// we gave them.
					if reqDir2.TopTakerDirTemp == "" {
						panic("reqDir2.TopTakerDirTemp should be set in reqDir2!")
					}
					if reqDir2.TopTakerDirTemp != reqDir.TopTakerDirTemp {
						panic("huh? reqDir2.TopTakerDirTemp != reqDir.TopTakerDirTemp")
					}
					if reqDir2.TopTakerDirTempDirID != reqDir.TopTakerDirTempDirID {
						panic("huh2? reqDir2.TopTakerDirTempDirID != reqDir.TopTakerDirTempDirID")
					}
				}

				// INVAR: reqDir is set.
				if weAreRemoteTaker {
					targetTakerTopTempDir, tmpDirID, err := s.mkTempDir(
						reqDir.TopTakerDirFinal)
					panicOn(err)
					vv("DirTaker (remote taker) made temp dir '%v' for finalDir '%v'", targetTakerTopTempDir, reqDir.TopTakerDirFinal)
					reqDir.TopTakerDirTemp = targetTakerTopTempDir
					reqDir.TopTakerDirTempDirID = tmpDirID
				}
				// INVAR: targetTakerTopTempDir is set.

				tmpReady := rpc.NewFragment()
				tmpReady.FragOp = OpRsync_DirSyncBeginReplyFromTaker // 23

				// useful visibility, but use the struct fields as definitive.
				tmpReady.SetUserArg("targetTakerTopTempDir",
					reqDir.TopTakerDirTemp)
				tmpReady.SetUserArg("targetTakerTopTempDirID",
					reqDir.TopTakerDirTempDirID)

				bts, err := reqDir.MarshalMsg(nil)
				panicOn(err)
				tmpReady.Payload = bts
				err = ckt.SendOneWay(tmpReady, 0)
				panicOn(err)

			case OpRsync_DirSyncEndToTaker: // 24, end of dir sync.
				vv("%v: (ckt '%v') (DirTaker) sees OpRsync_DirSyncEndToTaker", name, ckt.Name)
				// we (taker) can rename the temp top dir/replace any old top dir.

				// reply with OpRsync_DirSyncEndAckFromTaker, wait for FIN.

			case OpRsync_GiverSendsTopDirListing, OpRsync_GiverSendsTopDirListingMore, OpRsync_GiverSendsTopDirListingEnd: // 26/27/28
				vv("%v: (ckt '%v') (DirTaker) sees %v.", rpc.FragOpDecode(frag.FragOp), name, ckt.Name)
				// Getting this means here is the starting dir tree from giver.
				// or, to me (taker), here is more dir listing
				// or, to me (taker), here is end dir listing

				// reply to OpRsync_GiverSendsTopDirListingEnd
				// with OpRsync_TakerReadyForDirContents

			///////////////// end dir sync stuff

			case OpRsync_AckBackFIN_ToTaker:
				////vv("%v: (ckt '%v') (DirTaker) sees OpRsync_AckBackFIN_ToTaker. returning.", name, ckt.Name)
				return

			} // end switch FragOp

		case fragerr := <-ckt.Errors:
			//vv("%v: (ckt '%v') Taker fragerr = '%v'", name, ckt.Name, fragerr)
			_ = fragerr

			if fragerr != nil {
				// do we want to set reqDir.SR.Err? No, the defer will do it.
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
			//////vv("%v: (ckt '%v') (DirTaker) ckt halt requested.", name, ckt.Name)
			return
		}
	}

}
