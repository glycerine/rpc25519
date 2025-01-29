package jsync

import (
	"context"
	"fmt"
	//"os"
	"strings"
	//"sync"
	"path/filepath"
	"time"

	//"github.com/glycerine/rpc25519/progress"
	"github.com/glycerine/idem"
	rpc "github.com/glycerine/rpc25519"
)

var _ = time.Time{}

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
func (s *SyncService) DirGiver(ctx0 context.Context, ckt *rpc.Circuit, myPeer *rpc.LocalPeer, reqDir *RequestToSyncDir) (err0 error) {

	vv("SyncService.DirGiver top. We are local if reqDirp = %p != nil", reqDir)

	// If the local giver (pushing), send the dir listing over,
	// so the remote taker can tell what to delete.
	// then
	// fan out the request, one for each
	// actual file on the local giver.

	name := myPeer.PeerServiceName
	_ = name // used when logging is on.

	done0 := ctx0.Done()
	done := ckt.Context.Done()
	bt := &byteTracker{}

	weAreRemoteGiver := (reqDir == nil)
	_ = weAreRemoteGiver

	var haltDirScan *idem.Halter
	var packOfLeavesCh chan *PackOfLeafPaths
	var packOfFilesCh chan *PackOfFiles
	var packOfDirsCh chan *PackOfDirs

	defer func(reqDir *RequestToSyncDir) {
		vv("%v: (ckt '%v') defer running! finishing DirGiver; reqDir=%p; err0='%v'", name, ckt.Name, reqDir, err0)
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
				//vv("DirGiver suppressing rpc.ErrContextCancelled or ErrHaltRequested, this is normal shutdown.")
			}
		}
	}(reqDir)

	for {
		select {

		case frag0 := <-ckt.Reads:
			//vv("%v: (ckt '%v') (Giver) saw read frag0:'%v'", name, ckt.Name, frag0)

			////vv("frag0 = '%v'", frag0)
			switch frag0.FragOp {

			///////////// begin dir sync stuff
			case OpRsync_TakerRequestsDirSyncBegin: // 21
				vv("%v: (ckt '%v') (DirGiver) sees OpRsync_TakerRequestsDirSyncBegin.", name, ckt.Name)

				// taker gives us their top dir temp dir to write paths into.

				// reply with: 22 OpRsync_DirSyncBeginToTaker repeating the
				// path they already sent us, so we can join/reuse the flow.

				// (A local giver will similarly send
				// 22 OpRsync_DirSyncBeginToTaker in service Start()
				// to start a push to remote; at service.go:600,
				// but of course won't know the temp taker top dir.

				reqDir2 := &RequestToSyncDir{}
				_, err0 = reqDir2.UnmarshalMsg(frag0.Payload)
				panicOn(err0)
				bt.bread += len(frag0.Payload)
				if reqDir == nil {
					// weAreRemoteGiver true (set above)
					reqDir = reqDir2
				} else {
					// we are local giver doing push.
					// the echo we get back will have the
					// target temp dir available for the first time.
					// we need to copy it in.
					reqDir.TopTakerDirTemp = reqDir2.TopTakerDirTemp
					reqDir.TopTakerDirTempDirID = reqDir2.TopTakerDirTempDirID
				}

				begin := rpc.NewFragment()
				begin.FragOp = OpRsync_DirSyncBeginToTaker // 22
				bts, err := reqDir.MarshalMsg(nil)
				panicOn(err)
				begin.Payload = bts
				err = ckt.SendOneWay(begin, 0)
				panicOn(err)

			case OpRsync_DirSyncBeginReplyFromTaker: // 23
				vv("%v: (ckt '%v') (DirGiver) sees 23 OpRsync_DirSyncBeginReplyFromTaker", name, ckt.Name)

				reqDir2 := &RequestToSyncDir{}
				_, err0 = reqDir2.UnmarshalMsg(frag0.Payload)
				panicOn(err0)
				bt.bread += len(frag0.Payload)
				if reqDir == nil {
					// weAreRemoteGiver true (set above)
					reqDir = reqDir2
				} else {
					// we are local giver doing push.
					// the echo we get back will have the
					// target temp dir available for the first time.
					// we need to copy it in.
					reqDir.TopTakerDirTemp = reqDir2.TopTakerDirTemp
					reqDir.TopTakerDirTempDirID = reqDir2.TopTakerDirTempDirID
				}

				vv("DirGiver will use write targets to reqDir.TopTakerDirTemp = '%v' for final: '%v'", reqDir.TopTakerDirTemp, reqDir.TopTakerDirFinal)

				// after getting 23,
				// send 26/27/28
				// OpRsync_GiverSendsTopDirListing
				// OpRsync_GiverSendsTopDirListingMore
				// OpRsync_GiverSendsTopDirListingEnd
				var err error
				haltDirScan, packOfLeavesCh, packOfFilesCh, packOfDirsCh,
					err = ScanDirTree(ctx0, reqDir.GiverDir)
				_ = packOfFilesCh
				_ = packOfDirsCh
				panicOn(err)
				defer haltDirScan.ReqStop.Close()

			sendLeafDir:
				for i := 0; ; i++ {
					select {
					case pol, ok := <-packOfLeavesCh:
						if !ok {
							break sendLeafDir
						}
						bts, err := pol.MarshalMsg(nil)
						panicOn(err)
						leafy := rpc.NewFragment()
						leafy.SetUserArg("structType", "PackOfLeafPaths")
						leafy.Payload = bts
						switch {
						case pol.IsLast:
							leafy.FragOp = OpRsync_GiverSendsTopDirListingEnd
						case i == 0:
							leafy.FragOp = OpRsync_GiverSendsTopDirListing
						default:
							leafy.FragOp = OpRsync_GiverSendsTopDirListingMore
						}
						err = ckt.SendOneWay(leafy, 0)
						panicOn(err)
						if pol.IsLast {
							break sendLeafDir
						}

					case <-done:
						////vv("%v: (ckt '%v') (DirGiver) ctx.Done seen. cause: '%v'", name, ckt.Name, context.Cause(ckt.Context))
						return
					case <-done0:
						////vv("%v: (ckt '%v') (DirGiver) ctx.Done seen. cause: '%v'", name, ckt.Name, context.Cause(ctx0))
						return
					case <-ckt.Halt.ReqStop.Chan:
						////vv("%v: (ckt '%v') (DirGiver) ckt halt requested.", name, ckt.Name)
						return
					}
				}

				// and wait for OpRsync_TakerReadyForDirContents

			case OpRsync_TakerReadyForDirContents: // 29
				vv("%v: (ckt '%v') (DirGiver) sees OpRsync_TakerReadyForDirContents", name, ckt.Name)

				// we (giver) now do individual file syncs
				// (newly deleted files can be simply not
				// transferred on the taker side to the new dir!) ...
				// -> at end, giver -> DirSyncEndToTaker

				allBatches := idem.NewHalter()

			sendFiles:
				for i := 0; ; i++ {
					select {
					case pof, ok := <-packOfFilesCh:
						if !ok {
							break sendFiles
						}
						batchHalt := idem.NewHalter()
						allBatches.AddChild(batchHalt)
						defer batchHalt.ReqStop.Close()

						for _, file := range pof.Pack {
							goroHalt := idem.NewHalter()
							batchHalt.AddChild(goroHalt)

							go func(file *File, goroHalt *idem.Halter) {
								defer func() {
									goroHalt.ReqStop.Close()
									goroHalt.Done.Close()
								}()

								frag1 := rpc.NewFragment()
								sr := &RequestToSyncPath{
									GiverPath:   filepath.Join(reqDir.GiverDir, file.Path),
									TakerPath:   filepath.Join(reqDir.TopTakerDirTemp, file.Path),
									FileSize:    file.Size,
									ModTime:     file.ModTime,
									FileMode:    file.FileMode,
									RemoteTakes: true,
									Done:        idem.NewIdemCloseChan(),
									//Precis:      precis,
									//Chunks:      chunks,
								}
								bts, err := sr.MarshalMsg(nil)
								panicOn(err)
								frag1.Payload = bts
								// basic push flow. giver sends 1.
								frag1.FragOp = OpRsync_RequestRemoteToTake
								frag1.SetUserArg("structType", "RequestToSyncPath")
								cktName := rsyncRemoteTakesString
								ckt2, ctx2, err := ckt.NewCircuit(cktName, frag1)
								panicOn(err)
								defer func() {
									r := recover()
									if r != nil {
										err := fmt.Errorf(
											"panic recovered: '%v'", r)
										vv("error ckt2 close: '%v'", err)
										ckt2.Close(err)
									} else {
										vv("normal ckt2 close")
										ckt2.Close(nil)
									}
								}()
								// cancels us too early! avoid:
								// batchHalt.AddChild(ckt2.Halt)

								errg := s.Giver(ctx2, ckt2, myPeer, sr)
								panicOn(errg)

							}(file, goroHalt)
						}
						err := batchHalt.ReqStop.WaitTilChildrenDone(done)
						vv("batchHalt.ReqStop.WaitTilChildrenDone gave err = '%v'", err)
						batchHalt.ReqStop.Close()

						if pof.IsLast {
							break sendFiles
						}

					case <-done:
						////vv("%v: (ckt '%v') (DirGiver) ctx.Done seen. cause: '%v'", name, ckt.Name, context.Cause(ckt.Context))
						return
					case <-done0:
						////vv("%v: (ckt '%v') (DirGiver) ctx.Done seen. cause: '%v'", name, ckt.Name, context.Cause(ctx0))
						return
					case <-ckt.Halt.ReqStop.Chan:
						////vv("%v: (ckt '%v') (DirGiver) ckt halt requested.", name, ckt.Name)
						return
					}
				} // end for i sendfiles

				err := allBatches.ReqStop.WaitTilChildrenDone(done)
				panicOn(err)
				err = allBatches.Done.WaitTilChildrenDone(done)
				panicOn(err)

				vv("dirgiver: all batches of indiv file sync are done.")

				// wait to go on to sending OpRsync_ToTakerAllTreeModes
				// until after all the files are there.
				// UPDATE: that is what the allBatches.Done above
				// SHOULD be waiting for!

				// So wait for OpRsync_ToGiverDirContentsDoneAck
				// send OpRsync_ToTakerDirContentsDone
				allFilesDone := rpc.NewFragment()
				allFilesDone.FragOp = OpRsync_ToTakerDirContentsDone
				err = ckt.SendOneWay(allFilesDone, 0)
				panicOn(err)

			case OpRsync_ToGiverDirContentsDoneAck:

				vv("dirgiver sees OpRsync_ToGiverDirContentsDoneAck: on to phase 3, dirgiver sends directory modes")
				// last (3rd phase): send each directory node,
				// to set its permissions.

			sendDirs:
				for i := 0; ; i++ {
					select {
					case pod, ok := <-packOfDirsCh:
						if !ok {
							break sendDirs
						}
						podModes := rpc.NewFragment()
						bts, err := pod.MarshalMsg(nil)
						panicOn(err)
						podModes.Payload = bts

						podModes.FragOp = OpRsync_ToTakerAllTreeModes
						podModes.SetUserArg("structType", "PackOfDirs")
						err = ckt.SendOneWay(podModes, 0)
						panicOn(err)
						vv("dirgiver sent pod (last? %v): '%#v'", pod.IsLast, pod)
						if pod.IsLast {
							break sendDirs
						}

					case <-done:
						////vv("%v: (ckt '%v') (DirGiver) ctx.Done seen. cause: '%v'", name, ckt.Name, context.Cause(ckt.Context))
						return
					case <-done0:
						////vv("%v: (ckt '%v') (DirGiver) ctx.Done seen. cause: '%v'", name, ckt.Name, context.Cause(ctx0))
						return
					case <-ckt.Halt.ReqStop.Chan:
						////vv("%v: (ckt '%v') (DirGiver) ckt halt requested.", name, ckt.Name)
						return
					}
				} // end for i sendDirs

				// all 3 phases of fileystem sync done: send OpRsync_DirSyncEndToTaker,
				// wait for OpRsync_ToGiverAllTreeModesDone

			case OpRsync_ToGiverAllTreeModesDone:
				vv("dirgiver sees OpRsync_ToGiverAllTreeModesDone," +
					" sending OpRsync_DirSyncEndToTaker")
				dend := rpc.NewFragment()
				dend.FragOp = OpRsync_DirSyncEndToTaker
				err := ckt.SendOneWay(dend, 0)
				panicOn(err)

				// wait for OpRsync_DirSyncEndAckFromTaker.

			case OpRsync_DirSyncEndAckFromTaker: // 25
				vv("%v: (ckt '%v') (DirGiver) sees OpRsync_DirSyncEndAckFromTaker", name, ckt.Name)
				// shut down all dir sync stuff, send FIN.

				s.ackBackFINToTaker(ckt, frag0)
				frag0 = nil // GC early.
				// wait for FIN

				///////////// end dir sync stuff

			case OpRsync_AckBackFIN_ToGiver:
				//vv("%v: (ckt '%v') (DirGiver) sees OpRsync_AckBackFIN_ToGiver. returning.", name, ckt.Name)
				return

			} // end switch frag0.FragOp

		case fragerr := <-ckt.Errors:
			//vv("%v: (ckt %v) (DirGiver) ckt.Errors sees fragerr:'%s'", name, ckt.Name, fragerr)
			_ = fragerr
			// 	err := ckt.SendOneWay(frag, 0)
			// 	panicOn(err)
			if reqDir != nil {
				reqDir.SR.Errs = fragerr.Err
			}
			return
		case <-done:
			////vv("%v: (ckt '%v') (DirGiver) ctx.Done seen. cause: '%v'", name, ckt.Name, context.Cause(ckt.Context))
			return
		case <-done0:
			////vv("%v: (ckt '%v') (DirGiver) ctx.Done seen. cause: '%v'", name, ckt.Name, context.Cause(ctx0))
			return
		case <-ckt.Halt.ReqStop.Chan:
			////vv("%v: (ckt '%v') (DirGiver) ckt halt requested.", name, ckt.Name)
			return
		}
	}
}
