package jsync

import (
	"context"
	"fmt"
	//"os"
	"path/filepath"
	"strconv"
	"strings"
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

	//vv("SyncService.DirGiver top. We are local if reqDirp = %p != nil", reqDir)

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

	//var haltDirScan *idem.Halter
	//var packOfLeavesCh chan *PackOfLeafPaths
	var packOfFilesCh chan *PackOfFiles
	var packOfDirsCh chan *PackOfDirs

	defer func(reqDir *RequestToSyncDir) {
		//vv("%v: (ckt '%v') defer running! finishing DirGiver; reqDir=%p; err0='%v'", name, ckt.Name, reqDir, err0)
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
			vv("%v: (ckt '%v') (DirGiver) saw read frag0:'%v'", name, ckt.Name, frag0)

			////vv("frag0 = '%v'", frag0)
			switch frag0.FragOp {

			///////////// begin dir sync stuff
			case OpRsync_TakerRequestsDirSyncBegin: // 21
				//vv("%v: (ckt '%v') (DirGiver) sees OpRsync_TakerRequestsDirSyncBegin.", name, ckt.Name)

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

				begin := s.U.NewFragment()
				begin.FragOp = OpRsync_DirSyncBeginToTaker // 22
				bts, err := reqDir.MarshalMsg(nil)
				panicOn(err)
				begin.Payload = bts
				err = ckt.SendOneWay(begin, 0)
				panicOn(err)

			case OpRsync_DirSyncBeginReplyFromTaker: // new one-pass version 23

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

				// was it a file that was guessed to be a dir?
				if fileExists(reqDir.GiverDir) {
					panic(fmt.Sprintf("arg: somehow we guessed wrong. Trying to sync a file as a directory?!? reqDir.GiverDir = '%v' is a file, not a direcotry.", reqDir.GiverDir))
					// return s.Giver(ctx0, ckt, myPeer, syncReq)
				}

				// after getting 23,
				// send 26
				// OpRsync_GiverSendsTopDirListing
				haltDirScan, packOfFilesCh, totalFileCountCh, err := ScanDirTreeOnePass(
					ctx0, reqDir.GiverDir)
				panicOn(err)
				defer haltDirScan.ReqStop.Close()
				var nFiles int64

			sendOnePass:
				for i := 0; ; i++ {
					select {
					case nFiles = <-totalFileCountCh:
						totalFileCountCh = nil

					case pof := <-packOfFilesCh:
						lastser := int64(0)
						k := len(pof.Pack)
						if k > 0 {
							lastser = pof.Pack[k-1].Serial
						} else {
							lastser = nFiles // on first Frag, give total.
						}

						fragPOF := s.U.NewFragment()
						bts, err := pof.MarshalMsg(nil)
						panicOn(err)
						fragPOF.Payload = bts
						fragPOF.FragOp = OpRsync_GiverSendsTopDirListing
						fragPOF.FragPart = lastser
						fragPOF.SetUserArg("structType", "PackOfFiles")
						err = ckt.SendOneWay(fragPOF, 0)
						panicOn(err)
						if pof.IsLast {
							vv("dirgiver: pof IsLast true; end of all phases single pass")
							break sendOnePass
						}
					case <-done:
						return
					case <-done0:
						return
					case <-ckt.Halt.ReqStop.Chan:
						return
					}
				}

				/*
					case 999999: // old! OpRsync_DirSyncBeginReplyFromTaker: // 23
						//vv("%v: (ckt '%v') (DirGiver) sees 23 OpRsync_DirSyncBeginReplyFromTaker", name, ckt.Name)

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

						//vv("DirGiver will use write targets to reqDir.TopTakerDirTemp = '%v' for final: '%v'", reqDir.TopTakerDirTemp, reqDir.TopTakerDirFinal)

						// after getting 23,
						// send 26/27/28
						// OpRsync_GiverSendsTopDirListing
						// OpRsync_GiverSendsTopDirListingMore
						// OpRsync_GiverSendsTopDirListingEnd
						var err error
						haltDirScan, packOfLeavesCh, packOfFilesCh, packOfDirsCh,
							err = ScanDirTree(ctx0, reqDir.GiverDir)

						// nil able copies to we can do phases strinctly 1 at a time.
						polch := packOfLeavesCh     // start in phase 1.
						var pofch chan *PackOfFiles // start nil, no receives.
						var podch chan *PackOfDirs  // start nil, no receives.
						//pofch = packOfFilesCh  // to start phase 2, below.
						//podch = packOfDirsCh   // to start phase 3, below.

						panicOn(err)
						defer haltDirScan.ReqStop.Close()

					sendFor:
						for i := 0; ; i++ {
							select {
							case pol := <-polch: // packOfLeavesCh:
								bts, err := pol.MarshalMsg(nil)
								panicOn(err)
								leafy := s.U.NewFragment()
								leafy.SetUserArg("structType", "PackOfLeafPaths")
								leafy.Payload = bts
								leafy.FragOp = OpRsync_GiverSendsTopDirListing
								err = ckt.SendOneWay(leafy, 0)
								panicOn(err)
								if pol.IsLast {
									vv("dirgiver: pol IsLast true; on to phase 2")
									polch = nil           // end phase 1
									pofch = packOfFilesCh // to start phase 2
								}

							case pof := <-pofch:

								fragPOF := s.U.NewFragment()
								bts, err := pof.MarshalMsg(nil)
								panicOn(err)
								fragPOF.Payload = bts
								fragPOF.FragOp = OpRsync_GiverSendsPackOfFiles
								fragPOF.SetUserArg("structType", "PackOfFiles")
								err = ckt.SendOneWay(fragPOF, 0)
								panicOn(err)
								if pof.IsLast {
									vv("dirgiver: pof IsLast true; on to phase 3")
									pofch = nil          // end phase 2
									podch = packOfDirsCh // to start phase 3
								}

							case pod := <-podch: // phase 3
								podModes := s.U.NewFragment()
								bts, err := pod.MarshalMsg(nil)
								panicOn(err)
								podModes.Payload = bts

								podModes.FragOp = OpRsync_ToTakerAllTreeModes
								podModes.SetUserArg("structType", "PackOfDirs")
								err = ckt.SendOneWay(podModes, 0)
								panicOn(err)
								//vv("dirgiver sent pod (last? %v): '%#v'", pod.IsLast, pod)
								if pod.IsLast {
									break sendFor
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

						// whoa sending pof one per goro on a separate ckt
						// can be super slow. let's just send along. the
						// rest of the structure early, so we can find out
						// if we need to sync or not.

						// and wait for OpRsync_TakerReadyForDirContents
				*/
			case OpRsync_TakerReadyForDirContents: // 29
				vv("%v: (ckt '%v') (DirGiver) sees OpRsync_TakerReadyForDirContents", name, ckt.Name)

				// we (giver) now do individual file syncs
				// (newly deleted files can be simply not
				// transferred on the taker side to the new dir!) ...
				// -> at end, giver -> DirSyncEndToTaker

				var totalFileBytes int64
			sendFiles:
				for i := 0; ; i++ {
					select {
					case pof := <-packOfFilesCh:
						totalFileBytes += pof.TotalFileBytes
						batchHalt := idem.NewHalter()
						defer batchHalt.ReqStop.Close()

						for _, file := range pof.Pack {
							//vv("dirgiver: pof file = '%#v'", file)
							goroHalt := idem.NewHalter()
							batchHalt.AddChild(goroHalt)

							go func(file *File, goroHalt *idem.Halter) {
								defer func() {
									goroHalt.ReqStop.Close()
									goroHalt.Done.Close()
								}()

								frag1 := s.U.NewFragment()
								giverPath := filepath.Join(reqDir.GiverDir,
									file.Path)
								sr := &RequestToSyncPath{
									GiverPath:        giverPath,
									TakerPath:        file.Path,
									TakerTempDir:     reqDir.TopTakerDirTemp,
									TopTakerDirFinal: reqDir.TopTakerDirFinal,
									GiverDirAbs:      reqDir.GiverDir,
									GiverFileSize:    file.Size,
									GiverModTime:     file.ModTime,
									GiverFileMode:    file.FileMode,
									RemoteTakes:      true,
									Done:             idem.NewIdemCloseChan(),

									GiverScanFlags:     file.ScanFlags,
									GiverSymLinkTarget: file.SymLinkTarget,
								}
								bts, err := sr.MarshalMsg(nil)
								panicOn(err)
								frag1.Payload = bts
								// basic single file transfer flow. giver sends 1.
								frag1.FragOp = OpRsync_RequestRemoteToTake
								frag1.FragSubject = giverPath
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
										//vv("normal ckt2 close")
										ckt2.Close(nil)
									}
								}()
								// cancels us too early! avoid:
								// batchHalt.AddChild(ckt2.Halt)

								errg := s.Giver(ctx2, ckt2, myPeer, sr)
								panicOn(errg)

								if reqDir.SR.UpdateProgress != nil {
									report := fmt.Sprintf("%40s  done.", giverPath)
									select {
									case reqDir.SR.UpdateProgress <- report:
									case <-done:
										return
									}
								}
							}(file, goroHalt)
						}
						_ = batchHalt.ReqStop.WaitTilChildrenClosed(done)
						//vv("batchHalt.ReqStop.WaitTilChildrenDone back.")
						// do not panic, we might have seen closed(done).

						batchHalt.ReqStop.Close()

						if pof.IsLast {
							vv("dirgiver: pof IsLast true; break sendFiles")
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

				//vv("dirgiver: all batches of indiv file "+
				// "sync are done. totalFileBytes = %v", totalFileBytes)

				// wait to go on to sending OpRsync_ToTakerAllTreeModes
				// until after all the files are there.
				// UPDATE: that is what waiting for the last batch
				// SHOULD be waiting for!

				// wait for OpRsync_ToGiverDirContentsDoneAck
				// send OpRsync_ToTakerDirContentsDone
				allFilesDone := s.U.NewFragment()
				allFilesDone.FragOp = OpRsync_ToTakerDirContentsDone
				allFilesDone.SetUserArg("giverTotalFileBytes",
					fmt.Sprintf("%v", totalFileBytes))
				err3 := ckt.SendOneWay(allFilesDone, 0)
				panicOn(err3)

			case OpRsync_ToGiverDirContentsDoneAck:

				//vv("dirgiver sees OpRsync_ToGiverDirContentsDoneAck: on to phase 3, dirgiver sends directory modes")
				// last (3rd phase): send each directory node,
				// to set its permissions.

			sendDirs:
				for i := 0; ; i++ {
					select {
					case pod, ok := <-packOfDirsCh:
						if !ok {
							break sendDirs
						}
						podModes := s.U.NewFragment()
						bts, err := pod.MarshalMsg(nil)
						panicOn(err)
						podModes.Payload = bts

						podModes.FragOp = OpRsync_ToTakerAllTreeModes
						podModes.SetUserArg("structType", "PackOfDirs")
						err = ckt.SendOneWay(podModes, 0)
						panicOn(err)
						//vv("dirgiver sent pod (last? %v): '%#v'", pod.IsLast, pod)
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

				// meanwhile, record transfer stats.
				giverTotalFileBytesStr, ok := frag0.GetUserArg(
					"giverTotalFileBytes")
				if ok {
					giverTotalFileBytes, err := strconv.Atoi(
						giverTotalFileBytesStr)
					panicOn(err)
					//vv("OpRsync_ToGiverDirContentsDoneAck: "+
					//	"giverTotalFileBytes = %v", giverTotalFileBytes)
					if reqDir != nil {
						reqDir.GiverTotalFileBytes = int64(giverTotalFileBytes)
						reqDir.SR.GiverFileSize = int64(giverTotalFileBytes)
					}
				}

			case OpRsync_ToGiverAllTreeModesDone:
				//vv("dirgiver sees OpRsync_ToGiverAllTreeModesDone," +
				//	" sending OpRsync_DirSyncEndToTaker")
				dend := s.U.NewFragment()
				dend.FragOp = OpRsync_DirSyncEndToTaker
				err := ckt.SendOneWay(dend, 0)
				panicOn(err)

				// wait for OpRsync_DirSyncEndAckFromTaker.

			case OpRsync_DirSyncEndAckFromTaker: // 25
				//vv("%v: (ckt '%v') (DirGiver) sees OpRsync_DirSyncEndAckFromTaker", name, ckt.Name)
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
