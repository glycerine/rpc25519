package jsync

import (
	"context"
	"fmt"
	//"os"
	"path/filepath"
	"runtime"
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

	var fileCh chan *File
	var batchHalt *idem.Halter

	//var haltDirScan *idem.Halter
	//var packOfLeavesCh chan *PackOfLeafPaths
	var packOfFilesCh chan *PackOfFiles
	var packOfDirsCh chan *PackOfDirs

	defer func(reqDir *RequestToSyncDir) {
		//vv("%v: (ckt '%v') defer running! finishing DirGiver; reqDir=%p; err0='%v'", name, ckt.Name, reqDir, err0)
		//vv("bt = '%#v'", bt)

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
			//vv("%v: (ckt '%v') (DirGiver) saw read frag0:'%v'", name, ckt.Name, frag0)

			//vv("frag0 = '%v'", frag0)
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

					if reqDir.TakerTargetUnknown {
						//vv("dirgiver sees TakerTargetUnknown on reqDir: '%#v'", reqDir)
						// should we be running the full sync protocol here
						// to allow not transferring data if it is already
						// in place? i.e. allow TakerTargetUnknown to
						// be true but not really? well then the taker
						// has told us they don't have it when they do...
						// so really, just fix that part. Because if
						// they don't have it, we really do need to
						// send it in full; like this:

						if fileExists(reqDir.GiverDir) {
							err := s.convertedDirToFile_giveFile(
								ctx0, reqDir, ckt, frag0, bt)
							panicOn(err)
							// get the remote out of their sub-call to Taker.
							s.ackBackFINToTaker(ckt, frag0)
							frag0 = nil // GC early.
							continue
						}
					}
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

				// is it now a file that was guessed (or used to be) a dir?
				if fileExists(reqDir.GiverDir) {
					// reqDir.GiverDir is a file, not a directory as expected.

					err := s.convertedDirToFile_giveFile(
						ctx0, reqDir, ckt, frag0, bt)
					panicOn(err)
					// get the remote out of their sub-call to Taker.
					s.ackBackFINToTaker(ckt, frag0)
					frag0 = nil // GC early.
					continue
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
							//vv("dirgiver: pof IsLast true; end of all phases single pass")
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

			case OpRsync_TakerReadyForDirContents: // 29
				//vv("%v: (ckt '%v') (DirGiver) sees OpRsync_TakerReadyForDirContents", name, ckt.Name)

				// we (giver) now do individual file syncs
				// (newly deleted files can be simply not
				// transferred on the taker side to the new dir!) ...
				// or are detected at end by comparing existing
				// to recieved.
				// -> at end, giver -> DirSyncEndToTaker

				// start a worker pool, like dirtaker, rather than
				// goro per file.

				if fileCh == nil {
					// set up the worker pool
					fileCh = make(chan *File) // do not buffer, giving work.

					batchHalt = idem.NewHalter()
					defer batchHalt.ReqStop.Close()

					workPoolSize := runtime.NumCPU()
					for range workPoolSize {
						goroHalt := idem.NewHalter()
						batchHalt.AddChild(goroHalt)

						go func(goroHalt *idem.Halter) {
							defer func() {
								r := recover()
								if r != nil {
									//vv("DirGiver file worker (OpRsync_TakerReadyForDirContents) sees panic: '%v'", r)
									err := fmt.Errorf("DirGiver file worker (OpRsync_TakerReadyForDirContents) saw panic: '%v'", r)
									goroHalt.ReqStop.CloseWithReason(err)
									// also stop the whole batch on single err.
									// At least for now, sane debugging.
									batchHalt.ReqStop.CloseWithReason(err)
								} else {
									goroHalt.ReqStop.Close()
								}
								goroHalt.Done.Close()
							}()

							var file *File
							var t0 time.Time
							for {
								select {
								case file = <-fileCh:
									t0 = time.Now()
								case <-goroHalt.ReqStop.Chan:
									return
								case <-batchHalt.ReqStop.Chan:
									return
								case <-done:
									return
								case <-done0:
									return
								}

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
										//vv("error ckt2 close: '%v'", err)
										ckt2.Close(err)
									} else {
										//vv("normal ckt2 close")
										ckt2.Close(nil)
									}
								}()

								errg := s.Giver(ctx2, ckt2, myPeer, sr)
								panicOn(errg)
								batchHalt.ReqStop.TaskDone()

								if reqDir.SR.UpdateProgress != nil {
									//report := fmt.Sprintf("%40s  done.", giverPath)
									select {
									case reqDir.SR.UpdateProgress <- &ProgressUpdate{
										Path:   giverPath,
										Total:  file.Size,
										Latest: file.Size,
										T0:     t0,
									}:
									case <-done:
										return
									case <-goroHalt.ReqStop.Chan:
										return
									}
								}
							}
						}(goroHalt)
					}
				} // end set up worker pool

				var totalFileBytes int64
			sendFiles:
				for i := 0; ; i++ {
					select {
					case pof := <-packOfFilesCh:
						totalFileBytes += pof.TotalFileBytes
						batchHalt.ReqStop.TaskAdd(len(pof.Pack))

						for _, file := range pof.Pack {
							//vv("dirgiver: pof file = '%#v'", file)
							select {
							case fileCh <- file:
								// and do the next
							case <-batchHalt.ReqStop.Chan:
								break sendFiles
							case <-done:
								return
							case <-done0:
								return
							case <-ckt.Halt.ReqStop.Chan:
								return
							}
						} // end range over file in pof.Pack

						if pof.IsLast {
							//vv("dirgiver: pof IsLast true; break sendFiles")

							// wait for all files to be given == batch to
							// be closed (error or not); or done to fire.
							batchHalt.ReqStop.TaskWait(done)
							//vv("batchHalt.ReqStop.TaskWait back, err = '%v'", err)

							//_ = batchHalt.ReqStop.WaitTilChildrenClosed(done)
							//vv("batchHalt.ReqStop.WaitTilChildrenDone back.")
							// do not panic, we might have seen closed(done).
							//batchHalt.ReqStop.Close()

							// shutdown the worker pool (if/when we convert to a pool).
							batchHalt.StopTreeAndWaitTilDone(0, done, nil)
							// not sure we would get another dir, but reset anyway
							fileCh = nil
							break sendFiles
						}

					case <-done:
						//vv("%v: (ckt '%v') (DirGiver) ctx.Done seen. cause: '%v'", name, ckt.Name, context.Cause(ckt.Context))
						return
					case <-done0:
						//vv("%v: (ckt '%v') (DirGiver) ctx.Done seen. cause: '%v'", name, ckt.Name, context.Cause(ctx0))
						return
					case <-ckt.Halt.ReqStop.Chan:
						//vv("%v: (ckt '%v') (DirGiver) ckt halt requested.", name, ckt.Name)
						return
					}
				} // end for i sendfiles

				//vv("dirgiver: all batches of indiv file "+
				// "sync are done. totalFileBytes = %v", totalFileBytes)

				allFilesDone := s.U.NewFragment()
				allFilesDone.FragOp = OpRsync_ToTakerDirContentsDone
				allFilesDone.SetUserArg("giverTotalFileBytes",
					fmt.Sprintf("%v", totalFileBytes))
				err3 := ckt.SendOneWay(allFilesDone, 0)
				panicOn(err3)

			case OpRsync_ToGiverDirContentsDoneAck:
				// unused now, right? not sure!

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
						//vv("%v: (ckt '%v') (DirGiver) ctx.Done seen. cause: '%v'", name, ckt.Name, context.Cause(ckt.Context))
						return
					case <-done0:
						//vv("%v: (ckt '%v') (DirGiver) ctx.Done seen. cause: '%v'", name, ckt.Name, context.Cause(ctx0))
						return
					case <-ckt.Halt.ReqStop.Chan:
						//vv("%v: (ckt '%v') (DirGiver) ckt halt requested.", name, ckt.Name)
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
			//vv("%v: (ckt '%v') (DirGiver) ctx.Done seen. cause: '%v'", name, ckt.Name, context.Cause(ckt.Context))
			return
		case <-done0:
			//vv("%v: (ckt '%v') (DirGiver) ctx.Done seen. cause: '%v'", name, ckt.Name, context.Cause(ctx0))
			return
		case <-ckt.Halt.ReqStop.Chan:
			//vv("%v: (ckt '%v') (DirGiver) ckt halt requested.", name, ckt.Name)
			return
		}
	}
}

// yeah, we have a file not a directory as the
// target to give. It could have been changed
// from dir to file, or is a directory on the
// taker side that (maybe) should be
// converted to a file, depending on what
// the taker wants to do. We'll just send
// on the file for now.
//
// In order to handle renames of a directory
// to a file, communicate this specific
// scenario back, with its own Op:
// OpRsync_ToDirTakerGiverDirIsNowFile
func (s *SyncService) convertedDirToFile_giveFile(
	ctx0 context.Context,
	reqDir *RequestToSyncDir,
	ckt *rpc.Circuit,
	frag0 *rpc.Fragment,
	bt *byteTracker,
) error {
	path := reqDir.GiverDir

	tofile := s.U.NewFragment()
	tofile.FragSubject = path
	tofile.FragOp = OpRsync_ToDirTakerGiverDirIsNowFile
	// send back the dirReq for detail matching.
	tofile.Payload = frag0.Payload
	tofile.SetUserArg("structType", "RequestToSyncDir")
	err := ckt.SendOneWay(tofile, 0)
	panicOn(err)
	vv("Q: is this the right takerPath to pass to giverSendsWholefile? reqDir.TopTakerDirFinal = '%v'", reqDir.TopTakerDirFinal)
	err = s.giverSendsWholeFile(path, reqDir.TopTakerDirFinal, ckt, bt, frag0, reqDir.SR)
	panicOn(err)
	return err
}
