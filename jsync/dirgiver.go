package jsync

import (
	"context"
	"fmt"
	//"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	//"github.com/glycerine/rpc25519/progress"
	"github.com/glycerine/idem"
	rpc "github.com/glycerine/rpc25519"
)

var _ = fmt.Printf
var _ = filepath.Join
var _ = runtime.NumCPU
var _ = time.Time{}
var _ = strconv.Atoi

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
	_ = fileCh
	var batchHalt *idem.Halter
	_ = batchHalt

	//var haltDirScan *idem.Halter
	//var packOfLeavesCh chan *PackOfLeafPaths
	var packOfFilesCh chan *PackOfFiles
	_ = packOfFilesCh
	var packOfDirsCh chan *PackOfDirs
	_ = packOfDirsCh

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
			//vv("dirgiver sees panic: '%v'", r)
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
				alwaysPrintf("dirgiver sees abnormal shutdown panic: '%v'", r)
				//panic(r)
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
			case OpRsync_ToDirGiverEndingTotals: // 42
				reqDirFin := &RequestToSyncDir{}
				_, err0 = reqDirFin.UnmarshalMsg(frag0.Payload)
				panicOn(err0)
				bt.bread += len(frag0.Payload)
				//vv("dirgiver 42 sees reqDirFin.SR.BytesRead = %v; sent = %v",
				//reqDirFin.SR.BytesRead, reqDirFin.SR.BytesSent)

				// swap these around, since they are from the remote taker.
				bt.bread = int(reqDirFin.SR.BytesSent)
				bt.bsend = int(reqDirFin.SR.BytesRead)

				// tell dirtaker to shut down. dir sync is done.
				s.ackBackFINToTaker(ckt, frag0)
				frag0 = nil // GC early.
				continue

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
				bt.bsend += len(bts)

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

					// tell the indiv file givers where to log progress
					// notice this is the top level dirgiver
					// communicating with the paralle sub giver calls
					// that are actually invoked by the remote dirtaker.
					s.localProgressCh = reqDir.SR.UpdateProgress
				}

				// is it now a file that was guessed (or used to be) a dir?
				if fileExists(reqDir.GiverDir) {
					//vv("reqDir.GiverDir '%v' is a file, not a directory as expected.", reqDir.GiverDir)

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
				var dirsum *DirSummary

			sendOnePass:
				for i := 0; ; i++ {
					select {
					case dirsum = <-totalFileCountCh:
						totalFileCountCh = nil

					case pof := <-packOfFilesCh:
						lastser := int64(0)
						k := len(pof.Pack)
						if k > 0 {
							lastser = pof.Pack[k-1].Serial
						} else {
							lastser = dirsum.NumFiles // on first Frag, give total.
						}

						fragPOF := s.U.NewFragment()
						bts, err := pof.MarshalMsg(nil)
						panicOn(err)
						fragPOF.Payload = bts
						fragPOF.FragOp = OpRsync_GiverSendsTopDirListing // 26
						fragPOF.FragPart = lastser
						fragPOF.SetUserArg("structType", "PackOfFiles")
						//vv("dirgiver sending 26 with len(pof) = %v; pof[0] = '%#v'", len(pof.Pack), pof.Pack[0])
						err = ckt.SendOneWay(fragPOF, 0)
						panicOn(err)
						bt.bsend += len(bts)

						if reqDir != nil && reqDir.SR != nil {
							atomic.AddInt64(&reqDir.SR.GiverFileSize,
								pof.TotalFileBytes)
						}

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
				///////////// end dir sync stuff

			case OpRsync_AckBackFIN_ToGiver:
				//vv("%v: (ckt '%v') (DirGiver) sees OpRsync_AckBackFIN_ToGiver. returning.", name, ckt.Name)
				return

			} // end switch frag0.FragOp

		case fragerr := <-ckt.Errors:
			//vv("%v: (ckt %v) (DirGiver) ckt.Errors sees fragerr:'%s'", name, ckt.Name, fragerr)
			_ = fragerr
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
	tofile.FragOp = OpRsync_ToDirTakerGiverDirIsNowFile // 39
	// send back the dirReq for detail matching.
	tofile.Payload = frag0.Payload
	tofile.SetUserArg("structType", "RequestToSyncDir")
	bt.bsend += len(tofile.Payload)
	err := ckt.SendOneWay(tofile, 0)
	panicOn(err)

	//vv("Q: is this the right takerPath to pass to giverSendsWholefile? reqDir.TopTakerDirFinal = '%v'", reqDir.TopTakerDirFinal)
	err = s.giverSendsWholeFile(path, reqDir.TopTakerDirFinal, ckt, bt, frag0, reqDir.SR)
	panicOn(err)
	return err
}
