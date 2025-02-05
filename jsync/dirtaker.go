package jsync

import (
	//"bufio"
	"context"
	"fmt"
	//"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	//"sync"
	"time"

	"golang.org/x/sys/unix"
	//myblake3 "github.com/glycerine/rpc25519/hash"
	//"github.com/glycerine/rpc25519/progress"
	//"github.com/glycerine/idem"
	rpc "github.com/glycerine/rpc25519"
	//"lukechampine.com/blake3"
)

var _ = time.Time{}

// DirTaker is the directory top-level sync
// coordinator from the Taker side.
func (s *SyncService) DirTaker(ctx0 context.Context, ckt *rpc.Circuit, myPeer *rpc.LocalPeer, reqDir *RequestToSyncDir) (err0 error) {

	vv("SyncService.DirTaker top; we are local if reqDir = %p != nil", reqDir)

	name := myPeer.PeerServiceName
	_ = name // used when logging is on.

	done0 := ctx0.Done()
	done := ckt.Context.Done()
	bt := &byteTracker{}

	var needUpdate []*File
	totFiles := 0

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
		//vv("%v: (ckt '%v') defer running! finishing DirTaker; reqDir=%p; err0='%v'", name, ckt.Name, reqDir, err0)
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
				// we should: setup a top tempdir and prep to
				// pre-pend it to all paths we get from giver.

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
				//vv("%v: (ckt '%v') (DirTaker) sees OpRsync_DirSyncEndToTaker", name, ckt.Name)
				// we (taker) can rename the temp top dir/replace any old top dir.

				tmp := reqDir.TopTakerDirTemp
				// get rid of any trailing '/' slash, so we can tack on .oldvers
				final := filepath.Clean(reqDir.TopTakerDirFinal)
				//vv("dirtaker would rename completed dir into place!: %v -> %v", tmp, final)
				var err error

				rndsuf := rpc.NewCryRandSuffix()
				old := ""
				if dirExists(final) {
					// move the old dir out of the way.
					old = final + ".old." + rndsuf
					//vv("dirtaker backup previous dir '%v' -> '%v'", final, old)
					err := os.Rename(final, old)
					panicOn(err)
				}
				// put the new directory in its place.
				err = os.Rename(tmp, final)
				panicOn(err)
				if old != "" {
					// We have hard linked all the unchanged files into the new.
					// Now delete the old version (hard link count -> 1).
					//vv("TODO debug actually remove old dir: '%v'", old)
					panicOn(os.RemoveAll(old))
				}
				// and set the mod time
				if !reqDir.GiverDirModTime.IsZero() {
					//vv("setting final dir mod time: '%v'", reqDir.GiverDirModTime)
					err = os.Chtimes(final, time.Time{}, reqDir.GiverDirModTime)
					panicOn(err)
				}

				// reply with OpRsync_DirSyncEndAckFromTaker, wait for FIN.

				alldone := rpc.NewFragment()
				alldone.FragOp = OpRsync_DirSyncEndAckFromTaker
				err = ckt.SendOneWay(alldone, 0)
				panicOn(err)

			case OpRsync_GiverSendsTopDirListing: // 26, all-one-pass version
				//vv("%v: (ckt '%v') (DirTaker) sees %v.", rpc.FragOpDecode(frag.FragOp), name, ckt.Name)
				// Getting this means here is the starting dir tree from giver.
				// now all in one pass, as PackOfFiles

				pof := &PackOfFiles{}
				_, err := pof.UnmarshalMsg(frag.Payload)
				panicOn(err)
				bt.bread += len(frag.Payload)

				for _, f := range pof.Pack {
					switch {
					case f.ScanFlags&ScanFlagIsLeafDir != 0:
						if strings.HasPrefix(f.Path, "..") {
							panic(fmt.Sprintf("leafdir cannot start "+
								"with '..' or we will overwrite other"+
								" processes files in staging! bad leafdir: '%v'",
								f.Path))
						}
						fullpath := filepath.Join(reqDir.TopTakerDirTemp, f.Path)
						err = os.MkdirAll(fullpath, 0700)
						panicOn(err)
						//vv("dirtaker made leafdir fullpath '%v'", fullpath)
					case f.ScanFlags&ScanFlagIsMidDir != 0:

						fullpath := filepath.Join(reqDir.TopTakerDirTemp, f.Path)
						err = os.Chmod(fullpath, fs.FileMode(f.FileMode))
						panicOn(err)
						//vv("dirtaker set mode on dir = '%v'", f.Path)

					default:
						// regular file.
						totFiles++

						localPathToWrite := filepath.Join(
							reqDir.TopTakerDirTemp, f.Path)
						_ = localPathToWrite

						localPathToRead := filepath.Join(
							reqDir.TopTakerDirFinal, f.Path)

						//vv("dirTaker: f.Path = '%v' => localPathToRead = '%v'", f.Path, localPathToRead)
						//vv("dirTaker: f.Path = '%v' => localPathToWrite = '%v'", f.Path, localPathToWrite)
						fi, err := os.Lstat(localPathToRead)
						// might not exist, don't panic on err (really!)
						if err != nil {
							needUpdate = append(needUpdate, f)
							vv("Stat localPathToRead '%v' -> err '%v' so marking needUpdate", localPathToRead, err)
						} else {
							if f.IsSymlink() {
								curTarget, err := os.Readlink(localPathToRead)
								panicOn(err)
								if curTarget != f.SymLinkTarget {
									targ := f.SymLinkTarget
									os.Remove(localPathToWrite)
									err := os.Symlink(targ, localPathToWrite)
									panicOn(err)
								}
								tv := unix.NsecToTimeval(f.ModTime.UnixNano())
								unix.Lutimes(localPathToWrite, []unix.Timeval{tv, tv})
								continue // to next file
							}

							if !fi.ModTime().Truncate(time.Second).Equal(f.ModTime.Truncate(time.Second)) ||
								fi.Size() != f.Size {
								needUpdate = append(needUpdate, f)
								vv("fi.ModTime('%v') != f.ModTime '%v'; or", fi.ModTime(), f.ModTime)
								vv("OR: fi.Size(%v) != f.Size(%v); => needUpdate for localPathToRead = '%v'", fi.Size(), f.Size, localPathToRead)
							} else {
								//vv("good: no update needed for localPathToRead: '%v';   f.Path = '%v'", localPathToRead, f.Path)

								if localPathToWrite != localPathToRead {
									//vv("hard linking 10 '%v' <- '%v'",
									//	localPathToRead, localPathToWrite)
									panicOn(os.Link(localPathToRead, localPathToWrite))
								}
								// just adjust mod time and fin.
								err = os.Chtimes(localPathToWrite, time.Time{}, f.ModTime)
								panicOn(err)

							}
						}
					}
				}
				if pof.IsLast {
					vv("dirtaker sees pof.IsLast, sending "+
						"OpRsync_ToGiverAllTreeModesDone. "+
						"len(needUpdate) = %v; checked %v totFiles",
						len(needUpdate), totFiles)

					if len(needUpdate) == 0 {
						vv("got pof.IsLast, no update needed on "+
							"dirtaker side. checked %v files", totFiles)
					}

					modesDone := rpc.NewFragment()
					modesDone.FragOp = OpRsync_ToGiverAllTreeModesDone
					err = ckt.SendOneWay(modesDone, 0)
					panicOn(err)
				}

				// old? we think old:
				// reply to OpRsync_GiverSendsTopDirListingEnd
				// with OpRsync_TakerReadyForDirContents
				// old? we think old:
				// dirgiver should reply to OpRsync_TakerReadyForDirContents
				// with parallel individual file sends... then
				// OpRsync_ToTakerDirContentsDone

			case OpRsync_GiverSendsPackOfFiles: // 36

				pof := &PackOfFiles{}
				_, err := pof.UnmarshalMsg(frag.Payload)
				panicOn(err)
				bt.bread += len(frag.Payload)

				if pof.IsLast {
					vv("dirtaker sees last of pof PackOfFiles")
				}
				// TODO: compare with on disk, only get updates we need
				// if size and modtime mismatch.

			case OpRsync_ToTakerDirContentsDone:

				// wait for all our file syncs to finish.
				// How? I think the giver has to manage this.
				// They started them.

				giverTotalFileBytesStr, ok := frag.GetUserArg(
					"giverTotalFileBytes")
				if ok {
					giverTotalFileBytes, err := strconv.Atoi(
						giverTotalFileBytesStr)
					panicOn(err)
					//vv("OpRsync_ToTakerDirContentsDone: "+
					//	"giverTotalFileBytes = %v", giverTotalFileBytes)
					if reqDir != nil {
						reqDir.GiverTotalFileBytes = int64(giverTotalFileBytes)
						reqDir.SR.GiverFileSize = int64(giverTotalFileBytes)
					}
				}

				ackAllFilesDone := rpc.NewFragment()
				ackAllFilesDone.FragOp = OpRsync_ToGiverDirContentsDoneAck
				ackAllFilesDone.SetUserArg("giverTotalFileBytes",
					giverTotalFileBytesStr)
				err := ckt.SendOneWay(ackAllFilesDone, 0)
				panicOn(err)

				// dirgiver should reply to OpRsync_ToGiverDirContentsDoneAck
				// with OpRsync_ToTakerAllTreeModes

			case OpRsync_ToTakerAllTreeModes:
				// phase 3: set the mode of all dirs in the tree.

				//vv("%v: (ckt '%v') (DirTaker) sees %v.", rpc.FragOpDecode(frag.FragOp), name, ckt.Name)
				// Getting this means all the file content has been
				// sent to me, and now we are setting the mode of dirs
				// on the whole tree.

				pod := &PackOfDirs{}
				_, err := pod.UnmarshalMsg(frag.Payload)
				panicOn(err)
				bt.bread += len(frag.Payload)

				for _, dir := range pod.Pack {
					fullpath := filepath.Join(reqDir.TopTakerDirTemp, dir.Path)
					err = os.Chmod(fullpath, fs.FileMode(dir.FileMode))
					panicOn(err)
					//vv("dirtaker set mode on dir = '%v'", dir)
				}
				if pod.IsLast {
					vv("dirtaker sees pod.IsLast, sending " +
						"OpRsync_ToGiverAllTreeModesDone")
					modesDone := rpc.NewFragment()
					modesDone.FragOp = OpRsync_ToGiverAllTreeModesDone
					err = ckt.SendOneWay(modesDone, 0)
					panicOn(err)
				}
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
