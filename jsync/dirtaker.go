package jsync

import (
	//"bufio"
	"context"
	"fmt"
	//"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"sync/atomic"
	"time"

	//"golang.org/x/sys/unix"
	//myblake3 "github.com/glycerine/rpc25519/hash"
	//"github.com/glycerine/rpc25519/progress"
	"github.com/glycerine/idem"
	rpc "github.com/glycerine/rpc25519"
	//"lukechampine.com/blake3"
)

var _ = time.Time{}
var _ = strconv.Atoi

// just measure for now, no creating hard links etc.
const useTempDir = false

// DirTaker is the directory top-level sync
// coordinator from the Taker side.
func (s *SyncService) DirTaker(ctx0 context.Context, ckt *rpc.Circuit, myPeer *rpc.LocalPeer, reqDir *RequestToSyncDir) (err0 error) {

	//vv("SyncService.DirTaker top; we are local if reqDir = %p != nil", reqDir)

	name := myPeer.PeerServiceName
	_ = name // used when logging is on.

	done0 := ctx0.Done()
	done := ckt.Context.Done()
	bt := &byteTracker{}

	//var needUpdate []*File
	var totalExpectedFileCount int64
	totFiles := 0
	var seenGiverSendsTopDirListing bool
	var fileUpdateCh chan *File
	//var wgIndivFileCheck *sync.WaitGroup
	var haltIndivFileCheck *idem.Halter
	needUpdate := rpc.NewMutexmap[string, *File]()
	takerCatalog := rpc.NewMutexmap[string, *File]()

	weAreRemoteTaker := (reqDir == nil)

	if reqDir != nil {
		if useTempDir {
			if reqDir.TopTakerDirTemp == "" {
				panic("reqDir.TopTakerDirTemp should have been created/filled in!")
			}
			if !dirExists(reqDir.TopTakerDirTemp) {
				panic(fmt.Sprintf("why does not exist reqDir.TopTakerDirTemp = '%v' ???", reqDir.TopTakerDirTemp))
			}
		}
	}

	defer func(reqDir *RequestToSyncDir) {
		//vv("%v: (ckt '%v') defer running! finishing DirTaker; reqDir=%p; err0='%v'", name, ckt.Name, reqDir, err0)
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
			//vv("dirtaker sees panic: '%v'", r)
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
				alwaysPrintf("dirtaker sees abnormal shutdown panic: '%v'", r)
				//panic(r)
			} else {
				//vv("DirTaker suppressing rpc.ErrContextCancelled or ErrHaltRequested, this is normal shutdown.")
			}
		}
	}(reqDir)

	// this is the DirTaker side
	for {
		select { // local dir take hung here??
		case frag := <-ckt.Reads:
			//vv("%v: (ckt %v) (DirTaker) ckt.Reads sees frag:'%s'", name, ckt.Name, frag)
			_ = frag
			switch frag.FragOp {

			///////////////// begin dir sync stuff

			case OpRsync_ToDirTakerGiverDirIsNowFile: // 39
				// whoops. we guessed we were asking
				// for a directory, but it turns out it
				// is (at least now) a file. Could have also had
				// a directory before that has now been
				// converted to a just file, of course.
				// Either way, dirgiver will now be
				// calling giverSendsWholeFile to us.
				// which means: OpRsync_HereIsFullFileBegin3,
				// OpRsync_HereIsFullFileMore4, and
				// OpRsync_HereIsFullFileEnd5. Let us
				// try and have the Taker() handle this
				// for us!
				//vv("DirTaker calling Taker on 39 OpRsync_ToDirTakerGiverDirIsNowFile")
				// convert to dirReq to syncReq
				reqDir3 := &RequestToSyncDir{}
				_, err := reqDir3.UnmarshalMsg(frag.Payload)
				panicOn(err)
				bt.bread += len(frag.Payload)
				sr := reqDir3.SR
				sr.GiverIsDir = false
				sr.Done = idem.NewIdemCloseChan()

				// clear out (any) existing directory;
				// maybe todo: make this optional?
				//vv("dirtaker: to convert dir to file, we wipe out dir: '%v'", sr.TakerPath)
				if sr.TakerPath == "" {
					panic("cannot have empty sr.TakerPath here.")
				}
				if dirExists(sr.TakerPath) {
					if sr.DryRun {
						alwaysPrintf("dry: would os.RemoveAll('%v')", sr.TakerPath)
					} else {
						err = os.RemoveAll(sr.TakerPath)
						panicOn(err)
					}
				}

				err = s.Taker(ctx0, ckt, myPeer, sr)
				//vv("Taker call in DirTaker got err = '%v'", err)
				//vv("Taker call in DirTaker got sr.Errs = '%v'", sr.Errs)
				panicOn(err)
				if sr.Errs != "" {
					panic(sr.Errs)
				}

				// we need to tell the remote giver to end too.

				// return? continue?
				// simplest to continue for the moment, though we might hang?
				// we want to allow multiple conversions from
				// dir to file if that happens... but lets start simpler,
				// for now.
				return

			case OpRsync_DirSyncBeginToTaker: // 22
				//vv("%v: (ckt '%v') (DirTaker) sees OpRsync_DirSyncBeginToTaker.", name, ckt.Name)
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
				if weAreRemoteTaker && useTempDir {

					targetTakerTopTempDir, tmpDirID, err := s.mkTempDir(
						reqDir.TopTakerDirFinal)
					panicOn(err)
					//vv("DirTaker (remote taker) made temp dir '%v' for finalDir '%v'", targetTakerTopTempDir, reqDir.TopTakerDirFinal)
					reqDir.TopTakerDirTemp = targetTakerTopTempDir
					reqDir.TopTakerDirTempDirID = tmpDirID
				}
				// INVAR: targetTakerTopTempDir is set.

				tmpReady := s.U.NewFragment()
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
				bt.bsend += len(bts)
				panicOn(err)

			case OpRsync_GiverSendsTopDirListing: // 26, all-one-pass version
				//vv("%v: (ckt '%v') (DirTaker) sees %v.", rpc.FragOpDecode(frag.FragOp), name, ckt.Name)
				// Getting this means here is the starting dir tree from giver.
				// now all in one pass, as PackOfFiles

				if !seenGiverSendsTopDirListing {
					seenGiverSendsTopDirListing = true
					haltIndivFileCheck = idem.NewHalter()

					// add an extra task so we don't hit zero
					// until we are ready to.
					haltIndivFileCheck.ReqStop.TaskAdd(1)

					// show progress for dir taking on indiv takes.
					s.localProgressCh = reqDir.SR.UpdateProgress

					takerCatalog.Reset()
					di := NewDirIter()
					localTree := di.ParallelWalk(reqDir.TopTakerDirFinal)
					for _, file := range localTree {
						takerCatalog.Set(file.Path, file)
					}
					//vv("at beginning, takerCatalog = '%#v'", takerCatalog.GetKeySlice())

					// unbuffered => sure somebody has it,
					// and will finish processing it before shutdown.
					fileUpdateCh = make(chan *File)

					// goro getting only 200 msec or so faster.
					ngoro := runtime.NumCPU()
					//wgIndivFileCheck = &sync.WaitGroup{}
					//wgIndivFileCheck.Add(ngoro)

					for i := range ngoro {
						goroHalt := idem.NewHalter()
						haltIndivFileCheck.AddChild(goroHalt)

						go func(i int, reqDir *RequestToSyncDir,
							fileUpdateCh chan *File,
							needUpdate *rpc.Mutexmap[string, *File]) {

							defer func() {
								//wgIndivFileCheck.Done()
								//haltIndivFileCheck.ReqStop.Close()
								//haltIndivFileCheck.Done.Close()
								r := recover()
								if r != nil {
									err := fmt.Errorf("DirTake worker for file indiv file check 26 (OpRsync_GiverSendsTopDirListing) saw panic: '%v'", r)
									goroHalt.ReqStop.CloseWithReason(err)
									// also stop the whole batch on single err.
									// At least for now, sane debugging.
									haltIndivFileCheck.ReqStop.CloseWithReason(err)
									alwaysPrintf("dirtaker panic caught: '%v'", err.Error())
								} else {
									goroHalt.ReqStop.Close()
								}
								goroHalt.Done.Close()
							}()

							for {
								select {
								case f := <-fileUpdateCh:
									s.takeOneFile(f, reqDir, needUpdate, takerCatalog, useTempDir)
									haltIndivFileCheck.ReqStop.TaskDone()
									if reqDir != nil && reqDir.SR != nil {
										atomic.AddInt64(&reqDir.SR.GiverFileSize,
											f.Size)
									}
								case <-goroHalt.ReqStop.Chan:
									return
								case <-haltIndivFileCheck.ReqStop.Chan:
									return

								case <-done:
									return
								case <-done0:
									return
								case <-ckt.Halt.ReqStop.Chan:
									return
								}
							}
						}(i, reqDir, fileUpdateCh, needUpdate)
					}
				} // end if !seenGiverSendsTopDirListing

				pof := &PackOfFiles{}
				_, err := pof.UnmarshalMsg(frag.Payload)
				panicOn(err)
				bt.bread += len(frag.Payload)

				tdir := reqDir.TopTakerDirFinal
				if useTempDir {
					tdir = reqDir.TopTakerDirTemp
				}

				for _, f := range pof.Pack {
					if totalExpectedFileCount != 0 {
						// very first frag will have total
						totalExpectedFileCount = frag.FragPart
					}
					switch {
					case f.ScanFlags&ScanFlagIsLeafDir != 0:
						if strings.HasPrefix(f.Path, "..") {
							panic(fmt.Sprintf("leafdir cannot start "+
								"with '..' or we will overwrite other"+
								" processes files in staging! bad leafdir: '%v'",
								f.Path))
						}
						takerCatalog.Del(f.Path)
						fullpath := filepath.Join(tdir, f.Path)
						err = os.MkdirAll(fullpath, 0700)
						panicOn(err)
						//vv("dirtaker made leafdir fullpath '%v'", fullpath)
					case f.ScanFlags&ScanFlagIsMidDir != 0:

						takerCatalog.Del(f.Path)
						fullpath := filepath.Join(tdir, f.Path)
						err = os.Chmod(fullpath, fs.FileMode(f.FileMode))
						panicOn(err)
						//vv("dirtaker set mode on dir = '%v'", f.Path)
					default:
						// regular file.
						totFiles++
						haltIndivFileCheck.ReqStop.TaskAdd(1)

						select {
						case fileUpdateCh <- f:
							//vv("sent on fileUpdateCh: f.Path = '%v'", f.Path)
						case <-done:
							return
						case <-done0:
							return
						case <-ckt.Halt.ReqStop.Chan:
							return
						case <-haltIndivFileCheck.ReqStop.Chan:
							//reas, _ := haltIndivFileCheck.ReqStop.Reason()
							//vv("haltIndivFileCheck.ReqStop.Chan closed reason='%v'", reas) // tasks all done.
							return
						}
					}
				}
				if pof.IsLast {
					//vv("pof.IsLast seen") // seen

					// okay, now that all regular files have bee
					// sent on fileUpdateCh, we can allow the
					// task count to reach zero. Subtract our
					// artifical 1 floor.
					at := haltIndivFileCheck.ReqStop.TaskDone()
					_ = at
					//vv("taskDone returned %v", at)                   // 18 left
					why := haltIndivFileCheck.ReqStop.TaskWait(done) // hung here
					_ = why
					//vv("haltIndivFileCheck.ReqStop.TaskWait() why='%v'", why)
					haltIndivFileCheck.StopTreeAndWaitTilDone(0, done, nil)

					err, _ := haltIndivFileCheck.ReqStop.Reason()
					if err != idem.ErrTasksAllDone {
						panicOn(err)
					}

					nn := needUpdate.Len()
					//vv("dirtaker sees pof.IsLast, sending "+
					//	"OpRsync_ToGiverAllTreeModesDone. "+
					//	"len(needUpdate) = %v; checked %v totFiles",
					//	nn, totFiles)

					if nn == 0 {
						//vv("got pof.IsLast, no update needed on "+
						//	"dirtaker side. checked %v files", totFiles)
					} else {
						err = s.dirTakerRequestIndivFiles(myPeer, needUpdate,
							reqDir, ckt, done, done0, bt, useTempDir)

						if err != nil {
							if err != idem.ErrTasksAllDone {
								alwaysPrintf("dirTakerRequestIndivFiles err = '%v'", err)
							}
						}
					}

					//vv("and end, takerCatalog = '%#v'", takerCatalog.GetKeySlice())
					//vv("and end, takerCatalog len = '%v'", takerCatalog.Len())

					// temp dir does not need to delete, it just
					// won't write into the temp dir to begin with.
					if useTempDir {
						//vv("useTempDir true: doing rename '%v' -> '%v'", reqDir.TopTakerDirTemp, reqDir.TopTakerDirFinal)

						haveOld := dirExists(reqDir.TopTakerDirFinal)
						var tmp string
						// Could do the atomic swap renameat2 (linux has) but
						// then we use cgo and its less portable.
						// Just do an extra step, rename old, then move
						// new into its place.
						if haveOld {
							// move the old version out of the way
							rnd := cryRandBytesBase64(18)
							tmp = filepath.Clean(reqDir.TopTakerDirFinal) + ".old." + rnd
							//vv("haveOld true, renaming before delete: '%v' -> '%v'", reqDir.TopTakerDirFinal, tmp)
							if reqDir.DryRun {
								alwaysPrintf("dry: would os.Rename(reqDir.TopTakerDirFinal='%v', tmp='%v')", reqDir.TopTakerDirFinal, tmp)
							} else {
								os.Rename(reqDir.TopTakerDirFinal, tmp)
							}
						}
						if reqDir.DryRun {
							alwaysPrintf("dry: would rename '%v' -> '%v'"+
								" and delete all '%v'", reqDir.TopTakerDirTemp,
								reqDir.TopTakerDirFinal, tmp)
						} else {
							err = os.Rename(reqDir.TopTakerDirTemp,
								reqDir.TopTakerDirFinal)
							panicOn(err)
							if haveOld {
								//vv("final dirtake act: delete old dir: '%v'", tmp)
								os.RemoveAll(tmp)
							}
						}
					} else {

						for path, file := range takerCatalog.GetMapReset() {
							if path == "" {
								// ignore root of taker dir; although maybe
								// we want to set mod-time/mode?
								continue
							}
							path = filepath.Join(reqDir.TopTakerDirFinal,
								file.Path)

							//vv("deleting taker only path: '%v'", path)
							if reqDir.DryRun {
								alwaysPrintf("dry: would remove '%v'", path)
							} else {
								if file.IsDir() {
									os.RemoveAll(path)
								} else {
									os.Remove(path)
								}
							}
						}
					}
					if !reqDir.GiverDirModTime.IsZero() {
						//vv("setting final dir mod time: '%v'", reqDir.GiverDirModTime)
						err = os.Chtimes(reqDir.TopTakerDirFinal,
							time.Time{}, reqDir.GiverDirModTime)
						panicOn(err)
					}

					// NOTICE: we actually return from DirTaker now(!)
					// this is the end of
					// OpRsync_GiverSendsTopDirListing: // 26,
					// the all-one-pass version.
					// vv("dirtaker returning nil!")
					//return nil

					end := s.U.NewFragment()
					end.FragOp = OpRsync_ToDirGiverEndingTotals // 42
					reqDir.SR.BytesRead = int64(bt.bread)
					reqDir.SR.BytesSent = int64(bt.bsend)
					bts, err := reqDir.MarshalMsg(nil)
					panicOn(err)
					end.Payload = bts
					err = ckt.SendOneWay(end, 0)
					bt.bsend += len(bts)
					panicOn(err)
					// wait for them to reply with 43
				} // end if pof.IsLast

				// INVAR: we have not encountered pof.IsLast
				// DirGiver will send us many pof, dirgiver.go:229.

				///////////////// end dir sync stuff

			case OpRsync_AckBackFIN_ToTaker:
				//vv("%v: (ckt '%v') (DirTaker) sees OpRsync_AckBackFIN_ToTaker. returning.", name, ckt.Name)
				return

			} // end switch FragOp

		case fragerr := <-ckt.Errors:
			//vv("%v: (ckt '%v') Taker fragerr = '%v'", name, ckt.Name, fragerr)
			_ = fragerr

			if fragerr != nil {
				// do we want to set reqDir.SR.Err? No, the defer will do it.
				if fragerr.Err != "" {
					return fmt.Errorf("%v", fragerr.Err)
				}
				panic(fmt.Sprintf("unhandled ckt.Errors in taker: '%v'", fragerr))
			}
			panic(fmt.Sprintf("what does a nil fragerr from ckt.Errors mean!?!?"+
				" when do we see it? (%v: (ckt '%v') Taker fragerr = '%v'",
				name, ckt.Name, fragerr))
			return
		case <-done:
			//vv("%v: (ckt '%v') done! cause: '%v'", name, ckt.Name, context.Cause(ckt.Context))
			return
		case <-done0:
			//vv("%v: (ckt '%v') done0! reason: '%v'", name, ckt.Name, context.Cause(ctx0))
			return
			//case <-s.halt.ReqStop.Chan:
			//zz("%v: (ckt '%v') top func halt.ReqStop seen", name, ckt.Name)
			//	return
		case <-ckt.Halt.ReqStop.Chan:
			//vv("%v: (ckt '%v') (DirTaker) ckt halt requested.", name, ckt.Name)
			return
		}
	}

}

// set useTempDir = false to just evaluate without making hard links
func (s *SyncService) takeOneFile(f *File, reqDir *RequestToSyncDir, needUpdate, takerCatalog *rpc.Mutexmap[string, *File], useTempDir bool) {

	defer func() {
		r := recover()
		if r != nil {
			alwaysPrintf("arg. panic noticed in takeOneFile! '%v'", r)
			panic(r)
		}
	}()

	// subtract from taker starting set, so
	// we can determine what to delete at the
	// end on the taker side.
	//vv("takeOneFile sees f.Path = '%v'; useTempDir = %v", f.Path, useTempDir)
	takerCatalog.Del(f.Path)

	tdir := reqDir.TopTakerDirTemp

	if !useTempDir {
		tdir = reqDir.TopTakerDirFinal
	}

	localPathToWrite := filepath.Join(
		tdir, f.Path)
	_ = localPathToWrite

	localPathToRead := filepath.Join(
		reqDir.TopTakerDirFinal, f.Path)

	//vv("dirTaker: f.Path = '%v' => localPathToRead = '%v'", f.Path, localPathToRead)
	//vv("dirTaker: f.Path = '%v' => localPathToWrite = '%v'", f.Path, localPathToWrite)
	var err error
	var fi os.FileInfo
	isSym := f.IsSymlink()
	if isSym {
		//vv("yes, isSym is true: f.Path= '%v'", f.Path)
		//fi, err = os.Lstat(localPathToRead)
	} else {
		// this will get fooled if taker has a symlink but giver is not.
		// So I think we need to always to Lstat.
		//fi, err = os.Stat(localPathToRead)
	}
	fi, err = os.Lstat(localPathToRead)
	fileExists := (err == nil)

	//vv("debug log: localPathToRead = '%v'; localPathToWrite = '%v'; has Lstat fi.ModTime = '%v'; fileExists='%v'; isSym='%v'", localPathToRead, localPathToWrite, fi.ModTime().Format(time.RFC3339Nano), fileExists, isSym)

	// might not exist, don't panic on err (really!)
	if !fileExists && !isSym {
		// but we can fix non-existant symlinks immediately

		// also we can make size 0 files immediately, repairing that.
		if f.Size == 0 && useTempDir {
			//vv("size 0 file does not exist at localPathToRead '%v', so make it at '%v'", localPathToRead, localPathToWrite)
			fd, err := os.Create(localPathToWrite)
			panicOn(err)
			fd.Close()
			err = os.Chtimes(localPathToWrite, time.Time{}, f.ModTime)
			panicOn(err)

		} else {
			needUpdate.Set(f.Path, f)
			//vv("Stat localPathToRead '%v' -> err '%v' so marking needUpdate", localPathToRead, err)
		}
		return
	} else {
		if isSym {
			//vv("have sym link in f: '%#v'", f)
			needWrite := false
			if !fileExists || useTempDir {
				needWrite = true
			} else {
				if fi.Mode()&fs.ModeSymlink != 0 {
					// current is symlink
					curTarget, err := os.Readlink(localPathToRead)
					panicOn(err)
					if curTarget != f.SymLinkTarget {
						needWrite = true
					} else {
						// also! does mod time need updating?
						if !fi.ModTime().Equal(f.ModTime) {
							updateLinkModTime(localPathToWrite, f.ModTime)
						}
					}
				} else {
					// current is regular file, to be replaced
					// by symlink
					needWrite = true
				}
			}
			if needWrite {
				//vv("symlink needs write! useTempDir = %v", useTempDir)
				// need to install to the new temp dir no matter.
				targ := f.SymLinkTarget
				os.Remove(localPathToWrite)
				//vv("installing symlink '%v' -> '%v'", localPathToWrite, targ) // dirtaker.go:559 2025-02-06 09:23:33.654 -0600 CST installing symlink 'linux11/Documentation/Changes' -> 'process/changes.rst'
				err := os.Symlink(targ, localPathToWrite) // panic: symlink process/changes.rst linux11/Documentation/Changes: no such file or directory
				panicOn(err)
				//vv("updating Lutimes for '%v'", localPathToWrite)
				updateLinkModTime(localPathToWrite, f.ModTime)
			}
			return // all symlinks done
		}

		if !fi.ModTime().Truncate(time.Second).Equal(f.ModTime.Truncate(time.Second)) ||
			fi.Size() != f.Size {
			needUpdate.Set(f.Path, f)
			//vv("fi.ModTime('%v') != f.ModTime '%v'; or", fi.ModTime(), f.ModTime)
			//vv("OR: fi.Size(%v) != f.Size(%v); => needUpdate for localPathToRead = '%v'", fi.Size(), f.Size, localPathToRead)
		} else {
			//vv("good: no update needed for localPathToRead: '%v';   f.Path = '%v'", localPathToRead, f.Path)

			if useTempDir {
				//vv("hard linking 10 '%v' <- '%v'", localPathToRead, localPathToWrite)
				panicOn(os.Link(localPathToRead, localPathToWrite))
				// just adjust mod time and fin.
				err = os.Chtimes(localPathToWrite, time.Time{}, f.ModTime)
				panicOn(err)
			}
		}
	}
}

func (s *SyncService) dirTakerRequestIndivFiles(
	myPeer *rpc.LocalPeer,
	needUpdate *rpc.Mutexmap[string, *File],
	reqDir *RequestToSyncDir,
	ckt *rpc.Circuit,
	done, done0 <-chan struct{},
	gbt *byteTracker,
	useTempDir bool,

) (err0 error) {

	t0 := time.Now()
	_ = t0
	nn := needUpdate.GetN()
	_ = nn
	//vv("top dirTakerRequestIndivFiles() with %v files needing updates.", nn)

	batchHalt := idem.NewHalter()
	// if we return early, this will shut down the
	// worker pool, since they have each have
	// a goroHalt that is added as a child.
	defer batchHalt.ReqStop.Close()

	//var totalFileBytes int64

	updateMap := needUpdate.GetMapReset()
	var bts []*byteTracker

	//wgjobs := &sync.WaitGroup{}
	//wgjobs.Add(len(updateMap))
	batchHalt.ReqStop.TaskAdd(len(updateMap))

	fileCh := make(chan *File) // do not buffer, giving work.

	workPoolSize := runtime.NumCPU()
	for worker := range workPoolSize {

		goroHalt := idem.NewHalter()
		batchHalt.AddChild(goroHalt)
		bt := &byteTracker{}
		bts = append(bts, bt)

		// background goro gets us 3x better throughput.
		// We are similar to rsync, within 2x the time
		// rsync: 1.6s vs jcp 2.8s to restore linux/Documentation
		// over LAN.
		//func(file *File, goroHalt *idem.Halter, bt *byteTracker, w int) {
		go func(fileCh chan *File, goroHalt *idem.Halter, bt *byteTracker, w int) {
			defer func() {

				// other side ctrl-c will give us a panic here
				r := recover()
				if r != nil {
					alwaysPrintf("dirTakerRequestIndivFiles() supressing panic: '%v':\nstack:\n%v\n", r, stack())
					err := fmt.Errorf("dirTakerRequestIndivFiles saw error: '%v'", r)
					goroHalt.ReqStop.CloseWithReason(err)
					// also stop the whole batch.
					// At least for now, sane debugging.
					batchHalt.ReqStop.CloseWithReason(err)
				} else {
					goroHalt.ReqStop.Close()
				}
				//vv("dirTakerRequestIndivFiles: dirtaker worker w=%v done.", w)
				goroHalt.Done.Close()
			}()
			//vv("top of dirTakerRequestIndivFiles: dirtaker worker w=%v.", w)

			var file *File
			var t1 time.Time
			for {
				select {
				case file = <-fileCh:
					//vv("dirtaker worker got file!")
					t1 = time.Now()

					// does its own SR == nil check.
					reqDir.SR.ReportProgress(
						"begin: "+filepath.Base(file.Path), file.Size, 0, t1)

				case <-goroHalt.ReqStop.Chan:
					//vv("worker w=%v exit on goroHalt.ReqStop.Chan: '%v'", w, goroHalt.ReqStop.Reason1())
					return
				}

				giverPath := filepath.Join(reqDir.GiverDir,
					file.Path)

				takerFinalPath := filepath.Join(reqDir.TopTakerDirFinal,
					file.Path)

				const keepData = false
				const wantChunks = true

				var err error
				var precis *FilePrecis
				var chunks *Chunks
				if parallelChunking {
					precis, chunks, err = ChunkFile(takerFinalPath)
				} else {
					precis, chunks, err = GetHashesOneByOne(rpc.Hostname,
						takerFinalPath)
				}
				panicOn(err)

				frag := s.U.NewFragment()
				frag.FragOp = OpRsync_RequestRemoteToGive // 12
				frag.FragSubject = giverPath
				//vv("dirtaker file worker sending 12")

				tmp := reqDir.TopTakerDirTemp
				if !useTempDir {
					tmp = ""
				}

				syncReq := &RequestToSyncPath{
					GiverPath:        giverPath,
					TakerPath:        file.Path,
					TakerTempDir:     tmp,
					TopTakerDirFinal: reqDir.TopTakerDirFinal,
					GiverDirAbs:      reqDir.GiverDir,

					GiverFileSize: file.Size,
					GiverModTime:  file.ModTime,
					GiverFileMode: file.FileMode,

					TakerFileSize: int64(precis.FileSize),
					TakerModTime:  precis.ModTime,
					TakerFileMode: precis.FileMode,

					RemoteTakes: false,
					Done:        idem.NewIdemCloseChan(),

					GiverScanFlags:     file.ScanFlags,
					GiverSymLinkTarget: file.SymLinkTarget,
					Precis:             precis,
					Chunks:             chunks,
				}
				// chunks likely big and need to be
				// grouped like in service.go

				var origChunks []*Chunk
				const K = 5000 // how many we keep in first message
				extraComing := false

				if len(syncReq.Chunks.Chunks) > K ||
					syncReq.Msgsize() > rpc.UserMaxPayload-10_000 {

					// must send chunks separately
					extraComing = true
					syncReq.MoreChunksComming = true
					//vv("set syncReq.MoreChunksComming = true")
					origChunks = syncReq.Chunks.Chunks

					// truncate down the initial Message,
					syncReq.Chunks.Chunks = syncReq.Chunks.Chunks[:K]

					upperBound := syncReq.Msgsize()
					if upperBound > rpc.UserMaxPayload-10_000 {
						panic(fmt.Sprintf("upperBound = %v > %v = "+
							"rpc.UserMaxPayload-10_000 even after splitting at K=%v",
							upperBound, rpc.UserMaxPayload-10_000, K))
					}
				}
				data, err := syncReq.MarshalMsg(nil)
				panicOn(err)
				bt.bsend += len(data)

				// restore so locals get it!
				if extraComing {
					syncReq.Chunks.Chunks = origChunks
				}

				frag.Payload = data

				// back to orig:
				frag.Payload = data
				frag.SetUserArg("structType", "RequestToSyncPath")
				cktName := rsyncRemoteGivesString // vs rsyncRemoteTakesString
				ckt2, ctx2, err := ckt.NewCircuit(cktName, frag)
				panicOn(err)

				if extraComing {

					xtra := &Chunks{
						Path:   syncReq.Chunks.Path,
						Chunks: origChunks[K:],
					}
					err = s.packAndSendChunksLimitedSize(
						xtra,
						frag.FragSubject,
						OpRsync_RequestRemoteToGive_ChunksLast,
						OpRsync_RequestRemoteToGive_ChunksMore,
						ckt2,
						bt,
						syncReq.Chunks.Path,
						syncReq,
					)
					if err != nil {
						alwaysPrintf("error back from packAndSendChunksLimitedSize()"+
							" during xtra sending: '%v'", err)
						//return err
						panicOn(err)
					}

				}

				defer func() {
					r := recover()
					if r != nil {
						err := fmt.Errorf(
							"panic recovered: '%v'", r)
						alwaysPrintf("Taker paniced! error ckt2.Close(err= '%v'); \nstack=\n%v", err, stack())
						ckt2.Close(err)
						panic(r) // let above defer also report errors.
					} else {
						//vv("normal ckt2 close")
						ckt2.Close(nil)
					}
				}()

				//vv("dirtaker worker: about to call s.Taker()")
				errg := s.Taker(ctx2, ckt2, myPeer, syncReq)
				//vv("dirtaker worker: got from Taker errg = '%v'", errg)
				panicOn(errg)
				left := batchHalt.ReqStop.TaskDone()
				_ = left
				//vv("dirtaker worker: back from s.Taker(), and TaskDone left=%v; errg='%v'", left, errg)
				// does its own SR == nil check.
				reqDir.SR.ReportProgress(
					"done : "+filepath.Base(giverPath), file.Size, file.Size, t1)

				bt.bsend += int(syncReq.BytesSent)
				bt.bread += int(syncReq.BytesRead)

			} // end for
		}(fileCh, goroHalt, bt, worker)
	} // end work pool starting

	k := -1
	_ = k
	for path, file := range updateMap {
		_ = path

		// can be slowing us down to print too much.
		k++
		//if k%1000 == 0 {
		//fmt.Printf("\nupdateMap progress:  %v  out of %v. elap %v\n", k, nn, time.Since(t0))
		//vv("dirtaker: needUpdate path '%v' -> file: '%#v'", path, file)

		//}

		select { // hung here
		case fileCh <- file:
			// and do another, until all updateMap files are done.
		case <-done:
			return
		case <-done0:
			return
		case <-batchHalt.ReqStop.Chan:
			// if we abort early on error, this will be closed.
			return
		}
	} // end range needUpdate

	err0 = batchHalt.ReqStop.TaskWait(done)
	//vv("batchHalt.ReqStop.TaskWait returned, err0 = '%v'", err0)

	batchHalt.StopTreeAndWaitTilDone(0, done, nil)
	//err0 = batchHalt.ReqStop.WaitTilChildrenClosed(done)
	//vv("batchHalt.ReqStop.WaitTilChildrenDone back.")
	// do not panic, we might have seen closed(done).

	//batchHalt.ReqStop.Close()

	// add up all the bts
	for i := range bts {
		gbt.bsend += bts[i].bsend
		gbt.bread += bts[i].bread
	}

	//vv("end dirTakerRequestIndivFiles: elap = '%v'; gbt='%#v'; err0 = '%v'", time.Since(t0), gbt, err0)
	return
}
