package jsync

import (
	"context"
	"fmt"
	//"io"
	"iter"
	"os"
	"strings"
	"time"

	"github.com/glycerine/idem"
	rpc "github.com/glycerine/rpc25519"
)

//go:generate greenpack -no-dedup=true

//msgp:ignore DirScanner
type DirScanner struct {
	RootPath string
}

// Directory scanning protocol:
//
// We want to scan the directory a little
// at a time, as efficiently as
// we can, allowing for very large numbers
// of files and sub directories. An
// AWS S3 bucket of keys will be a completely
// flat tree, for instance -- and we should
// not choke.
//
// It is hard for us to do anything in parallel until
// we know that our directory structure is in place.
// So first scan and send the only the directory
// structure, in a depth first search. In fact,
// we only need to find the leaves.
// Once the leaves are are made, we have the full
// tree, and files can be synced. Only
// after that do we want to
// we send Packs of Dirs for all
// the intermediate sub dir
// to set their Mode (permissions) properly.

// PackOfLeafPaths is phase 1: it is streamed first.
// We send in a Fragments (incrementally)
// all of the leaf paths to recreate a directory structure.
// Once we see IsLast true, we know
// we've seen, and created, the whole tree.
// Then we can proceed to streaming files
// through.
type PackOfLeafPaths struct {
	// Each member of Pack is a leaf dir
	// that needs to have os.MkdirAll() called on it.
	// these should all be the same relative
	// path, whether relative to the taker or the giver.
	Pack   []string `zid:"0"`
	IsLast bool     `zid:"1"`
}

// A File is a directory or regular file,
// and it should be sent in either PackOfFiles
// or PackOfDir, in phase 2.
type File struct {
	// Path should always be relative
	// to the GiverRoot or the TakerRoot,
	// and be the same in either case.
	Path     string    `zid:"0"`
	Size     int64     `zid:"1"`
	FileMode uint32    `zid:"2"`
	ModTime  time.Time `zid:"3"`
}

// PackOfFiles is streamed ih phase 2.
// All of these should be (only) files, not directories.
// They can and will include symlinks, at the moment anyway.
type PackOfFiles struct {
	Pack   []*File `zid:"0"`
	IsLast bool    `zid:"1"`
}

// PackOfDir is streamed last/3rd; in phase 3,
// when we set the mode/modtimes.
// All of these should be directories.
type PackOfDirs struct {
	Pack   []*File `zid:"0"`
	IsLast bool    `zid:"1"`
}

// ScanDirTree returns three channels
// that will have packs of leaves, files,
// and dirs. They should be processed
// in that order, in three phasees.
// First all the leaves should finish
// being created. Then the
// files can be sent in parallel, and
// once they have all been synced, the
// PackOfDir can be sent to adjust
// the mode/modTime on each dir.
// The internal scanning goroutine
// will return its halter too.
func ScanDirTree(
	ctx context.Context,
	giverRoot string,

) (halt *idem.Halter,
	packOfLeavesCh chan *PackOfLeafPaths,
	packOfFilesCh chan *PackOfFiles,
	packOfDirsCh chan *PackOfDirs,
	err0 error,
) {
	halt = idem.NewHalter()

	if !dirExists(giverRoot) {
		return nil, nil, nil, nil, fmt.Errorf("ScanDirTree error: giverRoot not found, or not a directory: '%v'", giverRoot)
	}

	// make sure it ends in "/" or sep.
	if !strings.HasSuffix(giverRoot, sep) {
		giverRoot += sep
	}

	packOfLeavesCh = make(chan *PackOfLeafPaths)
	packOfFilesCh = make(chan *PackOfFiles)
	packOfDirsCh = make(chan *PackOfDirs)

	done := ctx.Done()
	_ = done
	go func() {
		defer func() {
			halt.ReqStop.Close()
			halt.Done.Close()
		}()

		// ====================
		// phase one: sending the leaf-directory-only structure
		// ====================

		di := NewDirIter()
		next, stop := iter.Pull2(di.DirsDepthFirstLeafOnly(giverRoot))
		defer stop()

		// pack up to max bytes of Chunks into a message.
		max := rpc.UserMaxPayload - 10_000
		pre := len(giverRoot) // how much to discard giverRoot, leave off sep.

		var leafpack *PackOfLeafPaths
		var have int

		for {
			if leafpack == nil {
				// we've just sent off the last
				leafpack = &PackOfLeafPaths{}
				have = leafpack.Msgsize()
			}

			leafdir, ok, valid := next()
			if !valid {
				//vv("not valid, breaking, ok = %v", ok)
				break
			}
			if !ok {
				break
			}

			// trim off giverRoot
			leafdir = leafdir[pre:]
			//vv("leafdir = '%v'", leafdir)
			uses := len(leafdir)

			if have+uses < max {
				leafpack.Pack = append(leafpack.Pack, leafdir)
				have = leafpack.Msgsize()
			} else {
				// send it off
				select {
				case packOfLeavesCh <- leafpack:
					leafpack = nil
				case <-halt.ReqStop.Chan:
					return
				case <-done:
					return
				}
			}
		} // for
		stop()
		// send the last one
		if leafpack == nil {
			leafpack = &PackOfLeafPaths{}
		}
		leafpack.IsLast = true
		select {
		case packOfLeavesCh <- leafpack:
			leafpack = nil
			close(packOfLeavesCh)
		case <-halt.ReqStop.Chan:
			return
		case <-done:
			return
		}

		// ====================
		// phase two: sending the file paths + stat info
		// ====================

		have = 0
		var pof *PackOfFiles

		next, stop = iter.Pull2(di.FilesOnly(giverRoot))
		defer stop()

		for {
			if pof == nil {
				// we've just sent off the last
				pof = &PackOfFiles{}
				have = pof.Msgsize()
			}

			path, ok, valid := next()
			if !valid {
				//vv("not valid, breaking, ok = %v", ok)
				break
			}
			if !ok {
				break
			}

			fi, err := os.Stat(path)
			panicOn(err)

			// trim off giverRoot
			path = path[pre:]

			f := &File{
				Path:     path,
				Size:     fi.Size(),
				FileMode: uint32(fi.Mode()),
				ModTime:  fi.ModTime(),
			}

			uses := f.Msgsize()

			if have+uses < max {
				pof.Pack = append(pof.Pack, f)
				have = pof.Msgsize()
			} else {
				// send it off
				select {
				case packOfFilesCh <- pof:
					pof = nil
				case <-halt.ReqStop.Chan:
					return
				case <-done:
					return
				}
			}
		} // for
		stop()
		// send the last one
		if pof == nil {
			pof = &PackOfFiles{}
		}
		pof.IsLast = true
		select {
		case packOfFilesCh <- pof:
			pof = nil
			close(packOfFilesCh)
		case <-halt.ReqStop.Chan:
			return
		case <-done:
			return
		}

		// =======================
		// phase three: sending all the directories with their mod/modtimes.
		// =======================

		next, stop = iter.Pull2(di.AllDirsOnlyDirs(giverRoot))
		defer stop()

		have = 0
		var pod *PackOfDirs

		for {
			if pod == nil {
				// we've just sent off the last
				pod = &PackOfDirs{}
				have = pod.Msgsize()
			}

			path, ok, valid := next()
			if !valid {
				//vv("not valid, breaking, ok = %v", ok)
				break
			}
			if !ok {
				break
			}

			fi, err := os.Stat(path)
			panicOn(err)

			// trim off giverRoot
			path = path[pre:]

			dir := &File{
				Path:     path,
				Size:     fi.Size(),
				FileMode: uint32(fi.Mode()),
				ModTime:  fi.ModTime(),
			}

			uses := dir.Msgsize()

			if have+uses < max {
				pod.Pack = append(pod.Pack, dir)
				have = pod.Msgsize()
			} else {
				// send it off
				select {
				case packOfDirsCh <- pod:
					pod = nil
					close(packOfDirsCh)
				case <-halt.ReqStop.Chan:
					return
				case <-done:
					return
				}
			}
		} // for
		stop()
		// send the last one
		if pod == nil {
			pod = &PackOfDirs{}
		}
		pod.IsLast = true
		select {
		case packOfDirsCh <- pod:
			pod = nil
		case <-halt.ReqStop.Chan:
			return
		case <-done:
			return
		}
	}()

	return
}
