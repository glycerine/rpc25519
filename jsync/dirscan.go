package jsync

import (
	"context"
	"fmt"
	"io/fs"
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
	Path     string `zid:"0"`
	Size     int64  `zid:"1"`
	FileMode uint32 `zid:"2"`

	// See the ScanFlag const values below
	ScanFlags uint32 `zid:"3"`

	ModTime time.Time `zid:"4"`

	// symlink support
	SymLinkTarget string `zid:"5"`

	// Serially assigned number to allow
	// parallelization on the taker end.
	// Assigned in the order yielded.
	Serial int64 `zid:"6"`
}

const (
	ScanFlagFollowedSymlink uint32 = 1
	ScanFlagIsLeafDir       uint32 = 2
	ScanFlagIsMidDir        uint32 = 4
	ScanFlagIsSymLink       uint32 = uint32(fs.ModeSymlink) // 0x08000000
	ScanFlagIsDir           uint32 = uint32(fs.ModeDir)     // 0x80000000
)

func (f *File) IsSymlink() bool {
	return f.ScanFlags&ScanFlagIsSymLink != 0
}
func (f *File) IsLeafDir() bool {
	return f.ScanFlags&ScanFlagIsLeafDir != 0
}
func (f *File) IsMidDir() bool {
	return f.ScanFlags&ScanFlagIsMidDir != 0
}
func (f *File) IsDir() bool {
	return f.ScanFlags&ScanFlagIsDir != 0
}

// PackOfFiles is streamed in phase 2.
// All of these should be (only) files, not directories.
// They can and will include symlinks, at the moment anyway.
type PackOfFiles struct {
	Pack   []*File `zid:"0"`
	IsLast bool    `zid:"1"`

	TotalFileBytes int64 `zid:"2"`
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

	packOfLeavesCh = make(chan *PackOfLeafPaths, 5000)
	packOfFilesCh = make(chan *PackOfFiles, 5000)
	packOfDirsCh = make(chan *PackOfDirs, 5000)

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
				have += leafpack.Msgsize()
			} else {
				// send it off
				select {
				case packOfLeavesCh <- leafpack:
					// don't drop leafdir!
					leafpack = &PackOfLeafPaths{
						Pack: []string{leafdir},
					}
					have = leafpack.Msgsize()
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
			//close(packOfLeavesCh)
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

		nextF, stopF := iter.Pull2(di.FilesOnly(giverRoot))
		defer stopF()

		for {
			if pof == nil {
				// we've just sent off the last
				pof = &PackOfFiles{}
				have = pof.Msgsize()
			}

			regfile, ok, valid := nextF()
			if !valid {
				//vv("not valid, breaking, ok = %v", ok)
				break
			}
			if !ok {
				break
			}

			regfile.Path = regfile.Path[pre:]
			f := regfile
			//vv("pof packing File f = '%#v'", f)

			uses := f.Msgsize()

			if have+uses < max {
				pof.Pack = append(pof.Pack, f)
				have += pof.Msgsize()
				pof.TotalFileBytes += f.Size
			} else {
				// send it off
				select {
				case packOfFilesCh <- pof:
					// don't drop f!
					pof = &PackOfFiles{
						Pack: []*File{f},
					}
					have = pof.Msgsize()
				case <-halt.ReqStop.Chan:
					return
				case <-done:
					return
				}
			}
		} // for
		stopF()
		// send the last one
		if pof == nil {
			pof = &PackOfFiles{}
		}
		pof.IsLast = true
		select {
		case packOfFilesCh <- pof:
			pof = nil
			//close(packOfFilesCh)
		case <-halt.ReqStop.Chan:
			return
		case <-done:
			return
		}

		// =======================
		// phase three: sending all the directories with their mod/modtimes.
		// =======================

		nextD, stopD := iter.Pull2(di.AllDirsOnlyDirs(giverRoot))
		defer stopD()

		have = 0
		var pod *PackOfDirs

		for {
			if pod == nil {
				// we've just sent off the last
				pod = &PackOfDirs{}
				have = pod.Msgsize()
			}

			path, ok, valid := nextD()
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
				have += pod.Msgsize()
			} else {
				// send it off
				select {
				case packOfDirsCh <- pod:
					// don't drop dir!
					pod = &PackOfDirs{
						Pack: []*File{dir},
					}
					have = pod.Msgsize()

				case <-halt.ReqStop.Chan:
					return
				case <-done:
					return
				}
			}
		} // for
		stopD()
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

type DirSummary struct {
	NumFiles int64 `zid:"0"`
	NumBytes int64 `zid:"1"`
}

func ScanDirTreeOnePass(
	ctx context.Context,
	giverRoot string,

) (halt *idem.Halter,
	packOfFilesCh chan *PackOfFiles,
	totalFileCountCh chan *DirSummary,
	err0 error,
) {
	halt = idem.NewHalter()

	if !dirExists(giverRoot) {
		return nil, nil, nil, fmt.Errorf("ScanDirTree error: giverRoot not found, or not a directory: '%v'", giverRoot)
	}

	// make sure it ends in "/" or sep.
	if !strings.HasSuffix(giverRoot, sep) {
		giverRoot += sep
	}

	packOfFilesCh = make(chan *PackOfFiles, 5000)

	totalFileCountCh = make(chan *DirSummary, 1)

	done := ctx.Done()
	_ = done
	go func() {
		defer func() {
			halt.ReqStop.Close()
			halt.Done.Close()
		}()

		// ====================
		// all phases from one walk
		// ====================

		var sol []*File // slice of leaves
		var sof []*File // slice of files
		var sod []*File // slice of dirs

		di := NewDirIter()
		next, stop := iter.Pull2(di.OneWalkForAll(giverRoot))
		defer stop()

		// todo: could also measure/compare the parallel version:
		// resCh := ParallelOneWalkForAll(halt, giverRoot)

		pre := len(giverRoot) // how much to discard giverRoot, leave off sep.

		var nextSerial int64
		var totalDirBytes int64

		// first just fill up pof with everything.
		for {
			f, ok, valid := next()
			if !valid || !ok {
				//vv("not valid, breaking, ok = %v", ok)
				break
			}
			if f.Path[:pre] == giverRoot {
				//vv("trimming off giver root, pre=%v (giverRoot='%v'); f.Path '%v' -> '%v'", pre, giverRoot, f.Path, f.Path[pre:])
				f.Path = f.Path[pre:] // trim off giverRoot
				//vv("leafdir = '%v'", leafdir)
			} else {
				// jcp of . taker gets here, we were
				// corrupting the output of walk on such paths
				// by trimming them. So don't. I think
				// paths were just shorter.
			}

			f.Serial = nextSerial
			nextSerial++

			switch {
			case f.ScanFlags&ScanFlagIsLeafDir != 0:
				sol = append(sol, f)
			case f.ScanFlags&ScanFlagIsMidDir != 0:
				sod = append(sod, f)
			default:
				sof = append(sof, f)
				totalDirBytes += f.Size
			}
		}
		stop()

		var all []*File
		all = append(all, sol...)
		all = append(all, sof...)
		all = append(all, sod...)

		totalFileCountCh <- &DirSummary{
			NumFiles: int64(len(all)),
			NumBytes: totalDirBytes,
		}

		// pack up to max bytes of Chunks into a message.
		max := rpc.UserMaxPayload - 10_000

		// ====================
		// phase all: sending them all
		// ====================

		have := 0
		var pof *PackOfFiles

		for _, f := range all {
			if pof == nil {
				// we've just sent off the last
				pof = &PackOfFiles{}
				have = pof.Msgsize()
			}

			uses := f.Msgsize()

			if have+uses < max {
				pof.Pack = append(pof.Pack, f)
				have += pof.Msgsize()
				pof.TotalFileBytes += f.Size
			} else {
				// send it off
				select {
				case packOfFilesCh <- pof:
					// don't drop f!
					pof = &PackOfFiles{
						Pack: []*File{f},
					}
					have = pof.Msgsize()
				case <-halt.ReqStop.Chan:
					return
				case <-done:
					return
				}
			}
		} // for f := range all

		// send the last one
		if pof == nil {
			pof = &PackOfFiles{}
		}
		pof.IsLast = true
		select {
		case packOfFilesCh <- pof:
			pof = nil
		case <-halt.ReqStop.Chan:
			return
		case <-done:
			return
		}
	}()

	return
}

/*

package main

import (
	"fmt"
	"io/fs"
)

func main() {
	fmt.Printf("ModeSymlink = %x\n", uint32(fs.ModeSymlink))
	fmt.Printf("%x\n", uint32(0x8000000))
	//ModeSymlink = 8000000
	//              8000000

	fmt.Printf("ModeDir = %x\n", uint32(fs.ModeDir))             //        //FileMode = 1 << (32 - 1 - iota) // d: is a directory
	fmt.Printf("ModeAppend = %x\n", uint32(fs.ModeAppend))       //                                     // a: append-only
	fmt.Printf("ModeExclusive = %x\n", uint32(fs.ModeExclusive)) //                      // l: exclusive use
	fmt.Printf("ModeTemporary = %x\n", uint32(fs.ModeTemporary)) //
}

go run f.go
ModeSymlink = 8000000
8000000
ModeDir = 80000000
ModeAppend = 40000000
ModeExclusive = 20000000
ModeTemporary = 10000000

Compilation finished at Tue Feb  4 20:44:30
*/
