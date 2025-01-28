package jsync

import (
	"context"
	//"fmt"
	//"io"
	"os"
	"time"

	"github.com/glycerine/idem"
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
// we send Packs of Dir for all
// the intermediate sub dir
// to set their Mode properly.

// PackOfLeafPaths is streamed first.
// We send in a Fragment some
// but not necessarily all of the leaf
// paths to recreate a directory structure.
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
// or PackOfDir.
type File struct {
	// Path should always be relative
	// to the GiverRoot or the TakerRoot,
	// and be the same in either case.
	Path     string    `zid:"0"`
	Size     int64     `zid:"1"`
	FileMode uint32    `zid:"2"`
	ModTime  time.Time `zid:"3"`
}

// PackOfFiles is streamed 2nd.
// All of these should be files.
type PackOfFiles struct {
	Files  []*File `zid:"0"`
	IsLast bool    `zid:"1"`
}

// PackOfDir is streamed last/3rd,
// when we set the mode/modtimes.
// All of these should be directories.
type PackOfDirs struct {
	Dirs   []*File `zid:"0"`
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
	takerRoot string,
	maxPackSz int,

) (halt *idem.Halter,
	packOfLeavesCh chan *PackOfLeafPaths,
	filesCh chan *PackOfFiles,
	packOfDirCh chan *PackOfDirs,
	err0 error,
) {
	halt = idem.NewHalter()

	if !dirExists(giverRoot) {
		return nil, nil, nil, fmt.Errorf("ScanDirTree error: giverRoot not found: '%v'", giverRoot)
	}

	packOfLeavesCh = make(chan *PackOfLeafPaths)
	filesCh = make(chan *PackOfFiles)
	packOfDirCh = make(chan *PackOfDirs)

	done := ctx.Done()
	_ = done
	go func() {
		defer func() {
			halt.ReqStop.Close()
			halt.Done.Close()
		}()

		// phase one: sending the leaf-directory-only structure

		di := NewDirIter()
		di.OmitRoot = true
		k := 0
		next, stop := iter.Pull2(di.DirsDepthFirstLeafOnly(giverRoot))
		defer stop()

		for {
			leafdir, ok, valid := next()
			_ = dir
			if !valid {
				vv("not valid, breaking, ok = %v", ok)
				break
			}
			if !ok {
				break
			}

			k++

		}

		/*
			// phase two: sending the file paths
			for {
				select {
				case <-halt.ReqStop.Chan:
					return
				case <-done:
					return
				}
			}

			// phase three: sending all the directories with their mod/modtimes.
			for {
				select {
				case <-halt.ReqStop.Chan:
					return
				case <-done:
					return
				}
			}
		*/
	}()

	return
}
