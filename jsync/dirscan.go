package jsync

import (
	"context"
	"os"
	"time"

	"github.com/glycerine/idem"
)

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
	packOfLeavesCh chan *PackOfLeaves,
	filesCh chan *PackOfFiles,
	packOfDirCh chan *PackOfDir,
	err0 error,
) {
	halt = idem.NewHalter()

	fd, err := os.Open(giverRoot)
	if err != nil {
		return nil, nil, nil, err
	}

	packOfLeavesCh = make(chan *PackOfLeaves)
	filesCh = make(chan *PackOfFiles)
	packOfDirCh = make(chan *PackOfDirs)

	n := 100

	// Important: we use fd.ReadDir so that
	// we get "directory order" rather than
	// sorted order. That way we can read large
	// flat directories incrementally, a small
	// bit at a time, without
	// waiting to read, or fit, all of their
	// entries into memory (or a Fragment) at once.
	dirents, derr := fd.ReadDir(n)
	if derr == io.EOF {
		// its fine, we have just read all
		// of the records in this first batch.
	} else if derr != nil {
		return nil, nil, nil, fmt.Errorf("ReadDir error on path '%v': '%v'", giverRoot, derr)
	}

	done := ctx.Done()
	go func() {
		defer func() {
			halt.ReqStop.Close()
			halt.Done.Close()
		}()

		gotDir := make(chan os.DirEntry, 100)

		var dfs1helper func(de os.DirEntry)
		dfs1helper = func(pre string, de os.DirEntry) {

			mypre := pre + de.Name()
			// start scan, finding all dir

			if de.IsDir() {
				dfs1helper(mypre, de)
				select {
				case gotDir <- de:
				case <-halt.ReqStop.Chan:
					return
				case <-done:
					return
				}
			}
		}

		// phase one: sending the leaf-directory-only structure
		for derr != io.EOF {

			// process the dirents we have
			for _, d := range dirents {
				if d.IsDir() {
					dfs1helper(giverRoot, d)
				}
			}

			select {
			case <-halt.ReqStop.Chan:
				return
			case <-done:
				return
			}
		}

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

	}()

	return
}
