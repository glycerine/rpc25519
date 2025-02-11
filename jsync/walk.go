package jsync

import (
	"fmt"
	"io/fs"
	"iter"
	"os"
	"path/filepath"

	"github.com/glycerine/idem"
	pwalk "github.com/glycerine/parallelwalk"
)

var ErrHaltRequested = fmt.Errorf("halt requested")

// DirIter efficiently scans a filesystems directory
// file tree. The BatchSize is used to limit how
// many directory entries (files and sub-directories)
// are read at once, allowing efficiency on very
// flat, broad directories with large numbers of files
// (like S3).
//
// It uses the new Go iter approach to provide iter.Pull2
// usable iterators.
//
// MaxDepth and FollowSymlinks are available as options --
// they apply only to the FilesOnly iteration.
type DirIter struct {
	// BatchSize is how many directory entries we read at once,
	// to keep memory use low.
	BatchSize int

	// FollowSymlinks can result in results that must
	// be de-duplicated if multiple symlink paths give
	// the same file.
	FollowSymlinks bool

	// MaxDepth restricts how deeply we walk into the
	// filesystem tree. Resolving a symlink only counts
	// as one depth level, even if it involved
	// chasing multiple symlinks to their target.
	MaxDepth int
}

// NewDirIter creates a new DirIter.
func NewDirIter() *DirIter {
	return &DirIter{
		BatchSize: 100,
	}
}

// DirsDepthFirstLeafOnly walks the filesystem from root.
// It returns only the deepest (leaf) directories.
// These suffice to re-create the directory structure.
func (di *DirIter) DirsDepthFirstLeafOnly(root string) iter.Seq2[string, bool] {
	return func(yield func(string, bool) bool) {

		// Helper function for recursive traversal
		var visit func(path string) bool
		visit = func(path string) bool {
			dir, err := os.Open(path)
			if err != nil {
				return yield(path, false)
			}
			defer dir.Close()

			hasSubdirs := false
			for {
				entries, err := dir.ReadDir(di.BatchSize)
				// Process entries in directory order
				for _, entry := range entries {
					if entry.IsDir() {
						hasSubdirs = true
						// Recurse immediately when we find a directory
						if !visit(filepath.Join(path, entry.Name())) {
							return false
						}
					}
				}

				if err != nil || len(entries) < di.BatchSize {
					break
				}
			}

			// If this is a leaf directory, yield it
			if !hasSubdirs {
				return yield(path, true)
			}
			return true
		}

		// Start the recursion
		visit(root)
	}
}

/* from dirscan for reference:
type File struct {
	Path      string   `zid:"0"`
	Size      int64    `zid:"1"`
	FileMode  uint32   `zid:"2"`
	ScanFlags uint32   `zid:"3"`

	ModTime  time.Time `zid:"4"`

	// symlink support
	SymLinkTarget string `zid:"5"`

	// Serially assigned number to allow
	// parallelization on the taker end.
	// Assigned in the order yielded.
	Serial int64 `zid:"6"`
}
*/

// FilesOnly returns only files, skipping directories. This does
// return symlinks as files too, if di.FollowSymlinks is false.
// If di.FollowSymlinks is true, and a symlink links to a
// directory, the recursion will follow the symlink down
// that directory tree.
//
// While this can result in finding the same file
// mmultiple times if there
// are multiple paths throught symlinks to the same file,
// we now use an internal map and dedup so duplicate
// paths are not returned a second time. This only
// happens when the FollowSymlinks option is true.
//
// Resolving a symlink through multiple other symlinks
// only counts as one "depth level" for MaxDepth stopping.
func (di *DirIter) FilesOnly(root string) iter.Seq2[*File, bool] {
	return func(yield func(*File, bool) bool) {

		var seen map[string]bool
		if di.FollowSymlinks {
			// symlinks can cause duplicated paths, so
			// we de-duplicate the paths when following
			// symlinks.
			seen = make(map[string]bool)
		}

		// Helper function for recursive traversal
		var visit func(path string, depth int) bool

		visit = func(path string, depth int) bool {
			//vv("top of visit, path = '%v'; depth = %v", path, depth)
			if di.MaxDepth > 0 && depth >= di.MaxDepth {
				return true // true lets cousins also get to max depth.
			}
			dir, err := os.Open(path)
			if err != nil {
				return yield(nil, false)
			}
			defer dir.Close()

			for {
				entries, err := dir.ReadDir(di.BatchSize)
				// Process entries in directory order
				for _, entry := range entries {
					//vv("entry = '%#v'; entry.Type()&fs.ModeSymlink = %v", entry, entry.Type()&fs.ModeSymlink)

					resolveMe := filepath.Join(path, entry.Name())

					if entry.Type()&fs.ModeSymlink != 0 {
						//vv("have symlink '%v'", resolveMe)

						//vv("resolveMe:'%v' -> target:'%v'", resolveMe, target)
						if di.FollowSymlinks {
							target, err := filepath.EvalSymlinks(resolveMe)
							if err != nil {
								// allow dangling links to not stop the walk.
								continue
							}
							fi, err := os.Stat(target)
							if err != nil {
								return false
							}
							if fi.IsDir() {
								// Recurse immediately when we find a directory
								if !visit(target, depth+1) {
									return false
								}
							} else {
								if seen != nil {
									if seen[target] {
										continue
									} else {
										seen[target] = true
									}
								}

								scanFlags := ScanFlagFollowedSymlink
								rf := &File{
									Path:      target,
									Size:      fi.Size(),
									FileMode:  uint32(fi.Mode()),
									ModTime:   fi.ModTime(),
									ScanFlags: scanFlags,
									//IsSymLink: false,
									//SymLinkTarget: resolveMe,
									//FollowedSymlink: true,
								}

								if !yield(rf, true) {
									return false
								}
							}
							continue
						} else {
							// do not follow symlinks

							target, err := os.Readlink(resolveMe)
							if err != nil {
								// allow dangling links to not stop the walk.
								continue
							}

							//vv("returning IsSymLink true regular File")
							fi, err := entry.Info()
							panicOn(err)
							scanFlags := ScanFlagIsSymLink
							rf := &File{
								Path:      resolveMe,
								Size:      fi.Size(),
								FileMode:  uint32(fi.Mode()),
								ModTime:   fi.ModTime(),
								ScanFlags: scanFlags,
								//IsSymLink:       true,
								SymLinkTarget: target,
								//FollowedSymlink: false,
							}
							if !yield(rf, true) {
								return false
							}
							continue
						}
					} // if symlink

					if entry.IsDir() {
						// Recurse immediately when we find a directory
						if !visit(filepath.Join(path, entry.Name()), depth+1) {
							return false
						}
					} else {
						fullpath := filepath.Join(path, entry.Name())
						if seen != nil {
							if seen[fullpath] {
								continue
							} else {
								seen[fullpath] = true
							}
						}
						fi, err := entry.Info()
						panicOn(err)
						rf := &File{
							Path:     fullpath,
							Size:     fi.Size(),
							FileMode: uint32(fi.Mode()),
							ModTime:  fi.ModTime(),
						}
						if !yield(rf, true) {
							return false
						}
					}
				}

				if err != nil || len(entries) < di.BatchSize {
					break
				}
			}

			return true
		}

		// Start the recursion
		visit(root, 0)
	}
}

// AllDirsOnlyDirs returns all subdirectories of root.
// It does return any files.
func (di *DirIter) AllDirsOnlyDirs(root string) iter.Seq2[string, bool] {
	return func(yield func(string, bool) bool) {

		// Helper function for recursive traversal
		var visit func(path string) bool
		visit = func(path string) bool {
			fi, err := os.Stat(path)
			if err != nil {
				return yield(path, false)
			}
			if !fi.IsDir() {
				return false
			}
			dir, err := os.Open(path)
			if err != nil {
				return yield(path, false)
			}
			defer dir.Close()

			for {
				entries, err := dir.ReadDir(di.BatchSize)
				// Process entries in directory order
				for _, entry := range entries {
					if entry.IsDir() {
						// Recurse immediately when we find a directory
						if !visit(filepath.Join(path, entry.Name())) {
							return false
						}
					}
				}

				if err != nil || len(entries) < di.BatchSize {
					break
				}
			}
			// we are a directory, yield ourselves.
			return yield(path, true)

		} // end of visit

		// Start the recursion
		visit(root)
	}
}

// Avoid 3 walks and just do it all in one walk.
// Use the File.ScanFlags to tell what kind
// of data you have
/*
// switch {
// case f.ScanFlags&ScanFlagIsLeafDir != 0:
//    // leaf dirs are sent first
// 	  sol = append(sol, f)
// case f.ScanFlags&ScanFlagIsMidDir != 0:
//    // middle dirs are sent last
// 	  sod = append(sod, f)
// default:
//    // regular files are sent in the middle
// 	  sof = append(sof, f)
// }
*/
func (di *DirIter) OneWalkForAll(root string) iter.Seq2[*File, bool] {
	return func(yield func(*File, bool) bool) {

		var seen map[string]bool
		if di.FollowSymlinks {
			// symlinks can cause duplicated paths, so
			// we de-duplicate the paths when following
			// symlinks.
			seen = make(map[string]bool)
		}

		// Helper function for recursive traversal
		var visit func(path string, depth int) bool

		visit = func(path string, depth int) bool {
			//vv("top of visit, path = '%v'; depth = %v", path, depth)
			if di.MaxDepth > 0 && depth >= di.MaxDepth {
				return true // true lets cousins also get to max depth.
			}
			dir, err := os.Open(path)
			if err != nil {
				return yield(nil, false)
			}
			defer dir.Close()

			hasSubdirs := false
			for {
				entries, err := dir.ReadDir(di.BatchSize)
				// Process entries in directory order
				for _, entry := range entries {
					if entry.IsDir() {
						hasSubdirs = true
					}
					//vv("entry = '%#v'; entry.Type()&fs.ModeSymlink = %v", entry, entry.Type()&fs.ModeSymlink)

					resolveMe := filepath.Join(path, entry.Name())

					if entry.Type()&fs.ModeSymlink != 0 {
						//vv("have symlink '%v'", resolveMe)

						//vv("resolveMe:'%v' -> target:'%v'", resolveMe, target)
						if di.FollowSymlinks {
							target, err := filepath.EvalSymlinks(resolveMe)
							if err != nil {
								// allow dangling links to not stop the walk.
								continue
							}
							fi, err := os.Stat(target)
							if err != nil {
								return false
							}
							if fi.IsDir() {
								// Recurse immediately when we find a directory
								if !visit(target, depth+1) {
									return false
								}
							} else {
								if seen != nil {
									if seen[target] {
										continue
									} else {
										seen[target] = true
									}
								}

								scanFlags := ScanFlagFollowedSymlink
								rf := &File{
									Path:      target,
									Size:      fi.Size(),
									FileMode:  uint32(fi.Mode()),
									ModTime:   fi.ModTime(),
									ScanFlags: scanFlags,
									//IsSymLink: false,
									//SymLinkTarget: resolveMe,
									//FollowedSymlink: true,
								}

								if !yield(rf, true) {
									return false
								}
							}
							continue
						} else {
							// do not follow symlinks

							target, err := os.Readlink(resolveMe)
							if err != nil {
								// allow dangling links to not stop the walk.
								continue
							}

							//vv("returning IsSymLink true regular File")
							fi, err := entry.Info()
							panicOn(err)
							scanFlags := ScanFlagIsSymLink
							rf := &File{
								Path:      resolveMe,
								Size:      fi.Size(),
								FileMode:  uint32(fi.Mode()),
								ModTime:   fi.ModTime(),
								ScanFlags: scanFlags,
								//IsSymLink:       true,
								SymLinkTarget: target,
								//FollowedSymlink: false,
							}
							if !yield(rf, true) {
								return false
							}
							continue
						}
					} // if symlink

					if entry.IsDir() {
						// Recurse immediately when we find a directory
						if !visit(filepath.Join(path, entry.Name()), depth+1) {
							return false
						}
					} else {
						fullpath := filepath.Join(path, entry.Name())
						if seen != nil {
							if seen[fullpath] {
								continue
							} else {
								seen[fullpath] = true
							}
						}
						fi, err := entry.Info()
						panicOn(err)
						rf := &File{
							Path:     fullpath,
							Size:     fi.Size(),
							FileMode: uint32(fi.Mode()),
							ModTime:  fi.ModTime(),
						}
						if !yield(rf, true) {
							return false
						}
					}
				}

				if err != nil || len(entries) < di.BatchSize {
					break
				}
			} // end for

			// we are a directory, yield ourselves.
			fi, err := dir.Stat()
			panicOn(err)
			var scanFlags uint32
			if hasSubdirs {
				scanFlags = ScanFlagIsDir | ScanFlagIsMidDir
			} else {
				scanFlags = ScanFlagIsDir | ScanFlagIsLeafDir
			}
			rf := &File{
				Path:      path,
				Size:      fi.Size(),
				FileMode:  uint32(fi.Mode()),
				ModTime:   fi.ModTime(),
				ScanFlags: scanFlags,
			}
			return yield(rf, true)
		}

		// Start the recursion
		visit(root, 0)
	}
}

// Note: do not loose resCh results! If halt is called,
// be sure to drain the resCh before returning, if you
// don't want to lose directories.
// same as above, but use pwalk for multiple goroutines
func (di *DirIter) ParallelOneWalkForAll(halt *idem.Halter, root string) (resCh chan *File) {

	resCh = make(chan *File, 4096)
	//var seen map[string]bool
	//if di.FollowSymlinks { // not fully implimented yet?
	// symlinks can cause duplicated paths, so
	// we de-duplicate the paths when following
	// symlinks.
	//	seen = make(map[string]bool)
	//}

	go func() {
		// we need to know when pwalk.Walk() returns: that is
		// when we are done with the walk. Then these defer can run.
		defer func() {
			//vv("pwalk.Walk must have finished. defers are running")
			halt.ReqStop.Close()
			halt.Done.Close()
		}()

		pwalk.Walk(root, func(path string, fi os.FileInfo, hasSubdirs bool, err error) error {

			if fi.Mode()&fs.ModeSymlink != 0 {
				//vv("have symlink '%v'", resolveMe)
				// atm do not follow symlinks, but return them.

				target, err := os.Readlink(path)
				if err != nil {
					// allow dangling links to not stop the walk.?
					//return nil
					panicOn(err) // do we need to pass on target even if dangling?
				}

				//vv("returning IsSymLink true regular File")
				scanFlags := ScanFlagIsSymLink
				rf := &File{
					Path:      path,
					Size:      fi.Size(),
					FileMode:  uint32(fi.Mode()),
					ModTime:   fi.ModTime(),
					ScanFlags: scanFlags,
					//IsSymLink:       true,
					SymLinkTarget: target,
					//FollowedSymlink: false,
				}
				select {
				case resCh <- rf:
				case <-halt.ReqStop.Chan:
					return ErrHaltRequested
				}
				// end if symlink
			} else {
				if fi.IsDir() {
					// we are a directory, yield ourselves.
					var scanFlags uint32
					if hasSubdirs {
						scanFlags = ScanFlagIsDir | ScanFlagIsMidDir
					} else {
						scanFlags = ScanFlagIsDir | ScanFlagIsLeafDir
					}
					rf := &File{
						Path:      path,
						Size:      fi.Size(),
						FileMode:  uint32(fi.Mode()),
						ModTime:   fi.ModTime(),
						ScanFlags: scanFlags,
					}
					select {
					case resCh <- rf:
					case <-halt.ReqStop.Chan:
						return ErrHaltRequested
					}

				} else {
					// regular file
					rf := &File{
						Path:     path,
						Size:     fi.Size(),
						FileMode: uint32(fi.Mode()),
						ModTime:  fi.ModTime(),
					}
					select {
					case resCh <- rf:
					case <-halt.ReqStop.Chan:
						return ErrHaltRequested
					}
				}
			}
			return nil
		})
		//vv("back from pwalk.Walk()")
	}()

	return
}

func (di *DirIter) ParallelWalk(root string) (files []*File) {

	// empty/non-existant root okay, just return empty slice.
	fi, err := os.Stat(root)
	if err != nil {
		return
	}
	_ = fi

	pre := len(root) + 1

	resCh := make(chan *File)
	//var seen map[string]bool
	//if di.FollowSymlinks { // not fully implimented yet?
	// symlinks can cause duplicated paths, so
	// we de-duplicate the paths when following
	// symlinks.
	//	seen = make(map[string]bool)
	//}

	halt := idem.NewHalter()

	go func() {
		// we need to know when pwalk.Walk() returns: that is
		// when we are done with the walk. Then these defer can run.
		defer func() {
			//vv("pwalk.Walk must have finished. defers are running")
			halt.ReqStop.Close()
			halt.Done.Close()
		}()

		pwalk.Walk(root, func(path string, fi os.FileInfo, hasSubdirs bool, err error) error {

			if fi.Mode()&fs.ModeSymlink != 0 {
				//vv("have symlink '%v'", resolveMe)
				// atm do not follow symlinks, but return them.

				target, err := os.Readlink(path)
				if err != nil {
					// allow dangling links to not stop the walk.?
					//return nil
					panicOn(err) // do we need to pass on target even if dangling?
				}

				//vv("returning IsSymLink true regular File")
				scanFlags := ScanFlagIsSymLink
				if len(path) >= pre {
					path = path[pre:]
				} else {
					path = ""
				}
				rf := &File{
					Path:      path,
					Size:      fi.Size(),
					FileMode:  uint32(fi.Mode()),
					ModTime:   fi.ModTime(),
					ScanFlags: scanFlags,
					//IsSymLink:       true,
					SymLinkTarget: target,
					//FollowedSymlink: false,
				}
				select {
				case resCh <- rf:
				case <-halt.ReqStop.Chan:
					return ErrHaltRequested
				}
				// end if symlink
			} else {
				if fi.IsDir() {
					// we are a directory, yield ourselves.
					var scanFlags uint32
					if hasSubdirs {
						scanFlags = ScanFlagIsDir | ScanFlagIsMidDir
					} else {
						scanFlags = ScanFlagIsDir | ScanFlagIsLeafDir
					}
					if len(path) >= pre {
						path = path[pre:]
					} else {
						path = ""
					}
					rf := &File{
						Path:      path,
						Size:      fi.Size(),
						FileMode:  uint32(fi.Mode()),
						ModTime:   fi.ModTime(),
						ScanFlags: scanFlags,
					}
					select {
					case resCh <- rf:
					case <-halt.ReqStop.Chan:
						return ErrHaltRequested
					}

				} else {
					// regular file
					if len(path) >= pre {
						path = path[pre:]
					} else {
						path = ""
					}
					rf := &File{
						Path:     path,
						Size:     fi.Size(),
						FileMode: uint32(fi.Mode()),
						ModTime:  fi.ModTime(),
					}
					select {
					case resCh <- rf:
					case <-halt.ReqStop.Chan:
						return ErrHaltRequested
					}
				}
			}
			return nil
		})
		//vv("back from pwalk.Walk()")
	}()

myloop:
	for {
		select {
		case f := <-resCh:
			files = append(files, f)
		case <-halt.Done.Chan:
			break myloop
		}
	}

	return
}
