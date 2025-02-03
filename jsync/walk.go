package jsync

import (
	"io/fs"
	"iter"
	"os"
	"path/filepath"
)

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

type RegularFile struct {
	Path          string `zid:"0"`
	IsSymLink     bool   `zid:"1"`
	SymLinkTarget string `zid:"2"`
	Follow        bool   `zid:"3"`
}

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
func (di *DirIter) FilesOnly(root string) iter.Seq2[*RegularFile, bool] {
	return func(yield func(*RegularFile, bool) bool) {

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

						target, err := filepath.EvalSymlinks(resolveMe)
						if err != nil {
							return false
						}

						//vv("resolveMe:'%v' -> target:'%v'", resolveMe, target)
						if di.FollowSymlinks {
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

								rf := &RegularFile{
									Path:          resolveMe,
									IsSymLink:     true,
									SymLinkTarget: target,
									Follow:        true,
								}

								if !yield(rf, true) {
									return false
								}
							}
							continue
						} else {
							// do not follow symlinks
							rf := &RegularFile{
								Path:          resolveMe,
								IsSymLink:     true,
								SymLinkTarget: target,
								Follow:        false,
							}
							if !yield(rf, true) {
								return false
							}
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
						rf := &RegularFile{
							Path: fullpath,
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
