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

// FilesOnly returns only files, skipping directories. This does
// return symlinks as files too, if di.FollowSymlinks is false.
// If di.FollowSymlinks is true, and a symlink links to a
// directory, the recursion will follow the symlink down
// that directory tree. Note that this can result in
// returning the same file multiple times if there
// are multiple paths throught symlinks to the same file.
// It is the user's responsibility to deduplicate the
// returned paths if need be when using FollowSymlinks true.
// Resolving a symlink through multiple other symlinks
// will only count as one depth level for MaxDepth stopping.
func (di *DirIter) FilesOnly(root string) iter.Seq2[string, bool] {
	return func(yield func(string, bool) bool) {

		// Helper function for recursive traversal
		var visit func(path string, depth int) bool

		visit = func(path string, depth int) bool {
			//vv("top of visit, path = '%v'; depth = %v", path, depth)
			if di.MaxDepth > 0 && depth >= di.MaxDepth {
				return true // true lets cousins also get to max depth.
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
					//vv("entry = '%#v'; entry.Type()&fs.ModeSymlink = %v", entry, entry.Type()&fs.ModeSymlink)

					if entry.Type()&fs.ModeSymlink != 0 && di.FollowSymlinks {
						resolveMe := filepath.Join(path, entry.Name())
						//vv("have symlink '%v'", resolveMe)
						target, err := filepath.EvalSymlinks(resolveMe)
						if err != nil {
							return false
						}

						//vv("resolveMe:'%v' -> target:'%v'", resolveMe, target)
						fi, err := os.Stat(target)
						if err != nil {
							return false
						}
						//entry = fs.FileInfoToDirEntry(fi)
						//vv("target entry = '%v'; entry.IsDir() = '%v'", fi.Name(), fi.IsDir())
						// we cannot use the below, because path may
						// no longer be the right prefix if the symlink
						// went .. or elsewhere.

						if fi.IsDir() {
							// Recurse immediately when we find a directory
							if !visit(target, depth+1) {
								return false
							}
						} else {
							if !yield(target, true) {
								return false
							}
						}
						continue
					}

					if entry.IsDir() {
						// Recurse immediately when we find a directory
						if !visit(filepath.Join(path, entry.Name()), depth+1) {
							return false
						}
					} else {
						if !yield(filepath.Join(path, entry.Name()), true) {
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
