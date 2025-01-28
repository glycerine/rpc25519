package jsync

import (
	"iter"
	"os"
	"path/filepath"
)

type DirIter struct {
	// how many directory entries we read at once,
	// to keep memory use low.
	BatchSize int
}

func NewDirIter() *DirIter {
	return &DirIter{
		BatchSize: 100,
	}
}

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

// FilesOnly version, no directories. This does
// return symlinks too. It does not follow them.
func (di *DirIter) FilesOnly(root string) iter.Seq2[string, bool] {
	return func(yield func(string, bool) bool) {

		// Helper function for recursive traversal
		var visit func(path string) bool
		visit = func(path string) bool {
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
		visit(root)
	}
}

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
