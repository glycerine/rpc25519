package jsync

import (
	"fmt"
	//"io"
	"iter"
	"os"
	"path/filepath"
	"testing"
)

// test the walk iterator

func PrintAll[V any](seq iter.Seq[V]) {
	for v := range seq {
		fmt.Println(v)
	}
}

func TestWalkIter(t *testing.T) {
	root := "/Users/jaten/cn/dfs"

	limit := 100
	i := 0

	next, stop := iter.Pull2(Dirs(root))
	defer stop()

	for {
		dir, ok, valid := next()
		if !valid {
			vv("not valid, breaking, ok = %v", ok)
			break
		}
		if ok {
			fmt.Println(dir)
		}

		i++
		if i > limit {
			vv("break on 100 limit")
			break
		}

	}

}

type DirIter struct {
	stack     []string
	batchSize int
}

func NewDirIter(root string) *DirIter {
	return &DirIter{
		stack:     []string{root},
		batchSize: 100,
	}
}

func Dirs(root string) iter.Seq2[string, bool] {
	return func(yield func(s string, b bool) bool) {
		vv("top of Dirs func.")

		stack := []string{root}
		batchSize := 100

		for len(stack) > 0 {
			current := stack[len(stack)-1]
			stack = stack[:len(stack)-1]

			dir, err := os.Open(current)
			if err != nil {
				vv("err on os.Open('%v'): '%v'", current, err)
				if !yield(current, false) {
					return
				}
				continue
			}

			hasSubdirs := false
			// scan all of this directory's children,
			// in batches, looking for sub-directories.
			for {
				entries, err := dir.ReadDir(batchSize)
				// Process entries even if err is io.EOF
				for _, entry := range entries {
					if entry.IsDir() {
						hasSubdirs = true
						stack = append(stack, filepath.Join(current, entry.Name()))
					}
				}

				// Break if EOF or real error, or if out of entries.
				if err != nil || len(entries) < batchSize {
					break
				}
			}
			dir.Close()

			if !hasSubdirs {
				if !yield(current, true) {
					return
				}
			}
		} // end for stack > 0
	} // end func we return
}

/*

func Dirs(root string) iter.Seq2[string, bool] {
	return func(yield func(string, bool) bool) {
		stack := []string{root}
		batchSize := 100

		for len(stack) > 0 {
			// Pop current directory from stack
			current := stack[len(stack)-1]
			stack = stack[:len(stack)-1]

			dir, err := os.Open(current)
			if err != nil {
				vv("err on Open current '%v' => '%v'", current, err)
				// If we can't open it, it's effectively a leaf
				if !yield(current, false) {
					return
				}
				continue
			}

			hasSubdirs := false
			// Read directory entries in batches
			for {
				entries, err := dir.ReadDir(batchSize)
				if err != nil {
					break
				}

				// Push subdirectories to stack
				for _, entry := range entries {
					if entry.IsDir() {
						hasSubdirs = true
						stack = append(stack, filepath.Join(current, entry.Name()))
					}
				}

				if len(entries) < batchSize {
					break
				}
			}
			dir.Close()

			// Only yield if this is a leaf directory (no subdirectories)
			if !hasSubdirs {
				if !yield(current, true) {
					return
				}
			}
		}
	}
}

/*
func Dirs(root string) iter.Seq2[string, bool] {
	return func(yield func(string, bool) bool) {
		stack := []string{root}
		batchSize := 100

		for len(stack) > 0 {
			// Pop current directory from stack
			current := stack[len(stack)-1]
			stack = stack[:len(stack)-1]

			// Open the directory before yielding, to check it's accessible
			dir, err := os.Open(current)
			if err != nil {
				// Yield current with false to indicate error
				yield(current, false)
				continue
			}

			// Yield current directory and continue traversal
			if !yield(current, true) {
				dir.Close()
				return
			}

			// Read directory entries in batches
			for {
				entries, err := dir.ReadDir(batchSize)
				if err != nil {
					break
				}

				// Push subdirectories to stack
				for _, entry := range entries {
					if entry.IsDir() {
						stack = append(stack, filepath.Join(current, entry.Name()))
					}
				}

				if len(entries) < batchSize {
					break
				}
			}
			dir.Close()
		}
	}
}


func (d *DirIter) Next() (string, bool) {
	if len(d.stack) == 0 {
		return "", false
	}

	// Pop current directory from stack
	current := d.stack[len(d.stack)-1]
	d.stack = d.stack[:len(d.stack)-1]

	// Open the directory
	dir, err := os.Open(current)
	if err != nil {
		return current, true
	}
	defer dir.Close()

	// Read directory entries in batches
	for {
		entries, err := dir.ReadDir(d.batchSize)
		if err != nil {
			break
		}

		// Push subdirectories to stack in natural order
		for _, entry := range entries {
			if entry.IsDir() {
				d.stack = append(d.stack, filepath.Join(current, entry.Name()))
			}
		}

		if len(entries) < d.batchSize {
			break
		}
	}

	return current, true
}

// Dirs returns an iterator over directories
func Dirs(root string) iter.Seq2[string, bool] {

	return func(yield func(string, bool) bool) {
		stack := []string{root}
		batchSize := 100

		for len(stack) > 0 {
			// Pop current directory from stack
			current := stack[len(stack)-1]
			stack = stack[:len(stack)-1]

			// Yield current directory
			if !yield(current, true) {
				return
			}

			// Open the directory
			dir, err := os.Open(current)
			if err != nil {
				continue
			}
			defer dir.Close()

			// Read directory entries in batches
			for {
				entries, err := dir.ReadDir(batchSize)
				if err != nil {
					break
				}

				// Push subdirectories to stack
				for _, entry := range entries {
					if entry.IsDir() {
						stack = append(stack, filepath.Join(current, entry.Name()))
					}
				}

				if len(entries) < batchSize {
					break
				}
			}
		}
	}
}
*/
