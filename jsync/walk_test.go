package jsync

import (
	"fmt"
	//"io"
	//"bufio"
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

func TestWalkDirsDFSIter(t *testing.T) {

	// walk_test.go:49 2025-01-27 20:28:23.161 -0600 CST total leaf dir = 4597
	// So the linux source tree has 4597 leaf directories.
	//root := "/home/jaten/go/src/github.com/PlakarKorp/Korpus/github.com/torvalds/linux"

	// We don't have linux all checked out everywhere,
	// and it takes 3 seconds.
	root := ".."

	limit := 100000

	di := NewDirIter()

	k := 0
	next, stop := iter.Pull2(di.DirsDepthFirstLeafOnly(root))
	defer stop()

	for {
		dir, ok, valid := next()
		_ = dir
		if !valid {
			vv("not valid, breaking, ok = %v", ok)
			break
		}
		if !ok {
			break
		}

		k++
		//fmt.Println(dir)

		if k > limit {
			vv("break on limit")
			break
		}
	}
	vv("total leaf dir = %v", k)
}

func TestWalkDirsFilesOnly(t *testing.T) {

	// total files only = 87822
	// So the linux source tree has 4597 leaf directories.
	root := "/home/jaten/go/src/github.com/PlakarKorp/Korpus/github.com/torvalds/linux"

	// We don't have linux all checked out everywhere,
	// and it takes 3 seconds.
	//root := ".."
	//root := "."
	/*
		ans, err := os.Create("found_files.txt")
		panicOn(err)
		buf := bufio.NewWriter(ans)
	*/
	limit := 100000

	di := NewDirIter()

	k := 0
	next, stop := iter.Pull2(di.FilesOnly(root))
	defer stop()

	for {
		dir, ok, valid := next()
		if !valid {
			vv("not valid, breaking, ok = %v", ok)
			break
		}
		_ = dir
		if !ok {
			break
		}

		k++
		//fmt.Fprintln(buf, dir)
		//fmt.Println(dir)

		if k > limit {
			vv("break on limit")
			break
		}
	}
	//buf.Flush()
	//ans.Close()
	vv("total files only = %v", k)
	if k != 87822 {
		t.Fatalf("expected %v, got %v", 87822, k)
	}
}

func TestWalkAllDirsOnlyDirs(t *testing.T) {

	//root := "/home/jaten/go/src/github.com/PlakarKorp/Korpus/github.com/torvalds/linux"

	// We don't have linux all checked out everywhere,
	// and it takes 3 seconds.
	root := ".."
	//root := "."
	/*
		ans, err := os.Create("found_files.txt")
		panicOn(err)
		buf := bufio.NewWriter(ans)
	*/
	limit := 100000

	di := NewDirIter()

	k := 0
	next, stop := iter.Pull2(di.AllDirsOnlyDirs(root))
	defer stop()

	for {
		dir, ok, valid := next()
		if !valid {
			vv("not valid, breaking, ok = %v", ok)
			break
		}
		_ = dir
		if !ok {
			break
		}

		k++
		//fmt.Fprintln(buf, dir)
		//fmt.Println(dir)

		if k > limit {
			vv("break on limit")
			break
		}
	}
	//buf.Flush()
	//ans.Close()
	vv("total dirs, all dirs, only dirs = %v", k)
}

func TestWalkDirs_FollowSymlinks(t *testing.T) {

	root := "test_root"

	os.RemoveAll(root) // cleanup any prior test output.
	defer os.RemoveAll(root)

	panicOn(os.MkdirAll(filepath.Join(root, "a/b/c/d"), 0700))
	panicOn(os.MkdirAll(filepath.Join(root, "z"), 0700))

	// two symlinks in a row
	panicOn(os.Symlink("../a", filepath.Join(root, "z/symlink2")))
	panicOn(os.Symlink("symlink2", filepath.Join(root, "z/symlink")))

	fd, err := os.Create(filepath.Join(root, "a/b/c/d/file0.txt"))
	panicOn(err)
	fd.Close()

	di := NewDirIter()
	di.FollowSymlinks = true
	next, stop := iter.Pull2(di.FilesOnly(filepath.Join(root, "z")))
	defer stop()

	seen := 0
	paths := make(map[string]int)
	for {
		regfile, ok, valid := next()
		if !valid {
			//vv("not valid, breaking, ok = %v", ok)
			break
		}
		if !ok {
			break
		}
		path := regfile.Path
		//vv("path = '%v'", path)
		if fileExists(path) {
			seen++
			paths[path]++
		}
	}
	if want, got := 1, seen; want != got {
		// there are 2 paths to file0.txt when resolving symlinks:
		// test_root/z/symlink/symlink2/a/b/c/d/file0.txt
		// test_root/z/symlink2/a/b/c/d/file0.txt
		//
		// both of which resolve ultimately to:
		//
		// test_root/a/b/c/d/file0.txt
		//
		// So this is a test that the internal dedup is working.
		t.Fatalf("want %v, got %v seen files", want, got)
	}
	if len(paths) != 1 {
		t.Fatalf("expected only 1 unique path in paths because of internal dedup now")
	}
	if 1 != paths["test_root/a/b/c/d/file0.txt"] {
		t.Fatalf("expected 'test_root/a/b/c/d/file0.txt' in paths")
	}
}

func TestWalkDirs_MaxDepth(t *testing.T) {

	// same test but if we cut off at max depth 4, we should get no files.

	root := "test_root"

	os.RemoveAll(root) // cleanup any prior test output.
	defer os.RemoveAll(root)

	panicOn(os.MkdirAll(filepath.Join(root, "a/b/c/d"), 0700))
	panicOn(os.MkdirAll(filepath.Join(root, "z"), 0700))

	// two symlinks in a row
	panicOn(os.Symlink("../a", filepath.Join(root, "z/symlink2")))
	panicOn(os.Symlink("symlink2", filepath.Join(root, "z/symlink")))

	fd, err := os.Create(filepath.Join(root, "a/b/c/d/file0.txt"))
	panicOn(err)
	fd.Close()

	di := NewDirIter()
	di.FollowSymlinks = true
	di.MaxDepth = 4
	next, stop := iter.Pull2(di.FilesOnly(filepath.Join(root, "z")))
	defer stop()

	seen := 0
	paths := make(map[string]bool)
	for {
		regfile, ok, valid := next()
		if !valid {
			//vv("not valid, breaking, ok = %v", ok)
			break
		}
		if !ok {
			break
		}
		path := regfile.Path
		//vv("path = '%v'", path)
		if fileExists(path) {
			seen++
			paths[path] = true
		}
	}
	if want, got := 0, seen; want != got {
		// there are 2 paths to file0.txt when resolving symlinks:
		// test_root/z/symlink/symlink2/a/b/c/d/file0.txt
		// test_root/z/symlink2/a/b/c/d/file0.txt
		//
		// both of which resolve ultimately to:
		//
		// test_root/a/b/c/d/file0.txt
		t.Fatalf("want %v, got %v seen files", want, got)
	}
	if len(paths) != 0 {
		t.Fatalf("expected only 0 unique path in paths")
	}
}

func TestWalkDirsFilesOnly2(t *testing.T) {

	root := "/Users/jaten/trash"
	limit := 100000

	di := NewDirIter()

	k := 0
	next, stop := iter.Pull2(di.FilesOnly(root))
	defer stop()

	for {
		dir, ok, valid := next()
		if !valid {
			vv("not valid, breaking, ok = %v", ok)
			break
		}
		_ = dir
		if !ok {
			break
		}

		k++
		//fmt.Fprintln(buf, dir)
		fmt.Println(dir)

		if k > limit {
			vv("break on limit")
			break
		}
	}
	//buf.Flush()
	//ans.Close()
	vv("total count, files only = %v", k)
}
