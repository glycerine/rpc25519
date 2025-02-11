package jsync

import (
	"bufio"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/glycerine/idem"
)

var _ = bufio.NewWriter

// test the walk iterator

func PrintAll[V any](seq iter.Seq[V]) {
	for v := range seq {
		fmt.Println(v)
	}
}

func TestWalkDirsDFSIter(t *testing.T) {

	// So the linux source tree has 4603 leaf directories.
	// with .git/branchs, and
	// checked out at 0de63bb7d91975e73338300a57c54b93d3cc151c
	// Date:   Mon Feb 3 13:39:55 2025 -0800

	// find . -type d -exec sh -c '[ $(find "{}" -maxdepth 1 -type d | wc -l) -eq 1 ]' \; -print | wc -l == 4603

	expect := 4603
	home := os.Getenv("HOME")

	root := filepath.Join(home, "/go/src/github.com/torvalds/linux")

	if !dirExists(root) {
		vv("skipping walk test on non existant root directory '%v'", root)
	}

	// We don't have linux all checked out everywhere,
	// and it takes 3 seconds.
	//root := ".."

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
	if k != expect {
		t.Fatalf("expected %v leaf dir but we see %v", expect, k)
	}
}

func TestWalkDirsFilesOnly(t *testing.T) {

	//expect := 87923 because we return symlinks (no following, by default).
	// find linux -type f |wc -l  == 87860 regular files
	// find linux -type l |wc -l  == 63    symlinks
	expect := 87866 + 63 // find regular files + symlinks == 87923

	home := os.Getenv("HOME")

	root := filepath.Join(home, "/go/src/github.com/torvalds/linux")

	if !dirExists(root) {
		vv("skipping walk test on non existant root directory '%v'", root)
	}

	//root := "."

	ans, err := os.Create("found_files.txt")
	panicOn(err)
	buf := bufio.NewWriter(ans)

	limit := 100000

	di := NewDirIter()

	pre := len(root) - len("linux")
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
		fmt.Fprintln(buf, dir.Path[pre:])
		//fmt.Println(dir)

		if k > limit {
			vv("break on limit")
			break
		}
	}
	buf.Flush()
	ans.Close()
	vv("total files only = %v", k)
	if k != expect {
		// changes with the kernel checked out all the time.
		//t.Fatalf("expected %v total files only, got %v", expect, k)
	}
}

func TestWalkAllDirsOnlyDirs(t *testing.T) {

	// find linux -type d|wc -l ==  5861
	expect := 5861

	home := os.Getenv("HOME")

	root := filepath.Join(home, "/go/src/github.com/torvalds/linux")

	if !dirExists(root) {
		vv("skipping walk test on non existant root directory '%v'", root)
	}

	//root := "."

	ans, err := os.Create("found_all_dirs_only_dirs.txt")
	panicOn(err)
	buf := bufio.NewWriter(ans)

	limit := 100000

	di := NewDirIter()

	pre := len(root) - len("linux")
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
		fmt.Fprintln(buf, dir[pre:])
		//fmt.Println(dir)

		if k > limit {
			vv("break on limit")
			break
		}
	}
	buf.Flush()
	ans.Close()
	vv("total dirs, all dirs, only dirs = %v", k)
	if k != expect {
		t.Fatalf("expected %v all dirs/only dirs, got %v", expect, k)
	}
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

func Test_ParallelOneWalkForAll(t *testing.T) {

	hardpath := "linux11/tools/testing/selftests/devices/probe/boards/Dell Inc.,XPS 13 9300.yaml"

	vv("after Clean, hardpath = '%v'", filepath.Clean(hardpath))

	home := os.Getenv("HOME")
	root := filepath.Join(home, "/go/src/github.com/torvalds/linux")

	di := NewDirIter()

	halt := idem.NewHalter()
	t0 := time.Now()
	resCh := di.ParallelOneWalkForAll(halt, root)
	k := 0
	leafDir := 0
	midDir := 0
forloop:
	for {
		select {
		case f := <-resCh:
			k++
			if f.IsLeafDir() {
				leafDir++
			}
			if f.IsMidDir() {
				midDir++
			}
		case <-halt.ReqStop.Chan:
			// must finish draining the resCh! else can drop results
			for {
				select {
				case f := <-resCh:
					k++
					if f.IsLeafDir() {
						leafDir++
					}
					if f.IsMidDir() {
						midDir++
					}
				default:
					break forloop
				}
			}
			break forloop
		}
	}
	elap := time.Since(t0)
	vv("total count, all in parallel = %v; \nelap = %v", k, elap)

	expectLeafDir := 4603
	expectMidDir := 1258

	// leafDir = 4603;  midDir = 1258
	vv("leafDir = %v;  midDir = %v\n", leafDir, midDir)

	if leafDir != expectLeafDir {
		//    walk_test.go:357: leafDir = 4353; expectLeafDir = 4603 ???
		t.Fatalf("leafDir = %v; expectLeafDir = %v", leafDir, expectLeafDir)
	}
	if midDir != expectMidDir {
		t.Fatalf("midDir = %v; expectMidDir = %v", midDir, expectMidDir)
	}
}
