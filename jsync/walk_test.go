package jsync

import (
	"fmt"
	//"io"
	"iter"
	//"os"
	//"path/filepath"
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
	root := "/home/jaten/go/src/github.com/PlakarKorp/Korpus/github.com/torvalds/linux"

	limit := 100000

	di := NewDirIter()

	k := 0
	next, stop := iter.Pull2(di.DirsDepthFirstLeafOnly(root))
	defer stop()

	for {
		dir, ok, valid := next()
		if !valid {
			vv("not valid, breaking, ok = %v", ok)
			break
		}
		if !ok {
			break
		}

		k++
		fmt.Println(dir)

		if k > limit {
			vv("break on limit")
			break
		}
	}
	vv("total leaf dir = %v", k)
}
