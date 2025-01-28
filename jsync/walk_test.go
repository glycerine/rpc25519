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
	root := ".."

	limit := 100
	i := 0

	di := NewDirIter()

	next, stop := iter.Pull2(di.DirsDepthFirstLeafOnly(root))
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
