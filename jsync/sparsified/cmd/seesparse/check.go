package main

import (
	"fmt"
	. "github.com/glycerine/rpc25519/jsync/sparsified"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("must supply path as argument\n")
		os.Exit(1)
	}
	path := os.Args[1]
	fmt.Printf("check path '%v' for sparse regions...\n", path)

	var fd *os.File
	var err error
	fd, err = os.Open(path)
	panicOn(err)

	sparseSum, spansRead, err := FindSparseRegions(fd)
	_ = sparseSum
	panicOn(err)
	vv("spansRead = '%v'", spansRead)
	return
}
