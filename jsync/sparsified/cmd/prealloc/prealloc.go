package main

import (
	"fmt"
	. "github.com/glycerine/rpc25519/jsync/sparsified"
	"os"
	"strconv"
)

func isNumber(last byte) bool {
	return last >= 48 && last <= 57
}

func isSuffix(b byte) bool {
	switch b {
	case 'b', 'B':
		return true
	case 'g', 'G':
		return true
	case 'm', 'M':
		return true
	case 'k', 'K':
		return true
	}
	return false
}

func legit(s string) (sz int64, err error) {
	n := len(s)
	if n == 0 {
		return
	}
	last := n - 1
	// "mb" -> "m", "gb" -> "g"
	if s[last] == 'b' || s[last] == 'B' {
		s = s[:last]
		n = len(s)
	}
	allnum := true
	non := -1
	for i, b := range s {
		if !isNumber(byte(b)) {
			if non < 0 {
				non = i
			}
			allnum = false
		}
	}
	if allnum {
		n, err := strconv.Atoi(s)
		panicOn(err)
		if n <= 0 {
			err = fmt.Errorf("unparsable size request: '%v' (must be positive and/or optionally end in one of {k,m,g,K,M,G}", s)
			return 0, err
		}
		sz = int64(n)
		return sz, nil
	}
	lastb := s[last]
	if isNumber(lastb) {
		err = fmt.Errorf("unparsable size request: '%v' (must be positive and/or optionally end in one of {k,m,g,K,M,G}", s)
		return
	}
	// INVAR: has some non-numeric suffix

	// parse the numeric part into sz
	numer := s[:non]
	n, err = strconv.Atoi(numer)
	panicOn(err)
	if n <= 0 {
		panic(fmt.Sprintf("unparsable size request: '%v' (must be positive and/or optionally end in one of {k,m,g,K,M,G}", s))
	}
	sz = int64(n)

	switch lastb {
	case 'g', 'G':
		sz = sz << 30
	case 'm', 'M':
		sz = sz << 20
	case 'k', 'K':
		sz = sz << 10
	default:
		panic(fmt.Sprintf("unknown size suffix: '%v' in '%v'", string(lastb), s))
	}
	return sz, nil
}

// minimum pre-alloc is 64MB
func main() {
	if len(os.Args) < 2 {
		fmt.Printf("must supply path as argument: prealloc path {size in bytes with k,m,g, or empty suffix}\n")
		os.Exit(1)
	}
	path := os.Args[1]

	var sz int64
	var err error
	if len(os.Args) >= 3 {
		req := os.Args[2]
		sz, err = legit(req)
		panicOn(err)
	}

	var mb64 int64 = 1 << 26
	if sz <= mb64 {
		sz = mb64
	}

	// round up to full 1MB boundary
	mb := int64(1 << 20)
	rem := sz % mb
	if rem != 0 {
		vv("rounding up from %v -> %v", sz, sz+(mb-rem))
		sz += (mb - rem)
	}

	formatted := formatUnder(sz/(1<<20)) + " MB"

	os.Remove(path)
	fd, err := os.Create(path)
	panicOn(err)
	fd.Truncate(0)
	_, err = Fallocate(fd, FALLOC_FL_KEEP_SIZE, 0, sz)
	panicOn(err)
	panicOn(fd.Close())

	fmt.Printf("created demo %v pre-allocated path '%v'.\n", formatted, path)
	return
}
