//go:build linux

package sparsified

import (
	"fmt"
	"os"
	//"syscall"
	//"golang.org/x/sys/unix"
	"testing"
)

func Test015_FieMapLinux(t *testing.T) {
	path := "blah.out"
	fd, err := os.Create(path)
	panicOn(err)
	defer fd.Close()
	defer os.Remove(path)

	fd.Truncate(4096 * 10)
	fd.Write(oneZeroBlock4k[:])

	fd.Seek(3*4096, 1)

	fd.Write(oneZeroBlock4k[:])

	r, err := FieMap(fd)
	panicOn(err)

	vv("FieMap result = '%v' for path = '%v'", r, path)
}

func Test016_FieMapLinux_compared_lseek(t *testing.T) {

	var seed [32]byte
	rng := newPRNG(seed)

	for i := range 100 {

		spec := genSpecSparse(rng)

		path := fmt.Sprintf("test005.outpath.%02d.sparsefile", i)
		////vv("on i = %v, path = '%v'; spec = '%v'", i, path, spec)

		var fd *os.File
		var err error
		os.Remove(path)

		fd, err = createSparseFileFromSpans(path, spec, nil)
		panicOn(err)

		spansRead, err := FindSparseRegions(fd)
		panicOn(err)
		////vv("spansRead = '%v'", spansRead)
		if spansRead.Equal(spec) {
			////vv("good: spansRead Equal spec, on i=%v, path='%v'", i, path)
		} else {
			//vv("bad:  spansRead NOT Equal spec")
			panic("fix this")
		}

		fieRead, err := FieMap(fd)
		panicOn(err)
		fd.Close()

		if fieRead.Equal(spec) {
			////vv("good: spansRead Equal spec, on i=%v, path='%v'", i, path)
		} else {
			vv("bad:  fieRead NOT Equal spec")
			vv("fieRead =\n '%v'\n for path = '%v'\n\n vs spec=\n%v\n", fieRead, path, spec)

			panic("fix this")
		}

		os.Remove(path)
	}
}

func Test017_FieMapLinux_preallocated(t *testing.T) {

	// fiemap should mark pre-allocated correctly.

	var mb64 int64 = 1 << 26
	spec := &Spans{Slc: []Span{
		Span{IsHole: false, IsUnwrittenPrealloc: true, Beg: 0, Endx: mb64},
	}}

	path := fmt.Sprintf("test017.outpath.pre_allocated")
	//vv("on path = '%v'; spec = '%v'", path, spec)

	var fd *os.File
	var err error

	os.Remove(path)
	defer os.Remove(path)

	//vv("did remove path='%v'; about to call createSparseFileFromSpans()", path)
	fd, err = createSparseFileFromSpans(path, spec, nil)
	panicOn(err)
	//vv("done with createSparseFileFromSpans()")
	defer fd.Close()

	isSparse, disksz, statsz, err := SparseFileSize(fd)
	panicOn(err) // no such device or address [recovered, repanicked]
	_, _, _ = isSparse, disksz, statsz
	//vv("SparseFileSize returned: isSparse=%v; disksz=%v; statsz=%v; err=%v", isSparse, disksz, statsz, err)

	spansRead, err := FindSparseRegions(fd)
	panicOn(err)
	//vv("spansRead = '%v'", spansRead)
	if spansRead.Equal(spec) {
		//vv("good: spansRead Equal spec")
	} else {
		//vv("bad:  spansRead NOT Equal spec")
		panic("fix this")
	}

	// and finally, checking fiemap.
	fieRead, err := FieMap(fd)
	panicOn(err)
	fd.Close()

	if fieRead.Equal(spec) {
		////vv("good: spansRead Equal spec, on i=%v, path='%v'", i, path)
	} else {
		vv("bad:  fieRead NOT Equal spec")
		vv("fieRead =\n '%v'\n for path = '%v'\n\n vs spec=\n%v\n", fieRead, path, spec)

		panic("fix this")
	}
}
