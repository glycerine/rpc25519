package jsync

import (
	//"bytes"
	//"compress/gzip"
	//"encoding/csv"
	//"fmt"
	"os"
	//"sort"
	//"strconv"
	//"strings"
	//"sync"
	"syscall"
	"time"
	//"unsafe"
)

type MappedFile struct {
	Frompath string
}

func (df *MappedFile) Slurp(path string) (err error) {

	t0 := time.Now()
	_ = t0
	// first argument is the path to the csv file to parse
	fd, err := os.Open(path)
	if err != nil {
		return err
	}
	defer fd.Close()
	df.Frompath = path
	fi, err := fd.Stat()
	panicOn(err)
	sz := fi.Size()
	_ = sz

	// the whole file, memory mapped into a single []byte slice.
	buf := MemoryMapFile(fd)
	defer func(buf []byte) {
		err := syscall.Munmap(buf)
		panicOn(err)
	}(buf)

	/*
		ngoro := 40
		minseg := int(1 << 20) // 1MB

		nseg := sz / minseg // how many 1MB segments we have to process
		if sz%minseg != 0 {
			nseg++
		}

		var each int // how many segments each goro does
		if nseg <= ngoro {
			ngoro = nseg
			each = 1
		} else {
			each = nseg / ngoro // integer divide does the floor automatically
		}

		numBackground := each * ngoro
		left := nseg - numBackground
		//vv("ngoro = %v; each = %v, numBackground = %v, left = %v, df.Nrow = %v\n", ngoro, each, numBackground, left, df.Nrow)

		var wg sync.WaitGroup
		waitCount := 0
		if each > 0 {
			waitCount = ngoro
		}
		if left > 0 {
			waitCount++
		}
		// do all the Adding before starting any goro,
		// or decrementing on the main goro with just a few left,
		// to avoid stopping prematurely. Per the docs.
		wg.Add(waitCount)

		if each > 0 {
			for i := 0; i < ngoro; i++ {
				beg := i * each
				endx := (i + 1) * each

				begM := beg * df.Ncol
				endxM := endx * df.Ncol

				go df.doChunk(&wg, byby[beg:endx], df.Matrix[begM:endxM], beg, endx)
			}
		}
		if left > 0 {
			// do any leftovers ourselves: usually less work.
			beg := numBackground
			endx := df.Nrow
			begM := beg * df.Ncol
			endxM := endx * df.Ncol
			df.doChunk(&wg, byby[beg:endx], df.Matrix[begM:endxM], beg, endx)
		}
		wg.Wait()
	*/
	return nil
}

func MemoryMapFile(fd *os.File) (mmap []byte) {
	var stat syscall.Stat_t
	err := syscall.Fstat(int(fd.Fd()), &stat)
	panicOn(err)
	sz := stat.Size
	vv("got sz = %v", sz)
	flags := syscall.MAP_SHARED
	prot := syscall.PROT_READ
	mmap, err = syscall.Mmap(int(fd.Fd()), 0, int(sz), prot, flags)
	panicOn(err)
	return
}
