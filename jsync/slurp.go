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
	//"time"
	//"unsafe"

	cristalbase64 "github.com/cristalhq/base64"
	"lukechampine.com/blake3"
)

// to allow parallel processing, we might well need
// to define only 1MB chunk boundaries as being the
// part within which CDC is applied. That is, guarantee
// that all chunks end on 1MB boundaries, even if
// they would not naturally end there.

func SlurpBlake(path string) (blake3sum string, err error) {

	// first argument is the path to the csv file to parse
	fd, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer fd.Close()

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

	h := blake3.New(64, nil)
	h.Write(buf)
	by := h.Sum(nil)

	blake3sum = "blake3.33B-" + cristalbase64.URLEncoding.EncodeToString(by[:33])

	return
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
