package sparsified

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	//"runtime"
	"syscall"

	"golang.org/x/sys/unix"
)

//go:generate greenpack

// fortunately, this is the same number on linux and darwin.
// const FALLOC_FL_INSERT_RANGE = unix.FALLOC_FL_INSERT_RANGE
const FALLOC_FL_INSERT_RANGE = 32

const linux_FALLOC_FL_COLLAPSE_RANGE = 8
const linux_FALLOC_FL_PUNCH_HOLE = 2 // linux
const darwin_F_PUNCHHOLE = 99        // from sys/fcntl.h:319

var oneZeroBlock4k [4096]byte
var databuf [4096]byte

// PunchBelowBytes gives the threshold for
// hole punching to preserve sparsity of small files.
// This is set to 64MB to reflect that Apple's Filesystem (APFS)
// will not reliably make a sparse file that is smaller
// than 32MB or so (depending on configuration) and so
// for files < PunchBelowBytes, we will come back after
// copying the file and punch out holes that should be there.
const PunchBelowBytes = 64 << 20

var ErrShortAlloc = fmt.Errorf("smaller extent than requested was allocated.")

// allocated probably zero in this case, especially since
// we asked for "all-or-nothing"
var ErrFileTooLarge = fmt.Errorf("extent requested was too large.")

// SparseSpan represents a sparse hole region, or a data region.
type SparseSpan struct {
	IsHole              bool `zid:"0"`
	IsUnwrittenPrealloc bool `zid:"1"`

	// notes:
	// 1. Regular data spans have both IsHole and
	//    IsUnwrittenPrealloc false.
	// 2. It is illegal to have both IsHole and
	//    IsUnwrittenPrealloc true on the same SparseSpan.

	Beg   int64  `zid:"2"`
	Endx  int64  `zid:"3"`
	Flags uint32 `zid:"4"`
}

type SparseSpans struct {
	Slc []SparseSpan `zid:"0"`
}

func (s *SparseSpans) String() (r string) {
	if len(s.Slc) == 0 {
		return "SparseSpans{}"
	}
	r = "SparseSpans{\n"
	for i, sp := range s.Slc {
		r += fmt.Sprintf("[%02d] %v\n", i, sp.String())
	}
	r += "}\n"
	return
}

// two nil SparseSpan are never equal; like NaN
func (s *SparseSpan) Equal(r *SparseSpan) bool {
	if s == nil || r == nil {
		return false
	}
	if s.IsHole != r.IsHole {
		return false
	}
	if s.Beg != r.Beg {
		return false
	}
	if s.Endx != r.Endx {
		return false
	}
	return true
}

// lastSparseSpanPreAlloc is a helper for Equal below.
func lastSparseSpanPreAlloc(slc []SparseSpan) bool {
	n := len(slc)
	if n <= 0 {
		return false
	}
	return slc[n-1].IsUnwrittenPrealloc
}

// two nil SparseSpans are never equal; like NaN
func (s *SparseSpans) Equal(r *SparseSpans) bool {
	if s == nil || r == nil {
		return false
	}
	ns := len(s.Slc)
	nr := len(r.Slc)
	if ns == 0 && nr == 0 {
		return false // same as both nil
	}
	if ns == 0 || nr == 0 {
		// one is zero span, the other not.
		return false
	}
	// setup to
	// allow one final pre-allocated unwritten,
	// for non-4K block sized files. See test 007.
	numMin := min(nr, ns)
	longer := s.Slc
	if nr > ns {
		longer = r.Slc
	}
	if ns != nr {
		// allow one final pre-allocated unwritten,
		// for non-4K block sized files.
		diff := ns - nr
		if diff < 0 {
			diff = -diff
		}
		if diff != 1 || !lastSparseSpanPreAlloc(longer) {
			return false
		}
	}
	for i := range numMin {
		ss := s.Slc[i]
		rs := r.Slc[i]
		if !rs.Equal(&ss) {
			return false
		}
	}
	return true
}

func (s *SparseSpan) String() string {
	var extra string
	x := (s.Endx - s.Beg) % 4096
	if x != 0 {
		extra = fmt.Sprintf(", and %v bytes", x)
	}
	return fmt.Sprintf("SparseSpan{IsHole:%v, Pre:%v, Beg:%v, Endx: %v} (%v pages of 4KB%v)",
		s.IsHole, s.IsUnwrittenPrealloc, formatUnder(s.Beg), formatUnder(s.Endx), formatUnder((s.Endx-s.Beg)/4096), extra)
}

// if file already exists we return nil, error.
// otherwise fd refers to an apparent nblock * 4KB but actual 0 byte file.
func CreateSparseFile(path string, nblock int64) (fd *os.File, err error) {

	if nblock < 1 {
		return nil, fmt.Errorf("nblock must be >= 1; not %v", nblock)
	}

	if fileExists(path) {
		//fd, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		//    if err != nil {
		//        return
		//    }
		return nil, fmt.Errorf("error in CreateSparseFile: file exists '%v'", path)
	}

	// so we can only insert if the file already has
	// a byte in it. Write a zero.
	fd, err = os.Create(path)
	if err != nil {
		return
	}

	// maybe in the future we will allow 0 block creation here.
	if nblock < 1 {
		return
	}

	// darwin:
	// stat -f "apparent_size: %z ; allocated blocks in use: %b" test003.sparse
	// apparent_size: 24576 ; allocated blocks in use: 0

	// punch out all of the first of those blocks
	//var offset int64

	length := int64(4096) * nblock
	err = fd.Truncate(length)
	panicOn(err)
	return
}

func IsSparseFile(fd *os.File) (isSparse bool, err error) {

	fi, err := fd.Stat()
	if err != nil {
		return false, err
	}
	stat := fi.Sys().(*syscall.Stat_t)
	apparent := stat.Size

	// we do have to seek for a hole to see if it is sparse at all.
	var holeBeg int64
	holeBeg, err = unix.Seek(int(fd.Fd()), 0, unix.SEEK_HOLE)
	if err != nil {
		if err == syscall.ENXIO { // No holes (all data till end)
			// not sparse
			return
		}
		panic(fmt.Errorf("error: failed to seek data from offset %v: '%v' for path '%v'", 0, err, fd.Name()))
		return
	}
	//vv("holeBeg = %v", holeBeg)
	if holeBeg >= apparent {
		// not sparse
		return
	}
	//vv("have holeBeg = %v <= apparent = %v", holeBeg, apparent)

	actual := stat.Blocks * 512 // lies: int64(stat.Blksize), says 4096 (on darwin).
	//vv("IsSparseFile fi = '%#v'; apparent = '%v'; stat.Blocks=%v; actual = '%v'; (actual/4096 = num pages=%v)", fi, apparent, stat.Blocks, actual, float64(actual)/4096)

	// are there are other ways to be sparse?
	// Just having multiple extents does not matter.
	// Checking the inodes for the sparseness flags seems
	// too much, and I'm not sure they would tell us
	// if the file is "still" sparse. This seems
	// reasonable for now; an "operational definition".

	return actual < apparent, nil
}

type SparseSum struct {
	IsSparse    bool
	DiskSize    int64 // bytes actually in use, including pre-allocation
	StatSize    int64 // what stat/ls reports
	FI          os.FileInfo
	UnwritBegin int64
	UnwritEndx  int64
}

// SparseFileSize returns isSparse true if the
// file fd has at least one sparse "hole" in it.
//
// I think pre-allocation (to avoid fragmentation) can
// make diskBytesInUse > statSize,
// and that is orthogonal to sparseness, which can (but
// does not always: e.g. when sparseness and pre-allocation
// are both in use) make diskBytesInUse < statSize.
//
// Hence comparing statSize and diskBytesInUse for
// a file cannot reliably determine the "sparseness"
// of a file. Instead we must seek for a hole
// using the unix.SEEK_HOLE option to unix.Seek().
//
// When copying a file that has pre-allocation, client code
// should decide if it wants to replicate the pre-allocation
// or not; we do nothing here about it.
func SparseFileSize(fd *os.File) (sum *SparseSum, err error) {

	fi, err := fd.Stat()
	if err != nil {
		//vv("fd.Stat err = '%v'", err)
		return
	}

	sum = &SparseSum{
		FI: fi,
		//	UnwritBegin    int64
		//	UnwritEndx     int64
	}
	stat := fi.Sys().(*syscall.Stat_t)
	sum.StatSize = stat.Size

	// the Go fd.Stat() API is tricky here: int64(stat.Blksize),
	// says 4096 (e.g. on darwin APFS), but the multiplier to
	// use to compute actual in-use space is 512.
	sum.DiskSize = stat.Blocks * 512

	// we do have to seek for a hole to see if it is sparse at all.
	var holeBeg int64
	holeBeg, err = unix.Seek(int(fd.Fd()), 0, unix.SEEK_HOLE)
	if err != nil {
		if err == syscall.ENXIO { // No holes (all data till end)
			// not sparse
			//vv("not sparse, no hole found")
			err = nil

			////vv("special extra check for pre-allocated file... diskBytesInUse = %v", diskBytesInUse)
			//var dataBeg int64
			//dataBeg, err = unix.Seek(int(fd.Fd()), 0, unix.SEEK_DATA)
			//if err != nil {
			//	//vv("err = '%v' from unix.Seek(SEEK_DATA); err == syscall.ENXIO ? %v", err, err == syscall.ENXIO) // fileop.go:234 2025-07-08 11:38:13.927 -0500 CDT err = 'device not configured' from unix.Seek(SEEK_DATA); err == syscall.ENXIO ? true
			//	err = nil
			//	return
			//}
			////vv("dataBeg = '%v' from unix.Seek(SEEK_DATA)", dataBeg)
			return
		}
		panic(fmt.Errorf("error: non-ENXIO error: failed to seek data from offset %v: err='%v' for path '%v'", 0, err, fd.Name()))
		return
	}
	////vv("holeBeg = %v", holeBeg)
	if holeBeg >= sum.StatSize {
		// not sparse.
		// A subtle case: for a 369 byte file, APFS will
		// report a hole at position 369
		// because the rest of rh 4K block is technically empty
		// but in use by the file. We don't call
		// that a sparse file. See Test007 in sparse_test.go.
		return
	}
	////vv("have holeBeg = %v < statSz = %v", holeBeg, statSz)
	sum.IsSparse = true

	//if diskBytesInUse > statSize {
	// actually pre-allocation of blocks can make this true, right?
	// Right. If FALLOC_FL_KEEP_SIZE is used. At least on linux.
	// In that case diskBytesInUse can be more that the
	// reported stat size.
	//
	// linux man 2 fallocate says
	//
	// "Allocating disk space
	//
	// "The default operation (i.e., mode is zero) of fallocate()
	// allocates the disk space within the range specified by
	// offset and len.  The file size (as reported by stat(2))
	// will be changed if offset+len is greater than
	// the file size.  Any subregion within the range
	// specified by offset and len that did not contain
	// data before the call will be initialized to
	// zero. This default behavior closely resembles the
	// behavior of the posix_fallocate(3) library function,
	// and is intended as a method of optimally implementing
	// that function."
	//
	// "If the FALLOC_FL_KEEP_SIZE flag is specified
	// in mode, the behavior of the call is similar,
	// but the file size will not be changed even if offset+len
	// is greater than the file size.  Preallocating
	// zeroed blocks beyond the end of the file in this
	// manner is useful for optimizing append workloads."
	//
	// Thus we do NOT assert this always holds:
	//panic(fmt.Sprintf("diskBytesInUse=%v > statSize=%v; we assumed this was impossible!", diskBytesInUse, statSize))
	//}

	/*
		// FindSparseRegions calls us, so this can
		// infinite loop.
		if !noSparseRegion {
			var spans *SparseSpans
			var unwritIndex int
			spans, unwritIndex, err = FindSparseRegions(fd)
			if err != nil {
				return
			}

			// report first unwritten preallocated region.
			if unwritIndex >= 0 {
				sum.UnwritBegin = spans.Slc[unwritIndex].Beg
				sum.UnwritEndx = spans.Slc[unwritIndex].Endx
			}
		}
	*/
	return
}

// FindSparseRegions finds data and hole regions in a sparse file.
// If unwritIndex >= 0, then it refers to the first
// unwritten/pre-allocated region >= 4096 bytes in the file.
func FindSparseRegions(fd *os.File) (sum *SparseSum, spans *SparseSpans, err error) {

	sum, err = SparseFileSize(fd)
	if err != nil {
		return
	}
	statsz := sum.StatSize
	disksz := sum.DiskSize
	fileSize := statsz

	spans = &SparseSpans{}
	unwritIndex := -1

	//vv("isSparse = %v; statsz = %v; disksz = %v", isSparse, statsz, disksz)

	// maybe todo: get fiemap working?
	// https://github.com/longhorn/sparse-tools/blob/master/sparse/fiemap.go
	// and
	// github.com/glycerine/go-fibmap should be avoided as they don't use
	// the actual C structs, and you cannot just fake it with Go structs.
	//
	//if runtime.GOOS == "linux" {
	//	spans2, err2 := LinuxFindSparseRegions(f)
	//	if err2 == nil {
	//		return spans2, err2
	//	}
	//}

	// non-fiemap workaround: only handles pre-allocation
	// at the first and second segment, but is portable to darwin.
	if disksz > statsz {
		// we have pre-allocated some of the file.
		if !sum.IsSparse {
			if statsz > 0 {
				spans.Slc = append(spans.Slc, SparseSpan{
					Beg:  0,
					Endx: statsz,
				})
			}
			spans.Slc = append(spans.Slc, SparseSpan{
				IsUnwrittenPrealloc: true,
				Beg:                 statsz,
				Endx:                disksz,
			})
			if disksz >= 4096+statsz && unwritIndex < 0 {
				// report first unwrit if any
				unwritIndex = len(spans.Slc) - 1
				sum.UnwritBegin = statsz
				sum.UnwritEndx = disksz
			}
			return
		}
	}

	if !sum.IsSparse {
		spans.Slc = append(spans.Slc, SparseSpan{
			IsHole: false,
			Beg:    0,
			Endx:   fileSize,
		})
		return
	}
	//vv("FindSparseRegions has isSparse=true from SparseFileSize() for '%v'", f.Name())

	fdint := int(fd.Fd())

	var dataBeg int64
	var holeBeg int64
	var offset int64

	for offset < fileSize {
		// Seek to the next data region
		dataBeg, err = unix.Seek(fdint, offset, unix.SEEK_DATA) // ok on darwin, not linux
		//dataBeg, err = f.Seek(offset, unix.SEEK_DATA)
		// on totally sparse file: err == 0x6 / syscall.Errno, dataBeg = -1
		//vv("for loop: offset=%v < fileSize=%v, Seek(offset, SEEK_DATA) gave err = '%v'/%T; dataBeg='%v'", offset, fileSize, err, err, dataBeg)
		if err != nil {

			if errno(err) == syscall.ENXIO {
				//vv("errno(err) == syscall.ENXIO")
				//errs := err.Error()
				//if strings.Contains(errs, `no such device or address`) {
				////vv("concluding just one hole left from offset %v", offset)
				spans.Slc = append(spans.Slc, SparseSpan{
					IsHole: true,
					Beg:    offset,
					Endx:   fileSize,
				})
				err = nil
				return
			}
			err = fmt.Errorf("error: failed to seek data from offset %v: '%v' for path '%v'", offset, err, fd.Name())
			return
		}

		// If dataBeg is greater than current offset, there's a hole before it
		if dataBeg > offset {
			spans.Slc = append(spans.Slc, SparseSpan{
				IsHole: true,
				Beg:    offset,
				Endx:   dataBeg,
			})
		}
		if dataBeg >= fileSize {
			return
		}

		// Now we are at a data region. Find the end of this data region.
		////vv("unix.SEEK_HOLE = %v", unix.SEEK_HOLE) // unix.SEEK_HOLE = 4 on linux
		holeBeg, err = unix.Seek(fdint, dataBeg, unix.SEEK_HOLE)
		if err != nil {
			if err == syscall.ENXIO { // No more holes (all data till end)
				spans.Slc = append(spans.Slc, SparseSpan{
					IsHole: false,
					Beg:    dataBeg,
					Endx:   fileSize,
				})
				err = nil
				return
			}
			err = fmt.Errorf("error: failed to seek data from offset %v: '%v' for path '%v'", dataBeg, err, fd.Name())
			return
		}

		// The region from dataBeg to holeBeg is data
		spans.Slc = append(spans.Slc, SparseSpan{
			IsHole: false,
			Beg:    dataBeg,
			Endx:   holeBeg,
		})

		// Move to the next offset to continue searching
		offset = holeBeg
		// INVAR: offset starts at a hole or end of file.
	}
	return
}

func demoAPFS() {
	// Create a dummy sparse file for testing
	sparseFileName := "sparse_test_file.bin"

	os.Remove(sparseFileName)
	defer os.Remove(sparseFileName)

	logicalSize := int64(1 << 20)
	nblock := logicalSize / 4096
	remainder := logicalSize % 4096
	if remainder > 0 {
		nblock++
	}
	fd, err := CreateSparseFile(sparseFileName, nblock)
	panicOn(err)
	fd.Close()

	fmt.Printf("Created sparse file '%s' with logical size %d bytes", sparseFileName, logicalSize)
	// Re-open the file for reading and hole detection to ensure fresh state
	f_read, err := os.Open(sparseFileName)
	if err != nil {
		fmt.Printf("Error opening file for detection: %v\n", err)
		return
	}
	defer f_read.Close()

	sum, spans, err := FindSparseRegions(f_read)
	_ = sum
	panicOn(err)

	fmt.Printf("spans:\n")
	for i, span := range spans.Slc {
		fmt.Printf("[%02d]  %v\n", i, span.String())
	}

	// Verify on-disk size
	fmt.Println("\nOn-disk usage vs. apparent size:")
	cmd := fmt.Sprintf("du -h %s", sparseFileName)
	output, err := runCommand(cmd)
	if err != nil {
		fmt.Printf("Error running 'du': %v\n", err)
	} else {
		fmt.Printf("du -h output:\n%s", output)
	}

	cmd = fmt.Sprintf("ls -l %s", sparseFileName)
	output, err = runCommand(cmd)
	if err != nil {
		fmt.Printf("Error running 'ls -l': %v\n", err)
	} else {
		fmt.Printf("ls -l output:\n%s", output)
	}

}

func runCommand(command string) ([]byte, error) {
	cmd := exec.Command("bash", "-c", command)
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("command '%s' failed: %v, stderr: %s", command, err, stderr.String())
	}
	return out.Bytes(), nil
}

// from https://go-review.googlesource.com/c/go/+/78030/5/src/archive/tar/sparse_unix.go#b59

// helper for sparseDetectUnix()
func fseek(f *os.File, pos int64, whence int) (int64, error) {
	pos, err := f.Seek(pos, whence)
	if errno(err) == syscall.ENXIO {
		// SEEK_DATA returns ENXIO when past the last data fragment,
		// which makes determining the size of the last hole difficult.
		pos, err = f.Seek(0, io.SeekEnd)
	}
	return pos, err
}

func errno(err error) error {
	if perr, ok := err.(*os.PathError); ok {
		return perr.Err
	}
	return err
}

func SparseDiff(srcpath, destpath string) (err error) {

	src, err := os.Open(srcpath)
	panicOn(err)
	if err != nil {
		return fmt.Errorf("SparseDiff error: could not open srcpath '%v': '%v'", srcpath, err)
	}

	dest, err := os.Open(destpath)
	panicOn(err)
	if err != nil {
		return fmt.Errorf("SparseDiff error: could not open destpath '%v': '%v'", destpath, err)
	}

	//isSparse0, disksz0, statsz0, _, err0 := SparseFileSize(src)
	sum0, err0 := SparseFileSize(src)
	panicOn(err0)
	if err0 != nil {
		return fmt.Errorf("SparseDiff error from SparseFileSize(srcpath='%v'): '%v'", srcpath, err0)
	}
	isSparse0 := sum0.IsSparse
	disksz0 := sum0.DiskSize
	statsz0 := sum0.StatSize

	//isSparse1, disksz1, statsz1, _, err1 := SparseFileSize(dest)
	sum1, err1 := SparseFileSize(dest)
	panicOn(err1)
	if err1 != nil {
		return fmt.Errorf("SparseDiff error from SparseFileSize(destpath='%v'): '%v'", destpath, err1)
	}
	isSparse1 := sum1.IsSparse
	disksz1 := sum1.DiskSize
	statsz1 := sum1.StatSize

	// check the basic outlines match.
	if isSparse0 != isSparse1 {
		return fmt.Errorf("SparseDiff error: srcpath('%v') has isSparse=%v, but destpath('%v') has isSparse=%v", srcpath, isSparse0, destpath, isSparse1)
	}
	if disksz0 != disksz1 {
		return fmt.Errorf("SparseDiff error: srcpath('%v') has disksz=%v, but destpath('%v') has disksz=%v", srcpath, disksz0, destpath, disksz1)
	}
	if statsz0 != statsz1 {
		return fmt.Errorf("SparseDiff error: srcpath('%v') has statsz=%v, but destpath('%v') has statsz=%v", srcpath, statsz0, destpath, statsz1)
	}

	fileSize := statsz0
	//vv("isSparse = %v; statsz = %v; disksz = %v", isSparse0, statsz0, disksz0)

	fdsrc := int(src.Fd())
	fddest := int(dest.Fd())

	var dataBeg0, dataBeg1 int64
	var holeBeg0, holeBeg1 int64
	var offset int64

	for offset < fileSize {
		// Seek to the next data region
		dataBeg0, err0 = unix.Seek(fdsrc, offset, unix.SEEK_DATA)
		//vv("src: for loop: offset=%v < fileSize=%v, Seek(offset, SEEK_DATA) gave err = '%v'/%T; dataBeg='%v'; err == syscall.ENXIO: %v", offset, fileSize, err0, err0, dataBeg0, err == syscall.ENXIO)

		dataBeg1, err1 = unix.Seek(fddest, offset, unix.SEEK_DATA)
		//vv("dest: for loop: offset=%v < fileSize=%v, Seek(offset, SEEK_DATA) gave err = '%v'/%T; dataBeg='%v'; err == syscall.ENXIO: %v", offset, fileSize, err1, err1, dataBeg1, err == syscall.ENXIO)

		if err0 != nil {
			if errno(err0) == syscall.ENXIO {
				//vv("errno(err0) == syscall.ENXIO")
				// [offset, fileSize) is a sparse hole on src.

				if errno(err1) != syscall.ENXIO {
					return fmt.Errorf("SparseDiff error: srcpath('%v') got ENXIO at offset=%v, but destpath('%v') got err1='%v'", srcpath, offset, destpath, err1)
				}
				return nil
			}
			//return fmt.Errorf("SparseDiff error: failed to seek data from offset %v: '%v' for srcpath '%v'", offset, err0, srcpath)
		}
		// symmetrically
		if err1 != nil {
			if errno(err1) == syscall.ENXIO {
				//vv("errno(err1) == syscall.ENXIO")
				// [offset, fileSize) is a sparse hole on dest.

				if errno(err0) != syscall.ENXIO {
					return fmt.Errorf("SparseDiff error: destpath('%v') got ENXIO at offset=%v, but srcpath('%v') got err0='%v'", destpath, offset, srcpath, err0)
				}
				return nil
			}
			return fmt.Errorf("SparseDiff error: failed to seek data from offset %v: '%v' for destpath '%v'", offset, err1, destpath)
		}
		// INVAR: err0 == nil && err1 == nil

		if dataBeg0 != dataBeg1 {
			return fmt.Errorf("SparseDiff error: diff error on SEEK_DATA from offset=%v: srcpath('%v') got dataBeg0=%v, but destpath('%v') got dataBeg1=%v", offset, srcpath, dataBeg0, destpath, dataBeg1)
		}
		// INVAR: dataBeg0 == dataBeg1

		// If dataBeg is greater than current offset, there's a
		// hole before it if dataBeg > offset {
		// 	spans.Slc = append(spans.Slc, SparseSpan{
		// 		IsHole: true,
		// 		Beg:    offset,
		// 		Endx:   dataBeg,
		// 	})
		// }
		if dataBeg0 >= fileSize {
			return
		}

		// Now we are at a data region. Find the end of this data region.
		////vv("unix.SEEK_HOLE = %v", unix.SEEK_HOLE) // unix.SEEK_HOLE = 4 on linux
		holeBeg0, err0 = unix.Seek(fdsrc, dataBeg0, unix.SEEK_HOLE)
		holeBeg1, err1 = unix.Seek(fddest, dataBeg0, unix.SEEK_HOLE)

		if err0 != err1 {
			return fmt.Errorf("SparseDiff error: diff error on SEEK_HOLE from dataBeg0=%v: srcpath('%v') got err0=%v, but destpath('%v') got err1=%v", dataBeg0, srcpath, err0, destpath, err1)
		}
		// INVAR: err0 == err1

		if holeBeg0 != holeBeg1 {
			return fmt.Errorf("SparseDiff error: diff error on SEEK_HOLE from dataBeg0=%v: srcpath('%v') got holeBeg0=%v, but destpath('%v') got holeBeg1=%v", dataBeg0, srcpath, holeBeg0, destpath, holeBeg1)
		}
		// INVAR: holeBeg0 == holeBeg1

		if err0 != nil && err0 != syscall.ENXIO {
			return fmt.Errorf("SparseDiff error: file (not ENXIO) error on SEEK_HOLE from dataBeg0=%v: srcpath('%v'): err0='%v'", dataBeg0, srcpath, err0)
		}

		endx := holeBeg0
		if err0 == syscall.ENXIO {
			// last data segment
			endx = fileSize
		}
		// compare [dataBeg0, endx)
		sz := endx - dataBeg0
		srcdata := make([]byte, sz)
		destdata := make([]byte, sz)
		_, err0 = src.Seek(dataBeg0, 0)
		panicOn(err0)
		_, err1 = dest.Seek(dataBeg0, 0)
		panicOn(err1)
		_, err0 = io.ReadFull(src, srcdata)
		panicOn(err0)
		_, err1 = io.ReadFull(dest, destdata)
		panicOn(err1)

		for j, s := range srcdata {
			d := destdata[j]
			if d != s {
				return fmt.Errorf("SparseDiff error: data diff error at pos %v: d('%v') != s('%v'). srcpath='%v'; destpath='%v'", j, d, s, srcpath, destpath)
			}
		}

		// Move to the next offset to continue searching
		offset = holeBeg0
		// INVAR: offset starts at a hole or end of file.
	}
	return
}

func CopySparseFile(srcpath, destpath string) (err error) {

	src, err0 := os.Open(srcpath)
	panicOn(err0)
	if err0 != nil {
		return fmt.Errorf("error in CopySparseFile: could not os.Open(srcpath='%v'): '%v'", srcpath, err0)
	}
	defer src.Close()

	dest, err1 := os.Create(destpath)
	panicOn(err1)
	if err1 != nil {
		return fmt.Errorf("error in CopySparseFile: could not os.Create(destpath='%v'): '%v'", destpath, err1)
	}
	defer dest.Close()

	sum0, err0 := SparseFileSize(src)
	panicOn(err0)
	//vv("isSparse0 = %v; statsz0 = %v; disksz0 = %v; srcpath='%v'", isSparse0, statsz0, disksz0, srcpath)

	if err0 != nil {
		return fmt.Errorf("error in CopySparseFile: error from SparseFileSize(srcpath='%v'): '%v'", srcpath, err0)
	}
	isSparse0 := sum0.IsSparse
	disksz0 := sum0.DiskSize
	statsz0 := sum0.StatSize

	// sanity check
	if disksz0 < 0 || statsz0 < 0 {
		panic(fmt.Sprintf("disksz0 = %v; statsz0 = %v; neither should be negative", disksz0, statsz0))
	}
	if disksz0 == 0 && statsz0 == 0 {
		// empty file, so we are done.
		return
	}
	if statsz0 == 0 {
		// disksz0 > 0, so we have pre-allocated but empty source file.
		// pre-allocate in the dest to match src.
		_, err = Fallocate(dest, FALLOC_FL_KEEP_SIZE, 0, disksz0)
		panicOn(err)
		//vv("fallocate with FALLOC_FL_KEEP_SIZE offset=%v; sz=%v was ok. statsz==0", 0, disksz0)
		return // done
	}
	// INVAR: statsz0 > 0

	didPrealloc := false
	if statsz0 < disksz0 {
		//vv("detected partly pre-allocated file")
		// partially pre-allocated file.
		// Almost the same as the above, but we also need
		// to copy, not just return.

		// WARNING
		// Note the big assumption here: we only handle pre-allocation
		// that goes through end of file at this point. This
		// is the common pre-allocation use-case as in seaweedfs.
		//
		// More complicated scenarios that intersperse sparse holes
		// and pre-allocated unwritten regions are deferred until
		// we have an actual use case at hand.
		//
		// Normally a user using pre-allocation would loathe
		// to have any sparsity in their file, because the point
		// of pre-allocation is to make appends super-fast,
		// avoid disk fragmentation, and hopefully almost
		// never get an out-of-disk-space error unexpectedly. If
		// you start punching holes in your file, those guarantees
		// are voided.
		//
		// Hence our thesis is that if pre-allocation is used to avoid
		// fragmentation and out-of-disk-space write failures,
		// then the file is unlikely to also be sparse: the typical
		// use is expected to actually fill some prefix of the
		// file with actual data.
		//
		// We should still be able to punch sparse holes just fine
		// early in the file if source has them -- while leaving
		// the tail pre-allocated. (A hole will de-allocate, voiding
		// the pre-allocation guarantee of course, and while strange,
		// that is user's perogative. We'll still try
		// to duplicate that hole into the destination below).
		//
		// Note also the heuristic deployed here: we could miss
		// partly sparse and partly pre-allocated files, if there
		// is more sparseness than pre-allocation (so statsz0 >= disksz0).
		// While fiemap on linux could allow us to more exactly duplicate (TODO),
		// since darwin lacks the same facility, we defer that
		// for later optimization.

		_, err = Fallocate(dest, FALLOC_FL_KEEP_SIZE, 0, disksz0)
		panicOn(err)
		//vv("fallocate with FALLOC_FL_KEEP_SIZE offset=%v; sz=%v was ok. 0 < statsz0(%v) < disksz0(%v)", 0, disksz0, statsz0, disksz0)
		didPrealloc = true
	}

	// no sparseness just means straight file copy.
	if !isSparse0 {
		//vv("srcpath '%v' is not sparse", srcpath)

		_, err = src.Seek(0, 0)
		panicOn(err)
		_, err = dest.Seek(0, 0)
		panicOn(err)

		var nw int64
		nw, err = io.Copy(dest, src)
		if nw != statsz0 {
			panic(fmt.Sprintf("short write??? nw(%v) != statsz0(%v). srcpath='%v'; destpath='%v'; err = '%v'", nw, statsz0, srcpath, destpath, err))
		}
		//vv("copied srcpath('%v') -> destpath('%v'); err='%v'", srcpath, destpath, err)
		return
	}
	// INVAR: isSparse0 true, source file is sparse.
	// INVAR: statsz0 > 0

	if !didPrealloc {
		// set the size, otherwise our file can end up too short.
		dest.Truncate(statsz0)
	}

	fileSize := statsz0
	fdsrc := int(src.Fd())
	//fddest := int(dest.Fd())

	var dataBeg int64
	var holeBeg int64
	var offset int64

	for offset < fileSize {
		// Seek to the next data region
		dataBeg, err = unix.Seek(fdsrc, offset, unix.SEEK_DATA)
		//vv("for loop: offset=%v < fileSize=%v, Seek(offset, SEEK_DATA) gave err = '%v'/%T; dataBeg='%v'", offset, fileSize, err, err, dataBeg)
		if err != nil {
			if errno(err) == syscall.ENXIO {
				//vv("errno(err) == syscall.ENXIO")
				//errs := err.Error()
				//if strings.Contains(errs, `no such device or address`) {
				//vv("concluding just one hole left from offset %v", offset)
				err = nil
				if !didPrealloc {
					sz := fileSize - offset
					//vv("did not preallocate, so punching hole in dest, offset=%v, sz=%v", offset, sz)
					// spans.Slc = append(spans.Slc, SparseSpan{
					// 	IsHole: true,
					// 	Beg:    offset,
					// 	Endx:   fileSize,
					// })
					_, err = Fallocate(dest, FALLOC_FL_PUNCH_HOLE, offset, sz)
					panicOn(err)
				}
				return
			}
			err = fmt.Errorf("error in CopySparseFile: failed to seek data from offset %v: '%v' for srcpath '%v'", offset, err, srcpath)
			return
		}

		// If dataBeg is greater than current offset, there's a hole before it
		if dataBeg > offset {
			// spans.Slc = append(spans.Slc, SparseSpan{
			// 	IsHole: true,
			// 	Beg:    offset,
			// 	Endx:   dataBeg,
			// })
			sz := dataBeg - offset
			_, err = Fallocate(dest, FALLOC_FL_PUNCH_HOLE, offset, sz)
			panicOn(err)
		}
		if dataBeg >= fileSize {
			return
		}

		// Now we are at a data region. Find the end of this data region.
		////vv("unix.SEEK_HOLE = %v", unix.SEEK_HOLE) // unix.SEEK_HOLE = 4 on linux
		holeBeg, err = unix.Seek(fdsrc, dataBeg, unix.SEEK_HOLE)
		if err != nil {
			if err == syscall.ENXIO { // No more holes (all data till end)
				err = nil
				// spans.Slc = append(spans.Slc, SparseSpan{
				// 	IsHole: false,
				// 	Beg:    dataBeg,
				// 	Endx:   fileSize,
				// })
				sz := fileSize - dataBeg
				_, err = src.Seek(dataBeg, 0)
				panicOn(err)
				_, err = dest.Seek(dataBeg, 0)
				panicOn(err)
				var nw int64
				nw, err = io.CopyN(dest, src, sz)
				if err != nil {
					return
				}
				if nw != sz {
					err = fmt.Errorf("error CopySparseFile: short CopyN? nw=%v but sz=%v, srcpath='%v'; destpath='%v'; err='%v'", nw, sz, srcpath, destpath, err)
					panic(err)
				}
				return
			}
			err = fmt.Errorf("error in CopySparseFile: failed to seek data from offset %v: '%v' for path '%v'", dataBeg, err, srcpath)
			return
		}

		// The region from dataBeg to holeBeg is data
		// spans.Slc = append(spans.Slc, SparseSpan{
		// 	IsHole: false,
		// 	Beg:    dataBeg,
		// 	Endx:   holeBeg,
		// })

		sz := holeBeg - dataBeg
		_, err = src.Seek(dataBeg, 0)
		panicOn(err)
		_, err = dest.Seek(dataBeg, 0)
		panicOn(err)
		var nw int64
		nw, err = io.CopyN(dest, src, sz)
		if err != nil {
			return
		}
		if nw != sz {
			err = fmt.Errorf("error CopySparseFile: short CopyN? nw=%v but sz=%v, srcpath='%v'; destpath='%v'; err='%v'; to next holeBeg=%v, from dataBeg=%v", nw, sz, srcpath, destpath, err, holeBeg, dataBeg)
			panic(err)
			return
		}

		// Move to the next offset to continue searching
		offset = holeBeg
		// INVAR: offset starts at a hole or end of file.
	}

	return
}

// MustGenerateSparseTestFiles makes 15 test files in dir.
// Since git/scp/etc may not be sparse file aware or
// preserving, for testing we must generate the
// sparse testfiles on demand just prior to running
// the sparse file funcationality tests. Hence this
// function generates a kind of "standard test suite"
// of sparse (and two pre-allocated) files. A minimum
// of 15 test files are created, and more can be
// requested by setting extra > 0. This function
// will panic with, rather than return, any error encountered.
// The seed0 can be varied to change the pseudo random
// data fill and sparse file structure.
// We will call os.MkdirAll to create dir, but will not
// panic if it already exists. The extra cannot
// be negative.
func MustGenerateSparseTestFiles(dir string, extra, seed0 int) {

	if extra < 0 {
		panic("extra cannot be negative")
	}

	os.MkdirAll(dir, 0777)

	var mb64 int64 = 1 << 26
	var seed [32]byte
	binary.LittleEndian.PutUint64(seed[:8], uint64(seed0))
	rng := newPRNG(seed)

	for i := range 15 + extra {

		var fd *os.File
		var err error
		path := filepath.Join(dir, fmt.Sprintf("testZZZ.outpath.%02d.sparsefile", i))

		if i <= 5 {
			// base cases to be sure we cover.
			switch i {
			case 0:
				// empty file. completely.
				fd, err := os.Create(path)
				panicOn(err)
				panicOn(fd.Close())
			case 1:
				// completely sparse file of some size but no data.
				fd, err := os.Create(path)
				panicOn(err)
				err = fd.Truncate(mb64)
				panicOn(err)
				panicOn(fd.Close())
			case 2:
				// some data, then the rest sparse (hole at end)
				fd, err := os.Create(path)
				panicOn(err)
				err = fd.Truncate(mb64)
				panicOn(err)
				_, err = fd.Seek(0, 0)
				panicOn(err)
				data := make([]byte, 4096)
				rng.cha8.Read(data)
				_, err = fd.Write(data)
				panicOn(err)
				panicOn(fd.Close())
			case 3:
				// start with sparse hole, then data to the end.
				fd, err := os.Create(path)
				panicOn(err)
				err = fd.Truncate(mb64)
				panicOn(err)
				_, err = fd.Seek((1<<26)-4096, 0)
				panicOn(err)
				data := make([]byte, 4096)
				rng.cha8.Read(data)
				_, err = fd.Write(data)
				panicOn(err)
				panicOn(fd.Close())
			case 4:
				// pre-allocated file of some size, but no data.
				fd, err := os.Create(path)
				panicOn(err)
				_, err = Fallocate(fd, FALLOC_FL_KEEP_SIZE, 0, mb64)
				panicOn(err)
				panicOn(fd.Close())
			case 5:
				// pre-allocated and some prefix of data.
				fd, err := os.Create(path)
				panicOn(err)
				_, err = Fallocate(fd, FALLOC_FL_KEEP_SIZE, 0, mb64)
				panicOn(err)
				_, err = fd.Seek(0, 0)
				panicOn(err)
				data := make([]byte, 4096)
				rng.cha8.Read(data)
				_, err = fd.Write(data)
				panicOn(err)
				panicOn(fd.Close())
			}
			continue
		}
		// the rest are randomly structured with any
		// data segments filled with random data.
		spec := genSpecSparse(rng)

		//vv("on i = %v, path = '%v'; spec = '%v'", i, path, spec)

		fd, err = createSparseFileFromSparseSpans(path, spec, rng)
		panicOn(err)
		panicOn(fd.Close())
	}
}

// mostly for tests; called by MustGenerateSparseTestFiles().
func genSpecSparse(rng *prng) *SparseSpans {

	spans := &SparseSpans{}

	const pagesz int64 = 4096
	var nextbeg int64
	numSegments := 1
	numSegments += int(rng.pseudoRandPositiveInt64() % 5)

	nextIsHole := false
	for k := range numSegments {

		if k == 0 {
			nextIsHole = rng.pseudoRandBool()
		} else {
			nextIsHole = !nextIsHole
		}
		npage := rng.pseudoRandPositiveInt64()%5 + 1
		nextLen := (npage) * pagesz
		////vv("npage = %v; nextLen = %v", npage, nextLen)

		sp := SparseSpan{
			IsHole: nextIsHole,
			Beg:    nextbeg,
			Endx:   nextbeg + nextLen,
		}
		spans.Slc = append(spans.Slc, sp)
		nextbeg = sp.Endx
	}
	return spans
}
