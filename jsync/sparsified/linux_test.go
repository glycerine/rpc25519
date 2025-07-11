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

		fd, err = createSparseFileFromSparseSpans(path, spec, nil)
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
	// 017 is fully pre-allocated, no data at all.
	// 018 adds one data page at the start.

	var mb64 int64 = 1 << 26
	spec := &SparseSpans{Slc: []SparseSpan{
		SparseSpan{IsHole: false, IsUnwrittenPrealloc: true, Beg: 0, Endx: mb64},
	}}

	path := fmt.Sprintf("test017.outpath.pre_allocated")
	//vv("on path = '%v'; spec = '%v'", path, spec)

	var fd *os.File
	var err error

	os.Remove(path)
	defer os.Remove(path)

	//vv("did remove path='%v'; about to call createSparseFileFromSparseSpans()", path)
	fd, err = createSparseFileFromSparseSpans(path, spec, nil)
	panicOn(err)
	//vv("done with createSparseFileFromSparseSpans()")
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
	fieRead, err := FieMap(fd) // errno == 22 if just one pre-alloc.
	panicOn(err)
	//defer fd.Close()

	if fieRead.Equal(spec) {
		vv("good: fieRead Equal spec, path='%v'; spec = '%v'", path, spec)
	} else {
		vv("bad:  fieRead NOT Equal spec")
		vv("fieRead =\n '%v'\n for path = '%v'\n\n vs spec=\n%v\n", fieRead, path, spec)

		panic("fix this")
	}
}

func Test018_FieMapLinux_preallocated2(t *testing.T) {

	// fiemap should mark pre-allocated correctly.
	// 017 is fully pre-allocated, no data at all.
	// 018 adds one data page at the start.

	var mb64 int64 = 1 << 26
	spec := &SparseSpans{Slc: []SparseSpan{
		SparseSpan{IsHole: false, IsUnwrittenPrealloc: true, Beg: 0, Endx: mb64},
	}}

	path := fmt.Sprintf("test018.outpath.pre_allocated2")
	//vv("on path = '%v'; spec = '%v'", path, spec)

	var fd *os.File
	var err error

	os.Remove(path)
	//defer os.Remove(path)

	//vv("did remove path='%v'; about to call createSparseFileFromSparseSpans()", path)
	fd, err = createSparseFileFromSparseSpans(path, spec, nil)
	panicOn(err)
	//vv("done with createSparseFileFromSparseSpans()")
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

	fd.Seek(0, 0)
	fd.Write(oneZeroBlock4k[:])

	// does Sync make a difference? YES!?!
	fd.Sync()
	// if we Sync, then we get what we orignally wanted/expected!
	// so we can tell, like filefrag, which are unwritten and preallocated.
	// The program closing doing a Sync was the difference.
	// [00] SparseSpan{IsHole:false, Pre:false, Beg:0, Endx: 4096} (1 pages of 4KB)
	// [01] SparseSpan{IsHole:false, Pre:true, Beg:4096, Endx: 67108864} (16383 pages of 4KB)

	// currently getting
	// [00] SparseSpan{IsHole:false, Pre:true, Beg:0, Endx: 67108864} (16384 pages of 4KB)
	// should not we get this? well we hoped. But we do not. so meh.
	// Implication: this means that for pre-alloc, we still must
	// manually scan for zero spans/un-written space??? that seems crazy.
	// Other tools can tell which are un-written portions!
	// example:
	// $ filefrag -v test018.outpath.pre_allocated2
	// Filesystem type is: ef53
	// File size of test018.outpath.pre_allocated2 is 4096 (1 block of 4096 bytes)
	//  ext:     logical_offset:        physical_offset: length:   expected: flags:
	//    0:        0..       0:  288817152.. 288817152:      1:            ,eof
	//    1:        1..   16383:  288817153.. 288833535:  16383:             last,unwritten,eof
	// using just these system calls, including a super-stat and a FIE_MAP:
	//
	// openat(AT_FDCWD, "test018.outpath.pre_allocated2", O_RDONLY) = 3
	// fstat(3, {st_mode=S_IFREG|0600, st_size=4096, ...}) = 0
	// fstatfs(3, {f_type=EXT2_SUPER_MAGIC, f_bsize=4096, f_blocks=424223951, f_bfree=91112231, f_bavail=69544399, f_files=107823104, f_ffree=84373119, f_fsid={val=[0x18923806, 0x1618697]}, f_namelen=255, f_frsize=4096, f_flags=ST_VALID|ST_RELATIME}) = 0
	// ioctl(3, FIGETBSZ, 0x651873918044)      = 0
	// fstat(1, {st_mode=S_IFCHR|0620, st_rdev=makedev(0x88, 0x2), ...}) = 0
	// getrandom("\x15\x26\x31\x2f\x4e\x3d\x2c\x20", 8, GRND_NONBLOCK) = 8
	// brk(NULL)                               = 0x651890c00000
	// brk(0x651890c21000)                     = 0x651890c21000
	// write(1, "Filesystem type is: ef53\n", 25Filesystem type is: ef53
	// ) = 25
	// ioctl(3, FS_IOC_GETFLAGS, [FS_EXTENT_FL]) = 0
	// write(1, "File size of test018.outpath.pre"..., 76File size of test018.outpath.pre_allocated2 is 4096 (1 block of 4096 bytes)
	// ) = 76
	// ioctl(3, FS_IOC_FIEMAP, {fm_start=0, fm_length=18446744073709551615, fm_flags=0, fm_extent_count=292} => {fm_flags=0, fm_mapped_extents=2, ...}) = 0
	// write(1, " ext:     logical_offset:       "..., 77 ext:     logical_offset:        physical_offset: length:   expected: flags:
	// ) = 77
	// write(1, "   0:        0..       0:  28881"..., 74   0:        0..       0:  288817152.. 288817152:      1:            ,eof
	// ) = 74
	// write(1, "   1:        1..   16383:  28881"..., 89   1:        1..   16383:  288817153.. 288833535:  16383:             last,unwritten,eof
	// ) = 89

	// in detail
	// $ strace -e ioctl -v -s 165536  --decode-fds=all filefrag -v test018.outpath.pre_allocated2
	// ioctl(3</mnt/oldrog/home/jaten/go/src/github.com/glycerine/sparsified/test018.outpath.pre_allocated2>, FIGETBSZ, 0x64b331eb5044) = 0
	// Filesystem type is: ef53
	// ioctl(3</mnt/oldrog/home/jaten/go/src/github.com/glycerine/sparsified/test018.outpath.pre_allocated2>, FS_IOC_GETFLAGS, [FS_EXTENT_FL]) = 0
	// File size of test018.outpath.pre_allocated2 is 4096 (1 block of 4096 bytes)
	// ioctl(3</mnt/oldrog/home/jaten/go/src/github.com/glycerine/sparsified/test018.outpath.pre_allocated2>, FS_IOC_FIEMAP, {fm_start=0, fm_length=18446744073709551615, fm_flags=0, fm_extent_count=292} => {fm_flags=0, fm_mapped_extents=2, fm_extents=[{fe_logical=0, fe_physical=1182995054592, fe_length=4096, fe_flags=0}, {fe_logical=4096, fe_physical=1182995058688, fe_length=67104768, fe_flags=FIEMAP_EXTENT_LAST|FIEMAP_EXTENT_UNWRITTEN}]}) = 0
	//  ext:     logical_offset:        physical_offset: length:   expected: flags:
	//    0:        0..       0:  288817152.. 288817152:      1:            ,eof
	//    1:        1..   16383:  288817153.. 288833535:  16383:             last,unwritten,eof

	// versus our call that is only getting 1 back: (BEFORE doing the fd.Sync)
	//
	// $ strace -e ioctl -v -s 165536  --decode-fds=all  ./test -test.v -test.run 018
	// fie2.go:75 2025-07-10 06:18:36.043 -0500 CDT sz = '4096'; vs disksz = 67108864; maxsz = 67108864
	// ioctl(3</mnt/oldrog/home/jaten/go/src/github.com/glycerine/sparsified/test018.outpath.pre_allocated2>, FS_IOC_FIEMAP, {fm_start=0, fm_length=67108864, fm_flags=0, fm_extent_count=292} => {fm_flags=0, fm_mapped_extents=1, fm_extents=[{fe_logical=0, fe_physical=1182592401408, fe_length=67108864, fe_flags=FIEMAP_EXTENT_LAST|FIEMAP_EXTENT_UNWRITTEN}]}) = 0

	// AFTER doing the fd.Sync() above, then we get proper 2 spans:
	//
	// ioctl(3</mnt/oldrog/home/jaten/go/src/github.com/glycerine/sparsified/test018.outpath.pre_allocated2>, FS_IOC_FIEMAP, {fm_start=0, fm_length=67108864, fm_flags=0, fm_extent_count=292} => {fm_flags=0, fm_mapped_extents=2, fm_extents=[{fe_logical=0, fe_physical=1180584173568, fe_length=4096, fe_flags=0}, {fe_logical=4096, fe_physical=1180584177664, fe_length=67104768, fe_flags=FIEMAP_EXTENT_LAST|FIEMAP_EXTENT_UNWRITTEN}]}) = 0

	updatedSpec := &SparseSpans{Slc: []SparseSpan{
		SparseSpan{IsHole: false, IsUnwrittenPrealloc: false, Beg: 0, Endx: 4096},
		SparseSpan{IsHole: false, IsUnwrittenPrealloc: true, Beg: 4096, Endx: mb64},
	}}
	_ = updatedSpec
	// actually original spec could be acceptable... and is what we get.??
	//updatedSpec = spec
	spec = updatedSpec

	// and finally, checking fiemap.
	fieRead, err := FieMap(fd) // errno == 22 if just one pre-alloc.
	panicOn(err)
	//defer fd.Close()

	if fieRead.Equal(spec) {
		vv("good: fieRead Equal spec, path='%v'; spec = '%v'", path, spec)
	} else {
		vv("bad:  fieRead NOT Equal spec")
		vv("fieRead =\n '%v'\n for path = '%v'\n\n vs spec=\n%v\n", fieRead, path, spec)

		panic("fix this")
	}

	// fieRead2, err := FieMap2(fd)
	// panicOn(err)
	// //defer fd.Close()

	// vv("fieRead after writing first page only = '%v'", fieRead2)
}

func Test019_FieMapLinux_empty_file(t *testing.T) {

	// fiemap should not choke on an empty file.

	path := fmt.Sprintf("test019.empty.file")
	//vv("on path = '%v'; spec = '%v'", path, spec)

	var fd *os.File
	var err error

	os.Remove(path)
	defer os.Remove(path)

	fd, err = os.Create(path)
	panicOn(err)
	defer fd.Close()
	fd.Sync()

	// and finally, checking fiemap.
	fieRead, err := FieMap(fd) // errno == 22 if just one pre-alloc.
	panicOn(err)

	if fieRead != nil {
		panic("expected nil err and nil fieRead b/c completely empty file")
	}
}

func Test020_FieMapLinux_fully_sparse_but_empty_file(t *testing.T) {

	// fiemap should not choke on fully sparse file

	var mb64 int64 = 1 << 26
	spec := &SparseSpans{Slc: []SparseSpan{
		SparseSpan{IsHole: true, IsUnwrittenPrealloc: false, Beg: 0, Endx: mb64}}}

	path := fmt.Sprintf("test020.fullsparse.file")
	vv("on path = '%v'; spec = '%v'", path, spec)

	os.Remove(path)
	defer os.Remove(path)

	// createSparseFileFromSparseSpans always does fd.Sync()
	// for us now. So we don't need to again.
	fd, err := createSparseFileFromSparseSpans(path, spec, nil)
	panicOn(err)
	defer fd.Close()

	// and finally, checking fiemap.
	fieRead, err := FieMap(fd)
	panicOn(err)

	if fieRead == nil {
		panic("expected 1 full sparse span back")
	}

	if fieRead.Equal(spec) {
		vv("good: fieRead Equal spec, path='%v'; spec = '%v'", path, spec)
	} else {
		vv("bad:  fieRead NOT Equal spec")
		vv("fieRead =\n '%v'\n for path = '%v'\n\n vs spec=\n%v\n", fieRead, path, spec)

		panic("fix this")
	}
}
