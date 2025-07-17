package sparsified

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	//"golang.org/x/sys/unix"
)

// path is just for reporting if intfd > 0 is given.
// Otherwise it is opened and the fd returned.
func insertRange(path string, fd *os.File, offset int64, length int64) (file *os.File, got int64, err error) {
	if fd == nil {
		// docs: "If the file does not exist, and the O_CREATE
		// flag is passed, it is created with mode perm (before umask);
		// the containing directory must exist."
		if fileExists(path) {
			file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
			if err != nil {
				return
			}
		} else {
			// so we can only insert if the file already has
			// a byte in it. Write a zero.
			fd, err = os.Create(path)
			if err != nil {
				return
			}
			// write a whole block so the rest of the
			// file can be block aligned too.
			var nw int
			nw, err = fd.Write(oneZeroBlock4k[:])
			if err != nil {
				return
			}
			if nw != len(oneZeroBlock4k) {
				return nil, 0, fmt.Errorf("could not write one 4096 zero page"+
					" to new path '%v': '%v'", path, err)
			}
		}
	} else {
		file = fd
	}
	got, err = Fallocate(file, FALLOC_FL_INSERT_RANGE, offset, length)
	return
}

// do we get an error when our allocation did not succeed?
// on AFPS and ext4 yes. XFS (aorus) actually succeeds, yikes!?! :)
func Test021_FallocateInsertRangeTooBig(t *testing.T) {

	path := "out.db"
	// note: file must be writable! so Open() does not suffice!
	//fd, err = os.OpenFile(path, os.O_RDWR, 0666)

	fd, err := os.Create(path)
	panicOn(err)
	defer fd.Close()
	defer os.Remove(path)

	fsDesc, err := FileSystemType(fd)
	panicOn(err)
	if fsDesc == "xfs" {
		t.Skip("xfs will claim to allocate 4 ExaB okay, so skip 021 test")
	}

	// does file need to be non-empty? yep. hmm... would be nice if that was not the case.
	_, err = fd.Write(oneZeroBlock4k[:])
	panicOn(err)

	offset := int64(0)
	// too biiiig!!!
	length := int64(4096) << 50 // 4 exa bytes // xfs_info says bsize and extsz are 4096
	//length := int64(64 << 20) // 64MB? yep, Apple Filesystem will happily give us this.

	//vv("length = '%v'", length)
	var got int64
	got, err = Fallocate(fd, FALLOC_FL_INSERT_RANGE, offset, length)
	if err == nil {
		panic(fmt.Sprintf("oh no. asked for 4 exabytes, got back: '%v' without an error??", got))
	}
	if err != ErrFileTooLarge {
		panic(fmt.Sprintf("wanted ErrFileTooLarge; got error '%v'", err))
	}
	fmt.Printf("asked for length='%v', got='%v'. wrote to path '%v'. all done.\n", length, got, path)
	// succeeds?!? oh noes...
	// asked for length='16777216', got='16777216'. wrote to path 'out.db'. all done.
	// got a 16MB span.
}

// Collapse-range is the opposite of Insert-range.
// These are from https://pkg.go.dev/golang.org/x/sys/unix#section-readme for Linux/amd64:
/*
FALLOC_FL_ALLOCATE_RANGE                    = 0x0
FALLOC_FL_COLLAPSE_RANGE                    = 0x8
FALLOC_FL_INSERT_RANGE                      = 0x20
FALLOC_FL_KEEP_SIZE                         = 0x1
FALLOC_FL_NO_HIDE_STALE                     = 0x4
FALLOC_FL_PUNCH_HOLE                        = 0x2
FALLOC_FL_UNSHARE_RANGE                     = 0x40
FALLOC_FL_ZERO_RANGE                        = 0x10
*/

// Collapse-Range was added in 2014 to Linux.
// https://lwn.net/Articles/587819/
// (about FALLOC_FL_COLLAPSE_RANGE)
/*
From: Namjae Jeon <namjae.jeon@samsung.com>

This patch series is in response of the following post:
http://lwn.net/Articles/556136/
"ext4: introduce two new ioctls"

Dave Chinner [XFS developer lead] suggested that truncate_block_range
(which was one of the ioctls name) should be an fallocate operation
and not any fs specific ioctl, hence we add this functionality to fallocate.

This patch series introduces new flag FALLOC_FL_COLLAPSE_RANGE for fallocate
and implements it for XFS and Ext4.

The semantics of this flag are following:
1) It collapses the range lying between offset and length by removing any data
   blocks which are present in this range and than updates all the logical
   offsets of extents beyond "offset + len" to nullify the hole created by
   removing blocks. In short, it does not leave a hole.
2) It should be used exclusively. No other fallocate flag in combination.
3) Offset and length supplied to fallocate should be fs block size aligned
   in case of xfs and ext4.
4) Collaspe range does not work beyond i_size.

This new functionality of collapsing range could be used by media editing tools
which does non linear editing to quickly purge and edit parts of a media file.
This will immensely improve the performance of these operations.
The limitation of fs block size aligned offsets can be easily handled
by media codecs which are encapsulated in a conatiner as they have to
just change the offset to next keyframe value to match the proper alignment.

*/

// on darwin, unix.FALLOC_FL_COLLAPSE_RANGE is not defined.
// on linux, unix.FALLOC_FL_COLLAPSE_RANGE = 8
//
// Of note, ZFS does not support this. Open issue for it:
// "Support FALLOC_FL_COLLAPSE_RANGE" Issue #15178
// https://github.com/openzfs/zfs/issues/15178

func TestCollapseRange(t *testing.T) {

	path := "out.db"
	// note: file must be writable! so Open() does not suffice!
	//fd, err = os.OpenFile(path, os.O_RDWR, 0666)

	fd, err := os.Create(path)
	panicOn(err)
	defer fd.Close()
	defer os.Remove(path)

	// does file need to be non-empty? yep. hmm... would be nice if that was not the case.
	var nw int
	nw, err = fd.Write(oneZeroBlock4k[:])
	if err != nil {
		return
	}
	if nw != len(oneZeroBlock4k) {
		panic(fmt.Errorf("could not write one 4096 zero page"+
			" to new path '%v': '%v'", path, err))
	}

	var sz, postsz int64
	_ = sz
	sz, err = fileSizeFromFile(fd)
	panicOn(err)
	//vv("starting file sz is '%v'", sz)

	//panicOn(err)
	//intfd := int(fd.Fd())
	//vv("got intfd = '%v'", intfd)

	offset := int64(0)
	// too biiiig!!!
	//length := int64(4096) << 50 // 4 exa bytes // xfs_info says bsize and extsz are 4096
	//length := int64(64 << 20) // 64MB? yep, Apple Filesystem will happily give us this.
	length := int64(4096)

	//vv("length = '%v'", length)
	// unix.FALLOC_FL_COLLAPSE_RANGE = 8
	////vv("unix.FALLOC_FL_COLLAPSE_RANGE = %v", unix.FALLOC_FL_COLLAPSE_RANGE)
	var got int64
	_ = got
	//got, err = Fallocate(fd, FALLOC_FL_COLLAPSE_RANGE, offset, length)
	got, err = Fallocate(fd, FALLOC_FL_PUNCH_HOLE, offset, length)
	//vv("Fallocate(FALLOC_FL_PUNCH_HOLE) err was '%v'; got = '%v'", err, got)
	if err == nil {
		//panic(fmt.Sprintf("oh no. asked for 4 exabytes, got back: '%v' without an error??", got))
	}
	if err == ErrShortAlloc {

	}
	//vv("got: '%v'; err = '%v'", got, err) // "smaller extent than requested was allocated."
	if err != ErrFileTooLarge {
		//panic(fmt.Sprintf("wanted ErrFileTooLarge; got error '%v'", err))
	}
	//fmt.Printf("asked for length='%v', got='%v'. wrote to path '%v'. all done.\n", length, got, path)

	fi, err := fd.Stat()
	panicOn(err)
	//vv("fi = '%#v'", fi)
	postsz = fi.Sys().(*syscall.Stat_t).Blocks * 512
	//vv("ending file postsz is '%v'", postsz)
	// same size still, but blocks were deallocated.
	//$ stat out.db
	//  File: out.db
	//  Size: 4096      	Blocks: 0          IO Block: 4096   regular file

	if postsz != 0 {
		panic("why not 0 size file after hole punching?")
	}
}

// test if file is sparse at all.
// and test if we can make a sparse file for sure.
func Test003_sparse_file_creation_and_detection(t *testing.T) {

	path := "test003.sparse"
	os.Remove(path) // don't panic, it might not exist
	defer os.Remove(path)

	nblock := int64(6)
	fd, err := CreateSparseFile(path, nblock)
	panicOn(err)
	isSparse, err := IsSparseFile(fd)
	panicOn(err)
	if !isSparse {
		t.Fatalf("problem: wanted sparse file '%v' but it was not sparse.", path)
	}

	// did we get the expected size
	sz, err := fileSizeFromFile(fd)
	panicOn(err)
	//vv("starting file sz is '%v'", sz)
	want := int64(6 * 4 << 10)
	if sz != want {
		panic(fmt.Sprintf("want %v, got %v", want, sz)) // want 24576, got 4096
	}
}

// if file is sparse, can we locate the holes in it efficiently?

func Test004_apfs(t *testing.T) {
	demoAPFS()
}

func Test005_apfs(t *testing.T) {

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

		_, spansRead, err := FindSparseRegions(fd)
		panicOn(err)
		fd.Close()
		////vv("spansRead = '%v'", spansRead)
		if spansRead.Equal(spec) {
			////vv("good: spansRead Equal spec, on i=%v, path='%v'", i, path)
		} else {
			//vv("bad:  spansRead NOT Equal spec")
			panic("fix this")
		}
		os.Remove(path)
	}
}

func Test006_darwin_mixed_sparse_file(t *testing.T) {

	//spec := genSpecSparse(rng)

	// 32KB is not enough on Sonoma 14.0 with amd64 to get sparse file out.
	// But now that we have a 2nd hole punching pass, this works.
	spec := &SparseSpans{Slc: []SparseSpan{
		SparseSpan{IsHole: true, Beg: 0, Endx: 12288},      // (3 pages of 4KB)
		SparseSpan{IsHole: false, Beg: 12288, Endx: 32768}, // (5 pages of 4KB)
	}}

	path := fmt.Sprintf("test006.outpath.sparsefile")
	//vv("on path = '%v'; spec = '%v'", path, spec)

	var fd *os.File
	var err error

	// green
	// under
	// strace -f -e lseek go test -v -run 005
	os.Remove(path)

	fd, err = createSparseFileFromSparseSpans(path, spec, nil)
	panicOn(err)

	//vv("done with createSparseFileFromSparseSpans, on to FindSparseRegions")
	_, spansRead, err := FindSparseRegions(fd)
	panicOn(err)
	//vv("spansRead = '%v'", spansRead)
	if spansRead.Equal(spec) {
		//vv("good: spansRead Equal spec")
	} else {
		//vv("bad:  spansRead NOT Equal spec")
		panic("fix this")
	}
	os.Remove(path)
}

func Test007_small_file_not_sparse_file(t *testing.T) {

	spec := &SparseSpans{Slc: []SparseSpan{
		SparseSpan{IsHole: false, Beg: 0, Endx: 369},
	}}

	path := fmt.Sprintf("test007.outpath.small_not_sparsefile")
	//vv("on path = '%v'; spec = '%v'", path, spec)

	var fd *os.File
	var err error

	os.Remove(path)

	fd, err = createSparseFileFromSparseSpans(path, spec, nil)
	panicOn(err)

	//vv("done with createSparseFileFromSparseSpans, on to FindSparseRegions")
	_, spansRead, err := FindSparseRegions(fd)
	panicOn(err)
	//vv("spansRead = '%v'", spansRead)
	if spansRead.Equal(spec) {
		//vv("good: spansRead Equal spec")
	} else {
		//vv("bad:  spansRead NOT Equal spec")
		panic("fix this")
	}
	os.Remove(path)
}

func Test008_pre_allocated_file(t *testing.T) {

	var mb64 int64 = 1 << 26
	spec := &SparseSpans{Slc: []SparseSpan{
		SparseSpan{IsHole: false, IsUnwrittenPrealloc: true, Beg: 0, Endx: mb64},
	}}

	path := fmt.Sprintf("test008.outpath.pre_allocated")
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

	_, err = SparseFileSize(fd)
	panicOn(err) // no such device or address [recovered, repanicked]

	//vv("SparseFileSize returned: isSparse=%v; disksz=%v; statsz=%v; err=%v", isSparse, disksz, statsz, err)

	_, spansRead, err := FindSparseRegions(fd)
	panicOn(err)
	//vv("spansRead = '%v'", spansRead)
	if spansRead.Equal(spec) {
		//vv("good: spansRead Equal spec")
	} else {
		//vv("bad:  spansRead NOT Equal spec")
		panic("fix this")
	}

	// and we should be able to copy a pre-allocated file.
	path2 := path + ".copy"
	os.Remove(path2)
	defer os.Remove(path2)

	err = CopySparseFile(path, path2)
	panicOn(err)

	err = SparseDiff(path, path2)
	panicOn(err)

}

func Test009_copy_sparse(t *testing.T) {

	var seed [32]byte
	rng := newPRNG(seed)

	for i := range 10 {

		spec := genSpecSparse(rng)

		path := fmt.Sprintf("test009.outpath.%02d.sparsefile", i)
		//vv("on i = %v, path = '%v'; spec = '%v'", i, path, spec)
		path2 := path + ".copy"

		var fd *os.File
		var err error
		os.Remove(path)
		os.Remove(path2)

		fd, err = createSparseFileFromSparseSpans(path, spec, rng)
		panicOn(err)
		fd.Sync()

		err = CopySparseFile(path, path2)
		panicOn(err)

		err = SparseDiff(path, path2)
		panicOn(err)

		os.Remove(path)
		os.Remove(path2)
	}
}

// copy of 008 but fill in a prefix of actual data.
func Test010_pre_allocated_file_some_data(t *testing.T) {

	// maybe darwin hates doing a pre-alloc not from Beg: 0.
	// invalid argument'/'0x16' back from
	// unix.FcntlFstore(fd.Fd(), int(unix.F_PREALLOCATE), store)
	// inside fallocate in sparse_darwin.go.
	// So we'll just write some stuff below.
	var mb64 int64 = 1 << 26
	spec := &SparseSpans{Slc: []SparseSpan{
		SparseSpan{IsHole: false, IsUnwrittenPrealloc: true, Beg: 0, Endx: mb64},
	}}

	path := fmt.Sprintf("test010.pre_allocated.some_data")
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

	_, err = SparseFileSize(fd)
	panicOn(err) // no such device or address [recovered, repanicked]

	//vv("SparseFileSize returned: isSparse=%v; disksz=%v; statsz=%v; err=%v", isSparse, disksz, statsz, err)

	_, spansRead, err := FindSparseRegions(fd)
	panicOn(err)
	//vv("spansRead = '%v'", spansRead)
	if spansRead.Equal(spec) {
		//vv("good: spansRead Equal spec")
	} else {
		//vv("bad:  spansRead NOT Equal spec")
		panic("fix this")
	}

	// write a little data
	data := make([]byte, 4096)
	rng := newPRNG([32]byte{})
	rng.cha8.Read(data)

	_, err = fd.Seek(0, 0)
	panicOn(err)
	_, err = fd.Write(data)
	panicOn(err)
	panicOn(fd.Sync())

	// and we should be able to copy a pre-allocated file.
	path2 := path + ".copy"
	os.Remove(path2)
	defer os.Remove(path2)

	err = CopySparseFile(path, path2)
	panicOn(err)

	err = SparseDiff(path, path2)
	panicOn(err)

}

func Test011_MinSparseHoleSize(t *testing.T) {
	path := "blah.out"
	fd, err := os.Create(path)
	panicOn(err)
	defer fd.Close()
	defer os.Remove(path)

	mn, err := MinSparseHoleSize(fd)
	panicOn(err)
	_ = mn

	fsname, err := FileSystemType(fd)
	panicOn(err)

	fmt.Printf("MinSparseHoleSize = %v, on '%v'\n", mn, fsname)
}

// similar to 009 but exercises GenerateSparseTestFiles() helper.
func Test022_copy_GenerateSparseTestFiles(t *testing.T) {

	dir := "dir.test.022"
	os.RemoveAll(dir)
	defer func() {
		r := recover()
		if r == nil {
			//os.RemoveAll(dir)
		} // else on error leave dir for inspection
	}()

	MustGenerateSparseTestFiles(dir, 0, 0)

	list, err := filepath.Glob(dir + "/*")
	panicOn(err)

	//vv("list = '%#v'", list)
	for i, path := range list {
		_ = i
		path2 := path + ".copy"
		//vv("i=%v, path='%v'; path2='%v'", i, path, path2)
		err = CopySparseFile(path, path2)
		panicOn(err)

		err = SparseDiff(path, path2)
		panicOn(err)
	}
}

// also 011 above.
func Test023_MinSparseHoleSize(t *testing.T) {

	path := "test.023.prealloc_file"
	os.Remove(path)
	defer os.Remove(path)
	fd, err := os.Create(path)
	panicOn(err)

	minHole, err := MinSparseHoleSize(fd)
	panicOn(err)
	if minHole < 4096 {
		panic(fmt.Sprintf("warning: minHole = %v < 4096; probably breaks alot of assumptions?", minHole))
	}
	vv("minHole = %v", minHole)
}
