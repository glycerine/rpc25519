//go:build linux

package sparsified

/*
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <linux/fs.h>
#include <linux/fiemap.h>
*/
import "C"

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// fiemap flag values, from /usr/include/linux/fiemap.h

// Last extent in file.
const FIEMAP_EXTENT_LAST = 0x00000001

// Data location unknown.
const FIEMAP_EXTENT_UNKNOWN = 0x00000002

// Location still pending. Sets EXTENT_UNKNOWN.
const FIEMAP_EXTENT_DELALLOC = 0x00000004

// Data can not be read while fs is unmounted
const FIEMAP_EXTENT_ENCODED = 0x00000008

// Data is encrypted by fs. Sets EXTENT_NO_BYPASS.
const FIEMAP_EXTENT_DATA_ENCRYPTED = 0x00000080

// Extent offsets may not be block aligned.
const FIEMAP_EXTENT_NOT_ALIGNED = 0x00000100

// Data mixed with metadata. Sets EXTENT_NOT_ALIGNED.
const FIEMAP_EXTENT_DATA_INLINE = 0x00000200

// Multiple files in block. Sets EXTENT_NOT_ALIGNED.
const FIEMAP_EXTENT_DATA_TAIL = 0x00000400

// Space allocated, but no data (i.e. zero).
const FIEMAP_EXTENT_UNWRITTEN = 0x00000800

// File does not natively support extents. Result merged for efficiency.
const FIEMAP_EXTENT_MERGED = 0x00001000

// end <linux/fiemap.h> constants

// Space shared with other files.
const FIEMAP_EXTENT_SHARED = 0x00002000

//var fcntl64Syscall uintptr = unix.SYS_FCNTL

// used by sparse_test.go, defined here for portability.
// sparse_darwin.go has its own, different, definition.
const FALLOC_FL_PUNCH_HOLE = unix.FALLOC_FL_PUNCH_HOLE // = 2
// FALLOC_FL_ALLOCATE_RANGE = 0

// commannd 1 what seaweedfs does to pre-allocate space
// that is not used yet. Not sure--does this also result in a sparse
// file on linux, but maybe not on Mac??
const FALLOC_FL_KEEP_SIZE = unix.FALLOC_FL_KEEP_SIZE // = 1

// FALLOC_FL_KEEP_SIZE = 1
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
// "After a successful call, subsequent writes into
// the range specified by offset and len are guaranteed
// not to fail because of lack of disk space.
//
// "If the FALLOC_FL_KEEP_SIZE flag is specified in mode, the behavior of
// the call is similar, but the file size will not be changed even if off‐
// set+len is greater than the file size.  Preallocating zeroed blocks be‐
// yond the end of the file in this manner is useful for optimizing append
// workloads."

// Deallocating file space
//     Specifying the FALLOC_FL_PUNCH_HOLE flag (available since Linux 2.6.38)
//     in mode deallocates space (i.e., creates a  hole)  in  the  byte  range
//     starting  at offset and continuing for len bytes.  Within the specified
//     range, partial filesystem  blocks  are  zeroed,  and  whole  filesystem
//     blocks  are removed from the file.  After a successful call, subsequent
//     reads from this range will return zeroes.
//
//     The FALLOC_FL_PUNCH_HOLE flag must be ORed with FALLOC_FL_KEEP_SIZE  in
//     mode;  in  other words, even when punching off the end of the file, the
//     file size (as reported by stat(2)) does not change.
//
//     Not all  filesystems  support  FALLOC_FL_PUNCH_HOLE;  if  a  filesystem
//     doesn't  support the operation, an error is returned.  The operation is
//     supported on at least the following filesystems:
//
//     *  XFS (since Linux 2.6.38)
//     *  ext4 (since Linux 3.0)
//     *  Btrfs (since Linux 3.7)
//     *  tmpfs(5) (since Linux 3.5)

// Collapsing file space
//     Specifying the FALLOC_FL_COLLAPSE_RANGE  flag  (available  since  Linux
//     3.15) in mode removes a byte range from a file, without leaving a hole.
//     The byte range to be collapsed starts at offset and continues  for  len
//     bytes.   At  the  completion of the operation, the contents of the file
//     starting at the location offset+len will be appended  at  the  location
//     offset, and the file will be len bytes smaller.

//     A filesystem may place limitations on the granularity of the operation,
//     in order to ensure efficient implementation.  Typically, offset and len
//     must  be  a multiple of the filesystem logical block size, which varies
//     according to the filesystem type and configuration.   If  a  filesystem
//     has such a requirement, fallocate() fails with the error EINVAL if this
//     requirement is violated.

//     If the region specified by offset plus len reaches or passes the end of
//     file,  an  error  is  returned; instead, use ftruncate(2) to truncate a
//     file.

//     No other flags may be  specified  in  mode  in  conjunction  with  FAL‐
//     LOC_FL_COLLAPSE_RANGE.

//     As  at  Linux 3.15, FALLOC_FL_COLLAPSE_RANGE is supported by ext4 (only
//     for extent-based files) and XFS.

// How do I make an extent-based file on ext4?
//
// A by LLM:
//
// On ext4, files are automatically extent-based by default
// since ext4 was introduced - you don't need to do anything
// special. The extent feature was one of the major
// improvements from ext3 to ext4.
//
// However, if you want to verify that your ext4 filesystem
// has extent support enabled:
//
// tune2fs -l /dev/your_device | grep extent
//
// example output, shows extent:
//
// Filesystem features:      has_journal ext_attr resize_inode dir_index filetype needs_recovery extent 64bit flex_bg sparse_super large_file huge_file dir_nlink extra_isize metadata_csum
//
// You should see "extent" in the features list.
//
// Check if a specific file is using extents:
//
// $ filefrag -v your_file
//
// If it shows "ext" in the output, it's using extents.
//
// example output:
//
// $ filefrag -v out.db
// Filesystem type is: ef53
// File size of out.db is 1 (1 block of 4096 bytes)
//  ext:     logical_offset:        physical_offset: length:   expected: flags:
//    0:        0..       0:   80912788..  80912788:      1:             last,eof
// out.db: 1 extent found
// $
//
// For an existing filesystem, extents can be enabled with:
//
// $ tune2fs -O extent /dev/your_device
//
// Important notes:
// * All modern ext4 filesystems enable extents by default
// * Files created on ext4 automatically use extents unless:
//   + The filesystem was upgraded from ext3 without enabling extents
//   + The file was created before extents were enabled
//   + The filesystem was mounted with noextent option (very rare)
//
// So for your fallocate with FALLOC_FL_COLLAPSE_RANGE operation,
// any newly created file on a modern ext4 filesystem will support it by default.

func Fallocate(file *os.File, mode uint32, offset int64, length int64) (allocated int64, err error) {

	//vv("top of fallocate(mode=%v, offset=%v, length=%v); file='%v'", mode, offset, length, file.Name())

	if mode == FALLOC_FL_PUNCH_HOLE {
		mode |= unix.FALLOC_FL_KEEP_SIZE
	}

	var preSz, postSz int64
	_, _ = preSz, postSz
	preSz, err = fileSizeFromFile(file)
	panicOn(err)

	intfd := int(file.Fd())

	err = unix.Fallocate(intfd, mode, offset, length)
	//vv("unix.Fallocate(mode=%v) returned err = '%v'", mode, err) // "file too large"

	// seaweedfs does:
	// file, e := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	// syscall.Fallocate(int(file.Fd()), 1, 0, preallocate)
	// where
	// 1 means FALLOC_FL_KEEP_SIZE; i.e. leave as un-written pre-allocation.
	// 0 is the starting offset;
	// preallocate is the byte count to pre-allocate.

	// TODO: TEST this assumption! can we get a less-than full allocation and err==nil?
	if err == nil {

		postSz, err = fileSizeFromFile(file)
		panicOn(err)
		//vv("preSz = %v; postSz = %v", preSz, postSz)

		allocated = int64(length)
		return
	}
	if err.Error() == "file too large" {
		err = ErrFileTooLarge
		return
	}
	if allocated < length {
		err = ErrShortAlloc
	}
	// unknown error, just pass to caller.
	return
}

// adapted from the mac version.
// can also make non-sparse files.
func createSparseFileFromSparseSpans(path string, spans *SparseSpans, rng *prng) (fd *os.File, err error) {
	//vv("top createSparseFileFromSparseSpans path='%v'", path)
	nspan := len(spans.Slc)
	if nspan == 0 {
		panic(fmt.Sprintf("spans.Slc was empty"))
	}

	fd, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	panicOn(err)

	// two truncates to get a completely empty sparse file;
	// we are just emulating what darwin cp does to create sparseness
	// in the destination/target/output file.

	// first truncate to zero size.
	err = fd.Truncate(0)
	panicOn(err)

	// make sure changes are committed at the end.
	// So we don't get caught out on the wrong foot
	// like 018 before we added
	// the manual fd.Sync().
	defer fd.Sync()

	lastSpan := spans.Slc[nspan-1]
	maxSz := lastSpan.Endx
	if maxSz <= 0 {
		// empty file, we are done.
		return
	}

	// second truncate to maximum possible full (if non-sparse) size.
	// But, this will mess up the first pre-allocation! so make conditional.
	didTruncateMax := false
	if len(spans.Slc) > 0 && spans.Slc[0].IsUnwrittenPrealloc {
		//vv("first segment is pre-allocation, so skipping Truncate(maxSz)")
	} else {
		fd.Truncate(maxSz)
		////vv("did fd.Truncate to maxSz = %v", maxSz)
		didTruncateMax = true
	}
	_ = didTruncateMax

	//haveSparse := false
	for i, sp := range spans.Slc {
		_ = i

		offset := sp.Beg
		sz := sp.Endx - sp.Beg

		if sp.IsUnwrittenPrealloc && sp.IsHole {
			panic("cannot have sp.IsUnwrittenPrealloc && sp.IsHole")
		}

		switch {
		case sp.IsUnwrittenPrealloc:
			_, err = Fallocate(fd, FALLOC_FL_KEEP_SIZE, offset, sz)
			panicOn(err)
			//vv("Fallocate with FALLOC_FL_KEEP_SIZE offset=%v; sz=%v was ok.", offset, sz)
		case sp.IsHole:
			//haveSparse = true
		default:
			// regular data.
			offset2, err := fd.Seek(offset, 0)
			panicOn(err)
			if offset2 != offset {
				panic(fmt.Sprintf("wat? fd.Seek(offset=%v,0) returned offset2=%v, expected same.", offset, offset2))
			}
			for j := int64(0); j < sz; j += 4096 {
				////vv("from offset=%v (page %v) writing to page j/4096 = %v", offset, offset/4096, j/4096)

				data := oneZeroBlock4k[:]
				if rng != nil {
					data = databuf[:]
					rng.cha8.Read(data)
				}
				amt := int64(len(data))
				if j+4096 > sz {
					amt = sz - j
				}
				_, err = fd.Write(data[:amt])
				panicOn(err)
			}
		}
	}

	// we don't need the 2nd pass on linux like darwin needs
	// to get small sparse files.

	return
}

// MinSparseHoleSize returns the minimum hole size
// for a sparse file on Linux.
// This corresponds to the underlying filesystem's block size.
// The value is obtained via fstat(2), as Linux does not support the
// _PC_MIN_HOLE_SIZE pathconf extension that Darwin does.
//
// Returns 4096 on ext4 and XFS, same as APFS/darwin on my laptop.
func MinSparseHoleSize(fd *os.File) (val int64, err error) {
	if fd == nil {
		return -1_000_000_000, fmt.Errorf("nil fd passed to MinSparseHoleSize")
	}

	fi, err := fd.Stat()
	if err != nil {
		return -1_000_000_000, err
	}
	stat := fi.Sys().(*syscall.Stat_t)
	return stat.Blksize, nil
}

// FieMap asks for all spans back in one go, using
// at most 2 system ioctl calls. FieMap2 by contrast
// guesses that most files will be minimally sparse
// and so it allocates a bunch of sparse extent
// memory and passes it in on the first call, hoping
// to avoid a second system call, but with the risk
// that it could take many such batches (more than
// two batches if the extent count is > 2*292 for
// instance) for very fragmented/sparse files. This is
// the heuristic that the linux utility filefrag uses;
// we have just emulated it in FieMap2 in fie.go.
//
// We still need to benchmark to see which is actually faster.
func FieMap(fd *os.File) (spans *SparseSpans, err error) {

	return FieMap2(fd)

	isSparse, disksz, statsz, err := SparseFileSize(fd)
	if err != nil {
		return nil, err
	}
	_ = isSparse
	if statsz <= 0 && disksz <= 0 {
		return
	}
	sz := statsz
	maxsz := sz
	if disksz > sz {
		maxsz = disksz
	}
	vv("sz = '%v'; vs disksz = %v; maxsz = %v", sz, disksz, maxsz)
	// INVAR: maxsz > 0 b/c we return above otherwise.

	if false { // check again what prealloc and fiemap do
		if disksz > statsz {
			// we have pre-allocated some of the file.
			if !isSparse {
				spans = &SparseSpans{}
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
				return
			}
		}
	}

	fiemapCheck := &C.struct_fiemap{}
	fiemapCheck.fm_start = 0
	fiemapCheck.fm_length = C.__u64(maxsz)
	fiemapCheck.fm_flags = 0
	fiemapCheck.fm_extent_count = 0
	_ = fiemapCheck
	fdint := uintptr(fd.Fd())
	//res := C.ioctl(C.int(fd.Fd()), C.FS_IOC_FIEMAP, &fiemapCheck)
	//res := unix.FcntlInt(C.int(fd.Fd()), C.FS_IOC_FIEMAP, &fiemapCheck)
	_, _, errno := unix.Syscall(unix.SYS_IOCTL,
		fdint, uintptr(C.FS_IOC_FIEMAP), uintptr(unsafe.Pointer(fiemapCheck)))

	vv("disksz=%v; sz = %v; errno = %#v; fiemapCheck = '%#v'", disksz, sz, errno, fiemapCheck) // 0x16, -1, 0 if empty file

	if errno != 0 {
		err = errno
		if errno == 22 {
			err = fmt.Errorf("error file could be empty: errno==22 '%v' (file sz = %v)", err, sz)
		}
		return
	}

	if fiemapCheck.fm_mapped_extents == 0 {
		vv("entirely sparse file: no extents (so just one hole)")
		spans = &SparseSpans{
			Slc: []SparseSpan{
				SparseSpan{
					IsHole: true,
					Beg:    0,
					Endx:   maxsz,
				},
			},
		}
		return
	}
	spans = &SparseSpans{}

	extentCount := fiemapCheck.fm_mapped_extents

	// Calculate the total size needed for the
	// fiemap struct and the flexible extent array.
	fiemapSize := unsafe.Sizeof(C.struct_fiemap{}) + (uintptr(extentCount) * unsafe.Sizeof(C.struct_fiemap_extent{}))

	// Allocate a byte slice of the required size.
	fiemapBytes := make([]byte, fiemapSize)

	// Cast the byte slice to a Fiemap pointer.
	// This is the Go way of handling
	// C-style flexible array member structs.
	fiemapData := (*C.struct_fiemap)(unsafe.Pointer(&fiemapBytes[0]))

	// Populate the struct for the real call.
	fiemapData.fm_start = 0
	fiemapData.fm_length = C.__u64(maxsz)
	fiemapData.fm_flags = 0
	fiemapData.fm_extent_count = extentCount

	// Call ioctl again to get the actual extent data.
	_, _, errno = unix.Syscall(
		unix.SYS_IOCTL,
		fdint,
		uintptr(C.FS_IOC_FIEMAP),
		uintptr(unsafe.Pointer(fiemapData)),
	)
	if errno != 0 {
		fmt.Fprintf(os.Stderr, "ioctl(FS_IOC_FIEMAP) to get extents failed: %v\n", errno)
		os.Exit(1)
	}

	//vv("look for holes in path: %v (Size: %v bytes)\n", fd.Name(), sz)
	//vv("found %v mapped extents.\n\n", fiemapData.fm_mapped_extents)

	// To access the extents, we create a slice header pointing to the memory
	// right after the main Fiemap struct fields.
	extents := (*[1 << 30]C.struct_fiemap_extent)(unsafe.Pointer(
		uintptr(unsafe.Pointer(fiemapData)) + unsafe.Sizeof(C.struct_fiemap{}),
	))[:fiemapData.fm_mapped_extents:fiemapData.fm_mapped_extents]

	var offset int64

	for i, extent := range extents {
		vv("i=%v, extent = '%#v'", i, extent)
		// sparse_linux.go:464 2025-07-10 03:31:49.465 -0500 CDT i=0, extent = 'sparsified._Ctype_struct_fiemap_extent{fe_logical:0x0, fe_physical:0xede8000000, fe_length:0x1000, fe_reserved64:[2]sparsified._Ctype_ulonglong{0x0, 0x0}, fe_flags:0x801, fe_reserved:[3]sparsified._Ctype_uint{0x0, 0x0, 0x0}}'
		// so FIEMAP_EXTENT_UNWRITTEN
		// and FIEMAP_EXTENT_LAST
		// fe_flags are set.

		// Check for a hole between the end of
		// the last extent and the start of this one.
		logical := int64(extent.fe_logical)
		extlen := int64(extent.fe_length)

		if logical > offset {
			holeStart := offset
			holeLength := logical - offset
			//vv("Hole found at offset %v with length %v bytes.\n", holeStart, holeLength)

			span := SparseSpan{
				IsHole: true,
				Beg:    int64(holeStart),
				Endx:   int64(holeStart) + int64(holeLength),
				Flags:  uint32(extent.fe_flags),
			}
			spans.Slc = append(spans.Slc, span)
		}
		vv("on i = %v, logical(%v) <= offset(%v)", i, logical, offset)
		span := SparseSpan{
			IsHole: false,
			Beg:    logical,
			Endx:   logical + extlen,
			Flags:  uint32(extent.fe_flags),
		}
		// pre-allocated?
		if (extent.fe_flags & C.FIEMAP_EXTENT_UNWRITTEN) != 0 {
			vv("Unwritten extent (hole) at offset %v with length %v bytes.\n", logical, extent.fe_length)
			span.IsUnwrittenPrealloc = true
		}
		spans.Slc = append(spans.Slc, span)

		offset = logical + extlen // (extlen * 512)
	} // end for extents

	// hole at the end?
	//vv("after extents: sz = %v; offset = %v", sz, offset)
	if sz > offset {
		spans.Slc = append(spans.Slc, SparseSpan{
			IsHole: true,
			Beg:    offset,
			Endx:   sz,
		})
		offset = sz
	}

	// pre-alloc hole at the end?
	//vv("after extents: disksz = %v; offset = %v", disksz, offset)
	if disksz > offset {
		spans.Slc = append(spans.Slc, SparseSpan{
			IsHole: true,
			Beg:    offset,
			Endx:   disksz,
		})
	}

	return
}
