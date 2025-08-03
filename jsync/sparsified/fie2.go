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
	//"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

const extentBatchCount = 292

// debug version
//const extentBatchCount = 1

// See if one system call will suffice most of the
// time by guessing less than extentBatchCount spans the first time.
// See if this is any better at pre-allocated reporting.
// The linux filefrag utility uses 292 spans, so we are
// just duplicating their heuristic.
//
// TODO: how do we know when there are MORE than the guessed
// extentBatchCount spans, and how do we get the rest? Have to look
// for the FIEMAP_EXTENT_LAST flag, or not (more available).
func FieMap2(fd *os.File) (spans *SparseSpans, err error) {

	// to avoid an extra disk access, filefrag
	// juse uses 18446744073709551615 as the
	// max size in the ioctl FIE_MAP call. We could
	// do the same... TODO?
	sum, err := SparseFileSize(fd)
	isSparse, disksz, statsz := sum.IsSparse, sum.DiskSize, sum.StatSize
	if err != nil {
		return nil, err
	}
	_ = isSparse
	if statsz <= 0 && disksz <= 0 {
		return
	}

	/* need this back to handle
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
	*/

	sz := statsz
	maxsz := sz
	if disksz > sz {
		maxsz = disksz
	}
	vv("sz = '%v'; vs disksz = %v; maxsz = %v", sz, disksz, maxsz)
	// INVAR: maxsz > 0 because we return above otherwise.

	reqExtentCount := extentBatchCount // guess will be less than

	// Compute total size needed for the
	// fiemap struct and the flexible extent array that follows.
	fiemapSize := unsafe.Sizeof(C.struct_fiemap{}) + (uintptr(reqExtentCount) * unsafe.Sizeof(C.struct_fiemap_extent{}))

	// Allocate a byte slice of sufficient size.
	fiemapBytes := make([]byte, fiemapSize)

	// Cast (convert) the byte slice to a Fiemap pointer
	// to do C-style flexible array member structs.
	fiemapData := (*C.struct_fiemap)(unsafe.Pointer(&fiemapBytes[0]))

	// Populate the struct for the real call.
	fiemapData.fm_start = 0                              // (in)
	fiemapData.fm_length = C.__u64(maxsz)                // (in)
	fiemapData.fm_flags = 0                              // (in/out)
	fiemapData.fm_extent_count = C.__u32(reqExtentCount) // (in)
	fiemapData.fm_mapped_extents = 0                     // (out)

	fdint := uintptr(fd.Fd())
	ioctlCallCount := 0

	// Call ioctl to get some actual extent data.
	_, _, errno := unix.Syscall(
		unix.SYS_IOCTL,
		fdint,
		uintptr(C.FS_IOC_FIEMAP),
		uintptr(unsafe.Pointer(fiemapData)),
	)
	ioctlCallCount++
	if errno != 0 {
		err = errno
		err = fmt.Errorf("error ioctl(FS_IOC_FIEMAP) errno != 0: file could be empty? err='%v' (file sz = %v; disksz = %v) errno==22: %v", err, sz, disksz, errno == 22)
		return
	}

	gotExtentCount := fiemapData.fm_mapped_extents

	vv("look for holes in path: %v (Size: %v bytes)\n", fd.Name(), sz)
	vv("asked for up to reqExtentCount = %v", reqExtentCount)
	vv("Got back/found %v mapped extents.\n\n", gotExtentCount)

	if gotExtentCount == 0 {
		vv("entirely sparse file: no extents at all (so just one hole)")
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

	// INVAR: at least one extent
	spans = &SparseSpans{}

	var offset int64

	moreExtentsAvail := true // updated below

haveMoreExtents:
	for {

		vv("gotExtentCount = %v; sz = %v; errno = %#v; fiemapData = '%#v'", gotExtentCount, sz, errno, fiemapData)

		//vv("look for holes in path: %v (Size: %v bytes)\n", fd.Name(), sz)
		//vv("found %v mapped extents.\n\n", gotExtentCount)

		// To access the extents, we create a slice header pointing to the memory
		// right after the main Fiemap struct fields.
		extents := (*[1 << 30]C.struct_fiemap_extent)(unsafe.Pointer(
			uintptr(unsafe.Pointer(fiemapData)) + unsafe.Sizeof(C.struct_fiemap{}),
		))[:gotExtentCount:gotExtentCount]

		lastFlags := uint32(extents[gotExtentCount-1].fe_flags)
		vv("flags from last extent %v are %v", gotExtentCount-1, lastFlags)
		moreExtentsAvail = (lastFlags & FIEMAP_EXTENT_LAST) == 0
		vv("moreExtentsAvail = %v", moreExtentsAvail)

		for i, extent := range extents {
			vv("i=%v, extent = '%#v'", i, extent)

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
			if (extent.fe_flags & FIEMAP_EXTENT_UNWRITTEN) != 0 {
				//vv("Unwritten extent (hole) at offset %v with length %v bytes.\n", logical, extent.fe_length)
				span.IsUnwrittenPrealloc = true
			}
			spans.Slc = append(spans.Slc, span)

			offset = logical + extlen // both in bytes
		} // end for extents

		if moreExtentsAvail {
			// re-fill, starting at offset
			fiemapData.fm_start = C.__u64(offset)                // (in)
			fiemapData.fm_length = C.__u64(maxsz)                // (in)
			fiemapData.fm_flags = 0                              // (in/out)
			fiemapData.fm_extent_count = C.__u32(reqExtentCount) // (in)
			fiemapData.fm_mapped_extents = 0                     // (out)
			// overwrite/re-use the space for extentBatchCount
			// extents that follows.

			// Call ioctl again to get some actual extent data.
			_, _, errno = unix.Syscall(
				unix.SYS_IOCTL,
				fdint,
				uintptr(C.FS_IOC_FIEMAP),
				uintptr(unsafe.Pointer(fiemapData)),
			)
			ioctlCallCount++
			if errno != 0 {
				err = errno
				err = fmt.Errorf("error ioctl(FS_IOC_FIEMAP) errno != 0: on call count %v: err='%v' (file sz = %v; disksz= %v) errno==22: %v", ioctlCallCount, err, sz, disksz, errno == 22)
				return
			}

			gotExtentCount = fiemapData.fm_mapped_extents

			vv("ioctlCallCount=%v looked for holes in path: %v (Size: %v bytes)\n", ioctlCallCount, fd.Name(), sz)
			vv("ioctlCallCount=%v asked for up to reqExtentCount = %v", ioctlCallCount, reqExtentCount)
			vv("ioctlCallCount=%v Got back/found %v mapped extents.\n\n", ioctlCallCount, gotExtentCount)

			if gotExtentCount == 0 {
				panic("should never happen, since have more extents!")
			}
			// loop to top to process them
			continue haveMoreExtents

		} // end if moreExtentsAvail
		break haveMoreExtents

	} // end for moreExtentsAvail

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
