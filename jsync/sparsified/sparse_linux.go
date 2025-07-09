//go:build linux

package sparsified

import (
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
	// TODO: fiemap
	//fibmap "github.com/glycerine/go-fibmap"
)

// used by fileop_test.go, defined here for portability.
// fileop_darwin.go has its own, different, definition.
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

/*
   Deallocating file space
       Specifying the FALLOC_FL_PUNCH_HOLE flag (available since Linux 2.6.38)
       in mode deallocates space (i.e., creates a  hole)  in  the  byte  range
       starting  at offset and continuing for len bytes.  Within the specified
       range, partial filesystem  blocks  are  zeroed,  and  whole  filesystem
       blocks  are removed from the file.  After a successful call, subsequent
       reads from this range will return zeroes.

       The FALLOC_FL_PUNCH_HOLE flag must be ORed with FALLOC_FL_KEEP_SIZE  in
       mode;  in  other words, even when punching off the end of the file, the
       file size (as reported by stat(2)) does not change.

       Not all  filesystems  support  FALLOC_FL_PUNCH_HOLE;  if  a  filesystem
       doesn't  support the operation, an error is returned.  The operation is
       supported on at least the following filesystems:

       *  XFS (since Linux 2.6.38)

       *  ext4 (since Linux 3.0)

       *  Btrfs (since Linux 3.7)

       *  tmpfs(5) (since Linux 3.5)

*/

/*
   Collapsing file space
       Specifying the FALLOC_FL_COLLAPSE_RANGE  flag  (available  since  Linux
       3.15) in mode removes a byte range from a file, without leaving a hole.
       The byte range to be collapsed starts at offset and continues  for  len
       bytes.   At  the  completion of the operation, the contents of the file
       starting at the location offset+len will be appended  at  the  location
       offset, and the file will be len bytes smaller.

       A filesystem may place limitations on the granularity of the operation,
       in order to ensure efficient implementation.  Typically, offset and len
       must  be  a multiple of the filesystem logical block size, which varies
       according to the filesystem type and configuration.   If  a  filesystem
       has such a requirement, fallocate() fails with the error EINVAL if this
       requirement is violated.

       If the region specified by offset plus len reaches or passes the end of
       file,  an  error  is  returned; instead, use ftruncate(2) to truncate a
       file.

       No other flags may be  specified  in  mode  in  conjunction  with  FAL‐
       LOC_FL_COLLAPSE_RANGE.

       As  at  Linux 3.15, FALLOC_FL_COLLAPSE_RANGE is supported by ext4 (only
       for extent-based files) and XFS.
*/
// How do I make an extent-based file on ext4?
/*
A by LLM:

On ext4, files are automatically extent-based by default
since ext4 was introduced - you don't need to do anything
special. The extent feature was one of the major
improvements from ext3 to ext4.

However, if you want to verify that your ext4 filesystem
has extent support enabled:

tune2fs -l /dev/your_device | grep extent

example output, shows extent:

Filesystem features:      has_journal ext_attr resize_inode dir_index filetype needs_recovery extent 64bit flex_bg sparse_super large_file huge_file dir_nlink extra_isize metadata_csum

You should see "extent" in the features list.

Check if a specific file is using extents:

$ filefrag -v your_file

If it shows "ext" in the output, it's using extents.

example output:

$ filefrag -v out.db
Filesystem type is: ef53
File size of out.db is 1 (1 block of 4096 bytes)
 ext:     logical_offset:        physical_offset: length:   expected: flags:
   0:        0..       0:   80912788..  80912788:      1:             last,eof
out.db: 1 extent found
$

For an existing filesystem, extents can be enabled with:

$ tune2fs -O extent /dev/your_device

Important notes:
* All modern ext4 filesystems enable extents by default
* Files created on ext4 automatically use extents unless:
  + The filesystem was upgraded from ext3 without enabling extents
  + The file was created before extents were enabled
  + The filesystem was mounted with noextent option (very rare)

So for your fallocate with FALLOC_FL_COLLAPSE_RANGE operation,
any newly created file on a modern ext4 filesystem will support it by default.
*/

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

		// Truncate helped on darwin. does Truncate help on linux? nope.
		/*if mode == 0 {
			err = file.Truncate(offset + length)
			panicOn(err)
			//vv("err = '%v'/'%#v'; offset=%v; length=%v; offset+length=%v", err, err, offset, length, offset+length)
		}*/
		// looks fine on disk afterwards... just needs a sync? did not help.
		//file.Sync()

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

// before writing new stuff into the extent, we got:
/*
	jaten@rog ~/go/src/github.com/glycerine/yogadb $ xfs_info /mnt/a
	meta-data=/dev/sda               isize=512    agcount=8, agsize=268435455 blks
	         =                       sectsz=512   attr=2, projid32bit=1
	         =                       crc=1        finobt=1 spinodes=0 rmapbt=0
	         =                       reflink=0
	data     =                       bsize=4096   blocks=1953506646, imaxpct=5
	         =                       sunit=0      swidth=0 blks
	naming   =version 2              bsize=4096   ascii-ci=0 ftype=1
	log      =internal               bsize=4096   blocks=521728, version=2
	         =                       sectsz=512   sunit=0 blks, lazy-count=1
	realtime =none                   extsz=4096   blocks=0, rtextents=0

*/

// filefrag shows that the fiemap is more accurate
// than our Stat in cases for total file size. prefer
// that on linux. should be faster too since just
// one syscall per file; instead of many per file.

/* old linux specific
func createSparseFileFromSpans(path string, spans *Spans) (fd *os.File, err error) {
	if len(spans.Slc) == 0 {
		panic(fmt.Sprintf("spans.Slc was empty"))
	}

	//if fileExists(path) {
	fd, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	panicOn(err)

	var offset int64
	// should not be needed?? try without?
	// offset, err := fd.Seek(0, 0)
	// panicOn(err)
	// if offset != 0 {
	// 	panic(fmt.Sprintf("could not seek to start of path '%v'", path))
	// }

	for i, sp := range spans.Slc {
		_ = i
		sz := sp.Endx - sp.Beg

		if sp.IsUnwrittenPrealloc {
			// gives Size:0 Blocks:32 (*512 = 16384 our goal size)
			// (what seaweedfs does to pre-allocate)
			// du -sh test005.outpath.00.sparsefile
			// 16K    test005.outpath.00.sparsefile
			//
			// ls -alh test005.outpath.00.sparsefile
			// 0B Jun 24 18:36 test005.outpath.00.sparsefile

			//$ filefrag -v test005.outpath.00.sparsefile
			//Filesystem type is: ef53
			//File size of test005.outpath.00.sparsefile is 0
			//                          (0 blocks of 4096 bytes)
			// ext:     logical_offset:        physical_offset: length:   expected: flags:
			//   0:        0..       3:  117276700.. 117276703:      4:             last,unwritten,eof
			// note that due to a bug in filefrag, the following
			// extent count will be wrong for adjacent but distinct extents.
			// So count the ext from the detailed FIE_MAP above,
			// and ignore the count on the final line like this:
			//test005.outpath.00.sparsefile: 1 extent found

			// this fallocate with FALLOC_FL_KEEP_SIZE gives pre-allocated
			//  but unwritten, which is
			// technically different from a sparse hole which is
			// not allocated, but logically the same: a span of logical zeros
			// that should be compressed away rather than stored.
			got, err := fallocate(fd, FALLOC_FL_KEEP_SIZE, offset, sz)
			panicOn(err)
			//vv("did fallocate(KEEP_SIZE) offset=%v, sz=%v; err=%v", offset, sz, err)
			_ = got
		} else if sp.IsHole {
			err = fd.Truncate(sp.Endx)
			panicOn(err)
			//vv("in sp.IsHole=true, did fd.Truncate(%v) ok.", sp.Endx)

			got, err := fallocate(fd, FALLOC_FL_PUNCH_HOLE, offset, sz)
			_ = got
			panicOn(err)
			//vv("did fallocate(KEEP_SIZE) offset=%v, sz=%v; err=%v", offset, sz, err)
		} else {
			// regular data. we don't want sparse, nor unwritten but pre-allocated.
			got, err := fallocate(fd, 0, offset, sz)
			panicOn(err)
			//vv("did fallocate(0) offset=%v, sz=%v; err=%v", offset, sz, err)
			_ = got

			offset2, err := fd.Seek(offset, 0)
			panicOn(err)
			if offset2 != offset {
				panic("wat?")
			}
			for j := int64(0); j < sz; j += 4096 {
				//vv("from offset=%v (page %v) writing to page j/4096 = %v", offset, offset/4096, j/4096)
				_, err = fd.Write(oneZeroBlock4k[:])
				panicOn(err)
			}
		}
		offset += sz

		//got, err = fallocate(fd, FALLOC_FL_PUNCH_HOLE, offset, length)
		//got, err = fallocate(fd, FALLOC_FL_INSERT_RANGE, offset, length)
		//err = fd.Truncate(sz)
		//panicOn(err)
		//_ = got
	}
	fd.Sync()
	return
}
*/

// adapt the mac version
// can also make non-sparse files.
func createSparseFileFromSpans(path string, spans *Spans, rng *prng) (fd *os.File, err error) {
	//vv("top createSparseFileFromSpans path='%v'", path)
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

	haveSparse := false
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
			haveSparse = true
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

	if false { // don't need on linux?
		// possible 2nd pass needed to make small sparse files on APFS
		if haveSparse && maxSz < PunchBelowBytes {
			////vv("2nd pass, hole punching")
			for _, sp := range spans.Slc {

				offset := sp.Beg
				sz := sp.Endx - sp.Beg

				switch {
				case sp.IsUnwrittenPrealloc:
				case sp.IsHole:
					got, err := Fallocate(fd, FALLOC_FL_PUNCH_HOLE, offset, sz)
					_ = got
					panicOn(err)
					////vv("did Fallocate(fd, FALLOC_FL_PUNCH_HOLE, offset=%v, sz=%v) -> err=%v; got = %v", offset, sz, err, got)

				default:
					// regular data.
				}
			}
		}
	}

	//if !didTruncateMax {
	//	fd.Truncate(maxSz)
	//}

	//fd.Sync()
	return
}

/*
jaten@rog ~/sparsified (master) $ sudo strace filefrag -v test008.outpath.pre_allocated
execve("/usr/sbin/filefrag", ["filefrag", "-v", "test008.outpath.pre_allocated"], 0x7ffe55f427d0 //15 vars// ) = 0
brk(NULL)                               = 0x5e97afa45000
mmap(NULL, 8192, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0) = 0x7a3cd35b5000
access("/etc/ld.so.preload", R_OK)      = -1 ENOENT (No such file or directory)
openat(AT_FDCWD, "/etc/ld.so.cache", O_RDONLY|O_CLOEXEC) = 3
fstat(3, {st_mode=S_IFREG|0644, st_size=117459, ...}) = 0
mmap(NULL, 117459, PROT_READ, MAP_PRIVATE, 3, 0) = 0x7a3cd3598000
close(3)                                = 0
openat(AT_FDCWD, "/lib/x86_64-linux-gnu/libc.so.6", O_RDONLY|O_CLOEXEC) = 3
read(3, "\177ELF\2\1\1\3\0\0\0\0\0\0\0\0\3\0>\0\1\0\0\0\220\243\2\0\0\0\0\0"..., 832) = 832
pread64(3, "\6\0\0\0\4\0\0\0@\0\0\0\0\0\0\0@\0\0\0\0\0\0\0@\0\0\0\0\0\0\0"..., 784, 64) = 784
fstat(3, {st_mode=S_IFREG|0755, st_size=2125328, ...}) = 0
pread64(3, "\6\0\0\0\4\0\0\0@\0\0\0\0\0\0\0@\0\0\0\0\0\0\0@\0\0\0\0\0\0\0"..., 784, 64) = 784
mmap(NULL, 2170256, PROT_READ, MAP_PRIVATE|MAP_DENYWRITE, 3, 0) = 0x7a3cd3200000
mmap(0x7a3cd3228000, 1605632, PROT_READ|PROT_EXEC, MAP_PRIVATE|MAP_FIXED|MAP_DENYWRITE, 3, 0x28000) = 0x7a3cd3228000
mmap(0x7a3cd33b0000, 323584, PROT_READ, MAP_PRIVATE|MAP_FIXED|MAP_DENYWRITE, 3, 0x1b0000) = 0x7a3cd33b0000
mmap(0x7a3cd33ff000, 24576, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_FIXED|MAP_DENYWRITE, 3, 0x1fe000) = 0x7a3cd33ff000
mmap(0x7a3cd3405000, 52624, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_FIXED|MAP_ANONYMOUS, -1, 0) = 0x7a3cd3405000
close(3)                                = 0
mmap(NULL, 12288, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0) = 0x7a3cd3595000
arch_prctl(ARCH_SET_FS, 0x7a3cd3595740) = 0
set_tid_address(0x7a3cd3595a10)         = 1029435
set_robust_list(0x7a3cd3595a20, 24)     = 0
rseq(0x7a3cd3596060, 0x20, 0, 0x53053053) = 0
mprotect(0x7a3cd33ff000, 16384, PROT_READ) = 0
mprotect(0x5e97ad77b000, 4096, PROT_READ) = 0
mprotect(0x7a3cd35f3000, 8192, PROT_READ) = 0
prlimit64(0, RLIMIT_STACK, NULL, {rlim_cur=8192*1024, rlim_max=RLIM64_INFINITY}) = 0
munmap(0x7a3cd3598000, 117459)          = 0
openat(AT_FDCWD, "test008.outpath.pre_allocated", O_RDONLY) = 3
fstat(3, {st_mode=S_IFREG|0600, st_size=67108864, ...}) = 0
fstatfs(3, {f_type=EXT2_SUPER_MAGIC, f_bsize=4096, f_blocks=424223951, f_bfree=89843218, f_bavail=68275386, f_files=107823104, f_ffree=84342267, f_fsid={val=[0x18923806, 0x1618697]}, f_namelen=255, f_frsize=4096, f_flags=ST_VALID|ST_RELATIME}) = 0
ioctl(3, FIGETBSZ, 0x5e97ad77c044)      = 0
fstat(1, {st_mode=S_IFCHR|0620, st_rdev=makedev(0x88, 0x8), ...}) = 0
getrandom("\xd2\xf9\x35\x8a\x13\x77\xa4\x91", 8, GRND_NONBLOCK) = 8
brk(NULL)                               = 0x5e97afa45000
brk(0x5e97afa66000)                     = 0x5e97afa66000
write(1, "Filesystem type is: ef53\n", 25Filesystem type is: ef53
) = 25
ioctl(3, FS_IOC_GETFLAGS, [FS_EXTENT_FL]) = 0
write(1, "File size of test008.outpath.pre"..., 84File size of test008.outpath.pre_allocated is 67108864 (16384 blocks of 4096 bytes)
) = 84
ioctl(3, FS_IOC_FIEMAP, {fm_start=0, fm_length=18446744073709551615, fm_flags=0, fm_extent_count=292} => {fm_flags=0, fm_mapped_extents=1, ...}) = 0
write(1, " ext:     logical_offset:       "..., 77 ext:     logical_offset:        physical_offset: length:   expected: flags:
) = 77
write(1, "   0:        0..   16383:  19716"..., 89   0:        0..   16383:  197165056.. 197181439:  16384:             last,unwritten,eof
) = 89
write(1, "test008.outpath.pre_allocated: 1"..., 46test008.outpath.pre_allocated: 1 extent found
) = 46
close(3)                                = 0
exit_group(0)                           = ?
+++ exited with 0 +++
jaten@rog ~/sparsified (master) $
*/

// func LinuxFindSparseRegions(f *os.File) (spans *Spans, err error) {
// 	m := fibmap.NewFibmapFile(f)
// 	bsz, errnoFigetbsz := m.Figetbsz()
// 	//vv("bsz = %v; errnoFigetbsz = %v", errnoFigetbsz)
// 	var sz uint32 = 300
// 	extents, errno := m.Fiemap(sz)
// 	//vv("Fiemap errno = %v", errno)
// 	//vv("extents = '%#v'", extents)
// }

// MinSparseHoleSize returns the minimum hole size
// for a sparse file on Linux.
// This corresponds to the underlying filesystem's block size.
// The value is obtained via fstat(2), as Linux does not support the
// _PC_MIN_HOLE_SIZE pathconf extension that Darwin does.
//
// Returns 4096 on ext4 and XFS, same as APFS/darwin on my laptop.
func MinSparseHoleSize(fd *os.File) (val int, err error) {
	if fd == nil {
		return -1, fmt.Errorf("nil fd passed to MinSparseHoleSize")
	}

	fi, err := fd.Stat()
	if err != nil {
		return -1, err
	}
	stat := fi.Sys().(*syscall.Stat_t)
	return int(stat.Blksize), nil
}
