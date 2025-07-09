//go:build windows

package sparsified

/*
(Windows not implemented yet)

notes:

"There’s some extra work required to use sparse files on Windows. You must first create an empty file, set the FSCTL_SET_SPARSE attribute on it, and close the file pointer. You then need to re-open a file pointer to the file before you can write any data to it sparsely.

It’s not a hugely complicated process, but the added complexity is enough to cause many programs to not bother supporting sparse files on Windows. Notably, closing file handlers is slow on Windows because it triggers indexing and virus scans

On FreeBSD, Linux, MacOS, and Solaris; you can check on a file’s sparseness using the ls -lsk test.file command and some arithmetic. The commands return a couple of columns; the first contains the sector allocation count (the on-disk storage size in blocks) and the sixth column returns the apparent file size in bytes. Take the first number and multiply it by 1024. The file is sparse (or compressed by the file system!) if the resulting number is lower than the sector allocation count.

me on darwin:
$ ls -lsk test003.sparse

sector allocation count       apparent file size in bytes
|                             |
v                             v
0 -rw-------  1 jaten  staff  4096 Apr 14 21:42 test003.sparse

what ls -k is doing here:
     -k      This has the same effect as setting environment variable
             BLOCKSIZE to 1024, except that it also nullifies any -h options
             to its left.


The above method is easy to memorize and is portable across all file systems and operating system (except Windows). On Windows, you can check whether a file is sparse using the fsutil sparse queryflag test.file command.

You can also get these numbers everywhere (except Windows) with the du (disk usage) command. However, its arguments and capabilities are different on each operating system, so it’s more difficult to memorize.

Recent versions of FreeBSD, Linux, MacOS, and Solaris include an API that can detect sparse “holes” in files. This is the SEEK_HOLE extension to the lseek function, detailed in its man page. https://www.man7.org/linux/man-pages/man2/lseek.2.html

I built a small program using the above API. My sparseseek program scans files and lists how much of it is stored as spares/holes and how much is data. (I initially named it sparsehole and didn’t notice the problem before reading it aloud.)
https://codeberg.org/da/sparseseek/src/branch/main/sparseseek.c


 -- https://www.ctrl.blog/entry/sparse-files.html
*/

// from package sparsecat/sparse_windows.go
// original: https://github.com/svenwiltink/sparsecat

/*
MIT License

Copyright (c) 2021 Sven Wiltink

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

import (
	"errors"
	"golang.org/x/sys/windows"
	"io"
	"os"
	"syscall"
	"unsafe"
)

const (
	queryAllocRanges = 0x000940CF
	setSparse        = 0x000900c4
)

// detectDataSection detects the start and end of the next section containing data. This
// skips any sparse sections.
func detectDataSection(file *os.File, offset int64) (start int64, end int64, err error) {
	// typedef struct _FILE_ALLOCATED_RANGE_BUFFER {
	//  LARGE_INTEGER FileOffset;
	//  LARGE_INTEGER Length;
	//} FILE_ALLOCATED_RANGE_BUFFER, *PFILE_ALLOCATED_RANGE_BUFFER;
	type allocRangeBuffer struct{ offset, length int64 }

	// TODO(sven): prevent this stat call
	s, err := file.Stat()
	if err != nil {
		return 0, 0, err
	}

	queryRange := allocRangeBuffer{offset, s.Size()}
	allocRanges := make([]allocRangeBuffer, 1)

	var bytesReturned uint32
	err = windows.DeviceIoControl(
		windows.Handle(file.Fd()), queryAllocRanges,
		(*byte)(unsafe.Pointer(&queryRange)), uint32(unsafe.Sizeof(queryRange)),
		(*byte)(unsafe.Pointer(&allocRanges[0])), uint32(len(allocRanges)*int(unsafe.Sizeof(allocRanges[0]))),
		&bytesReturned, nil,
	)

	if err != nil {
		if !errors.Is(err, syscall.ERROR_MORE_DATA) {
			panic(err)
		}
	}

	// no error and nothing returned, assume EOF
	if bytesReturned == 0 {
		return 0, 0, io.EOF
	}

	return allocRanges[0].offset, allocRanges[0].offset + allocRanges[0].length, nil
}

func supportsSeekHole(f *os.File) bool {
	return true
}

func SparseTruncate(file *os.File, size int64) error {
	err := windows.DeviceIoControl(
		windows.Handle(file.Fd()), setSparse,
		nil, 0,
		nil, 0,
		nil, nil,
	)

	if err != nil {
		return err
	}

	err = file.Truncate(size)
	if err != nil {
		return nil
	}
	return err
}
