//go:build linux
// +build linux

package main

import (
	"os"
	"syscall"
)

/*
#define _GNU_SOURCE
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <sys/syscall.h>

int my_renameat2(const char *oldpath, const char *newpath, unsigned int flags) {
    return syscall(SYS_renameat2, AT_FDCWD, oldpath, AT_FDCWD, newpath, flags);
}

int _linkat(int olddirfd, const char *oldpath, int newdirfd, const char *newpath, int flags) {
    return linkat(olddirfd, oldpath, newdirfd, newpath, flags);
}
*/
import "C"
import (
	"fmt"
)

const (
	_RENAME_EXCHANGE = 0x2
)

func atomicDirSwap(oldpath, newpath string) error {
	ret, err := C.my_renameat2(C.CString(oldpath), C.CString(newpath), _RENAME_EXCHANGE)
	if ret != 0 {
		return fmt.Errorf("renameat2 failed: %v", err)
	}
	return nil
}

func makeHardlink(origFilePath, newFilePath string) {
	origFilePathC := C.CString(origFilePath)
	newFilePathC := C.CString(newFilePath)

	result := C._linkat(C.AT_FDCWD, origFilePathC, C.AT_FDCWD, newFilePathC, 0)
	fmt.Println(result)
	if result < 0 {
		fmt.Print("errno: ")
		fmt.Println(syscall.Errno(-result))
		fmt.Println("Error creating hardlink")
		os.Exit(1)
	}
}
