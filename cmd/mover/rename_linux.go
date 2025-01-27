//go:build linux
// +build linux

package main

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
