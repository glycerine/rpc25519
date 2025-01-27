//go:build darwin
// +build darwin

package main

import (
	"os"
	"syscall"
)

/*
#include <fcntl.h>
#include <unistd.h>

extern int renamex_np(const char *old, const char *new, unsigned int flags);

int _linkat(int olddirfd, const char *oldpath, int newdirfd, const char *newpath, int flags) {
    return linkat(olddirfd, oldpath, newdirfd, newpath, flags);
}
*/
import "C"
import "fmt"

func atomicDirSwap(oldpath, newpath string) error {
	ret, err := C.renamex_np(C.CString(oldpath), C.CString(newpath), 2)
	if ret != 0 {
		return fmt.Errorf("renamex_np failed: %v", err)
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
