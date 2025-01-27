//go:build darwin
// +build darwin

package main

/*
#include <errno.h>
extern int renamex_np(const char *old, const char *new, unsigned int flags);
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

/*
import (
	"fmt"
	"syscall"
)

const (
	_RENAME_SWAP = 0x2 // Value for Darwin's RENAME_SWAP flag
)

//go:noescape
//go:nosplit
func renamex_np(oldpath string, newpath string, flags uint) (err syscall.Errno)

// Assembly implementation in rename_darwin_amd64.s or rename_darwin_arm64.s

func atomicDirSwap(oldpath, newpath string) error {
	if err := renamex_np(oldpath, newpath, _RENAME_SWAP); err != 0 {
		return fmt.Errorf("renamex_np failed: %s", err)
	}
	return nil
}

*/
