//go:build darwin
// +build darwin

package main

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

func atomicDirReplace(oldpath, newpath string) error {
	if err := renamex_np(oldpath, newpath, _RENAME_SWAP); err != 0 {
		return fmt.Errorf("renamex_np failed: %s", err)
	}
	return nil
}
