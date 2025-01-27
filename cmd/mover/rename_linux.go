//go:build linux
// +build linux

package main

import (
	"fmt"
	"syscall"
)

const (
	_RENAME_EXCHANGE = 0x2
)

//go:noescape
func renameat2(olddirfd int, oldpath string, newdirfd int, newpath string, flags uint) (err syscall.Errno)

func atomicDirSwap(oldpath, newpath string) error {
	if err := renameat2(-100, oldpath, -100, newpath, _RENAME_EXCHANGE); err != 0 {
		return fmt.Errorf("renameat2 failed: %s", err)
	}
	return nil
}
