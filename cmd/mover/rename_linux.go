//go:build linux
// +build linux

package main

import (
	"fmt"
	"syscall"
)

// Linux-specific constants
const (
	_RENAME_EXCHANGE = 0x2
)

func atomicDirReplace(oldpath, newpath string) error {
	// Get file descriptors for the parent directories
	oldDir, oldName := filepath.Split(oldpath)
	newDir, newName := filepath.Split(newpath)

	oldFd, err := syscall.Open(oldDir, syscall.O_DIRECTORY, 0)
	if err != nil {
		return fmt.Errorf("failed to open old directory: %w", err)
	}
	defer syscall.Close(oldFd)

	newFd, err := syscall.Open(newDir, syscall.O_DIRECTORY, 0)
	if err != nil {
		return fmt.Errorf("failed to open new directory: %w", err)
	}
	defer syscall.Close(newFd)

	// Perform the atomic rename
	err = syscall.Renameat2(oldFd, oldName, newFd, newName, _RENAME_EXCHANGE)
	if err != nil {
		return fmt.Errorf("renameat2 failed: %w", err)
	}

	return nil
}
