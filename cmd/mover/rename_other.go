//go:build !linux && !darwin
// +build !linux,!darwin

package main

import "fmt"

func atomicDirSwap(oldpath, newpath string) error {
	return fmt.Errorf("atomic directory swap not supported on this platform.")
}
