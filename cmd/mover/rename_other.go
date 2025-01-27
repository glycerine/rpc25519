//go:build !linux && !darwin
// +build !linux,!darwin

package main

import "fmt"

func atomicDirReplace(oldpath, newpath string) error {
	return fmt.Errorf("atomic directory replacement not supported on this platform.")
}
