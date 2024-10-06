package main

import (
	"os"
)

func FileExists(name string) bool {
	fi, err := os.Stat(name)
	if err != nil {
		return false
	}
	if fi.IsDir() {
		return false
	}
	return true
}

func DirExists(name string) bool {
	fi, err := os.Stat(name)
	if err != nil {
		return false
	}
	if fi.IsDir() {
		return true
	}
	return false
}

func FileSize(name string) (int64, error) {
	fi, err := os.Stat(name)
	if err != nil {
		return -1, err
	}
	return fi.Size(), nil
}

// IsWritable returns true if the file
// does not exist. Otherwise it checks
// the write bits. If any write bits
// (owner, group, others) are set, then
// we return true. Otherwise false.
// We panic if os.Stat() gives us an error.
func IsWritable(path string) bool {
	if !FileExists(path) {
		return true
	}
	fileInfo, err := os.Stat(path)
	if err != nil {
		panic(err)
	}

	// Get the file's mode (permission bits)
	mode := fileInfo.Mode()

	// Check write permission for owner, group, and others
	return mode&0222 != 0 // Write permission for any?
}
