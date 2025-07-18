package rpc25519

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func fileExists(name string) bool {
	fi, err := os.Stat(name)
	if err != nil {
		return false
	}
	if fi.IsDir() {
		return false
	}
	return true
}

func dirExists(name string) bool {
	fi, err := os.Stat(name)
	if err != nil {
		return false
	}
	if fi.IsDir() {
		return true
	}
	return false
}

func fileSize(name string) (int64, error) {
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
func isWritable(path string) bool {
	if !fileExists(path) {
		return true
	}
	fileInfo, err := os.Stat(path)
	panicOn(err)

	// Get the file's mode (permission bits)
	mode := fileInfo.Mode()

	// Check write permission for owner, group, and others
	return mode&0222 != 0 // Write permission for any?
}

func copyFileDestSrc(topath, frompath string) (int64, error) {
	if !fileExists(frompath) {
		return 0, fs.ErrNotExist
	}

	src, err := os.Open(frompath)
	if err != nil {
		return 0, err
	}
	defer src.Close()

	dest, err := os.Create(topath)
	if err != nil {
		return 0, err
	}
	defer dest.Close()

	return io.Copy(dest, src)
}

func truncateFileToZero(path string) error {
	var perm os.FileMode
	f, err := os.OpenFile(path, os.O_TRUNC, perm)
	if err != nil {
		return fmt.Errorf("could not open file %q for truncation: %v", path, err)
	}
	if err = f.Close(); err != nil {
		return fmt.Errorf("could not close file handler for %q after truncation: %v", path, err)
	}
	return nil
}

func FileSizeModTime(name string) (sz int64, modTime time.Time, err error) {
	var fi os.FileInfo
	fi, err = os.Stat(name)
	if err != nil {
		return
	}
	return fi.Size(), fi.ModTime(), nil
}

func homed(path string) string {
	home := os.Getenv("HOME")
	return strings.Replace(path, "~", home, 1)
}

func removeAllFilesWithPrefix(prefixPath string) {
	matches, err := filepath.Glob(prefixPath + "*")
	if err != nil {
		return
	}
	//vv("matches = '%#v'", matches)
	for _, m := range matches {
		//vv("would delete '%v'", m)
		os.Remove(m)
	}
}
