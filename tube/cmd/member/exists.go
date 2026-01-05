package main

import (
	"io"
	"os"
	"path/filepath"
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

func fileSize(name string) (int64, error) {
	fi, err := os.Stat(name)
	if err != nil {
		return -1, err
	}
	return fi.Size(), nil
}

func removeFiles(pattern string) {
	matches, err := filepath.Glob(pattern)
	panicOn(err)
	for _, filePath := range matches {
		os.Remove(filePath)
	}
}

func copyFile(dst, src string) error {
	sourceFile, err := os.Open(src)
	panicOn(err)
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	panicOn(err)
	defer destFile.Close()

	// Copy the contents from source to destination.
	_, err = io.Copy(destFile, sourceFile)
	panicOn(err)
	return nil
}
