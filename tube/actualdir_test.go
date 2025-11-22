package tube

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestActualDir(t *testing.T) {

	// --- Example Setup ---
	// Create a temporary environment:
	tempDir, err := os.MkdirTemp("", "actualdir-test")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tempDir) // Clean up

	// 1. Create the actual directory
	targetDir := filepath.Join(tempDir, "target-dir")
	if err := os.Mkdir(targetDir, 0755); err != nil {
		panic(err)
	}

	// 2. Create the actual file inside the target directory
	targetFilePath := filepath.Join(targetDir, "data.txt")
	if err := os.WriteFile(targetFilePath, []byte("hello"), 0644); err != nil {
		panic(err)
	}

	// 3. Create a symlink to the actual directory
	symlinkDir := filepath.Join(tempDir, "link-dir")
	if err := os.Symlink(targetDir, symlinkDir); err != nil {
		panic(err)
	}

	// 4. Create a path using the symlink
	linkFilePath := filepath.Join(symlinkDir, "data.txt")

	// --- Core Logic ---
	// Open the file using the symlinked path.
	f, err := os.Open(linkFilePath)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// Call the function
	actualDirPath, err := getActualParentDirForFsync(linkFilePath)
	panicOn(err)
	if strings.HasSuffix(actualDirPath, "/target-dir") {
		// good
	} else {
		vv("actualDirPath = '%v'", actualDirPath)
		panicf("expected actualDirPath '%v' to end in /target-dir", actualDirPath)
	}
}
