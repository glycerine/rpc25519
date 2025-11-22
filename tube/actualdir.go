package tube

import (
	"fmt"
	"path/filepath"
)

// getActualDirForFsync resolves symlinks and
// opens the actual containing directory
// of the file for fsyncing its metadata.
func getActualParentDirForFsync(path string) (actualParentPath string, err error) {

	absPath, err1 := filepath.Abs(path)
	if err1 != nil {
		return "", fmt.Errorf("getActualDirForFsync: filepath.Abs(path='%v') error: '%v'", path, err1)
	}

	absParent := filepath.Dir(absPath)

	actualParentPath, err = filepath.EvalSymlinks(absParent)
	if err != nil {
		return "", fmt.Errorf("getActualDirForFsync: filepath.EvalSymlinks(absParent='%v') error: '%v'", absParent, err)
	}
	return
}
