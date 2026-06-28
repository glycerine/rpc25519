//go:build !windows

package selfcert

import (
	"fmt"
	"os"
)

// ownerOnly removes all group and other permissions from path.
func ownerOnly(path string) error {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat %q: %w", path, err)
	}
	newPerm := fileInfo.Mode().Perm() &^ os.FileMode(0o077)
	if err := os.Chmod(path, newPerm); err != nil {
		return fmt.Errorf("chmod %q: %w", path, err)
	}
	return nil
}

func sshPublicKeyPerm(path string) error {
	if err := os.Chmod(path, 0o644); err != nil {
		return fmt.Errorf("chmod %q: %w", path, err)
	}
	return nil
}
