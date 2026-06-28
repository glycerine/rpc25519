//go:build windows

package selfcert

import (
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"strings"
)

// ownerOnly removes inherited broad ACLs and grants access only to the current
// user. This keeps Windows-created SSH keys acceptable to OpenSSH and Cygwin.
func ownerOnly(path string) error {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat %q: %w", path, err)
	}
	newPerm := fileInfo.Mode().Perm() &^ os.FileMode(0o077)
	if err := os.Chmod(path, newPerm); err != nil {
		return fmt.Errorf("chmod %q: %w", path, err)
	}

	rights := "(R,W)"
	if fileInfo.IsDir() {
		rights = "(OI)(CI)(F)"
	}
	return lockWindowsACL(path, rights)
}

func sshPublicKeyPerm(path string) error {
	return ownerOnly(path)
}

func lockWindowsACL(path, rights string) error {
	principal, err := currentWindowsPrincipal()
	if err != nil {
		return err
	}
	if err := runICACLS(path, "/inheritance:r", "/grant:r", principal+":"+rights); err != nil {
		return err
	}
	return runICACLS(path, "/remove:g",
		"*S-1-1-0",      // Everyone
		"*S-1-5-11",     // Authenticated Users
		"*S-1-5-32-545", // Users
	)
}

func currentWindowsPrincipal() (string, error) {
	u, err := user.Current()
	if err != nil {
		return "", fmt.Errorf("find current user: %w", err)
	}
	if strings.HasPrefix(u.Uid, "S-") {
		return "*" + u.Uid, nil
	}
	if u.Username != "" {
		return u.Username, nil
	}
	return "", fmt.Errorf("current user has no Windows SID or username")
}

func runICACLS(path string, args ...string) error {
	cmdArgs := append([]string{path}, args...)
	cmd := exec.Command("icacls.exe", cmdArgs...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("icacls %s: %w: %s", strings.Join(cmdArgs, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}
