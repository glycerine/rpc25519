//go:build unix

package jsync

import (
	"fmt"
	"os"
	"os/user"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// returns empty string on error.
func getFileOwnerName(filepath string) string {
	// Get file info
	fileInfo, err := os.Stat(filepath)
	if err != nil {
		return "" //, err
	}

	// Get system-specific file info
	stat := fileInfo.Sys()
	if stat == nil {
		return "" //, fmt.Errorf("no system-specific file info available")
	}

	// Get owner UID
	uid := stat.(*syscall.Stat_t).Uid

	// Look up user by UID
	owner, err := user.LookupId(fmt.Sprint(uid))
	if err != nil {
		return "" //, err
	}

	return owner.Username
}

func updateLinkModTime(path string, modtm time.Time) {
	tv := unix.NsecToTimeval(modtm.UnixNano())
	unix.Lutimes(path, []unix.Timeval{tv, tv})
}
