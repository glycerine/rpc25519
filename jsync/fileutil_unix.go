//go:build unix

package jsync

import (
	"fmt"
	"os"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

func updateLinkModTime(path string, modtm time.Time) {
	//vv("updateLinkModTime path = '%v', modtm = '%v'", path, modtm)
	tv := unix.NsecToTimeval(modtm.UnixNano())
	unix.Lutimes(path, []unix.Timeval{tv, tv})
}

func loadLocalAccountDB() (*accountDB, error) {
	passwd, err := os.Open("/etc/passwd")
	if err != nil {
		return nil, err
	}
	defer passwd.Close()
	users, err := parseAccountMap(passwd, 7)
	if err != nil {
		return nil, fmt.Errorf("read /etc/passwd: %w", err)
	}
	group, err := os.Open("/etc/group")
	if err != nil {
		return nil, err
	}
	defer group.Close()
	groups, err := parseAccountMap(group, 4)
	if err != nil {
		return nil, fmt.Errorf("read /etc/group: %w", err)
	}
	return newAccountDB(users, groups), nil
}

// Collect numeric IDs without per-file account lookups or allocations.
func fileOwnership(fi os.FileInfo) (bool, uint32, uint32) {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return true, st.Uid, st.Gid
	}
	return false, 0, 0
}

func privilegedReceiver() bool { return os.Geteuid() == 0 }

// Return the special mode bits to clear when an unknown account falls back
// to UID/GID zero. Never grant setuid/setgid through a fallback.
func restoreOwnership(path string, conveyed bool, sourceUID, sourceGID uint32, accounts *OwnershipAccounts) (os.FileMode, error) {
	if !conveyed || !privilegedReceiver() {
		return 0, nil
	}
	if accounts == nil {
		return 0, fmt.Errorf("ownership metadata without negotiated account maps")
	}
	db, err := localAccounts()
	if err != nil {
		return 0, err
	}
	uid, knownUser := db.destinationUID(accounts.Users[int64(sourceUID)])
	gid, knownGroup := db.destinationGID(accounts.Groups[int64(sourceGID)])
	var clearBits os.FileMode
	if !knownUser {
		clearBits |= os.ModeSetuid
	}
	if !knownGroup {
		clearBits |= os.ModeSetgid
	}
	return clearBits, os.Lchown(path, uid, gid)
}
