package jsync

import (
	"os"
	"time"
)

func updateLinkModTime(path string, modtm time.Time) {
	//tv := unix.NsecToTimeval(modtm)
	//unix.Lutimes(path, []unix.Timeval{tv, tv})
}

func fileOwnership(fi os.FileInfo) (bool, uint32, uint32) { return false, 0, 0 }
func privilegedReceiver() bool                            { return false }
func restoreOwnership(path string, conveyed bool, uid, gid uint32, accounts *OwnershipAccounts) (os.FileMode, error) {
	return 0, nil
}

func loadLocalAccountDB() (*accountDB, error) { return newAccountDB(nil, nil), nil }
