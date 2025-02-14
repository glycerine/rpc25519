package jsync

import (
	"os"
)

// returns empty string on error.
func getFileOwnerName(filepath string) string {
	// not implemented on windows yet.
	return ""
}

func updateLinkModTime(path string, modtm time.Time) {
	//tv := unix.NsecToTimeval(modtm)
	//unix.Lutimes(path, []unix.Timeval{tv, tv})
}
