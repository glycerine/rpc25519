package jsync

import (
	"os"
	"time"
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

func getFileGroupAndID(fi os.FileInfo) (fileGroup string, gid uint32) {
	// if stat_t, ok := fi.Sys().(*syscall.Stat_t); ok {
	// 	gid = stat_t.Gid

	// 	group, err := user.LookupGroupId(fmt.Sprint(gid))
	// 	if err == nil && group != nil {
	// 		fileGroup = group.Name
	// 	}
	// }
	return
}

func getFileOwnerAndID(fi os.FileInfo) (fileOwner string, uid uint32) {

	// if stat_t, ok := fi.Sys().(*syscall.Stat_t); ok {
	// 	uid = stat_t.Uid

	// 	owner, err := user.LookupId(fmt.Sprint(uid))
	// 	if err == nil && owner != nil {
	// 		fileOwner = owner.Username
	// 	}
	// }
	return
}
