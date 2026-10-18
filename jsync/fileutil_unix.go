//go:build unix

package jsync

import (
	"fmt"
	"os"
	"os/user"
	"strconv"
	"sync"
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
	//vv("updateLinkModTime path = '%v', modtm = '%v'", path, modtm)
	tv := unix.NsecToTimeval(modtm.UnixNano())
	unix.Lutimes(path, []unix.Timeval{tv, tv})
}

// Indirection lets transfer tests model the source host's group database
// independently of the receiving host's real group database.
var lookupFileGroupID = user.LookupGroupId

func getFileGroupAndID(fi os.FileInfo) (fileGroup string, gid uint32) {
	if stat_t, ok := fi.Sys().(*syscall.Stat_t); ok {
		gid = stat_t.Gid

		fileGroup = ownershipGroups.groupname(gid)
	}
	return
}

func getFileOwnerAndID(fi os.FileInfo) (fileOwner string, uid uint32) {

	if stat_t, ok := fi.Sys().(*syscall.Stat_t); ok {
		uid = stat_t.Uid

		fileOwner = ownershipUsers.username(uid)
	}
	return
}

// Carry the username as well as the source IDs so different hosts may use
// different UIDs and GIDs for the same accounts.
func fileOwnership(fi os.FileInfo) *FileOwnership {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return &FileOwnership{UID: st.Uid, GID: st.Gid, Username: ownershipUsers.username(st.Uid), Groupname: ownershipGroups.groupname(st.Gid)}
	}
	return nil
}

// Return the special mode bits to clear when an unknown account falls back
// to UID/GID zero. Never grant setuid/setgid through a fallback.
func restoreOwnership(path string, owner *FileOwnership) (os.FileMode, error) {
	if owner == nil || os.Geteuid() != 0 {
		return 0, nil
	}
	// Lchown also handles dangling symlinks without changing their targets.
	uid, knownUser := ownershipUsers.destinationUID(owner.Username)
	gid, knownGroup := ownershipGroups.destinationGID(owner.Groupname)
	var clearBits os.FileMode
	if !knownUser {
		clearBits |= os.ModeSetuid
	}
	if !knownGroup {
		clearBits |= os.ModeSetgid
	}
	return clearBits, os.Lchown(path, uid, gid)
}

// These small, bounded caches avoid account database lookups per file. Cache
// misses too: unknown users are common when copying system trees. Lookups and
// cache resets share a lock because transfers use multiple file workers.
const ownershipCacheLimit = 256

type ownershipUserMap struct {
	mu         sync.Mutex
	byID       map[uint32]string
	byName     map[string]resolvedOwner
	lookupID   func(string) (*user.User, error)
	lookupName func(string) (*user.User, error)
}

type resolvedOwner struct {
	uid   int
	known bool
}

var ownershipUsers = ownershipUserMap{lookupID: user.LookupId, lookupName: user.Lookup}

func (m *ownershipUserMap) username(uid uint32) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if name, ok := m.byID[uid]; ok {
		return name
	}
	name := ""
	if u, err := m.lookupID(strconv.FormatUint(uint64(uid), 10)); err == nil && u != nil {
		name = u.Username
	}
	if m.byID == nil || len(m.byID) >= ownershipCacheLimit {
		m.byID = make(map[uint32]string)
	}
	m.byID[uid] = name
	return name
}

func (m *ownershipUserMap) destinationUID(name string) (int, bool) {
	// Never fall back to a source UID: it might belong to someone else here.
	if name == "" {
		return 0, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if owner, ok := m.byName[name]; ok {
		return owner.uid, owner.known
	}
	owner := resolvedOwner{}
	if u, err := m.lookupName(name); err == nil && u != nil {
		// Some Unix account databases (e.g. macOS nobody) spell uint32 UIDs as
		// negative numbers. Exclude -1, which chown treats as "do not change".
		if n, err := strconv.ParseInt(u.Uid, 10, 64); err == nil && n >= -2147483648 && n < 4294967295 && n != -1 {
			owner = resolvedOwner{uid: int(uint32(n)), known: true}
		}
	}
	if m.byName == nil || len(m.byName) >= ownershipCacheLimit {
		m.byName = make(map[string]resolvedOwner)
	}
	m.byName[name] = owner
	return owner.uid, owner.known
}

// Group lookups use the same bounded, synchronized caching policy as users.
// Keep source-ID and destination-name maps separate: the same group name can
// have a different numeric ID on each host.
type ownershipGroupMap struct {
	mu         sync.Mutex
	byID       map[uint32]string
	byName     map[string]resolvedGroup
	lookupID   func(string) (*user.Group, error)
	lookupName func(string) (*user.Group, error)
}

type resolvedGroup struct {
	gid   int
	known bool
}

var ownershipGroups = ownershipGroupMap{
	lookupID:   func(id string) (*user.Group, error) { return lookupFileGroupID(id) },
	lookupName: user.LookupGroup,
}

func (m *ownershipGroupMap) groupname(gid uint32) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if name, ok := m.byID[gid]; ok {
		return name
	}
	name := ""
	if g, err := m.lookupID(strconv.FormatUint(uint64(gid), 10)); err == nil && g != nil {
		name = g.Name
	}
	if m.byID == nil || len(m.byID) >= ownershipCacheLimit {
		m.byID = make(map[uint32]string)
	}
	m.byID[gid] = name
	return name
}

func (m *ownershipGroupMap) destinationGID(name string) (int, bool) {
	if name == "" {
		return 0, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if group, ok := m.byName[name]; ok {
		return group.gid, group.known
	}
	group := resolvedGroup{}
	if g, err := m.lookupName(name); err == nil && g != nil {
		// Account databases can spell uint32 group IDs as negative numbers too.
		if n, err := strconv.ParseInt(g.Gid, 10, 64); err == nil && n >= -2147483648 && n < 4294967295 && n != -1 {
			group = resolvedGroup{gid: int(uint32(n)), known: true}
		}
	}
	if m.byName == nil || len(m.byName) >= ownershipCacheLimit {
		m.byName = make(map[string]resolvedGroup)
	}
	m.byName[name] = group
	return group.gid, group.known
}
