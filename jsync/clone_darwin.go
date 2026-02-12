//go:build darwin
// +build darwin

package jsync

import (
	"golang.org/x/sys/unix"
)

// See clone_darwin.go for the
// rationale [from gemini] as to why hardlinks are so slow on MacOS.
//
// "On APFS, Hard Links are 'Siblings'.
// APFS does not use the simple reference count model. Instead,
// when you hard link File A to File B:
// APFS creates a new Inode for File B.
// It marks both Inodes as "Siblings" that share the same storage blocks.
// It updates a hidden "sibling link" map to ensure metadata
// updates (like chmod) propagate to all siblings.
//
// This 'Sibling' overhead is why Apple removed
// directory hard links in APFS and rewrote Time Machine
// to use Volume Snapshots instead.
//
// "2. Why clonefile is Faster
//
// "The clonefile syscall (Copy-On-Write) avoids the Sibling overhead.
// It creates a new Inode (like a hard link).
// It points to the same data blocks (like a hard link).
// Crucially: It does not link their metadata. The two files
// are immediately independent.
// Because APFS doesn't have to maintain the 'Sibling'
// relationship or ensure that a chmod on one reflects on the
// other, clonefile skips the expensive bookkeeping that link performs."
// See clone_darwin.go cloneFile().
//
// "Option B: The "Apple Native" Way (Volume Snapshots)
// If you want the truest instant snapshot on macOS (like
// Time Machine), you shouldn't use file-level linking at all.
// You should use APFS Volume Snapshots (fs_snapshot_create).
// Pros: O(1) for the entire database, regardless of size or
// file count. Instant.
// Cons: Extremely complex to implement; requires root/entitlements"

func cloneFile(src, dst string) error {
	// fast, atomic, copy-on-write clone
	// 0 is the flags argument (currently reserved, must be 0)
	return unix.Clonefile(src, dst, 0)
}
