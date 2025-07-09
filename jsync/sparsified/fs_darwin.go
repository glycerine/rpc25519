package sparsified

import (
	"fmt"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

// A map of common filesystem magic numbers to their string names.
// You can add more as needed from <linux/magic.h> or other sources.
var fsMagicMap = map[int64]string{
	0xEF53:     "ext4/ext3/ext2",
	0x58465342: "xfs",
	0x6969:     "nfs",
	0x4244:     "btrfs",
	0x5346544e: "ntfs",
	0x4d44:     "fat",
	0x137D:     "apfs", // Value for APFS from Darwin sources
}

// FileSystemType determines and returns the filesystem type for a given file.
func FileSystemType(f *os.File) (string, error) {
	if f == nil {
		return "", fmt.Errorf("file is nil")
	}

	// The unix.Statfs_t struct holds the filesystem statistics.
	var statfs unix.Statfs_t

	// unix.Statfs takes the file's path.
	err := unix.Statfs(f.Name(), &statfs)
	if err != nil {
		return "", fmt.Errorf("statfs failed: %w", err)
	}

	// The Type field contains the filesystem's magic number.
	fsTypeMagic := int64(statfs.Type)

	// Look up the magic number in our map.
	fsName, found := fsMagicMap[fsTypeMagic]
	if !found {
		// On Darwin, the filesystem name is often in the Fstypename field.
		// We check this as a fallback.
		// The field is a fixed-size char array, so we must convert it carefully.
		var fsNameDarwin string
		if len(statfs.Fstypename) > 0 {
			// Find the null terminator
			end := 0
			for end < len(statfs.Fstypename) && statfs.Fstypename[end] != 0 {
				end++
			}
			fsNameDarwin = string(statfs.Fstypename[:end])
		}

		if fsNameDarwin != "" {
			fsName = strings.ToUpper(fsNameDarwin)
		} else {
			fsName = fmt.Sprintf("Unknown (Magic: 0x%X)", fsTypeMagic)
		}
	}

	//fmt.Printf("File '%s' is on a '%s' filesystem.\n", f.Name(), fsName)
	return fsName, nil
}
