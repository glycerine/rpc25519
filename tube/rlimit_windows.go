//go:build windows
// +build windows

package tube

func raiseFileHandleLimit(target uint64) {
	// no-op on windows. haven't ported this yet.
	return nil
}
