//go:build !darwin
// +build !darwin

package jsync

func cloneFile(src, dst string) error {
	panic("TODO: implement cloneFile as hardlink on non darwin?")
}
