package bytes

// AllZeroSIMD reports whether b consists entirely of zero bytes.
// It uses ARM64 NEON instructions, available on all ARM64 CPUs.
//
//go:noescape
func AllZeroSIMD(b []byte) bool
