//go:build !amd64 && !arm64 && !wasm

package bytes

// AllZeroSIMD falls back to the portable implementation on this architecture.
func AllZeroSIMD(b []byte) bool { return AllZero(b) }
