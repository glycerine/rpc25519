//go:build !amd64 && !wasm

package bytes

// These x86 features are unavailable on other architectures.
const (
	x86HasAVX2   = false
	x86HasAVX512 = false
)
