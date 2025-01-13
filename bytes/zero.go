package bytes

import "github.com/templexxx/cpu"

// CPU feature detection flags
var (
	x86HasSSE2   = cpu.X86.HasSSE2
	x86HasAVX2   = cpu.X86.HasAVX2
	x86HasAVX512 = cpu.X86.HasAVX512F
)

// AllZero reports whether b consists entirely of zero bytes.
//
//go:noescape
func AllZero(b []byte) bool

// Internal implementations for different CPU features
//
//go:noescape
func allZeroSSE2(b []byte) bool

//go:noescape
func allZeroAVX2(b []byte) bool

//go:noescape
func allZeroAVX512(b []byte) bool
