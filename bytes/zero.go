package bytes

import "github.com/templexxx/cpu"

// CPU feature detection flags
var (
	x86HasSSE2   = cpu.X86.HasSSE2
	x86HasAVX2   = cpu.X86.HasAVX2
	x86HasAVX512 = cpu.X86.HasAVX512F
)

// AllZero reports whether b consists entirely of zero bytes.
// This simple version is on-par with the assembly, so
// typically we prefer it for portability.
func AllZero(b []byte) bool {
	for i := range b {
		if b[i] != 0 {
			return false
		}
	}
	return true
}

// AllZeroSIMD reports whether b consists entirely of zero bytes.
// It uses assembly, which may or may not be faster.
//
//go:noescape
func AllZeroSIMD(b []byte) bool

// Internal implementations for different CPU features
//
//go:noescape
func allZeroSSE2(b []byte) bool

//go:noescape
func allZeroAVX2(b []byte) bool

//go:noescape
func allZeroAVX512(b []byte) bool

// LongestZeroSpan returns the start index and length of the longest continuous
// span of zero bytes in the given slice. If no zeros are found, returns (0, 0).
// If multiple spans of the same longest length exist, returns the first one.
func SIMDLongestZeroSpan(b []byte) (start, length int) {
	if len(b) == 0 {
		return 0, 0
	}

	var (
		currentStart = 0
		currentLen   = 0
		maxStart     = 0
		maxLen       = 0
	)

	// Process the slice looking for spans of zeros
	for i := 0; i < len(b); {
		// If we find a zero byte, try to extend the span using SIMD
		if b[i] == 0 {
			if currentLen == 0 {
				currentStart = i
			}

			// Try to find the end of this zero span using SIMD
			remainingSlice := b[i:]
			j := 0
			for j < len(remainingSlice) {
				// Try chunks of increasing size
				var chunkSize int
				switch {
				case len(remainingSlice[j:]) >= 64 && x86HasAVX512:
					chunkSize = 64
				case len(remainingSlice[j:]) >= 32 && x86HasAVX2:
					chunkSize = 32
				case len(remainingSlice[j:]) >= 16:
					chunkSize = 16
				default:
					chunkSize = 1
				}

				chunk := remainingSlice[j : j+chunkSize]
				if !AllZeroSIMD(chunk) {
					// Found a non-zero byte, need to find exactly where
					for k := 0; k < chunkSize; k++ {
						if chunk[k] != 0 {
							j += k
							break
						}
					}
					break
				}
				j += chunkSize
			}

			// Update current span length
			currentLen = j
			i += j

			// If we hit the end of the slice or a non-zero byte
			if currentLen > maxLen {
				maxLen = currentLen
				maxStart = currentStart
			}
			currentLen = 0
		} else {
			i++
			if currentLen > maxLen {
				maxLen = currentLen
				maxStart = currentStart
			}
			currentLen = 0
		}
	}

	// Handle case where the longest span ends at slice end
	if currentLen > maxLen {
		maxLen = currentLen
		maxStart = currentStart
	}

	return maxStart, maxLen
}

// LongestZeroSpan is a simple byte-by-byte implementation for benchmarking.
// This naive version is on par with the SIMD version above.
// To see on your machine, run: go test -bench=. -benchmem
func LongestZeroSpan(b []byte) (start, length int) {
	if len(b) == 0 {
		return 0, 0
	}

	var (
		currentStart = 0
		currentLen   = 0
		maxStart     = 0
		maxLen       = 0
	)

	for i := range b {
		if b[i] == 0 {
			if currentLen == 0 {
				currentStart = i
			}
			currentLen++
		} else {
			if currentLen > maxLen {
				maxLen = currentLen
				maxStart = currentStart
			}
			currentLen = 0
		}
	}

	// Handle case where the longest span ends at slice end
	if currentLen > maxLen {
		maxLen = currentLen
		maxStart = currentStart
	}

	return maxStart, maxLen
}
