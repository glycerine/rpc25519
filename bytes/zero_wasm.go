//go:build wasm

package bytes

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
