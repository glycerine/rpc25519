package bytes

import (
	"math/rand"
	"testing"
)

// run with go test -bench=. -benchmem

func generateTestData(size int, zeroSpanStart, zeroSpanLen int) []byte {
	data := make([]byte, size)
	// Fill with random non-zero bytes
	for i := range data {
		data[i] = byte(rand.Intn(254) + 1) // 1-255
	}
	// Insert zero span
	for i := 0; i < zeroSpanLen && (i+zeroSpanStart) < len(data); i++ {
		data[zeroSpanStart+i] = 0
	}
	return data
}

func BenchmarkSIMDLongestZeroSpan(b *testing.B) {
	cases := []struct {
		name        string
		size        int
		spanStart   int
		spanLen     int
		description string
	}{
		{"small_start", 100, 0, 16, "16-byte span at start"},
		{"small_middle", 100, 42, 16, "16-byte span in middle"},
		{"small_end", 100, 84, 16, "16-byte span at end"},
		{"medium_start", 1000, 0, 64, "64-byte span at start"},
		{"medium_middle", 1000, 468, 64, "64-byte span in middle"},
		{"medium_end", 1000, 936, 64, "64-byte span at end"},
		{"large_start", 10000, 0, 256, "256-byte span at start"},
		{"large_middle", 10000, 4872, 256, "256-byte span in middle"},
		{"large_end", 10000, 9744, 256, "256-byte span at end"},
		{"worst_case", 10000, 0, 0, "no zeros"},
		{"best_case", 10000, 0, 10000, "all zeros"},
	}

	for _, tc := range cases {
		data := generateTestData(tc.size, tc.spanStart, tc.spanLen)

		b.Run("SIMD_"+tc.name, func(b *testing.B) {
			b.SetBytes(int64(tc.size))
			for i := 0; i < b.N; i++ {
				SIMDLongestZeroSpan(data)
			}
		})

		b.Run("Naive_"+tc.name, func(b *testing.B) {
			b.SetBytes(int64(tc.size))
			for i := 0; i < b.N; i++ {
				LongestZeroSpan(data)
			}
		})
	}
}

// Verify both implementations return the same results
func TestLongestZeroSpanMatchesNaive(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	cases := []struct {
		size      int
		spanStart int
		spanLen   int
	}{
		{100, 0, 16},
		{100, 42, 16},
		{100, 84, 16},
		{1000, 0, 64},
		{1000, 468, 64},
		{1000, 936, 64},
		{10000, 0, 256},
		{10000, 4872, 256},
		{10000, 9744, 256},
	}

	for i, tc := range cases {
		data := generateTestData(tc.size, tc.spanStart, tc.spanLen)

		simdStart, simdLen := SIMDLongestZeroSpan(data)
		naiveStart, naiveLen := LongestZeroSpan(data)

		if simdStart != naiveStart || simdLen != naiveLen {
			t.Errorf("Case %d: SIMD(%d,%d) != Naive(%d,%d)",
				i, simdStart, simdLen, naiveStart, naiveLen)
		}
	}

	// Also test some random cases
	for i := 0; i < 100; i++ {
		size := rng.Intn(10000) + 100
		spanStart := rng.Intn(size)
		spanLen := rng.Intn(size - spanStart)

		data := generateTestData(size, spanStart, spanLen)

		simdStart, simdLen := SIMDLongestZeroSpan(data)
		naiveStart, naiveLen := LongestZeroSpan(data)

		if simdStart != naiveStart || simdLen != naiveLen {
			t.Errorf("Random case %d: SIMD(%d,%d) != Naive(%d,%d)",
				i, simdStart, simdLen, naiveStart, naiveLen)
		}
	}
}
