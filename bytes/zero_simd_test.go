package bytes

import (
	"math/rand"
	"strconv"
	"testing"
)

func TestAllZeroSIMD(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want bool
	}{
		{
			name: "empty slice",
			data: []byte{},
			want: true,
		},
		{
			name: "single zero",
			data: []byte{0},
			want: true,
		},
		{
			name: "single non-zero",
			data: []byte{1},
			want: false,
		},
		{
			name: "15 zeros",
			data: make([]byte, 15),
			want: true,
		},
		{
			name: "16 zeros (SSE2 size)",
			data: make([]byte, 16),
			want: true,
		},
		{
			name: "31 zeros",
			data: make([]byte, 31),
			want: true,
		},
		{
			name: "32 zeros (AVX2 size)",
			data: make([]byte, 32),
			want: true,
		},
		{
			name: "63 zeros",
			data: make([]byte, 63),
			want: true,
		},
		{
			name: "64 zeros (AVX512 size)",
			data: make([]byte, 64),
			want: true,
		},
		{
			name: "large buffer of zeros",
			data: make([]byte, 1024),
			want: true,
		},
	}

	// Add test cases with non-zero bytes at different positions
	sizes := []int{1, 15, 16, 31, 32, 63, 64, 1024}
	for _, size := range sizes {
		data := make([]byte, size)
		// Put non-zero byte at start
		data[0] = 1
		tests = append(tests, struct {
			name string
			data []byte
			want bool
		}{
			name: "non-zero at start of " + strconv.Itoa(size),
			data: data,
			want: false,
		})

		// Put non-zero byte at end
		data = make([]byte, size)
		data[size-1] = 1
		tests = append(tests, struct {
			name string
			data []byte
			want bool
		}{
			name: "non-zero at end of " + strconv.Itoa(size),
			data: data,
			want: false,
		})

		// Put non-zero byte in middle
		data = make([]byte, size)
		data[size/2] = 1
		tests = append(tests, struct {
			name string
			data []byte
			want bool
		}{
			name: "non-zero in middle of " + strconv.Itoa(size),
			data: data,
			want: false,
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := AllZeroSIMD(tt.data); got != tt.want {
				t.Errorf("AllZeroSIMD() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Benchmark different sizes to test different SIMD paths
func BenchmarkAllZeroSIMD(b *testing.B) {
	sizes := []int{16, 32, 64, 128, 256, 512, 1024, 4096}

	for _, size := range sizes {
		b.Run("size_"+strconv.Itoa(size), func(b *testing.B) {
			data := make([]byte, size)
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				AllZeroSIMD(data)
			}
		})

		// Also benchmark worst case (early exit on first non-zero byte)
		b.Run("size_"+strconv.Itoa(size)+"_early_exit", func(b *testing.B) {
			data := make([]byte, size)
			data[0] = 1
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				AllZeroSIMD(data)
			}
		})
	}
}

// Test random data to catch any edge cases
func TestAllZeroSIMDRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	for i := 0; i < 1000; i++ {
		size := rng.Intn(8192)
		data := make([]byte, size)

		// Randomly decide if this should be all zeros
		allZeros := rng.Float32() < 0.5
		if !allZeros {
			// Put a non-zero byte at a random position
			pos := rng.Intn(size)
			data[pos] = byte(rng.Intn(255) + 1)
		}

		result := AllZeroSIMD(data)
		if result != allZeros {
			t.Errorf("Random test failed: size=%d, expected=%v, got=%v", size, allZeros, result)
		}
	}
}
