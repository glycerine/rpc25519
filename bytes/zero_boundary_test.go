//go:build !wasm

package bytes

import "testing"

func TestAllZeroSIMDBoundaries(t *testing.T) {
	if !AllZeroSIMD(nil) {
		t.Fatal("nil slice must be all zero")
	}
	for offset := 0; offset < 32; offset++ {
		for size := 0; size <= 257; size++ {
			storage := make([]byte, offset+size+32)
			for i := range storage {
				storage[i] = 255
			}
			data := storage[offset : offset+size : offset+size]
			clear(data)
			if !AllZeroSIMD(data) {
				t.Fatalf("zero slice: offset=%d size=%d", offset, size)
			}
			for pos := range data {
				data[pos] = 128 // Also detect bytes with only their high bit set.
				if AllZeroSIMD(data) {
					t.Fatalf("missed nonzero: offset=%d size=%d pos=%d", offset, size, pos)
				}
				data[pos] = 0
			}
		}
	}
}

func TestSIMDLongestZeroSpanBoundaries(t *testing.T) {
	for offset := 0; offset < 16; offset++ {
		for size := 0; size <= 129; size++ {
			data := make([]byte, offset+size)[offset:]
			for pos := 0; pos <= size; pos++ {
				clear(data)
				if pos < size {
					data[pos] = 128
				}
				wantStart, wantLen := LongestZeroSpan(data)
				gotStart, gotLen := SIMDLongestZeroSpan(data)
				if gotStart != wantStart || gotLen != wantLen {
					t.Fatalf("offset=%d size=%d pos=%d: got (%d,%d), want (%d,%d)", offset, size, pos, gotStart, gotLen, wantStart, wantLen)
				}
			}
		}
	}
}
