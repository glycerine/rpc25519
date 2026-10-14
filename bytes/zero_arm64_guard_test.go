//go:build darwin && arm64

package bytes

import (
	"os"
	"syscall"
	"testing"
)

// Place slices against inaccessible pages to detect assembly reads outside them.
func TestAllZeroSIMDGuardPages(t *testing.T) {
	page := os.Getpagesize()
	memory, err := syscall.Mmap(-1, 0, 3*page, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := syscall.Munmap(memory); err != nil {
			t.Error(err)
		}
	})
	for _, guard := range [][]byte{memory[:page], memory[2*page:]} {
		if err := syscall.Mprotect(guard, syscall.PROT_NONE); err != nil {
			t.Fatal(err)
		}
	}
	for size := 0; size <= 257; size++ {
		for _, data := range [][]byte{memory[page : page+size], memory[2*page-size : 2*page]} {
			clear(data)
			if !AllZeroSIMD(data) {
				t.Fatalf("zero slice of length %d", size)
			}
			for pos := range data {
				data[pos] = 255
				if AllZeroSIMD(data) {
					t.Fatalf("missed nonzero: size=%d pos=%d", size, pos)
				}
				data[pos] = 0
			}
		}
	}
}
