package sparsified

import (
	"bytes"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestCopySparseFileSmallHoles(t *testing.T) {
	for _, tc := range []struct {
		name  string
		spans []SparseSpan
	}{
		{"leading", []SparseSpan{{IsHole: true, Beg: 0, Endx: 12288}, {Beg: 12288, Endx: 32768}}},
		{"interior", []SparseSpan{{Beg: 0, Endx: 4096}, {IsHole: true, Beg: 4096, Endx: 16384}, {Beg: 16384, Endx: 32768}}},
		{"trailing", []SparseSpan{{Beg: 0, Endx: 20480}, {IsHole: true, Beg: 20480, Endx: 32768}}},
		{"all", []SparseSpan{{IsHole: true, Beg: 0, Endx: 32768}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			src, dst := filepath.Join(dir, "src"), filepath.Join(dir, "dst")
			fd, err := createSparseFileFromSparseSpans(src, &SparseSpans{Slc: tc.spans}, newPRNG([32]byte{}))
			if err != nil {
				t.Fatal(err)
			}
			if err := fd.Close(); err != nil {
				t.Fatal(err)
			}
			if err := CopySparseFile(src, dst, false); err != nil {
				t.Fatal(err)
			}
			a, err := os.ReadFile(src)
			if err != nil {
				t.Fatal(err)
			}
			b, err := os.ReadFile(dst)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(a, b) {
				t.Fatal("copied contents differ")
			}
			if err := SparseDiff(src, dst); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Sample file allocation on a background goroutine during copying as well
// as checking the final layout.
// Sampling cannot prove an absolute peak, but catches sustained full allocation.
func Test2244_CopySparseFileLargeAllocation(t *testing.T) {
	const size int64 = 1 << 30
	dir := "test2244_outdir"
	os.RemoveAll(dir)
	os.MkdirAll(dir, 0700)
	// cleanup
	defer os.RemoveAll(dir)

	src, dst := filepath.Join(dir, "src"), filepath.Join(dir, "dst")
	fd, err := os.Create(src)
	if err != nil {
		t.Fatal(err)
	}
	defer fd.Close()
	if err := fd.Truncate(size); err != nil {
		t.Fatal(err)
	}
	block := bytes.Repeat([]byte{0x5a}, 4096)
	for off := int64(0); off < size; off += 64 << 20 {
		if _, err := fd.WriteAt(block, off); err != nil {
			t.Fatal(err)
		}
	}
	if err := fd.Sync(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	type observation struct {
		peak int64
		err  error
	}
	observed := make(chan observation, 1)
	go func() {
		var result observation
		for {
			fi, err := os.Stat(dst)
			if err == nil {
				allocated := fi.Sys().(*syscall.Stat_t).Blocks * 512
				if allocated > result.peak {
					result.peak = allocated
				}
			} else if !os.IsNotExist(err) {
				result.err = err
			}
			select {
			case <-done:
				observed <- result
				return
			default:
			}
		}
	}()

	// ext4 (linux) can delay allocation in dst, causing the SparseDiff() to fail unless we
	// force allocation with a sync; hence we use false for skipFsyncDest in CopySparseFile() next.

	// The symptom was:
	// === RUN   Test2244_CopySparseFileLargeAllocation
	// ...
	// copy_sparse_test.go:129: SparseDiff error: srcpath('test2244_outdir/src') has disksz=69632 (136 blocks of 512 bytes), but destpath('test2244_outdir/dst') has disksz=65536 (128 blocks of 512 bytes); difference src - dest => 4096
	// --- FAIL: Test2244_CopySparseFileLargeAllocation (0.00s)

	copyErr := CopySparseFile(src, dst, false)
	close(done)
	result := <-observed
	if copyErr != nil {
		t.Fatal(copyErr)
	}
	if result.err != nil {
		t.Fatal(result.err)
	}
	fi, err := os.Stat(dst)
	if err != nil {
		t.Fatal(err)
	}
	final := fi.Sys().(*syscall.Stat_t).Blocks * 512
	if final > result.peak {
		result.peak = final
	}
	t.Logf("logical size=%d, sampled peak allocation=%d, final allocation=%d", size, result.peak, final)
	if result.peak >= size/8 {
		t.Fatalf("copy allocated too much space: %d bytes", result.peak)
	}
	if err := SparseDiff(src, dst); err != nil {
		t.Fatal(err)
	}
}
