//go:build !linux

package sparsified

import "os"

func findPreallocatedRegions(fd *os.File, sum *SparseSum) (*SparseSpans, bool, error) {
	return nil, false, nil
}
