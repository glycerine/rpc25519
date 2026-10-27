//go:build linux

package sparsified

import "os"

// findPreallocatedRegions returns FIEMAP's logical layout. Unlike st_blocks,
// FIEMAP extent offsets do not include filesystem metadata allocation.
func findPreallocatedRegions(fd *os.File, sum *SparseSum) (*SparseSpans, bool, error) {
	spans, err := FieMap2(fd)
	if err != nil || spans == nil {
		return nil, false, err
	}
	// With KEEP_SIZE preallocation, ext4 can temporarily report a recently
	// written prefix as FIEMAP_EXTENT_UNWRITTEN until writeback completes. Bytes
	// below EOF are the file's data prefix in the model supported by this
	// package; only the beyond-EOF tail is the reservation being transferred.
	var normalized []SparseSpan
	for _, span := range spans.Slc {
		if span.IsUnwrittenPrealloc && span.Beg < sum.StatSize {
			writtenEnd := min(span.Endx, sum.StatSize)
			written := span
			written.Endx = writtenEnd
			written.IsUnwrittenPrealloc = false
			normalized = append(normalized, written)
			if span.Endx > writtenEnd {
				span.Beg = writtenEnd
				normalized = append(normalized, span)
			}
			continue
		}
		normalized = append(normalized, span)
	}

	// Physical fragmentation may split one logical preallocation into many
	// adjacent FIEMAP extents. Consumers need the logical reservation, not the
	// largest physical fragment.
	spans.Slc = logicalSpans(normalized)

	var largest SparseSpan
	for _, span := range spans.Slc {
		if span.IsUnwrittenPrealloc && span.Endx-span.Beg > largest.Endx-largest.Beg {
			largest = span
		}
	}
	if largest.Endx == 0 {
		return nil, false, nil
	}
	sum.UnwritBegin = largest.Beg
	sum.UnwritEndx = largest.Endx
	return spans, true, nil
}
