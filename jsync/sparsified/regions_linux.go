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
	// FIEMAP reports block-sized extents, so clamp ordinary data and holes to
	// EOF. With KEEP_SIZE preallocation, ext4 can temporarily report a recently
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
		if !span.IsUnwrittenPrealloc {
			if span.Beg >= sum.StatSize {
				continue
			}
			span.Endx = min(span.Endx, sum.StatSize)
		}
		normalized = append(normalized, span)
	}

	// Physical fragmentation may split one logical preallocation into many
	// adjacent FIEMAP extents. Consumers need the logical reservation, not the
	// largest physical fragment.
	spans.Slc = logicalSpans(normalized)

	// ext4 may retain one speculative unwritten block past EOF after ordinary
	// writes. It is indistinguishable in FIEMAP from a tiny KEEP_SIZE request,
	// and the wire format already defines real preallocation as larger than one
	// filesystem block. Do not turn that allocator detail into file semantics.
	minPrealloc, err := MinSparseHoleSize(fd)
	if err != nil || minPrealloc <= 0 {
		minPrealloc = 4096
	}
	filtered := spans.Slc[:0]
	for _, span := range spans.Slc {
		if span.IsUnwrittenPrealloc && span.Beg >= sum.StatSize &&
			span.Endx-span.Beg <= minPrealloc {
			continue
		}
		filtered = append(filtered, span)
	}
	spans.Slc = filtered

	var largest SparseSpan
	for _, span := range spans.Slc {
		if span.IsUnwrittenPrealloc && span.Endx-span.Beg > largest.Endx-largest.Beg {
			largest = span
		}
	}
	if largest.Endx == 0 {
		// FIEMAP succeeded and authoritatively found no transferable
		// preallocation. Do not fall back to interpreting st_blocks as offsets.
		return spans, true, nil
	}
	sum.UnwritBegin = largest.Beg
	sum.UnwritEndx = largest.Endx
	return spans, true, nil
}
