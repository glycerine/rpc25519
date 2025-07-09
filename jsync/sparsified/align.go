package sparsified

// AlignedSparseSpan is
// for punching holes/rsync write sparse holes efficiently,
// determine if a span of zeros has 4096 block aligned span
// within it. Returns nil if no such span available.
func AlignedSparseSpan(beg, endx int64) (s *Span) {

	const block = 4096

	if beg < 0 {
		panic("beg must be >= 0")
	}
	if endx < 0 {
		panic("endx must be >= 0")
	}
	sz := endx - beg
	if sz < block {
		return nil
	}
	if beg == 0 {
		// round down to nearest multiple of block
		n := endx / block
		rem := endx % block
		if rem == 0 {
			s = &Span{
				Beg:  0,
				Endx: endx,
			}
			return
		}
		return &Span{
			Beg:  0,
			Endx: n * block,
		}
	}
	// INVAR: beg > 0.
	// we might still have to return nil if we
	// don't overlap a fullly aligned block.

	b := beg / block
	brem := beg % block
	var start int64
	if brem == 0 {
		start = beg
	} else {
		start = (b + 1) * block
	}
	// INVAR: start is block aligned.
	avail := endx - start
	if avail < block {
		return nil
	}
	// INVAR: avail >= block
	blocksAvail := avail / block
	return &Span{
		Beg:  start,
		Endx: start + (blocksAvail * block),
	}
}
