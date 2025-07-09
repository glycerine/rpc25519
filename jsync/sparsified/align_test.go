package sparsified

import (
	"testing"
)

func Test012_aligned_sparse_span(t *testing.T) {
	// for punching holes/rsync write sparse holes efficiently,
	// determine if a span of zeros has 4096 block aligned span
	// within it.

	a := AlignedSparseSpan(4, 2*4096+5)
	//vv("a = '%#v'", a)
	if a == nil {
		panic("expecte Span back")
	}
	if a.Beg != 4096 {
		panic("expected aligned span to start at 4096")
	}
	if a.Endx != 2*4096 {
		panic("expected aligned span to endx at 2*4096")
	}

	if nil != AlignedSparseSpan(0, 4095) {
		panic("expected no span available")
	}
	if nil != AlignedSparseSpan(1, 2*4096-1) {
		panic("expected no span available")
	}
	if nil != AlignedSparseSpan(4096+1, 3*4096-1) {
		panic("expected no span available")
	}
	s := AlignedSparseSpan(4096+1, 4*4096+1)
	if s.Beg != 2*4096 {
		panic("expected aligned span to start at 2*4096")
	}
	if s.Endx != 4*4096 {
		panic("expected aligned span to endx at 4*4096")
	}
}
