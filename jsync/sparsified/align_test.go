package sparsified

import (
	"fmt"
	"testing"
)

func Test012_aligned_sparse_span(t *testing.T) {
	// for punching holes/rsync write sparse holes efficiently,
	// determine if a span of zeros has 4096 block aligned span
	// within it.

	a, w := AlignedSparseSpan(4, 2*4096+5)
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
	if w.Pre.Beg != 4 || w.Pre.Endx != 4096 {
		panic("expected wing.Pre to have [4, 4096)")
	}
	if w.Post.Beg != 2*4096 || w.Post.Endx != 2*4096+5 {
		panic("expected wing.Pre to have [2*4096, 2*4096+5)")
	}

	if a, _ = AlignedSparseSpan(0, 4095); a != nil {
		panic("expected no span available")
	}
	if a, _ = AlignedSparseSpan(1, 2*4096-1); a != nil {
		panic("expected no span available")
	}
	a, w = AlignedSparseSpan(4096+1, 3*4096-1)
	if a != nil {
		panic("expected no span available")
	}
	if w.Pre.Beg != 4097 || w.Pre.Endx != 3*4096-1 {
		panic("expected wing.Pre to have the whole rest of the span")
	}

	s, w := AlignedSparseSpan(4096+1, 4*4096+1)
	if s.Beg != 2*4096 {
		panic("expected aligned span to start at 2*4096")
	}
	if s.Endx != 4*4096 {
		panic("expected aligned span to endx at 4*4096")
	}
	if w.Pre.Beg != 4097 {
		panic(fmt.Sprintf("expected wing.Pre.Beg == 4097, got %v", w.Pre.Beg))
	}
	if w.Pre.Endx != s.Beg {
		panic(fmt.Sprintf("expected wing.Pre.Endx == %v, got %v", s.Beg, w.Pre.Endx))
	}
	if w.Post.Beg != 4*4096 {
		panic(fmt.Sprintf("expected wing.Post.Beg == 4*4096, got %v", w.Post.Beg))
	}
	if w.Post.Endx != 4*4096+1 {
		panic(fmt.Sprintf("expected wing.Post.Endx == %v, got %v", 4*4096+1, w.Post.Endx))
	}
}
