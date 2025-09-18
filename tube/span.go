package tube

// span overlap math

type span struct {
	beg  int64 // included
	endx int64 // excluded
}

func spanBegxEndi(begx, endi int64) *span {
	return &span{
		beg:  begx + 1,
		endx: endi + 1,
	}
}
func spanBegiEndx(begi, endx int64) *span {
	return &span{
		beg:  begi,
		endx: endx,
	}
}

func (a *span) after(b *span) bool {
	return a.beg >= b.endx
}
func (a *span) before(b *span) bool {
	return a.endx <= b.beg
}

func (a *span) overlaps(b *span) bool {

	// have to allow points of Span(a,a) to fall inside Span(a, b)
	var apoint, bpoint bool

	if b.endx == b.beg {
		bpoint = true
		//vv("bpoint true")
	}
	if a.endx == a.beg {
		apoint = true
		//vv("apoint true")
	}
	switch {
	case apoint && bpoint:
		return a.beg == b.beg
	case apoint:
		return a.beg >= b.beg && a.beg < b.endx
	case bpoint:
		return b.beg >= a.beg && b.beg < a.endx
	}
	// INVAR: neither span is a point

	if a.endx <= b.beg {
		// a1)
		//   [b0,
		return false
	}
	// INVAR: a.endx > b.beg
	//          a1)
	//   [b0,

	if b.endx <= a.beg {
		//     [a0
		//   b1)
		return false
	}
	// INVAR: b.endx > a.beg
	//   [a0
	//        b1)

	// the question is: a overlaps b?
	// we have established:
	//       [a0,        a1)
	// [b0 or     [b0,b1)    or b1)
	return true
}

// is b identical to a, or smaller and contained in a?
func (a *span) isSuperSetOf(b *span) bool {
	if b.beg < a.beg {
		return false
	}
	// INVAR: b.beg >= a.beg

	if b.endx > a.endx {
		return false
	}
	// INVAR: b.endx <= a.endx

	return true
}

func (a *span) contains(x int64) bool {
	if x < a.beg {
		return false
	}
	if x >= a.endx {
		return false
	}
	return true
}

func (a *span) equal(b *span) bool {
	if a.beg != b.beg {
		return false
	}
	if a.endx != b.endx {
		return false
	}
	return true
}

func (a *span) intersect(b *span) (res span, empty bool) {

	if !a.overlaps(b) {
		empty = true
		return
	}

	// deal with points, which have no effect
	if a.beg == a.endx {
		empty = true
		return
	}
	if b.beg == b.endx {
		empty = true
		return
	}
	// INVAR: not dealing with points, and we have overlap

	if a.isSuperSetOf(b) {
		res = *b
		return
	}

	if b.isSuperSetOf(a) {
		res = *a
		return
	}
	// partially overlapping
	if a.beg < b.beg {
		//   [a0,     a1)
		//        [b0,      b1)
		res.beg = b.beg
		res.endx = a.endx
		return
	}

	//        [a0,     a1)
	//  [b0,      b1)
	res.beg = a.beg
	res.endx = b.endx
	return
}
