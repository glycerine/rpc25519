package tube

import (
	"testing"
)

func TestSpanConstructors(t *testing.T) {
	t.Run("spanBegxEndi", func(t *testing.T) {
		s := spanBegxEndi(9, 19)
		expected := &span{beg: 10, endx: 20}
		if !s.equal(expected) {
			t.Errorf("spanBegxEndi(9, 19) = %+v, want %+v", s, expected)
		}
	})

	t.Run("spanBegiEndx", func(t *testing.T) {
		s := spanBegiEndx(10, 20)
		expected := &span{beg: 10, endx: 20}
		if !s.equal(expected) {
			t.Errorf("spanBegiEndx(10, 20) = %+v, want %+v", s, expected)
		}
	})
}

func TestSpanAfter(t *testing.T) {
	tests := []struct {
		name string
		a    *span
		b    *span
		want bool
	}{
		{"a is after b", &span{20, 30}, &span{10, 20}, true},
		{"a is adjacent after b", &span{20, 30}, &span{10, 20}, true},
		{"a overlaps b", &span{15, 25}, &span{10, 20}, false},
		{"a is before b", &span{0, 5}, &span{10, 20}, false},
		{"a is identical to b", &span{10, 20}, &span{10, 20}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.after(tt.b); got != tt.want {
				t.Errorf("(%+v).after(%+v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestSpanBefore(t *testing.T) {
	tests := []struct {
		name string
		a    *span
		b    *span
		want bool
	}{
		{"a is before b", &span{0, 10}, &span{10, 20}, true},
		{"a is adjacent before b", &span{0, 10}, &span{10, 20}, true},
		{"a overlaps b", &span{5, 15}, &span{10, 20}, false},
		{"a is after b", &span{20, 30}, &span{10, 20}, false},
		{"a is identical to b", &span{10, 20}, &span{10, 20}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.before(tt.b); got != tt.want {
				t.Errorf("(%+v).before(%+v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestSpanOverlaps(t *testing.T) {
	tests := []struct {
		name string
		a    *span
		b    *span
		want bool
	}{
		{"a overlaps beginning of b", spanBegiEndx(0, 15), spanBegiEndx(10, 20), true},
		{"a overlaps end of b", spanBegiEndx(15, 25), spanBegiEndx(10, 20), true},
		{"a contains b", spanBegiEndx(0, 30), spanBegiEndx(10, 20), true},
		{"b contains a", spanBegiEndx(10, 20), spanBegiEndx(0, 30), true},
		{"a and b are identical", spanBegiEndx(10, 20), spanBegiEndx(10, 20), true},
		{"a is before b, no overlap", spanBegiEndx(0, 10), spanBegiEndx(10, 20), false},
		{"a is after b, no overlap", spanBegiEndx(20, 30), spanBegiEndx(10, 20), false},
		{"a is far before b", spanBegiEndx(0, 5), spanBegiEndx(10, 20), false},
		{"a is far after b", spanBegiEndx(25, 30), spanBegiEndx(10, 20), false},
		// Point tests
		{"a is a point inside b", spanBegiEndx(15, 15), spanBegiEndx(10, 20), true},
		{"b is a point inside a", spanBegiEndx(10, 20), spanBegiEndx(15, 15), true},
		{"a is a point at start of b", spanBegiEndx(10, 10), spanBegiEndx(10, 20), true},
		{"a is a point at end of b (exclusive)", spanBegiEndx(20, 20), spanBegiEndx(10, 20), false},
		{"two points are identical", spanBegiEndx(15, 15), spanBegiEndx(15, 15), true},
		{"two points are different", spanBegiEndx(15, 15), spanBegiEndx(16, 16), false},
		{"a is point outside b", spanBegiEndx(25, 25), spanBegiEndx(10, 20), false},
		{"b is point outside a", spanBegiEndx(10, 20), spanBegiEndx(25, 25), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.overlaps(tt.b); got != tt.want {
				t.Errorf("(%#v).overlaps(%#v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestSpanIsSuperSetOf(t *testing.T) {
	tests := []struct {
		name string
		a    *span
		b    *span
		want bool
	}{
		{"a is superset of b", &span{0, 30}, &span{10, 20}, true},
		{"a is identical to b", &span{10, 20}, &span{10, 20}, true},
		{"a contains b with same start", &span{10, 30}, &span{10, 20}, true},
		{"a contains b with same end", &span{0, 20}, &span{10, 20}, true},
		{"b is superset of a", &span{10, 20}, &span{0, 30}, false},
		{"a overlaps start of b", &span{0, 15}, &span{10, 20}, false},
		{"a overlaps end of b", &span{15, 25}, &span{10, 20}, false},
		{"a is before b", &span{0, 5}, &span{10, 20}, false},
		{"a is after b", &span{25, 30}, &span{10, 20}, false},
		{"a is superset of a point", &span{10, 20}, &span{15, 15}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.isSuperSetOf(tt.b); got != tt.want {
				t.Errorf("(%+v).isSuperSetOf(%+v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestSpanContains(t *testing.T) {
	s := &span{10, 20}
	tests := []struct {
		name string
		x    int64
		want bool
	}{
		{"point inside", 15, true},
		{"point at beginning", 10, true},
		{"point at end (exclusive)", 20, false},
		{"point before", 5, false},
		{"point after", 25, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := s.contains(tt.x); got != tt.want {
				t.Errorf("(%+v).contains(%v) = %v, want %v", s, tt.x, got, tt.want)
			}
		})
	}
}

func TestSpanEqual(t *testing.T) {
	tests := []struct {
		name string
		a    *span
		b    *span
		want bool
	}{
		{"identical spans", &span{10, 20}, &span{10, 20}, true},
		{"different end", &span{10, 20}, &span{10, 21}, false},
		{"different beginning", &span{10, 20}, &span{11, 20}, false},
		{"completely different", &span{10, 20}, &span{30, 40}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.equal(tt.b); got != tt.want {
				t.Errorf("(%+v).equal(%+v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestSpanIntersect(t *testing.T) {
	tests := []struct {
		name      string
		a         *span
		b         *span
		want      span
		wantEmpty bool
	}{
		{"no overlap", &span{0, 10}, &span{10, 20}, span{}, true},
		{"a contains b", &span{0, 30}, &span{10, 20}, span{10, 20}, false},
		{"b contains a", &span{10, 20}, &span{0, 30}, span{10, 20}, false},
		{"partial overlap a starts first", &span{10, 20}, &span{15, 25}, span{15, 20}, false},
		{"partial overlap b starts first", &span{15, 25}, &span{10, 20}, span{15, 20}, false},
		{"identical spans", &span{10, 20}, &span{10, 20}, span{10, 20}, false},
		{"a is a point", &span{15, 15}, &span{10, 20}, span{}, true},
		{"b is a point", &span{10, 20}, &span{15, 15}, span{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotEmpty := tt.a.intersect(tt.b)
			if gotEmpty != tt.wantEmpty {
				t.Errorf("(%+v).intersect(%+v) empty = %v, want %v", tt.a, tt.b, gotEmpty, tt.wantEmpty)
			}
			if !gotEmpty && !(&got).equal(&tt.want) {
				t.Errorf("(%+v).intersect(%+v) = %+v, want %+v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}
