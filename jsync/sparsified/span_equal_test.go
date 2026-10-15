package sparsified

import (
	"reflect"
	"testing"
)

func TestSparseSpansEqualLogicalLayout(t *testing.T) {
	const end = 64 << 20
	spec := &SparseSpans{Slc: []SparseSpan{
		{Beg: 0, Endx: 4096},
		{Beg: 4096, Endx: end, IsUnwrittenPrealloc: true},
	}}
	for _, tc := range []struct {
		name  string
		spans []SparseSpan
		want  bool
	}{
		{"fragmented preallocation", []SparseSpan{
			{Beg: 0, Endx: 4096},
			{Beg: 4096, Endx: 42098688, IsUnwrittenPrealloc: true},
			{Beg: 42098688, Endx: end, IsUnwrittenPrealloc: true, Flags: 1},
		}, true},
		{"written instead of preallocated", []SparseSpan{
			{Beg: 0, Endx: 4096},
			{Beg: 4096, Endx: end},
		}, false},
		{"preallocated instead of written", []SparseSpan{
			{Beg: 0, Endx: end, IsUnwrittenPrealloc: true},
		}, false},
		{"gap", []SparseSpan{
			{Beg: 0, Endx: 4096},
			{Beg: 8192, Endx: end, IsUnwrittenPrealloc: true},
		}, false},
		{"overlap", []SparseSpan{
			{Beg: 0, Endx: 4096},
			{Beg: 4096, Endx: 16384, IsUnwrittenPrealloc: true},
			{Beg: 8192, Endx: end, IsUnwrittenPrealloc: true},
		}, false},
		{"hole instead of preallocation", []SparseSpan{
			{Beg: 0, Endx: 4096},
			{Beg: 4096, Endx: end, IsHole: true},
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := &SparseSpans{Slc: tc.spans}
			before := append([]SparseSpan(nil), got.Slc...)
			if got.Equal(spec) != tc.want || spec.Equal(got) != tc.want {
				t.Fatalf("Equal should be %v in both directions: got %v, spec %v", tc.want, got, spec)
			}
			if !reflect.DeepEqual(got.Slc, before) {
				t.Fatal("Equal modified its input")
			}
		})
	}
}

func TestSparseSpanEqualPreallocation(t *testing.T) {
	written := SparseSpan{Beg: 0, Endx: 4096}
	preallocated := written
	preallocated.IsUnwrittenPrealloc = true
	if written.Equal(&preallocated) || preallocated.Equal(&written) {
		t.Fatal("written and unwritten preallocated spans must differ")
	}
}
