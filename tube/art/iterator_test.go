package art

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
)

var _ = bytes.Compare
var _ = fmt.Sprintf
var _ = strconv.Atoi
var _ = strings.Split

func equalStringSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestIterConcurrentExpansion(t *testing.T) {

	var (
		tree = NewArtTree()
		keys = [][]byte{
			[]byte("aaba"),
			[]byte("aabb"),
		}
	)

	for _, key := range keys {
		tree.Insert(key, key)
	}
	//vv("orig tree: %v", tree)
	iter := tree.Iter(nil, nil)
	if !iter.Next() {
		t.Fatal("expected Next() to return true")
	}
	if got, want := iter.Key(), Key(keys[0]); !bytes.Equal(got, want) {
		t.Errorf("got key %v, want %v", got, want)
	}

	// adding a 3rd key, after iter started,
	// that is after the 2nd key we have not read yet.
	tree.Insert([]byte("aaca"), nil)

	//vv("after adding 'aaca', tree: %v", tree)

	if !iter.Next() {
		t.Fatal("expected Next() to return true")
	}
	if got, want := iter.Key(), Key(keys[1]); !bytes.Equal(got, want) {
		t.Errorf("got key %v, want %v", string(got), string(want))
	}

	if !iter.Next() {
		t.Fatal("expected Next() to return true")
	}
	if got, want := iter.Key(), Key("aaca"); !bytes.Equal(got, want) {
		t.Errorf("got key %v, want %v", string(got), string(want))
	}
}

func TestIterDeleteBehindFwd(t *testing.T) {

	tree := NewArtTree()
	N := 60000
	for i := range N {
		k := fmt.Sprintf("%09d", i)
		key := Key(k) // []byte
		tree.Insert(key, key)
	}
	//vv("full tree before any delete/iter: '%s'", tree)
	got := make(map[int]int)
	deleted := make(map[int]int)
	kept := make(map[int]int)

	iter := tree.Iter(nil, nil)
	thresh := 5000
	for iter.Next() {
		sz := tree.Size()
		k := iter.Key()
		nk, err := strconv.Atoi(strings.TrimSpace(string(k)))
		panicOn(err)
		got[nk] = len(got)
		// e.g. for N=6 and thresh=4 => delete 0,1,2,3. keep 4,5
		if nk < thresh {
			gone, _ := tree.Remove(k)
			if !gone {
				panic("should have gone")
			}
			deleted[nk] = len(deleted)

			sz2 := tree.Size()
			if sz2 != sz-1 {
				//vv("tree now '%s'", tree)
				panic("should have shrunk tree")
			}
		} else {
			kept[nk] = len(kept)
		}
	}
	sz := tree.Size()
	//vv("after iter, sz = %v", sz)
	//vv("got (len %v) = '%#v'", len(got), got)
	//vv("deleted (len %v) = '%#v'", len(deleted), deleted)
	//vv("kept (len %v) = '%#v'", len(kept), kept)

	if thresh > N {
		thresh = N // simpler verification below, no change in above.
	}

	if sz != (N - thresh) {
		t.Fatalf("expected tree to be size %v, but see %v", N-thresh, sz)
	}
	if len(got) != N {
		t.Fatalf("expected got(len %v) to be len %v", len(got), N)
	}
	if len(deleted) != thresh {
		t.Fatalf("expected deleted(len %v) to be len %v",
			len(deleted), thresh)
	}
	//vv("tree at end '%s'", tree)
	for i := thresh; i < N; i++ {
		k := fmt.Sprintf("%09d", i)
		key := Key(k) // []b
		_, _, found := tree.FindExact(key)
		if !found {
			t.Fatalf("expected to find '%v' still in tree", k)
		}
	}

	for i := 0; i < N; i++ {
		if _, ok := got[i]; !ok {
			t.Fatalf("expected got[i=%v] to be present.", i)
		}
		if i < thresh {
			if _, ok := deleted[i]; !ok {
				t.Fatalf("expected deleted[i=%v] to be present.", i)
			}
		} else {
			if _, ok := kept[i]; !ok {
				t.Fatalf("expected kept[i=%v] to be present.", i)
			}
		}
	}
}

func TestIterDeleteBehindReverse(t *testing.T) {

	tree := NewArtTree()
	N := 60_000
	if N >= 1_000_000_000 {
		panic(`must bump up the Sprintf("%09d", i) ` +
			`have sufficient lead 0 padding`)
	}
	for i := range N {
		// if we don't zero pad, then lexicographic
		// delete order is very different from
		// numerical order, and we might get
		// confused below--like we did at first
		// when wondering why 8 and 9 are the
		// first two deletions with 60 keys
		// in the tree. Lexicographically, they
		// are the largest.
		k := fmt.Sprintf("%09d", i)
		key := Key(k) // []byte
		tree.Insert(key, key)
	}
	//vv("full tree before any delete/iter: '%s'", tree)
	got := make(map[int]int)
	deleted := make(map[int]int)
	kept := make(map[int]int)

	iter := tree.RevIter(nil, nil)

	thresh := 20_000
	callcount := 0
	for iter.Next() {
		callcount++
		sz := tree.Size()
		k := iter.Key()
		nk, err := strconv.Atoi(strings.TrimSpace(string(k)))
		panicOn(err)
		got[nk] = len(got)
		// reversed testing uses callcount here,
		// so that reversed (order issued) actually matters.
		// e.g. for N=6, iter should return     5,4,3,2,1,0
		// and so for thresh = 2, we should del 5,4         (len thresh)
		//                         and keep         3,2,1,0 (len N-thresh)
		// kept is < N-thresh;
		// deleted is >= N-thresh
		if callcount <= thresh {
			//vv("calling Remove(%v)", nk)
			gone, _ := tree.Remove(k)
			if !gone {
				panic("should have gone")
			}
			deleted[nk] = len(deleted)

			sz2 := tree.Size()
			if sz2 != sz-1 {
				//vv("tree now '%s'", tree)
				panic("should have shrunk tree")
			}
		} else {
			kept[nk] = len(kept)
		}
	}
	sz := tree.Size()
	//vv("after iter, sz = %v", sz)
	//vv("got (len %v) = '%#v'", len(got), got)
	//vv("deleted (len %v) = '%#v'", len(deleted), deleted)
	//vv("kept (len %v) = '%#v'", len(kept), kept)

	if thresh > N {
		thresh = N // simpler verification below
	}

	if sz != (N - thresh) {
		t.Fatalf("expected tree to be size %v, but see %v", N-thresh, sz)
	}
	if len(got) != N {
		t.Fatalf("expected got(len %v) to be len %v", len(got), N)
	}
	if len(deleted) != thresh {
		t.Fatalf("expected deleted(len %v) to be len %v",
			len(deleted), thresh)
	}
	//vv("tree at end '%s'", tree)
	// kept: i < N-thresh
	for i := 0; i < N-thresh; i++ {
		k := fmt.Sprintf("%09d", i)
		key := Key(k) // []b
		_, _, found := tree.FindExact(key)
		if !found {
			t.Fatalf("expected to find '%v' still in tree", k)
		}
	}

	for i := 0; i < N; i++ {
		if _, ok := got[i]; !ok {
			t.Fatalf("expected got[i=%v] to be present.", i)
		}
		if i >= N-thresh {
			if _, ok := deleted[i]; !ok {
				t.Fatalf("expected deleted[i=%v] to be present.", i)
			}
		} else {
			if _, ok := kept[i]; !ok {
				t.Fatalf("expected kept[i=%v] to be present.", i)
			}
		}
	}
}

// [start, end) semantics version; not (start, end].
func TestIterator(t *testing.T) {

	keys := []string{
		"1234",
		"1245",
		"1267",
		"1345",
	}
	sorted := make([]string, len(keys))
	copy(sorted, keys)
	sort.Strings(sorted)

	reversed := make([]string, len(keys))
	copy(reversed, keys)
	sort.Sort(sort.Reverse(sort.StringSlice(reversed)))

	for _, tc := range []struct {
		desc       string
		keys       []string
		start, end string
		reverse    bool
		want       []string
	}{
		{
			desc: "full",
			keys: keys,
			want: sorted,
		},
		{
			desc: "empty",
			want: []string{},
		},
		{
			desc: "matching leaf",
			keys: keys[:1],
			want: keys[:1],
		},
		{
			desc:  "non matching leaf",
			keys:  keys[:1],
			want:  []string{},
			start: "13",
		},
		{
			desc: "limited by end",
			keys: keys,
			end:  "125",
			want: sorted[:2],
		},
		{
			desc:  "limited by start",
			keys:  keys,
			start: "124",
			want:  sorted[1:],
		},
		{
			desc: "end is excluded",
			keys: keys,
			end:  "1345",
			want: sorted[:3],
		},
		{
			desc:  "start to end",
			keys:  keys,
			start: "125",
			end:   "1345",
			want:  sorted[2:3],
		},
		{
			desc:    "reverse",
			keys:    keys,
			want:    reversed,
			reverse: true,
		},
		{
			desc:    "reverse until",
			keys:    keys,
			end:     "1200",
			want:    reversed,
			reverse: true,
		},
		{
			desc:    "reverse from3",
			keys:    keys,
			start:   "1268",
			want:    reversed[1:],
			reverse: true,
		},
		{
			desc:    "reverse from until",
			keys:    keys,
			end:     "1235",
			start:   "1268",
			want:    reversed[1:3],
			reverse: true,
		},
	} {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			tree := NewArtTree()
			for _, key := range tc.keys {
				tree.Insert([]byte(key), []byte(key))
			}
			//vv("tree = '%v'", tree)
			var iter *iterator
			if tc.reverse {
				//vv("reverse is true")
				iter = tree.RevIter([]byte(tc.end), []byte(tc.start))
				//vv("iter.reverse is %v", iter.reverse)
			} else {
				iter = tree.Iter([]byte(tc.start), []byte(tc.end))
			}
			want := []string{}
			for iter.Next() {
				want = append(want, string(iter.Value()))
			}
			if !equalStringSlice(want, tc.want) {
				t.Fatalf("got='%v'; want '%v'", want, tc.want)
			}
		})
	}
}

func TestIterRange(t *testing.T) {

	tree := NewArtTree()
	N := 3

	// pick out the extremes to specify the range.
	var first, last []byte

	for i := range N {
		k := fmt.Sprintf("%09d", i)
		key := Key(k) // []byte
		if i == 0 {
			first = append([]byte{}, []byte(key)...)
		}
		if i == N-1 {
			last = append([]byte{}, []byte(key)...)
		}
		tree.Insert(key, key)
	}
	//vv("tree: '%s'", tree)
	//vv("first = '%v'", string(first)) // 0
	//vv("last = '%v'", string(last)) // 2
	if true {
		expect := []int{0, 1}
		iter := tree.Iter(first, last) // [0, 2) so 0, 1
		n := 0
		for iter.Next() {
			key := iter.Key()
			k, err := strconv.Atoi(strings.TrimSpace(string(key)))
			panicOn(err)
			//fmt.Printf("item %v was key '%v'\n", n, string(key))
			if k != expect[n] {
				t.Fatalf("want %v, got %v", n, k)
			}
			n++
		}
	}

	if true {
		expect := []int{2, 1}
		riter := tree.RevIter(first, last) // (0,2], so 2, 1
		n := 0
		for riter.Next() {
			key := riter.Key()
			k, err := strconv.Atoi(strings.TrimSpace(string(key)))
			panicOn(err)
			//fmt.Printf("riter item %v was key '%v' -> k=%v; expect: '%v'\n", n, string(key), k, expect[n])
			if k != expect[n] {
				t.Fatalf("want %v, got %v", n, k)
			}
			n++
		}
	}
}
