package art

import (
	"testing"
)

type quest struct {
	key byte
}
type ans struct {
	b  byte
	ch *bnode
}

func TestNode_gte_(t *testing.T) {

	tests := []struct {
		name   string
		keys   []byte
		needle *quest
		want   ans
	}{
		{
			name:   "nil gte gives first",
			keys:   []byte{2, 4, 6, 8},
			needle: nil,
			want:   ans{b: 2},
		},
		{
			name:   "1 -> 2",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 1},
			want:   ans{b: 2},
		},
		{
			name:   "2 -> 2",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 2},
			want:   ans{b: 2},
		},
		{
			name:   "3 -> 4",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 3},
			want:   ans{b: 4},
		},
		{
			name:   "4 -> 4",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 4},
			want:   ans{b: 4},
		},
		{
			name:   "5 -> 6",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 5},
			want:   ans{b: 6},
		},
		{
			name:   "6 -> 6",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 6},
			want:   ans{b: 6},
		},
		{
			name:   "7 -> 8",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 7},
			want:   ans{b: 8},
		},
		{
			name:   "8 -> 8",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 8},
			want:   ans{b: 8},
		},
		{
			name:   "9 -> 0",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 9},
			want:   ans{b: 0},
		},
	}
	test_gte := func(n inode, keys []byte, needle *quest) ans {
		for _, k := range keys {
			n.addChild(k, &bnode{isLeaf: true, leaf: &Leaf{}}) // can't add nil for n256. added a fake Leaf for subn() never 0 checking in node.go:99
		}
		var b byte
		var ch *bnode
		if needle == nil {
			b, ch = n.gte(nil)
		} else {
			b, ch = n.gte(&needle.key)
		}
		return ans{b: b, ch: ch}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var n inode
			for i := range 4 {
				switch i {
				case 0:
					n = &node4{}
				case 1:
					n = &node16{}
				case 2:
					n = &node48{}
				case 3:
					n = &node256{}
				}
				if got := test_gte(n, tt.keys, tt.needle); got.b !=
					tt.want.b {

					t.Errorf("on i=%v, test_gte = %#v, want %#v", i, got, tt.want)
				}
			}
		})
	}
}

func TestNode_gt_(t *testing.T) {

	tests := []struct {
		name   string
		keys   []byte
		needle *quest
		want   ans
	}{
		{
			name:   "nil gt gives first",
			keys:   []byte{2, 4, 6, 8},
			needle: nil,
			want:   ans{b: 2},
		},
		{
			name:   "0 -> 2",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 0},
			want:   ans{b: 2},
		},
		{
			name:   "1 -> 2",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 1},
			want:   ans{b: 2},
		},
		{
			name:   "2 -> 4",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 2},
			want:   ans{b: 4},
		},
		{
			name:   "3 -> 4",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 3},
			want:   ans{b: 4},
		},
		{
			name:   "4 -> 6",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 4},
			want:   ans{b: 6},
		},
		{
			name:   "5 -> 6",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 5},
			want:   ans{b: 6},
		},
		{
			name:   "6 -> 8",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 6},
			want:   ans{b: 8},
		},
		{
			name:   "8 -> 0",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 8},
			want:   ans{b: 0},
		},
		{
			name:   "9 -> 0",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 9},
			want:   ans{b: 0},
		},
	}
	test_gt := func(n inode, keys []byte, needle *quest) ans {
		for _, k := range keys {
			n.addChild(k, &bnode{isLeaf: true, leaf: &Leaf{}}) // can't add nil for n256. fake leaf so subn() doesn't crash.
		}
		var b byte
		var ch *bnode
		if needle == nil {
			b, ch = n.gt(nil)
		} else {
			b, ch = n.gt(&needle.key)
		}
		return ans{b: b, ch: ch}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var n inode
			for i := range 4 {
				switch i {
				case 0:
					n = &node4{}
				case 1:
					n = &node16{}
				case 2:
					n = &node48{}
				case 3:
					n = &node256{}
				}
				if got := test_gt(n, tt.keys, tt.needle); got.b !=
					tt.want.b {

					t.Errorf("on i=%v, test_gt = %#v, want %#v", i, got, tt.want)
				}
			}
		})
	}
}

func TestNode_lt_(t *testing.T) {

	tests := []struct {
		name   string
		keys   []byte
		needle *quest
		want   ans
	}{
		{
			name:   "nil gt gives last",
			keys:   []byte{2, 4, 6, 8},
			needle: nil,
			want:   ans{b: 8},
		},
		{
			name:   "0 -> 0",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 0},
			want:   ans{b: 0},
		},
		{
			name:   "1 -> 0",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 1},
			want:   ans{b: 0},
		},
		{
			name:   "2 -> 0",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 2},
			want:   ans{b: 0},
		},
		{
			name:   "3 -> 2",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 3},
			want:   ans{b: 2},
		},
		{
			name:   "4 -> 2",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 4},
			want:   ans{b: 2},
		},
		{
			name:   "5 -> 4",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 5},
			want:   ans{b: 4},
		},
		{
			name:   "6 -> 4",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 6},
			want:   ans{b: 4},
		},
		{
			name:   "8 -> 6",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 8},
			want:   ans{b: 6},
		},
		{
			name:   "9 -> 8",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 9},
			want:   ans{b: 8},
		},
	}
	test_lt := func(n inode, keys []byte, needle *quest) ans {
		for _, k := range keys {
			n.addChild(k, &bnode{isLeaf: true, leaf: &Leaf{}}) // can't add nil for n256
		}
		var b byte
		var ch *bnode
		if needle == nil {
			b, ch = n.lt(nil)
		} else {
			b, ch = n.lt(&needle.key)
		}
		return ans{b: b, ch: ch}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var n inode
			for i := range 4 {
				switch i {
				case 0:
					n = &node4{}
				case 1:
					n = &node16{}
				case 2:
					n = &node48{}
				case 3:
					n = &node256{}
				}
				if got := test_lt(n, tt.keys, tt.needle); got.b !=
					tt.want.b {

					t.Errorf("on i=%v, test_lt = %#v, want %#v", i, got, tt.want)
				}
			}
		})
	}
}

func TestNode_lte_(t *testing.T) {

	tests := []struct {
		name   string
		keys   []byte
		needle *quest
		want   ans
	}{
		{
			name:   "nil gt gives last",
			keys:   []byte{2, 4, 6, 8},
			needle: nil,
			want:   ans{b: 8},
		},
		{
			name:   "0 -> 0",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 0},
			want:   ans{b: 0},
		},
		{
			name:   "1 -> 0",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 1},
			want:   ans{b: 0},
		},
		{
			name:   "2 -> 2",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 2},
			want:   ans{b: 2},
		},
		{
			name:   "3 -> 2",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 3},
			want:   ans{b: 2},
		},
		{
			name:   "4 -> 4",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 4},
			want:   ans{b: 4},
		},
		{
			name:   "5 -> 4",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 5},
			want:   ans{b: 4},
		},
		{
			name:   "6 -> 6",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 6},
			want:   ans{b: 6},
		},
		{
			name:   "7 -> 6",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 7},
			want:   ans{b: 6},
		},
		{
			name:   "8 -> 8",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 8},
			want:   ans{b: 8},
		},
		{
			name:   "9 -> 8",
			keys:   []byte{2, 4, 6, 8},
			needle: &quest{key: 9},
			want:   ans{b: 8},
		},
	}
	test_lte := func(n inode, keys []byte, needle *quest) ans {
		for _, k := range keys {
			n.addChild(k, &bnode{isLeaf: true, leaf: &Leaf{}}) // can't add nil for n256
		}
		var b byte
		var ch *bnode
		if needle == nil {
			b, ch = n.lte(nil)
		} else {
			b, ch = n.lte(&needle.key)
		}
		return ans{b: b, ch: ch}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var n inode
			for i := range 4 {
				switch i {
				case 0:
					n = &node4{}
				case 1:
					n = &node16{}
				case 2:
					n = &node48{}
				case 3:
					n = &node256{}
				}
				if got := test_lte(n, tt.keys, tt.needle); got.b !=
					tt.want.b {

					t.Errorf("on i=%v, test_lte = %#v, want %#v", i, got, tt.want)
				}
			}
		})
	}
}

/*
func TestNode_gte_is_ended(t *testing.T) {
	// termination condition: needNextLeaf all
	// up the right side.
	lf = make([]*Leaf)
	n := &node4{}

}



func Test_n4_gte(t *testing.T) {

	lf := make([]*bnode, 6)
	for i := range lf {
		lf[i] = bnodeLeaf(NewLeaf([]byte(fmt.Sprintf("%v", i)), nil, nil))
	}
	//n4 := newNode4()
	n4 := &node4{}
	for i := range 4 {
		n4.addChild(lf[i].leaf.Key, lf[i])
	}

	check := func(needle *Leaf, expectKeyb byte, expectCh *Leaf) {
		var b byte
		var ch *bnode
		if needle == nil {
			b, ch = n4.gte(nil)
		} else {
			b, ch = n4.gte(needle.Key)
		}
		_ = b
		if ch.leaf != expectCh {
			t.Fatalf("keybyte expected: %v, got %v", expectCh, ch.leaf)
		}
	}

	check(nil, lf[0])
	check(lf[4], nil)

}
*/
