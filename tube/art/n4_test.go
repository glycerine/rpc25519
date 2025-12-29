package art

import (
	"fmt"
	"testing"
)

func Test_n4_clone_grow_shrink(t *testing.T) {

	lf := make([]*Leaf, 6)
	for i := range lf {
		lf[i] = NewLeaf([]byte(fmt.Sprintf("leaf%v", i)), nil, "")
	}
	//n4 := newNode4()
	n4 := &node4{}
	n4.addChild('0', bnodeLeaf(lf[0]))
	if got, want := int(n4.lth), 1; got != want {
		t.Fatalf("lth wrong: want %v, got %v", want, got)
	}
	if got, want := n4.index('0'), 0; got != want {
		t.Fatalf("index('0') wrong: want %v, got %v", want, got)
	}
	//vv("added lf0, now '%v'", n4.String())

	n4.addChild('1', bnodeLeaf(lf[1]))
	if got, want := int(n4.lth), 2; got != want {
		t.Fatalf("lth wrong: want %v, got %v", want, got)
	}
	if got, want := n4.index('1'), 1; got != want {
		t.Fatalf("index('1') wrong: want %v, got %v", want, got)
	}
	//vv("added lf1, now '%v'", n4.String())

	n4.addChild('2', bnodeLeaf(lf[2]))
	if got, want := int(n4.lth), 3; got != want {
		t.Fatalf("lth wrong: want %v, got %v", want, got)
	}
	if got, want := n4.index('2'), 2; got != want {
		t.Fatalf("index('2') wrong: want %v, got %v", want, got)
	}
	//vv("added lf2, now '%v'", n4.String())

	n4.addChild('3', bnodeLeaf(lf[3]))
	if got, want := int(n4.lth), 4; got != want {
		t.Fatalf("lth wrong: want %v, got %v", want, got)
	}
	if got, want := n4.index('3'), 3; got != want {
		t.Fatalf("index('3') wrong: want %v, got %v", want, got)
	}
	//vv("added lf3, now '%v'", n4.String())

	check := func(n inode) {
		var nextkey *byte
		var a *bnode
		var key byte
		for i := range 4 {
			key, a = n.next(nextkey)
			if a == nil {
				break
			}
			if got, want := a.leaf, lf[i]; got != want {
				t.Fatalf("next i=%v sequence wrong: want %v, got %v",
					i, want, got)
			}
			nextkey = &key
		}
	}
	check(n4)
	n16 := n4.grow().(*node16)
	check(n16)
	n48 := n16.grow().(*node48)
	check(n48)
	n256 := n48.grow().(*node256)
	check(n256)

	nn48 := n256.shrink().(*node48)
	check(nn48)
	nn16 := nn48.shrink().(*node16)
	check(nn16)
	nn4 := nn16.shrink().(*node4)
	check(nn4)

}
