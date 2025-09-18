package art

import (
	"bytes"
	"encoding/hex"
	"fmt"
)

type node48 struct {
	lth int // uint8 dangerous due to underflow bugs.

	// keys[j] non-zero maps to children[keys[j]-1]
	keys     [256]uint16
	children [48]*bnode
}

func (n *node48) last() (byte, *bnode) {
	for i := 255; i >= 0; i-- {
		k := n.keys[i]
		if k != 0 {
			return byte(k - 1), n.children[k-1]
		}
	}
	//panic("unreachable since node48 must have >= 17 children")
	return 0, nil // forgo panic, try to make inline-able.
}

func (n *node48) first() (byte, *bnode) {
	for _, k := range n.keys {
		if k != 0 {
			return byte(k - 1), n.children[k-1]
		}
	}
	//panic("unreachable since node48 must have >= 17 children")
	return 0, nil // forgo panic, try to make inline-able.
}

func (n *node48) nchild() int {
	return int(n.lth)
}

func (n *node48) childkeysString() (s string) {
	s = "["
	for i, k := range n.keys {
		if k == 0 {
			continue
		}
		kb := byte(i)
		// INVAR: k > 0, and k-1 is the index into children.
		// the key is in kb
		if len(s) > 1 {
			s += ", "
		}
		if kb == 0 {
			s += "zero"
		} else {
			if kb < 33 || kb > '~' {
				s += fmt.Sprintf("0x%x", kb)
			} else {
				s += fmt.Sprintf("'%v'", string(kb))
			}
		}
	}
	return s + "]"
}

func (n *node48) kind() kind {
	return _Node48
}

func (n *node48) child(k byte) (int, *bnode) {
	idx := n.keys[k]
	if idx == 0 {
		return 0, nil
	}
	return int(k), n.children[idx-1]
}

func (n *node48) next(k *byte) (byte, *bnode) {
	for b, idx := range n.keys {
		if (k == nil || byte(b) > *k) && idx != 0 {
			return byte(b), n.children[idx-1]
		}
	}
	return 0, nil
}

// gte: A nil k will return the first key.
//
// Otherwise, we return the first
// (smallest) key that is >= *k.
//
// A nil bnode back means that all keys were < *k.
func (n *node48) gte(k *byte) (byte, *bnode) {
	for b, idx := range n.keys {
		if (k == nil || byte(b) >= *k) && idx != 0 {
			return byte(b), n.children[idx-1]
		}
	}
	return 0, nil
}

func (n *node48) gt(k *byte) (keyb byte, ch *bnode) {
	if k == nil {
		// request for 1st key
		return n.gte(nil)
	}
	// INVAR: k != nil
	for b, idx := range n.keys {
		if (byte(b) > *k) && idx != 0 {
			return byte(b), n.children[idx-1]
		}
	}
	return 0, nil
}

func (n *node48) prev(k *byte) (byte, *bnode) {

	// must scan ALL the keys, not just n.lth!
	for b := 255; b >= 0; b-- {
		idx := n.keys[b]
		if idx == 0 {
			continue
		}
		if k == nil || byte(b) < *k {
			return byte(b), n.children[idx-1]
		}
	}
	return 0, nil
}

func (n *node48) full() bool {
	return n.lth == 48
}

func (n *node48) addChild(k byte, child *bnode) {
	for idx, existing := range n.children {
		if existing == nil {
			n.keys[k] = uint16(idx + 1)
			n.children[idx] = child
			n.lth++
			n.redoPren()
			return
		}
	}
	//panic("no empty slots") // make inlinable.
}

// update pren cache of cumulative SubN
func (n *node48) redoPren() {
	return // lazy now, use prenOK to determine when.

	tot := 0
	for _, idx := range n.keys {
		if idx == 0 {
			continue
		}
		ch := n.children[idx-1]
		ch.pren = tot
		//tot += ch.subn()
		if ch.isLeaf {
			tot += 1
		} else {
			tot += ch.inner.SubN
		}
	}
}

func (n *node48) grow() inode {
	nn := &node256{
		lth: n.lth,
	}
	for b, i := range n.keys {
		if i == 0 {
			continue
		}
		nn.children[b] = n.children[i-1]
	}
	nn.redoPren()
	return nn
}

func (n *node48) replace(k int, child *bnode, del bool) (old *bnode) {
	idx := n.keys[k]
	//if idx == 0 {
	//	panic("replace can't be called for idx=0") // make inlinable.
	//}
	old = n.children[idx-1]
	n.children[idx-1] = child
	if child == nil {
		n.keys[k] = 0
		n.lth--
		if del {
			n.redoPren()
		}
	} else {
		if del && child.pren != old.pren {
			n.redoPren()
		}
	}
	return
}

func (n *node48) min() bool {
	return n.lth <= 17
}

func (n *node48) shrink() inode {
	nn := &node16{
		lth: n.lth,
	}
	nni := 0
	for i, idx := range n.keys {
		if idx == 0 {
			continue
		}
		child := n.children[idx-1]
		if child != nil {
			nn.keys[nni] = byte(i)
			nn.children[nni] = child
			nni++
		}
	}
	nn.redoPren()
	return nn
}

func (n *node48) String() string {
	var b bytes.Buffer
	_, _ = b.WriteString("n48[")
	encoder := hex.NewEncoder(&b)
	for i, idx := range n.keys {
		if idx == 0 {
			continue
		}
		child := n.children[idx-1]
		if child != nil {
			_, _ = encoder.Write([]byte{byte(i)})
		}
	}
	_, _ = b.WriteString("]")
	return b.String()
}

// lte: A nil k will return the last key.
//
// Otherwise, we return the largest
// (right-most) key that is <= *k.
//
// A nil bnode back means that all keys were > *k.
func (n *node48) lte(k *byte) (byte, *bnode) {
	for i := 255; i >= 0; i-- {
		w := n.keys[i]
		if w == 0 {
			continue
		}
		b := byte(i)
		if k == nil {
			return b, n.children[w-1]
		}
		if b <= *k {
			return b, n.children[w-1]
		}
	}
	return 0, nil
}

// lt: A nil k will return the last key.
//
// Otherwise, we return the largest
// (right-most) key that is < *k.
//
// A nil bnode back means that all keys were >= *k.
func (n *node48) lt(k *byte) (keyb byte, ch *bnode) {
	for i := 255; i >= 0; i-- {
		w := n.keys[i]
		if w == 0 {
			continue
		}
		b := byte(i)
		if k == nil {
			return b, n.children[w-1]
		}
		if b < *k {
			return b, n.children[w-1]
		}
	}
	return 0, nil
}
