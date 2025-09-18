package art

import "fmt"

type node16 struct {
	lth      int
	keys     [16]byte
	children [16]*bnode
}

func (n *node16) last() (byte, *bnode) {
	return n.keys[n.lth-1], n.children[n.lth-1]
}
func (n *node16) first() (byte, *bnode) {
	return n.keys[0], n.children[0]
}

func (n *node16) nchild() int {
	return int(n.lth)
}
func (n *node16) childkeysString() (s string) {
	s = "["
	for i := range n.lth {
		if i > 0 {
			s += ", "
		}
		k := n.keys[i]
		if k == 0 {
			s += "zero"
		} else {
			if k < 33 || k > '~' {
				s += fmt.Sprintf("0x%x", byte(k))
			} else {
				s += fmt.Sprintf("'%v'", string(k))
			}
		}
	}
	return s + "]"
}

func (n *node16) kind() kind {
	return _Node16
}

func (n *node16) index(k byte) int {
	for i, b := range n.keys {
		if k <= b {
			return i
		}
	}
	return int(n.lth)
}

// we get more inlining by putting this in the
// same file. Faster than the assembly.
func index(key *byte, nkey *[16]byte) (int, bool) {
	for i := range nkey {
		if nkey[i] == *key {
			return i, true
		}
	}
	return 0, false
}

func (n *node16) child(k byte) (idx int, ch *bnode) {
	var key byte
	for idx, key = range n.keys {
		if key == k {
			ch = n.children[idx]
			return
		}
	}
	return
}

func (n *node16) next(k *byte) (byte, *bnode) {
	if k == nil {
		return n.keys[0], n.children[0]
	}
	for i, b := range n.keys {
		if b > *k {
			return b, n.children[i]
		}
	}
	return 0, nil
}

// A nil k will return the first key.
//
// Otherwise, we return the first
// (smallest) key that is >= *k.
//
// A nil bnode back means that all keys were < *k.
func (n *node16) gte(k *byte) (byte, *bnode) {
	if k == nil {
		return n.keys[0], n.children[0]
	}

	for i, b := range n.keys {
		if b >= *k {
			return b, n.children[i]
		}
	}
	return 0, nil
}

// A nil k will return the first key.
//
// Otherwise, we return the first
// (smallest) key that is > *k.
//
// A nil bnode back means that all keys were <= *k.
func (n *node16) gt(k *byte) (byte, *bnode) {
	if k == nil {
		return n.keys[0], n.children[0]
	}
	for i, b := range n.keys {
		if b > *k {
			return b, n.children[i]
		}
	}
	return 0, nil
}

func (n *node16) prev(k *byte) (byte, *bnode) {
	if k == nil {
		idx := n.lth - 1
		return n.keys[idx], n.children[idx]
	}
	// we use an int for lnt now to avoid underflow.
	for i := n.lth - 1; i >= 0; i-- {
		if n.keys[i] < *k {
			return n.keys[i], n.children[i]
		}
	}
	return 0, nil
}

func (n *node16) replace(idx int, child *bnode, del bool) (old *bnode) {
	old = n.children[idx]
	if child == nil {
		copy(n.keys[idx:], n.keys[idx+1:])
		copy(n.children[idx:], n.children[idx+1:])
		n.keys[n.lth-1] = 0
		n.children[n.lth-1] = nil
		n.lth--
		if del && idx < n.lth {
			n.redoPren()
		}
	} else {
		n.children[idx] = child
		if del && child.pren != old.pren {
			n.redoPren()
		}
	}
	return
}

func (n *node16) full() bool {
	return n.lth == 16
}

func (n *node16) addChild(k byte, child *bnode) {
	idx := n.index(k)
	copy(n.children[idx+1:], n.children[idx:])
	copy(n.keys[idx+1:], n.keys[idx:])
	n.keys[idx] = k
	n.children[idx] = child
	n.lth++
	n.redoPren()
}

// update pren cache of cumulative SubN
func (n *node16) redoPren() {
	return // lazy now, use prenOK to determine when.

	tot := 0
	for i, ch := range n.children {
		if i >= n.lth {
			break
		}
		ch.pren = tot
		//tot += ch.subn()
		if ch.isLeaf {
			tot += 1
		} else {
			tot += ch.inner.SubN
		}
	}
}

func (n *node16) grow() inode {
	nn := &node48{
		lth: n.lth,
	}
	copy(nn.children[:], n.children[:])
	for i, child := range n.children {
		if child == nil {
			continue
		}
		nn.keys[n.keys[i]] = uint16(i) + 1
	}
	nn.redoPren()
	return nn
}

func (n *node16) min() bool {
	return n.lth <= 5
}

func (n *node16) shrink() inode {
	nn := node4{}
	copy(nn.keys[:], n.keys[:])
	copy(nn.children[:], n.children[:])
	nn.lth = n.lth
	nn.redoPren()
	return &nn
}

func (n *node16) String() string {
	return fmt.Sprintf("n16[%x]", n.keys[:n.lth])
}

// lt (less than): A nil k will return the last key.
//
// Otherwise, we return the largest
// (right-most) key that is < *k.
//
// A nil bnode back means that all keys were > *k.
func (n *node16) lt(k *byte) (keyb byte, ch *bnode) {
	if k == nil {
		return n.keys[n.lth-1], n.children[n.lth-1]
	}
	for idx := n.lth - 1; idx >= 0; idx-- {
		b := n.keys[idx]
		if b < *k {
			return b, n.children[idx]
		}
	}
	return 0, nil
}

// lte: A nil k will return the last key.
//
// Otherwise, we return the largest
// (right-most) key that is <= *k.
//
// A nil bnode back means that all keys were > *k.
func (n *node16) lte(k *byte) (byte, *bnode) {
	if k == nil {
		return n.keys[n.lth-1], n.children[n.lth-1]
	}
	for idx := n.lth - 1; idx >= 0; idx-- {
		b := n.keys[idx]
		if b <= *k {
			return b, n.children[idx]
		}
	}
	return 0, nil
}
