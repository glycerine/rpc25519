package art

import "fmt"

type node4 struct {
	lth      int
	children [4]*bnode
	// benchmarking likes keys at end when mostly reading.
	keys [4]byte
}

func (n *node4) kind() kind {
	return _Node4
}

func (n *node4) nchild() int {
	return int(n.lth)
}

func (n *node4) childkeysString() (s string) {
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

// index returns the first child (index) whose key <= k.
// indexLTE would be an equivalent name
func (n *node4) index(k byte) int {
	for i, b := range n.keys {
		if k <= b {
			return i
		}
	}
	return int(n.lth)
}

func (n *node4) last() (byte, *bnode) {
	return n.keys[n.lth-1], n.children[n.lth-1]
}

func (n *node4) first() (byte, *bnode) {
	return n.keys[0], n.children[0]
}

func (n *node4) next(k *byte) (byte, *bnode) {
	if k == nil {
		return n.keys[0], n.children[0]
	}
	for idx, b := range n.keys {
		if b > *k {
			return b, n.children[idx]
		}
	}
	return 0, nil
}

// gt: A nil k will return the first key.
//
// Otherwise, we return the first
// (smallest) key that is > *k.
//
// A nil bnode back means that all keys were <= *k.
func (n *node4) gt(k *byte) (byte, *bnode) {
	if k == nil {
		return n.keys[0], n.children[0]
	}
	for idx, b := range n.keys {
		if b > *k {
			return b, n.children[idx]
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
func (n *node4) gte(k *byte) (byte, *bnode) {
	if k == nil {
		return n.keys[0], n.children[0]
	}
	for idx, b := range n.keys {
		if b >= *k {
			return b, n.children[idx]
		}
	}
	return 0, nil
}

func (n *node4) prev(k *byte) (byte, *bnode) {
	if n.lth == 0 {
		return 0, nil
	}
	if k == nil {
		idx := n.lth - 1
		return n.keys[idx], n.children[idx]
	}
	// we use an int for lth to avoid underflow.
	for i := n.lth - 1; i >= 0; i-- {
		if n.keys[i] < *k {
			return n.keys[i], n.children[i]
		}
	}
	return 0, nil
}

func (n *node4) child(k byte) (idx int, ch *bnode) {

	for i, b := range n.keys {
		if k == b {
			return i, n.children[i]
		}
	}
	return 0, nil
}

func (n *node4) addChild(k byte, child *bnode) {
	idx := n.index(k)
	copy(n.children[idx+1:], n.children[idx:])
	copy(n.keys[idx+1:], n.keys[idx:])
	n.keys[idx] = k
	n.children[idx] = child
	n.lth++
	n.redoPren()
}

// update pren cache of cumulative SubN
func (n *node4) redoPren() {
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
func (n *node4) replace(idx int, child *bnode, del bool) (old *bnode) {
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

func (n *node4) full() bool {
	return n.lth == 4
}

func (n *node4) grow() inode {
	nn := &node16{}
	nn.lth = n.lth
	copy(nn.keys[:], n.keys[:])
	copy(nn.children[:], n.children[:])
	nn.redoPren()
	return nn
}

func (n *node4) min() bool {
	return n.lth <= 2
}

func (n *node4) shrink() inode {
	panic("can't shrink node4")
}

func (n *node4) String() (s string) {
	s = "n4[keys: "
	for i := range n.lth {
		if i > 0 {
			s += ","
		}
		s += fmt.Sprintf("%v", string(n.keys[:n.lth]))
	}
	s += "]"
	return
}

// lt: return the largest key < k, or nil if none.
// A nil k request returns the largest key.
func (n *node4) lt(k *byte) (keyb byte, ch *bnode) {
	if k == nil {
		// want the largest key
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

// A nil k will return the last key.
//
// Otherwise, we return the largest
// (right-most) key that is <= *k.
//
// A nil bnode back means that all keys were > *k.
func (n *node4) lte(k *byte) (byte, *bnode) {
	if k == nil {
		// want the largest key
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
