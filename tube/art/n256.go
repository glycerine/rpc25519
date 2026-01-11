package art

import (
	"bytes"
	"encoding/hex"
	"fmt"
)

type node256 struct {
	lth      int
	children [256]*bnode
}

func (n *node256) last() (byte, *bnode) {
	for i := 255; i >= 0; i-- {
		ch := n.children[i]
		if ch != nil {
			return byte(i), ch
		}
	}
	//panic("unreachable since node256 must have min 49 children")
	return 0, nil // forgo panic, try to make inline-able.
}

func (n *node256) first() (byte, *bnode) {
	for i, ch := range n.children {
		if ch != nil {
			return byte(i), ch
		}
	}
	//panic("unreachable since node256 must have min 49 children")
	return 0, nil // forgo panic, try to make inline-able.
}

func (n *node256) nchild() int {
	return int(n.lth)
}

func (n *node256) childkeysString() (s string) {
	s = "["
	for k, ch := range n.children {
		if ch != nil {
			if s != "" {
				s += ", "
			}
			if k == 0 {
				s += "zero"
			} else {
				if k < 33 || k > '~' {
					s += fmt.Sprintf("0x%x", byte(k))
				} else {
					s += fmt.Sprintf("'%v'", string(byte(k)))
				}
			}
		}
	}
	return s + "]"
}

func (n *node256) kind() kind {
	return _Node256
}

func (n *node256) child(k byte) (int, *bnode) {
	return int(k), n.children[k]
}

func (n *node256) next(k *byte) (byte, *bnode) {
	for b, child := range n.children {
		if (k == nil || byte(b) > *k) && child != nil {
			return byte(b), child
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
func (n *node256) gte(k *byte) (byte, *bnode) {
	for b, child := range n.children {
		if (k == nil || byte(b) >= *k) && child != nil {
			return byte(b), child
		}
	}
	return 0, nil
}

func (n *node256) gt(k *byte) (keyb byte, ch *bnode) {
	if k == nil {
		// request for 1st key
		return n.gte(nil)
	}
	// INVAR: k != nil
	for b, child := range n.children {
		if (byte(b) > *k) && child != nil {
			return byte(b), child
		}
	}
	return 0, nil
}

func (n *node256) prev(k *byte) (byte, *bnode) {
	for idx := 255; idx >= 0; idx-- {
		b := byte(idx)
		child := n.children[idx]
		if (k == nil || b < *k) && child != nil {
			return b, child
		}
	}
	return 0, nil
}

func (n *node256) replace(idx int, child *bnode, del bool) (old *bnode) {
	old = n.children[byte(idx)]
	n.children[byte(idx)] = child
	if child == nil {
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

func (n *node256) full() bool {
	return n.lth == 256
}

func (n *node256) addChild(k byte, child *bnode) {
	n.children[k] = child
	n.lth++
	n.redoPren()
}

// update pren cache of cumulative SubN
func (n *node256) redoPren() {
	return // lazy now, use prenOK to determine when.

	tot := 0
	for _, ch := range n.children {
		if ch == nil {
			continue
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

func (n *node256) grow() inode {
	return nil
}

func (n *node256) min() bool {
	return n.lth <= 49
}

func (n *node256) shrink() inode {
	nn := &node48{
		lth: n.lth,
	}
	var index uint16
	for i := range n.children {
		if n.children[i] == nil {
			continue
		}
		index++
		nn.keys[i] = index
		nn.children[index-1] = n.children[i]
	}
	nn.redoPren()
	return nn
}

func (n *node256) String() string {
	var b bytes.Buffer
	_, _ = b.WriteString("n256[")
	encoder := hex.NewEncoder(&b)
	for i := range n.children {
		if n.children[i] != nil {
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
func (n *node256) lte(k *byte) (keyb byte, ch *bnode) {
	for i := 255; i >= 0; i-- {
		ch = n.children[i]
		if ch != nil {
			keyb = byte(i)
			if k == nil {
				// return the last (largest) key.
				return
			}
			if keyb <= *k {
				return
			}
		}
	}
	return 0, nil
}

// lt: A nil k will return the last key.
//
// Otherwise, we return the largest
// (right-most) key that is < *k.
//
// A nil ch *bnode back means that all keys were >= *k.
func (n *node256) lt(k *byte) (keyb byte, ch *bnode) {
	for i := 255; i >= 0; i-- {
		ch = n.children[i]
		if ch != nil {
			keyb = byte(i)
			if k == nil {
				// return the last (largest) key.
				return
			}
			if keyb < *k {
				return
			}
		}
	}
	return 0, nil
}
