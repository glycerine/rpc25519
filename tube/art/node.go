package art

import (
	"fmt"
	"sync"
	//"sync/atomic"
)

var _ = sync.RWMutex{}

// turn off n4/n16/... fine grained locking,
// depending only on the inner and Leaf RWMutex.
type artlock = nolock

// adds ~ 120 nsec to each operation (800 nsec -> 920 nsec)
// so about 15% longer (under 100% write contention).
//type artlock = sync.RWMutex // turn on n4... locking

type nolock struct{}

func (n *nolock) Lock()    {}
func (n *nolock) Unlock()  {}
func (n *nolock) RLock()   {}
func (n *nolock) RUnlock() {}

// type Key []byte
type kind uint8

const (
	_Leafy kind = iota
	_Node4
	_Node16
	_Node48
	_Node256
)

func bnodeLeaf(lf *Leaf) *bnode {
	return &bnode{
		leaf:   lf,
		isLeaf: true,
	}
}
func bnodeInner(n *inner) *bnode {
	return &bnode{
		inner: n,
	}
}

// bnode replaced node, giving us
// a struct instead of an interface
// for the two types of nodes in the
// tree: inner or leaf nodes.
// ...much easier to deal with.
type bnode struct {
	leaf   *Leaf
	inner  *inner
	isLeaf bool

	// pren is a cache of the sum of
	// the SubN counts for all children
	// earlier to us in our n4/n16/n48/n256 node.
	// Found to be ssential to avoid very expensive
	// summing on the fly of SubN counts
	// during find/get/gte/lte. It allows
	// the LeafIndex functionality to work and be fast.
	pren int
}

func (a *bnode) kind() kind {
	if a.isLeaf {
		return _Leafy
	}
	return a.inner.kind()
}

func (a *bnode) last() (byte, *bnode) {
	if a.isLeaf {
		// fake keyb
		return 0, a
	}
	return a.inner.last()
}

func (a *bnode) first() (byte, *bnode) {
	if a.isLeaf {
		// fake keyb
		return 0, a
	}
	return a.inner.first()
}

func (a *bnode) subn() (count int) {
	if a.isLeaf {
		return 1
	}
	count = a.inner.SubN
	return
}

func (a *bnode) at(i int) (r *Leaf, ok bool) {
	//vv("at(i=%v) called on a='%v'", i, a)
	if i < 0 {
		return nil, false
	}
	if a.isLeaf {
		if i != 0 {
			return nil, false
		}
		return a.leaf, true
	}
	// INVAR: a is inner
	n := a.inner
	if i >= n.SubN {
		// i too large, out of bounds
		return nil, false
	}
	tot := 0
	pre := 0
	subn := 0
	key, b := n.Node.next(nil)
	for b != nil {
		subn = b.subn() // leaf returns 1
		pre = tot
		tot += subn
		//vv("pre=%v; tot=%v; i=%v; subn=%v", pre, tot, i, subn)
		if i < tot {
			return b.at(i - pre)
		}
		key, b = n.Node.next(&key)
	}
	// i too big, out of bounds; but should
	// never been reached because i >= n.SubN
	// already checked above.
	//panic("unreachable") // comment to allow inlining.
	return nil, false
}

func (a *bnode) String() string {
	if a == nil {
		return ""
	}
	if a.isLeaf {
		return a.leaf.String()
	}
	return a.inner.String()
}

func (a *bnode) get(key Key, depth int, selfb *bnode, calldepth int, tree *Tree) (value *bnode, found bool, dir direc, id int) {
	if a.isLeaf {
		return a.leaf.get(key, depth, a)
	}
	return a.inner.get(key, depth, a, calldepth, tree)
}

func (a *bnode) del(key Key, depth int, selfb *bnode, parentUpdate func(*bnode)) (deleted bool, deletedNode *bnode) {
	if a.isLeaf {
		return a.leaf.del(key, depth, selfb, parentUpdate)
	}
	return a.inner.del(key, depth, selfb, parentUpdate)
}

func (a *bnode) insert(lf *Leaf, depth int, selfb *bnode, tree *Tree, par *inner) (*bnode, bool) {
	if a.isLeaf {
		return a.leaf.insert(lf, depth, selfb, tree, par)
	}
	return a.inner.insert(lf, depth, selfb, tree, par)
}

func (a *bnode) FlatString(depth int, recurse int, selfb *bnode) (s string) {
	if a.isLeaf {
		return a.leaf.FlatString(depth, recurse)
	}
	return a.inner.FlatString(depth, recurse, a)
}

func (a *bnode) stringNoKeys(depth int, recurse int, selfb *bnode) (s string) {
	if a.isLeaf {
		return a.leaf.stringNoKeys(depth)
	}
	return a.inner.stringNoKeys(depth, recurse, a)
}

func (k kind) String() string {
	switch k {
	case _Leafy:
		return "leaf"
	case _Node4:
		return "node4"
	case _Node16:
		return "node16"
	case _Node48:
		return "node48"
	case _Node256:
		return "node256"
	}
	//panic(fmt.Sprintf("unknown kind '%v'", int(k)))
	return ""
}

// At() returns the char at key[pos], or a 0 if out of bounds.
func (key Key) At(pos int) byte {
	if pos < 0 || pos >= len(key) {
		// imitate the C-like string termination character
		return 0
	}
	return key[pos]
}

// inner node with varying fanout though
// the Node field. An Inode can have
// node4/node16/node48/node256 inside.
type inner struct {

	// compressed implements path compression.
	compressed []byte

	// try lazy updating of pren to
	// allow bulk writes to not trash the L1 cache
	// on doing redoPren() every time.
	// The get/gte/lte queries will need to check this,
	// and it will need to propagate up from inserts
	// so that parents know their pren is stale too;
	// really just the same as when SubN is updated.
	// renamed to prenOK instead of stalepren so that
	// the default is false.
	prenOK bool

	// Note: keep this commented out path field for debugging!
	// For sane debugging, comment this in
	// back in to store the full path on each inner node.
	//path []byte

	// counted B-tree style: how many
	// leaves are stored in our sub-tree.
	SubN int

	// Node holds one of node4, node16, node48, or node256.
	// inode is an interface that all of them implement.
	Node inode

	// keybyte gives the byte that leads
	// to us in the parent index.
	keybyte byte
}

func (n *inner) gte(k *byte) (byte, *bnode) {
	return n.Node.gte(k)
}
func (n *inner) gt(k *byte) (byte, *bnode) {
	return n.Node.gt(k)
}
func (n *inner) lte(k *byte) (byte, *bnode) {
	return n.Node.lte(k)
}
func (n *inner) lt(k *byte) (byte, *bnode) {
	return n.Node.lt(k)
}

func (n *inner) last() (byte, *bnode) {
	return n.Node.last()
}

func (n *inner) first() (byte, *bnode) {
	return n.Node.first()
}

func (a *bnode) redoPren() {
	return // lazy now
	if a.isLeaf {
		return
	}
	a.inner.Node.redoPren()
}

// implemented by node4, node16, node48, node256
type inode interface {
	// re-compute the cumulative previous child subN cache
	redoPren()
	// last gives the greatest key (right-most) child
	first() (byte, *bnode)
	last() (byte, *bnode)
	gt(*byte) (byte, *bnode)
	gte(*byte) (byte, *bnode)
	lt(*byte) (byte, *bnode)
	lte(*byte) (byte, *bnode)
	//depth() int
	//setDepth(d int)
	nchild() int
	childkeysString() string
	kind() kind
	// next returns child after the requested byte
	// if byte is nil - returns leftmost (first) child
	next(*byte) (byte, *bnode)
	prev(*byte) (byte, *bnode)

	// child return index of the child together with the child
	child(byte) (int, *bnode)
	// addChild inserts child at the specified byte
	addChild(byte, *bnode)
	// these call addChid after constructing the anode
	//addLeafChild(k byte, child *Leaf)
	//addInnerChild(k byte, child *inner)

	// replace updates node at specified index
	// if node is nil - delete the node and adjust metadata.
	// return replaced node
	replace(idx int, child *bnode, insert bool) *bnode

	// full is true if node reached max size
	full() bool
	// grow the node to next size
	// node256 can't grow and will return nil
	grow() inode

	// min is true if node reached min size
	min() bool
	// shrink is the opposite to grow
	// if node is of the smallest type (node4) nil will be returned
	shrink() inode

	String() string
}

// get the smallest key/first (left-most) leaf in our subtree.
func (b *bnode) recursiveFirst() (lf *bnode, ok bool) {
	for {
		// recurse until we hit the leaf.
		if b.isLeaf {
			return b, true
		}
		_, b = b.first()
		if b == nil {
			//panic("should never happen!") // comment to allow inlining
			return nil, false
		}
	}
}

// get the smallest key/first (left-most) leaf in our subtree.
func (n *inner) recursiveFirst() (lf *bnode, ok bool) {
	_, b := n.Node.first()
	if b.isLeaf {
		return b, true
	}
	return b.recursiveFirst()
}

// get the larget key/last (right-most) leaf in our subtree
func (n *inner) recursiveLast() (lf *bnode, ok bool) {
	_, b := n.Node.last()
	if b.isLeaf {
		return b, true
	}
	return b.recursiveLast()
}

// get the larget key/last (right-most) leaf in our subtree
func (b *bnode) recursiveLast() (lf *bnode, ok bool) {
	for {
		if b.isLeaf {
			return b, true
		}
		// recurse until we hit the leaf.
		_, b = b.last()
		if b == nil {
			//panic("should be impossible! ") // comment to allow inlining.
			return nil, false
		}
	}
}

func (a *bnode) prev(k *byte) (byte, *bnode) {
	if a.isLeaf {
		return 0, nil
	}
	return a.inner.Node.prev(k)
}

func (a *bnode) next(k *byte) (byte, *bnode) {
	if a.isLeaf {
		return 0, nil
	}
	return a.inner.Node.next(k)
}

func (a *bnode) getGTE(key Key, depth int, smod SearchModifier, selfb *bnode, tree *Tree, calldepth int, smallestWillDo bool, keyCmpPath int) (value *bnode, found bool, dir direc, id int) {
	if a.isLeaf {
		return a.leaf.get(key, depth, a)
	}
	return a.inner.getGTE(key, depth, smod, a, tree, calldepth, smallestWillDo, keyCmpPath)
}

func (a *bnode) getLTE(key Key, depth int, smod SearchModifier, selfb *bnode, tree *Tree, calldepth int, largestWillDo bool, keyCmpPath int) (value *bnode, found bool, dir direc, id int) {
	if a.isLeaf {
		return a.leaf.get(key, depth, a)
	}
	return a.inner.getLTE(key, depth, smod, a, tree, calldepth, largestWillDo, keyCmpPath)
}

// lazily do the minimum amount of
// summing to redo pren on any inner
// node with prenOK = false marking.
func (b *bnode) subTreeRedoPren() (leafcount int) {

	if b == nil {
		return 0
	}
	if b.isLeaf {
		return 1
	}
	if b.inner.prenOK {
		return b.inner.SubN
	}
	// INVAR: b is an inner, and has a stale pren somewhere.

	var pren int
	var subn int

	inode := b.inner.Node
	switch n := inode.(type) {
	case *node4:
		for i, ch := range n.children {
			if i < n.lth {
				subn = ch.subTreeRedoPren()
				leafcount += subn

				// update ch.pren
				ch.pren = pren
				pren += subn
			}
		}
	case *node16:
		for i, ch := range n.children {
			if i < n.lth {
				subn = ch.subTreeRedoPren()
				leafcount += subn

				// update ch.pren
				ch.pren = pren
				pren += subn
			}
		}
	case *node48:
		for _, k := range n.keys {

			if k == 0 {
				continue
			}
			ch := n.children[k-1]
			subn = ch.subTreeRedoPren()
			leafcount += subn

			// update ch.pren
			ch.pren = pren
			pren += subn
		}
	case *node256:
		for _, ch := range n.children {
			if ch != nil {
				subn = ch.subTreeRedoPren()
				leafcount += subn

				// update ch.pren
				ch.pren = pren
				pren += subn
			}
		}
	}

	// todo remove this, once sanity check ensured.
	if b.inner.SubN != leafcount {
		panic(fmt.Sprintf("leafcount=%v, but n.SubN = %v", leafcount, b.inner.SubN))
	}

	// mark ourselves ok good now,
	// to avoid a ton of recursion.
	b.inner.prenOK = true

	return leafcount
}
