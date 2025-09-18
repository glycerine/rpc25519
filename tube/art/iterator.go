package art

import (
	"bytes"
	//"fmt"
	"iter"
)

// define an interface only so that
// the documentation for Next() appears in godoc.

// An Iterator will scan the tree in
// lexicographic (shortlex) order. See the Iter() and
// RevIter() methods on Tree.
type Iterator interface {

	// Next() must be called to start the iteration before
	// Key(), Value(), or Leaf() will be meaningful.
	//
	// Next will iterate over all leaf nodes in
	// the specified range in the chosen direction.
	//
	// When the iteration is done, Next returns false.
	//
	// If the tree is modified between calls to Next,
	// a version change will be recognized, and
	// Next will transparently resume iteration
	// from the successor to the last returned key.
	Next() (ok bool)

	// Leaf returns the current leaf in
	// the iterator after the first successful
	// Next() call.
	Leaf() *Leaf

	// Value returns the current value of
	// the iterator after the first successful
	// Next() call. This is the same as Leaf().Value
	Value() []byte

	// Index returns the current integer index of
	// the iterator after the first successful
	// Next() call.
	Index() int

	// Key returns the current key of
	// the iterator after the first successful
	// Next() call.
	//
	// Warning: the user must not modify
	// this returned key! The tree depends on
	// its value for correctness. Make a copy
	// before modifying the copy. To change
	// a key in the tree, call tree.Remove(old)
	// and then tree.Insert(new) with the new key.
	Key() Key
}

type iterator struct {
	tree *Tree

	treeVersion int64

	stack *checkpoint

	initDone bool
	closed   bool

	start     []byte
	cursor    []byte
	terminate []byte

	reverse bool

	begIdx int // corresponding to initial key
	curIdx int // corresponding to current key after the first Next()
	//endxIdx int // corresponding to 1 past the last key

	// current:
	key   []byte
	value []byte
	leaf  *Leaf
}

// Iter starts a traversal over the range [start, end)
// in ascending order.
//
// iter.Next() must be called to start the iteration before
// iter.Key(), iter.Value(), or iter.Leaf() will be meaningful.
//
// When iter.Next() returns false, the iteration
// has completed.
//
// We begin with the first key that is >= start and < end.
//
// The end key must be > the start key, or no values
// will be returned. Either start or end
// can be nil to indicate the furthest possible range
// in that direction.
//
// For example, note that [x, x) will return the
// empty set, unless x is nil. If x _is_ nil, this
// will return the entire tree in ascending order.
//
// For another example, suppose the keys {0, 1, 2} are
// in the tree, and tree.Iter(0, 2) is called.
// Forward iteration will return 0, then 1.
//
// The returned iterator is not concurrent/multiple goroutine safe.
// Iteration does no synchronization. This
// allows for single goroutine code that deletes from
// (or inserts into) the tree during the iteration,
// which is not an uncommon need.
func (t *Tree) Iter(start, end []byte) (iter *iterator) {

	if t.root == nil || t.size < 1 {
		return &iterator{
			initDone: true,
			closed:   true,
		}
	}

	// get the integer range [begIdx, endIdx]
	_, begIdx, ok := t.find_unlocked(GTE, start)
	if !ok {
		// no such key to start from, iteration already over.
		return &iterator{
			initDone: true,
			closed:   true,
		}
	}

	_, endIdx, ok := t.find_unlocked(LT, end)
	_ = endIdx // might need in future. not used atm.
	if !ok {
		return &iterator{
			initDone: true,
			closed:   true,
		}
	}

	return &iterator{
		tree:        t,
		treeVersion: t.treeVersion,
		start:       start,
		cursor:      start,
		terminate:   end,
		begIdx:      begIdx,
		curIdx:      begIdx - 1,
		//endxIdx:     endIdx + 1,
	}
}

// RevIter starts a traversal over
// the range (end, start] in descending order.
//
// Note that the first argument to RevIter()
// is the smaller (if the two differ), assuming
// you don't want the empty set.
// This is true for Iter() as well.
//
// iter.Next() must be called to start the iteration before
// iter.Key(), iter.Value(), or iter.Leaf() will be meaningful.
//
// When iter.Next() returns false, the iteration
// has completed.
//
// We begin with the first key that is <= start and > end.
//
// The end key must be < the start key, or no values
// will be returned. Either start or end
// can be nil to indicate the furthest possible range
// in that direction.
//
// For example, note that (x, x] will return the
// empty set, unless x is nil. If x _is_ nil, this
// will return the entire tree in descending order.
//
// For another example, suppose the keys {0, 1, 2} are
// in the tree, and tree.RevIter(0, 2) is called.
// Reverse iteration will return 2, then 1.
// The same holds true if start (2 here) is replaced by
// by any integer > 2.
//
// tree.RevIter(nil, 2) will yield 2, then 1, then 0;
// as will tree.RevIter(nil, nil).
//
// The returned iterator is not concurrent/multiple goroutine safe.
// Iteration does no synchronization. This
// allows for single goroutine code that deletes from
// (or inserts into) the tree during the iteration,
// which is not an uncommon need.
func (t *Tree) RevIter(end, start []byte) (iter *iterator) {

	if t.root == nil || t.size < 1 {
		return &iterator{
			initDone: true,
			closed:   true,
		}
	}

	// get the integer range [endIdx, begIdx]
	_, begIdx, ok := t.find_unlocked(LTE, start)
	if !ok {
		//vv("FindLTE start found nothing!")
		return &iterator{
			initDone: true,
			closed:   true,
		}
	}

	gtLeaf, endIdx, ok := t.find_unlocked(GT, end)
	_ = gtLeaf
	_ = endIdx // might need in future, not used atm.
	if !ok {
		//vv("in revIt: FindGT(end='%v') got ok=false, endIdx = %v; gtLeaf='%v'", string(end), endIdx, gtLeaf)
		// this is okay if end is nil, of course

		//vv("FindGT end found nothing! end='%v'", string(end))
		return &iterator{
			initDone: true,
			closed:   true,
		}
	}

	//vv("revIt starting cursor=start='%v'", string(start))
	return &iterator{
		tree:        t,
		treeVersion: t.treeVersion,
		reverse:     true,
		start:       start,
		cursor:      start,
		terminate:   end,
		begIdx:      begIdx,
		curIdx:      begIdx + 1,
		//endxIdx:     endIdx - 1,
	}
}

type checkpoint struct {
	node   *inner
	curkey *byte

	prev *checkpoint
}

// Next will iterate over all leaf nodes in
// the specified range in the chosen direction.
func (i *iterator) Next() (ok bool) {
	if i.closed {
		return false
	}
	if i.treeVersion != i.tree.treeVersion {
		// there has been a modification
		// to the tree, reset the stack and
		// indexes. Proceed from the
		// last provided key+1 (-1 for reverse).
		//vv("tree modified, reseting iterator state")

		smod := GT
		if i.reverse {
			smod = LT
		}
		leaf, idx, ok := i.tree.find_unlocked(smod, i.cursor)
		if !ok {
			//vv("ugh. could not find successor to i.cursor '%v'. terminating iteration", string(i.cursor))

			// user modification may have deleted all further keys,
			// so terminate the iteration.
			i.closed = true
			return false
		}
		i.curIdx = idx
		i.leaf = leaf
		i.key = leaf.Key
		i.value = leaf.Value
		i.treeVersion = i.tree.treeVersion

		// reset the stack from scratch
		i.cursor = leaf.Key
		i.stack = nil
		// let re-init code below start the stack again.

	} else {
		// no change in treeVersion
		if i.reverse {
			i.curIdx--
		} else {
			//vv("incrementing i.curIdx to %v", i.curIdx+1)
			i.curIdx++
		}
	}

	if i.stack == nil {
		// initialize iterator
		if exit, next := i.init(); exit {
			return next
		}
	}
	ok = i.iterate()

	// these are assertions that were useful for catching issues,
	// but just slow down production use.
	// if false {
	// 	if ok {
	// 		// confirm our indexes are in correspondence.
	// 		_, leafIdx, leafIdxOK := i.tree.find_unlocked(Exact, i.leaf.Key)
	// 		if !leafIdxOK {
	// 			panic("iterate was ok but LeafIndex was not")
	// 		}
	// 		if leafIdx != i.curIdx {
	// 			panic(fmt.Sprintf("leafIdx = %v but i.curIdx = %v; i.leaf='%v'", leafIdx, i.curIdx, i.leaf))
	// 		}
	// 	}
	// }
	return
}

// exit returned true means only 0 or 1 nodes in tree,
// so Next() won't call iterate.
func (i *iterator) init() (exit bool, nextOK bool) {

	root := i.tree.root
	if root == nil {
		i.closed = true
		return true, false
	}

	if root.isLeaf {
		l := root.leaf
		i.closed = true
		if i.inRange(l.Key) {
			i.key = l.Key
			i.value = l.Value
			i.leaf = l
			return true, true
		}
		return true, false
	}
	i.stack = &checkpoint{
		node: root.inner,
	}
	return false, false
}

func (i *iterator) iterate() bool {
	for i.stack != nil {
		more, restart := i.tryAdvance()
		if more {
			return more
		} else if restart {
			i.stack = i.stack.prev
			if i.stack == nil {
				// checkpoint is root
				i.stack = nil
				if exit, next := i.init(); exit {
					return next
				}
			}
		}
	}
	i.closed = true
	return false
}

func (i *iterator) tryAdvance() (bool, bool) {
	//vv("top of tryAdvance")
	//defer vv("end of tryAdvance")

	for adv := 0; ; adv++ {
		_ = adv

		tail := i.stack

		//vv("tryAdv calling i.next() with tail.curkey = '%#v'", tail.curkey) // nil on first call
		curkey, child := i.next(tail.node, tail.curkey)
		if child == nil {

			// inner node is exhausted, move one level up the stack
			i.stack = tail.prev
			return false, false
		}
		// advance curkey
		//vv("setting tail.curkey = '%v'", string(curkey))
		tail.curkey = &curkey

		if child.isLeaf {
			l := child.leaf
			if i.inRange(l.Key) {
				//vv("inRange true")
				i.key = l.Key
				i.value = l.Value
				i.cursor = l.Key
				i.leaf = l
				return true, false
			}
			//vv("inRange false")
			return false, false

		}
		i.stack = &checkpoint{
			node: child.inner,
			prev: tail,
		}
		return false, false
	}
}

func (i *iterator) next(n *inner, curkey *byte) (keyb byte, b *bnode) {
	//defer func() {
	//	vv("it.next returning keyb='%v', b='%v'", string(keyb), b.String())
	//}()
	if !i.reverse {
		return n.Node.next(curkey)
	}
	return n.Node.prev(curkey)
}

func (i *iterator) Leaf() *Leaf {
	return i.leaf
}

func (i *iterator) Value() []byte {
	return i.value
}

func (i *iterator) Index() int {
	return i.curIdx
}

// Key returns the current value of
// the iterator after the first successful
// Next() call.
//
// Warning: the user must not modify
// the key -- in particular if concurrent changes to
// the tree are made. We depend
// on its value to reset and continue
// the iteration after any tree changes.
//
// After tree modification, we continue
// from the successor to the last good key.
func (i *iterator) Key() Key {
	return i.key
}

func (i *iterator) inRange(key []byte) (inside bool) {
	//defer func() {
	//vv("inRange returns inside=%v; reverse is %v; key='%v'; cursor='%v; terminate='%v'", inside, i.reverse, string(key), string(i.cursor), string(i.terminate))
	//}()
	if i.reverse {
		return (bytes.Compare(key, i.cursor) <= 0 || len(i.cursor) == 0) && (len(i.terminate) == 0 || bytes.Compare(key, i.terminate) > 0)
	}
	// forward iteration:
	return bytes.Compare(key, i.cursor) >= 0 && (len(i.terminate) == 0 || bytes.Compare(key, i.terminate) < 0)
}

// Ascend wraps a tree.Iter() iteration in
// ascending lexicographic (shortlex) order. See the
// Tree.Iter description for details.
func Ascend(t *Tree, beg, endx Key) iter.Seq2[Key, *Leaf] {
	return func(yield func(key Key, value *Leaf) bool) {
		it := t.Iter(beg, endx)
		for it.Next() {
			if !yield(it.Key(), it.Leaf()) {
				return
			}
		}
	}
}

// Descend iterates from highest to lowest key
// in lexicographic (shortlex) order. Perhaps counter-intuitively,
// the smaller (endx) key is always the first argument.
// "Smallest-first" is an easy
// way to remember this, as it applies to both
// directions. Descend is a simple wrapper around
// the RevIter method. In reverse iteration,
// the range covered is (endx, start], so the
// endx key itself will not be seen. Use
// Descend(nil, nil) to see all keys in the tree.
func Descend(t *Tree, endx, start Key) iter.Seq2[Key, *Leaf] {
	return func(yield func(key Key, value *Leaf) bool) {
		it := t.RevIter(endx, start)
		for it.Next() {
			if !yield(it.Key(), it.Leaf()) {
				return
			}
		}
	}
}

// dfs does depth-first-search.
//
// Useful for debugging/visualizing
// the full tree. Used in some tests.
func dfs(root *bnode) iter.Seq2[*bnode, bool] {
	return func(yield func(*bnode, bool) bool) {

		// Helper function for recursive traversal
		var visit func(keybyte byte, root *bnode, depth int) bool
		visit = func(keybyte byte, root *bnode, d int) bool {

			if root.isLeaf {
				//case *Leaf:
				return yield(root, true)
			} else {
				//case *inner:
				inode := root.inner.Node // interface
				switch n := inode.(type) {
				case *node4:
					for i := range n.children {
						if i < n.lth {
							if !visit(n.keys[i], n.children[i], d+1) {
								return false
							}
						}
					}
				case *node16:
					for i := range n.children {
						if i < n.lth {
							if !visit(n.keys[i], n.children[i], d+1) {
								return false
							}
						}
					}
				case *node48:
					for i, k := range n.keys {
						if k == 0 {
							continue
						}
						child := n.children[k-1]
						if !visit(byte(i), child, d+1) {
							return false
						}
					}
				case *node256:
					for i, child := range n.children {
						if child != nil {
							if !visit(byte(i), child, d+1) {
								return false
							}
						}
					}
				}
				// self after children
				return yield(root, true)
			}
			return true
		}
		// Start the recursion

		// the root keybyte is zero always.
		// This a pretense as there is no
		// keybyte that leads to the root really.
		var k byte

		if root.isLeaf {
			yield(root, true)
			return
		}
		visit(k, root, 0)
	}
}
