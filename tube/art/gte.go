package art

import (
	"bytes"
	"fmt"
)

var _ = fmt.Sprintf

// getGTE returns a leaf with a leaf.Key >= key.
// And, importantly, it returns the leaf with the
// smallest such key (the smallest leaf.Key that is >= key).
//
// Since we don't want just any leaf > key, but
// the smallest available, we have to find where the key
// would be inserted (or its exact matching location,
// if the key is already in the tree).
//
// When we are back-tracking
// up the tree, we need some way to convey
// smallestWillDo to the caller. We added
// another dir state; 2 to mean greater (search
// forward) but smallest will do. A caller
// can, in turn, convey this to other
// recursive invocations of getGTE() in
// its smallestWillDo parameter.
//
// So: set smallestWillDo when we are in the phase
// of the search where we know anything in
// this subtree will be GTE our query. This
// is not true at the start on the full tree,
// of course, because that would always
// return the minimum leaf. But once we
// have found the natural insert place for key,
// we can leverage the fact that any leaf
// after that will be GTE, even if we
// have to backtrack up and back-down-to-the-right
// in the tree to find it. Simply put, the +2
// direction means go forward (greater keys) and
// return the very next (smallest) key/leaf on
// the forward path.
//
// The keyCmpPath maintains the comparison of
// key to path as we descend the tree. It
// saves us from having to store the full
// paths in the inner nodes, which would
// consume lots of redundant memory.
//
// In debugging, the paths are super helpful,
// so we'll try to retain some efficient
// means of activating them for diagnostics.
//
// On GTE verus GT search:
//
// GT is identical to GTE until we find a leaf.
// Then GT advances by one iff the leaf is
// an exact match to the key. The differences
// are compact and found right after the
// first getGTE() recursion.
func (n *inner) getGTE(
	key Key,
	depth int, // how far along the key we are
	smod SearchModifier,
	selfb *bnode, // selfb.inner == n
	tree *Tree,
	calldepth int, // how deep in recurisve getGTE calls are we.
	smallestWillDo bool, // want the next largest key.
	keyCmpPath int, // equal to binary.Compare(key, n.path)

) (value *bnode, found bool, dir direc, id int) {

	//pp("%p top of getGTE(key='%v'), path='%v'; we are '%v' %v;  smallestWillDo=%v; calldepth='%v'; smod='%v'", n, string(key), string(n.path), n.FlatString(depth, 0), n.rangestr(), smallestWillDo, calldepth, smod)

	//defer func() {
	//pp("%p returning from calldepth=%v getGTE(key='%v') value='%v', found='%v'; dir='%v'; id='%v'; my inner %v;  smallestWillDo=%v; id=%v; smod='%v'", n, calldepth, string(key), value, found, dir, id, n.rangestr(), smallestWillDo, id, smod)
	//}()

	// PHASE ZERO: smallestWillDo during backtracking.
	if smallestWillDo {
		dir = 0
		found = true
		value, _ = n.recursiveFirst()
		// id 0 is correct.
		return
	}

	// So smallestWillDo is false
	// for a while after here, but it does
	// get set below for some of the
	// recursive calls, and informs
	// the dir we return.

	// PHASE ONE: handle nil keys.
	// They are used to ask for the first (last in LTE)
	// leaf in the tree.
	if len(key) == 0 {
		value, found = n.recursiveFirst()
		return
	}

	// During GTE, we could have a mismatch in key
	// at a place in the path higher up than depth,
	// because we are searching for the place that
	// a key which is not (necessarily) in the tree
	// would go. So compare paths using keyCmpPath first.

	switch keyCmpPath {
	case 1:
		dir = needNextLeaf
		value, _ = n.recursiveLast()
		return
	case -1:
		dir = needPrevLeaf
		value, _ = n.recursiveFirst()
		return
	}
	// end of keyCmpPath path comparison.

	// inlined below
	//_, fullmatch, gt := n.checkCompressed(key, depth)

	// Let's inline checkCompressed, as it profiles hot.
	var gt bool
	fullmatch := true
	maxCmp := len(n.compressed)
	kd := len(key) - depth
	if kd < maxCmp {
		maxCmp = kd
	}
	for idx := 0; idx < maxCmp; idx++ {
		ci := n.compressed[idx]
		kdi := key[depth+idx]
		if ci != kdi {
			if kdi > ci {
				gt = true
			}
			fullmatch = false
			break
		}
	}

	// PHASE TWO: handle incomplete match with the compressed prefix.

	if !fullmatch {

		if gt {
			dir = needNextLeaf
			value, _ = n.recursiveLast()
			return
		} else {
			dir = needPrevLeaf
			value, _ = n.recursiveFirst()
			return
		}
	} // end if !fullmatch

	// We have a full match of compressed prefix.

	// The adjacency condition to terminate the
	// GTE search: we can stop searching when
	// we have found a smaller (key) node that
	// says go in a positive direction, and
	// a larger (key) node that says go
	// in a negative direction. This still holds
	// if we substitute subtree for node.
	// The leftmost-leaf of the greater subtree
	// gives the GTE search result leaf.

	// PHASE THREE: we have a full compressed prefix match,
	// so our subtree is relavant and must be searched.
	// Do a bisecting GTE search.

	// This is imporant, because only we (a this
	// stage in the tree descent) can make the
	// determination that two adjacent leaves meet
	// the GTE termination condition. Otherwise the
	// desired leaf could well be outside our purview;
	// either greater than our largest leaf, or less
	// than our smallest leaf. We want to efficiently
	// discover this, without having to go deeper
	// than necessary.

	nextDepth := depth + len(n.compressed)

	var querykey byte
	if nextDepth < len(key) {
		querykey = key[nextDepth]
	}

	// the GTE bisecting call:
	nextKeyb, next := n.Node.gte(&querykey)

	// nil means querykey > all keys in n.Node
	if next == nil {
		// dir = 2 will tell our caller to
		// look for the very next
		// biggest key: the minimal key in the next
		// bigger subtree (or their next child).
		dir = 2 // essential! +1 will not suffice.
		// never going to be used, so don't bother:
		//_, value = n.Node.last()
		return value, false, dir, 0
	}

	// It could still be that key > all of our
	// sub-tree keys, even thought next != nil,
	// because we've only looked at one additional byte,
	// the querykey. We must recurse with next.getGTE()
	// check the next bytes.

	// PHASE FOUR: recursion. A play in three acts.
	//
	// There are three recursive getGTE() calls below,
	// but only at most two of them will happen.
	//
	// We will always do the next.getGTE().
	//
	// Afterwards, depending on the results,
	// may also do a nextnext.getGTE() or
	// a prev.getGTE() call.

	// This is the first recursive getGTE call.
	value, found, dir, id = next.getGTE(
		key,
		nextDepth+1,
		smod,
		next,
		tree,
		calldepth+1,
		false, // false for smallestWillDo
		byteCmp(querykey, nextKeyb, keyCmpPath),
	)

	// about to use pren, so make sure its not stale.
	if !n.prenOK {
		selfb.subTreeRedoPren()
	}

	id += next.pren
	if found {
		// exact GTE match
		switch smod {
		case GTE:
			return value, true, 0, id
		case GT:
			cmp := bytes.Compare(value.leaf.Key, key)
			if cmp > 0 {
				// strictly greater, done!
				return value, true, 0, id
			}
			// check do we have other sibs before returning
			_, nextLocal := n.Node.next(&nextKeyb)
			if nextLocal == nil {
				// gotta pop up and then back down.
				dir = 2 // first greater encounted, take it!
				found = false
				value = nil
				return
			}
			// the local next is available, use it.
			value, _ = nextLocal.recursiveFirst()
			found = true
			dir = 0
			id = nextLocal.pren
			return
			// end GT
		}
	}
	// INVAR: found is false.

	// a) We know that next has a keybyte >= key[some depth],
	// so it could qualify as a GTE subtree.
	//
	// b) We know next.getGTE returned without finding
	// an exact answer.
	//
	// Could this mean that key was > all leaves in next?
	// only if dir was positive.

	if dir > 0 {
		// key is > all leaves in next.
		// The key must fall between
		// next and next.next. Since it
		// was not found in next, it must
		// be in next.next, or in the next adjacent
		// subtree if next.next is nil.

		nextnextKeyb, nextnext := n.Node.next(&querykey)
		if nextnext == nil {
			_, value = n.Node.last()
			return value, false, needNextLeaf, 0
		}
		if dir > 1 {
			smallestWillDo = true
		}

		// the second recursive getGTE() call.
		value2, found2, dir2, id2 := nextnext.getGTE(
			key,
			nextDepth,
			smod,
			nextnext,
			tree,
			calldepth+1,
			smallestWillDo,
			byteCmp(querykey, nextnextKeyb, keyCmpPath),
		)

		id2 += nextnext.pren
		if found2 {
			return value2, true, 0, id2
		}

		// dir > 0 here.
		if dir2 < 0 {
			// The adjacency condition holds,
			// we have found our gte value.
			found = true
			value = value2
			dir = 0
			id = id2
			return
		}

		dir = needNextLeaf
		if smallestWillDo {
			dir = 2
		}
		return value2, false, dir, id2

	} // end if dir > 0

	// still handling the results of the
	// 	value, found, dir = next.getGTE(key, nextDepth+1, smod, next)
	// call.
	//
	// We know: !found
	// We know: dir < 0
	// We know 'next' was the smallest such that: nextKeyb >= key.
	//
	// Do we have an adjacency conclusion here? Maybe!

	prevKeyb, prev := n.Node.prev(&querykey)
	if prev == nil {
		// goal could be in previous subtree.
		//_, value = n.Node.first()
		value, _ = n.recursiveFirst()

		// Setting found like this lets us
		// correctly answer GT/GTE queries for
		// keys before the smallest key in the tree.
		found = (calldepth == 0)

		return value, found, needPrevLeaf, 0
	}

	if dir > 1 {
		smallestWillDo = true
	}

	// the third recursive getGTE() call.
	value2, found2, dir2, id2 := prev.getGTE(
		key,
		nextDepth+1,
		smod,
		prev,
		tree,
		calldepth+1,
		smallestWillDo,
		byteCmp(querykey, prevKeyb, keyCmpPath),
	)

	id2 += prev.pren
	if found2 {
		return value2, true, 0, id2
	}

	if dir2 > 0 {
		// adjacency conclusion holds: the
		// next.recursiveFirst() is our goal node.

		value, _ := next.recursiveFirst()
		return value, true, 0, next.pren
	}
	if dir2 > 0 && smallestWillDo {
		dir2 = 2
	}
	return value2, false, dir2, id2
}

// byteCmp helps us track how the key
// diverges from the inner node paths during
// the tree descent. Once we have gone
// in a direction, persist that direction:
// if keyCmpPath != 0 we just return it.
// Otherwise a > b => return 1; a < b => return -1.
func byteCmp(a, b byte, keyCmpPath int) int {
	// persist the very first difference.
	if keyCmpPath != 0 {
		return keyCmpPath
	}
	if a > b {
		return 1
	}
	if a < b {
		return -1
	}
	return 0
}
