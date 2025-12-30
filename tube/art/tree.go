package art

import (
	"fmt"
	"sync"
)

//go:generate greenpack

// Tree is a trie that implements
// the Adaptive Radix Tree (ART) algorithm
// to provide a sorted, key-value, in-memory
// dictionary[1]. The ART tree provides both path compression
// (vertical compression) and variable
// sized inner nodes (horizontal compression)
// for space-efficient fanout.
//
// Path compression is particularly attractive in
// situations where many keys have redudant
// prefixes. This is the common case for
// many ordered-key-value-map use cases, such
// as database indexes and file-system hierarchies.
// The Google File System paper, for example,
// mentions the efficiencies obtained
// by exploiting prefix compression in their
// distributed file system[2]. FoundationDB's
// new Redwood backend provides it as a feature[3],
// and users wish the API could be improved by
// offering it[4] in query result APIs.
//
// As an alternative to red-black trees,
// AVL trees, and other kinds of balanced binary trees,
// ART is particularly attractive. Like
// those trees, ART offers an ordered index
// of sorted keys allowing efficient O(log N) access
// for each unique key.
//
// Efficient key-range lookup and iteration, as well as the
// ability to treat the tree as array using
// integer indexes (based on the counted B-tree
// idea[5]), make this ART tree implementation
// particularly easy to use in practice.
//
// ART supports just a single value for each
// key -- it is not a "multi-map" in the C++ sense.
//
// Concurrency: this ART implementation is
// goroutine safe, as it uses a the Tree.RWmut
// sync.RWMutex for synchronization. Thus it
// allows only a single writer at a time, and any number
// of readers. Readers will block until
// the writer is done, and thus they see
// a fully consistent view of the tree.
// The RWMutex approach was the fastest
// and easiest to reason about in our
// applications without overly complicating
// the code base. The SkipLocking flag can
// be set to omit all locking if goroutine
// coordination is provided by other means,
// or unneeded (in the case of single goroutine
// only access).
//
// [1] "The Adaptive Radix Tree: ARTful
// Indexing for Main-Memory Databases"
// by Viktor Leis, Alfons Kemper, Thomas Neumann.
//
// [2] "The Google File System"
// SOSP’03, October 19–22, 2003, Bolton Landing, New York, USA.
// by Sanjay Ghemawat, Howard Gobioff, and Shun-Tak Leung.
// https://pdos.csail.mit.edu/6.824/papers/gfs.pdf
//
// [3] "How does FoundationDB store keys with duplicate prefixes?"
// https://forums.foundationdb.org/t/how-does-foundationdb-store-keys-with-duplicate-prefixes/1234
//
// [4] "Issue #2189: Prefix compress read range results"
// https://github.com/apple/foundationdb/issues/2189
//
// [5] "Counted B-Trees"
// https://www.chiark.greenend.org.uk/~sgtatham/algorithms/cbtree.html
type Tree struct {
	RWmut sync.RWMutex `msg:"-"`

	root *bnode
	size int64

	// At() calls are much slower than
	// iteration by default, because they
	// start at the root and go down the tree
	// each time. If the user is making successive
	// At() calls in order (for convenience) we will
	// try to make these sequential At() calls
	// faster by storing where in the tree
	// the last call left off. That is, we cache
	// where the last At() left off, and pick up
	// from there if the next call is for i+1 and
	// there have been no tree modifications.
	// Use Atfar() instead of At() to skip caching.
	atCache *iterator

	// The treeVersion Update protocol:
	// Writers increment this treeVersion number
	// to allow iterators to continue
	// efficiently past tree modifications
	// (deletions and/or insertions) that happen
	// behind them. If the iterator sees a
	// different treeVersion, it will use a
	// slightly more expensive way of getting
	// the next leaf, one that is resilient in
	// the face of insertions and deletions.
	// If no changes are detected, the
	// fast path is used.
	treeVersion int64

	// SkipLocking means do no internal
	// synchronization, because a higher
	// component is doing so.
	//
	// Warning: when using SkipLocking
	// the user's code _must_ synchronize (prevent
	// overlap) of readers and writers from
	// different goroutines who access the Tree
	// simultaneously. Under this setting,
	// the Tree will not do locking itself
	// (it does by default, with SkipLocking false).
	// Without synchronization, multiple goroutines
	// will create data races, lost data, and
	// segfaults from torn reads.
	//
	// The easiest way to do this is with a sync.RWMutex.
	// One such, the RWmut on this Tree, will be
	// employed if SkipLocking is allowed to
	// default to false.
	SkipLocking bool `msg:"-"`

	Leafz []*Leaf `zid:"0"`
}

// NewArtTree creates and returns a new ART Tree,
// ready for use.
func NewArtTree() *Tree {
	return &Tree{}
}

// Size returns the number of keys
// (leaf nodes) stored in the tree.
func (t *Tree) Size() (sz int) {
	if t.SkipLocking {
		return int(t.size)
	}
	t.RWmut.RLock()
	sz = int(t.size)
	t.RWmut.RUnlock()
	return
}

// String does no locking. It returns
// a string representation of the tree.
// This is mostly for debugging, and
// can be quite slow for large trees.
func (t *Tree) String() string {
	sz := t.Size()
	if t.root == nil {
		return "empty uart.Tree"
	}
	return fmt.Sprintf("tree of size %v: ", sz) +
		t.root.FlatString(0, -1, t.root)
}

// recurse -1 for full tree; otherwise only
// that many levels.
func (t *Tree) stringNoKeys(recurse int) string {
	sz := t.Size()
	if t.root == nil {
		return "empty uart.Tree"
	}
	return fmt.Sprintf("tree of size %v: ", sz) +
		t.root.stringNoKeys(0, recurse, t.root)
}

// Insert makes a copy of key to avoid sharing bugs.
// The value is only stored and not copied.
// The return value updated is true if the
// size of the tree did not change because
// an existing key was given the new value.
func (t *Tree) Insert(key Key, value []byte, vtype string) (updated bool) {

	// make a copy of key that we own, so
	// caller can alter/reuse without messing us up.
	// This was a frequent source of bugs, so
	// it is important. The benchmarks will crash
	// without it, for instance, since they
	// re-use key []byte memory alot.
	key2 := Key(append([]byte{}, key...))
	lf := NewLeaf(key2, value, vtype)

	return t.InsertLeaf(lf)
}

// InsertLeaf: the *Leaf lf *must* own the lf.Key it holds.
// It cannot be shared. Callers must guarantee this,
// copying the slice if necessary before
// submitting the Leaf.
func (t *Tree) InsertLeaf(lf *Leaf) (updated bool) {

	if !t.SkipLocking {
		t.RWmut.Lock()
		defer t.RWmut.Unlock()
	}

	var replacement *bnode

	if t.root == nil {
		// first leaf in the tree
		t.size++
		t.root = bnodeLeaf(lf)
		t.treeVersion++
		return false
	}

	//vv("t.size = %v", t.size)
	replacement, updated = t.root.insert(lf, 0, t.root, t, nil)
	if replacement != nil {
		t.root = replacement
	}
	if !updated {
		t.size++
	}
	t.treeVersion++
	return
}

// FindGT returns the first element whose key
// is greater than the supplied key.
func (t *Tree) FindGT(key Key) (val []byte, idx int, found bool, lf *Leaf) {
	lf, idx, found = t.Find(GT, key)
	//vv("FindGT got back lf='%v'; idx=%v; found = %v", lf, idx, found)
	if found && lf != nil {
		val = lf.Value
	}
	return
}

// FindGTE returns the first element whose key
// is greater than, or equal to, the supplied key.
func (t *Tree) FindGTE(key Key) (val []byte, idx int, found bool, lf *Leaf) {
	lf, idx, found = t.Find(GTE, key)
	if found && lf != nil {
		val = lf.Value
	}
	return
}

// FindGT returns the first element whose key
// is less than the supplied key.
func (t *Tree) FindLT(key Key) (val []byte, idx int, found bool, lf *Leaf) {
	lf, idx, found = t.Find(LT, key)
	if found && lf != nil {
		val = lf.Value
	}
	return
}

// FindLTE returns the first element whose key
// is less-than-or-equal to the supplied key.
func (t *Tree) FindLTE(key Key) (val []byte, idx int, found bool, lf *Leaf) {
	lf, idx, found = t.Find(LTE, key)
	if found && lf != nil {
		val = lf.Value
	}
	return
}

// FindExact returns the element whose key
// matches the supplied key.
func (t *Tree) FindExact(key Key) (val []byte, idx int, found bool, vtype string) {
	var lf *Leaf
	lf, idx, found = t.Find(Exact, key)
	if found && lf != nil {
		val = lf.Value
		vtype = lf.Vtype
	}
	return
}

// FirstLeaf returns the first leaf in the Tree.
func (t *Tree) FirstLeaf() (lf *Leaf, idx int, found bool) {
	return t.Find(GTE, nil)
}

// FirstLeaf returns the last leaf in the Tree.
func (t *Tree) LastLeaf() (lf *Leaf, idx int, found bool) {
	return t.Find(LTE, nil)
}

// Find allows GTE, GT, LTE, LT, and Exact searches.
//
// GTE: find a leaf greater-than-or-equal to key;
// the smallest such key.
//
// GT: find a leaf strictly greater-than key;
// the smallest such key.
//
// LTE: find a leaf less-than-or-equal to key;
// the largest such key.
//
// LT: find a leaf less-than key; the
// largest such key.
//
// Exact: find leaf whose key matches the supplied
// key exactly. This is the default. It acts
// like a hash table. A key can only be stored
// once in the tree. (It is not a multi-map
// in the C++ STL sense).
//
// If key is nil, then GTE and GT return
// the first leaf in the tree, while LTE
// and LT return the last leaf in the tree.
//
// Clients must take care not to modify the
// returned Leaf.Key, as it is not copied to
// keep memory use low. Doing so will result
// in undefined behavior.
//
// The FindGTE, FindGT, FindLTE, and FindLT
// methods provide a more convenient interface
// to obtain the stored Leaf.Value if the
// full generality of Leaf access is not required.
//
// By default, Find obtains a read-lock on the
// Tree.RWmut. This can be omitted by setting the
// Tree.SkipLocking option to true.
func (t *Tree) Find(smod SearchModifier, key Key) (lf *Leaf, idx int, found bool) {
	if !t.SkipLocking {
		t.RWmut.RLock()
		defer t.RWmut.RUnlock()
	}
	return t.find_unlocked(smod, key)
}

func (t *Tree) find_unlocked(smod SearchModifier, key Key) (lf *Leaf, idx int, found bool) {

	//vv("Find, smod='%v; key='%v'; t.size='%v'", smod, string(key), t.size)
	if t.root == nil {
		return
	}
	if len(key) == 0 && t.size == 1 {
		// nil query asks for first leaf, or last, depending.
		// here it is the same.
		return t.root.leaf, 0, true
	}
	var b *bnode
	var dir direc
	switch smod {
	case GTE, GT:
		b, found, dir, idx = t.root.getGTE(key, 0, smod, t.root, t, 0, false, 0)
	case LTE, LT:
		b, found, dir, idx = t.root.getLTE(key, 0, smod, t.root, t, 0, false, 0)
	default:
		b, found, dir, idx = t.root.get(key, 0, t.root, 0, t)
	}
	if t.size == 1 {
		// Test 505 in tree_test.go needs this.
		//
		// We special case is a leaf at the root, as
		// we don't want to slow down the hot path
		// leaf.go code with this uncommon situation.
		//vv("smod = %v; dir=%v; found=%v; b=%v", smod, dir, found, b)
		switch smod {
		// note the dir is opposite of might be expected. correctly.
		case GTE:
			if dir <= 0 && b != nil {
				found = true
			}
		case GT:
			if dir < 0 && b != nil {
				found = true
			}
		case LTE:
			if dir >= 0 && b != nil {
				found = true
			}
		case LT:
			if dir > 0 && b != nil {
				found = true
			}
		default:
			// Exact match on leaf
			if !found {
				b = nil
			}
		}
	}
	if b != nil {
		lf = b.leaf
	}
	return
}

type SearchModifier int

const (
	// Exact is the default.
	Exact SearchModifier = 0 // exact matches only; like a hash table
	GTE   SearchModifier = 1 // greater than or equal to this key.
	LTE   SearchModifier = 2 // less than or equal to this key.
	GT    SearchModifier = 3 // strictly greater than this key.
	LT    SearchModifier = 4 // strictly less than this key.
)

func (smod SearchModifier) String() string {
	switch smod {
	case Exact:
		return "Exact"
	case GTE:
		return "GTE"
	case LTE:
		return "LTE"
	case GT:
		return "GT"
	case LT:
		return "LT"
	}
	return fmt.Sprintf("unknown smod '%v'", int(smod))
}

// Remove deletes the key from the Tree.
// If the key is not present deleted will return false.
// If the key was present, deletedLeaf will supply
// its associated Leaf from which value, in
// the deletedLeaf.Value field, can be obtained.
func (t *Tree) Remove(key Key) (deleted bool, deletedLeaf *Leaf) {

	if !t.SkipLocking {
		t.RWmut.Lock()
		defer t.RWmut.Unlock()
	}

	var deletedNode *bnode
	if t.root == nil {
		return
	}

	deleted, deletedNode = t.root.del(key, 0, t.root, func(rn *bnode) {
		t.root = rn
	})
	if deleted {
		deletedLeaf = deletedNode.leaf
		t.size--
		t.treeVersion++
	}
	return
}

// IsEmpty returns true iff the Tree is empty.
func (t *Tree) IsEmpty() (empty bool) {
	if t.SkipLocking {
		return t.root == nil
	}
	t.RWmut.RLock()
	empty = t.root == nil
	t.RWmut.RUnlock()
	return
}

// At(i) lets us think of the tree as a
// array, returning the i-th leaf
// from the sorted leaf nodes, using
// an efficient O(log N) time algorithm.
// Here N is the size or count of elements
// stored in the tree.
//
// At() uses the counted B-tree approach
// described by Simon Tatham[1].
// This is also known as an Order-Statistic tree
// in the literature[2].
//
// Optimization: for the common case of sequential calls
// going forward, At(i) will transparently
// use an iterator to cache the tree
// traversal point so that the next At(i+1) call can
// pick up where the previous tree search left
// off. Since we don't have to repeat the
// mostly-the-same traversal down the tree again, the
// speed-up in benchmark (see Test620) for this
// common case is a dramatic 6x, from 240 nsec to 40 nsec
// per call. The trade-off is that we must
// do a small amount of allocation
// to maintain a stack during the calls.
// We will try to add a small pool of iterator
// checkpoint frames in the future to minimize it,
// but complete zero-alloc is almost impossible
// if we want this optimization. My call is
// that this a trade-off well worth making.
//
// If you will be doing alot of random (un-sequential/non-linear)
// access to the tree, use Atfar() instead of At().
// They are the same, except that Atfar will not
// spend any time trying to cache the tree traversal
// paths to your random access points.
//
// [1] https://www.chiark.greenend.org.uk/~sgtatham/algorithms/cbtree.html
//
// [2] https://en.wikipedia.org/wiki/Order_statistic_tree
func (t *Tree) At(i int) (lf *Leaf, ok bool) {
	if t.SkipLocking {
		return t.at_unlocked(i)
	}
	t.RWmut.RLock()
	lf, ok = t.at_unlocked(i)
	t.RWmut.RUnlock()
	return
}

// Atfar is more suitable that At for random
// (non-linear sequential) access to the tree.
//
// Atfar() is the same as At() except that
// it does not attempt to do any caching of
// the tree traversal point to speed up sequential
// calls such as At(i), At(i+1), At(i+2), ... like At() does.
//
// Hence Atfar() saves the (relatively small) time
// that At() spends filling the iterator checkpoint cache.
//
// If you will be doing alot of random (un-sequential)
// access to the tree, use Atfar() instead of At().
// The name tries to suggest that this access is "far" away
// from any others.
func (t *Tree) Atfar(i int) (lf *Leaf, ok bool) {
	if t.SkipLocking {
		if t == nil || t.root == nil {
			return
		}
		return t.root.at(i)
	}
	t.RWmut.RLock()
	if t == nil || t.root == nil {
		return
	}
	lf, ok = t.root.at(i)
	t.RWmut.RUnlock()
	return
}

func (t *Tree) at_unlocked(i int) (lf *Leaf, ok bool) {
	if t == nil || t.root == nil {
		return
	}
	if t.atCache != nil {
		if t.atCache.treeVersion == t.treeVersion {
			if i == t.atCache.curIdx+1 {
				ok = t.atCache.Next()
				if ok {
					lf = t.atCache.leaf
					return
				}
			}
		}
		t.atCache = nil
	}
	// INVAR: t.atCache == nil

	lf, ok = t.root.at(i)

	// try to cache At() iteration. Gives 6x speedup
	// for common case of going forward.
	if ok && t.size > 1 {
		t.atCache = t.Iter(lf.Key, nil)
		t.atCache.Next()
	}

	return
}

// Atv(i) is like At(i) but returns the value
// from the Leaf instead of the actual *Leaf itself,
// simply for convenience.
func (t *Tree) Atv(i int) (val []byte, ok bool) {
	var lf *Leaf
	if t.SkipLocking {
		lf, ok = t.at_unlocked(i)
		if ok {
			val = lf.Value
		}
		return
	}
	t.RWmut.RLock()
	lf, ok = t.at_unlocked(i)
	if ok {
		val = lf.Value
	}
	t.RWmut.RUnlock()
	return
}

// LeafIndex returns the integer index
// of the leaf in the tree using exact
// key matching. The index represents
// the position in the lexicographic (shortlex)
// sorted order of keys, and so can be
// used to compute quantile and other statistics
// efficiently. The time complexity
// is O(log N).
func (t *Tree) LeafIndex(leaf *Leaf) (idx int, ok bool) {
	t.RWmut.RLock()
	_, idx, ok = t.find_unlocked(Exact, leaf.Key)
	t.RWmut.RUnlock()
	return
}

// CompressedStats returns the count
// of inner nodes with a given compressed
// prefix length.
func (t *Tree) CompressedStats() (cs map[int]int, bytesSaved int) {
	cs = make(map[int]int)
	if t == nil || t.root == nil {
		return
	}
	for b := range dfs(t.root) {
		if !b.isLeaf {
			n := len(b.inner.compressed)
			cs[n]++
			bytesSaved += n * b.inner.SubN
		}
	}
	return
}
