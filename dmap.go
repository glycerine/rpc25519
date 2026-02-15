package rpc25519

import (
	"fmt"
	"iter"
	"sync/atomic"

	rb "github.com/glycerine/rbtree"
)

// The ided interface allows any object
// to be a key in a dmap, simply by providing an id()
// method whose returned string we can sort on.
type ided interface {
	id() string
}

// dmap is a deterministic map.
//
// Unlike Go's builtin map, a dmap can be
// range iterated in a repeatable order,
// This is critical for simulation testing
// to give reproducible test runs.
//
// However, like the built-in map, dmap does no
// internal locking, and is not goroutine safe.
// The user must provide external sync.Mutex or otherwise
// coordinate access if a dmap is shared
// across goroutines. This allows dmap to
// also provide for deletion or value update (not
// key modification, of course) during a
// for-range dmap.all() iteration.
//
// In what order does a dmap return keys?
// How fast is it?
//
// The key's ided interface supplies a
// sortable id() string which determines
// the range all() order, and gives O(log n)
// set (upsert) time. The get and del
// methods are O(1) time, as is deleteAll.
//
// For repeated full range all
// scans, we cache the ikv pointers in contiguous
// memory to maximize L1 cache hits and
// minimize pointer chasing in the underlying
// red-black tree.
//
// Thus dmap aims to be almost as fast, or
// faster, than the built in Go map, for common
// use patterns, while providing deterministic,
// repeatable iteration order. The quick benchmarks
// in dmap_test show that repeated full-range
// scans are between 2x and 14x faster than
// the built-in Go map. The 7x difference in
// dmap vs dmap is due to the iter/coroutine
// iterator overhead. Use the cached() method
// instead of all() to range over a slice and
// maximize your L1 cache performance.
//
// To provide the reproducible sorted range
// order and efficient get/set/delete/deleteAll
// operations, dmap uses approximately 3x
// the memory of the builtin map. The
// memory goes towards:
//
// (1) a built-in map for fast O(1) get and delkey operations;
//
// (2) full range scans are cached in contiguous
// slice of memory, so that the common case of repeated
// full range scans should be even faster than a
// builtin map by allowing better L1 cache performance; and
//
// (3) a red-black tree is maintained to
// allow efficient insertion into/update of the
// ordered key-value dictionary in O(log n) time.
type dmap[K ided, V any] struct {
	version int64

	tree *rb.Tree
	idx  map[string]rb.Iterator

	// cache the first range all, and use
	// ordercache if we range all again without
	// intervening upsert or deletes.
	ordercache   []*ikv[K, V]
	cacheversion int64
}

// cached returns the raw internal ikv slice
// for very fast iteration in a for-range loop.
func (s *dmap[K, V]) cached() []*ikv[K, V] {
	n := s.tree.Len()
	nc := len(s.ordercache)
	vers := atomic.LoadInt64(&s.version)
	if nc == n && s.cacheversion == vers {
		return s.ordercache
	}
	// refill ordercache
	s.ordercache = nil
	s.cacheversion = vers
	for it := s.tree.Min(); !it.Limit(); it = it.Next() {
		kv := it.Item().(*ikv[K, V])
		s.ordercache = append(s.ordercache, kv)
	}
	return s.ordercache
}

// newDmap makes a new dmap.
func newDmap[K ided, V any]() *dmap[K, V] {
	return &dmap[K, V]{
		idx: make(map[string]rb.Iterator),
		tree: rb.NewTree(func(a, b rb.Item) int {
			ak := a.(*ikv[K, V]).id
			bk := b.(*ikv[K, V]).id
			if ak < bk {
				return -1
			}
			if ak > bk {
				return 1
			}
			return 0
		}),
	}
}

type ikv[K ided, V any] struct {
	id  string // sorted order
	key K
	val V
	it  rb.Iterator
}

// Len returns the number of keys stored in the dmap.
func (s *dmap[K, V]) Len() int {
	return len(s.idx)
}

func (s *dmap[K, V]) String() (r string) {
	vers := atomic.LoadInt64(&s.version)
	r = fmt.Sprintf("dmap{ version:%v id:{", vers)
	it := s.tree.Min()
	i := 0
	extra := ""
	for !it.Limit() {
		kv := it.Item().(*ikv[K, V])
		if i == 1 {
			extra = ", "
		}
		r += fmt.Sprintf("%v%v", extra, kv.id)
		it = it.Next()
		i++
	}
	r += "}}"
	return
}

// delkey deletes a key from the dmap, if present.
// This is a constant O(1) time operation.
//
// If found returns true, next has the
// iterator following the deleted key.
//
// If found returns false, next is s.tree.Limit(),
// which can be used to terminate an iteration.
//
// Using next provides "advance and delete behind"
// semantics.
func (s *dmap[K, V]) delkey(key K) (found bool, next rb.Iterator) {
	if isNil(key) {
		next = s.tree.Limit()
		return
	}

	id := key.id()
	//vv("delkey id = '%v'", id)
	var it rb.Iterator
	var ok bool
	if s.idx == nil {
		// not present
		next = s.tree.Limit()
		return
	} else {
		it, ok = s.idx[id]
		if !ok {
			// not present
			next = s.tree.Limit()
			return
		}
	}
	found = true
	//vv("deleting id='%v' -> it.Item() = '%v'", id, it.Item())
	atomic.AddInt64(&s.version, 1)
	s.ordercache = nil
	s.cacheversion = 0
	next = it.Next()
	s.tree.DeleteWithIterator(it)
	delete(s.idx, id)
	return
}

func (s *dmap[K, V]) deleteWithIter(it rb.Iterator) (found bool, next rb.Iterator) {
	if it.Limit() {
		// return Limit, this one is
		// at hand, and any will do.
		next = it
		return
	}

	kv, ok := it.Item().(*ikv[K, V])
	if !ok {
		// bad it
		next = s.tree.Limit()
		return
	}

	if s.idx == nil {
		// nothing present
		next = s.tree.Limit()
		return
	}
	_, ok = s.idx[kv.id]
	if !ok {
		// not present
		next = s.tree.Limit()
		return
	}

	//vv("deleteWithIter before changes: '%v'", s)
	found = true
	atomic.AddInt64(&s.version, 1)
	s.ordercache = nil
	s.cacheversion = 0
	next = it.Next()
	s.tree.DeleteWithIterator(it)
	delete(s.idx, kv.id)
	//vv("deleteWithIter after changes: '%v'", s)

	return
}

// deleteAll clears the tree in O(1) time.
func (s *dmap[K, V]) deleteAll() {
	atomic.AddInt64(&s.version, 1)
	s.ordercache = nil
	s.cacheversion = 0
	s.tree.DeleteAll()
	s.idx = nil
}

// set is an upsert. It does an insert if the key is
// not already present returning newlyAdded true;
// otherwise it updates the current key's value in place.
func (s *dmap[K, V]) set(key K, val V) (newlyAdded bool) {
	if isNil(key) {
		return
	}
	atomic.AddInt64(&s.version, 1)
	s.ordercache = nil
	s.cacheversion = 0

	id := key.id()
	//vv("set id = '%v'", id)
	var it rb.Iterator
	var ok bool
	if s.idx == nil {
		s.idx = make(map[string]rb.Iterator)
	} else {
		it, ok = s.idx[id]
	}

	if !ok {
		//vv("not yet in idx/tree, so add it. id='%v'", id)
		newlyAdded = true
		item := &ikv[K, V]{id: id, key: key, val: val}
		added, it2 := s.tree.InsertGetIt(item)
		item.it = it2
		if !added {
			panic("should have hit in s.idx, out of sync with tree")
		}
		s.idx[id] = it2
		return
	}
	prev := it.Item().(*ikv[K, V])
	//vv("id already in tree, just update in place: id='%v'; prev='%#v'", id, prev)
	prev.val = val
	return
}

// all starts an iteration over all elements in
// the dmap. To allow the user to delete in
// the middle of iteration, there is no locking
// internally.
func (s *dmap[K, V]) all() iter.Seq2[K, V] {

	seq2 := func(yield func(K, V) bool) {

		if s == nil || s.tree == nil {
			return
		}

		//vv("start of all iteration.")
		n := s.tree.Len()
		nc := len(s.ordercache)

		// detect deletes in the middle of using s.ordercache.
		vers := atomic.LoadInt64(&s.version)

		if nc == n && s.cacheversion == vers {
			// s.ordercache is usable.
			for i, kv := range s.ordercache {
				nextit := kv.it.Next() // in case of slow path below
				if !yield(kv.key, kv.val) {
					return
				}
				vers2 := atomic.LoadInt64(&s.version)
				if vers2 == vers {
					continue
				} else {
					// delete in middle of iteration.
					// abandon oc, down shift to
					// slow/safe path using nextit.
					n2 := s.tree.Len()
					if i >= n2-1 {
						// we were on the last anyway. done.
						return
					}
					// still have some left
					it := nextit
					var kv *ikv[K, V]
					for !nextit.Limit() {
						it = nextit
						kv = it.Item().(*ikv[K, V])
						// pre-advance, allows deletion of it.
						nextit = nextit.Next()
						if !yield(kv.key, kv.val) {
							return
						}
						//vv("back from yield 2nd")
					}
					return // essential, cannot resume 1st loop.
				} // end if else vers2 != vers
			} // end for i over s.ordercache
			return
		} // end if ordercache hit

		// cache miss. cannot read from
		// s.ordercache, but we will try to fill
		// it on this pass. only do full fills
		// for simplicity.
		s.ordercache = nil
		s.cacheversion = vers
		cachegood := true // invalidate if delete in middle of all.
		it := s.tree.Min()
		for !it.Limit() {

			kv := it.Item().(*ikv[K, V])
			// advance before yeilding so user
			// can delete at it if desired, and
			// we will keep on going
			it = it.Next()

			if cachegood {
				s.ordercache = append(s.ordercache, kv)
			}
			if !yield(kv.key, kv.val) {
				return
			}
			// check for delete/change in middle.
			vers2 := atomic.LoadInt64(&s.version)
			if vers2 != vers {
				cachegood = false
				s.ordercache = nil
				s.cacheversion = 0
			}

		} // end for it != lim
	} // end seq2 definition
	return seq2
}

// allikv returns the ikv(s) not the val. This
// allows highly efficient val updates in place, but
// is mildly vulnerable to mis-use: the user must not
// change the other ikv.id field. Otherwise the
// red-black tree will be borked.
//
// Hence this function is for performance oriented users who
// can guarantee their code will leave ikv.id (and ikv.it,
// and most probably ikv.key too) alone. You can
// read these, but don't write. If you
// need to change the ikv.id/key, you must delkey or
// deleteWithIter to remove the old key from the tree first;
// then add in the new key. This allows the tree
// to properly rebalance itself.
//
// The tree does not care about ikv.val, so the user
// can update that at will. The ikv.it, like the ikv.id/key
// should be considered const/not be altered by user code.
// It is the iterator that points into the red-back
// tree, and so allows efficient start of iteration in the
// middle and/or delete in O(1) rather than O(log n) from
// the middle of the tree.
func (s *dmap[K, V]) allikv() iter.Seq2[K, *ikv[K, V]] {

	seq2 := func(yield func(K, *ikv[K, V]) bool) {

		//vv("start of all iteration.")
		n := s.tree.Len()
		nc := len(s.ordercache)

		// detect deletes in the middle of using s.ordercache.
		vers := atomic.LoadInt64(&s.version)

		if nc == n && s.cacheversion == vers {
			// s.ordercache is usable.
			for i, kv := range s.ordercache {
				nextit := kv.it.Next() // in case of slow path below
				if !yield(kv.key, kv) {
					return
				}
				vers2 := atomic.LoadInt64(&s.version)
				if vers2 == vers {
					continue
				} else {
					// delete in middle of iteration.
					// abandon oc, down shift to
					// slow/safe path using nextit.
					n2 := s.tree.Len()
					if i >= n2-1 {
						// we were on the last anyway. done.
						return
					}
					// still have some left
					it := nextit
					var kv *ikv[K, V]
					for !nextit.Limit() {
						it = nextit
						kv = it.Item().(*ikv[K, V])
						// pre-advance, allows deletion of it.
						nextit = nextit.Next()
						if !yield(kv.key, kv) {
							return
						}
						//vv("back from yield 2nd")
					}
					return // essential, cannot resume 1st loop.
				} // end if else vers2 != vers
			} // end for i over s.ordercache
			return
		} // end if ordercache hit

		// cache miss. cannot read from
		// s.ordercache, but we will try to fill
		// it on this pass. only do full fills
		// for simplicity.
		s.ordercache = nil
		s.cacheversion = vers
		cachegood := true // invalidate if delete in middle of all.
		it := s.tree.Min()
		for !it.Limit() {

			kv := it.Item().(*ikv[K, V])
			// advance before yeilding so user
			// can delete at it if desired, and
			// we will keep on going
			it = it.Next()

			if cachegood {
				s.ordercache = append(s.ordercache, kv)
			}
			if !yield(kv.key, kv) {
				return
			}
			// check for delete/change in middle.
			vers2 := atomic.LoadInt64(&s.version)
			if vers2 != vers {
				cachegood = false
				s.ordercache = nil
			}

		} // end for it != lim
	} // end seq2 definition
	return seq2
}

// get2 returns the val corresponding to key in
// O(1) constant time per query. found will be
// false iff the key was not present.
func (s *dmap[K, V]) get2(key K) (val V, found bool) {
	if s.idx == nil || isNil(key) {
		// not present, or nil key request.
		return
	}
	id := key.id()
	var it rb.Iterator
	it, found = s.idx[id]
	if !found {
		return
	}
	val = it.Item().(*ikv[K, V]).val
	return
}

// get does get2 but without the found flag.
func (s *dmap[K, V]) get(key K) (val V) {
	if s.idx == nil || isNil(key) {
		// not present, or nil key
		return
	}
	id := key.id()
	it, found := s.idx[id]
	if !found {
		return
	}
	return it.Item().(*ikv[K, V]).val
}

// getikv returns the ikv[K,V] struct corresponding to key in
// O(1) constant time per query. If the key is
// found, the kv.it will point to it in the dmap tree,
// which can be used to walk the
// tree in sorted order forwards or
// back from that point. The ikv is what the
// tree stores, so this provides for
// for fast updates of ikv.val if required. Note
// the ikv.id and should not be changed, as that
// would invalidate the tree without notifying
// it of the need to rebalance.
func (s *dmap[K, V]) getikv(key K) (kv *ikv[K, V], found bool) {

	if s.idx == nil || isNil(key) {
		// not present
		return
	}
	var it rb.Iterator
	id := key.id()
	it, found = s.idx[id]
	if !found {
		return
	}
	kv = it.Item().(*ikv[K, V])
	return
}
