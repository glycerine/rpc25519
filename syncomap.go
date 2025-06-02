package rpc25519

import (
	"cmp"
	"fmt"
	"iter"
	"sync"
	"sync/atomic"

	rb "github.com/glycerine/rbtree"
)

// an omap with a mutex.
type syncomap[K cmp.Ordered, V any] struct {
	mu sync.Mutex

	version int64

	tree *rb.Tree

	// cache the first range all, and use
	// ordercache if we range all again without
	// intervening upsert or deletes.
	ordercache   []*okv[K, V]
	cacheversion int64
}

// cached returns the raw internal okv slice
// for very fast iteration in a for-range loop.
func (s *syncomap[K, V]) cached() []*okv[K, V] {
	s.mu.Lock()
	defer s.mu.Unlock()

	n := s.tree.Len()
	nc := len(s.ordercache)
	vers := atomic.LoadInt64(&s.version)
	if nc == n && s.cacheversion == vers {
		return append([]*okv[K, V]{}, s.ordercache...)
	}
	// refill ordercache
	s.ordercache = nil
	s.cacheversion = vers
	for it := s.tree.Min(); !it.Limit(); it = it.Next() {
		kv := it.Item().(*okv[K, V])
		s.ordercache = append(s.ordercache, kv)
	}
	return append([]*okv[K, V]{}, s.ordercache...)
}

// newSyncomap makes a new syncomap.
func newSyncomap[K cmp.Ordered, V any]() *syncomap[K, V] {
	return &syncomap[K, V]{
		tree: rb.NewTree(func(a, b rb.Item) int {
			ak := a.(*okv[K, V]).key
			bk := b.(*okv[K, V]).key
			return cmp.Compare(ak, bk)
		}),
	}
}

// Len returns the number of keys stored in the omap.
func (s *syncomap[K, V]) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.tree.Len()
}

func (s *syncomap[K, V]) String() (r string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	vers := atomic.LoadInt64(&s.version)
	r = fmt.Sprintf("omap{ version:%v {", vers)
	it := s.tree.Min()
	i := 0
	extra := ""
	for !it.Limit() {
		kv := it.Item().(*okv[K, V])
		if i == 1 {
			extra = ", "
		}
		r += fmt.Sprintf("%v%v:%v", extra, kv.key, kv.val)
		it = it.Next()
		i++
	}
	r += "}}"
	return
}

// delkey deletes a key from the omap, if present.
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
func (s *syncomap[K, V]) delkey(key K) (found bool, next rb.Iterator) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if isNil(key) {
		next = s.tree.Limit()
		return
	}

	//vv("deleting id='%v' -> it.Item() = '%v'", id, it.Item())
	query := &okv[K, V]{key: key}
	var it rb.Iterator
	it, found = s.tree.FindGE_isEqual(query)
	if found {
		atomic.AddInt64(&s.version, 1)
		s.ordercache = nil
		s.cacheversion = 0
		next = it.Next()
		s.tree.DeleteWithIterator(it)
	} else {
		next = it // Limit
	}
	return
}

func (s *syncomap[K, V]) deleteWithIter(it rb.Iterator) (found bool, next rb.Iterator) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if it.Limit() {
		// return Limit, this one is
		// at hand, and any will do.
		next = it
		return
	}

	kv, ok := it.Item().(*okv[K, V])
	if !ok {
		// bad it
		next = s.tree.Limit()
		return
	}

	// verify in tree first.
	//vv("deleteWithIter before changes: '%v'", s)
	it, found = s.tree.FindGE_isEqual(kv)
	if found {
		atomic.AddInt64(&s.version, 1)
		s.ordercache = nil
		s.cacheversion = 0
		next = it.Next()
		s.tree.DeleteWithIterator(it)
	} else {
		next = it // Limit
	}
	//vv("deleteWithIter after changes: '%v'", s)
	return
}

// deleteAll clears the tree in O(1) time.
func (s *syncomap[K, V]) deleteAll() {
	s.mu.Lock()
	defer s.mu.Unlock()

	atomic.AddInt64(&s.version, 1)
	s.ordercache = nil
	s.cacheversion = 0
	s.tree.DeleteAll()
}

// set is an upsert. It does an insert if the key is
// not already present returning newlyAdded true;
// otherwise it updates the current key's value in place.
func (s *syncomap[K, V]) set(key K, val V) (newlyAdded bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if isNil(key) {
		return
	}
	atomic.AddInt64(&s.version, 1)
	s.ordercache = nil
	s.cacheversion = 0

	query := &okv[K, V]{key: key, val: val}
	it, found := s.tree.FindGE_isEqual(query)
	if found {
		prev := it.Item().(*okv[K, V])
		//vv("id already in tree, just update in place: key='%v'; prev='%#v'", keyprev)
		prev.val = val
		return
	}
	newlyAdded = true
	_, it = s.tree.InsertGetIt(query)

	return
}

// all starts an iteration over all elements in
// the omap. To allow the user to delete in
// the middle of iteration, there is no locking
// internally.
func (s *syncomap[K, V]) all() iter.Seq2[K, V] {

	seq2 := func(yield func(K, V) bool) {

		//vv("start of all iteration.")
		n := s.tree.Len()
		nc := len(s.ordercache)

		// detect deletes in the middle of using s.ordercache.
		vers := atomic.LoadInt64(&s.version)

		if nc == n && s.cacheversion == vers {
			// s.ordercache is usable.
			nextit := s.tree.Min()
			for i, kv := range s.ordercache {
				nextit = nextit.Next() // in case of slow path below
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
					var kv *okv[K, V]
					for !nextit.Limit() {
						kv = nextit.Item().(*okv[K, V])
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

			kv := it.Item().(*okv[K, V])
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

// allokv returns the okv(s) not the val. This
// allows highly efficient val updates in place, but
// is mildly vulnerable to mis-use: the user must not
// change the other okv.id field. Otherwise the
// red-black tree will be borked.
//
// Hence this function is for performance oriented users who
// can guarantee their code will leave okv.id (and okv.it,
// and most probably okv.key too) alone. You can
// read these, but don't write. If you
// need to change the okv.id/key, you must delkey or
// deleteWithIter to remove the old key from the tree first;
// then add in the new key. This allows the tree
// to properly rebalance itself.
//
// The tree does not care about okv.val, so the user
// can update that at will. The okv.it, like the okv.id/key
// should be considered const/not be altered by user code.
// It is the iterator that points into the red-back
// tree, and so allows efficient start of iteration in the
// middle and/or delete in O(1) rather than O(log n) from
// the middle of the tree.
func (s *syncomap[K, V]) allokv() iter.Seq2[K, *okv[K, V]] {

	seq2 := func(yield func(K, *okv[K, V]) bool) {

		//vv("start of all iteration.")
		n := s.tree.Len()
		nc := len(s.ordercache)

		// detect deletes in the middle of using s.ordercache.
		vers := atomic.LoadInt64(&s.version)

		if nc == n && s.cacheversion == vers {
			// s.ordercache is usable.
			nextit := s.tree.Min()
			for i, kv := range s.ordercache {
				nextit = nextit.Next()
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
					var kv *okv[K, V]
					for !nextit.Limit() {
						kv = nextit.Item().(*okv[K, V])
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

			kv := it.Item().(*okv[K, V])
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
func (s *syncomap[K, V]) get2(key K) (val V, found bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if isNil(key) {
		return
	}
	var it rb.Iterator
	query := &okv[K, V]{key: key}
	it, found = s.tree.FindGE_isEqual(query)
	if found {
		prev := it.Item().(*okv[K, V])
		val = prev.val
		return
	}
	return
}

// get does get2 but without the found flag.
func (s *syncomap[K, V]) get(key K) (val V) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if isNil(key) {
		return
	}
	query := &okv[K, V]{key: key}
	it, found := s.tree.FindGE_isEqual(query)
	if found {
		val = it.Item().(*okv[K, V]).val
	}
	return
}

// getokv returns the okv[K,V] struct corresponding to key in
// O(1) constant time per query. If the key is
// found, the kv.it will point to it in the omap tree,
// which can be used to walk the
// tree in sorted order forwards or
// back from that point. The okv is what the
// tree stores, so this provides for
// for fast updates of okv.val if required. Note
// the okv.id and should not be changed, as that
// would invalidate the tree without notifying
// it of the need to rebalance.
func (s *syncomap[K, V]) getokv(key K) (kv *okv[K, V], found bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if isNil(key) {
		return
	}
	query := &okv[K, V]{key: key}
	var it rb.Iterator
	it, found = s.tree.FindGE_isEqual(query)
	if found {
		kv = it.Item().(*okv[K, V])
	}
	return
}
