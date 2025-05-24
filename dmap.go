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

// dmap is a deterministic map, that can be
// range iterated in a repeatable order,
// unlike the Go's builtin map. This is
// important for simulation testing.
//
// The key's ided interface supplies a
// sortable id() string which determines
// the range all() order, and gives O(log n)
// upserts. The get and del methods are O(1),
// as is deleteAll. For repeated full range all
// scans, we cache the values in contiguous
// memory to maximize L1 cache hits and
// minimize pointer chasing in the underlying
// red-black tree.
//
// Thus dmap aims to be almost as fast, or
// faster, than the built in Go map, for common
// use patterns, while providing deterministic,
// repeatable iteration order.
//
// To provide these guarantees, it uses approximately 3x
// the memory compared to the builtin map. The
// memory goes towards (1) a built in map for
// fast O(1) get and del operations; (2) full range scans are
// cached in contiguous slice of memory; and (3)
// a red-black tree is maintained to allow efficient
// insertion into/update of the ordered key-value dictionary
// in O(log n) time.
type dmap[K ided, V any] struct {
	version int64

	tree *rb.Tree
	idx  map[string]rb.Iterator

	// cache the first range all, and use
	// ordercache if we range all again without
	// intervening upsert or deletes.
	ordercache []*ikv[K, V]
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
	lim := s.tree.Limit()
	it := s.tree.Min()
	i := 0
	extra := ""
	for it != lim {
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

	id := key.id()

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
	atomic.AddInt64(&s.version, 1)
	s.ordercache = nil
	next = it.Next()
	s.tree.DeleteWithIterator(it)
	delete(s.idx, id)
	return
}

func (s *dmap[K, V]) deleteWithIter(it rb.Iterator) (found bool, next rb.Iterator) {

	kv, ok := it.Item().(*ikv[K, V])
	if !ok {
		// bad it
		return
	}

	if s.idx == nil {
		// nothing present
		next = s.tree.Limit()
		return
	} else {
		_, ok = s.idx[kv.id]
		if !ok {
			// not present
			next = s.tree.Limit()
			return
		}
	}
	//vv("deleteWithIter before changes: '%v'", s)
	atomic.AddInt64(&s.version, 1)
	s.ordercache = nil
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
	s.tree.DeleteAll()
	s.idx = nil
}

// upsert does an insert if the key is
// not already present returning newlyAdded true;
// otherwise it updates the current key's value in place.
func (s *dmap[K, V]) upsert(key K, val V) (newlyAdded bool) {

	atomic.AddInt64(&s.version, 1)
	s.ordercache = nil

	id := key.id()

	var it rb.Iterator
	var ok bool
	if s.idx == nil {
		s.idx = make(map[string]rb.Iterator)
	} else {
		it, ok = s.idx[id]
	}

	if !ok {
		// not yet in idx/tree, so add it.
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
	// id already in tree, just update in place
	prev := it.Item().(*ikv[K, V])
	prev.key = key
	prev.val = val
	return
}

// unlocked true means we yield values without
// holding any locks, so the user can delete.
func all[K ided, V any](s *dmap[K, V]) iter.Seq2[K, *ikv[K, V]] {

	seq2 := func(yield func(K, *ikv[K, V]) bool) {

		//vv("start of all iteration.")
		n := s.tree.Len()
		nc := len(s.ordercache)
		vers := atomic.LoadInt64(&s.version)
		hit := false
		if nc == n {
			hit = true // s.ordercache is usable.
		}
		// allow the user to delete in the middle of iteration.
		if hit {
			for i, kv := range s.ordercache {
				nextit := kv.it.Next() // in case of slow path below
				if !yield(kv.key, kv) {
					return
				}
				vers2 := atomic.LoadInt64(&s.version)
				if vers2 != vers {
					//vv("vers2 %v != vers %v down shifting stack=\n%v", vers2, vers, stack())
					// delete in middle of iteration.
					// abandon oc, down shift to
					// slow/safe path using nextit.
					n2 := s.tree.Len()
					if i < n2-1 {
						// still have some left
						lim := s.tree.Limit()
						it := nextit
						var kv *ikv[K, V]
						for nextit != lim {
							it = nextit
							kv = it.Item().(*ikv[K, V])
							// pre-advance, allows deletion via it.
							nextit = nextit.Next()
							if !yield(kv.key, kv) {
								return
							}
							//vv("back from yield 2nd")
						}
					}
					return // essential, cannot resume 1st loop.
				} // end if vers2 != vers
			} // end for i over oc
			return
		} // end if hit

		// cache miss. only do full fills for simplicity.
		s.ordercache = nil
		cachegood := true // invalidate if delete in middle of all.
		lim := s.tree.Limit()
		it := s.tree.Min()
		for it != lim {

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

// get returns the val corresponding to key in
// O(1) constant time per query. If they key
// found, the it will point to it in the dmap tree,
// which can be used to iterator forward or
// back from that point.
func (s *dmap[K, V]) get(key K) (kv *ikv[K, V], found bool) {

	id := key.id()
	var it rb.Iterator
	if s.idx == nil {
		// not present
		return
	} else {
		it, found = s.idx[id]
		if !found {
			return
		}
	}
	kv = it.Item().(*ikv[K, V])
	return
}
