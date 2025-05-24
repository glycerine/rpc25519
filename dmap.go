package rpc25519

import (
	"iter"

	rb "github.com/glycerine/rbtree"
)

type ided interface {
	id() string
}

// dmap is a deterministic map, that can be
// range iterated in a deterministic order.
// the key's ided interace supplies a
// sortable id() string which determintes
// the range all() order.
type dmap[K ided, V any] struct {
	tree *rb.Tree
	idx  map[string]rb.Iterator
}

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
}

// delete key from the dmap, if present.
//
// If found returns true, next has the
// iterator following the deleted key.
//
// If found returns false, next is s.tree.Limit(),
// which can be used to terminate an iteration.
//
// Using next provides "advance and delete behind"
// semantics.
func (s *dmap[K, V]) del(key K) (found bool, next rb.Iterator) {
	query := &ikv[K, V]{id: key.id()}
	var it rb.Iterator
	it, found = s.tree.FindGE_isEqual(query)
	if !found {
		next = s.tree.Limit()
		return
	}
	next = it.Next()
	s.tree.DeleteWithIterator(it)
	return
}

func (s *dmap[K, V]) deleteAll() {
	s.tree.DeleteAll()
}

func (s *dmap[K, V]) upsert(key K, val V) {

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
		item := &ikv[K, V]{id: id, key: key, val: val}
		added, it2 := s.tree.InsertGetIt(item)
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
}

func all[K ided, V any](m *dmap[K, V]) iter.Seq2[K, V] {

	return func(yield func(K, V) bool) {

		lim := m.tree.Limit()

		for it := m.tree.Min(); it != lim; it = it.Next() {
			kv := it.Item().(*ikv[K, V])
			if !yield(kv.key, kv.val) {
				return
			}
		}
	}
}
