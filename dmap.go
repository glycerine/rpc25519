package rpc25519

import (
	"iter"
	"sort"
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
	keys  []string
	vals  []V
	ideds []K
	// lazy index, can be made on demand so our
	// zero value is useful without an init.
	idx map[string]bool
}

func newDmap[K ided, V any]() *dmap[K, V] {
	return &dmap[K, V]{
		idx: make(map[string]bool),
	}
}

func (s *dmap[K, V]) upsert(k K, val V) {
	key := k.id()
	if s.idx == nil {
		s.idx = make(map[string]bool)
	} else {
		if s.idx[key] {
			i := sort.Search(len(s.keys), func(i int) bool {
				return key <= s.keys[i]
			})
			s.vals[i] = val // updated value for key
			return
		}
	}
	// not present already
	s.idx[key] = true

	i := sort.Search(len(s.keys), func(i int) bool {
		return key <= s.keys[i]
	})
	if i == len(s.keys) {
		// key is larger than everything else
		s.keys = append(s.keys, key)
		s.vals = append(s.vals, val)
		s.ideds = append(s.ideds, k)
		return
	}
	s.keys = append(s.keys[:i], append([]string{key}, s.keys[i:]...)...)
	s.vals = append(s.vals[:i], append([]V{val}, s.vals[i:]...)...)
	s.ideds = append(s.ideds[:i], append([]K{k}, s.ideds[i:]...)...)
	//we did the equivalent of: sort.Sort(s)
}

func (s *dmap[K, V]) Len() int {
	return len(s.keys)
}
func (s *dmap[K, V]) Swap(i, j int) {
	s.keys[i], s.keys[j] = s.keys[j], s.keys[i]
	s.vals[i], s.vals[j] = s.vals[j], s.vals[i]
	s.ideds[i], s.ideds[j] = s.ideds[j], s.ideds[i]
}
func (s *dmap[K, V]) Less(i, j int) bool {
	return s.keys[i] < s.keys[j]
}

func all[K ided, V any](m *dmap[K, V]) iter.Seq2[K, V] {

	return func(yield func(K, V) bool) {
		for i := range m.keys {
			if !yield(m.ideds[i], m.vals[i]) {
				return
			}
		}
	}
}
