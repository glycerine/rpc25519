package rpc25519

import (
	"cmp"
	"iter"
)

// dmap is a deterministic map, that
// can be iterated in a deterministic order.
type dmap[K cmp.Ordered, V any] struct {
	keys []K
	vals []V
	// lazy index, only made on demand so our
	// zero value is useful without an init.
	lazy map[K]int
}

func (s *dmap[K, V]) Len() int {
	return len(s.keys)
}
func (s *dmap[K, V]) Swap(i, j int) {
	s.keys[i], s.keys[j] = s.keys[j], s.keys[i]
	s.vals[i], s.vals[j] = s.vals[j], s.vals[i]
}
func (s *dmap[K, V]) Less(i, j int) bool {
	return cmp.Less(s.keys[i], s.keys[j])
	// or cmp.Compare() < 0
}

func rangeAll[K cmp.Ordered, V any](m *dmap[K, V]) iter.Seq2[K, V] {

	return func(yield func(K, V) bool) {
		for i, k := range m.keys {
			if !yield(k, m.vals[i]) {
				return
			}
		}
	}
}
