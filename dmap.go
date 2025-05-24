package rpc25519

import (
	"cmp"
	"iter"
	"sort"
)

type ided interface {
	id() string
}

/*type ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr |
		~float32 | ~float64 |
		~string
}
*/

// dmap is a deterministic map, that
// can be iterated in a deterministic order.
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
				return s.keys[i] >= key
			})
			s.vals[i] = val // updated value for key
			return
		}
	}
	// not present already
	s.idx[key] = true
	s.keys = append(s.keys, key)
	s.vals = append(s.vals, val)
	s.ideds = append(s.ideds, k)
	sort.Sort(s)
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

func rangeAll[K ided, V any](m *dmap[K, V]) iter.Seq2[K, V] {

	return func(yield func(K, V) bool) {
		for i := range m.keys {
			if !yield(m.ideds[i], m.vals[i]) {
				return
			}
		}
	}
}
