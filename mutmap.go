package rpc25519

import (
	"sync"
)

type mutmap[K comparable, V any] struct {
	mut sync.RWMutex
	m   map[K]V
}

func newMutmap[K comparable, V any]() *mutmap[K, V] {
	return &mutmap[K, V]{
		m: make(map[K]V),
	}
}

func (m *mutmap[K, V]) get(key K) (val V, ok bool) {
	m.mut.RLock()
	val, ok = m.m[key]
	m.mut.RUnlock()
	return
}

func (m *mutmap[K, V]) getValSlice() (slc []V) {
	m.mut.RLock()
	for _, v := range m.m {
		slc = append(slc, v)
	}
	m.mut.RUnlock()
	return
}

func (m *mutmap[K, V]) set(key K, val V) {
	m.mut.Lock()
	m.m[key] = val
	m.mut.Unlock()
}

func (m *mutmap[K, V]) del(key K) {
	m.mut.Lock()
	delete(m.m, key)
	m.mut.Unlock()
}

// n gives the count of items left in map after deleting key.
func (m *mutmap[K, V]) getValNDel(key K) (val V, n int, ok bool) {
	m.mut.Lock()
	val, ok = m.m[key]
	if ok {
		delete(m.m, key)
	}
	n = len(m.m)
	m.mut.Unlock()
	return
}

// getN returns the number of keys in the map.
func (m *mutmap[K, V]) getN() (n int) {
	m.mut.RLock()
	n = len(m.m)
	m.mut.RUnlock()
	return
}

func (m *mutmap[K, V]) clear() {
	m.mut.Lock()
	clear(m.m)
	m.mut.Unlock()
}
