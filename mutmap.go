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

func (m *mutmap[K, V]) clear() {
	m.mut.Lock()
	clear(m.m)
	m.mut.Unlock()
}
