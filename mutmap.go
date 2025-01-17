package rpc25519

import (
	"sync"
)

// Mutexmap is simply a generic map protected by a sync.RWMutex.
// This provides fine-grained locking for goroutine safety.
type Mutexmap[K comparable, V any] struct {
	mut sync.RWMutex
	m   map[K]V
}

// NewMutexmap creates a new mutex-protected map.
func NewMutexmap[K comparable, V any]() *Mutexmap[K, V] {
	return &Mutexmap[K, V]{
		m: make(map[K]V),
	}
}

// Get returns the value val for key.
func (m *Mutexmap[K, V]) Get(key K) (val V, ok bool) {
	m.mut.RLock()
	val, ok = m.m[key]
	m.mut.RUnlock()
	return
}

// GetValSlice returns all the values in the map in slc.
func (m *Mutexmap[K, V]) GetValSlice() (slc []V) {
	m.mut.RLock()
	for _, v := range m.m {
		slc = append(slc, v)
	}
	m.mut.RUnlock()
	return
}

// Set a single key to value val.
func (m *Mutexmap[K, V]) Set(key K, val V) {
	m.mut.Lock()
	m.m[key] = val
	m.mut.Unlock()
}

// Del deletes key from the map.
func (m *Mutexmap[K, V]) Del(key K) {
	m.mut.Lock()
	delete(m.m, key)
	m.mut.Unlock()
}

// GetValNDel returns the val for key, and deletes it.
// The returned n gives the count of items left in map after deleting key.
func (m *Mutexmap[K, V]) GetValNDel(key K) (val V, n int, ok bool) {
	m.mut.Lock()
	val, ok = m.m[key]
	if ok {
		delete(m.m, key)
	}
	n = len(m.m)
	m.mut.Unlock()
	return
}

// GetN returns the number of keys in the map.
func (m *Mutexmap[K, V]) GetN() (n int) {
	m.mut.RLock()
	n = len(m.m)
	m.mut.RUnlock()
	return
}

// Clear deletes all keys from the map.
func (m *Mutexmap[K, V]) Clear() {
	m.mut.Lock()
	clear(m.m)
	m.mut.Unlock()
}
