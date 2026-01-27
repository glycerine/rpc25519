package rpc25519

import (
	"fmt"
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

// see also GetN()
func (m *Mutexmap[K, V]) Len() (n int) {
	m.mut.RLock()
	n = len(m.m)
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

// GetKeySlice returns all the keys in the map in slc.
func (m *Mutexmap[K, V]) GetKeySlice() (slc []K) {
	m.mut.RLock()
	for k, _ := range m.m {
		slc = append(slc, k)
	}
	m.mut.RUnlock()
	return
}

func (m *Mutexmap[K, V]) String() (r string) {
	m.mut.RLock()
	i := 0
	for k, v := range m.m {
		if i == 0 {
			r = fmt.Sprintf("Mutexmap[%T, %T] of len(%v):\n", k, v, len(m.m))
		}
		r += fmt.Sprintf("key['%v'] -> val:'%v'\n", k, v)
		i++
	}
	if len(m.m) == 0 {
		r = "Mutexmap of len(0):\n"
	} else {
		r += "\n"
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

// Del deletes key from the map. Returns new size.
func (m *Mutexmap[K, V]) Del(key K) (newSz int) {
	m.mut.Lock()
	delete(m.m, key)
	newSz = len(m.m)
	m.mut.Unlock()
	return
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

// Update atomically runs updateFunc on the Mutexmap.
func (m *Mutexmap[K, V]) Update(updateFunc func(m map[K]V)) {
	m.mut.Lock()
	updateFunc(m.m)
	m.mut.Unlock()
	return
}

// ReadOnlyView atomically runs viewFunc on the Mutexmap.
// viewFunc must not alter or mutate m in any way,
// or else data races and undefined behavior will result.
func (m *Mutexmap[K, V]) ReadOnlyView(viewFunc func(m map[K]V)) {
	m.mut.RLock()
	viewFunc(m.m)
	m.mut.RUnlock()
	return
}

// GetMapReset returns the underlying map and
// resets the internal map by re-allocating it anew.
// This is useful when you want to discard
// synchronization going forward.
func (m *Mutexmap[K, V]) GetMapReset() (mm map[K]V) {
	m.mut.Lock()
	mm = m.m
	m.m = make(map[K]V)
	m.mut.Unlock()
	return
}

// GetMapCloneAtomic atomically returns a clone
// of the map, giving a consistent snapshot.
func (m *Mutexmap[K, V]) GetMapCloneAtomic() (mm map[K]V) {
	m.mut.Lock()
	mm = make(map[K]V)
	for k, v := range m.m {
		mm[k] = v
	}
	m.mut.Unlock()
	return
}

// Reset discards map contents, allocating it anew.
func (m *Mutexmap[K, V]) Reset() {
	m.mut.Lock()
	m.m = make(map[K]V)
	m.mut.Unlock()
}
