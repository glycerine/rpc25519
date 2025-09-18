package tube

import (
	"sync"
)

// Syncmap is a generic map with a sync.Map underneath.
type SyncMap[K comparable, V any] struct {
	m sync.Map
}

/*
docs:
The zero Map is empty and ready for use. A Map must
not be copied after first use.
func (m *Map) Clear()
func (m *Map) CompareAndDelete(key, old any) (deleted bool)
func (m *Map) CompareAndSwap(key, old, new any) (swapped bool)
func (m *Map) Delete(key any)
func (m *Map) Load(key any) (value any, ok bool)
func (m *Map) LoadAndDelete(key any) (value any, loaded bool)
func (m *Map) LoadOrStore(key, value any) (actual any, loaded bool)
func (m *Map) Range(f func(key, value any) bool)
func (m *Map) Store(key, value any)
func (m *Map) Swap(key, value any) (previous any, loaded bool)
*/

// NewSyncMap creates a new mutex-protected map.
func NewSyncMap[K comparable, V any]() *SyncMap[K, V] {
	return &SyncMap[K, V]{}
}

// Get returns the value val for key.
func (m *SyncMap[K, V]) Get(key K) (val V, ok bool) {
	var v any
	v, ok = m.m.Load(key)
	val = v.(V)
	return
}

func (m *SyncMap[K, V]) Len() (n int) {
	// not a consistent snapshot if
	// deletes or adds happen during, but we don't care.
	m.m.Range(func(key, value any) bool {
		n++
		return true
	})
	return
}

// GetValSlice returns all the values in the map in slc.
func (m *SyncMap[K, V]) GetValSlice() (slc []V) {

	m.m.Range(func(key, value any) bool {
		slc = append(slc, value.(V))
		return true
	})
	return
}

// GetKeySlice returns all the keys in the map in slc.
func (m *SyncMap[K, V]) GetKeySlice() (slc []K) {
	m.m.Range(func(key, value any) bool {
		slc = append(slc, key.(K))
		return true
	})
	return
}

// Set a single key to value val.
func (m *SyncMap[K, V]) Set(key K, val V) {
	m.m.Store(key, val)
}

// Del deletes key from the map.
func (m *SyncMap[K, V]) Del(key K) {
	m.m.Delete(key)
}

// GetValNDel returns the val for key, and deletes it.
func (m *SyncMap[K, V]) GetValNDel(key K) (val V, ok bool) {

	var v any
	v, ok = m.m.LoadAndDelete(key)
	if ok {
		val = v.(V)
	}
	return
}

// Clear deletes all keys from the map.
func (m *SyncMap[K, V]) Clear() {
	m.m.Clear()
}

// Update atomically runs updateFunc on the SyncMap.
func (m *SyncMap[K, V]) Range(updateFunc func(key K, value V)) {
	m.m.Range(func(key, value any) bool {
		k := key.(K)
		v := value.(V)
		updateFunc(k, v)
		return true
	})
}

// Reset discards map contents, same as Clear().
func (m *SyncMap[K, V]) Reset() {
	m.m.Clear()
}
