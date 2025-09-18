package art

import (
	"bytes"
	"encoding/binary"
	"fmt"
	//"runtime"
	//mathrand2 "math/rand/v2"

	"math/rand"
	"sync"
	"testing"
	//"time"
)

const seed = 1

func newValue(v int) []byte {
	return []byte(fmt.Sprintf("%05d", v))
}

func randomKey(rng *rand.Rand, b []byte) []byte {
	key := rng.Uint32()
	key2 := rng.Uint32()
	binary.LittleEndian.PutUint32(b, key)
	binary.LittleEndian.PutUint32(b[4:], key2)
	return b
}

func randomKey2(rng *rand.Rand) []byte {
	b := make([]byte, 8)
	key := rng.Uint32()
	key2 := rng.Uint32()
	binary.LittleEndian.PutUint32(b, key)
	binary.LittleEndian.PutUint32(b[4:], key2)
	return b
}

// Standard test. Some fraction is read. Some fraction is write. Writes have
// to go through mutex lock.

// Standard test. Some fraction is read. Some fraction is write. Writes have
// to go through mutex lock.
func BenchmarkArtReadWrite(b *testing.B) {
	value := newValue(123)
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			l := NewArtTree()
			b.ResetTimer()
			//var count int
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(seed))
				var rkey [8]byte
				for pb.Next() {
					rk := randomKey(rng, rkey[:])

					if rng.Float32() < readFrac {
						l.FindExact(rk)
					} else {
						l.Insert(rk, value)
					}
				}
			})
		})
	}
}

/*
// makes no difference for Art, the allocating
// the key inside the FindExact()/Insert() calls.
// Bizarrely it makes a 2x difference for the Ctrie.
func BenchmarkArtReadWrite2(b *testing.B) {
	value := newValue(123)
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			l := NewArtTree()
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(seed))
				for pb.Next() {

					if rng.Float32() < readFrac {
						l.FindExact(randomKey2(rng))
					} else {
						l.Insert(randomKey2(rng), value)
					}
				}
			})
		})
	}
}
*/

func BenchmarkArtLinuxPaths(b *testing.B) {

	paths := loadTestFile("assets/linux.txt")
	n := len(paths)
	_ = n

	//for i := 0; i <= 1; i++ {
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		_ = readFrac
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			l := NewArtTree()
			b.ResetTimer()
			//var count int
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(seed))
				for pb.Next() {
					for k := range paths {
						if rng.Float32() < readFrac {
							//l.FindExact(randomKey(rng))
							l.FindExact(paths[k])
							//l.Remove(paths[k])
						} else {
							//l.Insert(randomKey(rng), value)
							l.Insert(paths[k], paths[k])
						}
					}
				}
			})
		})
	}
}

// Standard test. Some fraction is read. Some fraction is write. Writes have
// to go through mutex lock.
func BenchmarkReadWrite_map_RWMutex_wrapped(b *testing.B) {
	value := newValue(123)
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			m := make(map[string][]byte)
			var mutex sync.RWMutex
			b.ResetTimer()
			var count int
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(seed))
				var rkey [8]byte
				for pb.Next() {
					rk := randomKey(rng, rkey[:])
					if rng.Float32() < readFrac {
						mutex.RLock()
						_, ok := m[string(rk)]
						mutex.RUnlock()
						if ok {
							count++
						}
					} else {
						mutex.Lock()
						m[string(rk)] = value
						mutex.Unlock()
					}
				}
			})
		})
	}
}

// bah. will crash the tester if run in parallel.
// so don't run in parallel.
func BenchmarkReadWrite_Map_NoMutex_NoParallel(b *testing.B) {
	value := newValue(123)
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			m := make(map[string][]byte)
			b.ResetTimer()
			var count int

			rng := rand.New(rand.NewSource(seed))
			var rkey [8]byte

			for range b.N {
				rk := randomKey(rng, rkey[:])
				if rng.Float32() < readFrac {
					_, ok := m[string(rk)]
					if ok {
						count++
					}
				} else {
					m[string(rk)] = value
				}
			}
		})
	}
}

// Hmm... turns out the locking makes little (in a single
// threaded scenario, but of course!)
//
// without locking: (100% writes 1st, 100% reads 2nd) single goroutine
// BenchmarkArtReadWrite_NoLocking_NoParallel/frac_0-8         	 1750966	       697.6 ns/op	     192 B/op	       4 allocs/op
// BenchmarkArtReadWrite_NoLocking_NoParallel/frac_10-8        	86214415	        13.32 ns/op	       0 B/op	       0 allocs/op
//
// with locking (single goroutine)
// BenchmarkArtReadWrite_NoLocking_NoParallel/frac_0-8         	 1652438	       704.1 ns/op	     194 B/op	       4 allocs/op
// BenchmarkArtReadWrite_NoLocking_NoParallel/frac_10-8        	53834798	        20.88 ns/op	       0 B/op	       0 allocs/op
//

func BenchmarkArtReadWrite_NoLocking_NoParallel(b *testing.B) {
	value := newValue(123)
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			l := NewArtTree()
			l.SkipLocking = true
			b.ResetTimer()

			rng := rand.New(rand.NewSource(seed))
			var rkey [8]byte

			for range b.N {
				rk := randomKey(rng, rkey[:])
				if rng.Float32() < readFrac {
					l.FindExact(rk)
				} else {
					l.Insert(rk, value)
				}
			}
		})
	}
}

type kvs struct {
	key string
	val string
}

func LeafLess(a, b *Leaf) bool {
	return bytes.Compare(a.Key, b.Key) < 0
}

// Standard test. Some fraction is read. Some fraction is write. Writes have
// to go through mutex lock.
func BenchmarkReadWriteSyncMap(b *testing.B) {
	value := newValue(123)
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			var m sync.Map
			b.ResetTimer()
			var count int
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(seed))
				for pb.Next() {
					if rng.Float32() < readFrac {
						_, ok := m.Load(string(randomKey2(rng)))
						if ok {
							count++
						}
					} else {
						m.Store(string(randomKey2(rng)), value)
					}
				}
			})
		})
	}
}
