package rpc25519

import (
	"fmt"
	"math/rand"
	"testing"
)

var _ = fmt.Sprintf
var _ = rand.Intn

func TestOmap(t *testing.T) {
	m := newOmap[int, int]()

	for i := range 9 {
		m.set(8-i, 8-i)
	}
	//vv("m = '%#v'", m)
	i := 0
	for pd, j := range m.all() {
		if j != i {
			t.Fatalf("expected val %v, got %v for pd='%#v'", i, j, pd)
		}
		//vv("at i = %v, see pd = '%#v'", i, pd)
		i++
	}
	//vv("done with first all: have %v elements", i)
	i = 0
	// delete odds over 2
	for pd, kv := range m.allokv() {
		_ = pd
		//j := kv.val
		//if j != i {
		//	t.Fatalf("expected val %v, got %v for pd='%#v'", i, j, pd)
		//}
		if i > 2 && i%2 == 1 {
			//vv("about to deleteWithIter(%v)", i)
			m.deleteWithIter(kv.it)
		}
		//vv("at i = %v, see pd = '%#v'", i, pd)
		i++
	}
	ne := m.Len()
	//vv("done with deleting 3,5,7: have %v elements. omap = '%v'", ne, m)
	if ne != 6 {
		t.Fatalf("expected 6 now, have %v", ne)
	}

	expect := []int{0, 1, 2, 4, 6, 8} // , deleted 3,5,7
	i = 0
	for pd, kv := range m.allokv() {
		j := kv.val
		if j != expect[i] {
			t.Fatalf("expected val %v, got %v for pd='%#v'", expect[i], j, pd)
		}
		//vv("at i = %v, see pd = '%#v'", i, pd)
		i++
	}
	if i != len(expect) {
		t.Fatalf("rest of the set? missing '%#v'", expect[i:])
	}
}

// TestOmapBasicOperations tests basic map operations (get, set, delete)
func TestOmapBasicOperations(t *testing.T) {
	m := newOmap[int, int]()

	// Test empty map
	if m.Len() != 0 {
		t.Errorf("expected empty map, got len %d", m.Len())
	}

	// Test set and get
	m.set(1, 42)
	if val, found := m.get2(1); !found || val != 42 {
		t.Errorf("get2 after set: expected (42, true), got (%v, %v)", val, found)
	}

	// Test update
	m.set(1, 43)
	if val, found := m.get2(1); !found || val != 43 {
		t.Errorf("get2 after update: expected (43, true), got (%v, %v)", val, found)
	}

	// Test delete
	if found, _ := m.delkey(1); !found {
		t.Error("delkey: expected true, got false")
	}
	if val, found := m.get2(1); found {
		t.Errorf("get2 after delete: expected (0, false), got (%v, %v)", val, found)
	}

	// Test delete non-existent key
	if found, _ := m.delkey(2); found {
		t.Error("delkey non-existent: expected false, got true")
	}
}

// TestOmapDeterministicOrder verifies that range iteration order is deterministic
func TestOmapDeterministicOrder(t *testing.T) {
	m := newOmap[int, int]()

	// Insert keys in random order
	keys := []int{26, 0, 13, 2, 24, 3}
	for i, k := range keys {
		m.set(k, i)
	}

	// First iteration
	var firstOrder []int
	for k, _ := range m.all() {
		firstOrder = append(firstOrder, k)
	}

	// Second iteration should match first
	var secondOrder []int
	for k, _ := range m.all() {
		secondOrder = append(secondOrder, k)
	}

	if len(firstOrder) != len(secondOrder) {
		t.Errorf("iteration lengths differ: %d vs %d", len(firstOrder), len(secondOrder))
	}

	for i := range firstOrder {
		if firstOrder[i] != secondOrder[i] {
			t.Errorf("iteration order differs at index %v: %v vs %v",
				i, firstOrder[i], secondOrder[i])
		}
	}

	// Verify order is actually sorted
	for i := 1; i < len(firstOrder); i++ {
		if firstOrder[i-1] > firstOrder[i] {
			t.Errorf("iteration not sorted: %v > %v", firstOrder[i-1], firstOrder[i])
		}
	}
}

// TestOmapVsBuiltinMap compares omap behavior with built-in map
func TestOmapVsBuiltinMap(t *testing.T) {
	omap := newOmap[int, int]()
	builtin := make(map[int]int)

	// Test operations that should behave identically
	ops := []struct {
		name string
		key  int
		val  int
	}{
		{"set1", 1, 1},
		{"set2", 2, 2},
		{"update1", 1, 3},
		{"set3", 3, 4},
		{"delete1", 2, 0},
	}

	for _, op := range ops {
		k := op.key

		// Perform operation on both maps
		switch op.name[:3] {
		case "set":
			omap.set(k, op.val)
			builtin[op.key] = op.val
		case "del":
			omap.delkey(k)
			delete(builtin, op.key)
		}

		// Verify results match
		dval, dfound := omap.get2(k)
		bval, bfound := builtin[op.key]

		if dfound != bfound {
			t.Errorf("%s: found mismatch: omap=%v, builtin=%v",
				op.name, dfound, bfound)
		}
		if dfound && dval != bval {
			t.Errorf("%s: value mismatch: omap=%v, builtin=%v",
				op.name, dval, bval)
		}
	}

	// Verify final lengths match
	if omap.Len() != len(builtin) {
		t.Errorf("final length mismatch: omap=%d, builtin=%d",
			omap.Len(), len(builtin))
	}
}

// TestOmapDeleteDuringIteration tests deletion during iteration
func TestOmapDeleteDuringIteration(t *testing.T) {
	m := newOmap[int, int]()

	// Insert some keys
	for i := range 10 {
		m.set(i, i)
	}

	// Delete every other key during iteration
	deleted := make(map[int]bool)
	for k, v := range m.all() {
		if v%2 == 0 {
			m.delkey(k)
			deleted[k] = true
		}
	}

	// Verify remaining keys
	for k, v := range m.all() {
		if deleted[k] {
			t.Errorf("key %v should have been deleted", k)
		}
		if v%2 != 1 {
			t.Errorf("remaining key %v has even value %d", k, v)
		}
	}
}

// TestOmapEdgeCases tests edge cases and corner conditions
func TestOmapEdgeCases(t *testing.T) {
	m := newOmap[int, int]()

	// Test deleteAll
	m.set(0, 1)
	m.set(1, 2)
	m.deleteAll()
	if m.Len() != 0 {
		t.Errorf("after deleteAll: expected len 0, got %d", m.Len())
	}

	// Test getokv
	k := 9
	m.set(k, 42)
	okv, found := m.getokv(k)
	if !found {
		t.Error("getokv: expected true, got false")
	}
	if okv.val != 42 {
		t.Errorf("getokv: expected val 42, got %d", okv.val)
	}

	// Test getokv non-existent
	k2 := 595
	if okv, found := m.getokv(k2); found {
		t.Errorf("getokv non-existent: expected false, got true with val %d", okv.val)
	}
}

// TestOmapString tests the String() method
func TestOmapString(t *testing.T) {
	m := newOmap[int, int]()

	// Empty map
	if s := m.String(); s != "omap{ version:0 {}}" {
		t.Errorf("empty map String(): expected 'omap{ version:0 {}}', got '%v'", s)
	}

	// Map with some keys
	m.set(1, 1)
	m.set(2, 2)
	s := m.String()
	if s != "omap{ version:2 {1:1, 2:2}}" {
		t.Errorf("non-empty map String(): expected 'omap{ version:2 {1:1, 2:2}}', got '%v'", s)
	}
}

// TestOmapRandomOperations performs random operations and verifies
// omap matches built-in map behavior
func TestOmapRandomOperations(t *testing.T) {
	const (
		numKeys = 7
		numOps  = 1_000_000
		seed    = 42 // fixed seed for reproducibility
	)

	// Create test keys
	keys := make([]int, numKeys)
	for i := range keys {
		keys[i] = i
	}

	// Initialize maps
	omap := newOmap[int, int]()
	builtin := make(map[int]int)

	// Create random number generator with fixed seed
	rng := rand.New(rand.NewSource(seed))

	// Track operations for debugging
	type op struct {
		typ string // "set", "get", "del"
		key int
		val int
	}
	ops := make([]op, 0, numOps)

	// Perform random operations
	for i := 0; i < numOps; i++ {
		// Choose random key
		keyIdx := rng.Intn(numKeys)
		k := keys[keyIdx]

		// Choose random operation
		opType := rng.Intn(3) // 0=set, 1=get, 2=del
		var val int
		var opStr string

		switch opType {
		case 0: // set
			val = rng.Intn(1000)
			omap.set(k, val)
			builtin[k] = val
			opStr = "set"
		case 1: // get
			dval, dfound := omap.get2(k)
			bval, bfound := builtin[k]
			if dfound != bfound {
				t.Errorf("get mismatch at op %d: omap=%v, builtin=%v",
					i, dfound, bfound)
			}
			if dfound && dval != bval {
				t.Errorf("get value mismatch at op %d: omap=%v, builtin=%v",
					i, dval, bval)
			}
			opStr = "get"
		case 2: // del
			dfound, _ := omap.delkey(k)
			bval, bfound := builtin[k]
			delete(builtin, k)
			if dfound != bfound {
				t.Errorf("del mismatch at op %d: omap=%v, builtin=%v",
					i, dfound, bfound)
			}
			val = bval
			opStr = "del"
		}

		ops = append(ops, op{opStr, k, val})

		// verify lengths match
		if omap.Len() != len(builtin) {
			t.Errorf("length mismatch at op %d: omap=%d, builtin=%d",
				i, omap.Len(), len(builtin))
			// Print last few operations for debugging
			start := max(0, len(ops)-5)
			t.Errorf("last operations: %v", ops[start:])
		}

		// Verify all keys in omap are in builtin and vice versa
		for k, v := range omap.all() {
			if bval, found := builtin[k]; !found || bval != v {
				t.Errorf("final key mismatch: key=%v, omap=%v, builtin=%v (found=%v)",
					k, v, bval, found)
			}
		}
		for k, v := range builtin {
			if dval, found := omap.get2(k); !found || dval != v {
				t.Errorf("final key mismatch: key=%v, omap=%v (found=%v), builtin=%v",
					k, dval, found, v)
			}
		}
	}
	fmt.Printf("omap and map match, %v ops\n", numOps)
}

// BenchmarkOmapVsBuiltin benchmarks omap against built-in map
func BenchmarkOmapVsBuiltin(b *testing.B) {
	const numKeys = 1000

	// Create test keys
	keys := make([]int, numKeys)
	for i := range keys {
		keys[i] = i
	}

	// Benchmark Set operations
	b.Run("Set", func(b *testing.B) {
		b.Run("Omap", func(b *testing.B) {
			m := newOmap[int, int]()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				k := keys[i%numKeys]
				m.set(k, i)
			}
		})
		b.Run("Builtin", func(b *testing.B) {
			m := make(map[int]int)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				k := keys[i%numKeys]
				m[k] = i
			}
		})
	})

	// Benchmark Get operations
	b.Run("Get", func(b *testing.B) {
		// Setup maps with data
		omap := newOmap[int, int]()
		builtin := make(map[int]int)
		for i := range keys {
			omap.set(keys[i], i)
			builtin[keys[i]] = i
		}

		b.Run("Omap", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				k := keys[i%numKeys]
				_ = omap.get(k)
			}
		})
		b.Run("Builtin", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				k := keys[i%numKeys]
				_ = builtin[k]
			}
		})
	})

	// Benchmark Delete operations
	b.Run("Delete", func(b *testing.B) {
		b.Run("Omap", func(b *testing.B) {
			m := newOmap[int, int]()
			// Pre-fill map
			for i := range keys {
				m.set(keys[i], i)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				k := keys[i%numKeys]
				m.delkey(k)
				// Re-insert to maintain size
				m.set(k, i)
			}
		})
		b.Run("Builtin", func(b *testing.B) {
			m := make(map[int]int)
			// Pre-fill map
			for i := range keys {
				m[keys[i]] = i
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				k := keys[i%numKeys]
				delete(m, k)
				// Re-insert to maintain size
				m[k] = i
			}
		})
	})

	// Benchmark Range iteration
	b.Run("Range", func(b *testing.B) {
		// Setup maps with data
		omap := newOmap[int, int]()
		builtin := make(map[int]int)
		for i := range keys {
			omap.set(keys[i], i)
			builtin[keys[i]] = i
		}

		b.Run("Omap", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				sum := 0
				for _, v := range omap.all() {
					sum += v
				}
				_ = sum
			}
		})
		b.Run("Omap ordercache", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				sum := 0
				for _, kv := range omap.cached() {
					sum += kv.val
				}
				_ = sum
			}
		})
		b.Run("Builtin", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				sum := 0
				for _, v := range builtin {
					sum += v
				}
				_ = sum
			}
		})
	})
}

/*
goos: darwin
goarch: amd64
pkg: github.com/glycerine/rpc25519
cpu: Intel(R) Core(TM) i7-1068NG7 CPU @ 2.30GHz
BenchmarkOmapVsBuiltin
BenchmarkOmapVsBuiltin/Set
BenchmarkOmapVsBuiltin/Set/Omap
BenchmarkOmapVsBuiltin/Set/Omap-8      12419175  100.8 ns/op
BenchmarkOmapVsBuiltin/Set/Builtin
BenchmarkOmapVsBuiltin/Set/Builtin-8   100000000  11.21 ns/op
BenchmarkOmapVsBuiltin/Get
BenchmarkOmapVsBuiltin/Get/Omap
BenchmarkOmapVsBuiltin/Get/Omap-8      12415286    95.29 ns/op
BenchmarkOmapVsBuiltin/Get/Builtin
BenchmarkOmapVsBuiltin/Get/Builtin-8   157784671    7.273 ns/op
BenchmarkOmapVsBuiltin/Delete
BenchmarkOmapVsBuiltin/Delete/Omap
BenchmarkOmapVsBuiltin/Delete/Omap-8    3309788      359.5 ns/op
BenchmarkOmapVsBuiltin/Delete/Builtin
BenchmarkOmapVsBuiltin/Delete/Builtin-8 36208432      33.29 ns/op
BenchmarkOmapVsBuiltin/Range
BenchmarkOmapVsBuiltin/Range/Omap
BenchmarkOmapVsBuiltin/Range/Omap-8              324283   3792 ns/op
BenchmarkOmapVsBuiltin/Range/Omap_ordercache
BenchmarkOmapVsBuiltin/Range/Omap_ordercache-8  2984863    396.2 ns/op
BenchmarkOmapVsBuiltin/Range/Builtin
BenchmarkOmapVsBuiltin/Range/Builtin-8           143560   8109 ns/op
PASS
ok    github.com/glycerine/rpc25519  13.470s

Compilation finished at Sun May 25 22:51:33
*/
