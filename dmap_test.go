package rpc25519

import (
	"fmt"
	"math/rand"
	"testing"
)

// dmap tester
type dmapt struct {
	name string
}

func (s *dmapt) id() string {
	if s == nil {
		return ""
	}
	return s.name
}

func TestDmap(t *testing.T) {
	var slc []*dmapt
	m := newDmap[*dmapt, int]()

	for i := range 9 {
		d := &dmapt{name: fmt.Sprintf("%v", 8-i)}
		slc = append(slc, d)
		m.set(d, 8-i)
	}
	//vv("m = '%#v'", m)
	i := 0
	for pd, j := range all(m) {
		if j != i {
			t.Fatalf("expected val %v, got %v for pd='%#v'", i, j, pd)
		}
		//vv("at i = %v, see pd = '%#v'", i, pd)
		i++
	}
	//vv("done with first all: have %v elements", i)
	i = 0
	// delete odds over 2
	for pd, kv := range allikv(m) {
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
	//vv("done with deleting 3,5,7: have %v elements. dmap = '%v'", ne, m)
	if ne != 6 {
		t.Fatalf("expected 6 now, have %v", ne)
	}

	expect := []int{0, 1, 2, 4, 6, 8} // , deleted 3,5,7
	i = 0
	for pd, kv := range allikv(m) {
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

// TestDmapBasicOperations tests basic map operations (get, set, delete)
func TestDmapBasicOperations(t *testing.T) {
	m := newDmap[*dmapt, int]()

	// Test empty map
	if m.Len() != 0 {
		t.Errorf("expected empty map, got len %d", m.Len())
	}

	// Test set and get
	d1 := &dmapt{name: "key1"}
	m.set(d1, 42)
	if val, found := m.get2(d1); !found || val != 42 {
		t.Errorf("get2 after set: expected (42, true), got (%v, %v)", val, found)
	}

	// Test update
	m.set(d1, 43)
	if val, found := m.get2(d1); !found || val != 43 {
		t.Errorf("get2 after update: expected (43, true), got (%v, %v)", val, found)
	}

	// Test delete
	if found, _ := m.delkey(d1); !found {
		t.Error("delkey: expected true, got false")
	}
	if val, found := m.get2(d1); found {
		t.Errorf("get2 after delete: expected (0, false), got (%v, %v)", val, found)
	}

	// Test delete non-existent key
	d2 := &dmapt{name: "key2"}
	if found, _ := m.delkey(d2); found {
		t.Error("delkey non-existent: expected false, got true")
	}
}

// TestDmapDeterministicOrder verifies that range iteration order is deterministic
func TestDmapDeterministicOrder(t *testing.T) {
	m := newDmap[*dmapt, int]()

	// Insert keys in random order
	keys := []string{"z", "a", "m", "b", "y", "c"}
	for i, k := range keys {
		m.set(&dmapt{name: k}, i)
	}

	// First iteration
	var firstOrder []string
	for k, _ := range all(m) {
		firstOrder = append(firstOrder, k.id())
	}

	// Second iteration should match first
	var secondOrder []string
	for k, _ := range all(m) {
		secondOrder = append(secondOrder, k.id())
	}

	if len(firstOrder) != len(secondOrder) {
		t.Errorf("iteration lengths differ: %d vs %d", len(firstOrder), len(secondOrder))
	}

	for i := range firstOrder {
		if firstOrder[i] != secondOrder[i] {
			t.Errorf("iteration order differs at index %d: %s vs %s",
				i, firstOrder[i], secondOrder[i])
		}
	}

	// Verify order is actually sorted
	for i := 1; i < len(firstOrder); i++ {
		if firstOrder[i-1] > firstOrder[i] {
			t.Errorf("iteration not sorted: %s > %s", firstOrder[i-1], firstOrder[i])
		}
	}
}

// TestDmapVsBuiltinMap compares dmap behavior with built-in map
func TestDmapVsBuiltinMap(t *testing.T) {
	dmap := newDmap[*dmapt, int]()
	builtin := make(map[string]int)

	// Test operations that should behave identically
	ops := []struct {
		name string
		key  string
		val  int
	}{
		{"set1", "a", 1},
		{"set2", "b", 2},
		{"update1", "a", 3},
		{"set3", "c", 4},
		{"delete1", "b", 0},
	}

	for _, op := range ops {
		k := &dmapt{name: op.key}

		// Perform operation on both maps
		switch op.name[:3] {
		case "set":
			dmap.set(k, op.val)
			builtin[op.key] = op.val
		case "del":
			dmap.delkey(k)
			delete(builtin, op.key)
		}

		// Verify results match
		dval, dfound := dmap.get2(k)
		bval, bfound := builtin[op.key]

		if dfound != bfound {
			t.Errorf("%s: found mismatch: dmap=%v, builtin=%v",
				op.name, dfound, bfound)
		}
		if dfound && dval != bval {
			t.Errorf("%s: value mismatch: dmap=%v, builtin=%v",
				op.name, dval, bval)
		}
	}

	// Verify final lengths match
	if dmap.Len() != len(builtin) {
		t.Errorf("final length mismatch: dmap=%d, builtin=%d",
			dmap.Len(), len(builtin))
	}
}

// TestDmapDeleteDuringIteration tests deletion during iteration
func TestDmapDeleteDuringIteration(t *testing.T) {
	m := newDmap[*dmapt, int]()

	// Insert some keys
	for i := range 10 {
		m.set(&dmapt{name: fmt.Sprintf("%d", i)}, i)
	}

	// Delete every other key during iteration
	deleted := make(map[string]bool)
	for k, v := range all(m) {
		if v%2 == 0 {
			m.delkey(k)
			deleted[k.id()] = true
		}
	}

	// Verify remaining keys
	for k, v := range all(m) {
		if deleted[k.id()] {
			t.Errorf("key %s should have been deleted", k.id())
		}
		if v%2 != 1 {
			t.Errorf("remaining key %s has even value %d", k.id(), v)
		}
	}
}

// TestDmapEdgeCases tests edge cases and corner conditions
func TestDmapEdgeCases(t *testing.T) {
	m := newDmap[*dmapt, int]()

	// Test nil key
	var nilKey *dmapt
	if _, found := m.get2(nilKey); found {
		t.Error("get2 with nil key: expected false, got true")
	}

	// Test deleteAll
	m.set(&dmapt{name: "a"}, 1)
	m.set(&dmapt{name: "b"}, 2)
	m.deleteAll()
	if m.Len() != 0 {
		t.Errorf("after deleteAll: expected len 0, got %d", m.Len())
	}

	// Test getikv
	k := &dmapt{name: "test"}
	m.set(k, 42)
	ikv, found := m.getikv(k)
	if !found {
		t.Error("getikv: expected true, got false")
	}
	if ikv.val != 42 {
		t.Errorf("getikv: expected val 42, got %d", ikv.val)
	}

	// Test getikv non-existent
	k2 := &dmapt{name: "nonexistent"}
	if ikv, found := m.getikv(k2); found {
		t.Errorf("getikv non-existent: expected false, got true with val %d", ikv.val)
	}
}

// TestDmapString tests the String() method
func TestDmapString(t *testing.T) {
	m := newDmap[*dmapt, int]()

	// Empty map
	if s := m.String(); s != "dmap{ version:0 id:{}}" {
		t.Errorf("empty map String(): expected 'dmap{ version:0 id:{}}', got '%s'", s)
	}

	// Map with some keys
	m.set(&dmapt{name: "a"}, 1)
	m.set(&dmapt{name: "b"}, 2)
	s := m.String()
	if s != "dmap{ version:2 id:{a, b}}" {
		t.Errorf("non-empty map String(): expected 'dmap{ version:2 id:{a, b}}', got '%s'", s)
	}
}

// TestDmapRandomOperations performs random operations and verifies
// dmap matches built-in map behavior
func TestDmapRandomOperations(t *testing.T) {
	const (
		numKeys = 7
		numOps  = 1000
		seed    = 42 // fixed seed for reproducibility
	)

	// Create test keys
	keys := make([]*dmapt, numKeys)
	for i := range keys {
		keys[i] = &dmapt{name: fmt.Sprintf("key%d", i)}
	}

	// Initialize maps
	dmap := newDmap[*dmapt, int]()
	builtin := make(map[string]int)

	// Create random number generator with fixed seed
	rng := rand.New(rand.NewSource(seed))

	// Track operations for debugging
	type op struct {
		typ string // "set", "get", "del"
		key string
		val int
	}
	ops := make([]op, 0, numOps)

	// Perform random operations
	for i := 0; i < numOps; i++ {
		// Choose random key
		keyIdx := rng.Intn(numKeys)
		k := keys[keyIdx]
		keyStr := k.id()

		// Choose random operation
		opType := rng.Intn(3) // 0=set, 1=get, 2=del
		var val int
		var opStr string

		switch opType {
		case 0: // set
			val = rng.Intn(1000)
			dmap.set(k, val)
			builtin[keyStr] = val
			opStr = "set"
		case 1: // get
			dval, dfound := dmap.get2(k)
			bval, bfound := builtin[keyStr]
			if dfound != bfound {
				t.Errorf("get mismatch at op %d: dmap=%v, builtin=%v",
					i, dfound, bfound)
			}
			if dfound && dval != bval {
				t.Errorf("get value mismatch at op %d: dmap=%v, builtin=%v",
					i, dval, bval)
			}
			opStr = "get"
		case 2: // del
			dfound, _ := dmap.delkey(k)
			bval, bfound := builtin[keyStr]
			delete(builtin, keyStr)
			if dfound != bfound {
				t.Errorf("del mismatch at op %d: dmap=%v, builtin=%v",
					i, dfound, bfound)
			}
			val = bval
			opStr = "del"
		}

		ops = append(ops, op{opStr, keyStr, val})

		// Periodically verify lengths match
		if i%100 == 0 {
			if dmap.Len() != len(builtin) {
				t.Errorf("length mismatch at op %d: dmap=%d, builtin=%d",
					i, dmap.Len(), len(builtin))
				// Print last few operations for debugging
				start := max(0, len(ops)-5)
				t.Errorf("last operations: %v", ops[start:])
			}
		}
	}

	// Final verification
	if dmap.Len() != len(builtin) {
		t.Errorf("final length mismatch: dmap=%d, builtin=%d",
			dmap.Len(), len(builtin))
	}

	// Verify all keys in dmap are in builtin and vice versa
	for k, v := range all(dmap) {
		if bval, found := builtin[k.id()]; !found || bval != v {
			t.Errorf("final key mismatch: key=%s, dmap=%v, builtin=%v (found=%v)",
				k.id(), v, bval, found)
		}
	}
	for k, v := range builtin {
		if dval, found := dmap.get2(&dmapt{name: k}); !found || dval != v {
			t.Errorf("final key mismatch: key=%s, dmap=%v (found=%v), builtin=%v",
				k, dval, found, v)
		}
	}
}

// BenchmarkDmapVsBuiltin benchmarks dmap against built-in map
func BenchmarkDmapVsBuiltin(b *testing.B) {
	const numKeys = 1000

	// Create test keys
	keys := make([]*dmapt, numKeys)
	for i := range keys {
		keys[i] = &dmapt{name: fmt.Sprintf("key%d", i)}
	}

	// Benchmark Set operations
	b.Run("Set", func(b *testing.B) {
		b.Run("Dmap", func(b *testing.B) {
			m := newDmap[*dmapt, int]()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				k := keys[i%numKeys]
				m.set(k, i)
			}
		})
		b.Run("Builtin", func(b *testing.B) {
			m := make(map[string]int)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				k := keys[i%numKeys].id()
				m[k] = i
			}
		})
	})

	// Benchmark Get operations
	b.Run("Get", func(b *testing.B) {
		// Setup maps with data
		dmap := newDmap[*dmapt, int]()
		builtin := make(map[string]int)
		for i := range keys {
			dmap.set(keys[i], i)
			builtin[keys[i].id()] = i
		}

		b.Run("Dmap", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				k := keys[i%numKeys]
				_ = dmap.get(k)
			}
		})
		b.Run("Builtin", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				k := keys[i%numKeys].id()
				_ = builtin[k]
			}
		})
	})

	// Benchmark Delete operations
	b.Run("Delete", func(b *testing.B) {
		b.Run("Dmap", func(b *testing.B) {
			m := newDmap[*dmapt, int]()
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
			m := make(map[string]int)
			// Pre-fill map
			for i := range keys {
				m[keys[i].id()] = i
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				k := keys[i%numKeys].id()
				delete(m, k)
				// Re-insert to maintain size
				m[k] = i
			}
		})
	})

	// Benchmark Range iteration
	b.Run("Range", func(b *testing.B) {
		// Setup maps with data
		dmap := newDmap[*dmapt, int]()
		builtin := make(map[string]int)
		for i := range keys {
			dmap.set(keys[i], i)
			builtin[keys[i].id()] = i
		}

		b.Run("Dmap", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				sum := 0
				for _, v := range all(dmap) {
					sum += v
				}
				_ = sum
			}
		})
		b.Run("Dmap ordercache", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				sum := 0
				for _, kv := range dmap.cached() {
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

Compilation started at Sun May 25 03:54:29

GOTRACEBACK=all GOEXPERIMENT=synctest go test -v -race -bench=BenchmarkDmapVsBuiltin -run=blah
faketime = true
goos: darwin
goarch: amd64
pkg: github.com/glycerine/rpc25519
cpu: Intel(R) Core(TM) i7-1068NG7 CPU @ 2.30GHz
BenchmarkDmapVsBuiltin
BenchmarkDmapVsBuiltin/Set
BenchmarkDmapVsBuiltin/Set/Dmap
BenchmarkDmapVsBuiltin/Set/Dmap-8    	 3876800	       301.5 ns/op
BenchmarkDmapVsBuiltin/Set/Builtin
BenchmarkDmapVsBuiltin/Set/Builtin-8 	34976158	        33.93 ns/op
BenchmarkDmapVsBuiltin/Get
BenchmarkDmapVsBuiltin/Get/Dmap
BenchmarkDmapVsBuiltin/Get/Dmap-8    	 7941308	       138.4 ns/op
BenchmarkDmapVsBuiltin/Get/Builtin
BenchmarkDmapVsBuiltin/Get/Builtin-8 	33435048	        35.41 ns/op
BenchmarkDmapVsBuiltin/Delete
BenchmarkDmapVsBuiltin/Delete/Dmap
BenchmarkDmapVsBuiltin/Delete/Dmap-8 	  392959	      2971 ns/op
BenchmarkDmapVsBuiltin/Delete/Builtin
BenchmarkDmapVsBuiltin/Delete/Builtin-8         	17438089	        65.88 ns/op
BenchmarkDmapVsBuiltin/Range
BenchmarkDmapVsBuiltin/Range/Dmap
BenchmarkDmapVsBuiltin/Range/Dmap-8             	   12086	     96360 ns/op
BenchmarkDmapVsBuiltin/Range/Dmap_ordercache
BenchmarkDmapVsBuiltin/Range/Dmap_ordercache-8  	  149186	      7503 ns/op
BenchmarkDmapVsBuiltin/Range/Builtin
BenchmarkDmapVsBuiltin/Range/Builtin-8          	   26757	     42736 ns/op
PASS
ok  	github.com/glycerine/rpc25519	14.392s

Compilation finished at Sun May 25 03:54:47
*/
