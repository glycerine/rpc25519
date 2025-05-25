package rpc25519

import (
	"fmt"
	"testing"
)

// dmap tester
type dmapt struct {
	name string
}

func (s *dmapt) id() string {
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
