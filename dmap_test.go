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
