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
		m.upsert(d, 8-i)
	}
	//vv("m = '%#v'", m)
	i := 0
	for pd, kv := range all(m) {
		if kv.val != i {
			t.Fatalf("expected val %v, got %v for pd='%#v'", i, kv.val, pd)
		}
		//vv("at i = %v, see pd = '%#v'", i, pd)
		i++
	}
	i = 0
	for pd, kv := range all(m) {
		j := kv.val
		if j != i {
			t.Fatalf("expected val %v, got %v for pd='%#v'", i, j, pd)
		}
		//vv("at i = %v, see pd = '%#v'", i, pd)
		i++
	}
}
