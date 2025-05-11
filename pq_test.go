package rpc25519

import (
	"fmt"
	"testing"
	"time"
)

func Test555_preArrQ_deletes_actually_delete(t *testing.T) {

	// pq needs a scenario tie breaker...
	ms := time.Millisecond
	var seed [32]byte
	s := &simnet{
		scenario: newScenario(ms, ms, ms, seed),
	}
	rng := newPRNG(seed)

	pq := s.newPQarrivalTm("test 555 preArrQ deletes")
	m := make(map[time.Time]*mop)

	N := 10
	now := time.Now()
	for i := range N {
		r := rng.pseudoRandNonNegInt64() % 5
		arr := now.Add(time.Duration(r))
		op := &mop{
			arrivalTm: arr,
		}
		pq.add(op)
		m[arr] = op

		// assert same:
		n1 := pq.tree.Len()
		n2 := len(m)
		if n1 != n2 {
			panic(fmt.Sprintf("len pq=%v, len map = %v", n1, n2))
		}
		j := 0
		for it := pq.tree.Min(); it != pq.tree.Limit(); it = it.Next() {
			op := it.Item().(*mop)
			v := m[op.arrivalTm]
			if v != op {
				panic(fmt.Sprintf("i=%v; j=%v;  difference: op.arrivalTm = %v; pq mop =%v,  map mop = %v", i, j, op.arrivalTm, op, v))
			}
			j++
		}
	}
}
