package rpc25519

/*
import (
	"fmt"
	"testing"
	"time"
)

// Deletes from pq work fine now.
// The issue was iteration order was non-deterministic,
// for arrival Q, which was a mistake! We took out
// the random tie breaker for preArrQ; that won't work for
// deleting things. iteration must be consistent
// so the original find and the delete find can
// do their jobs correctly and consistently.

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
	var now time.Time
	for i := range N {
		r := rng.pseudoRandNonNegInt64() % 5
		arr := now.Add(time.Duration(r))
		vv("r = %v; arr = %v", r, arr)
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
*/
