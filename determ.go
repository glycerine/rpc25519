package rpc25519

import (
// "sync/atomic"
)

// support for test 710 in simgrid_test.go.
// Now in its own file so simnet can
// compile in regular non-test mode.

// determMeetpoint allows us to do online comparison
// of the execution sequence of two simnets for
// the 710 test below.
//
// notice that these chan will be allocated
// outside of either simnet bubble, and
// so will be non-blocking to the bubble.
// This is fine since they are only
// consulted by the main scheduler loops
// when they are all alone (the only goro
// running in each respective simnet).
type determMeetpoint struct {

	// We have "A" and "B" simnets.
	//
	// Let "A" take the lead and do the check,
	// and have "B" tell "A" what its prand/i is.
	//
	// "A" will then confirm agreement or divergence.
	// No need to have both of them check. B
	// might race ahead but meh, A will panic/halt
	// immediately on divergence.

	toA   chan progressPoint
	every int64
}

// progressPoint communicates to the
// other simnet what iloop and prand
// we have seen during MEQ batching.
type progressPoint struct {
	i                      int64
	prandMEQ               uint64
	meqBatch               int64
	npop                   int
	topOfBatchNextDispatch int64 // s.nextDispatch before dispatching any of this batch
}

func newDetermCheckMeetpoint(every int64) *determMeetpoint {
	return &determMeetpoint{
		toA:   make(chan progressPoint),
		every: every,
	}
}

func (s *Simnet) meetpointCheck(meet *determMeetpoint, prandMEQ uint64, i int64, meqBatch int64, npop int, topOfBatchNextDispatch int64) (shutdown bool) {
	name := s.simnetName

	if i%meet.every != 0 {
		return
	}

	point := progressPoint{
		i:                      i,
		prandMEQ:               prandMEQ,
		meqBatch:               meqBatch,
		npop:                   npop,
		topOfBatchNextDispatch: topOfBatchNextDispatch,
	}

	//vv("simnetName:%v  i = %v and meet.every: %v, so conducting "+
	//	"determMeetpoint check", name, i, meet.every)

	// suffices to have A check and B just send where it is at.
	switch name {
	case "A":
		select {
		case pointB := <-meet.toA:
			if pointB.i != i {
				panicf("internal logic error: B.i(%v) != A.i(%v)", pointB.i, i)
			}
			if pointB.meqBatch != meqBatch {
				panicf("internal logic error: B.meqBatch(%v) != A.meqBatch(%v)", pointB.meqBatch, meqBatch)
			}
			if pointB.npop != npop {
				panicf("internal logic error: B.npop(%v) != A.npop(%v)", pointB.npop, npop)
			}
			if pointB.prandMEQ != prandMEQ {
				panicf("divergence detected! at i=%v, A.prandMEQ = %v, but B.prandMEQ = %v", i, point.prandMEQ, pointB.prandMEQ)
			}
			if pointB.topOfBatchNextDispatch != topOfBatchNextDispatch {
				panicf("divergence detected! at i=%v, A.topOfBatchNextDispatch = %v, but B.topOfBatchNextDispatch = %v", i, topOfBatchNextDispatch, pointB.topOfBatchNextDispatch)

			}
			//vv("good: agreement at i = %v: prandMEQ = %v; meqBatch = %v ; npop = %v; topOfBatchNextDispatch = %v", i, prandMEQ, meqBatch, npop, topOfBatchNextDispatch)

		case <-s.halt.ReqStop.Chan:
			shutdown = true
			return
		}
	case "B":
		select {
		case meet.toA <- point:
		case <-s.halt.ReqStop.Chan:
			shutdown = true
			return
		}
	}
	return
}
