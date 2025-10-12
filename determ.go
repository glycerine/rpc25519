package rpc25519

import (
// "sync/atomic"
)

// in its own file so simnet can
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
	endi  int64
}

// progressPoint communicates to the
// other simnet what iloop and prand
// we have seen during MEQ batching.
type progressPoint struct {
	i        int64
	prandMEQ uint64
}

func newDetermCheckMeetpoint(every, endi int64) *determMeetpoint {
	return &determMeetpoint{
		toA:   make(chan progressPoint),
		every: every,
		endi:  endi,
	}
}

func (s *Simnet) meetpointCheck(meet *determMeetpoint, prandMEQ uint64, i int64) (shutdown bool) {
	name := s.simnetName

	if i%meet.every != 0 {
		return
	}

	if i >= meet.endi {
		vv("i = %v >= meet.endi:%v for simnet '%v' so closing up simnet", i, meet.endi, name)
		shutdown = true
		return
	}

	point := progressPoint{
		i:        i,
		prandMEQ: prandMEQ,
	}

	vv("simnetName:%v  i = %v and meet.every: %v, so conducting "+
		"determMeetpoint check", name, i, meet.every)

	// suffices to have A check and B just send where it is at.
	switch name {
	case "A":
		select {
		case pointB := <-meet.toA:
			if pointB.i != point.i {
				panicf("internal logic error: pointB.i(%v) != i(%v)", pointB.i, i)
			}
			if pointB.prandMEQ != point.prandMEQ {
				panicf("divergence detected! at i=%v, A.prandMEQ = %v, but B.prandMEQ = %v", i, point.prandMEQ, pointB.prandMEQ)
			}

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
