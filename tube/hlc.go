package tube

import (
	"fmt"
	"sync/atomic"
	"time"
)

// HLC is a hybrid logical/physical clock, based
// on the 2014 paper
//
// "Logical Physical Clocks and Consistent
// Snapshots in Globally Distributed Databases"
// by Sandeep Kulkarni, Murat Demirbas, Deepak
// Madeppa, Bharadwaj Avva, and Marcelo Leone.
//
// Its physical clock resolution (the upper
// 48 bits) is in ~ 0.1 msec or about 100 microseconds.
// The lower 16 bits of this int64
// keep a logical clock counter. The paper's
// experiments observed counter values up to 10,
// nowhere near the 2^16-1 == 65535 maximum.
//
// Indeed one would have to execute one clock
// query every nanosecond for 65536 times in
// a row -- in a single contiguous execution --
// to overflow the counter. This seems unlikely
// with current technology where a PhysicalTime48()
// call takes ~66 nanoseconds and there is rarely
// a requirement for 65K timestamps in a row.
//
// See also: the use of hybrid logical clocks in
// CockroachDB, and Demirbas and Kulkarni's
// paper on using them to solve the consistent
// snapshot problem in Spanner. The naming has evolved;
// in that paper the approach was called "Augmented Time".
// "Beyond TrueTime: Using AugmentedTime for Improving Spanner"
// https://cse.buffalo.edu/~demirbas/publications/augmentedTime.pdf
// https://github.com/AugmentedTimeProject/AugmentedTimeProject
//
// Currently there is no mutual exclusion / synchronization
// provided, and the user must arrange for that separately if
// required.
// .
// defined in tube.go now for greenpack serz purposes,
// rather than in hlc.go
//type HLC int64

const getCount HLC = HLC(1<<16) - 1 // low 16 bits are 1
const getLC HLC = ^getCount         // low 16 bits are 0

func (hlc *HLC) LC() int64 {
	r := HLC(atomic.LoadInt64((*int64)(hlc)))
	return int64(r & getLC)
}

func (hlc *HLC) Count() int64 {
	r := HLC(atomic.LoadInt64((*int64)(hlc)))
	return int64(r & getCount)
}

// Aload does an atomic load of hlc and returns it.
// Both external and internal users of TubeNode call
// NewTicket, which creates a new HLC, and so we
// get data races under test without synchronization.
// We use atomic loads to Load/Store in methods.
// The arguments to methods should use Aload() to
// read their HLC atomically before calling the method,
// in the possibility of data races exists.
func (hlc *HLC) Aload() (r HLC) {
	r = HLC(atomic.LoadInt64((*int64)(hlc)))
	return
}

func (hlc *HLC) String() string {
	r := HLC(atomic.LoadInt64((*int64)(hlc)))

	lc := int64(r & getLC)
	count := int64(r & getCount)
	return fmt.Sprintf("HLC{Count: %v, LC:%v (%v)}",
		count, lc, time.Unix(0, lc).Format(rfc3339MsecTz0))
}

// AssembleHLC does the simple addition,
// but takes care of the type converstion too.
// For safety, it masks off the low 16 bits
// of lc that should always be 0 anyway before
// doing the addition.
func AssembleHLC(lc int64, count int64) HLC {
	return HLC(lc)&getLC + HLC(count)
}

// Here we use a bit-manipulation trick.
// By adding mask (all 1s in lower 16 bits),
// we increment the 16th bit if any lower
// bits were set. We then mask away the
// lower 16 bits.
//func roundUpTo16Bits(pt int64) int64 {
//	return (pt + getCount) & getLC
//}

// PhysicalTime48 rounds up to the 16th
// bit the UnixNano() of the current time,
// as requested by the Hybrid-Logical-Clock
// algorithm. The low order 16 bits are
// used for a logical counter rather than
// nanoseconds. The low 16 bits are always zero
// on return from this function.
func PhysicalTime48() HLC {
	pt := time.Now().UnixNano()

	// hybrid-logical-clocks (HLC) wants to
	// round up at the 48th bit.
	return (HLC(pt) + getCount) & getLC
}

// CreateSendOrLocalEvent
// updates the local hybrid clock j
// based on PhysicalTime48.
// POST: r == *hlc
func (hlc *HLC) CreateSendOrLocalEvent() (r HLC) {

	j := HLC(atomic.LoadInt64((*int64)(hlc)))
	// equivalent to: j := *hlc

	ptj := PhysicalTime48()
	jLC := j & getLC
	jCount := j & getCount

	jLC1 := jLC
	if ptj > jLC {
		jLC = ptj
	}
	if jLC == jLC1 {
		jCount++
	} else {
		jCount = 0
	}
	r = (jLC + jCount)

	// equivalent to: *hlc = r
	atomic.StoreInt64((*int64)(hlc), int64(r))

	return
}

// CreateAndNow is the same as CreateSendOrLocalEvent
// but also returns the raw time.Time before
// hlc conversion of the low 16 bits.
func (hlc *HLC) CreateAndNow() (r HLC, now time.Time) {

	j := HLC(atomic.LoadInt64((*int64)(hlc)))

	//inlined ptj := PhysicalTime48()
	now = time.Now()
	pt := now.UnixNano()
	ptj := (HLC(pt) + getCount) & getLC

	jLC := j & getLC
	jCount := j & getCount

	jLC1 := jLC
	if ptj > jLC {
		jLC = ptj
	}
	if jLC == jLC1 {
		jCount++
	} else {
		jCount = 0
	}
	r = (jLC + jCount)
	atomic.StoreInt64((*int64)(hlc), int64(r))
	return
}

// ReceiveMessageWithHLC
// updates the local hybrid clock hlc based on the
// received message m's hybrid clock.
// PRE: m should be owned exclusively or the result of an
// atomic load with Aload() to avoid data races.
// POST: r == *hlc
func (hlc *HLC) ReceiveMessageWithHLC(m HLC) (r HLC) {

	j := HLC(atomic.LoadInt64((*int64)(hlc)))

	jLC := j & getLC
	jCount := j & getCount
	jlcOrig := jLC

	mLC := m & getLC
	mCount := m & getCount

	ptj := PhysicalTime48()
	if ptj > jLC {
		jLC = ptj
	}
	if mLC > jLC {
		jLC = mLC
	}
	if jLC == jlcOrig && jlcOrig == mLC {
		jCount = max(jCount, mCount) + 1
	} else if jLC == jlcOrig {
		jCount++
	} else if jLC == mLC {
		jCount = mCount + 1
	} else {
		jCount = 0
	}
	r = (jLC + jCount)
	atomic.StoreInt64((*int64)(hlc), int64(r))
	return
}

// ToTime returns the Count as the nanoseconds.
func (hlc HLC) ToTime() time.Time {
	return time.Unix(0, int64(hlc))
}

// ToTime48 returns only the LC in the upper 48 bits
// of hlc; the lower 16 bits of r.UnixNano() will be all 0.
// See ToTime to include the Count as well.
func (hlc *HLC) ToTime48() (r time.Time) {

	j := HLC(atomic.LoadInt64((*int64)(hlc)))

	lc := int64(j & getLC)
	r = time.Unix(0, int64(lc))
	return
}
