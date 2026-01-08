package tube

import (
	"time"
)

// HLC is a hybrid logical/physical clock, based
// on the 2014 paper
// "Logical Physical Clocks and Consistent
// Snapshots in Globally Distributed Databases"
// by Sandeep Kulkarni, Murat Demirbas, Deepak
// Madeppa, Bharadwaj Avva, and Marcelo Leone.
type HLC int64

const mask48 = HLC(1<<48) - 1

func (hlc HLC) LC() int64 {
	return int64(hlc & ^mask48)

}
func (hlc HLC) Count() int64 {
	return int64(hlc & mask48)
}

// Here we use a bit-manipulation trick.
// By adding mask (all 1s in lower 48 bits),
// we increment the 48th bit if any lower
// bits were set. We then mask away the
// lower 48 bits.
//func roundUpTo48Bits(pt int64) int64 {
//	return (pt + mask48) & ^mask48
//}

// PhysicalTime48 rounds up to the 48th
// bit the UnixNano() of the current time,
// as requested by the Hybrid-Logical-Clock
// algorithm. The low order 16 bits are
// used for a logical counter rather than
// nanoseconds. The low 16 bits are always zero
// on return from this function.
func PhysicalTime48() HLC {
	pt := HLC(time.Now().UnixNano())

	// hybrid-logical-clocks (HLC) wants to
	// round up at the 48th bit.
	return (pt + mask48) & ^mask48
}

// CreateSendOrLocalEvent
// updates the local hybrid clock j
// based on PhysicalTime48.
func (j *HLC) CreateSendOrLocalEvent() {

	ptj := PhysicalTime48()
	jLC := *j & ^mask48
	jCount := *j & mask48

	if jLC == ptj {
		jCount++
	} else {
		jCount = 0
		if ptj > jLC {
			jLC = ptj
		}
	}
	*j = (jLC + jCount)
}

// ReceiveMessageWithHLC
// updates the local hybrid clock j based on the
// received message m's hybrid clock.
func (j *HLC) ReceiveMessageWithHLC(m HLC) {

	jLC := *j & ^mask48
	jCount := *j & mask48
	jlcOrig := jLC

	mLC := m & ^mask48
	mCount := m & mask48

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
	*j = (jLC + jCount)
}
