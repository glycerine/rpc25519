package tube

import (
	"fmt"
	"time"
)

// HLC is a hybrid logical/physical clock, based
// on the 2014 paper
// "Logical Physical Clocks and Consistent
// Snapshots in Globally Distributed Databases"
// by Sandeep Kulkarni, Murat Demirbas, Deepak
// Madeppa, Bharadwaj Avva, and Marcelo Leone.
type HLC int64

const mask16 HLC = HLC(1<<16) - 1 // low 16 bits are 1
const unmask16 HLC = ^mask16      // low 16 bits are 0

func (hlc HLC) LC() int64 {
	return int64(hlc & unmask16)

}
func (hlc HLC) Count() int64 {
	return int64(hlc & mask16)
}

func (hlc HLC) String() string {
	lc := hlc.LC()
	count := hlc.Count()
	return fmt.Sprintf("HLC{Count: %v, LC:%v (%v)}", count, lc, time.Unix(0, lc).Format(rfc3339MsecTz0))
}

// AseembleHLC does the simple addition,
// but takes care of the type converstion too.
// For safety, it masks off the low 16 bits
// of lc that should always be 0 anyway before
// doing the addition.
func AssembleHLC(lc int64, count int64) HLC {
	return HLC(lc)&unmask16 + HLC(count)
}

// Here we use a bit-manipulation trick.
// By adding mask (all 1s in lower 16 bits),
// we increment the 16th bit if any lower
// bits were set. We then mask away the
// lower 16 bits.
//func roundUpTo16Bits(pt int64) int64 {
//	return (pt + mask16) & unmask16
//}

// PhysicalTime48 rounds up to the 16th
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
	return (pt + mask16) & unmask16
}

// CreateSendOrLocalEvent
// updates the local hybrid clock j
// based on PhysicalTime48.
func (j *HLC) CreateSendOrLocalEvent() {

	ptj := PhysicalTime48()
	jLC := *j & unmask16
	jCount := *j & mask16

	jLC1 := jLC
	if ptj > jLC {
		jLC = ptj
	}
	if jLC == jLC1 {
		jCount++
	} else {
		jCount = 0
	}
	*j = (jLC + jCount)
}

// ReceiveMessageWithHLC
// updates the local hybrid clock j based on the
// received message m's hybrid clock.
func (j *HLC) ReceiveMessageWithHLC(m HLC) {

	jLC := *j & unmask16
	jCount := *j & mask16
	jlcOrig := jLC

	mLC := m & unmask16
	mCount := m & mask16

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

func (a HLC) GT(b HLC) bool {
	aLC := a & unmask16
	bLC := b & unmask16
	if aLC > bLC {
		return true
	}
	if aLC < bLC {
		return false
	}
	// INVAR: aLC == bLC
	aCount := a & mask16
	bCount := b & mask16
	return aCount > bCount
}

func (a HLC) GTE(b HLC) bool {
	aLC := a & unmask16
	bLC := b & unmask16
	if aLC > bLC {
		return true
	}
	if aLC < bLC {
		return false
	}
	// INVAR: aLC == bLC
	aCount := a & mask16
	bCount := b & mask16
	return aCount >= bCount
}

func (a HLC) LT(b HLC) bool {
	aLC := a & unmask16
	bLC := b & unmask16
	if aLC < bLC {
		return true
	}
	if aLC > bLC {
		return false
	}
	// INVAR: aLC == bLC
	aCount := a & mask16
	bCount := b & mask16
	return aCount < bCount
}

func (a HLC) LTE(b HLC) bool {
	aLC := a & unmask16
	bLC := b & unmask16
	if aLC < bLC {
		return true
	}
	if aLC > bLC {
		return false
	}
	// INVAR: aLC == bLC
	aCount := a & mask16
	bCount := b & mask16
	return aCount <= bCount
}
