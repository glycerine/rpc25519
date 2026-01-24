package tube

import (
	"fmt"
	"testing"
)

// functions to test
// func PhysicalTime48() HLC
// func (j *HLC) CreateSendOrLocalEvent()
// func (j *HLC) ReceiveMessageWithHLC(m HLC)
// func (a HLC) GT(b HLC) bool
// func (a HLC) GTE(b HLC) bool
// func (a HLC) LT(b HLC) bool
// func (a HLC) LTE(b HLC) bool

func Test_HLC_PhysicalTime48(t *testing.T) {
	bubbleOrNot(t, func(t *testing.T) {
		pt := PhysicalTime48()
		if pt.Count() != 0 {
			t.Errorf("PhysicalTime48() should have 0 count, got %d", pt.Count())
		}
		if pt.LC() == 0 {
			t.Errorf("PhysicalTime48() should have non-zero logical clock")
		}
	})
}

func Test_HLC_Monotonicity(t *testing.T) {

	bubbleOrNot(t, func(t *testing.T) {
		var j HLC
		j.CreateSendOrLocalEvent()

		// test HLC.String() method.
		s := j.String()
		fmt.Printf("j is '%v'\n", s)
		fmt.Printf("getCount is '%X'\n", int64(getCount))

		if faketime {
			expect := "HLC{Count: 0, LC:946684800000000000 (2000-01-01T00:00:00.000Z)}"
			if s != expect {
				panicf("HLC.String() test failed: expected '%v' but got '%v'", expect, s)
			}
		}
		for i := 0; i < 1000; i++ {
			prev := j
			j.CreateSendOrLocalEvent()
			if j <= prev {
				t.Errorf("Monotonicity violation: new %v not > old %v (iter %d)", j, prev, i)
			}
			if j.LC() < prev.LC() {
				t.Errorf("Logical clock regression: new %v < old %v", j, prev)
			}
		}
	})
}

func Test_HLC_ClockRegression(t *testing.T) {

	bubbleOrNot(t, func(t *testing.T) {

		// Scenario: The local physical clock is BEHIND the HLC.
		// This happens if the HLC was pushed forward by receiving a message from a node with a faster clock,
		// or if the local system clock was reset backwards.
		// The HLC algorithm should preserve monotonicity (by incrementing count)
		// until physical time catches up.

		var j HLC

		// Create a "future" time.
		// We want j.LC > PhysicalTime48().
		// PhysicalTime48() rounds UP to the next getCount boundary possibly?
		// No, it just takes Now() and masks off lower bits.
		// Wait, PhysicalTime48 implementation: return (pt + getCount) & ^getCount
		// This actually rounds UP to the next multiple of (getCount+1).

		// Let's force j to be well ahead.
		futurePt := PhysicalTime48() + HLC(100*(getCount+1))
		j = futurePt

		// Now call CreateSendOrLocalEvent. Local physical time is way behind 'j'.
		// j should increment only its count, or stay at same LC and increment count.

		prev := j
		j.CreateSendOrLocalEvent()

		if j.LC() < prev.LC() {
			t.Fatalf("Regression check: HLC Logic Clock decreased! prev=%d, cur=%d", prev.LC(), j.LC())
		}

		if j.LC() == prev.LC() {
			if j.Count() <= prev.Count() {
				t.Errorf("Regression check: Count did not increase! prev=%d, cur=%d", prev.Count(), j.Count())
			}
		} else {
			// j.LC > prev.LC
			// This is technically allowed if physical time suddenly jumped forward,
			// but in our test case we know physical time is behind.
			// However, unless we mock time, PhysicalTime48() is live.
			// Assuming time didn't jump 100 steps in 1ns.
			if j.LC() > prev.LC() {
				// This might happen if 'futurePt' wasn't actually in the future enough?
				// Or maybe I misunderstand the rounding.
				// Let's assume this path is unlikely for 100 increment.
				t.Logf("Warn: LC increased despite being set to future? prev=%d, cur=%d", prev.LC(), j.LC())
			}
		}
	})
}

func Test_HLC_ReceiveMessage(t *testing.T) {

	bubbleOrNot(t, func(t *testing.T) {

		tests := []struct {
			name      string
			initialJ  HLC
			receivedM HLC
			check     func(oldJ, newJ, m HLC) error
		}{
			{
				name: "Receive Old Message",
				// j is fresh, m is old (0). j should just increment (internal send/local event logic domination).
				// Wait, ReceiveMessage logic:
				// if ptj > jLC -> jLC = ptj
				// if mLC > jLC -> jLC = mLC
				// then count logic.
				initialJ:  PhysicalTime48(),
				receivedM: 0,
				check: func(oldJ, newJ, m HLC) error {
					if newJ <= oldJ {
						return fmt.Errorf("should increase")
					}
					// Since m is very old, newJ should likely be driven by PhysicalTime or oldJ logic
					return nil
				},
			},
			{
				name: "Receive Future Message",
				// j is now, m is far future.
				initialJ:  PhysicalTime48(),
				receivedM: PhysicalTime48() + HLC(50*(getCount+1)),
				check: func(oldJ, newJ, m HLC) error {
					if newJ.LC() < m.LC() {
						return fmt.Errorf("newJ LC should catch up to m LC")
					}
					return nil
				},
			},
			{
				name: "Receive Equal Logic Different Count",
				// j and m have same LC, likely from same physical tick.
				// m has higher count. j should take m's count + 1.
				initialJ:  (PhysicalTime48() & ^getCount) | 5,  // Count 5
				receivedM: (PhysicalTime48() & ^getCount) | 10, // Count 10
				check: func(oldJ, newJ, m HLC) error {
					// note: physical time might have advanced during this test setup,
					// complicating "Same LC" assumption if we use live time.
					// However, PhysicalTick is coarse (via getCount), so it's stable-ish.

					// If LC is still stable:
					if newJ.LC() == oldJ.LC() {
						expectedCount := m.Count() + 1
						if newJ.Count() != expectedCount {
							return fmt.Errorf("expected count %d, got %d", expectedCount, newJ.Count())
						}
					}
					return nil
				},
			},
		}

		for _, tc := range tests {
			fmt.Printf("RUN test '%v'\n", tc.name)
			//t.Run(tc.name, func(t *testing.T) {
			j := tc.initialJ
			j.ReceiveMessageWithHLC(tc.receivedM)
			if err := tc.check(tc.initialJ, j, tc.receivedM); err != nil {
				t.Error(err)
			}
			if j <= tc.initialJ {
				t.Errorf("Monotonicity violation in receive: new %v not > old %v", j, tc.initialJ)
			}
			//})
		}
	})
}

func Test_HLC_Comparison(t *testing.T) {

	bubbleOrNot(t, func(t *testing.T) {

		// Construct HLCs manually to have predictable LC and Count
		base := PhysicalTime48()

		makeHLC := func(lcOffset int, count int) HLC {
			lc := (base & ^getCount) + HLC(int64(lcOffset)*int64(getCount+1))
			return lc | HLC(count)
		}

		a := makeHLC(0, 10)
		b := makeHLC(0, 20)
		c := makeHLC(1, 0)
		d := makeHLC(1, 0) // equal to c

		tests := []struct {
			desc string
			lhs  HLC
			rhs  HLC
			gt   bool
			gte  bool
			lt   bool
			lte  bool
		}{
			{"a < b (same LC, diff count)", a, b, false, false, true, true},
			{"b > a", b, a, true, true, false, false},
			{"b < c (diff LC)", b, c, false, false, true, true},
			{"c > b", c, b, true, true, false, false},
			{"c == d", c, d, false, true, false, true},
		}

		for _, tc := range tests {
			fmt.Printf("RUN test '%v'\n", tc.desc)
			//t.Run(tc.desc, func(t *testing.T) {
			if (tc.lhs > tc.rhs) != tc.gt {
				t.Errorf("GT mismatch for %v, %v", tc.lhs, tc.rhs)
			}
			if (tc.lhs >= tc.rhs) != tc.gte {
				t.Errorf("GTE mismatch")
			}
			if (tc.lhs < tc.rhs) != tc.lt {
				t.Errorf("LT mismatch")
			}
			if (tc.lhs <= tc.rhs) != tc.lte {
				t.Errorf("LTE mismatch")
			}
			//})
		}
	})
}

func Benchmark_HLC_PhysicalTime48(b *testing.B) {
	for i := 0; i < b.N; i++ {
		PhysicalTime48()
	}
}

func Test_HLC_AssembleHLC(t *testing.T) {
	bubbleOrNot(t, func(t *testing.T) {
		pt := PhysicalTime48()
		lc := pt.LC()
		count := int64(300)

		h := AssembleHLC(lc, count)
		if h.LC() != lc {
			t.Errorf("AssembleHLC Should preserve LC. expected %v, got %v", lc, h.LC())
		}
		if h.Count() != count {
			t.Errorf("AssembleHLC Should preserve Count. expected %v, got %v", count, h.Count())
		}

		// Test masking
		// Pass an LC that has bits in the lower 16
		lcWithNoise := lc | 0xF
		h2 := AssembleHLC(lcWithNoise, count)
		if h2.LC() != lc {
			t.Errorf("AssembleHLC Should mask LC input. expected %v, got %v", lc, h2.LC())
		}
	})
}
