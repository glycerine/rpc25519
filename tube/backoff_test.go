package tube

import (
	"fmt"
	"testing"
	//"time"
)

var _ = fmt.Printf

func TestExpBackoff(t *testing.T) {
	// for range 100 {
	// 	f1 := float64(cryptoRandInt64RangePosOrNeg(1e6-1)) / 2e6 // in (-0.5, 0.5)
	// 	fmt.Printf("%v\n", f1)
	// }
	e := newExpBackoff(defaultExpBackoffConfig)
	_ = e
	for i := range 30 {
		_ = i
		//fmt.Printf("i=%v: next -> %v\n", i, e.next())
	}
}
