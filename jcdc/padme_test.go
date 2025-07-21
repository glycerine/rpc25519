package jcdc

import (
	"fmt"
	//"math"

	"testing"
)

func TestPadme(t *testing.T) {
	return

	for i := int64(1); i < 1e6; i = i * 2 {
		p := Padme(i + 3)
		fmt.Printf("i=%v -> padme(i) = %v; pct = %0.4f\n", i+3, p, float64(p)/float64(i))
	}
}

/*
i=4 -> padme(i) = 0; pct = 0.0000
i=5 -> padme(i) = 0; pct = 0.0000
i=7 -> padme(i) = 0; pct = 0.0000
i=11 -> padme(i) = 1; pct = 0.1250
i=19 -> padme(i) = 1; pct = 0.0625
i=35 -> padme(i) = 1; pct = 0.0312
i=67 -> padme(i) = 5; pct = 0.0781
i=131 -> padme(i) = 13; pct = 0.1016
i=259 -> padme(i) = 13; pct = 0.0508
i=515 -> padme(i) = 29; pct = 0.0566
i=1027 -> padme(i) = 61; pct = 0.0596
i=2051 -> padme(i) = 125; pct = 0.0610
i=4099 -> padme(i) = 253; pct = 0.0618
i=8195 -> padme(i) = 509; pct = 0.0621
i=16387 -> padme(i) = 1021; pct = 0.0623
i=32771 -> padme(i) = 2045; pct = 0.0624
i=65539 -> padme(i) = 2045; pct = 0.0312
i=131075 -> padme(i) = 4093; pct = 0.0312
i=262147 -> padme(i) = 8189; pct = 0.0312
i=524291 -> padme(i) = 16381; pct = 0.0312
--- PASS: TestPadme (0.00s)
*/
