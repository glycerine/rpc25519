package tube

import (
	cryrand "crypto/rand"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"testing"

	fet "github.com/glycerine/fisherexact"
)

var _ = bits.Len64
var _ = math.MaxInt64

func Test999_pseudoRandNonNegInt64Range(t *testing.T) {

	// 10 seconds to run (nChoices=2); too slow for regular testing.
	// Plus is flaky of course, b/c pvalue threshold
	// is expected to be hit under the null hypothesis
	// if we iterate enough.
	return

	for nChoices := 2; nChoices < 20; nChoices++ {
		var seed [32]byte
		cryrand.Read(seed[:])

		rng := newPRNG(seed)
		_ = rng
		nChoices := int64(8)
		nmap := make(map[int64]int)
		K := 100000
		N := K * int(nChoices)
		for range N {
			x := rng.pseudoRandNonNegInt64Range(nChoices)
			//x := cryptoRandNonNegInt64Range(nChoices)
			nmap[x]++
		}
		//vv("n = %#v", n)
		yates := true
		var n11, n21 int

		// null hypothesis: equal
		n12 := K
		n22 := K
		for k, v := range nmap {
			n11 = v

			n21 = K*2 - n11
			pvalue := fet.ChiSquaredTest22(n11, n12, n21, n22, yates)
			if pvalue < 0.01 {
				panic(fmt.Sprintf("pvalue = %v for not-random: n11=%v, n21=%v, n12=%v; n22=%v, K=%v; n=%#v; nChoices=%v; k = %v", pvalue, n11, n21, n12, n22, K, nmap, nChoices, k))
			}
		}
	}
}

func Test998_cryptoRandInt64RangePosOrNeg(t *testing.T) {
	return
	/*
		nChoices := int64(2)
		//	nChoices = math.MaxInt64 - 1
		//	nChoices = math.MaxInt64
		mask := (int64(1) << bits.Len64(uint64(nChoices-1))) - 1
		redrawAbove := math.MaxInt64 - (((math.MaxInt64 % nChoices) + 1) % nChoices)
		fmt.Printf("mask = %v; math.MaxInt64 = %v ; nChoices = %v; redrawAbove = %v ; un-usable=%v ; (redrawAbove+1) %% nChoices = %v; redrawAbove %% nChoices = %v\n", mask, math.MaxInt64, nChoices, redrawAbove, math.MaxInt64-redrawAbove, (redrawAbove+1)%nChoices, (redrawAbove)%nChoices)
		return
	*/
	//return
	//fmt.Printf("math.MinInt64 = %X \n", math.MinInt64)
	//x := math.MinInt64
	//mini := math.MinInt64
	//fmt.Printf("math.MinInt64 = %X; -math.MinInt64= %v ; math.MaxInt64 = %v \n", uint64(mini), -x, math.MaxInt64)
	//return

	largestPositiveChoice := int64(1)
	//for largestPositiveChoice := 2; largestPositiveChoice < 20; largestPositiveChoice++ {
	var seed [32]byte
	cryrand.Read(seed[:])

	rng := newPRNG(seed)
	_ = rng
	//largestPositiveChoice := int64(8)
	nmap := make(map[int64]int)
	K := 100000
	N := K * ((int(largestPositiveChoice) << 1) + 1)
	for range N {
		x := rng.pseudoRandInt64RangePosOrNeg(largestPositiveChoice)
		//x := cryptoRandInt64RangePosOrNeg(largestPositiveChoice)
		nmap[x]++
	}
	vv("nmap = %#v", nmap)
	yates := true
	var n11, n21 int

	// null hypothesis: equal
	n12 := K
	n22 := K
	for k, v := range nmap {
		n11 = v

		n21 = K*2 - n11
		pvalue := fet.ChiSquaredTest22(n11, n12, n21, n22, yates)
		if pvalue < 0.01 {
			panic(fmt.Sprintf("pvalue = %v for not-random: n11=%v, n21=%v, n12=%v; n22=%v, K=%v; n=%#v; largestPositiveChoice=%v; k = %v", pvalue, n11, n21, n12, n22, K, nmap, largestPositiveChoice, k))
		}
	}
	// }
}

func Test997_pseudoRandInt64RangePosOrNeg(t *testing.T) {
	return

	largestPositiveChoice := int64(1)
	//for largestPositiveChoice := 2; largestPositiveChoice < 20; largestPositiveChoice++ {
	var seed [32]byte
	cryrand.Read(seed[:])

	rng := newPRNG(seed)
	_ = rng
	//largestPositiveChoice := int64(8)
	nmap := make(map[int64]int)
	K := 100000
	N := K * ((int(largestPositiveChoice) << 1) + 1)
	for range N {
		x := rng.pseudoRandInt64RangePosOrNeg(largestPositiveChoice)
		//x := cryptoRandInt64RangePosOrNeg(largestPositiveChoice)
		nmap[x]++
	}
	vv("nmap = %#v", nmap)
	yates := true
	var n11, n21 int

	// null hypothesis: equal
	n12 := K
	n22 := K
	for k, v := range nmap {
		n11 = v

		n21 = K*2 - n11
		pvalue := fet.ChiSquaredTest22(n11, n12, n21, n22, yates)
		if pvalue < 0.01 {
			panic(fmt.Sprintf("pvalue = %v for not-random: n11=%v, n21=%v, n12=%v; n22=%v, K=%v; n=%#v; largestPositiveChoice=%v; k = %v", pvalue, n11, n21, n12, n22, K, nmap, largestPositiveChoice, k))
		}
	}
	// }
}

func Test996_pseudoRanBool(t *testing.T) {
	return

	var seed [32]byte
	cryrand.Read(seed[:])

	rng := newPRNG(seed)
	_ = rng
	nmap := make(map[int64]int)
	K := 100000
	N := K * 2
	for range N {
		b := rng.pseudoRandBool()
		x := int64(0)
		if b {
			x = 1
		}
		nmap[x]++
	}
	//vv("nmap = %#v", nmap)
	yates := true
	var n11, n21 int

	// null hypothesis: equal
	n12 := K
	n22 := K
	for k, v := range nmap {
		n11 = v

		n21 = K*2 - n11
		pvalue := fet.ChiSquaredTest22(n11, n12, n21, n22, yates)
		if pvalue < 0.01 {
			panic(fmt.Sprintf("pvalue = %v for not-random: n11=%v, n21=%v, n12=%v; n22=%v, K=%v; n=%#v; k = %v", pvalue, n11, n21, n12, n22, K, nmap, k))
		}
	}
	// }
}

func Test995_percentage_wasted_cryptoRandNonNegInt64Range(t *testing.T) {
	return

	// We instrumented our rejection sampling approach
	// to see how much re-sampling was needed. It is
	// pretty good really.

	// Generally for small nChoices, we do no
	// extra sampling; because with high probability
	// the number is not in the little extra bit
	// of math.MaxInt64 - nChoices or so that we
	// must reject and re-sample on.
	//
	// For large nChoices > math.MaxInt64, we can sample about 2x
	// on average, as expected.

	// compared to this approach, we only really do extra
	// work when our nChoices is very large. https://research.kudelskisecurity.com/2020/07/28/the-definitive-guide-to-modulo-bias-and-how-to-avoid-it/

	var seed [32]byte
	cryrand.Read(seed[:])
	rng := newPRNG(seed)
	_ = rng

	sampled := 0
	//x := cryptoRandNonNegInt64Range(nChoices)
	instrumentedNonNegInt64Range := func(nChoices int64) (r int64) {

		redrawAbove := math.MaxInt64 - (((math.MaxInt64 % nChoices) + 1) % nChoices)
		// INVAR: redrawAbove % nChoices == (nChoices - 1).

		b := make([]byte, 8)

		for {
			if false {
				// crypto random
				_, err := cryrand.Read(b)
				if err != nil {
					panic(err)
				}
			} else {
				// pseudo random
				rng.cha8.Read(b)
			}
			sampled++
			r = int64(binary.LittleEndian.Uint64(b))
			if r < 0 {
				if r == math.MinInt64 {
					return 0
				}
				r = -r
			}
			if r > redrawAbove {
				continue
			}
			return r % nChoices
		}
		panic("never reached")
	}

	//nChoices := 1 + (math.MaxInt64 / 2) // extra percent = 0
	//nChoices := 2 + (math.MaxInt64 / 2) // extra percent = 0.999
	nChoices := 10 + (math.MaxInt64 / 2) // extra percent = 1.0000682
	//nChoices := (math.MaxInt64 / 2) // extra percent = 0
	//nChoices := (math.MaxInt64 / 4) * 3 // extra percent = 0.3333103
	//nChoices := (math.MaxInt64 / 6) * 5 // extra percent = 0.20
	//nChoices := (math.MaxInt64 / 6) * 4 // extra percent = 0.49
	//redrawAbove := math.MaxInt64 - (((math.MaxInt64 % nChoices) + 1) % nChoices)
	//vv("nChoices %v -> redrawAbove = %v", nChoices, redrawAbove)

	n := 10_000_000
	//nChoices = 107 // 0% extra, nice.
	for range n {
		instrumentedNonNegInt64Range(int64(nChoices))
	}
	pct := float64(sampled-n) / float64(n)
	vv("sampled=%v; n=%v; percent extra=%v", sampled, n, pct)
}
