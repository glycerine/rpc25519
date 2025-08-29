package rpc25519

import (
	cryrand "crypto/rand"
	"encoding/binary"
	"fmt"
	"math"

	cristalbase64 "github.com/cristalhq/base64"
)

// returns r > 0
func cryptoRandPositiveInt64() (r int64) {
	for {
		r = cryptoRandNonNegInt64()
		if r != math.MaxInt64 {
			break
		}
		// avoid overflow, draw again.
	}
	return r + 1
}

// returns r >= 0
func cryptoRandNonNegInt64() (r int64) {
	b := make([]byte, 8)
	_, err := cryrand.Read(b)
	if err != nil {
		panic(err)
	}
	r = int64(binary.LittleEndian.Uint64(b))
	if r < 0 {
		if r == math.MinInt64 {
			return 0
		}
		r = -r
	}
	return r
}

// returns r in the full negative and positive range of int64
func cryptoRandInt64() (r int64) {
	b := make([]byte, 8)
	_, err := cryrand.Read(b)
	if err != nil {
		panic(err)
	}
	r = int64(binary.LittleEndian.Uint64(b))
	return r
}

func cryptoRandBool() (b bool) {
	by := make([]byte, 1)
	_, err := cryrand.Read(by)
	if err != nil {
		panic(err)
	}
	b = (by[0]%2 == 0)
	return
}

// return r in [0, nChoices] and avoid the inherent
// bias in modulo that starves the numbers in
// the region between the divisor and originally
// generated maximum number.
//
// nChoices must be > 1 or what
// is the point? (this would always return the value 0, just
// a single choice!) We panic if that is requested.
//
// If nChoices is MaxInt64 then
// we just return cryptoRandInt64(). No
// sampling + rejecting required.
//
// Otherwise we use a rejection sampling
// approach to get an un-biased random number.
func cryptoRandNonNegInt64Range(nChoices int64) (r int64) {
	if nChoices <= 1 {
		panic(fmt.Sprintf("nChoices must be in [2, MaxInt64]; we see %v", nChoices))
	}
	if nChoices == math.MaxInt64 {
		return cryptoRandNonNegInt64()
	}

	// compute the last valid acceptable value,
	// possibly leaving a small window at the top of the
	// int64 range that will require drawing again.
	// we will accept all values <= redrawAbove and
	// modulo them by nChoices.
	redrawAbove := math.MaxInt64 - (((math.MaxInt64 % nChoices) + 1) % nChoices)
	// INVAR: redrawAbove % nChoices == (nChoices - 1).

	b := make([]byte, 8)

	for {
		_, err := cryrand.Read(b)
		if err != nil {
			panic(err)
		}
		r = int64(binary.LittleEndian.Uint64(b))
		if r < 0 {
			// there is 1 more negative integer than
			// positive integers in 2's complement
			// representation on integers, so the probability
			// is exactly 1/2 of entering here.
			//
			// Does this not bias
			// against 0 though? Yep.
			//
			// Without this next check,
			// 0 has probability 1/2^64. Whereas
			// every other positive integer has
			// probability 2/2^64... So
			// without this next line we are
			// (very subtlely) biased against zero.
			// To correct that, we
			// give 0 one more chance by
			// letting it have the last negative
			// number too, which we never
			// want to return anyway.
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
	return r
}

// cryptoRandInt64RangePosOrNeg
// returns r in [-largestPositiveChoice, largestPositiveChoice]
// and avoids the inherent bias in modulo (when
// largestPositiveChoice is not a perfect power of 2).
//
// largestPositiveChoice must be > 0.
// Uses rejection sampling approach.
//
// This will never return math.MinInt64, even
// if largestPositiveChoice is math.MaxInt64;
// since math.MinInt64 is greater in
// absolute value than math.MaxInt64.
// Returning 0 is always a posibility, and
// there are always an odd number of
// possible returned r values.
func cryptoRandInt64RangePosOrNeg(largestPositiveChoice int64) (r int64) {
	if largestPositiveChoice < 1 {
		panic(fmt.Sprintf("error in cryptoRandInt64RangePosOrNeg(): largestPositiveChoice must be in [1, MaxInt64]; we see %v", largestPositiveChoice))
	}
	if largestPositiveChoice == math.MaxInt64 {
		r = cryptoRandInt64()
		for r == math.MinInt64 {
			// too big in absolute value, try again.
			r = cryptoRandInt64()
		}
		return
	}
	// INVAR: largestPositiveChoice < math.MaxInt64

	// handle largestPositiveChoice < math.MaxInt64/2

	// Suppose largestPositiveChoice = 1,
	// then we want to choose from [-1, 0, 1], and
	// our nChoices = 3, or 1 + (1 << 1) == 1 + 2 == 3.
	//   and return -1 + r
	// Suppose largestPositiveChoice = 2,
	// then we want to choose from [-2,-1,0,1,2], and
	// our nChoices = 5, or 1 + (2 << 1) == 1 + 4 == 5.
	//   and return -2 + r
	if largestPositiveChoice < (math.MaxInt64 >> 1) {
		r = cryptoRandNonNegInt64Range(1 + (largestPositiveChoice << 1))
		return -largestPositiveChoice + r
	}
	// INVAR: largestPositiveChoice in [math.MaxInt64/2, math.MaxInt64]

	b := make([]byte, 8)
	for {
		_, err := cryrand.Read(b)
		if err != nil {
			panic(err)
		}
		r = int64(binary.LittleEndian.Uint64(b))
		if r < -largestPositiveChoice {
			// reject: too large in magnitude, try again.
			continue
		}
		if r > largestPositiveChoice {
			// reject: too large in magnitude, try again.
			continue
		}
		return r // only ever exit loop here.
	}

	panic("never reached")
	return r
}

func cryRand33B() string {
	var by [33]byte
	_, err := cryrand.Read(by[:])
	panicOn(err)
	return cristalbase64.URLEncoding.EncodeToString(by[:])
}

func cryRand17B() string {
	var by [17]byte
	_, err := cryrand.Read(by[:])
	panicOn(err)
	return cristalbase64.URLEncoding.EncodeToString(by[:])
}
