package rpc25519

import (
	"encoding/binary"
	"fmt"
	"math"
	mathrand2 "math/rand/v2"
	"sync"
)

// pseudo random number generator
type prng struct {
	mut  sync.Mutex
	seed [32]byte
	cha8 *mathrand2.ChaCha8
}

func newPRNG(seed [32]byte) *prng {
	return &prng{
		seed: seed,
		cha8: mathrand2.NewChaCha8(seed),
	}
}

// returns r >= 0
func (rng *prng) pseudoRandNonNegInt64() (r int64) {
	rng.mut.Lock()
	defer rng.mut.Unlock()

	b := make([]byte, 8)
	rng.cha8.Read(b)
	r = int64(binary.LittleEndian.Uint64(b))
	if r < 0 {
		if r == math.MinInt64 {
			return 0
		}
		r = -r
	}
	return r
}

// returns r > 0
func (rng *prng) pseudoRandPositiveInt64() (r int64) {
	rng.mut.Lock()
	defer rng.mut.Unlock()

	for {
		r = rng.pseudoRandNonNegInt64()
		if r != math.MaxInt64 {
			break
		}
		// avoid overflow, draw again.
	}
	return r + 1
}

// returns r in the full negative and positive range of int64
func (rng *prng) pseudoRandInt64() (r int64) {
	rng.mut.Lock()
	defer rng.mut.Unlock()

	b := make([]byte, 8)
	rng.cha8.Read(b)
	r = int64(binary.LittleEndian.Uint64(b))
	return r
}

func (rng *prng) pseudoRandBool() (b bool) {
	rng.mut.Lock()
	by := make([]byte, 1)
	rng.cha8.Read(by)
	b = (by[0]%2 == 0)
	rng.mut.Unlock()
	return
}

// return r in [0, nChoices] and avoid the inherent
// bias in modulo. nChoices must be > 1 or what
// is the point? --we will panic (the answer would always be 0).
//
// If nChoices is MaxInt64 then
// we just return pseudoRandNonNegInt64(). No
// sampling + rejecting required.
//
// We use a bitmask + rejection approach; rejecting
// if our draw happens to fall between nChoices and
// (2^k)-1 where 2^k is the next highest power
// of 2 the occurs > nChoices. This gives
// an un-biased random number.
func (rng *prng) pseudoRandNonNegInt64Range(nChoices int64) (r int64) {
	if nChoices <= 1 {
		panic(fmt.Sprintf("nChoices must be in [2, MaxInt64]; we see %v", nChoices))
	}

	if nChoices == math.MaxInt64 {
		return rng.pseudoRandNonNegInt64()
	}

	rng.mut.Lock()
	defer rng.mut.Unlock()

	// compute the last valid acceptable value,
	// possibly leaving a small window at the top of the
	// int64 range that will require drawing again.
	// we will accept all values <= redrawAbove and
	// modulo them by nChoices.
	redrawAbove := math.MaxInt64 - (((math.MaxInt64 % nChoices) + 1) % nChoices)
	// INVAR: redrawAbove % nChoices == (nChoices - 1).

	b := make([]byte, 8)

	for {
		rng.cha8.Read(b)
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
			// (very subtly) biased against zero.
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
	return r
}

// pseudoRandInt64RangePosOrNeg
// returns r in [-largestPositiveChoice, largestPositiveChoice]
// and avoids the inherent bias in modulo (when
// largestPositiveChoice is not a perfect power of 2).
//
// largestPositiveChoice must be > 0.
// Uses bitmask + rejection approach.
//
// This will never return math.MinInt64, even
// if largestPositiveChoice is math.MaxInt64;
// since math.MinInt64 is greater in
// absolute value than math.MaxInt64.
// Returning 0 is always a posibility, and
// there are always an odd number of
// possible returned r values.
func (rng *prng) pseudoRandInt64RangePosOrNeg(largestPositiveChoice int64) (r int64) {
	if largestPositiveChoice < 1 {
		panic(fmt.Sprintf("error in prng.pseudoRandInt64RangePosOrNeg(): largestPositiveChoice must be in [1, MaxInt64]; we see %v", largestPositiveChoice))
	}

	if largestPositiveChoice == math.MaxInt64 {
		r = rng.pseudoRandInt64()
		for r == math.MinInt64 {
			// too big in absolute value, try again.
			// should happen only once in 2^64 so
			// the odds this loop goes more
			// than once are 1/2^128, very small.
			r = rng.pseudoRandInt64()
		}
		return
	}
	// INVAR: largestPositiveChoice < math.MaxInt64

	// Suppose largestPositiveChoice = 1,
	// then we want to choose from [-1, 0, 1], and
	// our nChoices = 3, or 1 + (1 << 1) == 1 + 2 == 3.
	//   and return -1 + r
	// Suppose largestPositiveChoice = 2,
	// then we want to choose from [-2,-1,0,1,2], and
	// our nChoices = 5, or 1 + (2 << 1) == 1 + 4 == 5.
	//   and return -2 + r
	if largestPositiveChoice < (math.MaxInt64 >> 1) {
		r = rng.pseudoRandNonNegInt64Range(1 + (largestPositiveChoice << 1))
		return -largestPositiveChoice + r
	}
	// INVAR: largestPositiveChoice in [math.MaxInt64/2, math.MaxInt64]

	rng.mut.Lock()
	defer rng.mut.Unlock()

	b := make([]byte, 8)
	for {
		rng.cha8.Read(b)
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
