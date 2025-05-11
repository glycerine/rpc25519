package rpc25519

import (
	"encoding/binary"
	"math"
	mathrand2 "math/rand/v2"
	"sync"
)

// pseudo random number generator using chacha8.
// goroutine safe b/c protected by mutex.
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
