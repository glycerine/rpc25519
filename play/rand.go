package main

import (
	cryrand "crypto/rand"
	"encoding/binary"
	"math"
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
		r = -r
	}
	return r
}

// returns r in negative and positive range of int64
func cryptoRandInt64() (r int64) {
	b := make([]byte, 8)
	_, err := cryrand.Read(b)
	if err != nil {
		panic(err)
	}
	r = int64(binary.LittleEndian.Uint64(b))
	return r
}
