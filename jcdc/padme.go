package jcdc

import (
	"math/bits"
)

// Padme algorithm from
// Kirill Nikitin, Ludovic Barman, Wouter Lueks,
// Matthew Underwood, Jean-Pierre Hubaux und Bryan Ford
// "Reducing Metadata Leakage from Encrypted Files and Communication with PURBs"
// Proceedings on Privacy Enhancing Technologies ; 2019 (4):6–33.
// https://www.petsymposium.org/2019/files/papers/issue4/popets-2019-0056.pdf
// Algorithm 1, page 17.

// Padme returns the amount of extra padding to add,
// rather than the new length itself (as in the paper).
// Just add the returned value to the input n to
// get the new length, if you need it.
func Padme(n int64) int64 {
	if n < 0 {
		panic("negative n not supported")
	}
	// x is floor(log2(n)); the floating point exponent of n.
	x := int64(bits.Len64(uint64(n))) - 1
	// b is the number of bits to represent x.
	b := int64(bits.Len64(uint64(x)))
	// z is the number of low bits to zero out.
	z := x - b
	// m is the mask of z 1's in LSB
	m := int64(1<<z) - 1
	// round up using mask m to clear last z bits
	return ((n + m) &^ m) - n
}
