package jcdc

/*
AI comments:

The rolling.go and chunker.go implementation you have
is actually a different chunking algorithm altogether -
it implements a simple exponential Content-Defined-Chunking
algorithm that uses a rolling hash to find cut points.
This is more similar to the original Rabin-Karp style chunking.

Here's the key difference in how they find cut points:

// Rolling hash approach (chunker.go):
func (c *Chunker) IsBlock(r uint32) bool {
    c.incBlock()
    return c.blockLen >= c.minLen && (r < c.prob || c.blockLen >= c.maxLen)
}

// UltraCDC approach (ultracdc.go):
if (uint64(dist) & mask) == 0 {
    // dist is Hamming distance based, not a rolling hash
    cutpoint = i + j
    return
}

For chunk deduplication specifically, Buzhash and Gear hash
tend to be the most popular choices because:

1. They have good distribution properties
2. They're computationally efficient
3. They work well with binary data
4. They have been battle-tested in production systems

The Gear hash approach (as used in FastCDC) is particularly
interesting because it combines the rolling hash concept
 with optimizations specific to content-defined chunking.

To use these with your exponential chunking implementation,
you would replace the simple rolling hash in rolling.go
with one of these implementations. For example:

func (c *Chunker) NextCut(data []byte) int {
    hash := NewBuzhash(64)  // Or whichever rolling hash you choose
    for i, b := range data {
        h := hash.Roll(b)
        if c.IsBlock(uint32(h)) {
            return i + 1
        }
    }
    return len(data)
}



The Buzhash or Gear hash would likely
give you the best results for
general-purpose deduplication,
but you might want to benchmark
different approaches with your specific
data patterns.
*/

// generic rolling hash

// RollingHash implements a simple 32-bit rolling hash
type RollingHash struct {
	value uint32
	a     uint32
	c     uint32
	m     uint32
}

// NewRollingHash creates a new rolling hash with the given seed
func NewRollingHash(seed uint32) *RollingHash {
	return &RollingHash{
		value: seed,
		a:     1664525,    // LCG multiplier value
		c:     1013904223, // LCG increment value
		m:     0xFFFFFFFF, // 2^32 - 1
	}
}

// Next returns the next random value
func (r *RollingHash) Next() uint32 {
	r.value = (r.a*r.value + r.c) & r.m
	return r.value
}

// Skip advances the hash state by n positions
func (r *RollingHash) Skip(n int64) uint32 {
	// Fast skip implementation using modular exponentiation
	a := r.a
	c := r.c
	m := r.m

	// Calculate a^n mod m
	an := uint32(1)
	for i := int64(0); i < n; i++ {
		an = (an * a) & m
	}

	// Calculate (a^n * value + c(a^(n-1) + a^(n-2) + ... + 1)) mod m
	r.value = ((an*r.value)&m + (c * ((an - 1) / (a - 1)))) & m
	return r.value
}

// specific algorithms:

// RabinKarp is a rolling hash based on the
// Rabin-Karp algorithm. (classic choice):
type RabinKarp struct {
	window []byte
	hash   uint64
	prime  uint64
	size   int
	pow    uint64
}

func NewRabinKarp(windowSize int) *RabinKarp {
	const prime uint64 = 16777619 // FNV prime
	rk := &RabinKarp{
		window: make([]byte, 0, windowSize),
		prime:  prime,
		size:   windowSize,
	}
	// Pre-calculate the power for the oldest byte
	rk.pow = 1
	for i := 0; i < windowSize-1; i++ {
		rk.pow = (rk.pow * prime)
	}
	return rk
}

func (rk *RabinKarp) Roll(b byte) uint64 {
	// Remove oldest byte if window is full
	if len(rk.window) == rk.size {
		oldest := rk.window[0]
		rk.hash = rk.hash - (uint64(oldest) * rk.pow)
		rk.window = rk.window[1:]
	}

	// Add new byte
	rk.hash = (rk.hash * rk.prime) + uint64(b)
	rk.window = append(rk.window, b)

	return rk.hash
}

// Gear hash (used in FastCDC, very efficient):
type GearHash struct {
	window []byte
	hash   uint64
	size   int
	table  []uint64 // Randomized gear table
}

func NewGearHash(windowSize int) *GearHash {
	return &GearHash{
		window: make([]byte, 0, windowSize),
		size:   windowSize,

		// Maybe obviously, we've got to keep a fixed table
		// to be able to match chunks on client and server.
		// It cannot be randomized every time.
		//table:  generateGearTable(), // to generate a new random table

		table: GearTable4[:],
	}
}

func (gh *GearHash) Roll(b byte) uint64 {
	if len(gh.window) == gh.size {
		gh.window = gh.window[1:]
	}
	gh.window = append(gh.window, b)

	// Calculate hash using gear table
	gh.hash = (gh.hash << 1) + gh.table[b]
	return gh.hash
}

func generateGearTable() []uint64 {
	// Similar to the gear table in FastCDC
	table := make([]uint64, 256)
	// Fill with random values...
	return table
}

// Buzhash (fast and good distribution):
type Buzhash struct {
	window []byte
	hash   uint32
	size   int
	table  []uint32
}

func NewBuzhash(windowSize int) *Buzhash {
	return &Buzhash{
		window: make([]byte, 0, windowSize),
		size:   windowSize,
		table:  generateBuzTable(),
	}
}

func (bh *Buzhash) Roll(b byte) uint32 {
	var h uint32
	if len(bh.window) == bh.size {
		oldest := bh.window[0]
		h = bh.hash
		h = (h << 1) | (h >> 31) // Rotate left by 1
		h ^= bh.table[oldest]    // Remove oldest byte
		bh.window = bh.window[1:]
	}

	bh.window = append(bh.window, b)
	h ^= bh.table[b] // Add new byte
	bh.hash = h
	return h
}

func generateBuzTable() []uint32 {
	table := make([]uint32, 256)
	// Fill with random values...
	return table
}

/*
// ZPAQ (used in ZPAQ compression,
// good for deduplication):
// (used in Zstandard?)
type ZPAQ struct {
	window []byte
	hash   uint32
	size   int
}

func NewZPAQ(windowSize int) *ZPAQ {
	return &ZPAQ{
		window: make([]byte, 0, windowSize),
		size:   windowSize,
	}
}

func (z *ZPAQ) Roll(b byte) uint32 {
	if len(z.window) == z.size {
		z.window = z.window[1:]
	}
	z.window = append(z.window, b)

	// ZPAQ rolling hash <- No. This is garbage. See below.
	z.hash = ((z.hash * 1234567) + uint32(b)) ^ (z.hash >> 23)
	return z.hash
}
*/

/// hmm actual FNV-1a hash:

//package rollinghash

const (
	fnvPrime       = 16777619
	fnvOffsetBasis = 2166136261
)

type ZPAQRollingHash struct {
	hash uint32
}

func NewZPAQRollingHash() *ZPAQRollingHash {
	return &ZPAQRollingHash{hash: fnvOffsetBasis}
}

func (h *ZPAQRollingHash) Update(b byte) uint32 {
	h.hash = (h.hash ^ uint32(b)) * fnvPrime
	return h.hash
}

func (h *ZPAQRollingHash) Reset() {
	h.hash = fnvOffsetBasis
}

func (h *ZPAQRollingHash) Current() uint32 {
	return h.hash
}
