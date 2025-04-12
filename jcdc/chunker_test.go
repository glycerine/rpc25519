package jcdc

import (
	"testing"
)

/*
Looking at the code, I can help clarify the
relationship between these different approaches:

The rolling.go and chunker.go implementation you have is
actually a different chunking algorithm altogether -
it implements a simple exponential Content-Defined-Chunking
algorithm that uses a rolling hash to find cut points.
This is more similar to the original Rabin-Karp style chunking.

The UltraCDC algorithm (in ultracdc.go) uses a completely different approach - it doesn't use a rolling hash at all. Instead, it:
 - Uses Hamming distance calculations against a fixed pattern (0xAA)
 - Looks at 8-byte windows at a time
 - Has special handling for low-entropy data
 - Uses two different masks (maskS and maskL) for different chunk size ranges

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
*/

func TestChunker(t *testing.T) {
	return

	// Create a chunker with target length 1024, min length 64, max length 8192
	chunker := NewChunker(1024, 64, 8192)

	// Create a rolling hash with some seed
	roller := NewRollingHash(1)

	// Process your data
	for {
		hash := roller.Next()
		if chunker.IsBlock(hash) {
			// This is a chunk boundary
			chunker.AddBlock(hash)
		}
	}
}
