package jcdc

//  AI port of chunker.py, dubious :)
import (
	"math"
)

/*
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

*/

// Chunker represents a content-defined chunking processor that produces
// chunks with exponentially distributed sizes between min and max length
type Chunker struct {
	targetLen int64
	minLen    int64
	maxLen    int64
	avgLen    float64
	prob      uint32
	blockLen  int64
	blocks    map[blockHash]int
}

type blockHash struct {
	hash uint32
	len  int64
}

// New creates a new Chunker with specified target, minimum and maximum lengths
func NewChunker(targetLen, minLen, maxLen int64) *Chunker {
	if minLen >= maxLen {
		panic("minLen must be less than maxLen")
	}

	c := &Chunker{
		targetLen: targetLen,
		minLen:    minLen,
		maxLen:    maxLen,
		blocks:    make(map[blockHash]int),
	}

	c.avgLen = c.getAvgLen(targetLen, minLen, maxLen)
	c.prob = uint32(math.Floor(float64(1<<32) / float64(targetLen)))
	c.blockLen = 0

	return c
}

// NewChunkerFromAvg creates a new Chunker using average
// length instead of target length
func NewChunkerFromAvg(avgLen, minLen, maxLen int64) *Chunker {
	targetLen := int64(math.Floor(getTargetLen(avgLen, minLen, maxLen) + 0.5))
	return NewChunker(targetLen, minLen, maxLen)
}

// IsBlock checks if the given rolling hash value represents a chunk boundary
func (c *Chunker) IsBlock(r uint32) bool {
	c.blockLen++
	return c.blockLen >= c.minLen && (r < c.prob || c.blockLen >= c.maxLen)
}

// AddBlock records a new block with the given hash
func (c *Chunker) AddBlock(h uint32) {
	b := blockHash{
		hash: h,
		len:  c.blockLen,
	}
	c.blocks[b]++
	c.blockLen = 0

}

// Helper functions for calculating lengths
func (c *Chunker) getAvgLen(targetLen, minLen, maxLen int64) float64 {
	if targetLen <= 0 {
		return float64(minLen)
	}
	z := float64(maxLen-minLen) / float64(targetLen)
	return float64(minLen) + float64(targetLen)*(1.0-math.Exp(-z))
}

func getTargetLen(avgLen, minLen, maxLen int64) float64 {
	// Binary search to find target length
	x0, x1 := 0.0, float64(1<<32)
	for x1-x0 > 0.5 {
		x := (x0 + x1) / 2.0
		avg := float64(minLen) + x*(1.0-math.Exp(-(float64(maxLen-minLen)/x)))
		if avg < float64(avgLen) {
			x0 = x
		} else {
			x1 = x
		}
	}
	return x0
}
