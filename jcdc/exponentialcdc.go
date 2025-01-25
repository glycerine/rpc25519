package jcdc

import (
	"math"
)

// port from the chunker.py of
// https://github.com/dbaarda/rollsum-chunking/blob/master/chunker.py
// this starts as a downright AWFUL AI generated port. Please fix!

func solve(f func(float64) float64, x0, x1, e float64) float64 {
	// set defaults
	if x0 == 0 {
		x0 = -1.0e9
	}
	if x1 == 0 {
		x1 = 1.0e9
	}
	if e == 0 {
		e = 1.0e-9
	}

	//""" Solve f(x)=0 for x where x0<=x<=x1 within +-e. """
	y0 := f(x0)
	y1 := f(x1)
	if y0*y1 <= 0 {
		panic("y0 and y1 must have different sign.")
	}
	for (x1 - x0) > e {
		xm := (x0 + x1) / 2.0
		ym := f(xm)
		if y0*ym > 0 {
			x0, y0 = xm, ym
		} else {
			x1, y1 = xm, ym
		}
	}
	return x0
}

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
func New(targetLen, minLen, maxLen int64) *Chunker {
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
	c.initBlock()

	return c
}

// NewFromAvg creates a new Chunker using average length instead of target length
func NewFromAvg(avgLen, minLen, maxLen int64) *Chunker {
	targetLen := int64(math.Floor(getTargetLen(avgLen, minLen, maxLen) + 0.5))
	return New(targetLen, minLen, maxLen)
}

func (c *Chunker) initBlock() {
	c.blockLen = 0
}

func (c *Chunker) incBlock() {
	c.blockLen++
}

// IsBlock checks if the given rolling hash value represents a chunk boundary
func (c *Chunker) IsBlock(r uint32) bool {
	c.incBlock()
	return c.blockLen >= c.minLen && (r < c.prob || c.blockLen >= c.maxLen)
}

// AddBlock records a new block with the given hash
func (c *Chunker) AddBlock(h uint32) {
	b := blockHash{
		hash: h,
		len:  c.blockLen,
	}
	c.blocks[b]++
	c.initBlock()
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
