package jcdc

import (
	//"bytes"
	//"crypto/sha256"
	"fmt"
	"math"
	//"io"
	//"math/rand"

	chunker "github.com/glycerine/restic-chunker-mod"
)

func evaluateDistribution() {
	b := chunker.NewBase(chunker.Pol(0x3DA3358B4DC173))
	_ = b
}

type ResticRabinCDC struct {
	Opts *CDC_Config `zid:"0"`

	chnkr *chunker.BaseChunker
}

func Default_ResticRabinCDC_Options() *CDC_Config {
	return &CDC_Config{
		MinSize:    2 * 1024,
		TargetSize: 10 * 1024,
		MaxSize:    64 * 1024,
	}
}

func NewResticRabinCDC(opts *CDC_Config) *ResticRabinCDC {
	if opts == nil {
		opts = Default_ResticRabinCDC_Options()
	}
	c := &ResticRabinCDC{}
	c.SetConfig(opts) // compute bits from Target
	return c
}

func (c *ResticRabinCDC) NextCut(data []byte) (cutpoint int) {
	return c.Algorithm(c.Opts, data, len(data))
}

// Algorithm implements the Rabin-Karp rolling hash chunking algorithm
func (c *ResticRabinCDC) Algorithm(options *CDC_Config, data []byte, n int) (cutpoint int) {
	if n > len(data) {
		panic(fmt.Sprintf("len(data) == %v and n == %v: n must be <= len(data)", len(data), n))
	}

	minSize := options.MinSize
	maxSize := options.MaxSize

	// Handle small inputs and bounds
	switch {
	case n <= minSize:
		return n
	case n >= maxSize:
		n = maxSize
	}

	// -1, 0 if not found
	// idx + i + 1, digest if found
	cutpoint, _ = c.chnkr.NextSplitPoint(data)
	if cutpoint < 0 {
		cutpoint = len(data)
	}

	return
}

// Cutpoints finds all cut points in the data
func (c *ResticRabinCDC) Cutpoints(data []byte, maxPoints int) (cuts []int) {
	var cutpoint int
	remaining := data

	for len(remaining) > 0 {
		cut := c.Algorithm(c.Opts, remaining, len(remaining))
		cutpoint += cut
		cuts = append(cuts, cutpoint)

		if maxPoints > 0 && len(cuts) >= maxPoints {
			return
		}

		remaining = remaining[cut:]
	}

	return cuts
}

func (c *ResticRabinCDC) Config() *CDC_Config {
	return c.Opts
}

func (c *ResticRabinCDC) Name() string {
	return "restic-rabin-chunker"
}

func (c *ResticRabinCDC) SetConfig(cfg *CDC_Config) {
	//	vv("top of SetConfig, c.WindowSize = %v; c  = %p", c.WindowSize, c)
	//	defer func() {
	//		vv("end of SetConfig, c.WindowSize = %v; c = %p", c.WindowSize, c)
	//	}()

	c.Opts = cfg

	//vv("SetConfig about to call initKarpRabinWithOpts()")
	//c.initKarpRabinWithOpts(cfg)

	bits := int(math.Log2(float64(cfg.TargetSize)))
	vv("for cfg.TargetSize = %v => using bits = %v", cfg.TargetSize, bits)
	c.chnkr = chunker.NewBase(chunker.Pol(0x3DA3358B4DC173), chunker.WithBaseAverageBits(bits))

}
