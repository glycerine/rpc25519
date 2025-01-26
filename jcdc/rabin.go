package jcdc

import (
	"fmt"
)

type RabinKarpCDC struct {
	Opts *CDC_Config `zid:"0"`

	// Rabin-Karp specific fields
	polynomial uint64
	Window     []byte
	hash       uint64
	mask       uint64
}

// NewRabinKarpCDC creates a new Rabin-Karp chunker with default or provided options
func NewRabinKarpCDC(opts *CDC_Config) *RabinKarpCDC {
	if opts == nil {
		opts = Default_RabinKarpCDC_Options()
	}

	// Window size calculation:
	// We want the window to be large enough to provide good content-based chunking,
	// but small enough to be efficient. A common approach is to use:
	// - Either ~1/4 of the minimum chunk size
	// - Or ~1/8 of the target size
	// - But capped between 32-64 bytes to keep computation reasonable
	windowSize := opts.MinSize / 4
	if windowSize > opts.TargetSize/8 {
		windowSize = opts.TargetSize / 8
	}
	if windowSize < 32 {
		windowSize = 32
	}
	if windowSize > 64 {
		windowSize = 64
	}

	r := &RabinKarpCDC{
		polynomial: 0x3DA3358B4DC173, // Random prime number
		Window:     make([]byte, windowSize),
	}
	r.Opts = opts

	// Calculate mask based on target size
	// We want ~1/targetSize probability of a match
	// So we use mask = (1 << bits) - 1 where bits = log2(targetSize)
	bits := uint(1)
	size := opts.TargetSize
	for size > 1 {
		bits++
		size >>= 1
	}
	r.mask = (uint64(1) << bits) - 1

	return r
}

func (c *RabinKarpCDC) SetConfig(cfg *CDC_Config) {
	c.Opts = cfg
}

func (c *RabinKarpCDC) Name() string {
	return "rabin-karp-chunker"
}

func Default_RabinKarpCDC_Options() *CDC_Config {
	return &CDC_Config{
		MinSize:    2 * 1024,
		TargetSize: 10 * 1024,
		MaxSize:    64 * 1024,
	}
}

func (c *RabinKarpCDC) Config() *CDC_Config {
	return c.Opts
}

func (c *RabinKarpCDC) Validate(options *CDC_Config) error {
	if options.TargetSize == 0 || options.TargetSize < 64 ||
		options.TargetSize > 1024*1024*1024 {
		return ErrTargetSize
	}
	if options.MinSize < 64 || options.MinSize > 1024*1024*1024 ||
		options.MinSize >= options.TargetSize {
		return ErrMinSize
	}
	if options.MaxSize < 64 || options.MaxSize > 1024*1024*1024 ||
		options.MaxSize <= options.TargetSize {
		return ErrMaxSize
	}
	return nil
}

func (c *RabinKarpCDC) NextCut(data []byte) (cutpoint int) {
	return c.Algorithm(c.Opts, data, len(data))
}

// Algorithm implements the Rabin-Karp rolling hash chunking algorithm
func (c *RabinKarpCDC) Algorithm(options *CDC_Config, data []byte, n int) (cutpoint int) {
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

	// Initialize rolling hash state
	c.hash = 0
	windowSize := len(c.Window)

	// Initialize the hash with the first window
	for i := 0; i < windowSize && i < n; i++ {
		c.hash = (c.hash * c.polynomial) + uint64(data[i])
	}

	// Roll the hash over the data
	for i := windowSize; i < n; i++ {
		// Remove oldest byte
		oldest := data[i-windowSize]
		c.hash = c.hash - (uint64(oldest) * powMod(c.polynomial, uint64(windowSize-1)))

		// Add newest byte
		c.hash = (c.hash * c.polynomial) + uint64(data[i])

		// Check if we've reached minSize and have a hash match
		if i >= minSize && (c.hash&c.mask) == 0 {
			return i + 1
		}
	}

	return n
}

// Cutpoints finds all cut points in the data
func (c *RabinKarpCDC) Cutpoints(data []byte, maxPoints int) (cuts []int) {
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

// Helper function to calculate (base^exp) efficiently
func powMod(base, exp uint64) uint64 {
	result := uint64(1)
	for exp > 0 {
		if exp&1 == 1 {
			result = result * base
		}
		base = base * base
		exp >>= 1
	}
	return result
}
