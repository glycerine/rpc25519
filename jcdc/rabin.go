package jcdc

import (
	"fmt"
)

type RabinKarpCDC struct {
	Opts *CDC_Config `zid:"0"`

	// Rabin-Karp specific fields matching Python implementation
	mult  uint32 // The Rabin-Karp multiplier (0x08104225)
	invm  uint32 // Modular multiplicative inverse of mult mod 2^32
	sum   uint32 // Current rolling hash value
	multn uint32 // mult^count mod 2^32
	//Window []byte // Sliding window buffer
	mask uint32 // Mask for finding chunk boundaries

	WindowSize int
}

// modinv calculates the modular multiplicative inverse of a mod m
// using the extended Euclidean algorithm
func modinv(a, m uint32) uint32 {
	t, newt := uint32(0), uint32(1)
	r, newr := m, a

	for newr != 0 {
		quotient := r / newr
		t, newt = newt, t-quotient*newt
		r, newr = newr, r-quotient*newr
	}

	if r > 1 {
		panic("a is not invertible")
	}
	if t < 0 {
		t = t + m
	}
	return t
}

// NewRabinKarpCDC creates a new Rabin-Karp chunker with default or provided options
func NewRabinKarpCDC(opts *CDC_Config) *RabinKarpCDC {
	if opts == nil {
		opts = Default_RabinKarpCDC_Options()
	}
	c := &RabinKarpCDC{}
	c.initKarpRabinWithOpts(opts)
	return c
}
func (r *RabinKarpCDC) initKarpRabinWithOpts(opts *CDC_Config) {
	// Window size calculation:
	// We want the window size to scale with target size
	// A good rule of thumb is log2(targetSize)
	// This gives us larger windows for larger chunks, which helps with
	// content-based chunking at larger scales

	targetSize := opts.TargetSize
	windowSize := 1
	//vv("pre:  targetSize = %v; windowSize = %v", targetSize, windowSize) // 40000, 1
	for targetSize > 1 {
		windowSize++
		targetSize >>= 1
	}

	//vv("post: targetSize = %v; windowSize = %v", targetSize, windowSize) // 1, 21

	// But still maintain some reasonable bounds
	if windowSize < 4 {
		windowSize = 4 // minimum reasonable window
	}
	if windowSize > 128 {
		windowSize = 128 // maximum reasonable window
	}
	//windowSize = 8

	//vv("alter windowSize clamping to [4,128]: targetSize = %v; windowSize = %v", targetSize, windowSize) // windowSize = 21
	// Use the same multiplier as the Python implementation
	mult := uint32(0x08104225)

	r.mult = mult
	r.invm = modinv(mult, 0xFFFFFFFF)
	r.multn = 1
	//Window= make([]byte, windowSize),
	r.WindowSize = windowSize

	//vv("we set WindowSize=%v", windowSize) // 21 here!
	r.Opts = opts

	// Calculate mask for chunk boundaries
	// We want ~1/targetSize probability of a match
	// So we use mask = (1 << bits) - 1 where bits = log2(targetSize)
	bits := uint(1)
	size := opts.TargetSize
	for size > 1 {
		bits++
		size >>= 1
	}
	r.mask = (uint32(1) << bits) - 1

}

func (c *RabinKarpCDC) SetConfig(cfg *CDC_Config) {
	vv("top of SetConfig, c.WindowSize = %v; c  = %p", c.WindowSize, c)
	defer func() {
		vv("end of SetConfig, c.WindowSize = %v; c = %p", c.WindowSize, c)
	}()

	c.Opts = cfg

	vv("SetConfig about to call initKarpRabinWithOpts()")
	c.initKarpRabinWithOpts(cfg)
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
	c.sum = 0
	c.multn = 1
	windowSize := c.WindowSize

	//vv("run Algorithm() windowSize = %v", windowSize)

	// Initialize the hash with the first window
	for i := 0; i < windowSize && i < n; i++ {
		c.sum = (c.sum*c.mult + uint32(data[i])) & 0xFFFFFFFF
		c.multn = (c.multn * c.mult) & 0xFFFFFFFF
	}

	// Roll the hash over the data
	for i := windowSize; i < n; i++ {
		// Remove oldest byte using the Python implementation's method
		oldest := data[i-windowSize]
		c.sum = (c.sum - c.multn*uint32(oldest)) & 0xFFFFFFFF

		// Add newest byte
		c.sum = (c.sum*c.mult + uint32(data[i])) & 0xFFFFFFFF
		c.multn = (c.multn * c.mult) & 0xFFFFFFFF

		// Check if we've reached minSize and have a hash match
		if i >= minSize && (c.sum&c.mask) == 0 {
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
