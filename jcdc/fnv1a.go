package jcdc

import (
	"errors"
	"fmt"
	"math"
)

type FNVCDC struct {
	Opts *CDC_Config

	NumBitsZeroAtCut uint32
}

func NewFNVCDC(opts *CDC_Config) *FNVCDC {
	f := &FNVCDC{}
	if opts == nil {
		opts = Default_FNVCDC_Options()
	}
	f.Opts = opts

	f.NumBitsZeroAtCut = exponentialOptimalChunkBits(opts.TargetSize)
	return f
}

func (f *FNVCDC) SetConfig(cfg *CDC_Config) {
	f.Opts = cfg
	f.NumBitsZeroAtCut = exponentialOptimalChunkBits(cfg.TargetSize)
}

func Default_FNVCDC_Options() *CDC_Config {
	return &CDC_Config{
		MinSize:    2 * 1024,
		TargetSize: 10 * 1024,
		MaxSize:    64 * 1024,
	}
}

func (f *FNVCDC) Name() string {
	return "fnv1a-rolling-cdc-golang"
}

func (f *FNVCDC) Config() *CDC_Config {
	return f.Opts
}

func (f *FNVCDC) Validate(options *CDC_Config) error {
	if options.TargetSize == 0 || options.TargetSize < 64 ||
		options.TargetSize > 1024*1024*1024 {
		return errors.New("TargetSize is required and must be 64B <= TargetSize <= 1GB")
	}
	return nil
}

func (f *FNVCDC) NextCut(data []byte) (cutpoint int) {
	return f.Algorithm(f.Opts, data, len(data))
}

func (f *FNVCDC) Cutpoints(data []byte, maxPoints int) (cuts []int) {
	cutpoint := 0
	for len(data) > 0 {
		cut := f.Algorithm(f.Opts, data, len(data))
		cutpoint += cut
		cuts = append(cuts, cutpoint)
		data = data[cut:]

		if len(data) == 0 || (maxPoints > 0 && len(cuts) >= maxPoints) {
			break
		}
	}
	return cuts
}

func (f *FNVCDC) Algorithm(options *CDC_Config, data []byte, n int) (cutpoint int) {
	const (
		fnvPrime       = 16777619
		fnvOffsetBasis = 2166136261
		//bits           = 13 // example number of zero bits for chunk boundary
	)

	if n > len(data) {
		panic(fmt.Sprintf("len(data) == %v and n == %v: n must be <= len(data)", len(data), n))
	}

	minSize := options.MinSize
	maxSize := options.MaxSize
	normalSize := options.TargetSize
	bits := f.NumBitsZeroAtCut

	switch {
	case n <= minSize:
		return n
	case n >= maxSize:
		n = maxSize
	case n <= normalSize:
		normalSize = n
	}

	hash := uint32(fnvOffsetBasis)
	for i := 0; i < n; i++ {
		hash = (hash ^ uint32(data[i])) * fnvPrime

		if i >= minSize && (hash&((1<<bits)-1)) == 0 {

			return i + 1
		}
	}

	return n
}

/*
Let's derive the chunk boundary probability
distribution rigorously. Key goals:

a) Expected chunk length = TargetSize

b) Derive minimum bits to zero to achieve this expectation

For exponential distribution P(x) = λe^(-λx), where x is chunk length:

Expected chunk length = 1/λ
1/λ = TargetSize
λ = 1/TargetSize

Probability of chunk boundary at position x:
P(boundary) = λe^(-λx)

Probability of zero bits in hash:
P(zero bits) = 2^-k, where k is bit count

Combining these requires numerical optimization to
match TargetSize precisely.
*/
func exponentialOptimalChunkBits(targetSize int) uint32 {
	// Derive optimal bit count for exponential distribution
	lambda := 1.0 / float64(targetSize)

	optimalBits := func(k int) float64 {
		// Probability of chunk boundary
		prob := math.Pow(2.0, -float64(k))

		// Expected chunk size
		expectedSize := 1.0 / (lambda * prob)

		// Squared error from target
		return math.Abs(expectedSize - float64(targetSize))
	}

	// Search for best bit count
	bestBits, minError := 0, math.Inf(1)
	for k := 1; k <= 30; k++ {
		error := optimalBits(k)
		if error < minError {
			minError = error
			bestBits = k
		}
	}

	return uint32(bestBits)
}
