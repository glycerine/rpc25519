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

	// compute our bits threshold, (rounding up is why the + 0.5)
	f.NumBitsZeroAtCut = uint32(0.5 + math.Log2(float64(opts.TargetSize-opts.MinSize)))

	//fmt.Printf("NewFNVCDC: TargetSize = %v -> f.NumBitsZeroAtCut = %v\n", f.Opts.TargetSize, f.NumBitsZeroAtCut)

	return f
}

func (f *FNVCDC) SetConfig(cfg *CDC_Config) {
	f.Opts = cfg
	f.NumBitsZeroAtCut = uint32(0.5 + math.Log2(float64(cfg.TargetSize-cfg.MinSize)))
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
