package jcdc

/*
 * Copyright (c) 2024 Jason E. Aten, Ph.D.
 * Copyright (c) 2023 Gilles Chehade <gilles@poolp.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

// from
//"github.com/PlakarKorp/go-cdc-chunkers"
//"github.com/PlakarKorp/go-cdc-chunkers/chunkers/ultracdc"

import (
	"bytes"
	"errors"
	"fmt"
	"math/bits"
)

type Cutpointer interface {
	Cutpoints(data []byte, maxPoints int) (cuts []int)
	Name() string
	Config() *CDC_Config
	SetConfig(cfg *CDC_Config)
	Algorithm(options *CDC_Config, data []byte, n int) (cutpoint int)

	// instead of Cutpoints getting a batch, just
	// get the single next cutpoints back
	NextCut(data []byte) (cutpoint int)
}

//go:generate greenpack

var _ = fmt.Printf

var ErrTargetSize = errors.New("TargetSize is required and must be 64B <= TargetSize <= 1GB")
var ErrMinSize = errors.New("MinSize is required and must be 64B <= MinSize <= 1GB && MinSize < TargetSize")
var ErrMaxSize = errors.New("MaxSize is required and must be 64B <= MaxSize <= 1GB && MaxSize > TargetSize")

type CDC_Config struct {
	MinSize    int `zid:"0"`
	TargetSize int `zid:"1"`
	MaxSize    int `zid:"2"`
}

//msgp:ignore UltraCDC
type UltraCDC struct {
	Opts *CDC_Config `zid:"0"`
}

// NewUltraCDC is for non-Plakar standalone clients. Plakar
// clients will use newUltraCDC via the
// chunkers.NewChunker("ultracdc", ...) factory.
func NewUltraCDC(opts *CDC_Config) *UltraCDC {
	u := &UltraCDC{}
	if opts == nil {
		opts = Default_UltraCDC_Options()
	}
	u.Opts = opts
	return u
}

func (c *UltraCDC) SetConfig(cfg *CDC_Config) {
	c.Opts = cfg
}

func (c *UltraCDC) Name() string {
	return "ultracdc-glycerine-golang-implementation"
}

// users frequently modify what they get
// back here, so give each caller their own copy.
func Default_UltraCDC_Options() *CDC_Config {
	return &CDC_Config{
		MinSize:    2 * 1024,
		TargetSize: 10 * 1024,
		MaxSize:    64 * 1024,
	}
}

func (c *UltraCDC) Config() *CDC_Config {
	return c.Opts
}

func (c *UltraCDC) Validate(options *CDC_Config) error {

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

func (c *UltraCDC) NextCut(data []byte) (cutpoint int) {
	return c.Algorithm(c.Opts, data, len(data))
}

// Algorithm's return value, cutpoint, might typically be used next in
// segment := data[:cutpoint], so we expect to exclude the cutpoint
// index value itself. Also commonly when n == len(data) and data is
// short, then the returned cutpoint will be n;
// n is the default to return when we did not find a shorter
// cutpoint. The segment := data[:len(data)] will then take
// all of data as the segment to hash.
//
// PRE condition: n must be <= len(data). We will panic if this does not hold.
// It is always safe to pass n = len(data).
//
// POST INVARIANT: cutpoint <= n. We never return a cutpoint > n.
func (c *UltraCDC) Algorithm(options *CDC_Config, data []byte, n int) (cutpoint int) {

	// A common case will be n == len(data), but n could certainly be less.
	// Confirm that it is never more.
	if n > len(data) {
		panic(fmt.Sprintf("len(data) == %v and n == %v: n must be <= len(data)", len(data), n))
	}

	const (
		maskS uint64 = 0x2F // binary 101111

		// maskL ignores 2 more bits than maskS, so
		// it is easier to match (so we get a higher
		// probability of match after the normal point).
		maskL uint64 = 0x2C // binary 101100

		lowEntropyStringThreshold int = 64 // LEST in the paper.
	)
	minSize := options.MinSize
	maxSize := options.MaxSize
	normalSize := options.TargetSize

	var lowEntropyCount int

	// initial mask for small cuts below the Normal point.
	mask := maskS

	if minSize < 8 {
		minSize = 8
	}

	switch {
	case n <= minSize+8:
		cutpoint = n
		return
	case n >= maxSize:
		n = maxSize
	case n <= normalSize:
		normalSize = n
	}

	// without the minSize minimum of 8 and the return if
	// n <= minSize+8, we can get a minsize of 1 request resulting in:
	// panic: runtime error: slice bounds out of range [:72] with capacity 68
	// vv("len data = %v, minSize=%v; n = %v", len(data), minSize, n)
	// len data = 1048576, minSize=1; n = 8000
	outBufWin := data[minSize : minSize+8]

	// Initialize hamming distance on outBufWin
	dist := 0
	for _, v := range outBufWin {
		// effectively the Pattern of 0xAAAAAAAAAAAAAAAA,
		// as referenced in the paper,
		// is expressed here, just one byte at a time.
		dist += bits.OnesCount8(v ^ 0xAA)
	}

	var inBufWin []byte
	for i := minSize + 8; i <= n-8; i += 8 {
		if i >= normalSize {
			// Yes, we write mask every time after the Normal point,
			// and at first this appears wasteful. However,
			// an engineering judgement call was made to keep it.
			//
			// The rationale is that this small redudancy
			// is cheaper, simpler, and safer
			// than duplicating all the logic below for the two different
			// masks and then having to be sure to keep
			// the duplicates in sync. We saw multiple bugs in the past from
			// MinSize and TargetSize not being 8 byte aligned,
			// and we'd also rather not impose that on users.
			// The POST invariance analysis is also much simplified.
			// The CPU has to do less branch prediction this way,
			// and maskL will almost surely be quickly
			// accessible in a cache line.
			mask = maskL
		}

		// If i == n-8 then i+8 == n, and since n <= len(data)
		// as a PRE condition, we never go out of bounds.
		inBufWin = data[i : i+8]

		if bytes.Equal(inBufWin, outBufWin) { // hung in here?
			lowEntropyCount++
			if lowEntropyCount >= lowEntropyStringThreshold {
				// on random (high-entropy) data, we don't expect to get here.

				// If i == n-8, its largest, then this returns n,
				// which maintains our POST INVARIANT that cutpoint <= n.
				cutpoint = i + 8
				return
			}
			continue
		}

		lowEntropyCount = 0
		for j := 0; j < 8; j++ {
			if (uint64(dist) & mask) == 0 {
				// Do we preserve the POST INVARIANT here?
				// if i == n-8 (the biggest possible), and
				// j is 7 (its biggest possible), then
				// i + j could here be as big as n - 8 + 7 == n-1
				// So, yes, n-1 is the biggest we could return here.
				cutpoint = i + j
				return
			}
			outByte := data[i+j-8]
			inByte := data[i+j]

			// The hamming distance instruction POPCNT is
			// typically available in today's hardware, but
			// upon measurement the lookup table still looks
			// faster; plus its more portable.
			//
			// I'll leave the bits.OnesCountXX (POPCNT based)
			// version here in case newer hardware gets even faster; or maybe we
			// weren't using the hardware right when we measured.
			// Or maybe only bits.OnesCount64 uses POPCNT? Not worth
			// going deeper at the moment.
			//
			// https://stackoverflow.com/questions/28802692/how-is-popcnt-implemented-in-hardware
			//
			//update := bits.OnesCount8(inByte^0xAA) - bits.OnesCount8(outByte^0xAA)
			update := hammingDistanceTo0xAA[inByte] - hammingDistanceTo0xAA[outByte]
			dist += update
		}
		outBufWin = inBufWin
	}

	// obviously preserves the POST INVARIANT that cutpoint <= n.
	cutpoint = n
	return
}

// Cutpoints computes all the cutpoints we can in a batch, all at once,
// if maxPoints <= 0; otherwise only up to a maximum of maxPoints.
// We may find fewer, of course. There will always be one, as
// len(data) is returned in cuts if no sooner cutpoint is found.
// If maxPoints <= 0 then the last cutpoint in cuts will
// always be len(data).
func (c *UltraCDC) Cutpoints(data []byte, maxPoints int) (cuts []int) {

	const (
		maskS uint64 = 0x2F // binary 101111

		// maskL ignores 2 more bits than maskS, so
		// it is easier to match (so we get a higher
		// probability of match after the normal point).
		maskL uint64 = 0x2C // binary 101100

		lowEntropyStringThreshold int = 64 // LEST in the paper.
	)
	minSize := c.Opts.MinSize
	maxSize := c.Opts.MaxSize
	normalSize := c.Opts.TargetSize

	if minSize <= 0 {
		panic("MinSize must be positive")
	}

	// most recently found cut.
	var cutpoint int

	// Find as many cutpoints as we can.
	// After each one, start again at begin.
begin:
	for {
		n := len(data)

		// We modify normal below, if end <= normalSize.
		// I doubt we can would normally do another loop
		// after that, but I cannot prove it would never
		// happen. So to be on the safe side, we just reset it
		// every time.
		normal := normalSize

		var lowEntropyCount int

		// initial mask for small cuts below the Normal point.
		mask := maskS

		//fmt.Printf("n = %v; minSize=%v; normalSize=%v; maxSize=%v; len(cuts)=%v\n",
		//	n, minSize, normalSize, maxSize, len(cuts))

		// how far we go on a single pass through
		// this loop looking for a cutpoint.
		end := maxSize
		if n < end {
			end = n
		}
		if end <= minSize {
			// clients should recognize that the very
			// last cut may be "pre-mature".
			cutpoint += end
			cuts = append(cuts, cutpoint)
			//fmt.Printf("end %v <= minSize = %v, returning\n", end, minSize)
			return
		}
		//fmt.Printf("end %v > minSize = %v, not returning\n", end, minSize)

		if end <= normalSize {
			normal = end
		}

		outBufWin := data[minSize : minSize+8]

		// Initialize hamming distance on outBufWin
		dist := 0
		for _, v := range outBufWin {
			// effectively the Pattern of 0xAAAAAAAAAAAAAAAA,
			// as referenced in the paper,
			// is expressed here, just one byte at a time.
			dist += bits.OnesCount8(v ^ 0xAA)
		}

		var inBufWin []byte

	innerLoop:
		for i := minSize + 8; i <= end-8; i += 8 {
			if i >= normal {
				// Yes, we write mask every time after the Normal point,
				// and at first this appears wasteful. However,
				// an engineering judgement call was made to keep it.
				//
				// The rationale is that this small redudancy
				// is cheaper, simpler, and safer
				// than duplicating all the logic below for the two different
				// masks and then having to be sure to keep
				// the duplicates in sync. We saw multiple bugs in the past from
				// MinSize and TargetSize not being 8 byte aligned,
				// and we'd also rather not impose that on users.
				// The POST invariance analysis is also much simplified.
				// The CPU has to do less branch prediction this way,
				// and maskL will almost surely be quickly
				// accessible in a cache line.
				mask = maskL
			}

			// If i == n-8 then i+8 == n, and since n <= len(data)
			// as a PRE condition, we never go out of bounds.
			inBufWin = data[i : i+8]

			if bytes.Equal(inBufWin, outBufWin) {
				lowEntropyCount++
				if lowEntropyCount >= lowEntropyStringThreshold {
					// on random (high-entropy) data, we don't expect to get here.

					// If i == n-8, its largest, then this returns n,
					// which maintains our POST INVARIANT that cutpoint <= n.
					cut := i + 8
					cutpoint += cut
					cuts = append(cuts, cutpoint)
					data = data[cut:]
					//fmt.Printf("found LEST cutpoint %v; data now len %v\n", cutpoint, len(data))
					if len(data) == 0 || (maxPoints > 0 && len(cuts) >= maxPoints) {
						return
					}
					continue begin
				}
				continue innerLoop
			}

			lowEntropyCount = 0
			for j := 0; j < 8; j++ {
				if (uint64(dist) & mask) == 0 {
					// Do we preserve the POST INVARIANT here?
					// if i == n-8 (the biggest possible), and
					// j is 7 (its biggest possible), then
					// i + j could here be as big as n - 8 + 7 == n-1
					// So, yes, n-1 is the biggest we could return here.
					cut := i + j
					cutpoint += cut
					cuts = append(cuts, cutpoint)
					data = data[cut:]
					//fmt.Printf("found dist cut %v; cutpoint %v; data now len %v\n", cut, cutpoint, len(data))
					if len(data) == 0 || (maxPoints > 0 && len(cuts) >= maxPoints) {
						return
					}
					continue begin
				}
				outByte := data[i+j-8]
				inByte := data[i+j]

				// The hamming distance instruction POPCNT is
				// typically available in today's hardware, but
				// upon measurement the lookup table still looks
				// faster; plus its more portable.
				//
				// I'll leave the bits.OnesCountXX (POPCNT based)
				// version here in case newer hardware gets even faster; or maybe we
				// weren't using the hardware right when we measured.
				// Or maybe only bits.OnesCount64 uses POPCNT? Not worth
				// going deeper at the moment.
				//
				// https://stackoverflow.com/questions/28802692/how-is-popcnt-implemented-in-hardware
				//
				//update := bits.OnesCount8(inByte^0xAA) - bits.OnesCount8(outByte^0xAA)
				update := hammingDistanceTo0xAA[inByte] - hammingDistanceTo0xAA[outByte]
				dist += update
			}
			outBufWin = inBufWin

		} // end i

		cut := end
		cutpoint += cut
		cuts = append(cuts, cutpoint)
		data = data[cut:]
		//fmt.Printf("found no other cutpoint %v; data now len %v\n", cutpoint, len(data))
		if len(data) == 0 || (maxPoints > 0 && len(cuts) >= maxPoints) {
			return
		}
	}

	panic("should never be reached now.")
}
