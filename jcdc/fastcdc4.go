package jcdc

/*
 * Copyright (c) 2021 Gilles Chehade <gilles@poolp.org>
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

import (
	//"errors"
	"unsafe"
)

//msgp:ignore FastCDC4
type FastCDC4 struct {
	Opts *CDC_Config `zid:"0"`
}

func Default_FastCDC4_Options() *CDC_Config {
	return &CDC_Config{
		MinSize:    2 * 1024,
		TargetSize: 8 * 1024,
		MaxSize:    64 * 1024,
	}
}

func (c *FastCDC4) Validate(options *CDC_Config) error {
	if options.TargetSize == 0 || options.TargetSize < 64 || options.TargetSize > 1024*1024*1024 {
		return ErrTargetSize
	}
	if options.MinSize < 64 || options.MinSize > 1024*1024*1024 || options.MinSize >= options.TargetSize {
		return ErrMinSize
	}
	if options.MaxSize < 64 || options.MaxSize > 1024*1024*1024 || options.MaxSize <= options.TargetSize {
		return ErrMaxSize
	}
	return nil
}

func (c *FastCDC4) NextCut(data []byte) (cutpoint int) {
	return c.Algorithm(c.Opts, data, len(data))
}

func (c *FastCDC4) Algorithm(options *CDC_Config, data []byte, n int) int {
	MinSize := options.MinSize
	MaxSize := options.MaxSize
	TargetSize := options.TargetSize

	const (
		MaskS = uint64(0x0003590703530000)
		MaskL = uint64(0x0000d90003530000)
	)

	switch {
	case n <= MinSize:
		return n
	case n >= MaxSize:
		n = MaxSize
	case n <= TargetSize:
		TargetSize = n
	}

	fp := uint64(0)
	i := MinSize
	mask := MaskS

	p := unsafe.Pointer(&data[i])
	for ; i < n; i++ {
		if i == TargetSize {
			mask = MaskL
		}
		fp = (fp << 1) + GearTable4[*(*byte)(p)]
		if (fp & mask) == 0 {
			return i
		}
		p = unsafe.Pointer(uintptr(p) + 1)
	}
	return i
}
