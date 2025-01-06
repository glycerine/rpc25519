package hash

import (
	"syscall"

	"github.com/klauspost/cpuid/v2"
)

/*
   from github.com/lukechampine/blake3
   which is imported as "lukechampine.com/blake3".

The MIT License (MIT)

Copyright (c) 2020 Luke Champine

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

var (
	haveAVX2   bool
	haveAVX512 bool
)

func init() {
	haveAVX2 = cpuid.CPU.Supports(cpuid.AVX2)
	haveAVX512 = cpuid.CPU.Supports(cpuid.AVX512F)
	if !haveAVX512 {
		// On some Macs, AVX512 detection is buggy, so fallback to sysctl
		b, _ := syscall.Sysctl("hw.optional.avx512f")
		haveAVX512 = len(b) > 0 && b[0] == 1
	}
}
