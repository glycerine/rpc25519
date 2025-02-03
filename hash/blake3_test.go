package hash

// See benchmark runs at the end of this file.

/* The BenchmarkSum256() test and cpu*go files are from github.com/lukechampine/blake3
   which is imported as "lukechampine.com/blake3".
   The other benchmarks are derived from this.

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

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/glycerine/blake3"
)

func TestBlake3(t *testing.T) {
	fmt.Printf("haveAVX2 = %v\n", haveAVX2)
	fmt.Printf("haveAVX512 = %v\n\n", haveAVX512)

	b3 := NewBlake3()
	data := []byte("hello world!")
	dig := b3.UnlockedDigest512(data)
	fmt.Printf("UnlockedDigest512 is %v bytes (%v bits)\n\n", len(dig), len(dig)*8)

	// confirm choice of methods for a 32 byte (256 bit) hash,
	// whether using truncation or not, will not matter:
	un512 := dig[:33]
	un264 := b3.UnlockedDigest264(data)
	lk512tmp := b3.LockedDigest512(data)
	lk512 := lk512tmp[:33]
	lk264 := b3.LockedDigest264(data)

	standalone512 := Blake3OfBytes(data)

	fmt.Printf("un512[:32] = '%x'\n", un512)
	fmt.Printf("lk512[:32] = '%x'\n", lk512)
	fmt.Printf("un264[:32] = '%x'\n", un264)
	fmt.Printf("lk264[:32] = '%x'\n", lk264)
	fmt.Printf("alone[:32] = '%x'\n", standalone512[:33])

	if !bytes.Equal(un512, standalone512[:33]) {
		panic("disagree!")
	}
	if !bytes.Equal(un512, un264) {
		panic("disagree!")
	}
	if !bytes.Equal(un512, lk264[:]) {
		panic("disagree!")
	}
	if !bytes.Equal(un512, lk512) {
		panic("disagree!")
	}

	fmt.Printf("ex: %v\n", Blake3OfBytesString(nil))
}

const runAllSizes = false // false => just run the 64K size inputs.

func BenchmarkUnlockedDigest512(b *testing.B) {
	b3 := NewBlake3()
	if runAllSizes {
		b.Run("64", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(64)
			buf := make([]byte, 64)
			for i := 0; i < b.N; i++ {
				b3.UnlockedDigest512(buf)
			}
		})
		b.Run("1024", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(1024)
			buf := make([]byte, 1024)
			for i := 0; i < b.N; i++ {
				b3.UnlockedDigest512(buf)
			}
		})
	}
	b.Run("65536", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(65536)
		buf := make([]byte, 65536)
		for i := 0; i < b.N; i++ {
			b3.UnlockedDigest512(buf)
		}
	})
}

func BenchmarkUnlockedDigest264(b *testing.B) {
	b3 := NewBlake3()
	if runAllSizes {
		b.Run("64", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(64)
			buf := make([]byte, 64)
			for i := 0; i < b.N; i++ {
				b3.UnlockedDigest264(buf)
			}
		})
		b.Run("1024", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(1024)
			buf := make([]byte, 1024)
			for i := 0; i < b.N; i++ {
				b3.UnlockedDigest264(buf)
			}
		})
	}
	b.Run("65536", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(65536)
		buf := make([]byte, 65536)
		for i := 0; i < b.N; i++ {
			b3.UnlockedDigest264(buf)
		}
	})
}

func BenchmarkLockedDigest264(b *testing.B) {
	b3 := NewBlake3()
	if runAllSizes {
		b.Run("64", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(64)
			buf := make([]byte, 64)
			for i := 0; i < b.N; i++ {
				b3.LockedDigest264(buf)
			}
		})
		b.Run("1024", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(1024)
			buf := make([]byte, 1024)
			for i := 0; i < b.N; i++ {
				b3.LockedDigest264(buf)
			}
		})
	}
	b.Run("65536", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(65536)
		buf := make([]byte, 65536)
		for i := 0; i < b.N; i++ {
			b3.LockedDigest264(buf)
		}
	})
}

func BenchmarkLockedDigest512(b *testing.B) {
	b3 := NewBlake3()
	if runAllSizes {
		b.Run("64", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(64)
			buf := make([]byte, 64)
			for i := 0; i < b.N; i++ {
				b3.LockedDigest512(buf)
			}
		})
		b.Run("1024", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(1024)
			buf := make([]byte, 1024)
			for i := 0; i < b.N; i++ {
				b3.LockedDigest512(buf)
			}
		})
	}
	b.Run("65536", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(65536)
		buf := make([]byte, 65536)
		for i := 0; i < b.N; i++ {
			b3.LockedDigest512(buf)
		}
	})
}

// above Unlocked methods use the generic Hasher interface;
// compare to the blake3.Sum256 method,
// which might have better hardware acceleration.
func BenchmarkSum256(b *testing.B) {
	if runAllSizes {
		b.Run("64", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(64)
			buf := make([]byte, 64)
			for i := 0; i < b.N; i++ {
				blake3.Sum256(buf)
			}
		})
		b.Run("1024", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(1024)
			buf := make([]byte, 1024)
			for i := 0; i < b.N; i++ {
				blake3.Sum256(buf)
			}
		})
	}
	b.Run("65536", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(65536)
		buf := make([]byte, 65536)
		for i := 0; i < b.N; i++ {
			blake3.Sum256(buf)
		}
	})
}

// our string returning method.
func BenchmarkHash32(b *testing.B) {
	b3 := NewBlake3()
	if runAllSizes {
		b.Run("64", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(64)
			buf := make([]byte, 64)
			for i := 0; i < b.N; i++ {
				b3.Hash32(buf)
			}
		})
		b.Run("1024", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(1024)
			buf := make([]byte, 1024)
			for i := 0; i < b.N; i++ {
				b3.Hash32(buf)
			}
		})
	}
	b.Run("65536", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(65536)
		buf := make([]byte, 65536)
		for i := 0; i < b.N; i++ {
			b3.Hash32(buf)
		}
	})
}

func Benchmark_Blake3OfBytesString(b *testing.B) {
	if runAllSizes {
		b.Run("64", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(64)
			buf := make([]byte, 64)
			for i := 0; i < b.N; i++ {
				Blake3OfBytesString(buf)
			}
		})
		b.Run("1024", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(1024)
			buf := make([]byte, 1024)
			for i := 0; i < b.N; i++ {
				Blake3OfBytesString(buf)
			}
		})
	}
	b.Run("65536", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(65536)
		buf := make([]byte, 65536)
		for i := 0; i < b.N; i++ {
			Blake3OfBytesString(buf)
		}
	})
}

func Benchmark_Blake3OfBytes(b *testing.B) {
	if runAllSizes {
		b.Run("64", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(64)
			buf := make([]byte, 64)
			for i := 0; i < b.N; i++ {
				Blake3OfBytes(buf)
			}
		})
		b.Run("1024", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(1024)
			buf := make([]byte, 1024)
			for i := 0; i < b.N; i++ {
				Blake3OfBytes(buf)
			}
		})
	}
	b.Run("65536", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(65536)
		buf := make([]byte, 65536)
		for i := 0; i < b.N; i++ {
			Blake3OfBytes(buf)
		}
	})
}

/* with only AVX2 and not AVX512: just the 64K input:

Compilation started at Mon Jan  6 13:08:14

go test -v -bench=.
=== RUN   TestBlake3
haveAVX2 = true
haveAVX512 = false

UnlockedDigest512 is 64 bytes (512 bits)

un512[:32] = '3aa61c409fd7717c9d9c639202af2fae470c0ef669be7ba2caea5779cb534e9d'
lk512[:32] = '3aa61c409fd7717c9d9c639202af2fae470c0ef669be7ba2caea5779cb534e9d'
un256[:32] = '3aa61c409fd7717c9d9c639202af2fae470c0ef669be7ba2caea5779cb534e9d'
lk256[:32] = '3aa61c409fd7717c9d9c639202af2fae470c0ef669be7ba2caea5779cb534e9d'
alone[:32] = '3aa61c409fd7717c9d9c639202af2fae470c0ef669be7ba2caea5779cb534e9d'
--- PASS: TestBlake3 (0.00s)
goos: linux
goarch: amd64
pkg: github.com/glycerine/rpc25519/hash
cpu: AMD Ryzen Threadripper 3960X 24-Core Processor
BenchmarkUnlockedDigest512
BenchmarkUnlockedDigest512/65536
BenchmarkUnlockedDigest512/65536-48 	   47662	     24739 ns/op	2649.09 MB/s	      64 B/op	       1 allocs/op
BenchmarkUnlockedDigest256
BenchmarkUnlockedDigest256/65536
BenchmarkUnlockedDigest256/65536-48 	   48900	     24882 ns/op	2633.84 MB/s	      64 B/op	       1 allocs/op
BenchmarkLockedDigest256
BenchmarkLockedDigest256/65536
BenchmarkLockedDigest256/65536-48   	   47431	     24990 ns/op	2622.48 MB/s	      64 B/op	       1 allocs/op
BenchmarkLockedDigest512
BenchmarkLockedDigest512/65536
BenchmarkLockedDigest512/65536-48   	   45727	     26109 ns/op	2510.05 MB/s	      64 B/op	       1 allocs/op
BenchmarkSum256
BenchmarkSum256/65536
BenchmarkSum256/65536-48            	   47292	     24859 ns/op	2636.33 MB/s	       0 B/op	       0 allocs/op
BenchmarkHash32
BenchmarkHash32/65536
BenchmarkHash32/65536-48            	   46792	     25175 ns/op	2603.20 MB/s	     176 B/op	       3 allocs/op
Benchmark_Blake3OfBytesString
Benchmark_Blake3OfBytesString/65536
Benchmark_Blake3OfBytesString/65536-48         	   45538	     25912 ns/op	2529.15 MB/s	     176 B/op	       3 allocs/op
Benchmark_Blake3OfBytes
Benchmark_Blake3OfBytes/65536
Benchmark_Blake3OfBytes/65536-48               	   46977	     25183 ns/op	2602.34 MB/s	      64 B/op	       1 allocs/op
PASS
ok  	github.com/glycerine/rpc25519/hash	11.597s

Compilation finished at Mon Jan  6 13:08:26, duration 11.8 s

*/

/* WITH AVX512, about 30% faster: (here on macOS/darwin):

=== RUN   TestBlake3
haveAVX2 = true
haveAVX512 = true

UnlockedDigest512 is 64 bytes (512 bits)

un512[:32] = '3aa61c409fd7717c9d9c639202af2fae470c0ef669be7ba2caea5779cb534e9d'
lk512[:32] = '3aa61c409fd7717c9d9c639202af2fae470c0ef669be7ba2caea5779cb534e9d'
un256[:32] = '3aa61c409fd7717c9d9c639202af2fae470c0ef669be7ba2caea5779cb534e9d'
lk256[:32] = '3aa61c409fd7717c9d9c639202af2fae470c0ef669be7ba2caea5779cb534e9d'
alone[:32] = '3aa61c409fd7717c9d9c639202af2fae470c0ef669be7ba2caea5779cb534e9d'
--- PASS: TestBlake3 (0.00s)
goos: darwin
goarch: amd64
pkg: github.com/glycerine/rpc25519/hash
cpu: Intel(R) Core(TM) i7-1068NG7 CPU @ 2.30GHz
BenchmarkUnlockedDigest512
BenchmarkUnlockedDigest512/65536
BenchmarkUnlockedDigest512/65536-8         	   64432	     18322 ns/op	3576.98 MB/s	      64 B/op	       1 allocs/op
BenchmarkUnlockedDigest256
BenchmarkUnlockedDigest256/65536
BenchmarkUnlockedDigest256/65536-8         	   62337	     18150 ns/op	3610.83 MB/s	      64 B/op	       1 allocs/op
BenchmarkLockedDigest256
BenchmarkLockedDigest256/65536
BenchmarkLockedDigest256/65536-8           	   63712	     18016 ns/op	3637.64 MB/s	      64 B/op	       1 allocs/op
BenchmarkLockedDigest512
BenchmarkLockedDigest512/65536
BenchmarkLockedDigest512/65536-8           	   63423	     18376 ns/op	3566.34 MB/s	      64 B/op	       1 allocs/op
BenchmarkSum256
BenchmarkSum256/65536
BenchmarkSum256/65536-8                    	   62223	     18152 ns/op	3610.47 MB/s	       0 B/op	       0 allocs/op
BenchmarkHash32
BenchmarkHash32/65536
BenchmarkHash32/65536-8                    	   62054	     18307 ns/op	3579.83 MB/s	     176 B/op	       3 allocs/op
Benchmark_Blake3OfBytesString
Benchmark_Blake3OfBytesString/65536
Benchmark_Blake3OfBytesString/65536-8      	   55426	     20535 ns/op	3191.43 MB/s	     176 B/op	       3 allocs/op
Benchmark_Blake3OfBytes
Benchmark_Blake3OfBytes/65536
Benchmark_Blake3OfBytes/65536-8            	   62908	     18083 ns/op	3624.10 MB/s	      64 B/op	       1 allocs/op
PASS
ok  	github.com/glycerine/rpc25519/hash	10.866s

*/
