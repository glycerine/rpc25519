package ultracdc

// Portions Copyright (c) 2024 Jason E. Aten, Ph.D. All rights reserved.
// See also below for the Apache2 licensed comments in this file.
/*
    BSD 3-clause style license:

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

  - Redistributions of source code must retain the above copyright
    notice, this list of conditions and the following disclaimer.
  - Redistributions in binary form must reproduce the above
    copyright notice, this list of conditions and the following disclaimer
    in the documentation and/or other materials provided with the
    distribution.
  - Neither the name of the author, nor Google Inc. nor the names of its
    contributors may be used to endorse or promote products derived from
    this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

/* Quotes below about the algorithm implemented here, and the
 * original C++ source which was ported to Go, were used
 * under the following license:
 *
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import (
	"fmt"
	"math"
)

var _ = fmt.Printf

//msgp:ignore FastCDC
type FastCDC struct {
	Opts *CDC_Config `zid:"0"`
}

func NewFastCDC(opts *CDC_Config) *FastCDC {
	u := &FastCDC{}
	if opts == nil {
		opts = Default_FastCDC_Options()
	}
	u.Opts = opts
	return u
}

func (c *FastCDC) Name() string {
	return "fastcdc-Stadia-Google-64bit-arbitrary-regression-jea"
}

func (c *FastCDC) Config() *CDC_Config {
	return c.Opts
}

func Default_FastCDC_Options() *CDC_Config {
	return &CDC_Config{
		MinSize:    2 * 1024,
		NormalSize: 10 * 1024,
		MaxSize:    64 * 1024,
	}
}

func (c *FastCDC) Validate(options *CDC_Config) error {

	if options.NormalSize == 0 || options.NormalSize < 64 ||
		options.NormalSize > 1024*1024*1024 {
		return ErrNormalSize
	}
	if options.MinSize < 64 || options.MinSize > 1024*1024*1024 ||
		options.MinSize >= options.NormalSize {
		return ErrMinSize
	}
	if options.MaxSize < 64 || options.MaxSize > 1024*1024*1024 ||
		options.MaxSize <= options.NormalSize {
		return ErrMaxSize
	}
	return nil
}

// from https://github.com/google/cdc-file-transfer/blob/main/fastcdc/fastcdc.h
// description of this particular FastCDC implementation:
//
// Implements a very fast content-defined chunking algorithm.
//
// FastCDC [1] identifies chunk boundaries based on a simple yet efficient
// "gear" rolling hash, a "normalized chunking" algorithm using a stepped
// chunk probability with a pair spread-out bitmasks for the '!(hash&mask)'
// "hash criteria".
//
// This library implements a modified version based on rollsum-chunking [2]
// tests and analysis that showed simple "exponential chunking" gives better
// deduplication, and a 'hash<=threshold' "hash criteria" works better for
// the gear rollsum and can support arbitrary non-power-of-two sizes.
//
// For limiting block sizes it uses a modified version of "Regression
// Chunking"[3] with an arbitrary number of regressions using power-of-2
// target block lengths (not multiples of the target block length, which
// doesn't have to be a power-of-2). This means we can use a bitmask for the
// most significant bits for the regression hash criteria.
//
// The Config struct passed in during construction defines the minimum, average,
// and maximum allowed chunk sizes. Those are runtime parameters.
//
// The template allows additional compile-time configuration:
//
// - T : The type used for the hash. Should be an unsigned integer type,
// ideally uint32_t or uint64_t. The number of bits of this type determines
// the "sliding window" size of the gear hash. A smaller type is likely to be
// faster at the expense of reduced deduplication. (JEA note: we only use uint64.)
//
// - gear: an array of random numbers that serves as a look-up table to
// modify be added to the rolling hash in each round based on the input data.
// This library comes with two different tables, one of type uint32_t and one
// of uint64_t. Both showed good results in our experiments, yet the 64-bit
// version provided slightly better deduplication. (JEA note: only 64-bit here.)
//
// [1] https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf.
// [2] https://github.com/dbaarda/rollsum-chunking/blob/master/RESULTS.rst
// [3] https://www.usenix.org/system/files/conference/atc12/atc12-final293.pdf
//
// My (JEA) notes:
//
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
func (c *FastCDC) Algorithm(options *CDC_Config, data []byte, N int) (cutpoint int) {

	// A common case will be n == len(data), but n could certainly be less.
	// Confirm that it is never more.
	if N > len(data) {
		panic(fmt.Sprintf("len(data) == %v and n == %v: n must be <= len(data)", len(data), N))
	}

	// the code below needs n to be uint64, so rename the
	// formal argument to N and let little n be uint64 from now on.
	n := uint64(N)

	// NB: the very last chunk might be smaller than this.
	minSize := uint64(options.MinSize)

	// chunks are never bigger.
	maxSize := uint64(options.MaxSize)

	// "The average chunk size is the target size for chunks, not including the
	// effects of max_size regression. Before regression, sizes will show an
	// offset exponential distribution decaying after min_size with the desired
	// average size. Regression will "reflect-back" the exponential
	// distribution past max_size, which reduces the actual average size and
	// gives a very flat distribution when max_size is small."
	//  -- https://github.com/google/cdc-file-transfer/blob/main/fastcdc/fastcdc.h

	normalSize := uint64(options.NormalSize)

	thresh := uint64(math.MaxUint64) / (normalSize - minSize + 1)

	switch {
	case n <= minSize:
		return int(n)
	case n >= maxSize:
		n = maxSize
	}

	// Initialize the regression length to len (the end) and the regression
	// mask to an empty bitmask (match any hash).
	regressionLen := n
	var regressionMask uint64 // == 0

	// Init hash to all 1's to avoid zero-length chunks with min_size=0.
	var hash uint64 = math.MaxUint64

	// Skip the first min_size bytes, but "warm up" the rolling hash for enough
	// rounds to make sure the hash has gathered full "content history".

	const kHashBits = 64

	var i uint64
	if minSize > kHashBits {
		i = minSize - kHashBits
	}

	for ; i < minSize; i++ {
		hash = (hash << 1) + gear64[data[i]]
	}

	// (leave i at minSize! do not set back to 0)
	for ; i < n; i++ {

		if hash&regressionMask == 0 {

			if hash <= thresh {
				// This hash matches the target length
				// hash criteria, return it.
				return int(i)
			}

			// This is a better regression point. Set it as the new regressionLen and
			// update regressionMask to check as many MSBits as this hash would pass.
			regressionLen = i
			regressionMask = math.MaxUint64

			for hash&regressionMask != 0 {
				regressionMask = regressionMask << 1
			}
		}
		hash = (hash << 1) + gear64[data[i]]
	}
	// Return best regression point we found or the end if it's better.
	if hash&regressionMask != 0 {
		return int(regressionLen)
	}
	return int(i)
}

// Cutpoints computes all the cutpoints we can in a batch, all at once,
// if maxPoints <= 0; otherwise only up to a maximum of maxPoints.
// We may find fewer, of course. There will always be one, as
// len(data) is returned in cuts if no sooner cutpoint is found.
// If maxPoints <= 0 then the last cutpoint in cuts will
// always be len(data).
func (c *FastCDC) Cutpoints(data []byte, maxPoints int) (cuts []int) {

	// TODO: not yet inlined! just calls Algorithm() above.

	// most recently found cut.
	var cutpoint int

	for len(data) > 0 {
		cut := c.Algorithm(c.Opts, data, len(data))
		cutpoint += cut
		cuts = append(cuts, cutpoint)
		data = data[cut:]
	}
	return
}

// random [256] slice
var gear64 = []uint64{

	0x651748f5a15f8222, 0xd6eda276c877d8ea, 0x66896ef9591b326b,
	0xcd97506b21370a12, 0x8c9c5c9acbeb2a05, 0xb8b9553ee17665ef,
	0x1784a989315b1de6, 0x947666c9c50df4bd, 0xb3f660ea7ff2d6a4,
	0xbcd6adb8d6d70eb5, 0xb0909464f9c63538, 0xe50e3e46a8e1b285,
	0x21ed7b80c0163ce0, 0xf209acd115f7b43b, 0xb8c9cb07eaf16a58,
	0xb60478aa97ba854c, 0x8fb213a0b5654c3d, 0x42e8e7bd9fb03710,
	0x737e3de60a90b54f, 0x9172885f5aa79c8b, 0x787faae7be109c36,
	0x86ad156f5274cb9f, 0x6ac0a8daa59ee1ab, 0x5e55bc229d5c618e,
	0xa54fb69a5f181d41, 0xc433d4cf44d8e974, 0xd9efe85b722e48a3,
	0x7a5e64f9ea3d9759, 0xba3771e13186015d, 0x5d468c5fad6ef629,
	0x96b1af02152ebfde, 0x63706f4aa70e0111, 0xe7a9169252de4749,
	0xf548d62570bc8329, 0xee639a9117e8c946, 0xd31b0f46f3ff6847,
	0xfed7938495624fc5, 0x1ef2271c5a28122e, 0x7fd8e0e95eac73ef,
	0x920558e0ee131d4c, 0xce2e67cb1034bcd1, 0x6f4b338d34b004ae,
	0x92f5e7271cf95c9a, 0x12e1305a9c558342, 0x1e30d88013ad77ae,
	0x09acc1a57bbb604e, 0xaf187082c6f56192, 0xd2e5d987f04ac6f0,
	0x3b22fca40423da70, 0x7dfba8ce699a9a87, 0xe8b15f90ea96bd2a,
	0xcda1a1089cc2cbe7, 0x72f70448459de898, 0x1ab992dbb61cd46e,
	0x912ad04becbb29da, 0x98c6bb3aa3ce09ed, 0x6373bd2e7a041f3a,
	0x1f98f28bd178c53a, 0xe6adbc82ba5d9f96, 0x7456da7d805cbe01,
	0xd673662dcc135eeb, 0xb299e26eaadcb311, 0x2c2582172f8114af,
	0xeded114d7f623da6, 0xb3462a0e623276e4, 0x3af752be3d34bfaa,
	0x1311ccc0a1855a89, 0x0812bbcecc92b2e4, 0x9974b5747289f2f5,
	0x3a030eff770f2026, 0x52462b2aa42a847a, 0x2beaa107d15a012b,
	0x0c0035e0fe073398, 0x4f2f9de2ac206766, 0x5dd51a617c291deb,
	0x1ac66905652cc03b, 0x11067b0947fc07a1, 0x02b5fcd96ad06d52,
	0x74244ec1aa2821fd, 0xf6089e32060e9439, 0xd8f076a33bcbf1a7,
	0x5162743c755d8d5e, 0x8d34fc683e4e3d06, 0x46efe9b21a0252a3,
	0x4631e8d0109c6145, 0xfdf7a14bc0223957, 0x750934b3d0b8bb1e,
	0x2ecd1b3efed5ddb9, 0x2bcbd89a83ccfbce, 0x3507c79e58dd5886,
	0x5476a67ecd4a772f, 0xaa0be3856dd76405, 0x22289a358a4dd421,
	0xf570433f14503ad1, 0x8a9f440251a722c3, 0x77dd711752b4398c,
	0xbbd9edf9c6160a31, 0xb94b59220b23f079, 0xfdca3d75d2f33ccf,
	0xb29452c460c9e977, 0xe89afe2dd4bf3b02, 0x47ec6f32c91bfee4,
	0x1aab5ec3445706b8, 0x588bf4fa55334006, 0xe2290ca1e29acd96,
	0x3c49e189f831c37c, 0x6448c973b5177498, 0x556a6e09ba158de7,
	0x90b25013a8d9a067, 0xa4f2f7a50c58e1c4, 0x5e765e871008700e,
	0x242f5ae7738327af, 0xc1e6a2819cc5a219, 0xcb48d801fd6a5449,
	0xa208de2301931383, 0xde3c143fe44e39b0, 0x6bb74b09c73e4133,
	0xb5b1ed1b63d54c11, 0x587567d454ce7716, 0xf47ddbc987cb0392,
	0x87b19254448f03f1, 0x985fd00ec372fafa, 0x64b92ba521aa46e4,
	0xce63f4013d587b0f, 0xa691ae698726030e, 0xeaefbf690264e9aa,
	0x68edd400523eb152, 0x35d9353aa1957c60, 0x2e2c2d7a9cb68385,
	0xfc7549edaf43bf9e, 0x48b2adb23026e2c7, 0x3777cb79a024bcf9,
	0x644128f7c184102d, 0x70189d3ca4390de9, 0x085fea7986d4cd34,
	0x6dbe7626c8457464, 0x9fa41cfa9c4265eb, 0xdaa163a641946463,
	0x02f5c4bd9efa2074, 0x783201871822c3c9, 0xb0dfec499202bce0,
	0x1f1c9c12d84dccab, 0x1596f8819f2ed68e, 0xb0352c3e9fc84468,
	0x24a6673db9122956, 0x84f5b9e60b274739, 0x7216b28a0b54ac46,
	0xc7789de20e9cdca4, 0x903db5d289dd6563, 0xce66a947f7033516,
	0x3677dbc62307b2ca, 0x8d8e9d5530eb46ac, 0x79c4bad281bd93e2,
	0x287d942042068c36, 0xde4b98e5464b6ad5, 0x612534b97d1d21bf,
	0xdf98659772d822a1, 0x93053df791aa6264, 0x2254a8a2d54528ba,
	0x2301164aeb69c43d, 0xf56863474ac2417f, 0x6136b73e1b75de42,
	0xc7c3bd487e06b532, 0x7232fbed1eb9be85, 0x36d60f0bd7909e43,
	0xe08cbf774a4ce1f2, 0xf75fbc0d97cb8384, 0xa5097e5af367637b,
	0x7bce2dcfa856dbb2, 0xfbfb729dd808c894, 0x3dc8eba10ad7112e,
	0xf2d1854eedce4928, 0xb705f5c1aebd2104, 0x78fa4d004417d956,
	0x9e5162660729f858, 0xda0bcd5eb9f91f0e, 0x748d1be11e06b362,
	0xf4c2be9a04547734, 0x6f2bcd7c88abdf9a, 0x50865dafdfd8a404,
	0x9d820665691728f0, 0x59fe7a56aa07118e, 0x4df1d768c23660ec,
	0xab6310b8edfb8c5e, 0x029b47623fc9ffe4, 0x50c2cca231374860,
	0x0561505a8dbbdc69, 0x8d07fe136de385f3, 0xc7fb6bb1731b1c1c,
	0x2496d1256f1fac7a, 0x79508cee90d84273, 0x09f51a2108676501,
	0x2ef72d3dc6a50061, 0xe4ad98f5792dd6d6, 0x69fa05e609ae7d33,
	0xf7f30a8b9ae54285, 0x04a2cb6a0744764b, 0xc4b0762f39679435,
	0x60401bc93ef6047b, 0x76f6aa76e23dbe0c, 0x8a209197811e39da,
	0x4489a9683fa03888, 0x2604ad5741a6f8d8, 0x7faa9e0c64a94532,
	0x0dbfee8cdae8f54e, 0x0a7c5885f0b76d4a, 0x55dfb1ac12e83645,
	0xedc967651c4938cc, 0x4e006ab71a48b85e, 0x193f621602de413c,
	0xb56458b71d56944f, 0xf2b639509a2fa5da, 0xb4a76f284c365450,
	0x4d3b65d2d2ae22f7, 0xbcc5f8303efca485, 0x8a044f312671aaea,
	0x688d69e89af0f57a, 0x229957dc1facede8, 0x2ed75c321073da13,
	0xf199e7ece5fcefef, 0x50c85b5c837a6c64, 0x71703c6e676bf698,
	0xc1b4eb52b1e5a518, 0x0f46a5e6c9cb68ca, 0xebb933688d69d7f7,
	0x5ab7404b8d1e3ef4, 0x261acc20c5a64a90, 0xb88788798adc718a,
	0x3e44e9b6bad5bc15, 0xf6bb456f086346bc, 0xd66e17e5734cbde1,
	0x392036dae96e389d, 0x4a62ceac9d4202de, 0x9d55f412f32e5f6e,
	0x0e1d841509d9ee9d, 0xc3130bdc638ed9e2, 0x0cd0e82af24964d9,
	0x3ec4c59463ba9b50, 0x055bc4d8685ab1bc, 0xb9e343c96a3a4253,
	0x8eba190d8688f7f9, 0xd31df36c792c629b, 0xddf82f659b127104,
	0x6f12dc8ba930fbb7, 0xa0aee6bb7e81a7f0, 0x8c6ba78747ae8777,
	0x86f00167eda1f9bc, 0x3a6f8b8f8a3790c9, 0x7845bb4a1c3bfbbb,
	0xc875ab077f66cf23, 0xa68b83d8d69b97ee, 0xb967199139f9a0a6,
	0x8a3a1a4d3de036b7, 0xdf3c5c0c017232a4, 0x8e60e63156990620,
	0xd31b4b03145f02fa}
