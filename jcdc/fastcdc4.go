package jcdc

// lightly modified from
// https://github.com/PlakarKorp/go-cdc-chunkers/blob/main/chunkers/fastcdc/fastcdc.go
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

// stay consistent with
var _ Cutpointer = &FastCDC_Plakar{}

//msgp:ignore FastCDC_Plakar
type FastCDC_Plakar struct {
	Opts *CDC_Config `zid:"0"`
}

func NewFastCDC_Plakar(opts *CDC_Config) *FastCDC_Plakar {
	u := &FastCDC_Plakar{}
	if opts == nil {
		opts = Default_FastCDC_Plakar_Options()
	}
	u.Opts = opts
	return u
}

func (c *FastCDC_Plakar) SetConfig(cfg *CDC_Config) {
	c.Opts = cfg
}

func (c *FastCDC_Plakar) Config() *CDC_Config {
	return c.Opts
}

func (c *FastCDC_Plakar) Name() string {
	return "fastcdc-plakar-go-cdc-chunkers"
}

func Default_FastCDC_Plakar_Options() *CDC_Config {
	return &CDC_Config{
		MinSize:    2 * 1024,
		TargetSize: 8 * 1024,
		MaxSize:    64 * 1024,
	}
}

func (c *FastCDC_Plakar) Validate(options *CDC_Config) error {
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

func (c *FastCDC_Plakar) NextCut(data []byte) (cutpoint int) {
	return c.Algorithm(c.Opts, data, len(data))
}

func (c *FastCDC_Plakar) Cutpoints(data []byte, maxPoints int) (cuts []int) {

	// not yet inlined! just calls Algorithm().

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

func (c *FastCDC_Plakar) Algorithm(options *CDC_Config, data []byte, n int) int {
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

// randomly generated Gear table
var GearTable4 [256]uint64 = [256]uint64{
	0x4d65822107fcfd52,
	0x78629a0f5f3f164f,
	0xd5104dc76695721d,
	0xb80704bb7b4d7c03,
	0x365a858149c6e2d1,
	0x57e9d1860d1d68d8,
	0x8866cb397916001e,
	0x9408d2ac22c4d294,
	0xc697f48392907a0,
	0xa68447a4189deb99,
	0x41f27cc6f3875d04,
	0x68255aaf95e94627,
	0x9b6cffa2ba517936,
	0x30b95ff183c471d4,
	0xa8b621587cb3ad0b,
	0x3c04951aa42655d9,
	0xa43a768b7c4e0b68,
	0xa5845c95d4491d1b,
	0x56ec3f2525632186,
	0x9bf98be2a9d78d73,
	0x1a02070f169c1121,
	0x2e3108dabb158644,
	0xc90bd268b68e6a3f,
	0x6e661e92759805f5,
	0xa584c47f2cdf5b8a,
	0x2606cd2b57d29245,
	0x6054502fc5d6d268,
	0x1a714cf86b83d0e2,
	0xeec34c367674cb74,
	0xd92e17f7b068d9db,
	0x430c8b35bb9457d8,
	0x39f6f78a15d523b,
	0x944419db794209ff,
	0x4dba7b0f9da1d7eb,
	0xfcd4b7a55a25e0cb,
	0x8a2b894cf840ec4b,
	0x4c22b02936d4ff9b,
	0x879143f7f4a5ee3b,
	0x589442fd5ad145f4,
	0x26984b92f6740304,
	0x962d968d3f71f8cb,
	0x4542c29291018d7c,
	0xc5a6e3cafccae224,
	0xa3a62343b186b51f,
	0xb629d9f17d9e8fbc,
	0xc3ea3b9393f93f33,
	0x207403def63a5b6f,
	0x241b3ae419476c36,
	0x64f1017fbc897d06,
	0x2e4fa459169873f5,
	0xf0b5a315724c7af1,
	0xa607c649581eeb39,
	0x727a71f52257bb7d,
	0xc7964976f269a28,
	0x7d0b9ca8be8e9981,
	0x89825e117039374b,
	0x9c73fac825416fed,
	0xd72d92faded7e411,
	0x1ee9f7676678e7aa,
	0xa7dff7ab244fcd36,
	0x7767830356aa6b86,
	0x5ef4e81ede4561ad,
	0x6688f8bd3e99b0a8,
	0x5d78399cbed80a3a,
	0x176a156ae58348b0,
	0xb6d467a4af63e58d,
	0xf2d0a1e9406aec9d,
	0x57613082c233f007,
	0xfd4d8e9fa5ead0bd,
	0x760b0d22050143a6,
	0xba08e4b738b6829,
	0xbf1f46e83699caf3,
	0x76a780ea967cd710,
	0x7a3ba6f606f665a6,
	0xac89c16725fd3d7f,
	0xd86d68260fd6e479,
	0x5aff01c926fbf29b,
	0x4829ee0716de4c35,
	0xd322787c2bf3394b,
	0x46a03cb44af864ba,
	0xe0bed31f1cb9e6c6,
	0xb3afd37941439089,
	0x90b92d0169a39144,
	0xfe34179dc34f182d,
	0xf2bb5389421657ff,
	0x293a0c2bf9fc6568,
	0x5c4e91e98b02c917,
	0x528047936c9c64b7,
	0xaf2560383d17909,
	0xd5b4a4b2ea3d4ca5,
	0xcfb58fbeaf635d47,
	0x2f5218587fc78769,
	0x9e503382be14186f,
	0x44841df33539b1ea,
	0x97f7ae24e9174548,
	0x1e925507c051e18a,
	0x5065855807b73658,
	0x103970a329ec300c,
	0xa402a18da250bf34,
	0x3485757ea7ed5d97,
	0xb7ab3641fe3dea79,
	0xd0031d27b8b352f7,
	0xc66b36dbc9b344e9,
	0x4fd269fd8e5f0475,
	0x5d55cb471941e52a,
	0xea4eef7a2694763d,
	0x8010d6326b40eabc,
	0xde377ef58485d68b,
	0xb332aafe336eacca,
	0x3fba24704399a363,
	0xcd4f278a67149b9c,
	0xb46e5f29ae10a901,
	0x83cc44bf5a5ffefb,
	0x803e6306563b26de,
	0x805d29286f00f02b,
	0x7539a2019f06397d,
	0xcb7fafc3545836c4,
	0xc79a2bf931d6416b,
	0xe85f325712f4128d,
	0xf062b076752f33ff,
	0xbaae3e3e4a305605,
	0x4cd239ea0c8dc214,
	0x835ca80d72521a90,
	0xec443faf8eb3e4a1,
	0x1ff5f26283efc6c6,
	0x5225fcd6090ec04f,
	0x1facfc5dc1540864,
	0x963a5aceec2c8aaa,
	0xcbdb185b70ab53ba,
	0xe83e14a538d3b494,
	0x58cfb024878d4063,
	0x3e19bf7a317ae3f,
	0xc504d6353cb62f07,
	0x7ce2e98ef360412c,
	0x601900fb4ffbf3a9,
	0xa5a1ffb522d554b4,
	0x606796b83f190476,
	0x1352ca320796a710,
	0x2d89c820f5c353cf,
	0x6a7cb5cf04f59bb7,
	0x9dac9b582d230176,
	0xd05ce263e2d6a9ce,
	0x3fcb626c3f1d7427,
	0xb7fbfbcafd915bb,
	0x83398e40b01aa47d,
	0x323423cfcde2c269,
	0xcb70e7ac7417bf38,
	0x76fd839a1e094f9a,
	0xc93a23eb55ece0ea,
	0x4b56783ccb94539b,
	0xb4b4a3c813d346b5,
	0x46baf44754e0c0c1,
	0x3eecfdbc6db30e37,
	0x7a9e3bdcdc02b390,
	0xe60aedf1a6e222f5,
	0xdbeaa0fe2f8c1fe,
	0xe43a7d712e166bdf,
	0x32560c7a67588a74,
	0x90b166a221898f34,
	0x1852fe624c330f1d,
	0x5eb29c7719af53ba,
	0x53b7a0ff70658b94,
	0x8c97d70a133c9673,
	0x429bd23a4efeeadd,
	0xcc3f10e0f212551,
	0x136f9ac7070f0914,
	0x89c09a3e6f241c57,
	0x2858bd10f13e41b7,
	0x146f70ff3be70cb0,
	0x91a39040f4b6f47f,
	0x294b4e8e20f31127,
	0xc50064ce6551cb89,
	0xc911aa87289cbd2c,
	0xc1a2d5288946f23d,
	0xd7930cf840a79c3b,
	0xd396d24a03c6d982,
	0xc322cee10365790c,
	0x53bf1faf0cf52517,
	0x5bb1f57b0bb131e8,
	0xd17d8ebf3da5475c,
	0x1a44786139efcca,
	0x83ed64e9bcd44eb4,
	0x8c8c4694a54af747,
	0xaf3f0d6fb73c32ed,
	0x69c93fb09f6c47ac,
	0xac80d58fe8ba8f22,
	0x2c1283b654043a66,
	0xa0624c583b0a7f20,
	0x1bb55397b4926431,
	0xc70a4f5ae17c02d5,
	0xb3770eb58f0d2558,
	0x40d4e552014fbff2,
	0x95974b9d7f803594,
	0x2a6a467079b76fbe,
	0xe9f98c4033fe2656,
	0xd9a30874792c8ee8,
	0x876a20af6b41292d,
	0x7fe4754afdff9c32,
	0xb4ad5ac882093298,
	0x8e4b5ac059483870,
	0xe3efbff5b2d5a113,
	0xbca82a42dd96e5a,
	0x6d8e96f5b8e56a9,
	0x5b7b2709ebd9dda9,
	0x2018fa6e04f9ce92,
	0xeca000e8cb440950,
	0xfca82947a67e52b1,
	0x1b35327a49f6d261,
	0x2c19e7792417fc3,
	0xf8fc24541c3b6bd9,
	0xbe67230b027b7e0,
	0xd2aaab031f765a41,
	0x27ebdd8f44c9ab40,
	0xb96747c045d99121,
	0xbe5ddb0efd7a84af,
	0xa8eb1ac99b75788,
	0xd5fe7f03e3abff4a,
	0xb3395eafa88aa67f,
	0xf33c374d736e41cc,
	0x7995c5dc9cbcbe5e,
	0xa8dfd8d37b3ccebc,
	0x3febdd25e1b7fa93,
	0xb3415dbd315ae6af,
	0x8289172b9cced2e2,
	0xd290a23119ea0f2f,
	0xb6df4331a9770722,
	0x2b77e80684a6bfdc,
	0xf197e13488f03f07,
	0x1e3ffa8aa44a03a4,
	0x61ebca0827a6b885,
	0x4939bb8b580c8ba,
	0xdd214064018153da,
	0xd01b6a22b648e604,
	0xc1acd9f551180278,
	0x8945fcdd893a310f,
	0xdcb389ac728f5f4c,
	0x709ec18437f5198b,
	0xfd275a873cc0ea9b,
	0xec7ae37ae39d02db,
	0x6a85764813883142,
	0x9fb95e8cca599392,
	0xf4ea42afc12d154e,
	0x99ad1bdc176163d,
	0xeae4ae6d5c92e2b8,
	0x508df0dcf9f95ede,
	0x60390908b802bdfc,
	0xd0e57d0f8a928585,
	0xc68571ddca6e10b,
	0x81e5dcfd887953e8,
	0x4abb18c948b9e962,
	0x88cd00c4e533e9a3,
	0x7fc76fad5e0ce6e5,
	0xd3189b251dba77ae,
	0x7e23bc6fc8214b8a,
	0xeadaea4753b428d7,
	0xaa80d0564cf20a65,
}
