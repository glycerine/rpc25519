package selfcert

// notes on if you wanted to convert an ed25519 key-pair
// from signing into an x25519 key-pair for encryption...
// See  https://words.filippo.io/using-ed25519-keys-for-encryption/

/*
from https://words.filippo.io/using-ed25519-keys-for-encryption/

First, we need to understand the difference between Ed25519 and X25519.
For that I recommend Montgomery curves and their arithmetic by
Craig Costello and Benjamin Smith [ https://eprint.iacr.org/2017/212.pdf ],
which is where I learned most of the underlying mechanics of Montgomery
curves. The high level summary is that the twisted Edwards curve
used by Ed25519 and the Montgomery curve used by X25519 are
birationally equivalent: you can convert points from one to
the other, and they behave the same way. The main difference
is that on Montgomery curves you can use the Montgomery ladder
to do scalar multiplication of x coordinates, which is fast,
constant time, and sufficient for Diffie-Hellman. Points on
the Edwards curve are usually referred to as (x, y),
while points on the Montgomery curve are usually referred to as (u, v).


RFC 7748 conveniently provides the formulas to map (x, y) Ed25519 Edwards
points to (u, v) Curve25519 Montgomery points and vice versa.

(u, v) = ((1+y)/(1-y), sqrt(-486664)*u/x)
(x, y) = (sqrt(-486664)*u/v, (u-1)/(u+1))

So that's what a X25519 public key is: a u coordinate on the Curve25519
Montgomery curve obtained by multiplying the basepoint by a secret scalar,
which is the private key. An Ed25519 public key instead is the compressed
encoding of a (x, y) point on the Ed25519 Edwards curve obtained by
multiplying the basepoint by a secret scalar derived from the private
key. (An Ed25519 private key is hashed to obtained two secrets,
the first is the secret scalar, the other is used elsewhere
in the signature scheme.)

*/

// This next part of the file is from
// https://github.com/perlin-network/noise/blob/master/ecdh.go
/*
MIT License

Copyright (c) 2020 Kenta Iwasaki

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

/*
Unrelated but interesting
- embedded systems may like (from FAQ https://libsodium.gitbook.io/doc/quickstart)
  https://libhydrogen.org/
- embedded with poor counters?
https://github.com/jedisct1/libsodium-xchacha20-siv
"Deterministic/nonce-reuse resistant authenticated
encryption scheme using XChaCha20, implemented on libsodium."
*/

/*
import (
	"crypto/ed25519"
	"crypto/sha512"
	"fmt"

	//"github.com/oasislabs/ed25519/extra/x25519"
	"golang.org/x/crypto/curve25519"
	"math/big"
)

var curve25519P, _ = new(big.Int).SetString("57896044618658097711785492504343953926634992332820282019728792003956564819949", 10)

func ed25519PublicKeyToCurve25519(pk PublicKey) []byte {
	// ed25519.PublicKey is a little endian representation of the y-coordinate,
	// with the most significant bit set based on the sign of the x-coordinate.
	bigEndianY := make([]byte, ed25519.PublicKeySize)
	for i, b := range pk {
		bigEndianY[ed25519.PublicKeySize-i-1] = b
	}
	bigEndianY[0] &= 0b0111_1111

	// The Montgomery u-coordinate is derived through the bilinear map
	//
	//     u = (1 + y) / (1 - y)
	//
	// See https://blog.filippo.io/using-ed25519-keys-for-encryption.
	y := new(big.Int).SetBytes(bigEndianY)
	denom := big.NewInt(1)
	denom.ModInverse(denom.Sub(denom, y), curve25519P) // 1 / (1 - y)
	u := y.Mul(y.Add(y, big.NewInt(1)), denom)
	u.Mod(u, curve25519P)

	out := make([]byte, curve25519.PointSize)
	uBytes := u.Bytes()
	for i, b := range uBytes {
		out[len(uBytes)-i-1] = b
	}

	return out
}

func ed25519PrivateKeyToCurve25519(pk PrivateKey) []byte {
	h := sha512.New()
	h.Write(pk[:curve25519.ScalarSize])
	out := h.Sum(nil)
	return out[:curve25519.ScalarSize]
}

// ECDH transform all Ed25519 points to Curve25519 points and performs a Diffie-Hellman handshake
// to derive a shared key. It throws an error should the Ed25519 points be invalid.
func ECDH(ourPrivateKey PrivateKey, peerPublicKey PublicKey) ([]byte, error) {

	// jea: was using "github.com/oasislabs/ed25519/extra/x25519"
	//  but I switched to "golang.org/x/crypto/curve25519". The
	//  doc was identitcal from the golang.org/x std lib, but
	//  the oasislabs was 4 years ago.
	//shared, err := x25519.X25519(ed25519PrivateKeyToCurve25519(ourPrivateKey), ed25519PublicKeyToCurve25519(peerPublicKey))
	shared, err := curve25519.X25519(ed25519PrivateKeyToCurve25519(ourPrivateKey), ed25519PublicKeyToCurve25519(peerPublicKey))
	if err != nil {
		return nil, fmt.Errorf("could not derive a shared key: %w", err)
	}

	return shared, nil
}
*/

// similar utilities
//from https://github.com/oasisprotocol/ed25519/blob/master/extra/x25519/x25519.go

// Copyright (c) 2016 The Go Authors. All rights reserved.
// Copyright (c) 2019 Oasis Labs Inc.  All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//   * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//    * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//    * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

// Package x25519 provides an implementation of the X25519 function, which
// performs scalar multiplication on the elliptic curve known as Curve25519.
// See RFC 7748.
//package x25519
/*
import (
	"crypto/sha512"
	"crypto/subtle"
	"fmt"

	xcurve "golang.org/x/crypto/curve25519"

	"github.com/oasisprotocol/ed25519"
	"github.com/oasisprotocol/ed25519/internal/curve25519"
	"github.com/oasisprotocol/ed25519/internal/ge25519"
	"github.com/oasisprotocol/ed25519/internal/modm"
)

const (
	// ScalarSize is the size of the scalar input to X25519.
	ScalarSize = 32
	// PointSize is the size of the point input to X25519.
	PointSize = 32
)

// Basepoint is the canonical Curve25519 generator.
var Basepoint []byte

var basePoint = [32]byte{9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

// ScalarMult sets dst to the product in*base where dst and base are the x
// coordinates of group points and all values are in little-endian form.
//
// Deprecated: when provided a low-order point, ScalarMult will set dst to all
// zeroes, irrespective of the scalar. Instead, use the X25519 function, which
// will return an error.
func ScalarMult(dst, in, base *[32]byte) {
	xcurve.ScalarMult(dst, in, base)
}

// ScalarBaseMult sets dst to the product in*base where dst and base are
// the x coordinates of group points, base is the standard generator and
// all values are in little-endian form.
//
// It is recommended to use the X25519 function with Basepoint instead, as
// copying into fixed size arrays can lead to unexpected bugs.
func ScalarBaseMult(dst, in *[32]byte) {
	// ED25519_FN(curved25519_scalarmult_basepoint) (curved25519_key pk, const curved25519_key e)
	var (
		ec              [32]byte
		s               modm.Bignum256
		p               ge25519.Ge25519
		yplusz, zminusy curve25519.Bignum25519
	)

	// clamp
	copy(ec[:], in[:])
	ec[0] &= 248
	ec[31] &= 127
	ec[31] |= 64

	modm.ExpandRaw(&s, ec[:])

	// scalar * basepoint
	ge25519.ScalarmultBaseNiels(&p, &ge25519.NielsBaseMultiples, &s)

	// u = (y + z) / (z - y)
	curve25519.Add(&yplusz, p.Y(), p.Z())
	curve25519.Sub(&zminusy, p.Z(), p.Y())
	curve25519.Recip(&zminusy, &zminusy)
	curve25519.Mul(&yplusz, &yplusz, &zminusy)
	curve25519.Contract(dst[:], &yplusz)

	s.Reset()
	for i := range ec {
		ec[i] = 0
	}
}

// X25519 returns the result of the scalar multiplication (scalar * point),
// according to RFC 7748, Section 5. scalar, point and the return value are
// slices of 32 bytes.
//
// scalar can be generated at random, for example with crypto/rand. point should
// be either Basepoint or the output of another X25519 call.
//
// If point is Basepoint (but not if it's a different slice with the same
// contents) a precomputed implementation might be used for performance.
func X25519(scalar, point []byte) ([]byte, error) {
	// Outline the body of function, to let the allocation be inlined in the
	// caller, and possibly avoid escaping to the heap.
	var dst [32]byte
	return x25519(&dst, scalar, point)
}

func x25519(dst *[32]byte, scalar, point []byte) ([]byte, error) {
	var in [32]byte
	if l := len(scalar); l != 32 {
		return nil, fmt.Errorf("bad scalar length: %d, expected %d", l, 32)
	}
	if l := len(point); l != 32 {
		return nil, fmt.Errorf("bad point length: %d, expected %d", l, 32)
	}
	copy(in[:], scalar)
	if &point[0] == &Basepoint[0] {
		checkBasepoint()
		ScalarBaseMult(dst, &in)
	} else {
		var base, zero [32]byte
		copy(base[:], point)
		ScalarMult(dst, &in, &base)
		if subtle.ConstantTimeCompare(dst[:], zero[:]) == 1 {
			return nil, fmt.Errorf("bad input point: low order point")
		}
	}
	return dst[:], nil
}

func checkBasepoint() {
	if subtle.ConstantTimeCompare(Basepoint, []byte{
		0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}) != 1 {
		panic("curve25519: global Basepoint value was modified")
	}
}

// EdPrivateKeyToX25519 converts an Ed25519 private key into a corresponding
// X25519 private key such that the resulting X25519 public key will equal
// the result from EdPublicKeyToX25519.
func EdPrivateKeyToX25519(privateKey ed25519.PrivateKey) []byte {
	h := sha512.New()
	_, _ = h.Write(privateKey[:32])
	digest := h.Sum(nil)
	h.Reset()

	digest[0] &= 248
	digest[31] &= 127
	digest[31] |= 64

	dst := make([]byte, ScalarSize)
	copy(dst, digest)

	return dst
}

func curve25519One(z *curve25519.Bignum25519) {
	z.Reset()
	z[0] = 1
}

func edwardsToMontgomeryX(outX, y *curve25519.Bignum25519) {
	// We only need the x-coordinate of the curve25519 point, which I'll
	// call u. The isomorphism is u=(y+1)/(1-y), since y=Y/Z, this gives
	// u=(Y+Z)/(Z-Y). We know that Z=1, thus u=(Y+1)/(1-Y).
	var oneMinusY curve25519.Bignum25519
	curve25519One(&oneMinusY)
	curve25519.Sub(&oneMinusY, &oneMinusY, y)
	curve25519.Recip(&oneMinusY, &oneMinusY)

	curve25519One(outX)
	curve25519.Add(outX, outX, y)

	curve25519.Mul(outX, outX, &oneMinusY)
}

// EdPublicKeyToX25519 converts an Ed25519 public key into the X25519 public
// key that would be generated from the same private key.
func EdPublicKeyToX25519(publicKey ed25519.PublicKey) ([]byte, bool) {
	var A ge25519.Ge25519
	if !ge25519.UnpackVartime(&A, publicKey[:]) {
		return nil, false
	}

	// A.Z = 1 as a postcondition of UnpackNegativeVartime.
	var x curve25519.Bignum25519
	edwardsToMontgomeryX(&x, A.Y())
	dst := make([]byte, PointSize)
	curve25519.Contract(dst, &x)

	return dst, true
}

func init() {
	Basepoint = basePoint[:]
}
*/
