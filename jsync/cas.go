package jsync

import (
	"fmt"
	//"github.com/glycerine/greenpack/msgp"
	// "github.com/glycerine/rpc25519/hash"
)

type CASIndex struct {
	Blake3 [56]byte

	Beg int64 // not serialized on disk. computed after read.

	// The first CAS starts at byte offset 0.
	// The next CAS starts at the Endx of the first.
	// so we really only need to store the endx of
	// each entry, since the prior one tells us the
	// beginning file position offset in bytes already.
	// Sure we need to read two entries. We would
	// probably read them anyway, and it seems
	// worth it to fit in a cache line and
	// not store redundant offsets in the index file.
	Endx int64
}

func NewCASIndex(blake3 string, endx int64) (r CASIndex) {
	n := len(blake3)
	// len is 55, so 0-byte terminated always too--
	// which should make the int64 8-byte aligned as well.
	//vv("n = %v", n) // n = 55
	if n > 56 {
		panic(fmt.Sprintf("blake3 string must be <= 56 bytes: %v", n))
	}
	copy(r.Blake3[:], []byte(blake3[:n]))
	r.Endx = endx
	return
}

var zero64 [64]byte

// ManualMarshalMsg is adapted from msgp but does NOT
// provide greenpack / msgpack serz. Instead it is
// custom, manual serization of the two fields so
// that they exactly fit into a single 64-byte cache line.
func (z *CASIndex) ManualMarshalMsg(b []byte) (o []byte, err error) {

	if cap(b) < 64 {
		panic("ManualMarshalMsg must have b with cap >= 64")
	}
	//o = msgp.Require(b, 64)
	//vv("len(o) = %v; cap(o) = %v", len(o), cap(o))
	o = b[:64]
	//o = append(o, zero64[:]...)
	copy(o, []byte(z.Blake3[:]))

	i := z.Endx
	o[56] = byte(i >> 56)
	o[57] = byte(i >> 48)
	o[58] = byte(i >> 40)
	o[59] = byte(i >> 32)
	o[60] = byte(i >> 24)
	o[61] = byte(i >> 16)
	o[62] = byte(i >> 8)
	o[63] = byte(i)

	return
}

// ManualUnmarshalMsg is adapted from msgp but does NOT
// provide greenpack / msgpack serz. Instead it is
// custom, manual serization of the two fields so
// that they exactly fit into a single 64-byte cache line.
func (z *CASIndex) ManualUnmarshalMsg(b []byte) (o []byte, err error) {

	copy(z.Blake3[:56], b[:56])
	z.Endx = (int64(b[56]) << 56) | (int64(b[57]) << 48) |
		(int64(b[58]) << 40) | (int64(b[59]) << 32) |
		(int64(b[60]) << 24) | (int64(b[61]) << 16) |
		(int64(b[62]) << 8) | (int64(b[63]))
	return b[64:], nil
}

// ManualMsgsize
func (z *CASIndex) ManualMsgsize() (s int) {
	return 64
}
