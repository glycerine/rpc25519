package jsync

import (
	"bytes"
	"testing"
	//"github.com/glycerine/greenpack/msgp"
	"github.com/glycerine/rpc25519/hash"
)

func TestManualMarshalUnmarshalCASIndex(t *testing.T) {
	hasher := hash.NewBlake3()
	hasher.Write([]byte("hello"))
	b3 := hasher.SumString()
	v := NewCASIndex(b3, 9223372036854775807)
	// v := CASIndex{
	// 	Endx: 9223372036854775807,
	// }
	// copy(v.Blake3[:], []byte(b3))
	buf := make([]byte, 64)
	bts, err := v.ManualMarshalMsg(buf[:0])
	if err != nil {
		t.Fatal(err)
	}
	vv("len bts for CAS is '%v'", len(bts))

	v2 := CASIndex{}
	left, err := v2.ManualUnmarshalMsg(bts)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) > 0 {
		t.Errorf("%d bytes left over after UnmarshalMsg(): %q", len(left), left)
	}
	if v2.Endx != v.Endx {
		panic("different")
	}
	if 0 != bytes.Compare(v2.Blake3[:], v.Blake3[:]) {
		panic("different")
	}
}

func BenchmarkManualMarshalMsgCASIndex(b *testing.B) {
	v := CASIndex{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v.ManualMarshalMsg(nil)
	}
}

func BenchmarkAppendMsgCASIndex(b *testing.B) {
	v := CASIndex{}
	bts := make([]byte, 0, v.ManualMsgsize())
	bts, _ = v.ManualMarshalMsg(bts[0:0])
	b.SetBytes(int64(len(bts)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bts, _ = v.ManualMarshalMsg(bts[0:0])
	}
}

func BenchmarkUnmarshalCASIndex(b *testing.B) {
	v := CASIndex{}
	bts, _ := v.ManualMarshalMsg(nil)
	b.ReportAllocs()
	b.SetBytes(int64(len(bts)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := v.ManualUnmarshalMsg(bts)
		if err != nil {
			b.Fatal(err)
		}
	}
}
