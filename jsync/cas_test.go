package jsync

import (
	"fmt"
	"os"
	//"bytes"
	"testing"
	//"github.com/glycerine/greenpack/msgp"
	"github.com/glycerine/rpc25519/hash"
)

func TestManualMarshalUnmarshalCASIndexEntry(t *testing.T) {
	hasher := hash.NewBlake3()
	hasher.Write([]byte("hello"))
	b3 := hasher.SumString()
	v := NewCASIndexEntry(b3, 9223372036854775807)
	// v := CASIndexEntry{
	// 	Endx: 9223372036854775807,
	// }
	// copy(v.Blake3[:], []byte(b3))
	buf := make([]byte, 64)
	bts, err := v.ManualMarshalMsg(buf[:0])
	if err != nil {
		t.Fatal(err)
	}
	vv("len bts for CAS is '%v'", len(bts))

	v2 := CASIndexEntry{}
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
	//if 0 != bytes.Compare(v2.Blake3[:], v.Blake3[:]) {
	if v2.Blake3 != v.Blake3 {
		panic("different")
	}
}

func BenchmarkManualMarshalMsgCASIndexEntry(b *testing.B) {
	v := CASIndexEntry{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v.ManualMarshalMsg(nil)
	}
}

func BenchmarkAppendMsgCASIndexEntry(b *testing.B) {
	v := CASIndexEntry{}
	bts := make([]byte, 0, v.ManualMsgsize())
	bts, _ = v.ManualMarshalMsg(bts[0:0])
	b.SetBytes(int64(len(bts)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bts, _ = v.ManualMarshalMsg(bts[0:0])
	}
}

func BenchmarkUnmarshalCASIndexEntry(b *testing.B) {
	v := CASIndexEntry{}
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

func Test_0909_NewCASIndex(t *testing.T) {
	path := "test0909_cas_data"
	pathIndex := path + ".index"

	os.Remove(path)
	os.Remove(pathIndex)

	preAllocDataSz := int64(128 << 20)
	idx, err := NewCASIndex(path, 2, preAllocDataSz)
	panicOn(err)
	datas := make([][]byte, 3)

	var seed [32]byte
	seed[0] = 3
	rng := newPRNG(seed)
	var keys []string
	var lens []int
	for i := range datas {
		// random size in [20, 100]
		sz := 20 + rng.pseudoRandNonNegInt64()%81
		datas[i] = make([]byte, sz)
		//rng.cha8.Read(datas[i])

		// easy to expect and verify data during Get()
		datas[i][0] = byte(i)
		key := hash.Blake3OfBytesString(datas[i])
		keys = append(keys, key)
		lens = append(lens, int(sz))
	}

	newCount, err := idx.Append(datas)
	panicOn(err)
	vv("saw newCount = %v", newCount)

	nTot, nMem := idx.TotMem()
	vv("nTot=%v; nMem=%v", nTot, nMem)
	if nTot != int64(len(keys)) {
		panic(fmt.Sprintf("missing some key(s): nTot=%v but len(keys)=%v", nTot, len(keys))) // panic: missing some key(s): nTot=15 but len(keys)=3 [recovered, repanicked]
	}
	for j, key := range keys {
		vv("confirm j=%v; key='%v'", j, key)
		data, ok := idx.Get(key)
		if !ok {
			panic(fmt.Sprintf("stored key '%v' but now its gone", key))
		}
		if len(data) != lens[j] {
			panic("bad len data")
		}
		if data[0] != byte(j) {
			panic("bad data[0]")
		}
	}
	// and test non-keys too
	for _, key := range keys {
		b := byte(key[len(key)-1])
		b++
		key = key[:len(key)-1] + string(b)
		data, ok := idx.Get(key)
		if ok {
			panic(fmt.Sprintf("key '%v' with last byte incremented should not be present; got data[0] = '%v' with len(data)=%v", key, data[0], len(data)))
		}
	}

}
