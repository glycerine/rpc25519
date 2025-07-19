package jsync

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"
	//"github.com/glycerine/greenpack/msgp"
	"github.com/glycerine/rpc25519/hash"
)

type CASIndex struct {
	path string

	// must keep index in memory
	index sync.Map // blake3 -> CASIndexEntry

	// can reload daa from fdData if need be,
	// so can be trimmed.
	mapData           sync.Map // blake3 -> []byte data
	mapDataTotalBytes int64

	hasher  *hash.Blake3
	workbuf []byte
	w       int64 // how much of workbuf used?

	fdData  *os.File
	fdIndex *os.File
}

func NewCASIndex(path string) (s *CASIndex, err error) {
	s = &CASIndex{
		path:    path,
		workbuf: make([]byte, 0, 1<<20),
		hasher:  hash.NewBlake3(),
	}
	s.fdData, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	panicOn(err)
	s.fdIndex, err = os.OpenFile(path+".index", os.O_RDWR|os.O_CREATE, 0644)
	panicOn(err)
	s.loadDataAndIndex()
	return
}

func (s *CASIndex) loadDataAndIndex() {

	// just seek to end for appending for now.
	// later TODO: read fdIndex and check it matches fdData.
	// For now we just confirm it is the right size.
	s.fdIndex.Seek(0, 2)
	fi, err := s.fdIndex.Stat()
	panicOn(err)
	indexSz := fi.Size()
	var foundEntries int64

	defer func() {
		if indexSz/64 != foundEntries {
			panic(fmt.Sprintf("bad: indexSz(%v)/64=%v != foundEntries(%v)", indexSz, indexSz/64, foundEntries))
		}
		vv("good: indexSz(%v)/64 == foundEntries(%v)", indexSz, foundEntries)
	}()

	beg := curpos(s.fdData)
	vv("top of loadDataAndIndex: beg = %v", beg)
	for i := int64(0); ; i++ {
		nr, err := io.ReadFull(s.fdData, s.workbuf[:64])
		_ = nr
		vv("i=%v; nr=%v", i, nr)
		switch err {
		case io.EOF:
			vv("no bytes read on first io.ReadFull(s.fdData) for header")
			return
		case io.ErrUnexpectedEOF:
			// fewer than 64 bytes read
			panic(fmt.Sprintf("ErrUnexpectedEOF after nr=%v, corrupted path? path='%v'", nr, s.path))
		case nil:
			// full 64 bytes read into s.workbuf[:64]
			e := &CASIndexEntry{}
			_, err = e.ManualUnmarshalMsg(s.workbuf[:64])
			panicOn(err)
			foundEntries++
			e.Beg = beg
			vv("read back from path '%v' gives e = '%#v'", s.path, e)

			endx := e.Endx
			sz := endx - beg - 64
			var nr2 int
			nr2, err = io.ReadFull(s.fdData, s.workbuf[:sz])
			vv("inner read of data: sz=%v; endx=%v; beg=%v; nr2=%v; err = '%v'", sz, endx, beg, nr2, err)
			_ = nr2
			switch err {
			case io.EOF:
				vv("no data bytes read io.EOF for data ")
				return
			case io.ErrUnexpectedEOF:
				// fewer than sz data bytes read
				panic(fmt.Sprintf("ErrUnexpectedEOF after nr2=%v, corrupted path? path='%v'", nr2, s.path))
			case nil:
				// sz bytes were just read into s.workbuf[:sz]
				data := s.workbuf[:sz]
				b3 := string(e.Blake3[:55])
				s.addToMapData(b3, data)
				s.index.LoadOrStore(b3, e)
			}
			beg = endx
			cur := curpos(s.fdData)
			if cur != beg {
				panic(fmt.Sprintf("sanity check failed, curpos(s.fdData)=%v != beg(%v)", cur, beg))
			}

			// TODO: also read path.index and
			// a) compare same, to detect a short write or corruption
			// b) verify the index entry matches
			//    what is in full path data storage.
			// c) check again our offset endx is correct.
		}
	}
}

func (s *CASIndex) Append(data [][]byte) (err error) {
	for _, by := range data {
		if len(by) == 0 {
			panic("dont try to Append an empty []byte")
			continue
		}
		s.hasher.Reset()
		s.hasher.Write(by)
		b3 := s.hasher.SumString()

		// dedup, don't write if already present.
		_, already := s.mapData.Load(b3)
		if already {
			continue
		}

		// create index entry
		beg := curpos(s.fdData)
		endx := beg + int64(len(by)) + 64
		e := NewCASIndexEntry(b3, endx)

		// store data to memory
		s.addToMapData(b3, by)

		// write new index entry to memory
		e.Beg = beg
		s.index.LoadOrStore(b3, e)
		vv("writing to disk e = '%#v'", e)

		// write new index entry to disk.
		// flush workbuf first if we are out
		// of workbuf space...
		if int64(len(s.workbuf))-s.w < 64 {
			_, err = s.fdIndex.Write(s.workbuf[:s.w])
			panicOn(err)
			if err != nil {
				return
			}
			s.w = 0
		}

		var ebts []byte
		ebts, err = e.ManualMarshalMsg(s.workbuf[s.w:s.w])
		panicOn(err)
		if err != nil {
			return
		}
		s.w += 64

		// store data to disk.
		// a) write 64 byte header first
		_, err = s.fdData.Write(ebts)
		panicOn(err)
		if err != nil {
			return
		}
		// b) write actual data by.
		_, err = s.fdData.Write(by)
		panicOn(err)
		if err != nil {
			return
		}

		// sanity check
		endx2 := curpos(s.fdData)
		if endx2 != endx {
			panic(fmt.Sprintf("endx2(%v) != endx(%v): bad computation of endx", endx2, endx))
		}
	}
	// flush any remaining index to disk
	if s.w > 0 {
		_, err = s.fdIndex.Write(s.workbuf[:s.w])
		panicOn(err)
		if err != nil {
			return
		}
		s.w = 0
	}
	err = s.fdData.Sync()
	err2 := s.fdIndex.Sync()
	panicOn(err)
	panicOn(err2)
	return nil
}

func (s *CASIndex) addToMapData(b3 string, data []byte) {
	s.mapDataTotalBytes += int64(len(data))
	actual, loaded := s.mapData.LoadOrStore(b3, data)
	// basic sanity, can be commented once working.
	if !loaded {
		dataPrev, ok := actual.([]byte)
		if !ok {
			panic(fmt.Sprintf("only []byte should be stored in m, not %T", actual))
		}
		if 0 != bytes.Compare(data, dataPrev) {
			panic(fmt.Sprintf("data(len %v) != dataPrev(len %v), bad hashing somewhere?", len(data), len(dataPrev)))
		}
	}
}

type CASIndexEntry struct {
	//Blake3 [56]byte
	Blake3 string

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

func NewCASIndexEntry(blake3str string, endx int64) (r CASIndexEntry) {
	n := len(blake3str)
	// len is 55, so 0-byte terminated always too--
	// which should make the int64 8-byte aligned as well.
	//vv("n = %v", n) // n = 55
	if n > 56 {
		panic(fmt.Sprintf("blake3 string must be <= 56 bytes: %v", n))
	}
	//copy(r.Blake3[:], []byte(blake3str[:n]))
	r.Blake3 = blake3str
	r.Endx = endx
	return
}

// ManualMarshalMsg is adapted from msgp but does NOT
// provide greenpack / msgpack serz. Instead it is
// custom, manual serization of the two fields so
// that they exactly fit into a single 64-byte cache line.
func (z *CASIndexEntry) ManualMarshalMsg(b []byte) (o []byte, err error) {

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
func (z *CASIndexEntry) ManualUnmarshalMsg(b []byte) (o []byte, err error) {

	z.Blake3 = string(b[:55])
	//copy(z.Blake3[:56], b[:56])
	z.Endx = (int64(b[56]) << 56) | (int64(b[57]) << 48) |
		(int64(b[58]) << 40) | (int64(b[59]) << 32) |
		(int64(b[60]) << 24) | (int64(b[61]) << 16) |
		(int64(b[62]) << 8) | (int64(b[63]))
	return b[64:], nil
}

// ManualMsgsize
func (z *CASIndexEntry) ManualMsgsize() (s int) {
	return 64
}
