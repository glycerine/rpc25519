package jsync

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/glycerine/rpc25519/hash"
	"github.com/glycerine/rpc25519/jsync/sparsified"
	"github.com/klauspost/compress/s2"
)

// CASIndex and CASIndexEntry provide a very
// simple Content-Addressable-Store backed
// by two files on disk. The data file is
// specified by cfg.Path, and it holds
// the (compressed) chunks (blobs) of data.
//
// The pathIndex for the index just includes
// the hashes and location of the blobs in
// the data file, so that it can be loaded
// into memory without having to scan
// through the full data, allowing lazy
// data loading.
//
// The mapData acts as a memory cache for
// disk chunks, and the MaxMemBlob controls how
// many chunks (blobs) are kept in memory.
// Random eviction is used when the memory
// cache count is maxed out.
//
// A complete index of disk is retained in
// memory using a sync.Map. The Get and
// Append methods are goroutine safe.
type CASIndex struct {
	mut sync.Mutex

	path      string
	pathIndex string

	// must keep index in memory
	index sync.Map // blake3 -> *CASIndexEntry

	// can reload data from fdData if need be,
	// so mapData can (and will) be trimmed when
	// we add more than maxBlobs.
	// mapData holds uncompressed data in memory.
	// data on disk is always s2.Encode() compressed.
	mapData           sync.Map // blake3 -> []byte data
	mapDataTotalBytes int64
	maxMemBlob        int64
	nMemBlob          int64 // len(mapData) is kept <= maxMemBlob

	preAllocSz int64

	// nKnownBlob == len(index), total number of known blobs
	// (in memory in index, and same on disk
	// in data path or index path).
	nKnownBlob int64

	// memkeys for the data that is in memory rather than
	// just on disk.
	memkeys []string

	hasher  *hash.Blake3
	workbuf []byte
	w       int64 // how much of workbuf used?

	fdData  *os.File
	fdIndex *os.File

	// for random cache evictions
	rng *prng
}

type CASIndexConfig struct {
	Path       string
	MaxMemBlob int64
	PreAllocSz int64
	VerifyData bool
}

// maxMemBlob is a count of blobs to cache.
// preAllocSz is the bytes to pre-allocate when
// path is new.
// If verifyData, we load the full data file and
// check it against the index. Otherwise we only
// load the index.
func NewCASIndex(cfg *CASIndexConfig) (s *CASIndex, err error) {
	if cfg.MaxMemBlob < 0 {
		panic(fmt.Sprintf("cfg.MaxMemBlob(%v) must be >= 0", cfg.MaxMemBlob))
	}
	var seed [32]byte
	s = &CASIndex{
		preAllocSz: cfg.PreAllocSz,
		maxMemBlob: cfg.MaxMemBlob,
		hasher:     hash.NewBlake3(),
		rng:        newPRNG(seed),
		path:       cfg.Path,
		pathIndex:  cfg.Path + ".index",

		workbuf: make([]byte, 1<<20),
		// exercise the "unused" logic, but always
		// must be a multiple of 64.
		//workbuf: make([]byte, 128*1024),

	}
	isNew := !fileExists(s.path)
	s.fdData, err = os.OpenFile(s.path, os.O_RDWR|os.O_CREATE, 0644)
	panicOn(err)
	s.fdIndex, err = os.OpenFile(s.pathIndex, os.O_RDWR|os.O_CREATE, 0644)
	panicOn(err)

	if isNew && s.preAllocSz > 0 {
		_, err = sparsified.Fallocate(s.fdData, sparsified.FALLOC_FL_KEEP_SIZE, 0, s.preAllocSz)
		panicOn(err)
		err = s.fdData.Sync()
		panicOn(err)

		// target of 8K / 64 = 128, so we expect
		// our index to be about 1/128 of the data.
		// If data prealloc is 8GB, then index prealloc will be 64MB.
		preAllocIndexSz := s.preAllocSz / 128
		_, err = sparsified.Fallocate(s.fdIndex, sparsified.FALLOC_FL_KEEP_SIZE, 0, preAllocIndexSz)
		panicOn(err)
		err = s.fdIndex.Sync()
		panicOn(err)
	}

	var indexSz int64
	indexSz, err = s.loadIndex()
	panicOn(err)

	if cfg.VerifyData {
		// grabs locks. fine b/c we don't have them.
		err = s.verifyDataAgainstIndex(indexSz) // assumes loadIndex already called.
		panicOn(err)
		//alwaysPrintf("good: no error from verifyDataAgainstIndex")
	}
	// can also be lazy and just skip reading data for fast start.

	// sanity check our workbuf is big enough
	if len(s.workbuf) < 128*1024 {
		panic(fmt.Sprintf("s.workbuf must be at least 128K, preferrably 1MB"))
	}
	return
}

// indexSz is the byte size of the pathIndex file.
// called as part of NewCASIndex so no locking needed.
func (s *CASIndex) loadIndex() (indexSz int64, err error) {
	//vv("top of loadIndex")

	s.fdIndex.Seek(0, 0)
	fi, err := s.fdIndex.Stat()
	panicOn(err)
	indexSz = fi.Size()
	if indexSz == 0 {
		vv("warning: empty index path '%v'", s.pathIndex)
		return
	}
	var foundIndexEntries int64

	defer func() {
		if indexSz/64 != foundIndexEntries {
			panic(fmt.Sprintf("loadIndex bad: indexSz(%v)/64=%v != foundIndexEntries(%v)", indexSz, indexSz/64, foundIndexEntries))
		}
		//vv("loadIndex good: indexSz(%v)/64 == foundIndexEntries(%v); s.nKnownBlob=%v", indexSz, foundIndexEntries, s.nKnownBlob)
	}()

	var beg int64
	var nr int
	var doneAfterThisRead bool
	for i := int64(0); !doneAfterThisRead; i++ {

		// read a batch of up to 1MB/64 == 16384 entries at once
		nr, err = io.ReadFull(s.fdIndex, s.workbuf)
		_ = nr
		//vv("loadIndex loop: i=%v; nr=%v; len(s.workbuf)=%v; pathIndex='%v'; indexSz = %v", i, nr, len(s.workbuf), s.pathIndex, indexSz)
		switch err {
		case io.EOF:
			//vv("no bytes read on first io.ReadFull(s.fdIndex)")
			err = nil
			return
		case io.ErrUnexpectedEOF:
			//vv("nr=%v fewer than 1MB bytes read, typical last read.", nr)
			if nr == 0 {
				err = nil
				return
			}
			doneAfterThisRead = true
			rem := nr % 64
			if rem != 0 {
				panic(fmt.Sprintf("how do deal with torn read (rem=%v) at pos %v of path '%v'? nr=%v, len(s.workbuf)=%v", rem, curpos(s.fdIndex), s.pathIndex, nr, len(s.workbuf)))
			}
			err = nil
			fallthrough
		case nil:
			//vv("err == nil case; nr=%v; doneAfterThisRead=%v", nr, doneAfterThisRead)
			// full 1MB bytes read into s.workbuf, or
			// fallthough from shorter read.
			if nr == 0 {
				panic("logic error, should never happen nr == 0 here")
			}
			buf := s.workbuf[:nr]
			rem2 := nr % 64
			if rem2 != 0 {
				panic(fmt.Sprintf("loadIndex error: how do deal with torn read nr = %v; (rem2=%v) at curpos now %v of path '%v'? We can only read in multiples of 64 since our index is constructed of 64 byte records.", nr, rem2, curpos(s.fdIndex), s.pathIndex))
			}

			nentry := nr / 64
			//vv("netry = %v", nentry)
			es := make([]CASIndexEntry, nentry)
			for j := range es {
				e := &es[j]
				_, err = e.ManualUnmarshalMsg(buf[j*64 : j*64+64])
				panicOn(err)
				foundIndexEntries++
				e.Beg = beg
				//vv("read back from index path '%v' gives e = '%#v'", s.pathIndex, e)
				endx := beg + int64(e.Clen)
				//sz := endx - beg - 64 // data payload size (but only in fdData, not in fdIndex)
				beg = endx
				_, already := s.index.LoadOrStore(e.Blake3, e)
				if already {
					panic(fmt.Sprintf("initial load of index '%v' sees duplicated entry! bad, should not happen! entry='%#v' at j=%v; i = %v", s.pathIndex, e, j, i))
				}
				//vv("added to s.index e = '%v'", e)
			}
			s.nKnownBlob += int64(nentry)
		}
	}
	return
}

// called as part of NewCASIndex so no locking needed.
// Assumes that loadIndex has already been called.
// indexSz < 0 means don't compare against total
// index size bytes on disk, but do check entries.
func (s *CASIndex) verifyDataAgainstIndex(indexSz int64) (err error) {

	//panicOn(s.diagnosticDisplayData())

	var foundDataEntries int64

	defer func() {
		if indexSz >= 0 {
			if indexSz/64 != foundDataEntries {
				panic(fmt.Sprintf("bad: indexSz(%v)/64=%v != foundDataEntries(%v)", indexSz, indexSz/64, foundDataEntries))
			}
			//vv("good: indexSz(%v)/64 == foundDataEntries(%v)", indexSz, foundDataEntries)
		}
	}()

	// load of data... for verifying against index.
	// would skip in prod unless trying to detect corruption.
	_, err = s.fdData.Seek(0, 0)
	panicOn(err)
	var beg int64

	fi, err := s.fdData.Stat()
	panicOn(err)
	dataSz := fi.Size()
	if dataSz == 0 {
		//vv("warning: empty data path '%v'", s.path)
		return
	}

	var nr int
	var totr int
	var unused int64
	var consumed int64
	var doneAfterThisRead bool
iloop:
	for i := int64(0); !doneAfterThisRead; i++ {
		// try to read a bunch of records en-mass, to avoid
		// too many syscalls.
		datapos := curpos(s.fdData)
		nr, err = io.ReadFull(s.fdData, s.workbuf[unused:])
		totr += nr
		_ = totr
		//vv("i=%v; unused=%v; nr=%v; totr=%v", i, unused, nr, totr)
		switch err {
		default:
			panicOn(err)
		case io.EOF:
			err = nil
			//vv("no bytes read on i=%v io.ReadFull(s.fdData) read of header; must be done.", i)
			return
		case io.ErrUnexpectedEOF:
			// fewer than 1MB - unused bytes read, okay
			// to use s.workbuf[:unused+nr)
			//vv("setting doneAfterThisRead true")
			doneAfterThisRead = true
			fallthrough
		case nil:
			// full 1MB - unused bytes read into s.workbuf,
			// okay to use s.workbuf[:unused+nr]

			// two indexes into s.workbuf
			avail := unused + int64(nr)
			consumed = 0

			if avail < 64 {
				panic(fmt.Sprintf("len(avail)=%v, not enough for an index entry even. data path='%v'; totr=%v; dataSz=%v", avail, s.path, totr, dataSz))
			}
			for avail > consumed+64 {
				e := &CASIndexEntry{}
				_, err = e.ManualUnmarshalMsg(s.workbuf[consumed : consumed+64])
				panicOn(err)
				e.Beg = beg
				//vv("at datapos(%v) + consumed(%v), we read in e = '%v'", datapos, consumed, e)
				endx := beg + int64(e.Clen)
				sz := int64(e.Clen - 64)
				if avail < consumed+64+sz {
					// we have a torn read, read again if we can
					if doneAfterThisRead {
						panic(fmt.Sprintf("endx=%v; e.Beg=%v; sz=%v; torn read not recoverable. avail=%v < 64+sz(%v). corrupt/truncated data file path = '%v'?? datapos=%v", endx, e.Beg, sz, avail, sz, s.path, datapos))
					} else {
						unused = avail - consumed
						//vv("unused(%v) = avail(%v) - consumed(%v)", unused, avail, consumed)
						copy(s.workbuf[:unused], s.workbuf[consumed:avail])
						continue iloop
					}
				}
				// INVAR: avail >= consumed + 64 + sz so we can
				// process both header and data together now.
				consumed += 64
				// s.addToDataMap will make a copy of data,
				// so we don't want to make an extra copy here.
				data := s.workbuf[consumed : consumed+sz]
				//vv("data = '%v'", string(data))
				consumed += sz
				unused = avail - consumed

				//vv("updating beg %v -> %v", beg, e.Endx)
				beg = endx // make beg ready to read next header

				foundDataEntries++

				// check against index:
				//vv("read back from path '%v' gives e = '%#v'", s.path, e)
				prior, already := s.index.LoadOrStore(e.Blake3, e)
				// assert our data path and indexPath are in sync;
				// and thus index should already have it every time.
				if !already {
					panic(fmt.Sprintf("s.index was missing '%v' seen in path '%v': why was it not in the pathIndex '%v'", e, s.path, s.pathIndex))
				}
				first := prior.(*CASIndexEntry)
				if !e.Equal(first) {
					panic(fmt.Sprintf("data path '%v' has different CASIndexEntry that indexPath '%v' at i=%v; e(%v) != first(%v)", s.path, s.pathIndex, i, e, first))
				}
				s.addToMapData(e.Blake3, data)
			} // end for avail > consumed+64
			if consumed < avail {
				unused = avail - consumed
				//vv("consumed(%v) < avail(%v), so transferring unused tail(%v) to beginning...", consumed, avail, unused)
				copy(s.workbuf[:unused], s.workbuf[consumed:avail])
			}
		} // end switch err
	} // for i
	return nil
}

func (s *CASIndex) Get(b3 string) (data []byte, ok bool) {
	// get the index, do we have it at all?
	entry, have := s.index.Load(b3)
	if !have {
		return // nope
	}
	// we have data. is it in memory, or only on disk?
	by, already := s.mapData.Load(b3)
	if already {
		// mapData cache hit. its in memory, just serve it.
		ok = true
		data = by.([]byte)
		return
	}
	// INVAR: have to go to disk to get it.

	// lock so we have exclusive access to fdData
	s.mut.Lock()
	defer s.mut.Unlock()

	e := entry.(*CASIndexEntry)
	_, err := s.fdData.Seek(e.Beg, 0)
	panicOn(err)
	sz := e.Clen
	if sz <= 64 {
		panic(fmt.Sprintf("sz must be > 64 to store header plus at least 1 bytes of data; sz = %v", sz))
	}

	var nr int
	nr, err = io.ReadFull(s.fdData, s.workbuf[:sz])
	_ = nr
	//vv("nr=%v; sz = %v", nr, sz)
	switch err {
	case io.EOF:
		panic(fmt.Sprintf("error corrupt path? could not load dataPath '%v' at [%v, %v) of size %v: got EOF", s.path, e.Beg, e.Beg+int64(e.Clen), sz))
		return
	case io.ErrUnexpectedEOF:
		// fewer than sz (header + blob) bytes read
		panic(fmt.Sprintf("ErrUnexpectedEOF after nr=%v, trying to read sz = %v at pos %v, corrupted path? path='%v'", nr, sz, e.Beg, s.path))
	case nil:
		// full sz bytes read into s.workbuf[:sz]
		// confirm the header matches (in first 64 bytes)
		e2 := &CASIndexEntry{}
		_, err = e2.ManualUnmarshalMsg(s.workbuf[:64])
		panicOn(err)
		if !e2.Equal(e) {
			err = fmt.Errorf("error data path '%v' out of correspondence with in memory index entry; at data path [%v, %v) for blake3 hash key '%v'; e2='%v'; e='%v'", s.path, e.Beg, e.Beg+int64(e.Clen), e.Blake3, e2, e)
		}
		// everything after the header is our data payload.

		// on disk data is s2 compressed; always.
		data, err = s2.Decode(nil, s.workbuf[64:sz])
		panicOn(err)
		//vv("doing s2 decompression: %v -> %v", sz-64, data)

		if e.Ulen != int32(len(data)) {
			panic(fmt.Sprintf("Get sees wrong size Ulen=%v but len uncompressed data = %v", e.Ulen, len(data)))
		}
		ok = true

		// cache it (uncompressed)
		s.addToMapData(e.Blake3, data)
		return

	default:
		panic(fmt.Sprintf("should be impossible; err = '%v' on Get from data path '%v' at [%v, %v); nr=%v, sz = %v", err, s.path, e.Beg, e.Beg+int64(e.Clen), nr, sz))
	}

	return
}

func (s *CASIndex) Append(data [][]byte) (newCount int64, err error) {
	s.mut.Lock()
	defer s.mut.Unlock()

	// make sure we are appending to the ends of the files.
	_, err = s.fdIndex.Seek(0, 2)
	panicOn(err)
	_, err = s.fdData.Seek(0, 2)
	panicOn(err)

	for _, by := range data {
		if len(by) == 0 {
			panic("dont try to Append an empty []byte")
			continue
		}
		s.hasher.Reset()
		s.hasher.Write(by)
		b3 := s.hasher.SumString()

		// dedup, don't write if already present.
		_, alreadyData := s.mapData.Load(b3)

		// assert that s.index agrees.
		_, alreadyIndex := s.index.Load(b3)
		if alreadyIndex != alreadyData {
			panic(fmt.Sprintf("alreadyIndex:'%v' but already:'%v' for data true. b3='%v'", alreadyIndex, alreadyData, b3))
		}

		if alreadyData {
			//vv("already have b3='%v' so ignoring", b3)
			continue
		}
		newCount++
		s.nKnownBlob++

		// s2 compress, always.
		ulen := int32(len(by)) // uncompressed length.
		s2by := s2.Encode(nil, by)
		//vv("s2.Encode: input len %v -> %v", len(by), len(s2by))
		// (the by slice still has the uncompressed data).

		clen := int32(len(s2by)) + 64

		// create index entry
		beg := curpos(s.fdData)
		endx := beg + int64(clen)
		e := NewCASIndexEntry(b3, beg, clen, ulen)

		// store data to memory (uncompressed)
		s.addToMapData(b3, by) // makes copy of by

		// write new index entry to memory
		e.Beg = beg
		s.index.LoadOrStore(b3, e)
		//vv("writing to disk e = '%#v'", e)

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
		// b) write actual data
		_, err = s.fdData.Write(s2by)
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
	return

} // end Append

// mut should be held or can be called during NewCASIndex.
// we assume exclusive access to s.
func (s *CASIndex) addToMapData(b3 string, data []byte) {

	//defer func() {
	//	vv("at end of addToMapData, s.nMemBlob=%v", s.nMemBlob)
	//}()
	if s.maxMemBlob == 0 {
		// mostly for testing, never cache anything.
		return
	}
	mycp := append([]byte{}, data...)

	var previous any
	previous, already := s.mapData.LoadOrStore(b3, mycp)
	// basic sanity, can be commented once working.
	if already {
		dataPrev, ok := previous.([]byte)
		if !ok {
			panic(fmt.Sprintf("only []byte should be stored in m, not %T", previous))
		}
		if 0 != bytes.Compare(data, dataPrev) {
			panic(fmt.Sprintf("b3=key='%v'; data('%v') with len %v != dataPrev('%v') with len %v; bad hashing somewhere?", b3, string(data), len(data), string(dataPrev), len(dataPrev)))
		}

		// no change in stored data, so no eviction needed.
		return
	}
	// we added new data
	//vv("first time mapData store under key b3='%v' of data='%v'", b3, string(data))
	s.mapDataTotalBytes += int64(len(data))
	if s.nMemBlob == s.maxMemBlob {
		// our in memory cache is over its limit,
		// so also do a random eviction.
		evict := s.rng.pseudoRandNonNegInt64() % s.nMemBlob
		victim := s.memkeys[evict]
		//vv("evicting %v => victim key = '%v'", evict, victim)
		// since b3 is new, it cannot be in s.memkeys yet,
		// so we will never evict the key we just added.
		old, present := s.mapData.LoadAndDelete(victim)
		if !present {
			panic("logic error")
		}
		s.mapDataTotalBytes -= int64(len(old.([]byte)))
		// replace the deleted key with the newly added one.
		s.memkeys[evict] = b3
	} else {
		s.nMemBlob++ // want this to be the only place we increment
		s.memkeys = append(s.memkeys, b3)
		// assert len(s.memkeys) == s.nMemBlob
		if int64(len(s.memkeys)) != s.nMemBlob {
			panic(fmt.Sprintf("expected s.nMemBlob(%v) == len(s.memkeys) == %v", s.nMemBlob, len(s.memkeys)))
		}
	}
}

// CASIndexEntry is the in memory index entry
// for the CASIndex Content-Addressable-Store.
type CASIndexEntry struct {

	// Blake3 gives the 55-byte long blake3 cryptographic
	// hash of the data blob we are indexing.
	Blake3 string

	// Beg gives the byte offset of the start of
	// the block in the file (it is not marshalled to disk
	// but recreated from the file read-offset when read).
	Beg int64

	// Clen is the compressed length (in bytes) of the chunk.
	// This is the size on disk.
	//
	// Chunks from fastcdc4 are maximum 64KB at
	// the moment, so an int32 with 2GB range is
	// more than sufficient.
	Clen int32

	// Ulen gives the uncompressed length of the chunk (in bytes).
	// This is the size in memory.
	Ulen int32
}

func (a *CASIndexEntry) Equal(b *CASIndexEntry) bool {
	if a.Blake3 != b.Blake3 {
		return false
	}
	if a.Beg != b.Beg {
		return false
	}
	if a.Clen != b.Clen {
		return false
	}
	if a.Ulen != b.Ulen {
		return false
	}
	return true
}

func (s *CASIndexEntry) String() string {
	return fmt.Sprintf(`CASIndexEntry{Beg:%v, Clen:%v, Ulen:%v, Blake3:"%v"}
`, s.Beg, s.Clen, s.Ulen, s.Blake3)
}

func NewCASIndexEntry(blake3str string, beg int64, clen, ulen int32) (r *CASIndexEntry) {
	n := len(blake3str)
	// len is 55, so 0-byte terminated always too--
	// which should make the int64 8-byte aligned as well.
	//vv("n = %v", n) // n = 55
	if n > 56 {
		panic(fmt.Sprintf("blake3 string must be <= 56 bytes: %v", n))
	}
	r = &CASIndexEntry{
		Blake3: blake3str,
		Beg:    beg,
		Clen:   clen,
		Ulen:   ulen,
	}
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
	o[0] = '\n' // unused for info, so make file more readable.
	copy(o[1:56], []byte(z.Blake3[:55]))

	fromInt32(z.Clen, o[56:60])
	fromInt32(z.Ulen, o[60:64])
	//i := z.Endx
	//fromInt64(i, o[56:64])
	// o[56] = byte(i >> 56)
	// o[57] = byte(i >> 48)
	// o[58] = byte(i >> 40)
	// o[59] = byte(i >> 32)
	// o[60] = byte(i >> 24)
	// o[61] = byte(i >> 16)
	// o[62] = byte(i >> 8)
	// o[63] = byte(i)

	return
}

// ManualUnmarshalMsg is adapted from msgp but does NOT
// provide greenpack / msgpack serz. Instead it is
// custom, manual serization of the two fields so
// that they exactly fit into a single 64-byte cache line.
func (z *CASIndexEntry) ManualUnmarshalMsg(b []byte) (o []byte, err error) {

	z.Blake3 = string(b[1:56])
	z.Clen = toInt32(b[56:60])
	z.Ulen = toInt32(b[60:64])
	//z.Endx = toInt64(b[56:64])
	return b[64:], nil
}

func toInt64(b []byte) int64 {
	return (int64(b[0]) << 56) | (int64(b[1]) << 48) |
		(int64(b[2]) << 40) | (int64(b[3]) << 32) |
		(int64(b[4]) << 24) | (int64(b[5]) << 16) |
		(int64(b[6]) << 8) | (int64(b[7]))
}

func fromInt64(i int64, o []byte) {
	o[0] = byte(i >> 56)
	o[1] = byte(i >> 48)
	o[2] = byte(i >> 40)
	o[3] = byte(i >> 32)
	o[4] = byte(i >> 24)
	o[5] = byte(i >> 16)
	o[6] = byte(i >> 8)
	o[7] = byte(i)
}

func toInt32(b []byte) int32 {
	return (int32(b[0]) << 24) | (int32(b[1]) << 16) |
		(int32(b[2]) << 8) | (int32(b[3]))
}
func toUint32(b []byte) uint32 {
	return (uint32(b[0]) << 24) | (uint32(b[1]) << 16) |
		(uint32(b[2]) << 8) | (uint32(b[3]))
}

func fromInt32(i int32, o []byte) {
	o[0] = byte(i >> 24)
	o[1] = byte(i >> 16)
	o[2] = byte(i >> 8)
	o[3] = byte(i)
}

func fromUint32(i uint32, o []byte) {
	o[0] = byte(i >> 24)
	o[1] = byte(i >> 16)
	o[2] = byte(i >> 8)
	o[3] = byte(i)
}

// ManualMsgsize
func (z *CASIndexEntry) ManualMsgsize() (s int) {
	return 64
}

func (s *CASIndex) TotMem() (nTot, nMem int64) {
	return s.nKnownBlob, s.nMemBlob
}

func (s *CASIndex) Close() error {
	err := s.fdData.Close()
	err2 := s.fdIndex.Close()
	if err != nil {
		return err
	}
	if err2 != nil {
		return err2
	}
	return nil
}
