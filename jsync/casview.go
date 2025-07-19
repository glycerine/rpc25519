package jsync

import (
	"fmt"
	"io"
)

// diagnostic helpers to dump the data or index files.
func (s *CASIndex) diagnosticDisplayData() (err error) {
	// grab exclusive s.fdIndex and s.workbuf access
	s.mut.Lock()
	defer s.mut.Unlock()

	var foundDataEntries int64

	_, err = s.fdData.Seek(0, 0)
	panicOn(err)
	var beg int64
	var nr int
	for i := int64(0); ; i++ {
		nr, err = io.ReadFull(s.fdData, s.workbuf[:64])
		_ = nr
		//vv("i=%v; nr=%v", i, nr)
		switch err {
		case io.EOF:
			err = nil
			vv("no bytes read on i=%v io.ReadFull(s.fdData) read of header; must be done.", i)
			return
		case io.ErrUnexpectedEOF:
			// fewer than 64 bytes read
			panic(fmt.Sprintf("ErrUnexpectedEOF after nr=%v, corrupted path? path='%v'", nr, s.path))
		case nil:
			// full 64 bytes read into s.workbuf[:64]
			e := &CASIndexEntry{}
			_, err = e.ManualUnmarshalMsg(s.workbuf[:64])
			panicOn(err)
			foundDataEntries++
			e.Beg = beg
			fmt.Printf("casview data [%04d] %v", i, e)
			//vv("read back from path '%v' gives e = '%v'", s.path, e)

			endx := beg + int64(e.Clen)
			sz := endx - beg - 64
			var nr2 int
			nr2, err = io.ReadFull(s.fdData, s.workbuf[:sz])
			//vv("inner read of data: sz=%v; endx=%v; beg=%v; nr2=%v; err = '%v'", sz, endx, beg, nr2, err)
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
				_ = data
				//s.addToMapData(e.Blake3, data)
			default:
				panicOn(err)
			}
			beg = endx
			cur := curpos(s.fdData)
			if cur != beg {
				panic(fmt.Sprintf("sanity check failed, curpos(s.fdData)=%v != beg(%v)", cur, beg))
			}

		default:
			panicOn(err)
		}
	}
}

func (s *CASIndex) diagnosticDisplayIndex() (err error) {
	// grab exclusive s.fdIndex and s.workbuf access
	s.mut.Lock()
	defer s.mut.Unlock()

	_, err = s.fdIndex.Seek(0, 0)
	panicOn(err)
	//vv("good: curpos = %v for fdIndex", cur)
	fi, err := s.fdIndex.Stat()
	panicOn(err)
	indexSz := fi.Size()
	if indexSz == 0 {
		vv("warning: empty index path '%v'", s.pathIndex)
		return
	}

	var foundIndexEntries int64

	beg := int64(0)
	//vv("top of diagnosticDisplayIndex")
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
				panic(fmt.Sprintf("how do deal with torn read (rem=%v) at pos %v of path '%v'?", rem, curpos(s.fdIndex), s.pathIndex))
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
				panic(fmt.Sprintf("diagnosticDisplayIndex error: how do deal with torn read nr = %v; (rem2=%v) at pos %v of path '%v'?", nr, rem2, curpos(s.fdIndex), s.pathIndex))
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
				beg = endx

				fmt.Printf("casview index [%04d] %v", i, e)
			}
		}
	}
	return
}
