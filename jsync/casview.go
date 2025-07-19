package jsync

import (
	//"bytes"
	"fmt"
	"io"
	//"os"
	//"sync"
	//"github.com/glycerine/greenpack/msgp"
	//"github.com/glycerine/rpc25519/hash"
)

// diagnostics
func (s *CASIndex) displayData() (err error) {

	var foundDataEntries int64

	_, err = s.fdData.Seek(0, 0)
	panicOn(err)
	var beg int64
	var nr int
	for i := int64(0); ; i++ {
		nr, err = io.ReadFull(s.fdData, s.workbuf[:64])
		_ = nr
		vv("i=%v; nr=%v", i, nr)
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
			fmt.Printf("[%04d] %v\n", i, e)
			//vv("read back from path '%v' gives e = '%v'", s.path, e)

			endx := e.Endx
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
