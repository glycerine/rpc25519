package tube

import (
	"fmt"
	"hash/crc64"
	"io"
	"os"
)

var crc64table = crc64.MakeTable(crc64.ECMA)

// protocol aware recovery implementation
// https://www.usenix.org/sites/default/files/conference/protected-files/fast18_slides_alagappan.pdf

//go:generate greenpack

// maximum size, including space for the 8 byte checksum.
const maxParRecord = 400

// ParRecord is the "persist record"
// in protocol aware recovery; the entry identifier.
// It is written to the header of the log.
// An 8 byte crc64.ECMA checksum follows
// the msgpack bytes on disk.
type ParRecord struct {
	Offset    int64  `zid:"0"`
	Len0      int64  `zid:"1"`
	Len2      int64  `zid:"2"`
	Index     int64  `zid:"3"`
	Term      int64  `zid:"4"`
	Epoch     int64  `zid:"5"`
	TicketID  string `zid:"6"` // 28 bytes
	ClusterID string `zid:"7"`
	RLEblake3 string `zid:"8"` // 56 bytes
}

type parLog struct {
	nodisk bool

	path        string
	fd          *os.File
	parentDirFd *os.File

	n int64

	e []*ParRecord
}

func newParLog(path string, nodisk bool) (s *parLog, err error) {

	var fd *os.File
	var sz int64
	var fi os.FileInfo

	if !nodisk {
		fi, err = os.Stat(path)
		if err == nil {
			sz = fi.Size()
			//vv("existing parlog is size %v => %v entries at path '%v'", sz, sz/maxParRecord, path)
		} else {
			//vv("no existing parlog at path '%v'", path)
			// file does not exist, don't freak.
		}

		fd, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
		panicOn(err)
		if err != nil {
			return nil, err
		}
		// same as in wal.go:182, per tigerbeetle, we want to sync before reading.
		err = fd.Sync()
		panicOn(err)

		if err != nil {
			return nil, err
		}
	}

	s = &parLog{
		nodisk: nodisk,
		path:   path,
		fd:     fd,
	}

	badsize := sz % maxParRecord
	if badsize != 0 {
		panic(fmt.Sprintf("bad parlog size(%v), not a multiple of maxParRecord(%v)", sz, maxParRecord))
	}
	nGoal := sz / maxParRecord
	for i := range nGoal {
		_ = i
		e, err := s.loadone(i) // newParLog reading existing parlog.
		panicOn(err)
		s.e = append(s.e, e)
		s.n++ // in newParLog load existing file.
	}

	if !nodisk {
		// setup for append
		setpos(fd, sz)
	}
	return
}

// nw0 is the length of the serialized RaftLogEntry without the checksum
// nw2 is with the checksum.
func (s *parLog) append1(e *RaftLogEntry, offset int64, nw0, nw2 int64, b3string string) (err0 error) {

	//vv("append1 s.n=%v, new e.Index = %v", s.n, e.Index)

	pr := &ParRecord{
		Index:     e.Index,
		Term:      e.Term,
		Offset:    offset,
		Len0:      nw0,
		Len2:      nw2,
		TicketID:  e.Ticket.TicketID,
		ClusterID: e.Ticket.ClusterID,
		RLEblake3: b3string,
	}

	s.e = append(s.e, pr)

	if !s.nodisk {
		by1, err := pr.MarshalMsg(nil)
		panicOn(err)

		bs := ByteSlice(by1)

		by := make([]byte, maxParRecord)
		var used []byte
		used, err = bs.MarshalMsg(by[:0])
		panicOn(err)

		if len(used)+8 > maxParRecord {
			panic(fmt.Sprintf("no space for checksum. len(used)=%v ; maxParRecord=%v", len(used), maxParRecord))
		}
		i := crc64.Checksum(used, crc64table)
		var c [8]byte
		c[0] = byte(i >> 56)
		c[1] = byte(i >> 48)
		c[2] = byte(i >> 40)
		c[3] = byte(i >> 32)
		c[4] = byte(i >> 24)
		c[5] = byte(i >> 16)
		c[6] = byte(i >> 8)
		c[7] = byte(i)
		//vv("on %v, computed checksum i=%v, appending bytes c='%#v'", s.n+1, i, c[:])
		used = append(used, c[:]...)

		pos := s.n * maxParRecord
		setpos(s.fd, pos)

		_, err0 = s.fd.Write(by)
		panicOn(err0)
		//vv("parlog wrote [%v, %v]; s.n=%v", pos, pos+int64(len(by)), s.n)
		s.sync()
	}

	s.n++ // in append1
	if int64(len(s.e)) != s.n {
		panic(fmt.Sprintf("par assert failed: len(s.e)=%v but n=%v", len(s.e), s.n))
	}

	return
}

func (s *parLog) at(i int64) (r *ParRecord, err error) {
	if i < int64(len(s.e)) {
		return s.e[i], nil
	}
	if s.nodisk {
		return nil, fmt.Errorf("parLog.at(i=%v) error: not found. len(s.e)=%v. nodisk is on.", i, len(s.e))
	}
	pos := i * maxParRecord
	setpos(s.fd, pos)
	return s.loadone(i)
}

func (s *parLog) truncate(keepcount int64) error {
	if keepcount == s.n {
		return nil
	}
	if keepcount > s.n {
		panic(fmt.Sprintf("bad keepcount(%v) > n(%v)", keepcount, s.n))
	}
	s.n = keepcount
	s.e = s.e[:keepcount]
	if !s.nodisk {
		newSz := s.n * maxParRecord
		s.fd.Truncate(newSz)
		s.sync()
		setpos(s.fd, newSz)
	}
	return nil
}

func (s *parLog) sync() error {
	if s.nodisk {
		return nil
	}
	err := s.fd.Sync()
	panicOn(err)

	return nil
}

func (s *parLog) close() (err error) {
	if s.nodisk {
		return nil
	}
	err = s.fd.Close()
	return
}

func (s *parLog) loadone(i int64) (r *ParRecord, err error) {
	//vv("loadone i = %v", i)

	if s.nodisk {
		if i < int64(len(s.e)) {
			return s.e[i], nil
		}
		return nil, fmt.Errorf("parLog.at(i=%v) error: not found. len(s.e)=%v. nodisk is on.", i, len(s.e))
	}

	setpos(s.fd, i*maxParRecord)

	readme, err := nextframe(s.fd, s.path)
	if err == io.EOF {
		return nil, err
	}
	panicOn(err)

	var by ByteSlice
	_, err = by.UnmarshalMsg(readme)
	//vv("parLog.loadone sees err=%v from UnmarshalMsg len(by)=%v", err, len(by))
	panicOn(err)
	if err != nil {
		return
	}
	// verify the crc64 checksum
	got := crc64.Checksum(readme, crc64table)

	var b [8]byte
	_, err = io.ReadFull(s.fd, b[:])
	//vv("read after: '%#v'", b[:])
	panicOn(err)
	want := (uint64(b[0]) << 56) | (uint64(b[1]) << 48) |
		(uint64(b[2]) << 40) | (uint64(b[3]) << 32) |
		(uint64(b[4]) << 24) | (uint64(b[5]) << 16) |
		(uint64(b[6]) << 8) | (uint64(b[7]))

	if got != want {
		panic(fmt.Sprintf("crc64 mismatch: want '%v', got '%v'", want, got))
	}

	r = &ParRecord{}
	_, err = r.UnmarshalMsg(by)
	panicOn(err)
	return
}
