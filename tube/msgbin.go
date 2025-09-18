package tube

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	//"github.com/glycerine/greenpack/msgp"
)

func nextframe(fd *os.File, path string) (readme []byte, err error) {
	// use unframeBinMsgpack to avoid moving the file position by much,
	// and then read exactly what we need, so we are
	// ready for the next read (or replacement!) and we
	// always leave (POST invariant) the file position perfectly aligned
	// for adding or overwriting a record.
	pos := curpos(fd)
	var peek [5]byte
	nr, err := fd.Read(peek[:])
	if err != nil {
		return nil, err
	}
	if nr != len(peek) {
		return nil, fmt.Errorf("could not read next record starting at pos %v in path '%v'", pos, path)
	}
	//ntotal, ninside, nheader, err := unframeBinMsgpack(peek[:])
	ntotal, _, _, err := unframeBinMsgpack(peek[:])
	panicOn(err)
	//vv("ntotal = %v", ntotal)
	readme = make([]byte, ntotal)
	nw := copy(readme, peek[:])

	// ReadFull docs:
	// The error is EOF only if no bytes were read.
	// If an EOF happens after reading some but not all the bytes,
	// ReadFull returns [ErrUnexpectedEOF].
	// On return, n == len(buf) if and only if err == nil.
	_, err = io.ReadFull(fd, readme[nw:])
	panicOn(err)
	return readme, nil
}

const (
	bin8  uint8 = 0xc4
	bin16 uint8 = 0xc5
	bin32 uint8 = 0xc6
)

type UnframeError int

const (
	NotEnoughBytes UnframeError = -1
	NotBinarySlice UnframeError = -2
)

func (e UnframeError) Error() string {
	switch e {
	case NotEnoughBytes:
		return "UnframeBinMsgpack() error: NotEnoughBytes"
	case NotBinarySlice:
		return "UnframeBinMsgpack() error: NotBinarySlice: could not find 0xC4, 0xC5, 0xC6 in start of binary msgpack"
	default:
		return "UnknownUnframeError"
	}
}

// unframeBinMsgpack() works on just the minimal 2-5 bytes peek ahead
// needed to see how much to read next.
//
// ninside returns the number of bytes inside/that follow the 2-5 byte
// binary msgpack header. The header frames the internal msgpack serialized
// object. ntotal returns the total number of bytes including the
// header bytes. The header is always a bin8/bin16/bin32 msgpack object
// itself, and so is 2-5 bytes extra, not counting the internal byte
// slice that makes up the internal msgp object. So there are two
// msgpack decoding steps to get a golang object back.
func unframeBinMsgpack(p []byte) (ntotal int, ninside int, nheader int, err error) {

	if len(p) == 0 {
		err = NotEnoughBytes
		return
	}
	switch p[0] {
	case bin8:
		if len(p) < 2 {
			err = NotEnoughBytes
			return
		}
		ninside = int(p[1])
		nheader = 2
		ntotal = ninside + nheader
	case bin16:
		if len(p) < 3 {
			err = NotEnoughBytes
			return
		}
		ninside = int(binary.BigEndian.Uint16(p[1:3]))
		nheader = 3
		ntotal = ninside + nheader
	case bin32:
		if len(p) < 5 {
			err = NotEnoughBytes
			return
		}
		ninside = int(binary.BigEndian.Uint32(p[1:5]))
		nheader = 5
		ntotal = ninside + nheader
	default:
		// corruption tests hit this path.
		//fmt.Printf("p bytes = '%#v'\n", p[:5])
		//fmt.Printf("p bytes = '%#v'/as string='%v'\n", p, string(p))
		err = NotBinarySlice
		panic(err)
	}
	return
}
