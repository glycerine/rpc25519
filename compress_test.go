package rpc25519

import (
	//"bytes"
	//cryrand "crypto/rand"
	//"fmt"
	//"net"
	"testing"
	//"time"

	cv "github.com/glycerine/goconvey/convey"
)

func Test030_compress_inverses(t *testing.T) {

	cv.Convey("compress.go pressor and decomp should be inverses", t, func() {

		p := newPressor(1000)
		d := newDecomp(1000)

		orig := append([]byte("hello rpc25519 world!"), make([]byte, 300)...)
		bytesMsg := append([]byte{}, orig...)

		for _, magic7 := range []byte{0, 1, 2, 3, 4, 5, 6} {

			nm, err := decodeMagic7(magic7)
			panicOn(err)

			msg1, err := p.handleCompress(magic7, bytesMsg)
			panicOn(err)
			vv("%v compressed from %v -> %v bytes", nm, len(orig), len(msg1))
			//vv("msg1 = '%v'", string(msg1))

			msg2, err := d.handleDecompress(magic7, msg1)
			panicOn(err)
			//vv("msg2 = '%v'", string(msg2))
			if string(msg2) != string(orig) {
				panic("compress output did not decompress to the same")
			}
		}
	})
}
