package tube

import (
	"bytes"
	"fmt"
	"io"
	//"os"

	"github.com/minio/minlz"
)

// minlz compression

type decomp struct {
	maxMsgSize int

	de *minlz.Reader

	decompSlice []byte
}

func newDecomp(maxMsgSize int) (d *decomp) {

	d = &decomp{
		maxMsgSize:  maxMsgSize,
		de:          minlz.NewReader(nil),
		decompSlice: make([]byte, maxMsgSize+80),
	}
	return d
}

// pressor handles all compression (not encryption;
// compression happens before encryption, and
// after decryption).
type pressor struct {
	maxMsgSize int
	com        *minlz.Writer

	// big buffer to write into
	compSlice []byte
}

func newPressor(maxMsgSize int) (p *pressor) {

	p = &pressor{
		maxMsgSize: maxMsgSize,
		com:        minlz.NewWriter(nil),
		compSlice:  make([]byte, maxMsgSize+80),
	}
	return p
}

func (p *pressor) compress(bytesMsg []byte) ([]byte, error) {

	c := p.com
	uncompressedLen := len(bytesMsg)
	_ = uncompressedLen
	compBuf := bytes.NewBuffer(bytesMsg)

	out := bytes.NewBuffer(p.compSlice[:0])

	c.Reset(out)

	_, err := io.Copy(c, compBuf)
	panicOn(c.Close())
	panicOn(err)
	vv("compression: %v bytes -> compressedLen: %v bytes", uncompressedLen, len(out.Bytes()))
	bytesMsg = out.Bytes()
	return bytesMsg, nil
}

func (decomp *decomp) decompress(message []byte) ([]byte, error) {

	d := decomp.de

	compressedLen := len(message)
	_ = compressedLen
	decompBuf := bytes.NewBuffer(message)
	d.Reset(decompBuf)

	out := bytes.NewBuffer(decomp.decompSlice[:0])

	n, err := io.Copy(out, d)
	panicOn(err)
	if n > int64(len(decomp.decompSlice)) {
		panic(fmt.Sprintf("we wrote more than our "+
			"pre-allocated buffer, up its size! "+
			"n(%v) > len(out) = %v", n, len(decomp.decompSlice)))
	}
	vv("decompression: %v bytes -> %v bytes; "+
		"len(out.Bytes())=%v", compressedLen, n, len(out.Bytes()))
	message = out.Bytes()
	return message, nil
}
