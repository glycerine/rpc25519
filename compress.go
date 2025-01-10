package rpc25519

import (
	"bytes"
	"fmt"
	//"io"
	"os"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

var _ = &zstd.Decoder{}
var _ = s2.NewWriter
var _ = lz4.NewWriter

const UseCompression = true
const UseCompressionAlgo = "s2"

// compressor is implemented by
// compressor *lz4.Writer
// compressor *s2.Writer
// compressor *zstd.Encoder
type compressor interface {
	Reset(io.Writer)
	Write(data []byte) (n int, err error)
	Close() error
}

// decompressor is implemented by
// decompressor *lz4.Reader
// decompressor *s2.Reader
// decompressor *zstd.Decoder
type decompressor interface {
	Reset(io.Reader) // s2, lz4
	//Reset(io.Reader) error // zstd
	Read(p []byte) (n int, err error)
	//ReadFrom(r io.Reader) (n int64, err error) // s2, lz4 do not implement.
}

// wrapZstdDecoder wraps a zstd.Decoder because we have to
// remove the returned error from the Reset method, going from
// Reset(io.Reader) error // zstd
// to
// Reset(io.Reader) // s2, lz4 so the
// so that the decompressor interface works for all three (s2,lz4,zstd).
type wrapZstdDecoder struct {
	zstd.Decoder
}

func (d *wrapZstdDecoder) Reset(r io.Reader) {
	d.Decoder.Reset(r)
}

type decomp struct {
	maxMsgSize int
	de_lz4     *lz4.Reader
	de_s2      *s2.Reader
	de_zstd    *wrapZstdDecoder // *zstd.Decoder

	decompBuf   *bytes.Buffer
	decompSlice []byte
}

func newDecomp(maxMsgSize int) (d *decomp) {

	var wrap wrapZstdDecoder
	zread, err := zstd.NewReader(nil)
	panicOn(err)
	// Fortunately the constructor zstd.NewReader
	// does not use the sync.WaitGroup contained in zstd.Decoder,
	// so we can copy the struct into the wrapper safely
	// before it is used.
	wrap.Decoder = *zread

	//	dec.decompressor =

	d = &decomp{
		maxMsgSize:  maxMsgSize,
		de_lz4:      lz4.NewReader(nil),
		de_s2:       s2.NewReader(nil),
		de_zstd:     &wrap, // *wrapZstdDecoder, not *zstd.Decoder
		decompSlice: make([]byte, maxMsgSize+80),
	}
	return d
}

// decompressor

type pressor struct {
	maxMsgSize int
	com_lz4    *lz4.Writer
	com_s2     *s2.Writer

	com_ztd11 *zstd.Encoder
	com_ztd07 *zstd.Encoder
	com_ztd03 *zstd.Encoder
	com_ztd01 *zstd.Encoder

	compBuf   *bytes.Buffer
	compSlice []byte
}

func newPressor(maxMsgSize int) (p *pressor) {

	com_lz4 := lz4.NewWriter(nil)

	options := []lz4.Option{
		lz4.BlockChecksumOption(true),
		lz4.CompressionLevelOption(lz4.Fast),
		// setting the concurrency option seems to make it hang.
		//lz4.ConcurrencyOption(-1),
	}
	if err := com_lz4.Apply(options...); err != nil {
		panic(fmt.Sprintf("error could not apply lz4 options: '%v'", err))
	}

	p = &pressor{
		maxMsgSize: maxMsgSize,
		com_lz4:    com_lz4,
		com_s2:     s2.NewWriter(nil),
		compSlice:  make([]byte, maxMsgSize+80),
	}

	var err error
	// The "Best"    is roughly equivalent to zstd level 11.
	p.com_ztd11, err = zstd.NewWriter(io.Discard,
		zstd.WithEncoderLevel(zstd.SpeedBestCompression))
	panicOn(err)

	// The "Better"  is roughly equivalent to zstd level 7.
	p.com_ztd07, err = zstd.NewWriter(io.Discard,
		zstd.WithEncoderLevel(zstd.SpeedBetter))
	panicOn(err)

	// The "Default" is roughly equivalent to zstd level 3 (default).
	p.com_ztd03, err = zstd.NewWriter(io.Discard,
		zstd.WithEncoderLevel(zstd.SpeedDefault))
	panicOn(err)

	// The "Fastest" is roughly equivalent to zstd level 1.
	p.com_ztd01, err = zstd.NewWriter(io.Discard,
		zstd.WithEncoderLevel(zstd.SpeedFastest))
	panicOn(err)

	return p
}
