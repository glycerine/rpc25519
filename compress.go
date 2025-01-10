package rpc25519

import (
	"bytes"
	"fmt"
	"io"
	//"os"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

var _ = &zstd.Decoder{}
var _ = s2.NewWriter
var _ = lz4.NewWriter

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

// decomp centalizes decompression
// so we can use it from common.go (unencrypted)
// and chacha.go (encrypted) {send/read}Message.
type decomp struct {
	maxMsgSize int
	de_lz4     *lz4.Reader
	de_s2      *s2.Reader
	de_zstd    *wrapZstdDecoder // *zstd.Decoder

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

	d = &decomp{
		maxMsgSize:  maxMsgSize,
		de_lz4:      lz4.NewReader(nil),
		de_s2:       s2.NewReader(nil),
		de_zstd:     &wrap, // *wrapZstdDecoder, not *zstd.Decoder
		decompSlice: make([]byte, maxMsgSize+80),
	}
	return d
}

// pressor handles all compression (not encryption;
// compression happens before encryption, and
// after decryption).
type pressor struct {
	maxMsgSize int
	com_lz4    *lz4.Writer
	com_s2     *s2.Writer

	// support any of these 4 Zstandard levels
	com_zstd11 *zstd.Encoder
	com_zstd07 *zstd.Encoder
	com_zstd03 *zstd.Encoder
	com_zstd01 *zstd.Encoder

	// big buffer to write into
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
	p.com_zstd11, err = zstd.NewWriter(io.Discard,
		zstd.WithEncoderLevel(zstd.SpeedBestCompression))
	panicOn(err)

	// The "Better"  is roughly equivalent to zstd level 7.
	p.com_zstd07, err = zstd.NewWriter(io.Discard,
		zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
	panicOn(err)

	// The "Default" is roughly equivalent to zstd level 3 (default).
	p.com_zstd03, err = zstd.NewWriter(io.Discard,
		zstd.WithEncoderLevel(zstd.SpeedDefault))
	panicOn(err)

	// The "Fastest" is roughly equivalent to zstd level 1.
	p.com_zstd01, err = zstd.NewWriter(io.Discard,
		zstd.WithEncoderLevel(zstd.SpeedFastest))
	panicOn(err)

	return p
}

func (p *pressor) handleCompress(magic7 byte, bytesMsg []byte) ([]byte, error) {

	vv("handleCompress(magic7=%v) is using '%v'", magic7, mustDecodeMagic7(magic7))

	var c compressor
	switch magic7 {
	// magic[7] (the last byte 0x00 here) can vary,
	// it indicates the compression in use:
	case 0:
		// no compression
		return bytesMsg, nil
	case 1:
		c = p.com_s2
		//return "s2", nil
	case 2:
		c = p.com_lz4
		//return "lz4", nil
	case 3:
		c = p.com_zstd11
		//return "bzst:11", nil
	case 4:
		c = p.com_zstd07
		//return "bzst:07", nil
	case 5:
		c = p.com_zstd03
		//return "bzst:03", nil
	case 6:
		c = p.com_zstd01
		//return "bzst:01", nil
	default:
		panic(fmt.Sprintf("unknown magic7 '%v'", magic7))
	}

	//vv("handleCompress(magic7=%v) is using '%v'", magic7, mustDecodeMagic7(magic7))

	uncompressedLen := len(bytesMsg)
	_ = uncompressedLen
	compBuf := bytes.NewBuffer(bytesMsg)
	// already done at init:
	//p.compSlice = make([]byte, maxMsgSize+80)

	// debug: new slice every time, is memory sharing the issue? nope.
	//big := make([]byte, maxMessage)
	//out := bytes.NewBuffer(big[:0])

	out := bytes.NewBuffer(p.compSlice[:0])

	c.Reset(out)

	_, err := io.Copy(c, compBuf)
	panicOn(c.Close())
	panicOn(err)
	compressedLen := len(out.Bytes())
	vv("compression: %v bytes -> %v bytes", uncompressedLen, compressedLen)
	bytesMsg = out.Bytes()
	return bytesMsg, nil
}

func (decomp *decomp) handleDecompress(magic7 byte, message []byte) ([]byte, error) {

	//vv("handle decompress using magic7 = %v", magic7)

	var d decompressor
	switch magic7 {
	// magic[7] (the last byte 0x00 here) can vary,
	// it indicates the compression in use:
	case 0:
		// no compression
		return message, nil
	case 1:
		d = decomp.de_s2
		//return "s2", nil
	case 2:
		d = decomp.de_lz4
		//return "lz4", nil
	case 3, 4, 5, 6:
		d = decomp.de_zstd
		//return "bzst:11", nil
		//return "bzst:07", nil
		//return "bzst:03", nil
		//return "bzst:01", nil
	default:
		panic(fmt.Sprintf("unknown magic7 '%v'", magic7))
	}

	vv("handleDecompress(magic7=%v) is using '%v'", magic7, mustDecodeMagic7(magic7))

	compressedLen := len(message)
	decompBuf := bytes.NewBuffer(message)
	d.Reset(decompBuf)
	// already init done:
	//d.decompSlice = make([]byte, maxMsgSize+80)

	//big := make([]byte, maxMessage)
	//out := bytes.NewBuffer(big[:0])
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
