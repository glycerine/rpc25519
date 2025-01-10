package rpc25519

/* just experiments, not really tests
import (
	"bytes"
	cryrand "crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"testing"

	"github.com/pierrec/lz4/v4"
	//snappy "github.com/glycerine/go-unsnap-stream"
	cv "github.com/glycerine/goconvey/convey"
	"github.com/klauspost/compress/zstd"
)

var _ = cryrand.Read

func Test400_zstd_compression(t *testing.T) {

	cv.Convey("our zstdCompressor wrapper should compress and decompress []byte", t, func() {
		zstd, err := newZstdCompressor()
		panicOn(err)
		defer zstd.Close()

		data := make([]byte, 20000)
		// copy so we are not using the underlying buffers
		// which will next be overwritten.
		compressed := append([]byte{}, zstd.Compress(data)...)
		vv("data len %v, compressed len %v: '%x'", len(data), len(compressed), compressed)
		decomp, err := zstd.Decompress(compressed)
		panicOn(err) // invalid input: magic number mismatch
		if !bytes.Equal(decomp, data) {
			panic("should be equal")
		}

	})

	cv.Convey("can we compress the bytes over top of the original? and decompress over top of the compressed? nope. must have an extra buffer available.", t, func() {
		z, err := newZstdCompressor()
		panicOn(err)
		defer z.Close()

		orig := make([]byte, 20000)
		//cryrand.Read(orig)
		data := make([]byte, len(orig))
		copy(data, orig)

		// copy so we are not using the underlying buffers
		// which will next be overwritten.

		output := make([]byte, 0, len(orig))
		compressed := z.compressor.EncodeAll(data, output)

		vv("data len %v, compressed len %v: '%x'; &compressed[0] = %p =?= %p == &data[0]", len(data), len(compressed), compressed, &compressed[0], &data[0])

		decomp, err := z.decomp.DecodeAll(compressed, data[:0])
		panicOn(err)

		if !bytes.Equal(decomp, orig) {
			panic("should be equal")
		}

	})

}

func Test401_zstd_expriment_with_streaming_for_smaller_buffers(t *testing.T) {

	cv.Convey("but would can compress a series of smaller frames and then concatenate these over top of the same buffer, using a smaller compression buffer. (we sure think we can. verify that here.", t, func() {

		z, err := newZstdCompressor()
		panicOn(err)
		defer z.Close()

		// can we have 64MB random message
		// and only 8MB Window/helper buffer?
		orig := make([]byte, 64<<20)
		cryrand.Read(orig)
		data := make([]byte, len(orig))
		copy(data, orig)

		// copy so we are not using the underlying buffers
		// which will next be overwritten.

		framesize := 16 << 20
		//	output := make([]byte, 0, framesize)
		buf := make([]byte, 0, 8<<20)
		src := data
		nw := 0

		maxFrameSizeCompressed := 0
		for i := 0; len(src) > 0; i++ {
			// compress one frame
			n := len(src)
			if n > framesize {
				n = framesize
			}
			// INVAR: n <= framesize
			compressed := z.compressor.EncodeAll(src[:n], buf[:0])
			vv("frame i=%v, starting len=%v; compressed len=%v", i, n, len(compressed))

			// now we should be able to copy over our original, as long as
			// compressed is not longer than the src.
			if len(compressed) > n {
				// panic: len(compressed) = 113 > n = 100;
				// well data IS random and so uncompressible.
				//panic(fmt.Sprintf("len(compressed) = %v > n = %v", len(compressed), n))
				vv(fmt.Sprintf("len(compressed) = %v > n = %v", len(compressed), n))
			}

			if len(compressed) > maxFrameSizeCompressed {
				maxFrameSizeCompressed = len(compressed)
			}

			nw += copy(data[nw:], compressed)
			if nw > len(data) {
				//panic("problem! our compressed exceeded the size of our original")
				vv("problem! our compressed exceeded the size of our original")
			}
			src = src[n:]
		}
		src = data[:nw]
		compressed := src
		// INVAR: src has our compressed frames, back-to-back, in the
		// same buffer that it originally was from.

		vv("data len %v, compressed len %v:; maxFrameSizeCompressed = %v", len(data), len(compressed), maxFrameSizeCompressed)

		// but where are the frame boundaries so we can read only a single frame?
		/// hard to get!
		// why want them? so I can reduce my buffer requirement.
		// standard say 8MB is max anyway, can we just use that?
		var header zstd.Header
		panicOn(header.Decode(compressed[:zstd.HeaderMaxSize]))
		vv("header = '%#v'", header)

		// header = 'zstd.Header{
		// SingleSegment:false,
		// WindowSize:0x400,
		// DictionaryID:0x0,
		// HasFCS:false,
		// FrameContentSize:0x0,
		// Skippable:false,
		// SkippableID:0,
		// SkippableSize:0x0,
		// HeaderSize:6,
		// FirstBlock:struct {
		//   OK bool;
		//   Last bool;
		//   Compressed bool;
		//   DecompressedSize int;
		//   CompressedSize int
		// }
		// {
		//   OK:true,
		//   Last:true,
		//   Compressed:true,
		//   DecompressedSize:100,
		//   CompressedSize:1
		// },
		//  HasCheckSum:true}'

		if header.SingleSegment && header.HasFCS {
			// implies that WindowSize is invalid and that FrameContentSize is valid.

			// FrameContentSize is the expected uncompressed size of the entire frame.
			vv("FrameContentSize = %v", header.FrameContentSize)
		} else {
			vv("WindowSize = %v", header.WindowSize) // WindowSize = 1024
		}

		// For skippable frames,
		// The total frame size is the HeaderSize plus the SkippableSize.
		framesz := 0
		if header.Skippable {
			framesz = header.HeaderSize + int(header.SkippableSize)
		} else {
			// else how to compute total frame size?
		}
		vv("framesz = %v", framesz)

		// frame header is 2-14 bytes

		// decomp, err := z.decomp.DecodeAll(compressed, data[:0])
		// panicOn(err)

		// if !bytes.Equal(decomp, orig) {
		// 	panic("should be equal")
		// }

	})

}

// https://github.com/facebook/zstd/blob/dev/doc/zstd_compression_format.md#window_descriptor
// For improved interoperability, it's recommended for
// decoders to support Window_Size of up to 8 MB,
// and it's recommended for encoders to not generate
// frame requiring Window_Size larger than 8 MB.
// It's merely a recommendation though, decoders
// are free to support larger or lower limits,
// depending on local limitations.
//
// jea: note this is the frame size, not the
// total Message byte size; Zstd frames are
// streamed back-to-back to get the total
// compressed stream.
//decompbuf []byte // 8 MB per above

func Test402_stream_oriented(t *testing.T) {

	cv.Convey("maybe better to use the streaming version than the []byte version?", t, func() {
		var out bytes.Buffer

		//compressor, err := zstd.NewWriter(&out)
		compressor := lz4.NewWriter(&out)
		vv("compressor = %T", compressor)
		//compressor, err := zstd.NewWriter(&out)
		//panicOn(err)

		in := make([]byte, 64<<20)
		cryrand.Read(in)

		inbuf := bytes.NewBuffer(in)
		n, err := io.Copy(compressor, inbuf)
		compressor.Close()
		panicOn(err)
		if n != int64(len(in)) {
			panic("short write -- we are in memory, should never happen!?!")
		}

		// decompress
		vv("out has %v bytes in it now", len(out.Bytes()))
		//decomp, err := zstd.NewReader(&out)
		decomp := lz4.NewReader(&out)
		//panicOn(err)

		var unpacked bytes.Buffer
		nr, err := io.Copy(&unpacked, decomp)

		vv("nr = %v, len in = %v", nr, len(in))
		panicOn(err)
		if nr != int64(len(in)) {
			panic("length mismatch")
		}

		if !bytes.Equal(unpacked.Bytes(), in) {
			panic("did not decompress back to the original")
		}
		vv("good: we got back the original input, in")
	})
}

func Test403_lz4_experiments_continue(t *testing.T) {

	cv.Convey("lz4 as it is used in chacha.go, test in isolation", t, func() {

		bytesMsg := make([]byte, 200)
		// compress
		uncompressedLen := len(bytesMsg)
		_ = uncompressedLen
		compBuf := bytes.NewBuffer(bytesMsg)

		// already done at init:
		compSlice := make([]byte, maxMessage+80)
		out := bytes.NewBuffer(compSlice[:0])
		compressor := lz4.NewWriter(out)

		_, err := io.Copy(compressor, compBuf)
		panicOn(compressor.Close())
		panicOn(err)
		m := len(out.Bytes())
		vv("compression: %v bytes -> m=%v", uncompressedLen, m)
		message := out.Bytes() // the compressed messsage.

		compressedLen := m

		// have magic?
		//vv("message = '%x' / '%v'", message, string(message))

		magicbuf := message[:4]
		magic := binary.LittleEndian.Uint32(magicbuf)
		//binary.LittleEndian.PutUint32(magicbuf, magic)
		vv("magicbuf = %x', magic = %v", magicbuf, magic) // 0, 0
		if magic == 0x184D2204 {
			vv("good magic number")
		} else {
			panic("bad magic!")
		}

		if compressedLen > 0 {
			_ = compressedLen
			decompBuf := bytes.NewBuffer(message)
			decompressor := lz4.NewReader(decompBuf)
			// already init done:
			decompSlice := make([]byte, maxMessage+80)
			out := bytes.NewBuffer(decompSlice[:0])
			n, err := io.Copy(out, decompressor)
			panicOn(err)
			if n > int64(len(decompSlice)) {
				panic(fmt.Sprintf("we wrote more than our pre-allocated buffer, up its size! n(%v) > len(out) = %v", n, len(decompSlice)))
			}
			vv("decompression: %v bytes -> %v bytes", compressedLen, n)
			messageDecomp := out.Bytes()
			if !bytes.Equal(messageDecomp, bytesMsg) {
				panic(fmt.Sprintf("not equal bytes!"))
			}
		}

	})
}
*/
