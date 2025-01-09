package rpc25519

import (
	"github.com/klauspost/compress/zstd"
)

type zstdCompressor struct {
	compressor *zstd.Encoder
	decomp     *zstd.Decoder

	decompWorkingBuf   []byte
	compressWorkingBuf []byte
}

func newZstdCompressor() (*zstdCompressor, error) {

	// encoder defaults to GOMAXPROCS
	// The nil argument here means only do []byte compressions,
	// unless you do a Reset(io.Writer)
	compressor, err := zstd.NewWriter(nil)
	panicOn(err)
	if err != nil {
		return nil, err
	}

	// zstd.WithDecoderConcurrency(0) => use all GOMAXPROCS goro
	// Otherwise, the default is 4 decompressor goro.
	//decomp, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
	decomp, err := zstd.NewReader(nil)
	panicOn(err)
	if err != nil {
		return nil, err
	}

	return &zstdCompressor{
		compressor:         compressor,
		decomp:             decomp,
		decompWorkingBuf:   make([]byte, 2<<20),
		compressWorkingBuf: make([]byte, 2<<20),
	}, nil
}

// Close releases held resources, important for cleanup.
func (c *zstdCompressor) Close() {
	c.compressor.Close()
	c.decomp.Close()
}

// Decompress a buffer. If over 1MB, up the buffer sizes above.
func (c *zstdCompressor) Decompress(src []byte) ([]byte, error) {
	return c.decomp.DecodeAll(src, c.decompWorkingBuf[:0])
}

// Compress a buffer. If over 1MB, raise the buffer sizes above.
func (c *zstdCompressor) Compress(src []byte) []byte {
	return c.compressor.EncodeAll(src, c.compressWorkingBuf[:0])
}
