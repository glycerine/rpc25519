package rpc25519

import (
	"bytes"
	"testing"

	cv "github.com/glycerine/goconvey/convey"
)

func Test400_zstd_compression(t *testing.T) {

	cv.Convey("our zstdCompressor wrapper should compress and decompress []byte", t, func() {

		zstd, err := newZstdCompressor()
		panicOn(err)
		defer zstd.Close()

		data := make([]byte, 20000)
		// copy so we are not using the underlying buffers
		// which will next be overwritten.
		compressed := append([]byte{}, zstd.Compress(data)...)
		decomp, err := zstd.Decompress(compressed)
		panicOn(err)
		if !bytes.Equal(decomp, compressed) {
			panic("should be equal")
		}

	})
}
