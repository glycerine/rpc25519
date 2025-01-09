package rpc25519

import (
	"bytes"
	"fmt"
	//"io"
	"os"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg"
	"github.com/klauspost/compress/zstd"
)

/*
From the seekable README:

[https://github.com/facebook/zstd/blob/dev/contrib/seekable_format/zstd_seekable_compression_format.md] Seekable ZSTD compression format implemented in Golang.

This library provides a random access reader (using uncompressed file offsets) for ZSTD-compressed streams. This can be used for creating transparent compression layers. Coupled with Content Defined Chunking (CDC) it can also be used as a robust de-duplication layer.
*/

func Compress(f *os.File) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	panicOn(err)
	if err != nil {
		panicOn(err)
	}
	defer enc.Close()

	w, err := seekable.NewWriter(f, enc)
	if err != nil {
		panicOn(err)
	}

	// Write data in chunks.
	for _, b := range [][]byte{[]byte("Hello"), []byte(" "), []byte("World!")} {
		_, err = w.Write(b)
		if err != nil {
			panicOn(err)
		}
	}

	// Close and flush seek table.
	err = w.Close()
	if err != nil {
		panicOn(err)
	}

	//NB! Do not forget to call `Close` since it is responsible for flushing the seek table.

}

func Uncompress(f *os.File) {
	//Reading can either be done through `ReaderAt` interface:

	dec, err := zstd.NewReader(nil)
	if err != nil {
		panicOn(err)
	}
	defer dec.Close()

	r, err := seekable.NewReader(f, dec)
	if err != nil {
		panicOn(err)
	}
	defer r.Close()

	ello := make([]byte, 4)
	// ReaderAt
	r.ReadAt(ello, 1)
	if !bytes.Equal(ello, []byte("ello")) {
		//log.Fatalf("%+v != ello", ello)
		panic(fmt.Sprintf("%+v != ello", ello))
	}

	// Or through the `ReadSeeker`:
	/*
		world := make([]byte, 5)
		// Seeker
		r.Seek(-6, io.SeekEnd)
		// Reader
		r.Read(world)
		if !bytes.Equal(world, []byte("World")) {
			log.Fatalf("%+v != World", world)
		}
	*/
}
