package rpc25519

import (
	"os"
	"testing"

	cv "github.com/glycerine/goconvey/convey"
)

func Test201_rsync_style_hash_generation(t *testing.T) {

	cv.Convey("rsync.go SummarizeFileInCDCHashes() should generate CDC UltraCDC hashes for a file", t, func() {
		path := "testdata/blob977k"

		data, err := os.ReadFile(path)
		panicOn(err)

		h, err := SummarizeBytesInCDCHashes(path, data)
		panicOn(err)
		_ = h
		//vv("scan of file gave: hashes '%v'", h.String())
		cv.So(h.NumChunks, cv.ShouldEqual, 16) // blob977k
	})
}
