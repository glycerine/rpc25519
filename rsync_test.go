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

		// now alter the data by prepending 2 bytes
		data2 := append([]byte{0x24, 0xff}, data...)
		h2, err := SummarizeBytesInCDCHashes(path+".prepend2bytes", data2)
		panicOn(err)

		diffs := h2.Diff(h)
		vv("diffs = '%#v'", diffs)
	})
}
