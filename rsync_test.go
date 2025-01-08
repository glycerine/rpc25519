package rpc25519

import (
	"os"
	"testing"
	"time"

	cv "github.com/glycerine/goconvey/convey"
)

func Test201_rsync_style_hash_generation(t *testing.T) {

	cv.Convey("rsync.go SummarizeFileInCDCHashes() should generate CDC UltraCDC hashes for a file", t, func() {
		host := "localhost"
		path := "testdata/blob977k"

		data, err := os.ReadFile(path)
		panicOn(err)

		var modTime time.Time
		h, err := SummarizeBytesInCDCHashes(host, path, data, modTime)
		panicOn(err)
		_ = h
		//vv("scan of file gave: hashes '%v'", h.String())
		cv.So(h.NumChunks, cv.ShouldEqual, 16) // blob977k

		// now alter the data by prepending 2 bytes
		data2 := append([]byte{0x24, 0xff}, data...)
		h2, err := SummarizeBytesInCDCHashes(host, path+".prepend2bytes", data2, modTime)
		panicOn(err)

		diffs2 := h2.Diff(h)
		//vv("diffs2 = '%s'", diffs2)
		cv.So(h2.NumChunks, cv.ShouldEqual, 16)
		cv.So(len(h2.Chunks), cv.ShouldEqual, 16)
		cv.So(len(diffs2.OnlyA), cv.ShouldEqual, 1)
		cv.So(len(diffs2.OnlyB), cv.ShouldEqual, 1)
		cv.So(len(diffs2.Both), cv.ShouldEqual, 15)
		cv.So(diffs2.OnlyA[0].ChunkNumber, cv.ShouldEqual, 0)
		cv.So(diffs2.OnlyB[0].ChunkNumber, cv.ShouldEqual, 0)

		// lets try putting 2 bytes at the end instead:
		data3 := append(data, []byte{0xf3, 0xee}...)
		h3, err := SummarizeBytesInCDCHashes(host, path+".postpend2bytes", data3, modTime)
		panicOn(err)

		diffs3 := h3.Diff(h)
		//vv("diffs3 = '%s'", diffs3)
		cv.So(h3.NumChunks, cv.ShouldEqual, 16)
		cv.So(len(h3.Chunks), cv.ShouldEqual, 16)
		cv.So(len(diffs3.OnlyA), cv.ShouldEqual, 1)
		cv.So(len(diffs3.OnlyB), cv.ShouldEqual, 1)
		cv.So(len(diffs3.Both), cv.ShouldEqual, 15)
		cv.So(diffs3.OnlyA[0].ChunkNumber, cv.ShouldEqual, 15)
		cv.So(diffs3.OnlyB[0].ChunkNumber, cv.ShouldEqual, 15)
	})
}
