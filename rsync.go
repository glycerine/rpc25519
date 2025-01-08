package rpc25519

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"

	"github.com/glycerine/rpc25519/hash"
	"github.com/glycerine/rpc25519/ultracdc"
)

//go:generate greenpack

type RsyncHashes struct {
	Path              string                `zid:"0"`
	Blake3FullFileSum string                `zid:"1"`
	ChunkerName       string                `zid:"2"`
	ChunkerOpts       *ultracdc.ChunkerOpts `zid:"3"`
	Chunks            []RsyncChunk          `zid:"4"`
	NumChunks         int                   `zid:"5"`
	HashName          string
}

type RsyncChunk struct {
	Beg        int    `zid:"0"`
	Endx       int    `zid:"1"`
	Blake3Hash string `zid:"2"`
	Len        int    `zid:"3"`
}

func (h *RsyncHashes) String() string {

	jsonData, err := json.Marshal(h)
	panicOn(err)

	var pretty bytes.Buffer
	err = json.Indent(&pretty, jsonData, "", "    ")
	panicOn(err)
	return pretty.String()
}

func SummarizeFileInCDCHashes(path string) (hashes *RsyncHashes, err error) {

	var data []byte
	data, err = os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("rsync.go error reading path '%v': '%v'", path, err)
	}

	return SummarizeBytesInCDCHashes(path, data)
}

func SummarizeBytesInCDCHashes(path string, data []byte) (hashes *RsyncHashes, err error) {

	u := ultracdc.NewUltraCDC()

	opts := &ultracdc.ChunkerOpts{
		MinSize:    2 * 1024,
		NormalSize: 10 * 1024,
		MaxSize:    64 * 1024,
	}
	u.Opts = opts

	hashes = &RsyncHashes{
		Path:              path,
		Blake3FullFileSum: hash.Blake3OfBytesString(data),
		ChunkerName:       "ultracdc",
		ChunkerOpts:       u.Opts,
		HashName:          "blake3.32B",
	}

	cuts := u.Cutpoints(data, 0)

	prev := 0
	for i, c := range cuts {
		hsh := hash.Blake3OfBytesString(data[prev:cuts[i]])
		chunk := RsyncChunk{
			Beg:        prev,
			Endx:       cuts[i],
			Blake3Hash: hsh,
			Len:        cuts[i] - prev,
		}
		hashes.Chunks = append(hashes.Chunks, chunk)
		prev = c
	}
	hashes.NumChunks = len(hashes.Chunks)
	return
}
