package rpc25519

import (
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
	Chunks            []*RsyncChunk         `zid:"4"`
}

type RsyncChunk struct {
	Beg        int    `zid:"0"`
	Endx       int    `zid:"1"`
	Blake3Hash string `zid:"2"`
}

func SummarizeFile(path string) (hashes *RsyncHashes, err error) {

	var data []byte
	data, err = os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("rsync.go error reading path '%v': '%v'", path, err)
	}

	u := ultracdc.NewUltraCDC()

	hashes = &RsyncHashes{
		Path:              path,
		Blake3FullFileSum: hash.Blake3OfBytesString(data),
		ChunkerName:       "ultracdc",
		ChunkerOpts:       u.Opts,
	}

	cuts := u.Cutpoints(data, 0)

	prev := 0
	for i, c := range cuts {
		hsh := hash.Blake3OfBytesString(data[prev:cuts[i]])
		chunk := &RsyncChunk{
			Beg:        prev,
			Endx:       cuts[i],
			Blake3Hash: hsh,
		}
		hashes.Chunks = append(hashes.Chunks, chunk)
		prev = c
	}
	return
}
