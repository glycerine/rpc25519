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
	Path            string                `zid:"0"`
	FullFileHashSum string                `zid:"1"`
	ChunkerName     string                `zid:"2"`
	ChunkerOpts     *ultracdc.ChunkerOpts `zid:"3"`
	Chunks          []*RsyncChunk         `zid:"4"`
	NumChunks       int                   `zid:"5"`

	// e.g. "blake3.32B"
	HashName string `zid:"6"`
}

type RsyncChunk struct {
	ChunkNumber int    `zid:"0"` // zero based index into Chunks slice.
	Beg         int    `zid:"1"`
	Endx        int    `zid:"2"`
	Hash        string `zid:"3"`
	Len         int    `zid:"4"`
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
		Path:            path,
		FullFileHashSum: hash.Blake3OfBytesString(data),
		ChunkerName:     "ultracdc",
		ChunkerOpts:     u.Opts,
		HashName:        "blake3.32B",
	}

	cuts := u.Cutpoints(data, 0)

	prev := 0
	for i, c := range cuts {
		hsh := hash.Blake3OfBytesString(data[prev:cuts[i]])
		chunk := &RsyncChunk{
			ChunkNumber: i,
			Beg:         prev,
			Endx:        cuts[i],
			Hash:        hsh,
			Len:         cuts[i] - prev,
		}
		hashes.Chunks = append(hashes.Chunks, chunk)
		prev = c
	}
	hashes.NumChunks = len(hashes.Chunks)
	return
}

type RsyncDiffs struct {
	PathA string           `zid:"0"`
	PathB string           `zid:"1"`
	Both  []*MatchHashPair `zid:"2"`
	OnlyA []*RsyncChunk    `zid:"3"`
	OnlyB []*RsyncChunk    `zid:"4"`
}

func (d *RsyncDiffs) String() string {

	jsonData, err := json.Marshal(d)
	panicOn(err)

	var pretty bytes.Buffer
	err = json.Indent(&pretty, jsonData, "", "    ")
	panicOn(err)
	return pretty.String()
}

type MatchHashPair struct {
	A *RsyncChunk `zid:"0"`
	B *RsyncChunk `zid:"1"`
}

func (a *RsyncHashes) Diff(b *RsyncHashes) (d *RsyncDiffs) {
	d = &RsyncDiffs{
		PathA: a.Path,
		PathB: b.Path,
	}

	ma := make(map[string]*RsyncChunk)
	for _, chunkA := range a.Chunks {
		ma[chunkA.Hash] = chunkA
	}
	for _, chunkB := range b.Chunks {
		chunkA, inBoth := ma[chunkB.Hash]
		if inBoth {
			pair := &MatchHashPair{
				A: chunkA,
				B: chunkB,
			}
			d.Both = append(d.Both, pair)
			delete(ma, chunkB.Hash)
		} else {
			d.OnlyB = append(d.OnlyB, chunkB)
		}
	}
	for _, chunkA := range a.Chunks {
		_, onlyA := ma[chunkA.Hash]
		if onlyA {
			d.OnlyA = append(d.OnlyA, chunkA)
		}
	}
	return
}
