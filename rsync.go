package rpc25519

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/glycerine/rpc25519/hash"
	"github.com/glycerine/rpc25519/ultracdc"
)

//go:generate greenpack

// RsyncHashes stores CDC (Content Dependent Chunking)
// chunks for a given Path on a given Host, using
// a specified chunking algorithm (e.g. "ultracdc"), its parameters,
// and a specified hash function (e.g. "blake3.32B"
type RsyncHashes struct {
	Host string `zid:"0"`
	Path string `zid:"1"`

	ModTime   time.Time `zid:"2"`
	FileSize  int64     `zid:"3"`
	FileMode  uint32    `zid:"4"`
	FileOwner uint32    `zid:"5"`
	FileGroup uint32    `zid:"6"`
	FileMeta  string    `zid:"7"`

	// HashName is e.g. "blake3.32B"
	HashName string `zid:"8"`

	FullFileHashSum string `zid:"9"`

	// ChunkerName is e.g. "ultracdc"
	ChunkerName string                `zid:"10"`
	ChunkerOpts *ultracdc.ChunkerOpts `zid:"11"`

	// NumChunks gives len(Chunks) for convenience.
	NumChunks int           `zid:"12"`
	Chunks    []*RsyncChunk `zid:"13"`
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

func SummarizeFileInCDCHashes(host, path string) (hashes *RsyncHashes, err error) {

	var data []byte
	data, err = os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("rsync.go error reading path '%v': '%v'", path, err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("rsync.go error on os.Stat() of '%v': '%v'", path, err)
	}

	hashes, err = SummarizeBytesInCDCHashes(host, path, data, fi.ModTime())
	hashes.FileMode = uint32(fi.Mode())

	if stat_t, ok := fi.Sys().(*syscall.Stat_t); ok {
		hashes.FileOwner = stat_t.Uid
		hashes.FileGroup = stat_t.Gid
	}
	return
}

func SummarizeBytesInCDCHashes(host, path string, data []byte, modTime time.Time) (hashes *RsyncHashes, err error) {

	u := ultracdc.NewUltraCDC()

	opts := &ultracdc.ChunkerOpts{
		MinSize:    2 * 1024,
		NormalSize: 10 * 1024,
		MaxSize:    64 * 1024,
	}
	u.Opts = opts

	hashes = &RsyncHashes{
		Host:            host,
		Path:            path,
		FileSize:        int64(len(data)),
		ModTime:         modTime,
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
	HostA string           `zid:"0"`
	PathA string           `zid:"1"`
	HostB string           `zid:"2"`
	PathB string           `zid:"3"`
	Both  []*MatchHashPair `zid:"4"`
	OnlyA []*RsyncChunk    `zid:"5"`
	OnlyB []*RsyncChunk    `zid:"6"`
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
		HostA: a.Host,
		PathA: a.Path,
		HostB: b.Host,
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
