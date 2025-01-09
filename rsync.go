package rpc25519

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/user"
	"syscall"
	"time"

	"github.com/glycerine/rpc25519/hash"
	"github.com/glycerine/rpc25519/jcdc"
)

//go:generate greenpack

// rsync operation for a single file. Steps and structs:
//
// NB: the client always has to start a request;
// the server cannot reach out to a client.
// But the client can request a file, and thus
// request to be the reader.
//
// Optional step 0: client requests to be the reader.
// The server should send the file requested.
//
// Or in step 0: client requests to send a file.
// The client can begin immediately with step 1,
// there really is no step 0 when the client is sending.
// The client just sends the RsyncStep1_SenderOverview.
//
// So only if the client wants to read a file
// from the server does the client need to send this:
type RsyncStep0_ClientRequestsRead struct {
	ReaderHost     string    `zid:"0"`
	ReaderPath     string    `zid:"1"`
	ReaderLenBytes int64     `zid:"2"`
	ReaderModTime  time.Time `zid:"3"`

	// if available/cheap, send
	ReaderFullHash string `zid:"4"`
}

// 1) sender sends path, length, mod time of file.
// Sender sends RsyncStep1_SenderOverview to reader.
// This starts the first of two RPCs from sender
// to reader. But can we have an RPC from a
// server to a client? We can have the client
// do up calls like the server does? or
// run a server too, if they want to be
// a reader. The main read-loop on the client
// can hand off server traffic to the
// server read-loop? For now keep it simple
// and always have the client be the initiator
// and sender, making two RPC round trips.
// Or we could structure it not as two RPC
// but just as four one-way sends? The
// last of which is not really even needed,
// it just confirms that the transaction
// completed. I think that would be fine;
// we would just need to add specific
// message types to support that, like
// a SymmetricFlow type, so the client
// or server getting that knows to do--the
// client still needs to know who to
// upcall when they get such a message; b/c
// the client may not have any code running
// at all if the server initiates a push
// and wants to be a sender to the client's
// reader. So it makes sense to have the
// client side just be able to register
// handler func for RPCs too; and for the
// server to be able to act like a client
// (sender) as well, symmetrically.
type RsyncStep1_SenderOverview struct {
	SenderHost     string    `zid:"0"`
	SenderPath     string    `zid:"1"`
	SenderLenBytes int64     `zid:"2"`
	SenderModTime  time.Time `zid:"3"`

	// if available/cheap, send
	SenderFullHash string `zid:"4"`
}

// 1) receiver/reader end gets path to the file, its
// length and modification time stamp. If length
// and time stamp math, stop. Ack back all good.
// Else ack back with RsyncHashes, "here are the chunks I have"
// and the whole file checksum.
// Reader replies to sender with RsyncStep2_AckOverview.
type RsyncStep2_AckOverview struct {
	// if true, no further action needed.
	// ReaderHashes can be nil then.
	ReaderMatchesSenderAllGood bool `zid:"0"`

	ReaderHashes *RsyncHashes `zid:"1"`
}

// 2) sender chunks the file, does the diff, and
// then sends along just the changed chunks, along
// with the chunk structure of the file on the
// sender so reader can reassemble it; and the
// whole file checksum.
// Sender sends RsyncStep3_SenderProvidesDeltas
// to reader.
type RsyncStep3_SenderProvidesDeltas struct {
	SenderHashes *RsyncHashes `zid:"0"`

	ChunkDiff *RsyncDiff `zid:"1"`

	DeltaHashesPreCompression []string `zid:"2"`
	CompressionAlgo           string   `zid:"3"`
	DeltaData                 [][]byte `zid:"4"`
}

// 3) reader gets the diff, the changed chunks (DeltaData),
// and it already has the current file structure;
// write out a new file with the correct chunks
// in the correct order. (Decompressing chunks
// before writing them). Reader verifies the final blake3 checksum,
// and sets the new ModTime on the file.
// Reader does a final ack of sending back
// RsyncStep4_ReaderAcksDeltasFin to the sender.
// This completes the rsync operation, which
// took two round-trips from sender to reader.
type RsyncStep4_ReaderAcksDeltasFin struct {
	ReaderHost     string    `zid:"0"`
	ReaderPath     string    `zid:"1"`
	ReaderLenBytes int64     `zid:"2"`
	ReaderModTime  time.Time `zid:"3"`
	ReaderFullHash string    `zid:"4"`
}

// RsyncHashes stores CDC (Content Dependent Chunking)
// chunks for a given Path on a given Host, using
// a specified chunking algorithm (e.g. "jcdc"), its parameters,
// and a specified hash function (e.g. "blake3.32B"
type RsyncHashes struct {
	// uniquely idenitify this hash set.
	Rsync0CallID string    `zid:"16"`
	IsFromSender bool      `zid:"17"`
	Created      time.Time `zid:"18"`

	Host string `zid:"0"`
	Path string `zid:"1"`

	ModTime     time.Time `zid:"2"`
	FileSize    int64     `zid:"3"`
	FileMode    uint32    `zid:"4"`
	FileOwner   string    `zid:"5"`
	FileOwnerID uint32    `zid:"6"`
	FileGroup   string    `zid:"7"`
	FileGroupID uint32    `zid:"8"`
	// other data, extension mechanism. Not used presently.
	FileMeta []byte `zid:"9"`

	// HashName is e.g. "blake3.32B"
	HashName string `zid:"10"`

	FullFileHashSum string `zid:"11"`

	// ChunkerName is e.g. "fastcdc", or "ultracdc"
	ChunkerName string           `zid:"12"`
	CDC_Config  *jcdc.CDC_Config `zid:"13"`

	// NumChunks gives len(Chunks) for convenience.
	NumChunks int           `zid:"14"`
	Chunks    []*RsyncChunk `zid:"15"`
}

type RsyncChunk struct {
	ChunkNumber int    `zid:"0"` // zero based index into Chunks slice.
	Beg         int    `zid:"1"`
	Endx        int    `zid:"2"`
	Hash        string `zid:"3"`
	Len         int    `zid:"4"`
}

type RsyncDiff struct {
	HostA   string       `zid:"0"`
	PathA   string       `zid:"1"`
	HashesA *RsyncHashes `zid:"2"`

	HostB   string       `zid:"3"`
	PathB   string       `zid:"4"`
	HashesB *RsyncHashes `zid:"5"`

	Both  []*MatchHashPair `zid:"6"`
	OnlyA []*RsyncChunk    `zid:"7"`
	OnlyB []*RsyncChunk    `zid:"8"`
}

type MatchHashPair struct {
	A *RsyncChunk `zid:"0"`
	B *RsyncChunk `zid:"1"`
}

func (d *RsyncDiff) String() string {

	jsonData, err := json.Marshal(d)
	panicOn(err)

	var pretty bytes.Buffer
	err = json.Indent(&pretty, jsonData, "", "    ")
	panicOn(err)
	return pretty.String()
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
		uid := stat_t.Uid
		hashes.FileOwnerID = uid
		gid := stat_t.Gid
		hashes.FileGroupID = gid

		owner, err := user.LookupId(fmt.Sprint(uid))
		if err == nil && owner != nil {
			hashes.FileOwner = owner.Username
		}
		group, err := user.LookupGroupId(fmt.Sprint(gid))
		if err == nil && group != nil {
			hashes.FileGroup = group.Name
		}
	}
	return
}

func SummarizeBytesInCDCHashes(host, path string, data []byte, modTime time.Time) (hashes *RsyncHashes, err error) {

	// These two different chunking approaches,
	// Jcdc and FastCDC, need very different
	// parameter min/max/average settings in
	// order to give good chunking.

	var opts *jcdc.CDC_Config

	const useFastCDC = true
	var cdc jcdc.Cutpointer

	if useFastCDC {

		// Stadia improved version of FastCDC
		opts = &jcdc.CDC_Config{
			MinSize:    4 * 1024,
			TargetSize: 60 * 1024,
			MaxSize:    80 * 1024,
		}
		cdc = jcdc.NewFastCDC(opts)

	} else {
		//jcdc
		opts = &jcdc.CDC_Config{
			MinSize:    2 * 1024,
			TargetSize: 10 * 1024,
			MaxSize:    64 * 1024,
		}
		cdc = jcdc.NewUltraCDC(opts)
	}

	hashes = &RsyncHashes{
		Host:            host,
		Path:            path,
		FileSize:        int64(len(data)),
		ModTime:         modTime,
		FullFileHashSum: hash.Blake3OfBytesString(data),
		ChunkerName:     cdc.Name(),
		CDC_Config:      cdc.Config(),
		HashName:        "blake3.32B",
	}

	cuts := cdc.Cutpoints(data, 0)

	//vv("cuts = '%#v'", cuts)

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

func (a *RsyncHashes) Diff(b *RsyncHashes) (d *RsyncDiff) {
	d = &RsyncDiff{
		HostA:   a.Host,
		PathA:   a.Path,
		HashesA: a,

		HostB:   b.Host,
		PathB:   b.Path,
		HashesB: b,
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
