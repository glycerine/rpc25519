package rpc25519

import (
	"bytes"
	"context"
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

// rsync operation for a single file. Steps
// and the structs that go with each step:
//
// NB: the client always has to start a request;
// the server cannot reach out to a client.
// But the client can still request a file, and thus
// request to be the reader.
//
// Optional step 0: client requests to be the reader.
// The server should send the file requested.
//
// Or in step 1: client requests to send a file.
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
// This starts the first of two round-trips.
// Note only 0 or 1 are are "RPC" like-calls in our lingo.
// The other steps/Messages (2,3,4) are one-ways
// with the same CallID.
//
// Note that the server will also send a final CallRPCReply
// when the Call from 0 or 1 finishes; but it
// should carry no content; the cli still
// needs to process 4 even if it gets such a reply
// during a client read. In detail:
//
// So our rsync-like protocol is either:
//
// for client read:
//
// cli(0)->srv(1)->cli(2)->srv(3 + CallRPCReply to 0)->cli(4); or
//
// for client send:
//
// cli(1)->srv(2)->cli(3)->srv(4 + CallRPCReply to 1);
//
// This means that the server has to be ready
// to listen for and handle 1,2,3,4; while
// the client has to listen for and handle 2,3,4.
// The client is always the one sending 0 or 1.
//
// (We use the same code on both cli and srv
// to processes these, to keep symmetrical correctness.)
//
// To do step 1 (client acts as sender, it sends:
// .
type RsyncStep1_SenderOverview struct {
	SenderHost     string    `zid:"0"`
	SenderPath     string    `zid:"1"`
	SenderLenBytes int64     `zid:"2"`
	SenderModTime  time.Time `zid:"3"`

	// if available/cheap, send
	SenderFullHash string `zid:"4"`
}

// 2) receiver/reader end gets path to the file, its
// length and modification time stamp. If length
// and time stamp match, stop. Ack back all good.
// Else ack back with RsyncHashes, "here are the chunks I have"
// and the whole file checksum.
// Reader replies to sender with RsyncStep2_AckOverview.
type RsyncStep2_AckOverview struct {
	// if true, no further action needed.
	// ReaderHashes can be nil then.
	ReaderMatchesSenderAllGood bool `zid:"0"`

	ReaderHashes *RsyncHashes `zid:"1"`
}

// 3) sender chunks the file, does the diff, and
// then sends along just the changed chunks, along
// with the chunk structure of the file on the
// sender so reader can reassemble it; and the
// whole file checksum.
// Sender sends RsyncStep3_SenderProvidesDeltas
// to reader.
//
// This step rsync may well send a very large message,
// much larger than our 1MB or so maxMessage size.
// Or even a 64MB max message.
// So rsync may need to use a Bistream that can handle lots of
// separate messages and reassemble them.
// For that matter, the RsyncHashes in step 2
// can be large too. As a part of the rsync
// protocol we want to be able to send
// "large files" that are actually large, streamed
// messages. We observe these may need to be backed by
// disk rather than memory to keep memory
// requirements sane.
//
// Thought/possibility: we could save them
// to /tmp since that might be memory backed
// or storage backed. Make that an option,
// but if we use a streaming Bistream download
// to disk then we'll handle the large
// message of step 2/3 problem.
//
// The thing is, we would like to use
// rsync underneath the bistream of a big
// file transparently. Circular. Ideally
// the rsync part can be used transparently
// by any streaming large file need.
//
// Let's start by layering rsync on top of
// Bistreaming, but we can add a separate
// header idea of a whole message worth
// of meta data for the stream file that
// can give the rsync step message
// so we know what to do with the file.

// What does this API on top of bistreaming look
// like? bistreaming gives us files that are
// written to disk already: since they may be
// quite large and/or just need to be indexed
// as is or moved into the right storage dir.
// So we would get path names on disk for our
// inputs??
//
// and io.Writer on both ends, that communicate
// with each other. Then we just encode
// our step struct here as greenpack msgpack
// streams back and forth,
// like usual. Something like InfiniteStreamFunc
// that can be registed on client OR server.

// Better to presume that we can fit the rsync
// parts in memory? we can't really stream out
// a partial response until we know them anyway,
// so just make the Message payload big enough
// to handle them; and increase the chunk size
// if need be to get the chunk index to fit
// within a single Message.JobSerz payload.

type InfiniteStreamFunc func(ctx context.Context, req *Message, r io.Reader, w io.Writer) error

type RsyncNode struct{}

func (s *RsyncNode) Step0_ClientRequestsRead(
	ctx context.Context, req *Message, r io.Reader, w io.Writer) error {

	step0 := &RsyncStep0_ClientRequestsRead{
		ReaderHost:     "",          // string
		ReaderPath:     "",          // string
		ReaderLenBytes: 0,           // int64
		ReaderModTime:  time.Time{}, //
		// if available/cheap, send
		ReaderFullHash: "", // string
	}
	err := msgp.Encode(w, step)
	panicOn(err)
	if err != nil {
		return err
	}
}
func (s *RsyncNode) Step1(ctx context.Context, req *Message, r io.Reader, w io.Writer) error {}
func (s *RsyncNode) Step2(ctx context.Context, req *Message, r io.Reader, w io.Writer) error {}
func (s *RsyncNode) Step3(ctx context.Context, req *Message, r io.Reader, w io.Writer) error {}
func (s *RsyncNode) Step4(ctx context.Context, req *Message, r io.Reader, w io.Writer) error {}

type RsyncStep3_SenderProvidesDeltas struct {
	SenderHashes *RsyncHashes `zid:"0"` // needs to be streamed too.

	ChunkDiff *RsyncDiff `zid:"1"`

	DeltaHashesPreCompression []string `zid:"2"`
	CompressionAlgo           string   `zid:"3"`

	// Bistream this separately as a file,
	// because it can be so big; if we have
	// no diff compression available it will
	// be the whole file anyway(!)
	// Hence we'll want to add compression to
	// the bistream download/upload actions.
	//DeltaData                 [][]byte `zid:"4"`
	DeltaDataStreamedPath string `zid:"4"`
}

// 4) reader gets the diff, the changed chunks (DeltaData),
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

// RsyncServerSide implements the server side
// of our rsync-like protocol. It is a BistreamFunc.
func (Server *Server) RsyncServerSide(
	srv *Server,
	ctx context.Context,
	req *Message,
	uploadsFromClientCh <-chan *Message,
	sendDownloadPartToClient func(ctx context.Context, msg *Message, last bool) error,
	lastReply *Message,
) (err error) {

	panic("RsyncServerSide called!")
	return
}

func (cli *Client) RsyncClientSide() {

}
