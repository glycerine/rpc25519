package rpc25519

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"lukechampine.com/blake3"
	//"io"
	"os"
	"os/user"
	"syscall"
	"time"

	//"github.com/glycerine/greenpack/msgp"
	"github.com/glycerine/rpc25519/hash"
	"github.com/glycerine/rpc25519/jcdc"
)

// pointer dedup was hitting some issues on Encode/Decode,
// so turn it off for now. Pointer dedup was reducing
// the diff messages in half, so hopefully we can
// debug the greenpack issue and get it working later.

//go:generate greenpack -no-dedup=true

// Chunk is a segment of bytes from some file,
// the location of which is given by [Beg, Endx).
// A Chunk is named by its Cry cryptographic hash. The actual
// bytes may or may not be attached in the Data field.
type Chunk struct {
	Beg  int    `zid:"0"`
	Endx int    `zid:"1"`
	Cry  string `zid:"2"` // a cryptographic hash identifying the chunk

	// Data might be nil for summary purposes,
	// or provided if we are transmitting a set of diffs.
	Data []byte `zid:"3" json:"-"`
}

func (c *Chunk) Len() int { return c.Endx - c.Beg }

// Chunks represents
// It could be all of the data in Path.
// Or it could just be a subset: the diffs that are needed to make Path.
// But either way, the Beg are always in sorted, ascending order. Hence:
//
// INVAR: Chunks[i].Beg < Chunks[i+1].Beg
//
// Path is really just a reporting/sanity convenience.
type Chunks struct {
	Chunks []*Chunk `zid:"0"`
	Path   string   `zid:"1"`

	// FileSize gives the total size of Path,
	// since we may have only a subset of
	// Path's data chunks (e.g. the updated ones).
	FileSize int    `zid:"2"`
	FileCry  string `zid:"3"` // the cryptographic hash of the whole file.
}

func NewChunks(path string) *Chunks {
	return &Chunks{
		Path: path,
	}
}

// Last gives the last Chunk in the Chunks.
func (c *Chunks) Last() *Chunk {
	n := len(c.Chunks)
	if n == 0 {
		return nil
	}
	return c.Chunks[n-1]
}

// UpdateLocalWithRemoteDiffs is the essence of the rsync
// algorithm for efficient file transfer. The remote
// chunks gives the update plan.
func UpdateLocalWithRemoteDiffs(
	localPathToWrite string,

	// map Cry hash -> chunk.
	// typically either these are the chunks
	// from the local version of the path; but
	// they could be from a larger data store
	// like a Git repo or database.
	localMap map[string]*Chunk,

	remote *Chunks,

) (err error) {

	if remote.FileSize == 0 {
		vv("remote.FileSize == 0 => truncate to zero localPathToWrite='%v'", localPathToWrite)
		return truncateFileToZero(localPathToWrite)
	}

	if len(remote.Chunks) == 0 {
		panic(fmt.Sprintf("missing remote chunks for non-size-zero file '%v'", localPathToWrite))
	}

	// make sure we have a full plan, not just a partial diff.
	if remote.FileSize != remote.Last().Endx {
		panic("remote was not a full plan for every byte!")
	}

	// assemble in memory first, later stream to disk.
	newvers := make([]byte, remote.FileSize)
	j := 0 // index to new version, how much we have written.

	// compute the full file hash/checksum as we go
	h := blake3.New(64, nil)

	// remote gives the plan of what to create
	for i, chnk := range remote.Chunks {
		_ = i

		if len(chnk.Data) == 0 {
			// the data is local
			lc, ok := localMap[chnk.Cry]
			if !ok {
				panic(fmt.Sprintf("rsync algo failed, the needed data is not "+
					"available locally: '%v'", chnk))
			}
			wb := copy(newvers[j:], lc.Data)
			j += wb
			if wb != len(lc.Data) {
				panic("newvers did not have enough space")
			}
			// sanity check the local chunk as a precaution.
			if wb != lc.Endx-lc.Beg {
				panic(fmt.Sprintf("lc.Endx = %v, lc.Beg = %v, but lc.Data len = %v", lc.Endx, lc.Beg, wb))
			}
			h.Write(lc.Data)
		} else {
			wb := copy(newvers[j:], chnk.Data)
			j += wb
			if wb != len(chnk.Data) {
				panic("newvers did not have enough space")
			}
			// sanity check the local chunk as a precaution.
			if wb != chnk.Endx-chnk.Beg {
				panic(fmt.Sprintf("lc.Endx = %v, lc.Beg = %v, but lc.Data len = %v", chnk.Endx, chnk.Beg, wb))
			}
			h.Write(chnk.Data)
		}
	}
	sum := hash.SumToString(h)
	if sum != remote.FileCry {
		err = fmt.Errorf("checksum mismatch error! reconstructed='%v'; expected='%v'; remote path = ''%v'", sum, remote.FileCry, remote.Path)
		panic(err)
		return err
	}

	var fd *os.File
	fd, err = os.Create(localPathToWrite)
	if err != nil {
		return err
	}
	defer fd.Close()
	_, err = fd.Write(newvers)
	panicOn(err)

	return
}

// RsyncSummary stores CDC (Content Dependent Chunking)
// chunks for a given Path on a given Host, using
// a specified chunking algorithm (e.g. "jcdc"), its parameters,
// and a specified hash function (e.g. "blake3.32B"
type RsyncSummary struct {
	// uniquely idenitify this hash set.
	Rsync0CallID string    `zid:"15"`
	IsFromSender bool      `zid:"16"`
	Created      time.Time `zid:"17"`

	Host string `zid:"0"`
	Path string `zid:"1"`

	ModTime     time.Time `zid:"2"`
	FileSize    int       `zid:"3"`
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

	// ChunkerName is e.g.
	// "fastcdc-Stadia-Google-64bit-arbitrary-regression-jea"
	//   or "ultracdc-glycerine-golang-implementation".
	// It should encapsulate any settings and
	// implementation version, to allow it to be reproduced.
	ChunkerName string           `zid:"12"`
	CDC_Config  *jcdc.CDC_Config `zid:"13"`

	Chunks *Chunks `zid:"14"`
}

// A BlobStore is CAS for blobs. It is Content Addressable
// Storage for binary large objects, like a Git Repo
// or database
type BlobStore struct {
	Map map[string]*Chunk
}

func NewBlobStore() *BlobStore {
	return &BlobStore{
		Map: make(map[string]*Chunk),
	}
}

// UpdateRemoteToMatchLocalPlan produces an
// update plan on how to update the
// remote path to match that a local file. The plan will have
// chunk.Data pointers set only for those chunks that are local only; that
// the remote side currently lacks. This is the essential
// rsync "diff" operation.
//
// We expect the remote chunks have no Data pointers available,
// and all the local to have them, or, as a fallback, for
// them to be available in the BlobStore.
// These pointers, from local or the BlobStore, are
// copied ino the plan chunks we return.
func (s *BlobStore) GetPlanToUpdateRemoteToMatchLocal(local, remote *Chunks) (plan *Chunks) {

	plan = NewChunks(remote.Path)
	plan.FileSize = local.FileSize
	plan.FileCry = local.FileCry

	// index the remote
	remotemap := make(map[string]*Chunk)
	for _, c := range remote.Chunks {
		remotemap[c.Cry] = c
	}
	var p []*Chunk

	// the local file layout is the template for the plan
	for i, c := range local.Chunks {
		_, ok := remotemap[c.Cry]

		// make a copy of the local chunk, no matter what.
		addme := *c
		if ok {
			vv("i=%v, remote already has it, do not send.", i)
			addme.Data = nil
		} else {
			vv("i=%v, leave addme.Data intact, because remote needs it.", i)

			// sanity check that we do infact have the Data.
			if len(c.Data) == 0 {
				// can we get it from the blobstore?
				bs, ok := s.Map[c.Cry]
				if !ok || bs == nil || len(bs.Data) == 0 {
					panic(fmt.Sprintf("local chunks missing Data!: '%v'", c))
				}
				addme.Data = bs.Data
			}
		}
		p = append(p, &addme)
	}

	plan.Chunks = p

	return
}

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

	ErrString string `zid:"5"`
}

// 2) receiver/reader end gets path to the file, its
// length and modification time stamp. If length
// and time stamp match, stop.
// Else: ack back with RsyncSummary, "here are the chunks I have"
// and the whole file checksum.
// Reader replies to sender with RsyncStep2_ReaderAcksOverview.
type RsyncStep2_ReaderAcksOverview struct {
	// if true, no further action needed.
	// ReaderHashes can be nil then.
	ReaderMatchesSenderAllGood bool   `zid:"0"`
	SenderPath                 string `zid:"1"`

	ReaderHashes *RsyncSummary `zid:"2"`
}

// 3) sender chunks the file, does the diff, and
// then sends along just the changed chunks, along
// with the chunk structure of the file on the
// sender so reader can reassemble it; and the
// whole file checksum.
// Sender sends RsyncStep3_SenderProvidesDatas
// to reader.
//
// This step rsync may well send a very large message,
// much larger than our 1MB or so maxMessage size.
// Or even a 64MB max message.
// So rsync may need to use a Bistream that can handle lots of
// separate messages and reassemble them.
// For that matter, the RsyncSummary in step 2
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

// as long as we don't fsync the data to disk,
// we'll be reading from the file system buffer
// cache anyway with the handle's approach...so
// we could pass in the handle to the serialized
// struct?

// wouldn't our rsync node want access to a
// bistreamer client/server? not if we want
// to be able to ride underneath at some point.
// but worry about that later. What's the simplest
// thing?

// type InfiniteStreamFunc func(ctx context.Context, req *Message, r io.Reader, w io.Writer) error

/*
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
	err := msgp.Encode(w, step0)
	panicOn(err)
	if err != nil {
		return err
	}
	return nil
}
*/

//type Ctx = context.Context

type Nil struct {
	Placeholder int
}

type RsyncNode struct {
	Host        string `zid:"0"`
	Placeholder int
}

// for client send:
// cli(1)->srv(2)->cli(3)->srv(4 + CallRPCReply to 1?);
func (s *RsyncNode) ClientSends() (err error) {

	var cli *Client

	req1 := &RsyncStep1_SenderOverview{}
	reply2 := &RsyncStep2_ReaderAcksOverview{}

	err = cli.Call("RsyncNode.Step2_ReaderAcksOverview", req1, reply2, nil)

	req3 := &RsyncStep3A_SenderProvidesData{}
	reply4 := &RsyncStep4_ReaderAcksDeltasFin{}

	err = cli.Call("RsyncNode.Step4_ReaderAcksDeltasFin", req3, reply4, nil)

	return
}

// for client read:
// cli(0)->srv(1)->cli(2)->srv(3 + CallRPCReply to 0?)->cli(4)
func (s *RsyncNode) ClientReads() (err error) {

	var cli *Client

	req0 := &RsyncStep0_ClientRequestsRead{}
	reply1 := &RsyncStep1_SenderOverview{}

	err = cli.Call("RsyncNode.Step1_SenderOverview", req0, reply1, nil)

	req2 := &RsyncStep2_ReaderAcksOverview{}
	reply3 := &RsyncStep3A_SenderProvidesData{}
	err = cli.Call("RsyncNode.Step3_SenderProvidesData", req2, reply3, nil)

	return
}

// or try with net/rpc api for stronger typing?
// need to have registration of func on client too.

// step0: start of client read.
func (s *RsyncNode) Step0_ClientRequestsRead(
	ctx context.Context,
	req *Nil,
	reply *RsyncStep0_ClientRequestsRead) error {

	return nil
}

// step 1: start of client send; or server responding to step0 (server to download to client).
// Note that req will be nil if client is initiating the send.
func (s *RsyncNode) Step1_SenderOverview(
	ctx context.Context,
	req *RsyncStep0_ClientRequestsRead,
	reply *RsyncStep1_SenderOverview) error {

	local := ""
	if hdr, ok := HDRFromContext(ctx); ok {
		local = hdr.Nc.LocalAddr().String()
		s.Host = local
		fmt.Printf("Step1 has HDR = '%v'; "+
			"HDR.Nc.RemoteAddr() gives '%v'; HDR.Nc.LocalAddr() gives '%v'\n",
			hdr.String(), hdr.Nc.RemoteAddr(), hdr.Nc.LocalAddr())
	}

	path := req.ReaderPath
	if !fileExists(path) {
		return fmt.Errorf("error on host '%v': file not found: '%v'", local, req.ReaderPath)
	}

	vv("RsyncNode.Step1_SenderOverview() called!")

	fi, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("error on host '%v': rsync.go error "+
			"on os.Stat() of '%v': '%v'", local, path, err)
	}

	blake3sum, err := hash.Blake3OfFileString(path)
	if err != nil {
		return fmt.Errorf("error on host '%v': rsync.go error "+
			"computing blake3sum of '%v': '%v'", local, path, err)
	}
	*reply = RsyncStep1_SenderOverview{
		SenderHost:     local,
		SenderPath:     path,
		SenderLenBytes: fi.Size(),
		SenderModTime:  fi.ModTime(),
		SenderFullHash: blake3sum,
	}

	return nil
}

func (s *RsyncNode) Step2_ReaderAcksOverview(
	ctx context.Context,
	req *RsyncStep1_SenderOverview,
	reply *RsyncStep2_ReaderAcksOverview) error {

	return nil
}

func (s *RsyncNode) Step3_SenderProvidesData(
	ctx context.Context,
	req *RsyncStep2_ReaderAcksOverview,
	reply *RsyncStep3A_SenderProvidesData) error {

	vv("top of Step3_SenderProvidesData(); req.SenderPath='%v'", req.SenderPath)

	if req.ReaderMatchesSenderAllGood {
		return nil // nothing for us to do, they are fine.
	}
	// they need file (parts at least).
	remoteSummary := req.ReaderHashes
	remote := remoteSummary.Chunks

	localSummary, err := SummarizeFileInCDCHashes(s.Host, req.SenderPath)
	if err != nil {
		vv("mid Step3_SenderProvidesData(); err = '%v'", err)
	} else {
		vv("localHashes ok") // long!: = '%v'", localHashes.String())
	}
	panicOn(err)

	local := localSummary.Chunks

	vv("Step3_SenderProvidesData(): server has local='%v'", local)
	vv("Step3_SenderProvidesData(): server sees remote='%v'", remote)

	bs := NewBlobStore() // TODO make persistent state.

	//a.Diff(b) (a is local, b is remote); need to send OnlyA
	//plan := localHashes.Diff(remoteHashes)
	plan := bs.GetPlanToUpdateRemoteToMatchLocal(local, remote)

	vv("plan = '%v'", plan)
	reply.Chunks = plan
	vv("end of Step3_SenderProvidesData()")

	//path := req.SenderPath // is this right? nope.

	return nil
}

func getCryMap(cs *Chunks) (m map[string]*Chunk) {
	m = make(map[string]*Chunk)
	for _, c := range cs.Chunks {
		m[c.Cry] = c
	}
	return
}

func (s *RsyncNode) Step4_ReaderAcksDeltasFin(
	ctx context.Context,
	req *RsyncStep3A_SenderProvidesData,
	reply *RsyncStep4_ReaderAcksDeltasFin) error {

	return nil
}

type RsyncStep3A_SenderProvidesData struct {
	SenderPath   string        `zid:"0"`
	SenderHashes *RsyncSummary `zid:"1"` // needs to be streamed too? could be very large?

	Chunks *Chunks `zid:"2"`
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

func (h *RsyncSummary) String() string {

	jsonData, err := json.Marshal(h)
	panicOn(err)

	var pretty bytes.Buffer
	err = json.Indent(&pretty, jsonData, "", "    ")
	panicOn(err)
	return pretty.String()
}

func SummarizeFileInCDCHashes(host, path string) (hashes *RsyncSummary, err error) {

	if !fileExists(path) {
		return SummarizeBytesInCDCHashes(host, path, nil, time.Time{})
	}

	// file system details fill in:
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

func SummarizeBytesInCDCHashes(host, path string, data []byte, modTime time.Time) (hashes *RsyncSummary, err error) {

	// These two different chunking approaches,
	// Jcdc and FastCDC, need very different
	// parameter min/max/average settings in
	// order to give good chunking.

	var opts *jcdc.CDC_Config

	const useFastCDC = true
	var cdc jcdc.Cutpointer

	if useFastCDC {

		// my take on the Stadia improved version of FastCDC
		opts = &jcdc.CDC_Config{
			MinSize:    4 * 1024,
			TargetSize: 60 * 1024,
			MaxSize:    80 * 1024,
		}
		cdc = jcdc.NewFastCDC(opts)

	} else {
		// UltraCDC that I implemented.
		opts = &jcdc.CDC_Config{
			MinSize:    2 * 1024,
			TargetSize: 10 * 1024,
			MaxSize:    64 * 1024,
		}
		cdc = jcdc.NewUltraCDC(opts)
	}

	hashes = &RsyncSummary{
		Host:            host,
		Path:            path,
		FileSize:        len(data),
		ModTime:         modTime,
		FullFileHashSum: hash.Blake3OfBytesString(data),
		ChunkerName:     cdc.Name(),
		CDC_Config:      cdc.Config(),
		HashName:        "blake3.32B",
		Chunks:          NewChunks(path),
	}
	hashes.Chunks.FileSize = hashes.FileSize
	hashes.Chunks.FileCry = hashes.FullFileHashSum

	if len(data) == 0 {
		return
	}

	cuts := cdc.Cutpoints(data, 0)

	//vv("cuts = '%#v'", cuts)

	prev := 0
	for i, c := range cuts {

		slc := data[prev:cuts[i]]

		hsh := hash.Blake3OfBytesString(slc)
		chunk := &Chunk{
			Beg:  prev,
			Endx: cuts[i],
			Cry:  hsh,
			Data: slc,
		}
		hashes.Chunks.Chunks = append(hashes.Chunks.Chunks, chunk)
		prev = c
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

func RsyncCliWantsToReadRemotePath(host, path string) (r *RsyncStep0_ClientRequestsRead, err error) {

	r = &RsyncStep0_ClientRequestsRead{
		ReaderHost:     host,
		ReaderPath:     path,
		ReaderLenBytes: -1,          // unknown
		ReaderModTime:  time.Time{}, // unknown
		// if available/cheap, send
		ReaderFullHash: "", // unknown
	}
	return r, nil
}

func (d *Chunks) String() string {

	jsonData, err := json.Marshal(d)
	panicOn(err)

	var pretty bytes.Buffer
	err = json.Indent(&pretty, jsonData, "", "    ")
	panicOn(err)
	return pretty.String()
}
