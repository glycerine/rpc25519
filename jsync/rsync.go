package jsync

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	//"os/user"
	//"syscall"
	"time"

	"github.com/glycerine/blake3"
	//"github.com/glycerine/greenpack/msgp"
	rpc "github.com/glycerine/rpc25519"
	"github.com/glycerine/rpc25519/hash"
	"github.com/glycerine/rpc25519/jcdc"
	"github.com/glycerine/rpc25519/jsync/sparsified"
)

//go:generate greenpack -no-dedup=true

// Chunk is a segment of bytes from some file,
// the location of which is given by [Beg, Endx).
// A Chunk is named by its Cry cryptographic hash. The actual
// bytes may or may not be attached in the Data field.
//
// See the Chunks (plural) container just below which is
// the most frequently used struct.
//
//mspg:tuple Chunk
type Chunk struct {
	Beg  int64 `zid:"0"`
	Endx int64 `zid:"1"`

	// Simple protocol for run-length-encoding (RLE) of zeros.
	// Cry of "RLE0;" means repeat 0 from [Beg:Endx)

	Cry string `zid:"2"` // a cryptographic hash identifying the chunk. Ex: blake3

	// Data might be nil for summary purposes,
	// or provided if we are transmitting a set of diffs.
	Data []byte `zid:"3" json:"-"`
}

func (c *Chunk) CloneNoData() (r *Chunk) {
	r = &Chunk{
		Beg:  c.Beg,
		Endx: c.Endx,
		Cry:  c.Cry,
	}
	return
}

func (c *Chunk) Len() int64 { return c.Endx - c.Beg }

// Chunks holds all the chunks for a file, at
// some specific version of that file.
//
// INVAR: Chunks[i+i].Beg > Chunks[i].Beg. They
// are sorted in ascending order. This is the order they
// appear when reading the file from beginning to end.
//
// When transmitting Chunks, the inner Chunks slice
// may or may not have .Data attached to them.
// This is because the point of the rsync-like
// protocol is that it attempts to minimize the amount
// of .Data actually sent over the wire.
type Chunks struct {
	Chunks []*Chunk `zid:"0"`
	Path   string   `zid:"1"`

	// FileSize gives the total size of Path,
	// since we may have only a subset of
	// Path's data chunks (e.g. the updated ones).
	FileSize int64  `zid:"2"`
	FileCry  string `zid:"3"` // the cryptographic hash of the whole file.
}

func NewChunks(path string) *Chunks {
	return &Chunks{
		Path: path,
	}
}

// DataPresent returns the byte count of
// actual Data segments in the set of Chunks,
// reflecting how much actually is to be
// transferred. If two files are the same,
// DataPresent will return 0 for the plan
// to update one to the other.
func (cs *Chunks) DataPresent() (tot int) {
	for _, c := range cs.Chunks {
		tot += len(c.Data)
	}
	return
}

// How many chunks have .Data actually available?
func (cs *Chunks) DataChunkCount() (count int) {
	for _, c := range cs.Chunks {
		if len(c.Data) > 0 {
			count++
		}
	}
	return
}

// Last gives the last Chunk in the Chunks.
func (c *Chunks) Last() *Chunk {
	n := len(c.Chunks)
	if n == 0 {
		return nil
	}
	return c.Chunks[n-1]
}

var zeros4k = make([]byte, 4096)

// UpdateLocalWithRemoteDiffs is the essence of the rsync
// algorithm for efficient file transfer. The remote
// chunks arguments gives us the update plan.
// The new version the file is constructed from
// existing data (in localMap) and the updated
// new chunk data we've received (in remote).
//
// Note that taker.go:405 is the "broken into pieces" version
// actually used.
func UpdateLocalWithRemoteDiffs(
	localPathToWrite string,

	// localMap: Cry -> chunk.
	// Typically either these are the chunks
	// from the local version of the path; but
	// they could be from a larger data store
	// like a Git repo or database.
	localMap map[string]*Chunk,

	remote *Chunks,

	goalPrecis *FilePrecis, // set mode, modtime from.

) (err error) {

	if remote.FileSize == 0 {
		vv("remote.FileSize == 0 => truncate to zero localPathToWrite='%v'", localPathToWrite)
		return truncateFileToZero(localPathToWrite)
	}

	// turn RLE0 into sparse holes
	var sparse []*sparsified.SparseSpan

	if len(remote.Chunks) == 0 {
		panic(fmt.Sprintf("missing remote chunks for non-size-zero file '%v'", localPathToWrite))
	}

	// make sure we have a full plan, not just a partial diff.
	if remote.FileSize > remote.Last().Endx {
		panic(fmt.Sprintf("remote was not a full plan for every byte! remote.FileSize=%v > remote.Last().Endx=%v", remote.FileSize, remote.Last().Endx))
	}

	// assemble in memory first, later stream to disk.
	newvers := make([]byte, remote.FileSize)
	var j int64 // index to new version, how much we have written.

	// compute the full file hash/checksum as we go
	h := blake3.New(64, nil)

	//vv("remote = '%v'", remote)

	// remote gives the plan of what to create
	for i, chunk := range remote.Chunks {
		_ = i

		// handle "RLE0;" case, run-length-encoded zeros.
		if chunk.Cry == "RLE0;" {
			vv("we see RLE0")
			// can we turn it into a sparse hole?
			span, wings := sparsified.AlignedSparseSpan(int64(chunk.Beg), int64(chunk.Endx))
			if span != nil {
				sparse = append(sparse, span)
			}
			_ = wings
			n := chunk.Endx - chunk.Beg
			ns := n / int64(len(zeros4k))
			rem := n % int64(len(zeros4k))
			for range ns {
				wb := int64(copy(newvers[j:], zeros4k))
				j += wb
				h.Write(zeros4k)
			}
			if rem > 0 {
				wb := int64(copy(newvers[j:], zeros4k[:rem]))
				j += wb
				h.Write(zeros4k[:rem])
			}
			continue
		} else if chunk.Cry == "UNWRIT;" {
			vv("skipping UNWRIT;")
			// ignore for now?
			continue
		}

		if len(chunk.Data) == 0 {

			//vv("the data is local") // not seen
			lc, ok := localMap[chunk.Cry]
			if !ok {
				panic(fmt.Sprintf("rsync algo failed, the needed data is not "+
					"available locally: '%v'; len(localMap) = %v", chunk, len(localMap))) // it was not this one, but further down at 220
			}

			// TODO: preserve/detect sparseness from local data too.

			wb := int64(copy(newvers[j:], lc.Data))
			j += wb
			if wb != int64(len(lc.Data)) {
				panic("newvers did not have enough space")
			}
			// sanity check the local chunk as a precaution.
			if wb != lc.Endx-lc.Beg {
				panic(fmt.Sprintf("lc.Endx = %v, lc.Beg = %v, but lc.Data len = %v", lc.Endx, lc.Beg, wb))
			}
			h.Write(lc.Data)
		} else {
			//vv("at j = %v, apply to newvers chunk.Data = '%v'", j, chunk.String())
			wb := int64(copy(newvers[j:], chunk.Data))
			j += wb
			if wb != int64(len(chunk.Data)) {
				panic("newvers did not have enough space")
			}
			//vv("copied wb=%v -> j = %v", wb, j) // seen lots
			// sanity check the local chunk as a precaution.
			if wb != chunk.Endx-chunk.Beg {
				panic(fmt.Sprintf("lc.Endx = %v, lc.Beg = %v, but lc.Data len = %v", chunk.Endx, chunk.Beg, wb))
			}
			h.Write(chunk.Data)
		}
	}
	sum := hash.SumToString(h)
	if sum != remote.FileCry {
		err = fmt.Errorf("checksum mismatch error A! reconstructed='%v'; expected='%v'; remote path = '%v'", sum, remote.FileCry, remote.Path)
		panic(err)
		return err
	}

	var fd *os.File
	rnd := cryRandBytesBase64(16)
	tmp := localPathToWrite + "_accept_plan_tmp_" + rnd
	fd, err = os.Create(tmp)
	if err != nil {
		return fmt.Errorf("error failed to create tmp file in UpdateLocalWithRemoteDiffs: '%v'", err)
	}

	sparsify := false
	sznew := int64(len(newvers))
	if len(sparse) > 0 {
		vv("UpdateLocalWithRemoteDiffs detected %v sparse holes", len(sparse))
		sparsify = true
		err = fd.Truncate(sznew)
		if err != nil {
			return fmt.Errorf("error failed to Truncate to max size (make sparse file) in UpdateLocalWithRemoteDiffs: '%v'", err)
		}
	} else {
		vv("UpdateLocalWithRemoteDiffs detected NO sparse holes")
	}

	// Close just returns an error if called 2x. That is fine.
	defer fd.Close()
	if !sparsify {
		_, err = fd.Write(newvers)
		panicOn(err)
	} else {
		var offset int64
		for _, span := range sparse {
			if offset < span.Beg {
				// data before the hole.
				_, err = fd.Write(newvers[offset:span.Beg])
				panicOn(err)
			}
			// skip to end of hole.
			offset = span.Endx
			_, err = fd.Seek(offset, 0)
			panicOn(err)
		}
		// might have data after last sparse span.
		if offset < sznew {
			// write last data segment
			_, err = fd.Write(newvers[offset:])
			panicOn(err)
		}
	}

	fd.Close()
	err = os.Rename(tmp, localPathToWrite)
	panicOn(err)

	// restore mode, modtime
	mode := goalPrecis.FileMode
	if mode == 0 {
		mode = 0600
	}
	err = os.Chmod(localPathToWrite, fs.FileMode(mode))
	panicOn(err)

	err = os.Chtimes(localPathToWrite, time.Time{}, goalPrecis.ModTime)
	panicOn(err)

	return
}

// FilePrecis stores file meta data like owner, group,
// and mode bits; as well as the details of how it
// was chunked with CDC (Content Dependent Chunking)
// chunking for a given Path on a given Host, using
// a specified chunking algorithm (e.g. "jcdc"), its parameters,
// and a specified hash function (e.g. "blake3.33B")
// for identifying the chunks.
//
// A FilePrecis is produced along side a set of Chunks
// by GetHashesOneByOne() and friends. They
// now return the Chunks themselves separately
// to make it easier to reuse those chunks,
// detaching and attaching Data as needed.
// This avoids having a forgotten Data pointer
// here in the precis.
type FilePrecis struct {

	// uniquely idenitify this FilePrecis.
	CallID string `zid:"0"`

	IsFromSender bool      `zid:"1"`
	Created      time.Time `zid:"2"`

	Host string `zid:"3"`
	Path string `zid:"4"`

	ModTime     time.Time `zid:"5"`
	FileSize    int64     `zid:"6"`
	FileMode    uint32    `zid:"7"`
	FileOwner   string    `zid:"8"`
	FileOwnerID uint32    `zid:"9"`
	FileGroup   string    `zid:"10"`
	FileGroupID uint32    `zid:"11"`

	// other data, extension mechanism. Not used presently; for future use.
	FileMeta []byte `zid:"12"`

	// HashName is e.g. "blake3.33B". This should match the
	// hash string prefix without any trailing "-" dash.
	//
	// For example, if your hash strings look like
	// "blake3.33B-89J1Jfa1AzfjiOcVOCLJXDsX3AANHWiSERgJwtaUSj8e"
	// then your HashName is "blake3.33B".
	HashName string `zid:"13"`

	FileCry string `zid:"14"`

	// ChunkerName is e.g.
	// "fastcdc-Stadia-Google-64bit-arbitrary-regression-jea"
	//   or "ultracdc-glycerine-golang-implementation".
	// It should encapsulate any settings and
	// implementation version needed to allow it to be
	// reproduced exactly.
	ChunkerName string           `zid:"15"`
	CDC_Config  *jcdc.CDC_Config `zid:"16"`

	// keep these separate so we don't
	// send all the data all the time.
	//Chunks *Chunks
}

// Equal compares two FilePrecis. Useful in the tests.
func (a *FilePrecis) Equal(b *FilePrecis) bool {

	//CallID string `zid:"0"`

	if a.IsFromSender != b.IsFromSender {
		return false
	}
	if a.Created != b.Created {
		return false
	}
	if a.Host != b.Host {
		return false
	}
	if a.Path != b.Path {
		return false
	}
	if a.ModTime != b.ModTime {
		return false
	}
	if a.FileSize != b.FileSize {
		return false
	}
	if a.FileMode != b.FileMode {
		return false
	}
	if a.FileOwner != b.FileOwner {
		return false
	}
	if a.FileOwnerID != b.FileOwnerID {
		return false
	}
	if a.FileGroup != b.FileGroup {
		return false
	}
	if a.FileGroupID != b.FileGroupID {
		return false
	}
	if a.HashName != b.HashName {
		return false
	}
	if a.FileCry != b.FileCry {
		return false
	}
	if a.ChunkerName != b.ChunkerName {
		return false
	}
	if a.CDC_Config == nil && b.CDC_Config != nil {
		return false
	}
	if a.CDC_Config != nil && b.CDC_Config == nil {
		return false
	}
	if a.CDC_Config == nil && b.CDC_Config == nil {
		return true
	} else {
		if a.CDC_Config.MinSize != b.CDC_Config.MinSize {
			return false
		}
		if a.CDC_Config.TargetSize != b.CDC_Config.TargetSize {
			return false
		}
		if a.CDC_Config.MaxSize != b.CDC_Config.MaxSize {
			return false
		}
	}
	//CDC_Config  *jcdc.CDC_Config `zid:"16"`
	return true
}

// A BlobStore is CAS for blobs. It is Content Addressable
// Storage for binary large objects, like a Git Repo
// or key/value database.
type BlobStore struct {
	Map map[string]*Chunk
}

func NewBlobStore() *BlobStore {
	return &BlobStore{
		Map: make(map[string]*Chunk),
	}
}

// marker for the .Data that heavy data needs to be sent
// from the file. This should be set in
// giverSendsPlanAndDataUpdates and replaced in
// packAndSendChunksLimitedSize().
var markToSendHeavyFromFile = []byte{'H'}

// GetPlanToUpdateFromGoal creates a plan. The
// plan describes how to update the 'updateme'
// file to match the 'goal' file. The plan will have
// chunk.Data pointers set only for those
// chunks that are goal only; that
// the (remote) updateme side currently lacks.
// This is the essential rsync "diff" operation.
//
// We expect the updateme chunks have no Data pointers available,
// and all the local to have them, or, as a fallback, for
// them to be available in the BlobStore.
// These pointers, from goal or the BlobStore, are
// copied ino the plan Chunk(s) we return.
//
// If dropGoalData, we also nil out the Data pointers
// in goal Chunks that the (remote) updateme file
// does not need, conserving memory.
//
// If usePlaceholders, we update goal in place and
// return it as plan. The update will have
// a one byte assignment to .Data when the data
// needs to be read in from the file. goal should
// not have prior .Data attachments.
func (s *BlobStore) GetPlanToUpdateFromGoal(updateme, goal *Chunks, dropGoalData, usePlaceHolders bool) (plan *Chunks) {
	//vv("top of GetPlan: usePlaceHolders=%v; goal.DataPreset()='%v'", usePlaceHolders, goal.DataPresent()) // , stack())

	//vv("top of GetPlan: len(updateme.Chunks)=%v", len(updateme.Chunks))
	//vv("top of GetPlan: len(goal.Chunks)=%v", len(goal.Chunks))

	// index the updateme
	updatememap := make(map[string]*Chunk)
	for i, c := range updateme.Chunks {
		_ = i
		//vv("i=%v, adding to updateme_map: '%v'", i, c)
		if c.Cry == "RLE0;" || c.Cry == "UNWRIT;" {
			// omit -- update: ?? do we need to transmit for sparseness TODO?
		} else {
			updatememap[c.Cry] = c
		}
	}
	//vv("len updatememap = %v", len(updatememap))

	if usePlaceHolders {
		// the goal file layout is the template for the plan
		for _, c := range goal.Chunks {
			if len(c.Data) != 0 {
				panic("the goal passed to usePlaceHolders should have no .Data on it!")
			}
			//vv("checking for goal.Chunk c = '%v'", c)
			if c.Cry == "RLE0;" || c.Cry == "UNWRIT;" {
				// other side never needs RLE0; its just all 0. // update TODO RLE0->sparse might need to transmit?
			} else {
				_, ok := updatememap[c.Cry]
				if !ok {
					// other side needs this.
					// assign this one-bye slice as sentinel to pull from file.
					//vv("setting one byte mark!")
					c.Data = markToSendHeavyFromFile
				}
			}
		}
		//vv("on return, goal.DataPresent() = %v", goal.DataPresent())
		return goal
	}

	plan = NewChunks(updateme.Path)
	plan.FileSize = goal.FileSize
	plan.FileCry = goal.FileCry

	var p []*Chunk

	// the goal file layout is the template for the plan
	for _, c := range goal.Chunks {
		var ok bool
		if c.Cry == "RLE0;" || c.Cry == "UNWRIT;" {
			ok = true
		} else {
			_, ok = updatememap[c.Cry]
		}

		// make a copy of the goal template chunk
		addme := *c
		if ok {
			//vv("i=%v, updateme already has it, do not send.", i)
			addme.Data = nil
			if dropGoalData {
				c.Data = nil // drop it goally too.
			}
		} else {
			//vv("i=%v, leave addme.Data intact, because updateme needs it.", i)

			// sanity check that we do infact have the Data.
			if len(c.Data) == 0 {
				// can we get it from the blobstore?
				bs, ok := s.Map[c.Cry]
				if !ok || bs == nil || len(bs.Data) == 0 {
					panic(fmt.Sprintf("goal chunks missing Data!: '%v'", c))
				}
				addme.Data = bs.Data
			}
		}
		p = append(p, &addme)
	}
	plan.Chunks = p

	return
}

// Light request asks for the
// remote Sender's path, and
// advertises the chunks for the
// readers's version that are
// already available. The
// responder will send back
// a HeavyPlan that includes
// the minimal necessary data and
// instructions on how to
// assemble the desired file from
// chunks.
type LightRequest struct {
	SenderPath string `zid:"0"`

	ReaderPrecis *FilePrecis `zid:"1"`
	ReaderChunks *Chunks     `zid:"2"`
}

type SenderPlan struct {
	SenderPath string `zid:"0"`

	SenderPrecis        *FilePrecis `zid:"1"`
	SenderChunksNoSlice *Chunks     `zid:"2"` // no data!

	FileIsDeleted bool `zid:"3"` // no other detail, just delete it.
}

type Nil struct {
	Placeholder int
}

type RsyncNode struct {
	Host        string `zid:"0"`
	Placeholder int
}

func (s *RsyncNode) RequestLatest(
	ctx context.Context,
	req *LightRequest,
	reply *HeavyPlan) error {

	//vv("top of RequestLatest(); req.SenderPath='%v'", req.SenderPath)

	// they need file (parts at least).
	remote := req.ReaderChunks

	localPrecis, local, err := SummarizeFileInCDCHashes(s.Host, req.SenderPath, true, true)

	//if err != nil {
	//vv("mid RequestLatest(); err = '%v'", err)
	//} else {
	//vv("step3: localHashes ok")
	//}
	panicOn(err)

	//local := localSummary.Chunks

	//vv("RequestLatest(): server has local='%v'", local)
	//vv("RequestLatest(): server sees remote='%v'", remote)

	// debug only
	//onlyL, onlyR, both := Diff(local, remote)
	//vv("len onlyL = %v", len(onlyL))
	//vv("len onlyR = %v", len(onlyR))
	//vv("len both  = %v", len(both))

	bs := NewBlobStore() // make persistent state, at some point.

	drop2ndData := true // local is in 2nd position here, drop unneeded data.
	plan := bs.GetPlanToUpdateFromGoal(remote, local, drop2ndData, false)

	//vv("plan = '%v'", plan)
	reply.SenderPlan = plan
	reply.SenderPrecis = localPrecis
	//vv("end of RequestLatest()")

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

// When the client wants to send a change to the
// server, and it already has a plan.

func (s *RsyncNode) AcceptHeavy(
	ctx context.Context,
	req *HeavyPlan,
	reply *HeavyPlan) error {

	plan := req.SenderPlan
	senderPrecis := req.SenderPrecis

	localPrecis, local, err := SummarizeFileInCDCHashes(s.Host, req.SenderPath, true, true)
	_ = localPrecis

	localMap := getCryMap(local) // pre-index them for the update.

	err = UpdateLocalWithRemoteDiffs(local.Path, localMap, plan, senderPrecis)
	panicOn(err)

	return err
}

type HeavyPlan struct {
	SenderPath   string      `zid:"0"`
	SenderPrecis *FilePrecis `zid:"1"` // needs to be streamed too? could be very large?
	SenderPlan   *Chunks     `zid:"2"`
}

func (h *FilePrecis) String() string {

	jsonData, err := json.Marshal(h)
	panicOn(err)

	var pretty bytes.Buffer
	err = json.Indent(&pretty, jsonData, "", "    ")
	panicOn(err)
	return pretty.String()
}

// SummarizeFileInCDCHashes summarizes path in the
// returned precis. The host should be the local
// host identifier, such as rpc25519.Hostname.
// If wantChunks is false, the returned chunks will be nil,
// and we will not bother to scan path; just return
// the precis. If keepData is false and wantChunks is
// true, the file will be scanned but the actual
// Data field in the chunks will be nil and not returned.
//
// For files under 1GB, LightlySummarize will call us.
// So do not recurse into LightlySummarize in here.
func SummarizeFileInCDCHashes(host, path string, wantChunks, keepData bool) (precis *FilePrecis, chunks *Chunks, err error) {

	if !fileExists(path) {
		return SummarizeBytesInCDCHashes(host, path, nil, time.Time{}, keepData, 0)
	}

	// file system details fill in:
	//var data []byte

	// need it all anyway, just read it in.
	// Update: no, not with sparse file support. wait.
	//data, err = os.ReadFile(path)
	fd, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("rsync.go error opening path '%v': '%v'", path, err)
	}
	defer fd.Close()

	fi, err := fd.Stat() // os.Stat(path)
	if err != nil {
		return nil, nil, fmt.Errorf("rsync.go error on os.Stat() of '%v': '%v'", path, err)
	}

	modTime := fi.ModTime()
	if wantChunks {
		//precis, chunks, err = SummarizeBytesInCDCHashes(host, path, data, modTime, keepData, false)
		precis, chunks, err = SummarizeBytesInCDCHashes(host, path, fd, modTime, keepData, fi.Size())
	}
	precis.FileMode = uint32(fi.Mode())
	precis.ModTime = modTime

	fileOwner, uid := getFileOwnerAndID(fi)
	fileGroup, gid := getFileGroupAndID(fi)

	precis.FileOwnerID = uid
	precis.FileGroupID = gid
	precis.FileOwner = fileOwner
	precis.FileGroup = fileGroup

	return
}

// SummarizeBytesInCDCHashes summarizes path in the
// returned precis. The host should be the local
// host identifier, such as rpc25519.Hostname.
// If keepData is false, the returned chunks will
// have nil Data.
func SummarizeBytesInCDCHashes(host, path string, fd *os.File, modTime time.Time, keepData bool, fileStatSz int64) (
	precis *FilePrecis, chunks *Chunks, err error) {

	// These two different chunking approaches,
	// Jcdc and FastCDC, need very different
	// parameter min/max/target settings in
	// order to give good chunking.

	cfg := Default_CDC_Config
	cdc := jcdc.GetCutpointer(Default_CDC, cfg)

	precis = &FilePrecis{
		Host:        host,
		Path:        path,
		FileSize:    fileStatSz, // len(data),
		ModTime:     modTime,
		ChunkerName: cdc.Name(),
		CDC_Config:  cdc.Config(),
		HashName:    "blake3.33B",
	}

	if fd == nil || fileStatSz == 0 {
		precis.FileCry = hash.Blake3OfBytesString([]byte{})
		//vv("path '%v' is empty or nil fd passed; fd = %p; fileStatSz = %v; returning precis.FileCry = '%v'", path, fd, fileStatSz, precis.FileCry)
	} else {
		precis.FileCry, err = hash.Blake3OfFile(path)
		panicOn(err)
	}

	chunks = NewChunks(path)
	chunks.FileSize = precis.FileSize
	chunks.FileCry = precis.FileCry

	if fd == nil {
		return
	}

	cuts, allzero, preun, spans := cdc.CutpointsAndAllZero(fd)

	//cutsOrig := cdc.Cutpoints(data2, 0)

	if len(cuts) == 0 {
		// okay, is truly an empty file
		//vv("empty file, return early after CutpointsAndAllZero")
		return
	}
	//vv("cuts = '%#v'", cuts)

	dataMaxSz := int64(1 << 20) // 1 MB
	cfgmax := int64(cfg.MaxSize)
	if cfgmax > dataMaxSz {
		dataMaxSz = cfgmax
	}
	data := make([]byte, dataMaxSz)
	// INVAR: data is big enough to hold
	// our largest possible chunk.

	// dsz == dendx-dbeg
	var dsz int64   // data holds this many file bytes.
	var dbeg int64  // file coordinates of data[0]
	var dendx int64 // file coordinates of data[datasz]

	haveSpans := false
	var nspan int
	var curSpan sparsified.SparseSpan
	curSpanIndex := -1
	if spans != nil {
		nspan = len(spans.Slc)
		if nspan > 0 {
			haveSpans = true
			curSpan = spans.Slc[0]
			curSpanIndex = 0
		}
	}
	if haveSpans &&
		!curSpan.IsHole &&
		!curSpan.IsUnwrittenPrealloc {

		dbeg = 0
		dendx = curSpan.Endx
		if dendx-dbeg > dataMaxSz {
			dendx = dbeg + dataMaxSz
		}
		dsz = dendx - dbeg
		if dsz > 0 {
			vv("1st data fill [dbeg, dendx) of size %v: [%v, %v)", dsz, dendx, dbeg)
			_, err = fd.Seek(dbeg, 0)
			panicOn(err)
			_, err = io.ReadFull(fd, data[:dsz])
			panicOn(err)
		}
		// INVAR: data buffers the first dsz bytes
		// from the first span. but dsz could == 0.
	}

	// helper to read more into data when it runs out.
	fillDataForChunk := func(prevcut, cut int64) bool {
		if !haveSpans {
			return false
		}
		// We know this chunk is in a dense data section of the file.
		// look for the next span that contains prevcut.
		// Look even in curSpanIndex, since we might well
		// not have exhausted it yet.
		found := false
		begi := curSpanIndex
		if begi < 0 {
			begi = 0
		}
		for i := begi; i < nspan; i++ {
			if spans.Slc[i].Beg <= prevcut &&
				prevcut < spans.Slc[i].Endx {
				found = true
				curSpanIndex = i
				curSpan = spans.Slc[i]
				break
			}
		}
		if !found {
			panic(fmt.Sprintf("no span contained prevcut='%v'??? spans='%v'", prevcut, spans))
			return false
		}
		if curSpan.Beg < cut && cut <= curSpan.Endx {
			// good: curSpan holds both prevcut and cut
		} else {
			vv("bad: curSpan holds prevcut(%v) but not cut(%v). curSpan='%v'", prevcut, cut, curSpan)
			panic("can this be fixed?") // may not always be fixable.
			return false
		}
		dbeg = prevcut
		dendx = curSpan.Endx
		if dendx-dbeg > dataMaxSz {
			dendx = dbeg + dataMaxSz
		}
		dsz = dendx - dbeg
		if dsz > 0 {
			vv("subsequent data fill [dbeg, dendx) of size %v: [%v, %v)", dsz, dendx, dbeg)
			_, err = fd.Seek(dbeg, 0)
			panicOn(err)
			_, err = io.ReadFull(fd, data[:dsz])
			panicOn(err)
		}
		// repeat the check that the data is usable.
		return prevcut >= dbeg && cut <= dendx
	}

	var prevcut int64
	var hsh string
	for i, cut := range cuts {

		var slc []byte
		switch {
		case allzero[i]:
			hsh = "RLE0;"
			//vv("saw RLE0;")
		case preun[i]:
			// pre-allocated yet unwritten. logical zeros.
			hsh = "UNWRIT;"
			vv("saw UNWRIT;") // not seen 710 test
		default:
			// this chunk in file is filepos [prev, c).
			if cut <= prevcut {
				panic(fmt.Sprintf("bad cuts: cut(%v) <= prevcut(%v)", cut, prevcut))
			}
			// INVAR: prev < cut

			// use the data buffer if we can
			if prevcut >= dbeg && cut <= dendx {
				// can use data. get coordinates to index data.
				b := prevcut - dbeg // always >= 0
				sz := cut - prevcut
				slc = data[b:(b + sz)]
				hsh = hash.Blake3OfBytesString(slc)
			} else {
				// data is insufficient/ not overlapping this chunk.
				if fillDataForChunk(prevcut, cut) {
					b := prevcut - dbeg // always >= 0
					sz := cut - prevcut
					slc = data[b:(b + sz)]
					hsh = hash.Blake3OfBytesString(slc)
				} else {
					// fallback to manually reading from file.
					fd.Seek(prevcut, 0)
					sz := cut - prevcut
					_, err := io.ReadFull(fd, data[:sz])
					panicOn(err)
					// update data trackers.
					dbeg = prevcut
					dendx = prevcut + sz
					dsz = sz

					slc = data[:sz]
					hsh = hash.Blake3OfBytesString(slc)
				}
			}
		}
		//fmt.Printf("[%03d]Summarize hsh = %v\n", i, hsh)
		chunk := &Chunk{
			Beg:  prevcut,
			Endx: cut,
			Cry:  hsh,
		}
		if keepData {
			chunk.Data = append([]byte{}, slc...)
		}
		chunks.Chunks = append(chunks.Chunks, chunk)
		prevcut = cut
	}
	return
}

// String pretty prints the Chunks.
func (d *Chunks) String() (s string) {
	s = fmt.Sprintf("\n&Chunks{ //(set of %v)\n     Path: \"%v\",\n"+
		" FileSize: %v,\n  FileCry: \"%v\",\n   Chunks: []*Chunk{\n",
		len(d.Chunks), d.Path, d.FileSize, d.FileCry)
	for i, chunk := range d.Chunks {
		s += fmt.Sprintf("// [%03d]\n", i) + chunk.String()
	}
	s += "}}\n"
	return
}

func (d *Chunk) String() string {

	return fmt.Sprintf(
		`&Chunk{
    Beg : %v,
    Endx: %v, // (len %v)
    Cry : "%v",
 // Data: length %v,
},
`, d.Beg, d.Endx, (d.Endx - d.Beg), d.Cry, len(d.Data))
}

// Pair is returned by Diff inside the both map.
type Pair struct {
	A, B *Chunk
}

// Diff compares two sets of Chunks.
func Diff(a, b *Chunks) (onlyA, onlyB map[string]*Chunk, both map[string]*Pair) {

	onlyA = make(map[string]*Chunk)
	onlyB = make(map[string]*Chunk)
	both = make(map[string]*Pair)

	for _, chunk := range a.Chunks {
		onlyA[chunk.Cry] = chunk
	}
	for _, chunkB := range b.Chunks {
		chunkA, inBoth := onlyA[chunkB.Cry]
		if inBoth {
			both[chunkB.Cry] = &Pair{A: chunkA, B: chunkB}
			delete(onlyA, chunkB.Cry)
		} else {
			onlyB[chunkB.Cry] = chunkB
		}
	}
	return
}

func (cs *Chunks) ClearData() {
	for _, chunk := range cs.Chunks {
		chunk.Data = nil
	}
}

func (cs *Chunks) CloneWithClearData() (r *Chunks) {
	r = NewChunks(cs.Path)
	r.FileSize = cs.FileSize
	r.FileCry = cs.FileCry
	for _, c := range cs.Chunks {
		r.Chunks = append(r.Chunks, &Chunk{
			Beg:  c.Beg,
			Endx: c.Endx,
			Cry:  c.Cry,
			// deliberately clearing .Data, of course.
		})
	}
	return
}

func (cs *Chunks) CloneWithNoChunks() (r *Chunks) {
	r = NewChunks(cs.Path)
	r.FileSize = cs.FileSize
	r.FileCry = cs.FileCry
	return
}

// DataFilter returns only those chunks with .Data
func (cs *Chunks) DataFilter() (r []*Chunk) {
	for _, chunk := range cs.Chunks {
		if len(chunk.Data) > 0 {
			r = append(r, chunk)
		}
	}
	return
}

// GetHashesOneByOne does one
// cutpoint location at a time. It returns the
// same set of chunks as SummarizeFileInCDCHashes.
// However it is the fastest
// of the three approaches tried; on my mac. Maybe this
// is due to better overlapping of pipe-lined
// disk I/O with computation.
func GetHashesOneByOne(host, path string) (precis *FilePrecis, chunks *Chunks, err error) {

	//vv("GetHashesOneByOne top")
	if !fileExists(path) {
		return SummarizeBytesInCDCHashes(host, path, nil, time.Time{}, false, 0)
	}

	// These two different chunking approaches,
	// Jcdc and FastCDC_Stadia, need very different
	// parameter min/max/target settings in
	// order to give good chunking.

	cdc := jcdc.GetCutpointer(Default_CDC, Default_CDC_Config)
	cdcCfg := cdc.Config()

	//vv("GetHashesOneByOne() using cdc = '%v'", cdc.Name())

	sz64, modTime, err := FileSizeModTime(path)
	if err != nil {
		return nil, nil, err
	}
	//sz := int(sz64)
	//vv("sz = %v", sz)

	precis = &FilePrecis{
		Host:     host,
		Path:     path,
		FileSize: sz64,
		ModTime:  modTime,
		//FileCry:     hash.Blake3OfBytesString(data),
		ChunkerName: cdc.Name(),
		CDC_Config:  cdcCfg,
		HashName:    "blake3.33B",
	}
	chunks = NewChunks(path)
	chunks.FileSize = precis.FileSize

	var fi os.FileInfo
	fi, err = os.Stat(path)
	if err != nil {
		return
	}
	precis.FileMode = uint32(fi.Mode())
	precis.ModTime = modTime

	fileOwner, uid := getFileOwnerAndID(fi)
	fileGroup, gid := getFileGroupAndID(fi)

	precis.FileOwnerID = uid
	precis.FileGroupID = gid
	precis.FileOwner = fileOwner
	precis.FileGroup = fileGroup

	h := blake3.New(64, nil)

	defer func() {
		if precis != nil && chunks != nil {
			precis.FileCry = hash.SumToString(h)
			chunks.FileCry = precis.FileCry
		}
	}()

	if sz64 == 0 {
		return
	}

	// this was the fastest buffer size on my mac.
	// It was better than any higher or lower power of 2.
	bufsz := 1 << 20 // 1.901690203s fo N=1000;  N=2000 => 3.800150287s

	fd, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer fd.Close()

	k := 0 // chunk count
	addChunk := func(slc []byte, beg int64) {
		hsh := hash.Blake3OfBytesString(slc)
		//fmt.Printf("[%03d]GetHashes hsh = %v\n", k, hsh)
		k++
		chunk := &Chunk{
			Beg:  beg,
			Endx: beg + int64(len(slc)),
			Cry:  hsh,
		}
		chunks.Chunks = append(chunks.Chunks, chunk)
		h.Write(slc)
	}

	//vv("using bufsz = %v in GetHashesOneByOne", bufsz)
	buf := make([]byte, bufsz)

	var totalread int64 // total bytes read from file so far.

	var bufbeg int64  // offset where buf starts in the origin file.
	var bufendx int64 // offset where buf ends in the origin file.
	var dataoff int64 // offset where data starts in the original file. pass to addChunk()

	var nr int
	nr, err = io.ReadFull(fd, buf)
	if err == io.EOF {
		// no more bytes, exactly 0.
		// "The error is EOF only if no bytes were read."
		// -- https://pkg.go.dev/io#ReadFull
		err = nil
		return
	}
	bufendx += int64(nr)
	totalread += int64(nr) // gives offset of end of data

	if err == io.ErrUnexpectedEOF {
		err = nil
		// ignore short read error in next err != nil check.
	}
	if err != nil {
		//panicOn(err)
		return nil, nil, err
	}
	data := buf[:nr]
	// dataoff = 0, as it was.

	for j := 0; len(data) > 0; j++ {

		cut := int64(cdc.NextCut(data))

		if len(data) >= cdcCfg.MaxSize {
			// legit cut
			addChunk(data[:cut], dataoff)
			//vv("j=%v  legit cut: '%v'", j, chunks.Chunks[len(chunks.Chunks)-1])
			data = data[cut:]
			dataoff += cut
			if len(data) > cdcCfg.MaxSize {
				continue
			}
		}
		// INVAR: len(data) < opts.MaxSize, we need to read from file, if we can.
		if bufendx == sz64 {
			//vv("no more data available, the last will be oddly truncated.")
			for len(data) > 0 {
				cut := int64(cdc.NextCut(data))
				addChunk(data[:cut], dataoff)
				data = data[cut:]
				dataoff += cut
			}
			//vv("j=%v  no more data, last chunk: '%v'", j, chunks.Chunks[len(chunks.Chunks)-1])
			return
		}
		// data is out of space, read from file again

		// move what we have left in data to the beginning of buf,
		// before we read again, so we don't lose it.
		k := int64(copy(buf, data))
		bufbeg = dataoff
		bufendx = dataoff + k
		//vv("data out of space, read from file. copied k = %v", k)

		nr, err = io.ReadFull(fd, buf[k:])
		if err == io.EOF {
			// no more bytes, exactly 0.
			// "The error is EOF only if no bytes were read."
			// -- https://pkg.go.dev/io#ReadFull
			err = nil
			//vv("we see EOF after totalread = %v", totalread)
			return
		}
		bufendx += int64(nr)
		totalread += int64(nr)

		if err == io.ErrUnexpectedEOF {
			// buf is less than full. meh. ignore this error.
			err = nil // prevent next if err != nil from returning.

			//vv("ignoring ErrUnexpectedEOF; nr = %v", nr)
		}
		if err != nil {
			//panicOn(err)
			return nil, nil, err
		}
		data = buf[:(bufendx - bufbeg)]
		// dataoff is fine.
		if len(data) == 0 && totalread != sz64 {
			panic("why is data len 0 when have not ready whole file?")
		}
	}
	//vv("GetHashesOneByOne returning")
	return
}

// GetPrecis is for when you only need the FilePrecis,
// no Chunks needed (but you do want the whole file's FileCry checksum).
func GetPrecis(host, path string) (precis *FilePrecis, err error) {

	if !fileExists(path) {
		precis, _, err = SummarizeBytesInCDCHashes(host, path, nil, time.Time{}, false, 0)
		return
	}

	cdc := jcdc.GetCutpointer(Default_CDC, Default_CDC_Config)

	sz64, modTime, err := FileSizeModTime(path)
	if err != nil {
		return nil, err
	}
	//sz := int(sz64)
	//vv("sz = %v", sz)

	precis = &FilePrecis{
		Host:     host,
		Path:     path,
		FileSize: sz64,
		ModTime:  modTime,
		//FileCry:     hash.Blake3OfBytesString(data),
		ChunkerName: cdc.Name(),
		CDC_Config:  cdc.Config(),
		HashName:    "blake3.33B",
	}

	var fi os.FileInfo
	fi, err = os.Stat(path)
	if err != nil {
		return nil, err
	}
	precis.FileMode = uint32(fi.Mode())
	precis.ModTime = modTime

	fileOwner, uid := getFileOwnerAndID(fi)
	fileGroup, gid := getFileGroupAndID(fi)

	precis.FileOwnerID = uid
	precis.FileGroupID = gid
	precis.FileOwner = fileOwner
	precis.FileGroup = fileGroup

	precis.FileCry, err = hash.Blake3OfFile(path)
	if err != nil {
		return nil, err
	}
	return
}

// seems to be only called from rsync_test.go and rsync_simnet_test.go
func UpdateLocalFileWithRemoteDiffs(
	localPathToWrite string,
	localPathToRead string,

	// localMap: Cry -> chunk.
	// Typically either these are the chunks
	// from the local version of the path; but
	// they could be from a larger data store
	// like a Git repo or database.
	localMap map[string]*Chunk,

	remote *Chunks,

	goalPrecis *FilePrecis, // set mode, modtime from.

) (err error) {

	if remote.FileSize == 0 {
		//vv("remote.FileSize == 0 => truncate to zero localPathToWrite='%v'", localPathToWrite)
		return truncateFileToZero(localPathToWrite)
	}

	if len(remote.Chunks) == 0 {
		panic(fmt.Sprintf("missing remote chunks for non-size-zero file '%v'", localPathToWrite))
	}

	// make sure we have a full plan, not just a partial diff.
	if remote.FileSize != remote.Last().Endx {
		panic(fmt.Sprintf("remote was not a full plan for every byte! remote.FileSize = %v; but remote.Last().Endx = %v", remote.FileSize, remote.Last().Endx))
	}

	// turn RLE0 into sparse holes
	var sparse []*sparsified.SparseSpan

	// working buffer to read local file chunks into.
	buf := make([]byte, rpc.UserMaxPayload+10_000)

	//newvers := make([]byte, remote.FileSize)
	var newversBufio *bufio.Writer
	var newversFd *os.File

	rnd := cryRandBytesBase64(17)
	tmp := localPathToWrite + "_accept_plan_tmp_" + rnd

	newversFd, err = os.Create(tmp)
	panicOn(err)

	//vv("UpdateLocalFileWithRemoteDiffs created file tmp = '%v'", tmp)
	newversBufio = bufio.NewWriterSize(newversFd, rpc.UserMaxPayload)

	// remember to Flush and Close!
	defer newversBufio.Flush() // must be first
	defer newversFd.Close()

	// prep local file too, for seeking to chunks.
	var origVersFd *os.File

	if localPathToRead == "" {
		panic("localPathToRead must have been set!")
	}
	if fileExists(localPathToRead) {
		origVersFd, err = os.Open(localPathToRead)
		panicOn(err)
		defer origVersFd.Close()
	}

	j := 0 // index to new version, how much we have written.

	// compute the full file hash/checksum as we go
	h := blake3.New(64, nil)

	// from taker.go:399

	// remote gives the plan of what to create
	for _, chunk := range remote.Chunks {

		// handle "RLE0;" case, run-length-encoded zeros.
		if chunk.Cry == "RLE0;" {
			// can we turn it into a sparse hole?
			span, wings := sparsified.AlignedSparseSpan(int64(chunk.Beg), int64(chunk.Endx))
			_ = wings
			if span != nil {
				sparse = append(sparse, span)
			}

			n := chunk.Endx - chunk.Beg
			ns := n / int64(len(zeros4k))
			rem := n % int64(len(zeros4k))
			for range ns {
				wb, err := newversBufio.Write(zeros4k)
				panicOn(err)
				j += wb
				h.Write(zeros4k)
			}
			if rem > 0 {
				wb, err := newversBufio.Write(zeros4k[:rem])
				panicOn(err)
				j += wb
				h.Write(zeros4k[:rem])
			}
			continue
		}

		if len(chunk.Data) == 0 {
			// the data is local
			lc, ok := localMap[chunk.Cry]
			if !ok {
				panic(fmt.Sprintf("rsync algo failed, "+
					"the needed data is not "+
					"available locally: '%v'; len(localMap)=%v",
					chunk, len(localMap)))
			}
			// data is typically nil!
			// localMap should have only hashes.
			// so this is just getting an empty slice.
			// Is this always true?
			data := lc.Data
			if origVersFd != nil {
				////vv("read from original on disk, for chunk '%v'", chunk)
				beg := int64(lc.Beg)
				newOffset, err := origVersFd.Seek(beg, 0)
				panicOn(err)
				if newOffset != beg {
					panic(fmt.Sprintf("huh? could not seek to %v in file '%v'", lc.Beg, localPathToRead))
				}
				data = buf[:lc.Len()]
				_, err = io.ReadFull(origVersFd, data)
				panicOn(err)
			}

			wb, err := newversBufio.Write(data)
			panicOn(err)

			j += wb
			if wb != len(data) {
				panic("short write?!?!")
			}
			// sanity check the local chunk as a precaution.
			if int64(wb) != lc.Endx-lc.Beg {
				panic(fmt.Sprintf("lc.Endx = %v, lc.Beg = %v, but "+
					"lc.Data len = %v", lc.Endx, lc.Beg, wb))
			} // panic: lc.Endx = 2992124, lc.Beg = 2914998, but lc.Data len = 0
			h.Write(data) // update checksum
		} else {
			// INVAR: len(chunk.Data) > 0
			wb, err := newversBufio.Write(chunk.Data)
			panicOn(err)

			j += wb
			if wb != len(chunk.Data) {
				panic("short write!?!!")
			}
			// sanity check the local chunk as a precaution.
			if int64(wb) != chunk.Endx-chunk.Beg {
				panic(fmt.Sprintf("lc.Endx = %v, lc.Beg = %v, but "+
					"lc.Data len = %v", chunk.Endx, chunk.Beg, wb))
			}
			vv("writing to hash h len %v of chunk.Data; tot = j = %v", wb, j) // not seen
			h.Write(chunk.Data)
		}
	} // end for chunk over chunks.Chunks

	if len(sparse) > 0 {
		vv("UpdateLocalFileWithRemoteDiffs detected %v sparse holes, TODO: implement below... take out Bufio?", len(sparse))
	} else {
		vv("UpdateLocalFileWithRemoteDiffs detected NO sparse holes")
	}

	newversBufio.Flush() // must be before newversFd.Close()
	newversFd.Close()

	sum := hash.SumToString(h)
	if sum != remote.FileCry {
		err = fmt.Errorf("checksum mismatch error B! reconstructed='%v'; expected='%v'; remote path = ''%v'", sum, remote.FileCry, remote.Path)
		panic(err)
		return err
	}

	err = os.Rename(tmp, localPathToWrite)
	panicOn(err)

	// restore mode, modtime
	mode := goalPrecis.FileMode
	if mode == 0 {
		mode = 0600
	}
	err = os.Chmod(localPathToWrite, fs.FileMode(mode))
	panicOn(err)

	err = os.Chtimes(localPathToWrite, time.Time{}, goalPrecis.ModTime)
	panicOn(err)

	return
}
