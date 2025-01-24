package jsync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"lukechampine.com/blake3"
	"os"
	"os/user"
	"syscall"
	"time"

	//"github.com/glycerine/greenpack/msgp"
	//rpc "github.com/glycerine/rpc25519"
	"github.com/glycerine/rpc25519/hash"
	"github.com/glycerine/rpc25519/jcdc"
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
	Beg  int    `zid:"0"`
	Endx int    `zid:"1"`
	Cry  string `zid:"2"` // a cryptographic hash identifying the chunk. Ex: blake3

	// Data might be nil for summary purposes,
	// or provided if we are transmitting a set of diffs.
	Data []byte `zid:"3" json:"-"`

	// does the taker have the chunk already, but just
	// at a different original (TakePath) file location?
	// In this case the giver can avoid sending
	// Data, but still tell the taker where
	// to get the data.
	//UseLocalTakerCopy *Chunk `zid:"4"`
}

func (c *Chunk) CloneNoData() (r *Chunk) {
	r = &Chunk{
		Beg:  c.Beg,
		Endx: c.Endx,
		Cry:  c.Cry,
	}
	return
}

func (c *Chunk) Len() int { return c.Endx - c.Beg }

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
	FileSize int    `zid:"2"`
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

// UpdateLocalWithRemoteDiffs is the essence of the rsync
// algorithm for efficient file transfer. The remote
// chunks arguments gives us the update plan.
// The new version the file is constructed from
// existing data (in localMap) and the updated
// new chunk data we've received (in remote).
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
		//vv("remote.FileSize == 0 => truncate to zero localPathToWrite='%v'", localPathToWrite)
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
	for i, chunk := range remote.Chunks {
		_ = i

		if len(chunk.Data) == 0 {
			// the data is local
			lc, ok := localMap[chunk.Cry]
			if !ok {
				panic(fmt.Sprintf("rsync algo failed, the needed data is not "+
					"available locally: '%v'; len(localMap) = %v", chunk, len(localMap))) // it was not this one, but further down at 220
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
			wb := copy(newvers[j:], chunk.Data)
			j += wb
			if wb != len(chunk.Data) {
				panic("newvers did not have enough space")
			}
			// sanity check the local chunk as a precaution.
			if wb != chunk.Endx-chunk.Beg {
				panic(fmt.Sprintf("lc.Endx = %v, lc.Beg = %v, but lc.Data len = %v", chunk.Endx, chunk.Beg, wb))
			}
			h.Write(chunk.Data)
		}
	}
	sum := hash.SumToString(h)
	if sum != remote.FileCry {
		err = fmt.Errorf("checksum mismatch error! reconstructed='%v'; expected='%v'; remote path = ''%v'", sum, remote.FileCry, remote.Path)
		panic(err)
		return err
	}

	var fd *os.File
	rnd := cryRandBytesBase64(16)
	tmp := localPathToWrite + "_accept_plan_tmp_" + rnd
	fd, err = os.Create(tmp)
	if err != nil {
		return err
	}

	// Close just returns an error if called 2x. That is fine.
	defer fd.Close()
	_, err = fd.Write(newvers)
	panicOn(err)

	fd.Close()
	err = os.Rename(tmp, localPathToWrite)
	panicOn(err)

	// restore mode, modtime
	err = os.Chmod(localPathToWrite, fs.FileMode(goalPrecis.FileMode))
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
// and a specified hash function (e.g. "blake3.32B")
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
	FileSize    int       `zid:"6"`
	FileMode    uint32    `zid:"7"`
	FileOwner   string    `zid:"8"`
	FileOwnerID uint32    `zid:"9"`
	FileGroup   string    `zid:"10"`
	FileGroupID uint32    `zid:"11"`

	// other data, extension mechanism. Not used presently; for future use.
	FileMeta []byte `zid:"12"`

	// HashName is e.g. "blake3.32B". This should match the
	// hash string prefix without any trailing "-" dash.
	//
	// For example, if your hash strings look like
	// "blake3.32B-89J1Jfa1AzfjiOcVOCLJXDsX3AANHWiSERgJwtaUSj8="
	// then your HashName is "blake3.32B".
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
	//vv("top of GetPlan: usePlaceHolders=%v; goal.DataPreset()='%v'; stack=\n%v", usePlaceHolders, goal.DataPresent(), stack())

	//vv("top of GetPlan: len(updateme.Chunks)=%v", len(updateme.Chunks))
	//vv("top of GetPlan: len(goal.Chunks)=%v", len(goal.Chunks))

	// index the updateme
	updatememap := make(map[string]*Chunk)
	for i, c := range updateme.Chunks {
		_ = i
		//vv("i=%v, adding to updateme_map: '%v'", i, c)
		updatememap[c.Cry] = c
	}
	//vv("len updatememap = %v", len(updatememap))

	if usePlaceHolders {
		// the goal file layout is the template for the plan
		for _, c := range goal.Chunks {
			if len(c.Data) != 0 {
				panic("the goal passed to usePlaceHolders should have no .Data on it!")
			}
			//vv("checking for goal.Chunk c = '%v'", c)
			_, ok := updatememap[c.Cry]
			if !ok {
				// other side needs this.
				// assign this one-bye slice as sentinel to pull from file.
				//vv("setting one byte mark!")
				c.Data = markToSendHeavyFromFile
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
		_, ok := updatememap[c.Cry]

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
// an HeavyPlan that includes
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

	if err != nil {
		//vv("mid RequestLatest(); err = '%v'", err)
	} else {
		//vv("step3: localHashes ok")
	}
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
		return SummarizeBytesInCDCHashes(host, path, nil, time.Time{}, keepData)
	}

	// file system details fill in:
	var data []byte

	// need it all anyway, just read it in
	data, err = os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("rsync.go error reading path '%v': '%v'", path, err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		return nil, nil, fmt.Errorf("rsync.go error on os.Stat() of '%v': '%v'", path, err)
	}

	modTime := fi.ModTime()
	if wantChunks {
		precis, chunks, err = SummarizeBytesInCDCHashes(host, path, data, modTime, keepData)
	}
	precis.FileMode = uint32(fi.Mode())
	precis.ModTime = modTime

	if stat_t, ok := fi.Sys().(*syscall.Stat_t); ok {
		uid := stat_t.Uid
		precis.FileOwnerID = uid
		gid := stat_t.Gid
		precis.FileGroupID = gid

		owner, err := user.LookupId(fmt.Sprint(uid))
		if err == nil && owner != nil {
			precis.FileOwner = owner.Username
		}
		group, err := user.LookupGroupId(fmt.Sprint(gid))
		if err == nil && group != nil {
			precis.FileGroup = group.Name
		}
	}
	return
}

const useFastCDC = true // MAYBE JUST SLOW!
//const useFastCDC = false

// SummarizeBytesInCDCHashes summarizes path in the
// returned precis. The host should be the local
// host identifier, such as rpc25519.Hostname.
// If keepData is false, the returned chunks will
// have nil Data.
func SummarizeBytesInCDCHashes(host, path string, data []byte, modTime time.Time, keepData bool) (
	precis *FilePrecis, chunks *Chunks, err error) {

	// These two different chunking approaches,
	// Jcdc and FastCDC, need very different
	// parameter min/max/average settings in
	// order to give good chunking.

	var opts *jcdc.CDC_Config

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

	precis = &FilePrecis{
		Host:        host,
		Path:        path,
		FileSize:    len(data),
		ModTime:     modTime,
		FileCry:     hash.Blake3OfBytesString(data),
		ChunkerName: cdc.Name(),
		CDC_Config:  cdc.Config(),
		HashName:    "blake3.32B",
	}
	chunks = NewChunks(path)
	chunks.FileSize = precis.FileSize
	chunks.FileCry = precis.FileCry

	if len(data) == 0 {
		return
	}

	cuts := cdc.Cutpoints(data, 0)

	//vv("cuts = '%#v'", cuts)

	prev := 0
	for i, c := range cuts {

		slc := data[prev:cuts[i]]

		hsh := hash.Blake3OfBytesString(slc)
		//fmt.Printf("[%03d]Summarize hsh = %v\n", i, hsh)
		chunk := &Chunk{
			Beg:  prev,
			Endx: cuts[i],
			Cry:  hsh,
		}
		if keepData {
			chunk.Data = slc
		}
		chunks.Chunks = append(chunks.Chunks, chunk)
		prev = c
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

	if !fileExists(path) {
		return SummarizeBytesInCDCHashes(host, path, nil, time.Time{}, false)
	}

	// These two different chunking approaches,
	// Jcdc and FastCDC, need very different
	// parameter min/max/average settings in
	// order to give good chunking.

	var opts *jcdc.CDC_Config

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

	sz64, modTime, err := FileSizeModTime(path)
	if err != nil {
		return nil, nil, err
	}
	sz := int(sz64)
	//vv("sz = %v", sz)

	precis = &FilePrecis{
		Host:     host,
		Path:     path,
		FileSize: sz,
		ModTime:  modTime,
		//FileCry:     hash.Blake3OfBytesString(data),
		ChunkerName: cdc.Name(),
		CDC_Config:  cdc.Config(),
		HashName:    "blake3.32B",
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

	if stat_t, ok := fi.Sys().(*syscall.Stat_t); ok {
		uid := stat_t.Uid
		precis.FileOwnerID = uid
		gid := stat_t.Gid
		precis.FileGroupID = gid

		owner, err := user.LookupId(fmt.Sprint(uid))
		if err == nil && owner != nil {
			precis.FileOwner = owner.Username
		}
		group, err := user.LookupGroupId(fmt.Sprint(gid))
		if err == nil && group != nil {
			precis.FileGroup = group.Name
		}
	}

	h := blake3.New(64, nil)

	defer func() {
		if precis != nil && chunks != nil {
			precis.FileCry = hash.SumToString(h)
			chunks.FileCry = precis.FileCry
		}
	}()

	if sz == 0 {
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
	addChunk := func(slc []byte, beg int) {
		hsh := hash.Blake3OfBytesString(slc)
		//fmt.Printf("[%03d]GetHashes hsh = %v\n", k, hsh)
		k++
		chunk := &Chunk{
			Beg:  beg,
			Endx: beg + len(slc),
			Cry:  hsh,
		}
		chunks.Chunks = append(chunks.Chunks, chunk)
		h.Write(slc)
	}

	//vv("using bufsz = %v in GetHashesOneByOne", bufsz)
	buf := make([]byte, bufsz)

	var totalread int // total bytes read from file so far.

	var bufbeg int  // offset where buf starts in the origin file.
	var bufendx int // offset where buf ends in the origin file.
	var dataoff int // offset where data starts in the original file. pass to addChunk()

	var nr int
	nr, err = io.ReadFull(fd, buf)
	if err == io.EOF {
		// no more bytes, exactly 0.
		// "The error is EOF only if no bytes were read."
		// -- https://pkg.go.dev/io#ReadFull
		err = nil
		return
	}
	bufendx += nr
	totalread += nr // gives offset of end of data

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

		cut := cdc.NextCut(data)

		if len(data) >= opts.MaxSize {
			// legit cut
			addChunk(data[:cut], dataoff)
			//vv("j=%v  legit cut: '%v'", j, chunks.Chunks[len(chunks.Chunks)-1])
			data = data[cut:]
			dataoff += cut
			if len(data) > opts.MaxSize {
				continue
			}
		}
		// INVAR: len(data) < opts.MaxSize, we need to read from file, if we can.
		if bufendx == sz {
			//vv("no more data available, the last will be oddly truncated.")
			for len(data) > 0 {
				cut := cdc.NextCut(data)
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
		k := copy(buf, data)
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
		bufendx += nr
		totalread += nr

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
		if len(data) == 0 && totalread != sz {
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
		precis, _, err = SummarizeBytesInCDCHashes(host, path, nil, time.Time{}, false)
		return
	}

	// These two different chunking approaches,
	// Jcdc and FastCDC, need very different
	// parameter min/max/average settings in
	// order to give good chunking.

	var opts *jcdc.CDC_Config

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

	sz64, modTime, err := FileSizeModTime(path)
	if err != nil {
		return nil, err
	}
	sz := int(sz64)
	//vv("sz = %v", sz)

	precis = &FilePrecis{
		Host:     host,
		Path:     path,
		FileSize: sz,
		ModTime:  modTime,
		//FileCry:     hash.Blake3OfBytesString(data),
		ChunkerName: cdc.Name(),
		CDC_Config:  cdc.Config(),
		HashName:    "blake3.32B",
	}

	var fi os.FileInfo
	fi, err = os.Stat(path)
	if err != nil {
		return nil, err
	}
	precis.FileMode = uint32(fi.Mode())
	precis.ModTime = modTime

	if stat_t, ok := fi.Sys().(*syscall.Stat_t); ok {
		uid := stat_t.Uid
		precis.FileOwnerID = uid
		gid := stat_t.Gid
		precis.FileGroupID = gid

		owner, err := user.LookupId(fmt.Sprint(uid))
		if err == nil && owner != nil {
			precis.FileOwner = owner.Username
		}
		group, err := user.LookupGroupId(fmt.Sprint(gid))
		if err == nil && group != nil {
			precis.FileGroup = group.Name
		}
	}

	precis.FileCry, err = Blake3OfFileIncremental(path)
	if err != nil {
		return nil, err
	}
	return
}

// Blake3OfFileIncremental hashes a whole, arbitrarily large
// file, using just a small 1MB read buffer.
func Blake3OfFileIncremental(path string) (fileCry string, err error) {

	var fi os.FileInfo
	fi, err = os.Stat(path)
	if err != nil {
		return "", err
	}
	sz := fi.Size()
	h := blake3.New(64, nil)

	defer func() {
		if err == nil {
			fileCry = hash.SumToString(h)
		}
	}()

	if sz == 0 {
		// defer sets fileCry
		return
	}

	// this was the fastest buffer size on my mac.
	// It was better than any higher or lower power of 2.
	bufsz := 1 << 20 // 1.901690203s fo N=1000;  N=2000 => 3.800150287s

	fd, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer fd.Close()

	//vv("using bufsz = %v in GetPrecis", bufsz)
	buf := make([]byte, bufsz)

	for {
		var nr int
		nr, err = io.ReadFull(fd, buf)
		if err == io.EOF {
			// no more bytes, exactly 0.
			// "The error is EOF only if no bytes were read."
			// -- https://pkg.go.dev/io#ReadFull
			err = nil
			return
		}
		if err == io.ErrUnexpectedEOF {
			err = nil
			// ignore short read error in next err != nil check.
		}
		if err != nil {
			return "", err
		}
		data := buf[:nr]
		h.Write(data)
	}
	return
}
