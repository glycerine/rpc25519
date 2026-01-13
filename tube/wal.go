package tube

import (
	//"encoding/binary"
	"fmt"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"strings"

	cristalbase64 "github.com/cristalhq/base64"
	"github.com/glycerine/blake3"
	"github.com/glycerine/greenpack/msgp"
	rpc "github.com/glycerine/rpc25519"
)

type raftWriteAheadLog struct {
	path        string
	fd          *os.File
	parentDirFd *os.File

	w *msgp.Writer
	r *msgp.Reader

	raftLog []*RaftLogEntry

	// these are used to allow state snapshots
	// to clear out the log and still let us
	// know logically where we are in the log.
	lli int64
	llt int64

	begseekpos []int64 // beginning position of each record in file.
	endpos     int64   // total bytes in file, seek to endpos to append after.
	parlog     *parLog

	logIndex *TermsRLE

	// check each TubeLogEntry entry.
	// re-use same for chain-hashing.
	checkEach *blake3.Hasher

	rpos int
	wpos int

	nodisk bool
	name   string

	isTest               bool
	testNum              int
	prevLastAppliedIndex int64
	noLogCompaction      bool
	// warning! if you add any new fields,
	// clear or copy them in Compact() below.

}

func dirExists(name string) bool {
	fi, err := os.Stat(name)
	if err != nil {
		return false
	}
	if fi.IsDir() {
		return true
	}
	return false
}

// DumpRaftWAL is used by tuber -log to dump the log for diagnostics.
func (s *TubeNode) DumpRaftWAL() error {

	if s.cfg.NoDisk {
		return s.wal.DumpRaftWAL_NODISK()
	}

	logPath := s.cfg.DataDir + sep + "tube.wal.msgp"

	wal, err := s.cfg.newRaftWriteAheadLog(logPath, true)
	if err != nil {
		return err
	}
	fmt.Printf("contents of disk raft wal '%v':\n", logPath)
	if len(wal.raftLog) == 0 {
		fmt.Printf("(empty disk Raft log)\n")

		if s.state != nil && s.state.ShadowReplicas != nil {
			fmt.Printf("s.state.ShadowReplicas = %v\n", s.state.ShadowReplicas.Short())
		} else {
			fmt.Printf("current s.state.ShadowReplicas = (not available)\n")
		}
		if s.state != nil && s.state.MC != nil {

			fmt.Printf("current s.state.MC = %v\ns.state.CurrentTerm: %v   s.state.CommitIndex: %v   ** len(wal.raftLog) == 0 => no way to know logIndex.BaseC/CompactTerm just from the (empty) log; state.CompactionDiscardedLastIndex=%v; state.CompactionDiscardedLastTerm = %v\n", s.state.MC.Short(), s.state.CurrentTerm, s.state.CommitIndex, s.state.CompactionDiscardedLast.Index, s.state.CompactionDiscardedLast.Term)
		} else {
			fmt.Printf("current s.state.MC = (not available)\n")
		}
		return nil
	}
	for i, e := range wal.raftLog {
		// count from 1 to match the Raft log Index.

		fmt.Printf("i=%03d %v {Lead:%v, Term:%02d, Index:%02d, Ticket: %v, CurrentCommitIndex:%v, Ticket.Op='%v', tkt4=%v Ticket.Desc: %v}\n", i+1, nice9(e.Tm.In(gtz)), e.LeaderName, e.Term, e.Index, e.Ticket.TicketID, e.CurrentCommitIndex, e.Ticket.Op, e.Ticket.TicketID[:4], e.Ticket.Desc)
	}
	fmt.Printf("\n")
	fmt.Printf("current s.state.MC = %v\n", s.state.MC.Short())
	fmt.Printf("s.state.ShadowReplicas = %v\n", s.state.ShadowReplicas.Short())
	fmt.Printf("s.state.CurrentTerm: %v   s.state.CommitIndex: %v   [logIndex.BaseC: %v; CompactTerm: %v]\n", s.state.CurrentTerm, s.state.CommitIndex, wal.logIndex.BaseC, wal.logIndex.CompactTerm)
	return nil
}

func (s *TubeNode) RaftWALString() (r string) {
	if s.cfg.NoDisk {
		return s.RaftWALString_NODISK()
	}

	logPath := s.cfg.DataDir + sep + "tube.wal.msgp"

	wal, err := s.cfg.newRaftWriteAheadLog(logPath, true)
	if err != nil {
		return fmt.Sprintf("(error in RaftWALString: '%v')\n", err)
	}
	r = fmt.Sprintf("contents of disk raft wal of len %v '%v':\n", logPath, len(wal.raftLog))
	if len(wal.raftLog) == 0 {
		r += fmt.Sprintf("(empty disk Raft log)\n")
		return
	}
	var ePrev *RaftLogEntry

	for i, e := range wal.raftLog {
		// count from 1 to match the Raft log Index.
		r += fmt.Sprintf("i=%03d  %v {Lead:%v, Term:%02d, Index:%02d, TicketID: %v, CurrentCommitIndex:%v, Ticket.Op='%v', tkt4=%v Ticket.Desc: %v}\n", i+1, nice9(e.Tm.In(gtz)), e.LeaderName, e.Term, e.Index, e.Ticket.TicketID, e.CurrentCommitIndex, e.Ticket.Op, e.Ticket.TicketID[:4], e.Ticket.Desc)

		// assert the chain is linked properly
		if i == 0 {
			if wal.logIndex.BaseC != e.PrevIndex {
				panicf("bad chain link index: wal.logIndex.BaseC(%v) != e.PrevIndex(%v)", wal.logIndex.BaseC, e.PrevIndex)
			}
			if wal.logIndex.CompactTerm != e.PrevTerm {
				panicf("bad chain link term: wal.logIndex.CompactTerm(%v) != e.PrevTerm(%v)", wal.logIndex.CompactTerm, e.PrevTerm)
			}
		} else {
			if ePrev.Index != e.PrevIndex {
				panicf("bad chain link index: ePrev.Index(%v) != e.PrevIndex(%v)", ePrev.Index, e.PrevIndex)
			}
			if ePrev.Term != e.PrevTerm {
				panicf("bad chain link term: ePrev.Term(%v) != e.PrevTerm(%v)", ePrev.Term, e.PrevTerm)
			}
		}
		ePrev = e
	}
	r += " --- (end of disk RaftWALString) ---\n"
	return
}

// readOnly flag provided for DumpRaftWAL above/tuber diagnostics.
func (cfg *TubeConfig) newRaftWriteAheadLog(path string, readOnly bool) (s *raftWriteAheadLog, err0 error) {
	//vv("newRaftWriteAheadLog(path = '%v'); nodisk=%v; cfg.MyName='%v'", path, cfg.NoDisk, cfg.MyName)

	defer func() {
		s.assertConsistentWalAndIndex(0)
	}()

	if cfg.NoDisk {
		return cfg.newRaftWriteAheadLogMemoryOnlyTestingOnly()
	}
	var parentDirFd *os.File
	if !readOnly {
		// make dir if necessary
		dir := filepath.Dir(path)
		panicOn(os.MkdirAll(dir, 0700))

		// To understand why parentDirFd is needed, and
		// why we must also fsync the parent directory,
		// we quote the fsync(2) - Linux man page:
		//
		// "Calling fsync() does not necessarily ensure that
		// the entry in the directory containing the file has
		// also reached disk. For that an explicit fsync() on
		// a file descriptor for the directory is also needed."
		// -- https://linux.die.net/man/2/fsync

		// don't get fooled by last dir symlinks.
		dir2, err2 := getActualParentDirForFsync(path)
		panicOn(err2)

		var err error
		parentDirFd, err = os.Open(dir2)
		panicOn(err)
	}

	var sz int64
	fi, err := os.Stat(path)
	if err == nil {
		sz = fi.Size()
	} // else file does not exist yet, most likely.

	fd, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	// From github.com/tigerbeetle/tigerbeetle/src/io/linux.zig:1640
	// in version 0.16.61 of tigerbeetle:
	// "The best fsync strategy is always to fsync before
	// reading because this prevents us from
	// making decisions on data that was never durably
	// written by a previously crashed process.
	// We therefore always fsync when we open the
	// path, also to wait for any pending O_DSYNC.
	// Thanks to Alex Miller from FoundationDB for
	// diving into our source and pointing this out."
	err = fd.Sync()
	panicOn(err)
	if err != nil {
		return nil, err
	}

	var parlog *parLog
	if !readOnly {
		parlog, err = newParLog(path+".parlog", cfg.NoDisk)
		if err != nil {
			return nil, fmt.Errorf("newParLog error: '%v'", err)
		}
	}
	s = &raftWriteAheadLog{
		path:            path,
		fd:              fd,
		parentDirFd:     parentDirFd,
		w:               msgp.NewWriter(fd),
		r:               msgp.NewReader(fd),
		logIndex:        newTermsRLE(),
		checkEach:       blake3.New(64, nil),
		parlog:          parlog,
		name:            cfg.MyName,
		noLogCompaction: cfg.NoLogCompaction,
		isTest:          cfg.isTest,
		testNum:         cfg.testNum,
		lli:             0,
		llt:             0,
	}
	//vv("%v made newRaftWriteAheadLog(path = '%v'); nodisk=%v; cfg.MyName='%v'; noLogCompaction=%v", s.name, path, cfg.NoDisk, cfg.MyName, s.noLogCompaction)

	if sz == 0 {
		return
	}
	err0 = s.loadPathHelper(fd, sz, readOnly)
	return
}

func (s *raftWriteAheadLog) logSizeOnDisk() int64 {
	if s.fd == nil {
		return 0
	}
	fi, err := s.fd.Stat()
	if err != nil {
		return 0
	}
	return fi.Size()
}

func (s *raftWriteAheadLog) loadPathHelper(fd *os.File, sz int64, readOnly bool) error {

	var nr int64 // number of bytes read in.
	_ = nr       // todo: verify / update s.endpos and s.seekendpos
	var ePrev *RaftLogEntry
	for j := 0; ; j++ {
		s.begseekpos = append(s.begseekpos, curpos(s.fd))
		e, err := s.loadRaftLogEntry(j, readOnly)
		if err == io.EOF {
			//vv("breaking on EOF at j = %v", j)
			break
		}
		if j == 0 {
			// restore Base b/c Compaction can make > 0.

			// here in wal.loadPathHelper()
			s.logIndex.BaseC = e.PrevIndex
			if e.PrevIndex != e.Index-1 {
				panic(fmt.Sprintf("assert red: chain link bad! en.Index-1=%v but en.PrevIndex = %v", e.Index-1, e.PrevIndex)) // panic: assert red: chain link bad! en.Index-1=1 but en.PrevIndex = 0
			}
			s.logIndex.CompactTerm = e.PrevTerm

			// we check these in the defer on newRaftWriteAheadLog,
			// and its tough to call here directly.
			//if syncme != nil {
			//	syncme.Index = e.logIndex.BaseC
			//	syncme.Term = e.logIndex.CompactTerm
			//}

			s.lli = e.Index - 1
			s.llt = e.PrevTerm
		} else {
			// in loadPathHelper, at j > 0 so ePrev is set.
			// assert the chain is linked properly
			if ePrev.Index != e.PrevIndex {
				panicf("j=%v: bad chain link index: ePrev.Index(%v) != e.PrevIndex(%v)", j, ePrev.Index, e.PrevIndex) // panic: j=2: bad chain link index: ePrev.Index(2) != e.PrevIndex(1) on Test201_raftWriteAheadLogSaveLoad
			}
			if ePrev.Term != e.PrevTerm {
				panicf("j=%v: bad chain link term: ePrev.Term(%v) != e.PrevTerm(%v)", j, ePrev.Term, e.PrevTerm)
			}
		}
		ePrev = e
		if err == nil {
			s.raftLog = append(s.raftLog, e)
			s.lli++
			s.llt = e.Term
			s.logIndex.AddTerm(e.Term)
		} else {
			panicOn(err)
		}
	}
	//vv("loaded %v", len(s.raftLog))

	// make sure we are next writing at the end.
	// go docs for Seek: "The behavior of Seek on a file opened
	// with O_APPEND is not specified."
	// Since we might trim the log, let us not use O_APPEND
	// and its unspecified behavior.
	sz2, err := fd.Seek(0, io.SeekEnd)
	panicOn(err)
	if sz2 != sz {
		panic(fmt.Sprintf("expected sz2(%v) == sz(%v)", sz2, sz))
	}
	return nil
}

func (s *raftWriteAheadLog) close() (err error) {
	if s.nodisk {
		return s.close_NODISK()
	}
	s.parlog.close()
	s.w.Flush()
	err = s.fd.Close()
	if err != nil {
		return
	}
	s.fd = nil
	s.w = nil
	return
}

func (s *raftWriteAheadLog) sync() error {
	err := s.fd.Sync()
	panicOn(err)

	// need to sync parent directory for durability too.
	if s.parentDirFd != nil {
		err = s.parentDirFd.Sync()
		panicOn(err)
	}
	return nil
}

// get current position in file fd.
func curpos(fd *os.File) int64 {
	pos, err := fd.Seek(0, io.SeekCurrent)
	panicOn(err)
	return int64(pos)
}

func setpos(fd *os.File, pos int64) {
	pos, err := fd.Seek(pos, io.SeekStart)
	panicOn(err)
}

func (s *raftWriteAheadLog) saveRaftLogEntry(entry *RaftLogEntry) (nw int, err error) {
	if s.nodisk {
		return s.saveRaftLogEntry_NODISK(entry)
	}
	nw, err = s.saveRaftLogEntryDoFsync(entry, true)
	return
}

func (s *raftWriteAheadLog) saveRaftLogEntryDoFsync(entry *RaftLogEntry, doFsync bool) (nw int, err error) {
	//vv("%v saveRaftLogEntryDoFsync called; entry = '%v'; doFsync = %v", s.name, entry, doFsync)
	if s.nodisk {
		return s.saveRaftLogEntry_NODISK(entry)
	}

	// note: the logIndex won't be in sync with the raftLog
	// until we return and overwriteEntries() can adjust the logIndex too.

	if s.lli+1 != entry.Index {
		panic(fmt.Sprintf("incorrect entry.Index = %v vs "+
			"lastIndex+1 = %v", entry.Index, s.lli+1))
	}

	llt := s.logIndex.lastTerm()
	lli := s.logIndex.lastIndex()

	// assert the outer (s.lli and s.llt) are in sync
	// with the logIndex.
	if llt != s.llt {
		panicf("why out of sync? llt=%v, s.llt=%v", llt, s.llt)
	}
	if lli != s.lli {
		panicf("why out of sync? lli=%v, s.lli=%v", lli, s.lli)
	}
	// these were useful for making sure 20_ tests were setup
	// correctly, but replicateTicket just wants us to set the
	// right thing; accounting for any compaction that has
	// happened or not.
	//if entry.PrevTerm != llt {
	//	panicf("logIndex.lastTerm out of sync! entry.PrevTerm(%v) != s.logIndex.lastTerm()=%v; logIndex='%v'", entry.PrevTerm, llt, s.logIndex)
	//}
	//if entry.PrevIndex != lli {
	//	panicf("logIndex.lastIndex out of sync! entry.PrevIndex(%v) != s.logIndex.lastIndex()=%v", entry.PrevIndex, lli)
	//}
	entry.PrevTerm = llt
	entry.PrevIndex = lli
	s.logIndex.AddTerm(entry.Term)

	//pos := curpos(s.fd)
	//vv("saveRaftLogEntry called, pos = %v", pos)

	posnow := curpos(s.fd)
	s.begseekpos = append(s.begseekpos, posnow)
	b, err := entry.MarshalMsg(nil)
	panicOn(err)

	entry.msgbytes = b

	bs := ByteSlice(b)
	by2, err := bs.MarshalMsg(nil)
	panicOn(err)
	nw0, err := s.fd.Write(by2)
	panicOn(err)
	nw = nw0

	var nw2 int
	var b3string string
	nw2, b3string, err = s.saveBlake3sumFor(b, doFsync)
	panicOn(err)
	nw += nw2
	// redundant: saveBlake3sumFor does this
	// sync already if doFsync.
	//err = s.sync()
	//panicOn(err)
	s.raftLog = append(s.raftLog, entry)
	// note that above we asserted s.lli+1 == entry.Index,
	// so this is the same as s.lli = entry.Index:
	s.lli++
	s.llt = entry.Term
	s.parlog.append1(entry, posnow, int64(nw0), int64(nw2), b3string)

	return
}

func (s *raftWriteAheadLog) loadRaftLogEntry(j int, readOnly bool) (ds *RaftLogEntry, err0 error) {

	if s.nodisk {
		return s.loadRaftLogEntry_NODISK(j)
	}

	pos := curpos(s.fd)
	//vv("loadRaftLogEntry called, pos = %v", pos)

	defer func() {
		if err0 != nil {
			setpos(s.fd, pos) // restore pos
		}
	}()

	readme, err := nextframe(s.fd, s.path)
	if err == io.EOF {
		return nil, err
	}
	panicOn(err)

	var by ByteSlice
	_, err = by.UnmarshalMsg(readme)
	//vv("load sees err=%v from UnmarshalMsg len(by)=%v", err, len(by))
	panicOn(err)
	if err != nil {
		return
	}
	s.rpos += len(by)

	ds = &RaftLogEntry{}
	_, err = ds.UnmarshalMsg(by)
	panicOn(err)
	//vv("ds = '%#v'", ds)

	// save for hash-chaining
	ds.msgbytes = by

	// read the checksum from disk
	onDisk, err := s.loadBlake3sum()
	panicOn(err)

	// verify checksum (really a cryptographic hash)
	s.checkEach.Reset()
	s.checkEach.Write(by)
	h := blake3ToString33B(s.checkEach)

	s.checkEach.Write(s.lastEntryBytes())
	hChain := blake3ToString33B(s.checkEach)

	if h != onDisk.Sum33B {
		panic(fmt.Sprintf("corrupt raftWriteAheadLog '%v' at pos '%v'. onDisk.Sum:'%v'; vs. re-computed-hash: '%v'", s.path, s.rpos, onDisk.Sum33B, h))
	}

	if hChain != onDisk.Sum33Bchain {
		panic(fmt.Sprintf("hash-chain checksum mismatch in raftWriteAheadLog '%v' at pos '%v'. onDisk.Sum33Bchain:'%v'; vs. re-computed-hash: '%v'", s.path, s.rpos, onDisk.Sum33Bchain, hChain))
	}

	if !readOnly { // read-only log dump skips creating parLog.
		// and check against the parlog blake3 hash too.
		s.validateParLog(pos, j, h, ds)
	}

	return
}

// TODO: par-log needs to handle both
// a) snapshot state transfer, where we no longer start at 1; and
//
// b) compaction that can trim a prefix of log entries off.
// I think we handle compaction now because we entirely re-write
// both log and par file in Compact(). maybe. not sure.
func (s *raftWriteAheadLog) validateParLog(pos int64, j int, h string, ds *RaftLogEntry) {
	pr, err := s.parlog.at(int64(j))
	panicOn(err)

	if h != pr.RLEblake3 {
		pos_same := pos == pr.Offset
		if !pos_same {
			panic(fmt.Sprintf("reading j=%v from pos %v but parlog has Offset '%v'", j, pos, pr.Offset))
		}
		tktsame := ds.Ticket.TicketID == pr.TicketID
		if !tktsame {
			panic(fmt.Sprintf("reading j=%v parlog.TicketID='%v' but read locally TicketID='%v'", j, pr.TicketID, ds.Ticket.TicketID))
		}
		idxsame := ds.Index == pr.Index
		if !idxsame {
			panic(fmt.Sprintf("at j = %v, parlog Index = '%v' but RLE Index ='%v'", j, pr.Index, ds.Index))
		}
		termsame := ds.Term == pr.Term
		if !termsame {
			panic(fmt.Sprintf("at j = %v, parlog Term = '%v' but RLE Term ='%v'", j, pr.Term, ds.Term))
		}
		clustersame := ds.Ticket.ClusterID == pr.ClusterID
		if !clustersame {
			panic(fmt.Sprintf("parlog ClusterID = '%v' but RLE Ticket.ClusterID='%v'", pr.ClusterID, ds.Ticket.ClusterID))
		}
		panic(fmt.Sprintf("parlog blake3 hash mismatch in raftWriteAheadLog '%v' at j=%v; pos '%v'. ParRecord.RLEblake3 = '%v'; vs. re-computed-hash: '%v'; tktsame='%v', idxsame='%v', termsame='%v', clustersame='%v'", s.path, j, s.rpos, pr.RLEblake3, h, tktsame, idxsame, termsame, clustersame))
	}
}

func (s *raftWriteAheadLog) loadBlake3sum() (bla *Blake3sum, err error) {
	//vv("load called")

	//pos := curpos(s.fd)
	//vv("loadBlake3sum called, pos = %v", pos)

	readme, err := nextframe(s.fd, s.path)
	if err == io.EOF {
		return nil, err
	}
	panicOn(err)

	var by ByteSlice
	_, err = by.UnmarshalMsg(readme)
	//vv("load sees err=%v from msgp.Decode; len(by)=%v", err, len(by))
	panicOn(err)
	if err != nil {
		return
	}
	s.rpos += len(by)

	bla = &Blake3sum{}
	_, err = bla.UnmarshalMsg(by)
	panicOn(err)

	//vv("loadBlake3sum called, pos = %v\n bla = '%#v'", pos, bla)

	return
}

func blake3ToString33B(h *blake3.Hasher) string {
	by := h.Sum(nil)
	return "blake3.33B-" + cristalbase64.URLEncoding.EncodeToString(by[:33]) + "\n"
}

func blake3ToInt64(h *blake3.Hasher) int64 {
	b := h.Sum(nil)
	return (int64(b[0]) << 56) | (int64(b[1]) << 48) |
		(int64(b[2]) << 40) | (int64(b[3]) << 32) |
		(int64(b[4]) << 24) | (int64(b[5]) << 16) |
		(int64(b[6]) << 8) | (int64(b[7]))
}

func checksum64crc(by []byte) uint64 {
	table := crc64.MakeTable(crc64.ECMA) // or crc64.ISO
	hash := crc64.New(table)
	_, err := hash.Write(by)
	panicOn(err)
	return hash.Sum64()
}

func (s *raftWriteAheadLog) saveBlake3sumFor(by []byte, doFsync bool) (nw int, b3string string, err error) {

	//pos := curpos(s.fd)
	//vv("saveBlake3sumFor called, pos = %v", pos)
	s.checkEach.Reset()
	s.checkEach.Write(by)
	b3string = blake3ToString33B(s.checkEach)

	// re-use the same hasher, first
	// bytes are already in it.
	prev := s.lastEntryBytes()
	//vv("prev = len %v: '%x", len(prev), prev)
	s.checkEach.Write(prev)
	hChain := blake3ToString33B(s.checkEach)

	//vv("saveBlake3sumFor called, pos = %v -> \n b3string = %v\n hChain = %v", pos, b3string, hChain)

	blak := &Blake3sum{
		Sum33B:      b3string,
		Sum33Bchain: hChain,
	}
	b, err := blak.MarshalMsg(nil)
	panicOn(err)

	bs := ByteSlice(b)
	by2, err := bs.MarshalMsg(nil)
	panicOn(err)
	nw, err = s.fd.Write(by2)
	panicOn(err)
	if doFsync {
		err = s.sync()
		panicOn(err)
	}
	return
}

// overwriteEntries will
// fsync once after writing a whole batch of appended entries.
// The entire point of the Raft log is to be able to
// fsync a whole batch of log appends at once; otherwise
// there is really no point in the large
// complexity of maintaining a log at all. Hence
// the implementation here must (and does) only fsync
// once at the end, after all entries are appended.
//
// if isLeader, also assert that we never overwrite
// any entries in our log, since that is
// a Raft principle that is necessary for safety.
// leaders should never call this, in fact! We will
// panic to remind them.
//
// curCommitIndex is to allow additional asserts,
// but ignore it if -1 b/c not sure if/how it should
// be set correctly in wal_test.go especially
// (maybe raft_test.go too).
//
// The return value deletedMemberConfigIdx will
// be > 0 iff (as a follower) we overwrite a
// pre-exiting MemberConfig entry, in which
// case, per page 36, the caller
// now needs to fall back on the prior config.
//
// The deletedMemberConfigIdx gives the
// log index of the overwritten config
// entry. It will be the maximum of any
// such overwritten entry.
//
// with compaction, maybe we need to
// think of keepCount as keepIndex(!)
//
// overwriteEntries may call maybeCompact()
// if log-compaction is on.
func (s *raftWriteAheadLog) overwriteEntries(keepIndex int64, es []*RaftLogEntry, isLeader bool, curCommitIndex, lastAppliedIndex int64, syncme *IndexTerm, node *TubeNode) (err error) {

	//vv("%v begin overwriteEntries()[isAppend=%v] keepIndex=%v; len(raftLog)=%v: %v", s.name, s.isAppendLoggingHelper(keepIndex), keepIndex, len(s.raftLog), s.StringWithCommit(curCommitIndex))

	if true { // TODO restore: isTest {
		defer s.assertConsistentWalAndIndex(keepIndex)
	}

	// figure out who is writing member change Ticket
	// before noop0. Yeah well, both tube.go:2220 and
	// tube.go:2290 do it by calling
	// s.setupFirstRaftLogEntryBootstrapLog(boot)
	// because we need to be able to bootstrap up
	// a cluster, possibly from a single node...
	// so we let it have priority over noop0. Thus
	// noop0 is no longer the first thing in every wal.

	if isLeader {
		panic("leaders should use saveRaftLogEntry(), never overwriteEntries, since Raft requires leaders never overwrite their logs.")
	}

	n := int64(len(s.raftLog))

	var overwriteCount int64
	var keepCount int64
	var firstIndex int64
	var lastIndex int64

	// snapshot aware: we cannot delete the barrier.
	earliestMustKeepBarrier := s.lli // assume snapshot installed this
	// snapshot installation may have left us with n==0
	if n > 0 {
		// either no snapshot yet, or we have had appends since then.
		// allow the first to be overwritten, if need be; after leader change.
		earliestMustKeepBarrier = s.raftLog[0].Index - 1
	}

	nes := int64(len(es))
	if keepIndex < earliestMustKeepBarrier {
		//panic(fmt.Sprintf("attempt to truncate before snapshot or compacted log: keepIndex(%v) < s.lli(%v)", keepIndex, s.lli))

		proposedNewLastIndex := keepIndex + nes
		if proposedNewLastIndex <= earliestMustKeepBarrier {
			vv("warning: dropping log entries! nothing we can do; all of es is already snapshot/compacted behind our barrier")
			return nil
		}
		// INVAR: keepIndex + nes > earliestMustKeepBarrier
		//  subtract keepIndex from both sides:
		//    so  nes > earliestMustKeepBarrier - keepIndex == discardN
		//    so  es = es[discardN:] should leave us at least 1 to append.
		if keepIndex <= earliestMustKeepBarrier {
			discardN := earliestMustKeepBarrier - keepIndex
			if discardN > 0 {
				// note how we are adjusting keepIndex and es here,
				// to account for log compaction and snapshot installation.
				keepIndex += discardN
				es = es[discardN:]
				nes = int64(len(es))
			}
		}
		// INVAR: keepIndex >= earliestMustKeepBarrier
		if keepIndex < earliestMustKeepBarrier {
			panic("assertion failed: logic above did not guarantee INVAR: keepIndex >= earliestMustKeepBarrier")
		}
	}
	// INVAR: keepIndex >= earliestMustKeepBarrier

	if n == 0 {
		keepCount = keepIndex - earliestMustKeepBarrier // >= 0
		// impossible to overwrite anything, since n == 0
		overwriteCount = 0 // for emphasis.
	} else {
		firstIndex = s.raftLog[0].Index
		lastIndex = s.raftLog[n-1].Index

		// original logic:
		if keepIndex >= lastIndex {
			// clamp it down to what we have.
			keepIndex = lastIndex
			keepCount = n
			overwriteCount = 0
		} else if keepIndex < firstIndex {
			// clamp it up to what we have.
			keepIndex = firstIndex - 1
			keepCount = 0
			overwriteCount = n
		} else {
			// INVAR: firstIndex <= keepIndex < lastIndex
			keepCount = keepIndex - firstIndex + 1
			overwriteCount = n - keepCount
		}
	}
	if overwriteCount < 0 {
		panic(fmt.Sprintf("invariant violated: overwrite(%v) must be >= 0 now", overwriteCount))
	}
	// INVAR: overwriteCount >= 0: how much of the log is re-written;
	// the log might be even longer afterwards too, of course.

	if s.nodisk {
		return s.overwriteEntries_NODISK(keepIndex, es, isLeader, curCommitIndex, lastAppliedIndex, keepCount, overwriteCount, syncme)
	}

	origSz := curpos(s.fd)
	mayNeedTruncate := false
	//vv("overwriteCount = %v; keepCount = %v", overwriteCount, keepCount)
	if overwriteCount == 0 {
		// just appending
	} else {
		// overwriting the tail
		beg := s.begseekpos[keepCount]
		setpos(s.fd, beg)
		// assert we are not killing any committed txn,
		// also that we are not rewriting the same
		// thing atop of itself.
		killed := s.raftLog[keepCount:]
		if len(killed) > 0 && isLeader {
			panic(fmt.Sprintf("bad! leader can never overwrite any (here %v) log entries!", len(killed)))
		}
		// to this point, only nes has referred to es at all. so
		// all other numbers are with respect to s.raftLog not es.
		// for instance, overwriteCount. we can overwrite more than
		// we are writing by truncating off 7 existing entries with
		// 1 new one that leaves now 6 un-used indexes, for example.
		// replacements := es[:overwriteCount] // panic: runtime error: slice bounds out of range [:51] with capacity 1
		// below we will write the full es. here is just some debug checking
		// for overwriting commited entries or self with self.
		replacements := es[:min(overwriteCount, int64(len(es)-1))]
		for i, e := range killed {

			if s.noLogCompaction {
				if replacements[i].Equal(killed[i]) {
					panic(fmt.Sprintf("don't overwrite an entry with itself! i=%v, replacements[%v]='%v'; killed[%v]='%v'; especially because we want to avoid truncation of a good log by a smaller write.", i, i, replacements[i], i, killed[i]))
				}
				if e.Index <= curCommitIndex {
					panic(fmt.Sprintf("curCommitIndex = %v; don't kill a committed log entry! %v", curCommitIndex, e))
				}
			}
		}

		// okay then
		s.begseekpos = s.begseekpos[:keepCount]
		s.raftLog = s.raftLog[:keepCount]

		err = s.parlog.truncate(keepCount)
		panicOn(err)
		mayNeedTruncate = true
		s.logIndex.Truncate(keepIndex, syncme)
		s.lli = keepIndex
		if keepCount == 0 {
			s.llt = 0
		} else {
			s.llt = s.raftLog[keepCount-1].Term
		}
	}
	var nw int
	// use nw to update s.endseekpos by using and then updating s.endpos
	_ = nw
	for _, e := range es {

		// false => skip fsync-ing each, we will do at the end.
		nw, err = s.saveRaftLogEntryDoFsync(e, false)
		if err != nil {
			s.sync()
			panicOn(err)
			return
		}
	}
	// saveRaftLogEntryDoFsync() takes care of updating lli,llt.

	if mayNeedTruncate {
		newSz := curpos(s.fd)
		if newSz < origSz {
			// get rid of any not overwritten garbage.
			s.fd.Truncate(newSz)
		}
	}
	s.sync() // this is the fsync
	//vv("%v raft log overwriteEntries() just did fsync", s.name)

	if !s.noLogCompaction {
		// THIS IS THE LOG COMPACTION CALL.
		//
		// We currently compact after
		// every new commit, to catch bugs/issues early ASAP--
		// per the Raft Dissertation suggestion (page 57,
		// section 5.1.3 -- "Implementation concerns").
		//
		// leave off until TermsRLE is compaction ready.
		s.maybeCompact(lastAppliedIndex, syncme, node)
	}
	return
}

// just help the isAppend logging at top of overwriteEntries;
// copies logic at top of overwriteEntries.
func (s *raftWriteAheadLog) isAppendLoggingHelper(keepIndex int64) bool {

	n := int64(len(s.raftLog))
	var overwriteCount, keepCount int64
	var firstIndex, lastIndex int64
	if n == 0 {
		return keepIndex == 0 // else we have gap so return false
	} else {
		firstIndex = s.raftLog[0].Index
		lastIndex = s.raftLog[n-1].Index
	}

	if keepIndex >= lastIndex {
		return true
	} else if keepIndex < firstIndex {
		return false
	} else {
		// INVAR: firstIndex <= keepIndex < lastIndex
		keepCount = keepIndex - firstIndex + 1
		overwriteCount = n - keepCount
	}
	return overwriteCount == 0
}

func (s *raftWriteAheadLog) maybeCompact(lastAppliedIndex int64, syncme *IndexTerm, node *TubeNode) (didCompact bool) {

	// defaults in case we bail early (important to keep stuff in sync!)
	if syncme != nil {
		syncme.Index = s.logIndex.BaseC
		syncme.Term = s.logIndex.CompactTerm
	}
	if s.noLogCompaction {
		return
	}
	if s.isTest && s.testNum == 802 {
		return // 802 not designed to test compaction
	}
	if lastAppliedIndex > s.prevLastAppliedIndex {
		//vv("%v compaction on, about to call Compact(lastAppliedIndex) because lastAppliedIndex(%v) > s.prevLastAppliedIndex(%v); before Compact(), our logIndex.BaseC=%v", s.name, lastAppliedIndex, s.prevLastAppliedIndex, s.logIndex.BaseC)
		//defer func() {
		//vv("%v after Compact() called, compactIndex=%v ; compactTerm=%v; s.logIndex=%v", s.name, compactIndex, compactTerm, s.logIndex)
		//}()

		s.prevLastAppliedIndex = lastAppliedIndex
		var err error
		// only live call (non-test) to Compact is here in maybeCompact().
		_, _, err = s.Compact(lastAppliedIndex, syncme, node)
		panicOn(err)
		didCompact = true
	} else {
		//vv("%v compaction is on, but skipping because lastAppliedIndex(%v) <= s.prevLastAppliedIndex(%v)", s.name, lastAppliedIndex, s.prevLastAppliedIndex)
	}
	return
}

func (s *raftWriteAheadLog) getTermsRLE() *TermsRLE {
	return s.logIndex.clone()
}

// the tree of life. Used as the
// previous bytes to start the chain.
// (Poetic Edda, Norse mythology).
var yggdrasill = []byte("yggdrasill")

func (s *raftWriteAheadLog) lastEntryBytes() (prevEntry []byte) {
	n := len(s.raftLog)
	switch n {
	case 0:
		return
	case 1:
		return yggdrasill
	default:
		return s.raftLog[n-1].msgbytes
	}
}

func (s *raftWriteAheadLog) panicOnStaleIndex() {
	if !s.indexInSync() {
		panic("logIndex not in sync with RaftLog Term structure")
	}
}

func (s *raftWriteAheadLog) indexInSync() bool {
	if s == nil {
		return true
	}
	n := int64(len(s.raftLog))
	if n != s.logIndex.Endi {
		panic("n != s.wal.logIndex.Endi")
		return false
	}
	i := 0
	rlog := s.raftLog

	frombeg := s.logIndex.BaseC + 1
	for _, run := range s.logIndex.Runs {
		for j := range run.Count {
			if rlog[i].Term != run.Term {
				panic(fmt.Sprintf("RaftLog[i=%v].Term = %v but TermsRLS says term %v at j=%v occurance of term %v", i, rlog[i].Term, run.Term, j, run.Term))
				return false
			}
			if rlog[i].Index != frombeg {
				panic(fmt.Sprintf("RaftLog[i=%v].Index = %v but TermsRLS says term %v at j=%v occurance of should be on frombeg= %v", i, rlog[i].Index, run.Term, j, frombeg))
				return false
			}
			i++
			frombeg++
		}
	}
	return true
}

// returns the index of the last log now,
// since due to log compaction our "length"
// may shrink and what append-entries needs
// to know is our "logical length", or exactly
// the Index of the last log entry; or 0 if
// no entries at all yet.
func (s *raftWriteAheadLog) LogicalLen() int64 {
	return s.lli
}

func (s *raftWriteAheadLog) LastLogIndexAndTerm() (idx, term int64) {
	return s.lli, s.llt
}

func (s *raftWriteAheadLog) LastLogTerm() int64 {
	return s.llt
}

func (s *raftWriteAheadLog) LastLogIndex() int64 {
	return s.lli
}

func (s *raftWriteAheadLog) StringWithCommit(commitIndex int64) (r string) {
	r = fmt.Sprintf("raftWriteAheadLog of len %v [compactIndex: %v; compactTerm: %v; noLogCompaction: %v]:\n", len(s.raftLog), s.logIndex.BaseC, s.logIndex.CompactTerm, s.noLogCompaction)
	var showcommit string
	for i, rle := range s.raftLog {
		_ = i
		if rle.Index <= commitIndex {
			showcommit = "*" // committed
		} else {
			showcommit = "_" // uncommitted
		}
		r += fmt.Sprintf("   [%02d%v/term %02d] %v\n", rle.Index, showcommit, rle.Term, rle.Ticket.Desc)
	}
	r += fmt.Sprintf("logIndex: %v\n", s.logIndex)
	return
}

var ErrEmptyRaftLog = fmt.Errorf("error: empty raft log")

func (s *raftWriteAheadLog) GetAllEntriesFrom(idx int64) (rles []*RaftLogEntry, preBase bool) {

	if idx < 1 {
		panic(fmt.Sprintf("bad idx=%v in call to GetAllEntriesFrom(). must be > 0", idx))
	}

	n := int64(len(s.raftLog))
	if n == 0 {
		return
	}

	firstIndex := s.raftLog[0].Index
	if idx < firstIndex {
		// we might have part, but we are definitely have gap
		// where snapshot would be needed too.
		preBase = true
		//err = fmt.Errorf("error: index not available; too small. firstIndex='%v'; requested idx='%v'", firstIndex, idx)
		return
	}
	// INVAR: idx >= firstIndex
	offset := idx - firstIndex
	if offset >= n {
		// definitely nothing to offer.
		//err = fmt.Errorf("error: index not available; too large. offset = '%v' >= n where n available='%v'; requested idx='%v'; firstIndex in memory='%v'", offset, n, idx, firstIndex)
		return
	}
	rles = s.raftLog[offset:]
	return
}

func (s *raftWriteAheadLog) GetEntry(idx int64) (rle *RaftLogEntry, err error) {
	n := int64(len(s.raftLog))
	if n == 0 {
		err = ErrEmptyRaftLog
		return
	}
	firstIndex := s.raftLog[0].Index
	if idx < firstIndex {
		err = fmt.Errorf("error: index not available; too small. firstIndex='%v'; requested idx='%v'", firstIndex, idx)
		return
	}
	// INVAR: idx >= firstIndex
	offset := idx - firstIndex
	if offset >= n {
		err = fmt.Errorf("error: index not available; too large. offset = '%v' >= n where n available='%v'; requested idx='%v'; firstIndex in memory='%v'", offset, n, idx, firstIndex)
		return
	}
	rle = s.raftLog[offset]
	return
}

/*
PAR / CTRL recovery logic:
from https://research.cs.wisc.edu/adsl/Software/par/proof.pdf

"During recovery, if the persist record is not present
and a checksum mismatch occurs for an entry,
Ctrl’s recovery code can determine that the mismatch
is due to an interrupted update (i.e., a crash)."

"Conversely, if the persist record is present and
a checksum mismatch occurs, the recovery code can
conclude that it is a storage corruption."

"The above disentanglement logic works correctly
when an entry is explicitly ordered before
its persist record using a fsync system call."

"However, such additional fsync calls could
affect the log-update performance significantly.
For this reason, Ctrl does not
explicitly order an entry before
its persist record."

"Without explicitly ordering a data item before its
persist record, Ctrl can still distinguish crashes from
corruptions in most cases. However, if a checksum
mismatch happens for the last entry in the log and if
its persist record is present, Ctrl cannot determine
whether the mismatch is a due to storage corruption
or a system crash. The inability to disentangle the
last entry when its persist record is present is
not specific to Ctrl, but rather a fundamental limitation in
any log-based system. The following section presents
the proof of this claim."

*/

// Compact leaves only the last entry in the log, trimming
// off all others. What if they are not committed though?
// keep everything >= keepIndex. We must retain at least one
// entry from the current log. We will panic to assert this.
// keepIndex == 0 by convention means keep everything => NO-OP.
// So any keepIndex <= 0 will be a NO-OP.
func (s *raftWriteAheadLog) Compact(keepIndex int64, syncme *IndexTerm, node *TubeNode) (origPath, origParPath string, err0 error) {

	//vv("%v Compact(keepIndex=%v) called.", s.name, keepIndex)

	if s.noLogCompaction {
		panic("s.noLogCompaction true, we should never have called Compact!")
	}

	defer func() {
		if syncme != nil {
			syncme.Index = s.logIndex.BaseC
			syncme.Term = s.logIndex.CompactTerm
		}
		s.assertConsistentWalAndIndex(keepIndex)
		if node != nil {
			node.assertCompactOK()
		}
	}()

	n := int64(len(s.raftLog))
	if n == 0 || keepIndex <= 0 {
		return
	}
	// INVAR: keepIndex > 0
	//vv("%v top of raftLog.Compact(keepIndex = %v)", s.name, keepIndex)
	//defer func() {
	//vv("%v end of raftLog.Compact(keepIndex = %v); AFTER:\n%v", s.name, keepIndex, s.StringWithCommit(s.prevLastAppliedIndex))
	//}()

	// validate keepIndex valid before closing existing log.
	// We must keep at least one log entry to anchor any snapshots.
	firstIndex := s.raftLog[0].Index
	lastIndex := s.raftLog[n-1].Index

	if keepIndex > lastIndex {
		panic(fmt.Sprintf("keepIndex must preserve at least one log entry. keepIndex = %v but can be at most lastIndex = %v)", keepIndex, lastIndex))
	}

	keep0 := keepIndex - firstIndex

	//vv("keep0=%v; keepIndex=%v; firstIndex=%v; lastIndex=%v", keep0, keepIndex, firstIndex, lastIndex)
	if keepIndex <= s.logIndex.BaseC || keepIndex <= firstIndex {
		//vv("%v no compaction desired/possible b/c Base after previous compaction.", s.name)
		return
	}
	// INVAR: keepIndex > firstIndex
	// INVAR: keep0 > 0

	// vv("%v wal.Compact() from keepIndex=%v; keep0=%v; we set s.logIndex.BaseC=%v; s.logIndex.CompactTerm=%v", s.name, keepIndex, keep0, s.logIndex.ompactIndex, s.logIndex.CompactTerm)

	s.logIndex.CompactNewBeg(keepIndex, syncme) // only non-test call.
	//vv("%v after CompactNewBeg(keepIndex=%v), s.logIndex = %v\n len(s.raftLog)=%v", s.name, keepIndex, s.logIndex, len(s.raftLog))
	// cannot assert here since we keep s original len for other tests
	//s.assertConsistentWalAndIndex(0)
	//vv("%v good: in sync after CompactNewBeg", s.name)

	// save these to restore below/be ready to return in nodisk.
	compactIndex := s.logIndex.BaseC
	compactTerm := s.logIndex.CompactTerm

	if s.nodisk {
		s.Compact_NODISK(keepIndex, keep0)
		return
	}

	oldPath := s.path
	oldParPath := s.parlog.path

	s.close()

	// keep the path from growing too long after
	// repeated compression.
	suff := rpc.NewCallID("") // temp file unique path name suffix.
	var newPath string
	pos := strings.LastIndex(s.path, ".compact_")
	if pos < 0 {
		newPath = s.path + ".compact_" + suff
	} else {
		newPath = s.path[:pos] + ".compact_" + suff
	}

	if newPath == oldPath {
		panic(fmt.Sprintf("logic error, newPath '%v' should never equal oldPath '%v', so we do not disturb old path in case we need to recover.", newPath, oldPath))
	}
	newParPath := newPath + ".parlog"

	if newParPath == oldParPath {
		panic(fmt.Sprintf("logic error, newParPath '%v' should never equal oldParPath '%v', so we do not disturb old path in case we need to recover.", newParPath, oldParPath))
	}

	fd2, err := os.OpenFile(newPath, os.O_RDWR|os.O_CREATE, 0644)
	panicOn(err)

	const noDiskFalse = false
	parlog2, err := newParLog(newParPath, noDiskFalse)
	panicOn(err)

	s2 := &raftWriteAheadLog{
		path:      newPath,
		fd:        fd2,
		w:         msgp.NewWriter(fd2),
		r:         msgp.NewReader(fd2),
		logIndex:  newTermsRLE(),
		checkEach: blake3.New(64, nil),
		parlog:    parlog2,
		name:      s.name,

		// note that s.logIndex.CompactNewBeg(keepIndex) was
		// called above, so the logIndex should have
		// these correctly set now:
		lli: s.logIndex.BaseC, // 206 wal_test green
		llt: s.logIndex.CompactTerm,
	}
	// each save below will build atop these (note this is s2 not s)
	s2.logIndex.BaseC = s2.lli // set on brand new s2.logIndex
	s2.logIndex.Endi = s2.lli
	s2.logIndex.CompactTerm = s2.llt

	for i := keep0; i < n; i++ {
		// the close() just below will fsync at end, so skip fsyncs here.
		s2.saveRaftLogEntryDoFsync(s.raftLog[i], false)
	}
	s2.close()

	// sanity check CompactNewBeg vs built up from scratch index.
	// Since we are going to blow away logIndex anyway from the reload
	// below, we don't bother to clone it.
	if !s.logIndex.Equal(s2.logIndex) {
		alwaysPrintf("cut s.logIndex=%v", s.logIndex)
		alwaysPrintf("s2.logIndex = %v", s2.logIndex)
		panic("cut != s2.logIndex, our CompactNewBeg and building from scratch did not result in the same index. fix the bug that is somewhere -- maybe above, maybe in CompactNewBeg() !")
	}

	//vv("keep0=%v; n=%v; rename '%v' -> '%v'", keep0, n, newParPath, oldParPath)
	//vv("rename '%v' -> '%v'", newPath, oldPath)
	if s.isTest {
		// preserve the originals so we can verify
		// the compressed is a suffix of the original.
		origParPath = oldParPath + ".orig"
		err = os.Rename(oldParPath, origParPath) // preserve orig with .orig
		panicOn(err)
		err = copyFile(oldParPath, newParPath) // update; leave new for inspection
		panicOn(err)

		origPath = oldPath + ".orig"
		err = os.Rename(oldPath, origPath) // preserve orig with .orig
		panicOn(err)
		err = copyFile(oldPath, newPath) // leave newPath available for inspection
		panicOn(err)
	} else {
		err = os.Rename(newParPath, oldParPath)
		panicOn(err)
		err = os.Rename(newPath, oldPath)
		panicOn(err)
	}
	// update all internals to be using the compacted log
	// by re-opening it.

	fd, err := os.OpenFile(oldPath, os.O_RDWR, 0644)
	panicOn(err)

	parlog, err := newParLog(oldParPath, noDiskFalse)
	panicOn(err)

	// s.path is fine
	s.fd = fd
	s.w = msgp.NewWriter(fd)
	s.r = msgp.NewReader(fd)

	s.raftLog = nil
	s.begseekpos = nil
	s.endpos = 0

	s.logIndex = newTermsRLE()
	// do we need to set s.logIndex.BaseC/Term or does
	// loadPathHelper take care of that? it does not. so:
	s.logIndex.BaseC = compactIndex
	s.logIndex.CompactTerm = compactTerm

	s.checkEach = blake3.New(64, nil)
	s.rpos = 0
	s.wpos = 0
	s.parlog = parlog
	// s.name is fine.
	// already set above
	// s.noLogCompaction is fine, though we would never get here.
	// s.isTest is fine
	// s.prevLastAppliedIndex is fine

	var sz int64
	fi, err := fd.Stat()
	if err == nil {
		sz = fi.Size()
	}
	err0 = s.loadPathHelper(fd, sz, false)
	//vv("after loadPathHelper, s.logIndex = %v", s.logIndex)

	return

}

// figure out where the index was going so many extra entries
// from. 707 with compaction.
func (s *raftWriteAheadLog) assertConsistentWalAndIndex(keepIndex int64) {
	idxBeg := s.logIndex.BaseC + 1
	idxSize := s.logIndex.Endi - s.logIndex.BaseC

	logSize := int64(len(s.raftLog))
	if logSize != idxSize {
		panic(fmt.Sprintf("bad: index desynced! logSize=%v but idxSize=%v; keepIndex=%v; s=%v", logSize, idxSize, keepIndex, s.StringWithCommit(keepIndex)))
	}
	idxCur := idxBeg
	for i, e := range s.raftLog {
		if e.Index != idxCur {
			panic(fmt.Sprintf("bad: index desynced 2! i=%v; e.Index=%v but idxCur=%v; keepIndex=%v; s=%v", i, e.Index, idxCur, keepIndex, s.StringWithCommit(keepIndex)))
		}
		idxTerm := s.logIndex.getTermForIndex(idxCur) // linear but meh, its a test assertion.
		if e.Term != idxTerm {
			panic(fmt.Sprintf("bad: index desynced 3! i=%v; e.Index=%v but idxCur=%v", i, e.Term, idxTerm))
		}
		idxCur++
	}
}

// mainline has installed a state snapshot to
// bring our log into a "caught-up" status.
func (s *raftWriteAheadLog) installedSnapshot(state *RaftState) {

	//s.lli = state.CommitIndex
	//s.llt = state.CommitIndexEntryTerm

	//vv("%v wal.installedSnapshot, writing s.lli from %v -> %v", s.name, s.lli, state.CompactionDiscardedLast.Index)

	s.lli = state.CompactionDiscardedLast.Index
	s.llt = state.CompactionDiscardedLast.Term

	s.raftLog = s.raftLog[:0]
	s.logIndex = newTermsRLE()
	s.logIndex.BaseC = s.lli
	s.logIndex.Endi = s.lli
	s.logIndex.CompactTerm = s.llt

	//vv("%v after wal.installedSnapshot, wal.lli=%v; wal.llt=%v; s.logIndex=%v", s.name, s.lli, s.llt, s.logIndex)

	// very bad hm... avoid!
	//s.logIndex.BaseC = state.CommitIndex

	// clear out the disk
	if s.fd != nil {
		_, err := s.fd.Seek(0, 0)
		panicOn(err)
		err = s.fd.Truncate(0)
		panicOn(err)
		s.sync()
	}
	// also clear out the par log
	s.parlog.truncate(0)
}

func (s *raftWriteAheadLog) getCompactBase() int64 {
	return s.logIndex.BaseC
}
