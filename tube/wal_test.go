package tube

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	rpc "github.com/glycerine/rpc25519"
)

func Test201_raftWriteAheadLogSaveLoad(t *testing.T) {
	path := "test201.plog"
	os.Remove(path)
	defer os.Remove(path)
	os.Remove(path + ".parlog")
	defer os.Remove(path + ".parlog")

	cfg := &TubeConfig{}

	plog, err := cfg.newRaftWriteAheadLog(path, false)
	panicOn(err)
	var prevTerm int64
	for i := range int64(3) {
		tkt := &Ticket{
			TicketID: rpc.NewCallID(""),
		}
		entry := &RaftLogEntry{
			Term:      1,
			Index:     i + 1,
			Ticket:    tkt,
			PrevIndex: i,
			PrevTerm:  prevTerm,
		}
		prevTerm = entry.Term

		_, err := plog.saveRaftLogEntry(entry)
		panicOn(err)
	}
	panicOn(plog.close())

	vv("done writing 3 entry raft wal.")

	plog2, err := cfg.newRaftWriteAheadLog(path, false)
	panicOn(err)
	defer plog2.close()

	ne := len(plog2.raftLog)
	if ne != 3 {
		t.Fatalf("expected 3 entries, got %v", ne)
	}
	for i := range int64(3) {
		ver := plog2.raftLog[i].Index
		if ver != i+1 {
			t.Fatalf("expected Version %v, got %v", i+1, ver)
		}
	}
}

func Test202_raftWriteAheadLogAppendsGoToTheEnd(t *testing.T) {
	path := "test202.plog"
	os.Remove(path)
	defer os.Remove(path)
	os.Remove(path + ".parlog")
	defer os.Remove(path + ".parlog")

	cfg := &TubeConfig{}

	plog, err := cfg.newRaftWriteAheadLog(path, false)
	panicOn(err)

	vv("opened new raft log")

	var prevTerm int64
	for i := range int64(3) {
		tkt := &Ticket{
			TicketID: rpc.NewCallID(""),
		}
		entry := &RaftLogEntry{
			Term:      1,
			Index:     i + 1,
			Ticket:    tkt,
			PrevIndex: i,
			PrevTerm:  prevTerm,
		}
		prevTerm = entry.Term

		_, err := plog.saveRaftLogEntry(entry)
		panicOn(err)
	}
	panicOn(plog.close())

	vv("202: closed plog, about to open again")

	plog2, err := cfg.newRaftWriteAheadLog(path, false)
	panicOn(err)
	//defer plog2.close()

	ne := len(plog2.raftLog)
	if ne != 3 {
		t.Fatalf("expected 3 entries, got %v", ne)
	}
	for i := range int64(3) {
		ver := plog2.raftLog[i].Index
		if ver != i+1 {
			t.Fatalf("expected Version %v, got %v", i+1, ver)
		}
	}

	// try to append entry 4
	tkt := &Ticket{
		TicketID: rpc.NewCallID(""),
	}
	entry := &RaftLogEntry{
		Term:      1,
		Index:     4,
		Ticket:    tkt,
		PrevIndex: 3,
		PrevTerm:  prevTerm,
	}
	prevTerm = entry.Term

	_, err = plog2.saveRaftLogEntry(entry)
	panicOn(err)

	// close will always fsync even if intermediate
	// entries are not fsynced.
	panicOn(plog2.close())

	// try to read all 4 back on re-open.

	plog4, err := cfg.newRaftWriteAheadLog(path, false)
	panicOn(err)
	defer plog4.close()

	ne = len(plog4.raftLog)
	if ne != 4 {
		t.Fatalf("expected 4 entries, got %v", ne)
	}
	for i := range int64(4) {
		idx := plog4.raftLog[i].Index
		if idx != i+1 {
			t.Fatalf("expected Index %v, got %v", i+1, idx)
		}
	}
}

func Test203_raftWriteAheadLogCorruptionNoticed(t *testing.T) {
	path := "test203.plog"
	os.Remove(path)
	defer os.Remove(path)
	os.Remove(path + ".parlog")
	defer os.Remove(path + ".parlog")

	cfg := &TubeConfig{}
	plog, err := cfg.newRaftWriteAheadLog(path, false)
	panicOn(err)

	var prevTerm int64
	for i := range int64(3) {
		tkt := &Ticket{
			TicketID: rpc.NewCallID(""),
		}
		entry := &RaftLogEntry{
			Term:      1,
			Index:     i + 1,
			Ticket:    tkt,
			PrevIndex: i,
			PrevTerm:  prevTerm,
		}
		prevTerm = entry.Term

		_, err := plog.saveRaftLogEntry(entry)
		panicOn(err)
	}
	panicOn(plog.close())

	// corrupt it
	fd2, err := os.OpenFile(path, os.O_RDWR, 0644)
	panicOn(err)
	_, err = fd2.Write([]byte{0, 0, 0, 0})
	panicOn(err)
	panicOn(fd2.Close())

	func() {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatalf("expected panic on corruption")
			} else {
				//vv("good, caught: '%v'", r)
				// example:
				// good, caught: 'msgp: attempted to decode type "int" with method for "bin"'
			}
		}()
		cfg.newRaftWriteAheadLog(path, false)
	}()
}

func Test204_raftWriteAheadLogChecksumWrongNoticed(t *testing.T) {
	path := "test204.plog"
	os.Remove(path)
	defer os.Remove(path)
	os.Remove(path + ".parlog")
	defer os.Remove(path + ".parlog")

	cfg := &TubeConfig{}
	plog, err := cfg.newRaftWriteAheadLog(path, false)
	panicOn(err)

	var prevTerm int64
	for i := range int64(3) {
		tkt := &Ticket{
			TicketID: rpc.NewCallID(""),
		}
		entry := &RaftLogEntry{
			Term:      1,
			Index:     i + 1,
			Ticket:    tkt,
			PrevIndex: i,
			PrevTerm:  prevTerm,
		}
		prevTerm = entry.Term
		_, err := plog.saveRaftLogEntry(entry)
		panicOn(err)
	}
	panicOn(plog.close())

	// corrupt it
	by, err := os.ReadFile(path)
	panicOn(err)
	hashPrefix := "blake3.33B-"
	pre := len(hashPrefix)
	pos := bytes.Index(by, []byte(hashPrefix))
	if pos == -1 {
		panic("file should have some DiskState + Blake3 in it")
	}
	copy(by[pos+pre:], []byte("shazam!"))

	fd2, err := os.Create(path)
	panicOn(err)
	_, err = fd2.Write(by)
	panicOn(err)
	panicOn(fd2.Close())

	func() {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatalf("expected panic on corruption")
			} else {
				//vv("good, caught: '%v'", r)
				// example:
				// good, caught: 'corrupt paxoslog 'test204.plog' at pos '0'. onDisk.Sum:'blake3.33B-shazam!Ft_lEWT-9DvJZxI9Jc3eJLuRHS7LsoEC00lHi
				// '; vs. re-computed-hash: 'blake3.33B-bpLA52kFt_lEWT-9DvJZxI9Jc3eJLuRHS7LsoEC00lHi
			}
		}()
		cfg.newRaftWriteAheadLog(path, false)
	}()
}

func Test205_raftWriteAheadLog_overwrite_of_entries_possible(t *testing.T) {
	path := "test205.plog"
	os.Remove(path)
	defer os.Remove(path)
	os.Remove(path + ".parlog")
	defer os.Remove(path + ".parlog")

	cfg := &TubeConfig{}
	plog, err := cfg.newRaftWriteAheadLog(path, false)
	panicOn(err)

	var prevTerm int64
	for i := range int64(3) {
		tkt := &Ticket{
			TicketID: rpc.NewCallID(""),
		}
		entry := &RaftLogEntry{
			Term:      1,
			Index:     i + 1,
			Ticket:    tkt,
			PrevIndex: i,
			PrevTerm:  prevTerm,
		}
		prevTerm = entry.Term

		_, err := plog.saveRaftLogEntry(entry)
		panicOn(err)
	}

	// overwrite the third, last entry (Index 3)
	tkt := &Ticket{
		TicketID: rpc.NewCallID(""),
	}
	over := &RaftLogEntry{
		Term:      2,
		Index:     3,
		Ticket:    tkt,
		PrevIndex: 2,
		PrevTerm:  1,
	}

	compactPrevIndex, compactPrevTerm, err := plog.overwriteEntries(2, []*RaftLogEntry{over}, false, -1, 0, nil)
	_, _ = compactPrevIndex, compactPrevTerm
	panicOn(err)

	// read back [term 1, idx 1], [term 1, idx 2], [term 2, idx 3]
	plog2, err := cfg.newRaftWriteAheadLog(path, false)
	panicOn(err)
	if plog2.raftLog[2].Term != 2 {
		panic(fmt.Sprintf("failed to overwrite in place index 3 with term 2"))
	}
}

func Test206_raftWriteAheadLog_Compact_0_keeps_all_logical_content(t *testing.T) {

	defer removeFiles("test206.plog*")

	maxLogSz := 5
	for j := range int64(maxLogSz) {
		if j != 4 {
			continue
		}
		path := "test206.plog"
		os.Remove(path)
		defer os.Remove(path)
		os.Remove(path + ".parlog")
		defer os.Remove(path + ".parlog")

		cfg := &TubeConfig{
			NoDisk: false, // so use disk.
		}
		const readOnlyFalse = false
		plog, err := cfg.newRaftWriteAheadLog(path, readOnlyFalse)
		panicOn(err)
		plog.isTest = true // preserve the old version for checking

		var prevTerm int64
		for i := range int64(maxLogSz) {
			tkt := &Ticket{
				TicketID: rpc.NewCallID(""),
			}
			entry := &RaftLogEntry{
				Term:      i + 1,
				Index:     i + 1,
				Ticket:    tkt,
				PrevIndex: i,
				PrevTerm:  prevTerm,
			}
			prevTerm = entry.Term

			_, err := plog.saveRaftLogEntry(entry)
			panicOn(err)
			if plog.LastLogIndex() != entry.Index {
				panic("wat?")
			}
		}

		path1 := plog.path
		vv("call Compact(j=%v)", j)
		path0, _, _, _, err1 := plog.Compact(j, nil)
		panicOn(err1)
		//vv("path0=origPath = '%v' after Compact(%v)", path0, j)

		if path0 != "" {
			if j == 0 {
				assertWalSameLogically(path0, path1)
			} else {
				assertPath1HasCommonSuffix(path0, path1)
			}
		}
	}
}

// used by 206 test above.
func assertWalSameLogically(path0, path1 string) {
	if path0 == path1 {
		//vv("same path is same.")
		return
	}
	cfg := &TubeConfig{}
	const readOnly = true
	s0, err := cfg.newRaftWriteAheadLog(path0, readOnly)
	panicOn(err)
	s1, err := cfg.newRaftWriteAheadLog(path1, readOnly)
	panicOn(err)
	n0 := len(s0.raftLog)
	n1 := len(s1.raftLog)
	if n0 != n1 {
		panic(fmt.Sprintf("wal '%v' and '%v' differ: n0=%v but n1=%v", path0, path1, n0, n1))
	}
	for i := n1 - 1; i >= 0; i-- {

		e0 := s0.raftLog[i]
		e1 := s1.raftLog[i]
		if e0.Index != e1.Index {
			panic(fmt.Sprintf("wal '%v' and '%v' differ: e0.Index=%v but e1.Index=%v at i = %v", path0, path1, e0.Index, e1.Index, i))
		}
		if e0.Term != e1.Term {
			panic(fmt.Sprintf("wal '%v' and '%v' differ: e0.Term=%v but e1.Term=%v at i = %v", path0, path1, e0.Term, e1.Term, i))
		}
		if e0.Ticket.Desc != e1.Ticket.Desc {
			panic(fmt.Sprintf("wal '%v' and '%v' differ: e0.Ticket.Desc=%v but e1.Ticket.Desc=%v at i = %v", path0, path1, e0.Ticket.Desc, e1.Ticket.Desc, i))
		}

	}
}

// used by 206 test above
// path1 should be after Compact() was applied to path0.
func assertPath1HasCommonSuffix(path0, path1 string) {
	if path0 == path1 {
		//vv("same path is same.")
		return
	}
	cfg := &TubeConfig{}
	const readOnly = true
	s0, err := cfg.newRaftWriteAheadLog(path0, readOnly)
	panicOn(err)
	s1, err := cfg.newRaftWriteAheadLog(path1, readOnly)
	panicOn(err)
	n0 := len(s0.raftLog)
	n1 := len(s1.raftLog)
	if n1 > n0 {
		panic(fmt.Sprintf("wal path0='%v'; but path1='%v' cannot have common suffix since it is longer: n1=%v > n0=%v", path0, path1, n1, n0))
	}
	j := n0 - 1
	for i := n1 - 1; i >= 0; i-- {

		e0 := s0.raftLog[j]
		e1 := s1.raftLog[i]
		if e0.Index != e1.Index {
			panic(fmt.Sprintf("wal '%v' and '%v' differ: e0.Index=%v but e1.Index=%v at i = %v; n1=%v; n0=%v", path0, path1, e0.Index, e1.Index, i, n1, n0))
		}
		if e0.Term != e1.Term {
			panic(fmt.Sprintf("wal '%v' and '%v' differ: e0.Term=%v but e1.Term=%v at i = %v", path0, path1, e0.Term, e1.Term, i))
		}
		if e0.Ticket.Desc != e1.Ticket.Desc {
			panic(fmt.Sprintf("wal '%v' and '%v' differ: e0.Ticket.Desc=%v but e1.Ticket.Desc=%v at i = %v", path0, path1, e0.Ticket.Desc, e1.Ticket.Desc, i))
		}
		j--
	}
}
