package tube

import (
	"fmt"
	"io"
	"os"
)

// memory only version of WAL. NOT SAFE. TESTING ONLY.
// This version of the write-ahead-log uses no
// disk. It is not safe for any real uses. It is
// activated in tests by setting TubeConfig.NoDisk = true.
func (cfg *TubeConfig) newRaftWriteAheadLogMemoryOnlyTestingOnly() (s *raftWriteAheadLog, err error) {
	//vv("newRaftWriteAheadLogMemoryOnlyTestingOnly()")

	// defer func() {
	// 	if s != nil {
	// 		vv("%v memwal made newRaftWriteAheadLogMemoryOnlyTestingOnly(); nodisk=%v; cfg.MyName='%v'; noLogCompaction=%v", s.name, s.nodisk, cfg.MyName, s.noLogCompaction)
	// 	}
	// }()

	parlog, err := newParLog("", true)
	if err != nil {
		return nil, fmt.Errorf("newParLog error: '%v'", err)
	}

	return &raftWriteAheadLog{
		name:            cfg.MyName,
		logIndex:        newTermsRLE(),
		nodisk:          true,
		parlog:          parlog,
		isTest:          cfg.isTest,
		testNum:         cfg.testNum,
		noLogCompaction: cfg.NoLogCompaction,
		lli:             0,
		llt:             0,
	}, nil
}

func (s *raftWriteAheadLog) close_NODISK() (err error) {
	return
}

func (s *raftWriteAheadLog) saveRaftLogEntry_NODISK(entry *RaftLogEntry) (nw int, err error) {

	//vv("%v saveRaftLogEntry_NODISK called; BEFORE: s.logIndex='%v'\nentry we are about to add=%v\nfull log:\n%v", s.name, s.logIndex, entry, s.StringWithCommit(s.prevLastAppliedIndex))

	var lastIndex int64
	n := int64(len(s.raftLog))
	if n > 0 {
		lastIndex = s.raftLog[n-1].Index
	} else {
		lastIndex = s.logIndex.BaseC
	}
	if lastIndex+1 != entry.Index {
		panic(fmt.Sprintf("incorrect entry.Index = %v vs "+
			"lastIndex+1 = %v", entry.Index, lastIndex+1))
	}

	s.logIndex.AddTerm(entry.Term)

	s.raftLog = append(s.raftLog, entry)
	s.lli++
	s.llt = entry.Term
	var posnow, nw0, nw2 int64
	b3string := ""
	s.parlog.append1(entry, posnow, nw0, nw2, b3string)
	return
}

func (s *raftWriteAheadLog) loadRaftLogEntry_NODISK(j int) (ds *RaftLogEntry, err0 error) {
	ds = s.raftLog[j]
	var pos int64
	h := ""
	s.validateParLog(pos, j, h, ds)
	//vv("loadRaftLogEntry called")
	return
}

// for raft_test
func (s *raftWriteAheadLog) clone() (c *raftWriteAheadLog) {
	if !s.nodisk {
		panic("can only clone nodisk raftWriteAheadLog for tests")
	}
	c = &raftWriteAheadLog{
		raftLog: append([]*RaftLogEntry{}, s.raftLog...),
	}
	if c.logIndex != nil {
		c.logIndex = s.logIndex.clone()
	}
	for i, e := range c.raftLog {
		c.raftLog[i] = e.clone()
	}
	return
}

// if isLeader, also assert that we never overwrite
// any entries in our log, since that is
// a Raft principle that is necessary for safety.
// leaders should never call this, in fact! We will
// panic to remind them.
func (s *raftWriteAheadLog) overwriteEntries_NODISK(keepIndex int64, es []*RaftLogEntry, isLeader bool, curCommitIndex, lastAppliedIndex, keepCount, overwriteCount int64, syncme *IndexTerm) (err error) {

	if isLeader {
		panic("leaders should use saveRaftLogEntry(), never overwriteEntries, since Raft requires leaders never overwrite their logs.")
	}

	//n := int64(len(s.raftLog))

	// INVAR: overwriteCount >= 0: how much of the log is re-written;
	// the log might be even longer afterwards too, of course.

	mayNeedTruncate := false
	//vv("overwriteCount = %v; keepCount = %v", overwriteCount, keepCount)
	if overwriteCount == 0 {
		// just appending
	} else {
		// overwriting the tail

		// assert we are not killing any committed txn,
		// also that we are not rewriting the same
		// thing atop of itself.
		killed := s.raftLog[keepCount:]
		if len(killed) > 0 && isLeader {
			panic(fmt.Sprintf("bad! leader can never overwrite any (here %v) log entries!", len(killed)))
		}
		nes := int64(len(es))
		if overwriteCount > nes {
			overwriteCount = nes
			// else, out of bounds [:2] on size 1 just below
		}
		replacements := es[:overwriteCount]
		for i, e := range killed {

			//if i < len(es) && replacements[i].Equal(killed[i]) {
			// update: our simae_test framework is not
			// sophisticated enought to not generate
			// follower logs that might randomly contain
			// the same term later even when the earlier
			// term is different from masters... maybe? this
			// could occur? we were assuming that an
			// earlier mis-match means that all subsequent
			// entries need to be overwritten; which does
			// seem the safer thing to do. Commenting out
			//panic(fmt.Sprintf("don't overwrite an entry with itself! i=%v, replacements[%v]='%v'; killed[%v]='%v'; especially because we want to avoid truncation of a good log by a smaller write.", i, i, replacements[i], i, killed[i]))
			//}
			if s.noLogCompaction {
				if e.Index <= curCommitIndex { // <= -1 will never happen, so wal_test okay.
					panic(fmt.Sprintf("curCommitIndex = %v; don't kill a committed log entry! %v\n\nwould have overwritten with: '%v'", curCommitIndex, e, replacements[i]))
				}
			}
		}

		if s.onOverwriteNotifyMe != nil {
			s.onOverwriteNotifyMe(killed, true)
		}

		// okay then
		s.raftLog = s.raftLog[:keepCount]
		mayNeedTruncate = true
		err = s.parlog.truncate(keepCount)
		panicOn(err)
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
		nw, err = s.saveRaftLogEntry(e)
		if err != nil {
			panicOn(err)
			return
		}
	}
	if mayNeedTruncate {
		// get rid of any not overwritten garbage.
	}
	if !s.noLogCompaction {
		// THIS IS THE LOG COMPACTION CALL.
		//
		// We currently compact after
		// every new commit, to catch bugs/issues early ASAP--
		// per the Raft Dissertation suggestion (page 57,
		// section 5.1.3 -- "Implementation concerns").
		//
		// leave off until TermsRLE is compaction ready.
		//s.maybeCompact(curCommitIndex, syncme, nil) // was for quite a while.
		s.maybeCompact(lastAppliedIndex, syncme, nil) // like wal.go
	}

	return
}

func (s *raftWriteAheadLog) DumpRaftWAL_NODISK(w io.Writer) error {

	if isNil(w) {
		w = os.Stdout
	}

	fmt.Fprintf(w, "contents of raft memwal of len %v:\n", len(s.raftLog))
	if len(s.raftLog) == 0 {
		fmt.Fprintf(w, "(empty memwal Raft log)\n")
		return nil
	}
	for i, e := range s.raftLog {
		// count from 1 to match the Raft log Index.
		fmt.Fprintf(w, "i=%03d  RaftLogEntry{Lead:%v, Term:%02d, Index:%02d, TicketID: %v, CurrentCommitIndex:%v, Ticket.Op='%v', tkt4=%v Ticket.Desc: %v}\n", i+1, e.LeaderName, e.Term, e.Index, e.Ticket.TicketID, e.CurrentCommitIndex, e.Ticket.Op, e.Ticket.TicketID[:4], e.Ticket.Desc)
	}

	fmt.Fprintf(w, "\nlogIndex:\n%v\n", s.logIndex)

	return nil
}

func (s *TubeNode) RaftWALString_NODISK() (r string) {
	//vv("%v top RaftWALString_NODISK; stack = \n%v", s.name, stack())
	r = fmt.Sprintf("contents of raft memwal of len %v:\n", len(s.wal.raftLog))
	if s.wal == nil || len(s.wal.raftLog) == 0 {
		r += fmt.Sprintf("(empty Raft log)\n")
		return
	}
	var ePrev *RaftLogEntry
	for i, e := range s.wal.raftLog {
		// count from 1 to match the Raft log Index.
		r += fmt.Sprintf("i=%03d  RaftLogEntry{Lead:%v, Term:%02d, Index:%02d, TicketID: %v, CurrentCommitIndex:%v, Ticket.Op='%v', tkt4=%v Ticket.Desc: %v}\n", i+1, e.LeaderName, e.Term, e.Index, e.Ticket.TicketID, e.CurrentCommitIndex, e.Ticket.Op, e.Ticket.TicketID[:4], e.Ticket.Desc)

		// assert the chain is linked properly
		if i == 0 {
			if s.wal.logIndex.BaseC != e.PrevIndex {
				panicf("bad chain link index: wal.logIndex.BaseC(%v) != e.PrevIndex(%v)", s.wal.logIndex.BaseC, e.PrevIndex)
			}
			if s.wal.logIndex.CompactTerm != e.PrevTerm {
				panicf("bad chain link term: wal.logIndex.CompactTerm(%v) != e.PrevTerm(%v)", s.wal.logIndex.CompactTerm, e.PrevTerm)
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
	r += " --- (end of memwal RaftWALString) ---\n"

	r += fmt.Sprintf("\ncurrent s.state.MC = %v\ns.state.CurrentTerm: %v   s.state.CommitIndex: %v   [logIndex.BaseC: %v; CompactTerm: %v]\n", s.state.MC.Short(), s.state.CurrentTerm, s.state.CommitIndex, s.wal.logIndex.BaseC, s.wal.logIndex.CompactTerm)

	return
}

func (s *raftWriteAheadLog) Compact_NODISK(keepIndex, keep0 int64) {

	//vv("%v Compact_NODISK keepIndex=%v; keep0=%v; len(s.raftLog) = %v", s.name, keepIndex, keep0, len(s.raftLog))
	//vv("%v before Compact_NODISK:\n%v", s.name, s.StringWithCommit(keepIndex))
	s.raftLog = s.raftLog[keep0:]

	// do this in caller now
	//s.logIndex.CompactNewBeg(keepIndex)

	//vv("%v after Compact_NODISK (len(s.raftLog)=%v):\n%v", s.name, len(s.raftLog), s.StringWithCommit(keepIndex))
	return
}
