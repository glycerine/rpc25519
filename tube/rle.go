package tube

import (
	"fmt"
	"iter"
	"sync/atomic"
)

var _ = atomic.AddInt64

// // moved from rle.go for greenpack inlining
// // TermsRLE conveys the structure of a log
// // in compressed form, using run-length encoding.
// type TermsRLE struct {
//	BaseC int64      `zid:"0"` // our zero. We hold (Endi-Base) count of entries.
//	Endi  int64      `zid:"1"` // 1-based (logical length)
// 	Runs  []*TermRLE `zid:"2"`
//
// track snapshot/compaction:
//  CompactIndex int64 `zid:"3"` // replaced with BaseC
//  CompactTerm  int64 `zid:"4"`
// }

// type TermRLE struct {
// 	Term  int64 `zid:"0"`
// 	Count int64 `zid:"1"`
// }

// tests have 0 tot but that's incorrect; fix it.
func (s *TermsRLE) fixTot() {
	s.Endi = s.BaseC
	for _, r := range s.Runs {
		s.Endi += r.Count
	}
}

/*
// planned but not used atm.
func (s *TermsRLE) setBase(base int64, syncme *IndexTerm) {

	s.BaseC = base
	if syncme != nil {
		syncme.Index = s.BaseC
		syncme.Term = s.CompactTerm
	}

	// should we be setting Endi too? not necessarily.
}
*/

func (s *TermsRLE) lastIndex() int64 {
	n := len(s.Runs)
	if n == 0 {
		if s.BaseC != s.Endi {
			// was firing on 206, maybe 201, now green with wal.go fixes.
			panicf("keep these in sync! at n==0, s.BaseC = %v but s.Endi = %v", s.BaseC, s.Endi)
		}
		return s.BaseC
	}
	return s.Endi
}

func (s *TermsRLE) lastTerm() int64 {
	n := len(s.Runs)
	if n == 0 {
		//vv("n==0, returning s.CompactTerm=%v", s.CompactTerm)
		return s.CompactTerm
	}
	//vv("n > 0, returning last Run .Term = %v", s.Runs[n-1].Term)
	return s.Runs[n-1].Term
}

func (s *TermsRLE) String() string {
	runs := ""
	for i, r := range s.Runs {
		_ = i
		//if i == 0 {
		runs += fmt.Sprintf("%#v\n", r) // r.String()
		//} else {
		//	runs += ", " + fmt.Sprintf("%#v", r) // r.String()
		//}
	}
	return fmt.Sprintf("[BaseC: %v[CompactTerm: %v]|logical len %v; (%v:%v]TermsRLE{ Base: %v, Endi: %v, Runs:\n%v}", s.BaseC, s.CompactTerm, s.Endi-s.BaseC, s.BaseC, s.Endi, s.BaseC, s.Endi, runs)
}
func (s *TermRLE) String() (r string) {
	if s == nil {
		return "nil ?!"
	}
	return fmt.Sprintf(`{Term: %v, Count: %v}`, s.Term, s.Count)
}

func newTermsRLE() (r *TermsRLE) {
	r = &TermsRLE{}
	return
}

func (s *TermsRLE) AddTerm(term int64) {
	if s.Endi == 0 {
		s.Endi = s.BaseC + 1
	} else {
		s.Endi++
	}
	// INVAR: s.Endi has been updated.
	// Maintain that terms never go backwards
	// into log-compacted away terms.
	if term < s.CompactTerm {
		panicf("bad AddTerm(term=%v) call: ugh cannot roll backwards into compacted term territory! term(%v) < s.CompactTerm=%v", term, term, s.CompactTerm)
	}

	n := len(s.Runs)
	if n == 0 {
		run := &TermRLE{
			Term:  term,
			Count: 1,
		}
		s.Runs = []*TermRLE{run}
		return
	}
	run := s.Runs[n-1]
	if run.Term == term {
		run.Count++
		return
	}
	// assert terms are monotone up (up or same, never down).
	if term < run.Term {
		panicf("cannot roll backwards terms! term=%v < last run.Term=%v", term, run.Term)
	}
	s.Runs = append(s.Runs, &TermRLE{Term: term, Count: 1})
}

// remove a suffix, keeping all <= keepIndex
func (s *TermsRLE) Truncate(keepIndex int64, syncme *IndexTerm) {
	if syncme != nil {
		syncme.Index = s.BaseC
		syncme.Term = s.CompactTerm
	}
	base := s.BaseC
	if keepIndex <= base {
		// 802 hits: panic("wat? keepIndex <= s.BaseC; tossing it all; try not to call here!")
		// clear out everything
		s.Runs = s.Runs[:0]
		s.BaseC = keepIndex
		s.CompactTerm = 0

		if syncme != nil {
			syncme.Index = s.BaseC
			syncme.Term = s.CompactTerm
		}
		s.Endi = keepIndex
		return
	}
	if keepIndex > s.Endi {
		return // noop
	}
	// INVAR: keepIndex <= s.Endi
	// INVAR: keepIndex > s.BaseC; will have 0 < keepCount < logical len
	//keepCount := keepIndex - base
	s.Endi = keepIndex

	for i, run := range s.Runs {
		thisRunEnd := base + run.Count
		switch {
		case keepIndex == thisRunEnd:
			// wipe out all after this run
			s.Runs = s.Runs[:i+1]
			s.CompactTerm = s.Runs[i].Term
			if syncme != nil {
				//syncme.Index = s.BaseC
				syncme.Term = s.CompactTerm
			}
			return
		case keepIndex < thisRunEnd:
			s.Runs = s.Runs[:i+1]                       // have to split the run
			s.Runs[i].Count -= (thisRunEnd - keepIndex) // split the run, keeping head.
			s.CompactTerm = s.Runs[i].Term
			if syncme != nil {
				//syncme.Index = s.BaseC
				syncme.Term = s.CompactTerm
			}
			return
		}
		base = thisRunEnd
	}
	panic("unreachable")
	return

}

// remove a prefix
func (s *TermsRLE) CompactNewBeg(newBeg int64, syncme *IndexTerm) {

	if newBeg < 0 {
		panic(fmt.Sprintf("bad! newBeg(%v) must be >= 0", newBeg))
	}
	if s.BaseC < 0 {
		panic(fmt.Sprintf("bad! s.BaseC(%v) must be >= 0", s.BaseC))
	}
	if s.Endi < 0 {
		panic(fmt.Sprintf("bad! s.Endi(%v) must be >= 0", s.Endi))
	}
	if newBeg == 0 {
		//vv("CompactNewBegin(newBeg=%v) wiping all: updating CompactIndex from %v -> 0", newBeg, s.BaseC, newBeg)
		s.BaseC = 0
		s.CompactTerm = 0
		if syncme != nil {
			syncme.Index = s.BaseC
			syncme.Term = s.CompactTerm
		}
		s.Endi = 0
		s.Runs = s.Runs[:0]
		return
	} else {
		// INVAR: newBeg > 0
		//vv("CompactNewBegin(newBeg=%v) updating CompactIndex from %v -> %v", newBeg, s.BaseC, newBeg-1)
		// wait to set this below so we don't destroy the info we need
		// to split the Runs.
		if newBeg == 1 {
			s.CompactTerm = 0
		} else {
			// INVAR: newBeg > 1
			s.CompactTerm = s.getTermForIndex(newBeg - 1)
		}
		if syncme != nil {
			//syncme.Index = s.BaseC
			syncme.Term = s.CompactTerm
		}

	}
	if newBeg <= s.BaseC {
		panic("nothing to cut away")
		return
	}
	// INVAR: newBeg > s.BaseC
	//if newBeg == s.BaseC+1 {
	//	return // no-op? no. must account for Runs too.
	//}

	// INVAR: newBeg > s.BaseC >= 0
	if s.Endi == 0 {
		panic("already empty, nothing to cut away.")
		return
	}
	if newBeg == s.Endi {
		//vv("CompactNewBeg(newBeg=%v): most common case: keep just the last", newBeg)
		s.BaseC = newBeg - 1 // leaving just the last term
		if syncme != nil {
			syncme.Index = s.BaseC
			syncme.Term = s.CompactTerm
		}
		n := len(s.Runs)
		if n == 0 {
			panic("internal logic error: Endi >= 1 so must have some Runs")
		}
		if n > 1 {
			s.Runs = s.Runs[n-1:] // keep just the last
		}
		s.Runs[0].Count = 1
		return
	}
	if newBeg > s.Endi {
		panic(fmt.Sprintf("newBeg would cut away more than we have: newBeg(%v) > s.Endi(%v)", newBeg, s.Endi))
	}
	// INVAR: 0 <= s.BaseC < newBeg < s.Endi-1;
	// and we might need to split Runs.
	n := int64(len(s.Runs))
	if n == 0 {
		panic("internal logic error: Endi >= 1 so must have some Runs")
	}
	retained := s.Endi - newBeg + 1
	if n == 1 {
		//vv("CompactNewBeg(newBeg=%v): easy case, no splitting required, just adjust Count. n=1; retained=%v", newBeg, retained)
		s.Runs[0].Count = retained
		s.BaseC = newBeg - 1
		if syncme != nil {
			syncme.Index = s.BaseC
			syncme.Term = s.CompactTerm
		}
		return
	}
	// have to find the split point and adjust the count on the
	// new first (kept) run.
	base := s.BaseC
	for i, run := range s.Runs {
		thisRunEnd := base + run.Count
		//vv("newBeg=%v; consider run: (base, thisRunEnd] = (%v, %v]", newBeg, base, thisRunEnd)
		switch {
		case newBeg == base+1:
			//vv("CompactNewBeg(newBeg=%v): no split needed, just discard prior to i=%v; will set s.BaseC=newBeg-1=%v", newBeg, i, newBeg-1)
			s.Runs = s.Runs[i:]
			s.BaseC = newBeg - 1
			if syncme != nil {
				syncme.Index = s.BaseC
				syncme.Term = s.CompactTerm
			}
			return
		case base < newBeg && newBeg <= thisRunEnd:
			//vv("CompactNewBeg(newBeg=%v): i=%v is our split point (keep >= i). newBeg(%v) <= thisRunEnd(%v)", newBeg, i, newBeg, thisRunEnd)
			retain := thisRunEnd - newBeg + 1
			s.Runs = s.Runs[i:]
			s.Runs[0].Count = retain
			s.BaseC = newBeg - 1
			if syncme != nil {
				syncme.Index = s.BaseC
				syncme.Term = s.CompactTerm
			}
			return
		}
		base = thisRunEnd
	}
	panic(fmt.Sprintf("unreachable. or at least should be! s='%v'", s))
}

func (s *TermsRLE) clone() *TermsRLE {
	if s == nil {
		return nil
	}
	r := &TermsRLE{
		BaseC:       s.BaseC,
		Endi:        s.Endi,
		CompactTerm: s.CompactTerm,
	}
	for _, run := range s.Runs {
		cp := *run
		r.Runs = append(r.Runs, &cp)
	}
	return r
}

func (m *TermsRLE) Equal(f *TermsRLE) bool {
	if m == nil && f == nil {
		return true // both nil
	}
	if m == nil || f == nil {
		return false // one nil, one not.
	}
	if m.Endi != f.Endi {
		return false
	}
	if m.BaseC != f.BaseC {
		return false
	}
	mlen := len(m.Runs)
	flen := len(f.Runs)
	if mlen == 0 && flen == 0 {
		return true
	}
	if mlen != flen {
		return false
	}
	if mlen == 0 || flen == 0 {
		return false
	}

	// INVAR: mlen > 0 && flen > 0

	// It is insufficient to check that only last Term matches;
	// *all* the terms must match. See counter example
	// in these tests:
	// === RUN   TestExtendsPredicateOnLogs/divergence_in_first_run_-_different_term
	//     rle_test.go:271: lead.Equal(foll) = true, want false
	//     rle_test.go:277: foll.Equal(lead) = true, want false
	// === RUN   TestExtendsPredicateOnLogs/divergence_in_first_run_-_different_count
	//     rle_test.go:271: lead.Equal(foll) = true, want false
	//     rle_test.go:277: foll.Equal(lead) = true, want false
	// === RUN   TestExtendsPredicateOnLogs/complex_divergence_pattern
	//     rle_test.go:271: lead.Equal(foll) = true, want false
	//     rle_test.go:277: foll.Equal(lead) = true, want false

	for i := range m.Runs {
		mr := m.Runs[i]
		fr := f.Runs[i]
		if mr.Term != fr.Term {
			return false
		}
		if mr.Count != fr.Count {
			return false
		}
	}
	return true
}

const NEED_LEADER_SNAPSHOT = true

// Extends: determine how to apply leader's log.
//
// (leaderLog).Extends(followerLog) returns extends = true if the
// leaderLog can be applied to the follower
// log without needing any deletions or overwrites
// on the follower's log. When extends is true, the follower's
// log is a perfect prefix of the leaders's log.
// ** You also must check for a gap, but assuming no gap: **
// In this happy path, the returned largestCommonRaftIndex will be the
// full length of the follower log, and
//
// follower = append(follower, leaderLog[largestCommonRaftIndex:]...)
//
// can be done to make the logs identical in a single step;
// when extends=true and there is no gap.
// A gap means the followers log is a prefix but...
//
// The second returned value, largestCommonRaftIndex
// (also called the length of the longest common prefix)
// gives the essential information needed to
// bring the two logs into matching when
// extends is false. The largestCommonRaftIndex
// should be used in the leader's next AppendEntries
// to the follower as the PrevLogIndex to
// enable log matching with a single update.
// This is the largest of index of an entry
// that the two logs share -- they diverge
// afterwards -- but note it
// can be zero (an invalid Raft index) if they disagree
// at the first index, index 1.
//
// For an empty follower log, leaderLog.Extends(followerLog) will
// always return (true, 0).
//
// Examples:
// a) returns (true, 0) if the follower's log is empty.
//
// b) returns (false, 0) if both logs are length one
// and differ in that first and only entry.
//
// c) If the follower's log has terms [3 5 7],
// and the leader's log has terms     [3 4 5 6 7],
// then Raft indexes the log entries   1 2 3 4 5
// so the largestCommonRaftIndex is    1. (extends=false)
//
// d) If the follower's log has terms [1 2 3],
// and the leader's log has terms     [1 2 3 4 5],
// then Raft indexes the log entries   1 2 3 4 5
// so the largestCommonRaftIndex would be  3.  (extends=true)
//
// e) But notice we do not prevent a gap!
// That must be checked separately.
//
// Gap in log means: (appendBegin > followerLen)
//
// Note this is the updated version for
// compaction. uses iterators. See below for original.
func (leaderLog *TermsRLE) Extends(followerLog *TermsRLE) (extends bool, largestCommonRaftIndex int64, needLeaderSnapshot bool) {

	//vv("top Extends(): followerLog = '%v', leaderLog='%v'", followerLog, leaderLog)
	//defer func() {
	//	vv("returning from Extends: extends=%v, largestCommonRaftIndex=%v, needLeaderSnapshot=%v", extends, largestCommonRaftIndex, needLeaderSnapshot)
	//}()

	// Handle nil cases
	if leaderLog == nil && followerLog == nil {
		// both empty
		return true, 0, false
	}
	if followerLog == nil {
		// follower empty, does not matter what leader has/not.
		return true, 0, false
	}
	// INVAR: followerLog != nil
	if followerLog.BaseC == 0 && len(followerLog.Runs) == 0 {
		// follower empty
		return true, 0, false
	}
	// INVAR: followerLog.BaseC > 0 || len(followerLog.Runs) > 0
	// i.e. follower has stuff.

	if leaderLog == nil {
		// follower has stuff, leader has nothing.
		return false, 0, false
	}
	// INVAR: leaderLog != nil
	if 0 == len(leaderLog.Runs) {
		if leaderLog.BaseC == 0 {
			// follower has stuff, leader has nothing.
			return false, 0, false
		}
		// INVAR: leaderLog.BaseC > 0 but no runs.

		if followerLog.BaseC == leaderLog.BaseC {
			if len(followerLog.Runs) == 0 {
				// equal
				return true, leaderLog.BaseC, false
			}
			// INVAR: len(followerLog.Runs) > 0 so diverging
			return false, leaderLog.BaseC, false
		}
		// INVAR: followerLog.BaseC != leaderLog.BaseC.
		// INVAR: leaderLog.BaseC > 0 but no runs.
		if followerLog.BaseC > leaderLog.BaseC {
			// can return the smaller
			return false, leaderLog.BaseC, false
		}
		// INVAR: followerLog.BaseC < leaderLog.BaseC
		// and leader has no runs, just BaseC > 0
		// They could agree for more than followerLog.BaseC,
		// but we have no data on anything before
		// leaderLog.BaseC. This is a case where
		// AE should be applying leaderLog from a snapshot
		// since we cannot really compare what we
		// cannot see from before leaderLog.BaseC.
		//vv("hitting the NEED_LEADER_SNAPSHOT case ") // not seen! 065
		return false, followerLog.BaseC, NEED_LEADER_SNAPSHOT
	}

	// INVAR: followerLog.BaseC > 0 || len(followerLog.Runs) > 0
	if leaderLog.BaseC == 0 && len(leaderLog.Runs) == 0 {
		// follower has stuff, leader empty.
		return false, 0, false
	}
	// INVAR: leaderLog.BaseC > 0 || len(leaderLog.Runs) > 0

	// both leader and follower have stuff; but might be compacted on either.

	// Track current position in both logs
	// m = master, f = follower
	m := leaderLog.BaseC
	f := followerLog.BaseC

	if m == 0 && f == 0 {
		// BaseC zero on both sides.
		extends, largestCommonRaftIndex = leaderLog.extends_aware_but_no_compaction_here(followerLog)
		return
	}

	// how are CompactIndex and Base different?
	// CompactIndex can be 0, while Base can be > 0; if no compaction.
	// Base will always be >= CompactIndex.
	// all actual entry.Index will be > CompactIndex and > Base.
	// Can we simplify and get rid of one? they get out of sync.

	// allow compaction. check the first "induction step".
	// any overlap?
	if followerLog.Endi <= m {
		// no overlap: extends true, but gap maybe yes.
		//vv("follower very behind leader. assume the matched up to follow.Endi;\n follower='%v'\n leader='%v'", followerLog, leaderLog)
		// can we assume that the follower does not need
		// to be overwritten? what if follower is just joining and
		// has nothing?
		return true, followerLog.Endi, false
	}

	// INVAR: leaderLog.BaseC <= followerLog.Endi
	if len(leaderLog.Runs) == 0 && len(followerLog.Runs) == 0 {
		// both have only up to CompactIndex
		if m > f {
			return true, f, false
			// I think below was throwing off our appendEntries.
			// So revert to the above.
			//
			// but we don't need anything ever before
			// a compacted index so give back m instead;
			// we know we have agreement on all compacted.
			//return true, m
		}
		if f > m {
			return false, m, false
		}
		// INVAR f == m
		return true, m, false
	}
	if len(followerLog.Runs) == 0 {
		// follower has up to CompactedIndex,
		// leader has Runs beyond its CompactedIndex.
		if m >= f {
			return true, f, false
			// again, revert to above.
			// again we can do better than f for sure,
			// since m is compacted.
			//return true, m
		}
		// INVAR: f > m, but we assume agreement for all
		// compacted entries, so we only need to look
		// at f+1 and following. Here though, the follower
		// has no runs beyond f; so they must all agree,
		// and f is the largest agreement we are sure of.
		return true, f, false
	}
	// INVAR: len(followerLog.Runs) > 0

	if len(leaderLog.Runs) == 0 {
		// leader has only up to CompactedIndex,
		// follower has Runs beyond its CompactedIndex.
		if f <= m {
			// we assume agreement up to any compacted index,
			// so we can return m here instead of f.
			// no point in looking beyond m since leader has no runs.
			//return true, m
			return true, f, false
		}
		// INVAR: f > m, and m has no runs.
		return false, m, false
	}
	// INVAR: len(leaderLog.Runs) > 0 && len(followerLog.Runs) > 0.

	if leaderLog.Endi <= f {
		//vv("no overlap of the runs data we have. follower is way far ahead.")
		return false, leaderLog.Endi, false
	}
	// we have an overlap of at least one entry...
	// INVAR: f.Base <            m.Endi
	// by construction:  m.Base < m.Endi
	//        f.Base <                     f.Endi
	// INVAR:            m.Base <          f.Endi

	// cases          A        B        C       D
	//          f   [    ]   [   ]    [   ]   [  ]
	//          m [  ]      [     ]    [ ]      [  ]
	// m extends f:  false    true     false   true
	// i.e.
	// m extends f iff m.Endi >= f.Endi and terms agree
	// in the whole overlap region. notice the gap
	// situation results in false above now.
	// f   [   ]
	// m         gap [    ]
	beginOverlap := max(f, m) + 1 // physical runs overlap, not compacted
	endiOverlap := min(followerLog.Endi, leaderLog.Endi)

	//vv("f=%v; m=%v; beginOverlap=%v", f, m, beginOverlap)

	/*
		// is the overlap entirely in the compacted regions?
		beginOverlap1 := max(f1, m1) + 1 // acount for Base, not CompactIndex
		// Base may be > CompactIndex.
		if beginOverlap1 > beginOverlap {

			// but what do we assume about the region between
			// CompactIndex and > Base? suppose we have
			//
			// 0   1               2       3   4...
			//     CompactIndex    Base
			//                             ^ Run[0] starts at 3
			//
			// The danger zone would be (1,2] here = (CompactIndex, Base].
			//
			// We only want to assume agreement up through CompactIndex,
			// so if there is a "danger zone" missing region from
			// CompactIndex to Base that overlaps in follower and leader,
			// then we have to reject as not extending.
			// But if both have the same Base? doesn't matter if
			// the danger zones overlap we could be in trouble, and
			// we want to return the (larger of) CompactIndex as lcp,
			// as that is all we can be sure is shared.
			mDanger := spanBegxEndi(m, m0) // (m, m0]
			fDanger := spanBegxEndi(f, f0) // (f, f0]
			if mDanger.overlaps(fDanger) {
				return false, max(m, f)
			}
			// INVAR: no overlap of danger zones. One
			// side has a CompactIndex (m or f) that
			// is strictly greater than the other side's Base.
			// So we should be fine to compare from
			// that CompactIndex + 1 point with confidence.

			// our iterator will stumble in the danger zone, so
			// correct the starting point now.
			beginOverlap = beginOverlap1
		}
	*/

	if false { // debug prints
		vv("INVAR: at least one overlap in index, but maybe not in term")
		// INVAR: have at least 1 overlap at an index > 0.
		// This does not mean we can return true though, since
		// if we diverge we do not want to simply append but
		// rather an overwrite is needed.

		vv("debug, here is leader log, BaseC=%v:", leaderLog.BaseC)
		k := 0
		mbase := max(leaderLog.BaseC, leaderLog.BaseC)
		if leaderLog.Endi > leaderLog.BaseC {

			for termL, idxL := range leaderLog.getTermIndexStartingAt(mbase + 1) {
				fmt.Printf("[%02d]  idxL: %v,  termL: %v\n", k, idxL, termL)
				k++
			}
		}
		if k == 0 {
			fmt.Printf("(empty leader log)\n")
		}

		vv("debug, here is follower log:, BaseC=%v:", followerLog.BaseC)
		k = 0
		fbase := max(followerLog.BaseC, followerLog.BaseC)
		if followerLog.Endi > fbase {
			for termF, idxF := range followerLog.getTermIndexStartingAt(fbase + 1) {
				fmt.Printf("[%02d]  idxF: %v,  termF: %v\n", k, idxF, termF)
				k++
			}
		}
		if k == 0 {
			fmt.Printf("(empty follower log)\n")
		}
		vv("(index not necessarily term): beginOverlap=%v; endiOverlap=%v", beginOverlap, endiOverlap)
	}

	// Track current run in both logs
	nextL, stopL := iter.Pull2(leaderLog.getTermIndexStartingAt(beginOverlap))
	defer stopL()
	termL, idxL, ok := nextL()
	if !ok {
		panic("wat? inconceivable!")
	}
	for termF, idxF := range followerLog.getTermIndexStartingAt(beginOverlap) {
		if termF != termL {
			//vv("termF(%v) != termL(%v)", termF, termL)
			return false, idxF - 1, false // divergence
		}
		var idxL1 int64
		termL, idxL1, ok = nextL()
		if !ok {
			//vv("leader has no more, at idxL=%v", idxL)
			// leader has no more, divergence or finished up equal.
			return leaderLog.Endi >= followerLog.Endi, idxL, false
		}
		idxL = idxL1
	}
	//vv("agreement in all area of overlap. returning %v (true if leaderLog.Endi(%v) >= endiOverlap(%v)", leaderLog.Endi >= endiOverlap, leaderLog.Endi, endiOverlap)
	return leaderLog.Endi >= followerLog.Endi, endiOverlap, false
}

// Assume CompactIndex==0 for both, just reference Base.
func (leaderLog *TermsRLE) extends_aware_but_no_compaction_here(followerLog *TermsRLE) (extends bool, largestCommonRaftIndex int64) {

	if leaderLog.BaseC != 0 || followerLog.BaseC != 0 {
		panic("use regular Extends, not extends_aware_but_no_compaction_here()")
	}
	//vv("top Extends(): followerLog = '%v', leaderLog='%v'", followerLog, leaderLog)
	//defer func() {
	//	vv("returning from Extends: extends=%v, largestCommonRaftIndex=%v", extends, largestCommonRaftIndex)
	//}()

	// Handle nil cases
	if leaderLog == nil || 0 == len(leaderLog.Runs) {
		if followerLog == nil || 0 == len(followerLog.Runs) {
			// both empty
			return true, 0
		}
		// follower has stuff, leader has nothing.
		return false, 0
	}
	// INVAR: leader log not empty
	if followerLog == nil || 0 == len(followerLog.Runs) {
		return true, 0
	}

	// Track current position in both logs
	// m = master, f = follower
	m := leaderLog.BaseC
	f := followerLog.BaseC

	// allow compaction. check the first "induction step".
	// any overlap?
	if followerLog.Endi <= leaderLog.BaseC {
		// no overlap: extends true, but gap yes.
		//vv("follower very behind leader. assume the matched up to follow.Endi;\n follower='%v'\n leader='%v'", followerLog, leaderLog)
		// can we assume that the follower does not need
		// to be overwritten? what if follower is just joining and
		// has nothing?
		return true, followerLog.Endi
		//return false, 0 // followerLog.Endi
	}
	// INVAR: leaderLog.BaseC <= followerLog.Endi
	if leaderLog.Endi <= followerLog.BaseC {
		//vv("no overlap of the data we have. follower is way far ahead.")
		return false, leaderLog.Endi
		//return false, 0 // leaderLog.Endi
	}
	// we have an overlap of at least one entry?
	// INVAR: f.Base <            m.Endi
	// by construction:  m.Base < m.Endi
	//        f.Base <                     f.Endi
	// INVAR:            m.Base <          f.Endi

	// cases          A        B        C       D
	//          f   [    ]   [   ]    [   ]   [  ]
	//          m [  ]      [     ]    [ ]      [  ]
	// m extends f:  false    true     false   true
	// i.e.
	// m extends f iff m.Endi >= f.Endi and terms agree
	// in the whole overlap region. notice the gap
	// situation results in false above now.
	// f   [   ]
	// m         gap [    ]
	beginOverlap := max(f, m) + 1
	endiOverlap := min(followerLog.Endi, leaderLog.Endi)

	if false { // debug prints
		vv("INVAR: at least one overlap")
		// INVAR: have at least 1 overlap at an index > 0.
		// This does not mean we can return true though, since
		// if we diverge we do not want to simply append but
		// rather an overwrite is needed.

		vv("debug, here is leader log:")
		k := 0
		for termL, idxL := range leaderLog.getTermIndexStartingAt(leaderLog.BaseC + 1) {
			fmt.Printf("[%02d]  idxL: %v,  termL: %v\n", k, idxL, termL)
			k++
		}

		vv("debug, here is follower log:")
		k = 0
		for termF, idxF := range followerLog.getTermIndexStartingAt(followerLog.BaseC + 1) {
			fmt.Printf("[%02d]  idxF: %v,  termF: %v\n", k, idxF, termF)
			k++
		}
	}

	// Track current run in both logs
	nextL, stopL := iter.Pull2(leaderLog.getTermIndexStartingAt(beginOverlap))
	defer stopL()
	termL, idxL, ok := nextL()
	if !ok {
		panic("wat?")
	}
	for termF, idxF := range followerLog.getTermIndexStartingAt(beginOverlap) {
		if termF != termL {
			//vv("termF(%v) != termL(%v)", termF, termL)
			return false, idxF - 1 // divergence
		}
		var idxL1 int64
		termL, idxL1, ok = nextL()
		if !ok {
			//vv("leader has no more, at idxL=%v", idxL)
			// leader has no more, divergence or finished up equal.
			return leaderLog.Endi >= followerLog.Endi, idxL
		}
		idxL = idxL1
	}
	//vv("agreement in all area of overlap. returning %v (true if leaderLog.Endi(%v) >= endiOverlap(%v)", leaderLog.Endi >= endiOverlap, leaderLog.Endi, endiOverlap)
	return leaderLog.Endi >= followerLog.Endi, endiOverlap
}

// original version of Extends
func (leaderLog *TermsRLE) extends_pre_compacting(followerLog *TermsRLE) (extends bool, largestCommonRaftIndex int64) {

	//vv("top Extends(): followerLog = '%v', leaderLog='%v'", followerLog, leaderLog)
	//defer func() {
	//	vv("returning from Extends: extends=%v, largestCommonRaftIndex=%v", extends, largestCommonRaftIndex)
	//}()

	// Handle nil cases
	if leaderLog == nil || 0 == len(leaderLog.Runs) {
		if followerLog == nil || 0 == len(followerLog.Runs) {
			// both empty
			return true, 0
		}
		// follower has stuff, leader has nothing.
		return false, 0
	}
	// INVAR: leader log not empty
	if followerLog == nil || 0 == len(followerLog.Runs) {
		return true, 0
	}

	// Track current position in both logs
	// m = master, f = follower
	//var m, f int64
	m := leaderLog.BaseC
	f := followerLog.BaseC

	// allow compaction. check the first "induction step".
	// any overlap?
	if followerLog.Endi <= leaderLog.BaseC {
		// no overlap
		//vv("follower very behind leader. assume the matched up to follow.Endi;\n follower='%v'\n leader='%v'", followerLog, leaderLog)
		return false, 0 // followerLog.Endi
	}
	// INVAR: leaderLog.BaseC <= followerLog.Endi
	if leaderLog.Endi <= followerLog.BaseC {
		//vv("no overlap of the data we have. follower is way far ahead.")
		return false, 0 // leaderLog.Endi
	}
	// we have an overlap of at least one entry?
	// INVAR: f.Base <            m.Endi
	// by construction:  m.Base < m.Endi
	//        f.Base <                     f.Endi
	// INVAR:            m.Base <          f.Endi

	f1 := followerLog.BaseC + 1 // f1 is the first follower index = f + 1
	if f1 > m && f1 <= leaderLog.Endi {
		// f1 overlaps leader's index range. Do we have agreement there?
		f1term := followerLog.Runs[0].Term
		m1term := leaderLog.getTermForIndex(f1)
		if f1term != m1term {
			//vv("f1term(%v) != m1term(%v)", f1term, m1term)
			return false, 0
		}
		// okay: we have agreement up to f1. We
		// assume agreement in the compacted f log behind us.
	} else {
		// gap/no overlap
		return false, 0
	}
	m1 := leaderLog.BaseC + 1 // m1 is first leader index = m + 1
	if m1 > f && m1 <= followerLog.Endi {
		// m1 overlaps follower's index range. Do we have agreement there?
		m1term := leaderLog.Runs[0].Term
		f1term := followerLog.getTermForIndex(m1)
		if f1term != m1term {
			//vv("f1term(%v) != m1term(%v)", f1term, m1term)
			return false, 0
		}
		// okay, we have agreement up to m1. We assume
		// agreement in the compacted m log behind.
	} else {
		// gap/no overlap
		return false, 0
	}
	// INVAR: have at least 1 overlap at an index > 0.
	// This does not mean we can return true though, since
	// if we diverge we do not want to simply append but
	// rather an overwrite is needed.
	// minAgreeIndex := max(m1, f1)

	// Track current run in both logs
	runL := 0
	runF := 0

	for runL < len(leaderLog.Runs) && runF < len(followerLog.Runs) {
		// Get current runs
		r1L := leaderLog.Runs[runL]
		r2F := followerLog.Runs[runF]

		if r1L.Term != r2F.Term {
			if f <= m {
				return false, f
			}
			// m < f
			return false, m
		}

		// If counts don't match, we found a fork.
		if r1L.Count < r2F.Count {
			return false, m + r1L.Count

		} else if r1L.Count > r2F.Count {
			// followers log stops in perfect alignment
			// BUT only if this is the last of the
			// follower's runs. Otherwise the
			// follower continues lower higher term while
			// the leader continues lower term.
			if runF+1 == len(followerLog.Runs) {
				return true, f + r2F.Count
			}
			return false, f + r2F.Count
		}

		// Move forward in both logs
		m += r1L.Count
		f += r2F.Count
		runL++
		runF++
	}

	// If we reached the end of one log but not the other,
	// the logs diverge at the end of the shorter one
	if runL < len(leaderLog.Runs) {
		// master still has entries left to apply.
		return true, f
	}
	if runF < len(followerLog.Runs) {
		// master is shorter than follower (follower
		// has entries from old, crashed leader)
		return false, m
	}

	// Logs are identical
	return true, m
}

func (s *TermsRLE) getTermIndexStartingAt(idx int64) iter.Seq2[int64, int64] {

	base := s.BaseC
	if idx < base || idx > s.Endi {
		panic(fmt.Sprintf("don't have it/don't know; idx=%v; s=%v", idx, s))
	}
	return func(yield func(int64, int64) bool) {
		nextIdx := idx
		foundFirst := false
		if idx == base {
			idx++ // advance for search below
			if !yield(s.CompactTerm, base) {
				return
			}
		}
		for _, run := range s.Runs {
			if !foundFirst {
				thisRunEnd := base + run.Count
				if idx > thisRunEnd {
					// jump past all of this run
					base = thisRunEnd
					continue
				}
				foundFirst = true
				leftInRun := thisRunEnd - idx + 1
				for range leftInRun {
					if !yield(run.Term, nextIdx) {
						return
					}
					nextIdx++
				}
			} else {
				for range run.Count {
					if !yield(run.Term, nextIdx) {
						return
					}
					nextIdx++
				}
			}
		}
	}
}

// lcp used in simae_test.go
func (leaderLog *TermsRLE) lcp(followerLog *TermsRLE) (common int64) {
	_, common, _ = leaderLog.Extends(followerLog)
	return
}

func (s *TermsRLE) getTermForIndex(idx int64) (term int64) {
	if idx > 0 {
		if idx == s.BaseC {
			return s.CompactTerm
		}
	}
	base := s.BaseC
	if idx < base || idx > s.Endi {
		return -1 // don't have it/don't know
	}
	for _, run := range s.Runs {
		thisRunEnd := base + run.Count
		switch {
		case idx <= thisRunEnd:
			return run.Term
		}
		base = thisRunEnd
	}
	panic(fmt.Sprintf("unreachable. idx=%v; s=%v", idx, s))
	return
}
