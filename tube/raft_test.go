package tube

import (
	"fmt"
	//"iter"
	//mathrand "math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/glycerine/idem"
	rpc "github.com/glycerine/rpc25519"
)

// keep the interface and implementations in sync.
// var _ hoster = &TubeNode{} // in host.go
var _ hoster = &TubeSim{}

func (a *RaftLogEntry) shallowCloneTestOnly() *RaftLogEntry {
	cp := *a
	// keep cp.committed same
	cp.testWrote = false // reset
	return &cp
}

func (s *TubeSim) newAE() (ae *AppendEntries) {
	ae = &AppendEntries{
		ClusterID:         s.ClusterID,
		FromPeerID:        s.PeerID,
		LeaderTerm:        s.state.CurrentTerm,
		LeaderID:          s.PeerID,
		LeaderCommitIndex: s.state.CommitIndex,
		LogTermsRLE:       s.wal.getTermsRLE(),
		AEID:              rpc.NewCallID(""),
	}
	return
}

func (s *TubeSim) newAEtoPeer(toPeerID, toPeerName, toPeerServiceName, toPeerServiceNameVersion string, sendThese []*RaftLogEntry) (ae *AppendEntries, foll *RaftNodeInfo) {
	ae = s.newAE()
	var ok bool
	foll, ok = s.peers[toPeerID]
	if !ok {
		foll = s.newRaftNodeInfo(toPeerID, toPeerName, toPeerServiceName, "")
		s.peers[toPeerID] = foll
	}
	foll.UnackedPing.Sent = time.Now()
	foll.UnackedPing.Term = ae.LeaderTerm
	foll.UnackedPing.AEID = ae.AEID

	return ae, foll
}

func (s *TubeSim) waitingSummary() (sum string, tot int) {
	n := len(s.Waiting)
	if n == 0 {
		return "(empty Waiting)", 0
	}
	tot = n
	// sort into log order
	var orderedTickets []*Ticket
	for _, tkt := range s.Waiting {
		orderedTickets = append(orderedTickets, tkt)
	}
	sort.Sort(logOrder(orderedTickets))
	sum = fmt.Sprintf("(%v tkt waiting: ", n)
	extra := ""
	i := 0
	var star string // "*" for commited, "_" for uncommited.
	for _, tkt := range orderedTickets {
		if tkt.Committed {
			star = "*"
		} else {
			star = "_"
		}
		sum += fmt.Sprintf("%v[%v%v] '%v'", extra, tkt.LogIndex, star, tkt.Desc)
		if i == 0 {
			extra = "; "
		}
		i++
	}
	sum += ")"
	return
}

func (s *TubeSim) me() string {
	waiting, _ := s.waitingSummary()
	return fmt.Sprintf("%v %v[%v] %v", s.myid, s.role.String(), s.state.CurrentTerm, waiting)
}

func (s *TubeSim) mustGetLastLogIndex() int64 {
	last := len(s.wal.raftLog) - 1
	if last <= 0 {
		panic("no log available!")
	}
	return s.wal.raftLog[last].Index
}

func (s *TubeSim) newRaftNodeInfo(peerID, peerName, peerServiceName, peerServiceNameVersion string) (info *RaftNodeInfo) {
	info = &RaftNodeInfo{
		PeerName:               peerName,
		PeerServiceName:        peerServiceName,
		PeerServiceNameVersion: peerServiceNameVersion,
		PeerID:                 peerID,
		MatchIndex:             0,
	}
	return
}

// ================================
// TubeSim/test versions of these methods
// TODO: store what we are given, and check
// correctness at the end of the test, adding extra tests.
// ================================
func (s *TubeSim) getRaftLogSummary() (localFirstIndex, localFirstTerm, localLastIndex, localLastTerm int64) {
	s.getRaftLogSummaryCount++
	if len(s.wal.raftLog) == 0 {
		return 0, 0, 0, 0
	}
	localFirstIndex = s.wal.raftLog[0].Index
	localFirstTerm = s.wal.raftLog[0].Term
	localLastIndex = s.wal.raftLog[len(s.wal.raftLog)-1].Index
	localLastTerm = s.wal.raftLog[len(s.wal.raftLog)-1].Term
	return
}

func (s *TubeSim) ackAE(ack *AppendEntriesAck, ae *AppendEntries) {
	s.ackAECount++
	if ack.Rejected {
		s.ackAERejectCount++
	}
	// For testing purposes, we just need to verify the ack was created
	// The actual RPC handling is not needed for this test
}

func (s *TubeSim) becomeFollower(term int64, mc *MemberConfig, save bool) {
	s.becomeFollowerCount++
	s.state.CurrentTerm = term
	s.role = FOLLOWER
}

func (s *TubeSim) dispatchAwaitingLeaderTickets() {
	s.dispatchAwaitingLeaderTicketsCount++
	// Not needed for this test
}

func (s *TubeSim) resetElectionTimeout(where string) time.Duration {
	s.resetElectionTimeoutCount++
	return 0
	// Not needed for this test
}

func (s *TubeSim) commitWhatWeCan(leader bool) {
	s.commitWhatWeCanCount++
	// Not needed for this test
}

func (s *TubeSim) choice(format string, a ...interface{}) {
	if s != nil && s.choiceLoud {
		tsPrintf("CHOICE: "+format, a...)
	}
}

type TubeSim struct {
	cfg         TubeConfig
	beforeAElog *raftWriteAheadLog

	// allow simae_test to switch to TubeNode
	// actual version of handleAppendEntries()
	host hoster
	node *TubeNode

	role                   RaftRole
	leader                 string
	electionTimeoutCh      <-chan time.Time
	lastLegitAppendEntries time.Time

	leaderSendsHeartbeatsCh <-chan time.Time

	ClusterID string `zid:"0"`

	// comms
	URL           string
	PeerID        string
	pushToPeerURL chan string
	myPeer        *rpc.LocalPeer
	halt          *idem.Halter

	// ckt tracks all our peer replicas (does not include ourselves)
	ckt        map[string]*rpc.Circuit
	rpccfg     *rpc.Config
	srv        *rpc.Server
	name       string // "tube_node_2" vs. srvServiceName, typically "tube-replica"
	writeReqCh chan *Ticket
	readReqCh  chan *Ticket

	nextWake       time.Time
	nextWakeCh     <-chan time.Time
	nextWakeTicket *Ticket

	// convenience in debug printing
	myid string //

	state *RaftState

	saver *raftStatePersistor

	t0             time.Time
	requestInpsect chan *Inspection

	Waiting map[string]*Ticket `zid:"-"`

	ticketsAwaitingLeader map[string]*Ticket

	clientSessionHighestTicketSerial map[string]*Session

	debugMsgStat map[int]int

	// serial number assigned to ticket.LeaderStampSN
	// when the leader receives a ticket.
	prevTicketStampSN int64

	// new leader must get this ticket committed before
	// doing anything else...
	// but we don't want to persist it across restarts
	// since it is from an old term, and the point was
	// that we needed to get a commit in the _current leader's term_
	// to not loose configurations;older transactions.
	initialNoop0Tkt *Ticket
	// commitWhatWeCan sets this once initialNoop0Tkt commits.
	initialNoop0HasCommitted bool

	// candidate running an election, tally votes here.
	// initialize in beginElection()
	votes    map[string]bool
	yesVotes int

	// log entries. Each entry contains
	// command for state machine, and term
	// when entry was received by leader (first index 1).
	// Note: wal.raftLog has our log (in memory version)
	wal *raftWriteAheadLog

	// do not include ourselves.
	peers map[string]*RaftNodeInfo

	// section 6.4, p73, "Processing read-only queries more efficiently"
	readIndex int64 // not really used at the moment.

	// pre-vote state (volatile)
	preVoteTerm     int64 // 0 means invalid
	lastPreVoteTerm int64 // holds copy of last preVoteTerm before zeroed out.
	preVotedFor     string
	preVotedAgainst string
	preVotes        map[string]bool
	yesPreVotes     int
	noPreVotes      int

	// cached election timeout param,
	// these don't change after startup,
	// so cache rather than recompute.
	cachedMinElectionTimeoutDur time.Duration
	cachedMaxElectionTimeoutDur time.Duration

	// how often our local read optimization
	localReadCount       int64
	totalReadCount       int64
	lastLocalReadLease   time.Duration
	lowestLocalReadLease time.Duration

	// channel stats
	countFrag      int
	countWakeCh    int
	countWriteCh   int
	countReadCh    int
	countElections int
	countHeartbeat int

	// Method call counters for debugging
	getRaftLogSummaryCount             int
	ackAECount                         int
	ackAERejectCount                   int
	becomeFollowerCount                int
	dispatchAwaitingLeaderTicketsCount int
	resetElectionTimeoutCount          int
	commitWhatWeCanCount               int

	// test helper
	choiceLoud bool
}

// Helper function to visualize log entries
func visualizeLog(entries []*RaftLogEntry, prefix string) string {
	if len(entries) == 0 {
		return prefix + " (empty)\n"
	}
	var result strings.Builder
	result.WriteString(prefix + ":\n")
	for _, entry := range entries {
		committed := " "
		if entry.committed {
			committed = "C"
		}
		result.WriteString(fmt.Sprintf("  [%v] term=%v %v\n",
			entry.Index, entry.Term, committed))
	}
	return result.String()
}

// Helper function to create a set of log entries
func createLogEntries(begin, endx int64, term int64) []*RaftLogEntry {
	var entries []*RaftLogEntry
	// Note: begin and endi are both 1-based log indices
	for i := begin; i < endx; i++ {
		e := &RaftLogEntry{
			Term:  term,
			Index: i, // Already 1-based
			Ticket: &Ticket{
				Key:  Key(fmt.Sprintf("key%v", i)),
				Val:  []byte(fmt.Sprintf("value%v", i)),
				Desc: fmt.Sprintf("entry %v", i),
			},
		}
		//vv("createLogEntries e = '%#v'", e)
		entries = append(entries, e)
	}
	//vv("createLogEntries len %v for beg=%v, endx=%v, should be %v", len(entries), begin, endx, endx-begin)
	return entries
}

// Helper function to create expected follower log entries
func createExpectedFollowerLog(tc *testCase) []*RaftLogEntry {
	var entries []*RaftLogEntry
	// Add initial entries (1-based indexing)
	if tc.followerLogLen > 0 {
		entries = createLogEntries(tc.followerLogBeg, tc.followerLogLen+1, tc.followFirstTerm)
		// Mark entries as committed up to committedLen
		for i := int64(0); i < tc.followerCommittedIndex; i++ {
			entries[i].committed = true
		}
	}
	// Add append entries if they should be accepted
	// Note: appendBegin is inclusive, appendLastIdx is inclusive
	if !tc.shouldReject && tc.appendBegin > tc.followerLogLen {
		appendEntries := createLogEntries(tc.appendBegin, tc.appendLastIdx+1, tc.leaderFirstTerm)
		entries = append(entries, appendEntries...)
	}
	return entries
}

// Helper function to set up initial follower log state
func setupInitialFollowerLog(s *TubeSim, tc *testCase) error {
	if tc.followerLogLen > 0 {
		followLog := createLogEntries(tc.followerLogBeg, tc.followerLogLen+1, 1)
		// Mark entries as committed up to committedLen
		for i := int64(0); i < tc.followerCommittedIndex; i++ {
			followLog[i].committed = true
		}
		s.state.CommitIndex = tc.followerCommittedIndex
		// note that s.state.CommitIndexEntryTerm was added much later.
		//s.wal.raftLog = followLog

		s.wal, _ = s.cfg.newRaftWriteAheadLogMemoryOnlyTestingOnly()
		s.wal.nodisk = true
		for _, e := range followLog {
			s.wal.saveRaftLogEntry(e)
		}
	}
	return nil
}

func showExpectedObserved(t *testing.T, scenario int64, tc *testCase, s *TubeSim, ae *AppendEntries, testOneScenario int64) {
	// Print expected and observed states
	expectedLog := createExpectedFollowerLog(tc)
	fmt.Printf("\nExpected final state (length=%v):\n%v",
		tc.expectedFollowLenFinal, visualizeLog(expectedLog, "Expected log"))
	fmt.Printf("\nObserved final state (length=%v):\n%v",
		len(s.wal.raftLog), visualizeLog(s.wal.raftLog, "Observed log"))
	fmt.Printf("%v", s.methodCallCounts())
}

// s is follower
func printScenario(scenario int64, tc *testCase, s, lead *TubeSim, ae *AppendEntries) {
	// Print test setup
	fmt.Printf("\nFailing Scenario %v follower:\n", scenario)
	fmt.Printf("Initial state (commitIndex=%v, currentTerm=%v):\n%v",
		s.state.CommitIndex, s.state.CurrentTerm, visualizeLog(s.wal.raftLog, "Initial log"))
	fmt.Printf("Append request (begin=%v, endi=%v):\n%v",
		tc.appendBegin, tc.appendLastIdx, visualizeLog(ae.Entries, "Append entries"))
}

// Reset all method call counters
func (s *TubeSim) resetMethodCounters() {
	s.getRaftLogSummaryCount = 0
	s.ackAECount = 0
	s.ackAERejectCount = 0
	s.becomeFollowerCount = 0
	s.dispatchAwaitingLeaderTicketsCount = 0
	s.resetElectionTimeoutCount = 0
	s.commitWhatWeCanCount = 0
}

// Get a string representation of method call counts
func (s *TubeSim) methodCallCounts() string {
	var result strings.Builder
	result.WriteString("Method call counts:\n")
	result.WriteString(fmt.Sprintf("  getRaftLogSummary: %v\n", s.getRaftLogSummaryCount))

	// Determine ackAE status
	ackStatus := "accept"
	if s.ackAERejectCount > 0 {
		ackStatus = "reject"
	}
	result.WriteString(fmt.Sprintf("  ackAE: %v  (%v)\n", s.ackAECount, ackStatus))

	result.WriteString(fmt.Sprintf("  becomeFollower: %v\n", s.becomeFollowerCount))
	result.WriteString(fmt.Sprintf("  dispatchAwaitingLeaderTickets: %v\n", s.dispatchAwaitingLeaderTicketsCount))
	result.WriteString(fmt.Sprintf("  resetElectionTimeout: %v\n", s.resetElectionTimeoutCount))
	result.WriteString(fmt.Sprintf("  commitWhatWeCan: %v\n", s.commitWhatWeCanCount))
	return result.String()
}

func (s *TubeSim) logsAreMismatched(ae *AppendEntries) (
	reject bool, // reject will be true if we cannot apply
	conflictTerm int64, // -1 if no conflict
	conflictTerm1stIndex int64) { // -1 if no conflict

	//ae.PrevLogIndex // index of log entry immediately preceeding new ones
	//ae.PrevLogTerm  // term of prevLogIndex entry	rlog := s.wal.raftLog

	if len(ae.Entries) == 0 {
		// don't reject heartbeats
		return false, -1, -1
	}

	if ae.PrevLogIndex <= 0 {
		// no constraint on log
		return false, -1, -1
	}

	rlog := s.wal.raftLog
	n := int64(len(rlog))
	if n < ae.PrevLogIndex {
		return true, -1, -1 // not present, our log is too short.
	}
	// page 19
	// "log entries never change their position in the log."

	prevTerm := rlog[ae.PrevLogIndex-1].Term
	if prevTerm == ae.PrevLogTerm {
		// good: acceptable. no reject.
		return false, -1, -1
	}
	// tell the server how we mismatch
	//
	// page 21
	// "For example, when rejecting an AppendEntries request,
	// the follower can include the term of the conflicting
	// entry and the first index it stores for that term.
	conflictTerm = prevTerm
	conflictTerm1stIndex = ae.PrevLogIndex
	i := ae.PrevLogIndex
	for i--; i > 0; i-- {
		if rlog[i-1].Term == conflictTerm {
			conflictTerm1stIndex--
		} else {
			break
		}
	}
	//vv("not matching term, reject. prevTerm=%v; ae.PrevLogTerm=%v", prevTerm, ae.PrevLogTerm)
	reject = true
	return
}

// TubeSim.handleAppendEntries started as a copy
// of the central tube.go TubeNode.handleAppendEntries,
// and then was instrumented for testing. It should be
// kept in sync with TubeNode.handleAppendEntries, so
// that the tests against this "model" reflect the actual
// live code there. All of the methods this TubeSim
// version calls have been duplicated and instrumented
// too, for ease of analysis.
func (s *TubeSim) handleAppendEntries(ae *AppendEntries, ckt0 *rpc.Circuit) (numOverwrote, numTruncated, numAppended int64) {

	panic("I thought we use the actual tube.go now?")

	if s.state.CurrentTerm <= 0 {
		// on first joining, we don't have a current term.
		// this is normal, the AE we just got will give a term.
	}
	if ae.LeaderTerm < 1 {
		// should never happen, our tests were just bad mannered.
		panic(fmt.Sprintf("handleAppendEntries() dropping AE with bad term: %v; is < 1", ae.LeaderTerm))
		return
	}

	numNew := len(ae.Entries)
	isHeartbeat := (numNew == 0)
	_ = isHeartbeat

	s.host.choice("Starting handleAppendEntries with %v new entries", numNew)

	rlog := s.wal.raftLog

	var entriesIdxBeg, entriesIdxEnd, entriesEndxIdx int64
	if numNew == 0 {
		s.host.choice("Empty heartbeat")
	} else {
		entriesIdxBeg = ae.Entries[0].Index
		entriesIdxEnd = ae.Entries[numNew-1].Index
		entriesEndxIdx = entriesIdxEnd + 1
		s.host.choice("Append range [%v:%v] aka [%v:%v); len = %v", entriesIdxBeg, entriesIdxEnd, entriesIdxBeg, entriesEndxIdx, numNew)
	}

	localFirstIndex, localFirstTerm, localLastIndex,
		localLastTerm := s.host.getRaftLogSummary()

	followerLog := s.wal.getTermsRLE()

	// be ready to reject half-dozen ways...
	ack := &AppendEntriesAck{
		ClusterID:  s.ClusterID,
		FromPeerID: s.PeerID,
		Rejected:   true,

		AEID:                  ae.AEID,
		Term:                  s.state.CurrentTerm,
		MinElectionTimeoutDur: s.cachedMinElectionTimeoutDur,

		LargestCommonRaftIndex: -1,

		PeerLogTermsRLE:     followerLog,
		PeerLogCompactIndex: s.wal.logIndex.BaseC,
		PeerLogCompactTerm:  s.wal.logIndex.CompactTerm,

		PeerLogFirstIndex: localFirstIndex,
		PeerLogFirstTerm:  localFirstTerm,
		PeerLogLastIndex:  localLastIndex,
		PeerLogLastTerm:   localLastTerm,

		SuppliedCompactIndex: ae.LeaderCompactIndex,
		SuppliedCompactTerm:  ae.LeaderCompactTerm,

		SuppliedPrevLogIndex:      ae.PrevLogIndex,
		SuppliedPrevLogTerm:       ae.PrevLogTerm,
		SuppliedEntriesIndexBeg:   entriesIdxBeg,
		SuppliedEntriesIndexEnd:   entriesIdxEnd,
		SuppliedLeaderCommitIndex: ae.LeaderCommitIndex,
		SuppliedLeader:            ae.LeaderID,
		SuppliedLeaderTermsRLE:    ae.LogTermsRLE,
	}

	if ae.LeaderTerm < s.state.CurrentTerm {
		s.host.choice("Rejecting: term too low (ae.LeaderTerm=%v < currentTerm=%v)", ae.LeaderTerm, s.state.CurrentTerm)
		ack.RejectReason = fmt.Sprintf("term too low: ae.LeaderTerm(%v) < s.state.CurrentTerm(%v)", ae.LeaderTerm, s.state.CurrentTerm)
		s.host.ackAE(ack, ae)
		return
	}

	s.host.choice("Term check passed: ae.LeaderTerm=%v >= currentTerm=%v", ae.LeaderTerm, s.state.CurrentTerm)
	s.lastLegitAppendEntries = time.Now()

	if ae.LeaderTerm > s.state.CurrentTerm {
		// not important: s.host.choice( "Becoming follower due to higher term")
		s.host.becomeFollower(ae.LeaderTerm, nil, true)
	} else {
		if s.role != FOLLOWER {
			s.host.choice("Stepping down to follower role")
			s.role = FOLLOWER
			s.leaderSendsHeartbeatsCh = nil
		}
		// moved below, like in tube.go; the new leader is legit too.
		//s.host.resetElectionTimeout("handleAppendEntries higher term seen.")
	}
	s.host.resetElectionTimeout("handleAppendEntries >= term seen.")
	ack.Term = s.state.CurrentTerm

	if s.leader != ae.FromPeerID {
		// always happens, of course, in our testing env.
		//s.choice( "New leader detected: %v", ae.FromPeerID)
		s.leader = ae.FromPeerID
		// not under test. no need for this.
		//defer func() {
		//	s.dispatchAwaitingLeaderTickets()
		//}()
	}

	leaderLog := ae.LogTermsRLE

	extends, largestCommonRaftIndex, _ := leaderLog.Extends(followerLog)
	s.host.choice("extends=%v, largestCommonRaftIndex=%v; leaderLog.Endi=%v ", extends, largestCommonRaftIndex, leaderLog.Endi)

	upToDate := largestCommonRaftIndex == leaderLog.Endi
	ack.LargestCommonRaftIndex = largestCommonRaftIndex

	if upToDate {
		s.host.choice("Logs are up to date")
		ack.Rejected = false
		ack.LogsMatchExactly = true
		s.host.ackAE(ack, ae)
		return
	}

	reject, conflictTerm, conflictTerm1stIndex := s.host.logsAreMismatched(ae)
	ack.ConflictTerm = conflictTerm
	ack.ConflictTerm1stIndex = conflictTerm1stIndex

	s.host.choice("Logs differ: extends=%v, largestCommonRaftIndex=%v; reject=%v, conflictTerm=%v, conflictTerm1stIndex=%v", extends, largestCommonRaftIndex, reject, conflictTerm, conflictTerm1stIndex)

	if reject {
		alsoGap := ae.PrevLogIndex > largestCommonRaftIndex
		ack.RejectReason = fmt.Sprintf("ae.PrevLogIndex(%v) not "+
			"compatible. conflictTerm=%v, conflictTerm1stIndex=%v; "+
			"largestCommonRaftIndex=%v; alsoGap=%v",
			ae.PrevLogIndex, conflictTerm, conflictTerm1stIndex,
			largestCommonRaftIndex, alsoGap)

		s.host.choice(ack.RejectReason)
		s.host.ackAE(ack, ae)
		return
	}
	// If this is a heartbeat, definitely reject so we get data.
	// Well, we get data either way. But this _is_ simpler.
	if numNew == 0 { // isHeartbeat equivalent
		ack.RejectReason = fmt.Sprintf("heartbeat detected log update needed. extends=%v, largestCommonRaftIndex=%v", extends, largestCommonRaftIndex)
		ack.Rejected = true
		s.host.choice(ack.RejectReason)
		s.host.ackAE(ack, ae)
		return
	}
	// INVAR: len(ae.Entries) > 0, since we just rejected heartbeats
	// (We could still have dups and no really new information).

	didAddData := false
	done := false

	if !extends && ae.PrevLogIndex > largestCommonRaftIndex {
		s.host.choice("Rejecting: gap in log at prev log index %v > lcp %v", ae.PrevLogIndex, largestCommonRaftIndex)
		ack.RejectReason = fmt.Sprintf("gap: need earlier in leader log: ae.PrevLogIndex(%v) > largestCommonRaftIndex(%v)",
			ae.PrevLogIndex, largestCommonRaftIndex)
		s.host.ackAE(ack, ae)
		return
	}

	if !extends {
		// already returned above on numNew == 0
		// if numNew == 0 {
		// 	s.host.choice( "Rejecting: no data to bridge gap")
		// 	ack.RejectReason = "!extends, so gap we cannot bridge, and we got no data this time."
		// 	s.host.ackAE(ack, ae)
		// 	return
		// }

		// INVAR: numNew > 0
		if entriesIdxBeg > largestCommonRaftIndex+1 {
			s.host.choice("Rejecting: cannot bridge gap at index %v", entriesIdxBeg)
			ack.RejectReason = fmt.Sprintf("cannot gap: entriesIdxBeg(%v) > largestCommonRaftIndex+1(%v)",
				entriesIdxBeg, largestCommonRaftIndex+1)
			s.host.ackAE(ack, ae)
			return
		} // else we can apply (append or overwrite, or both).

		// INVAR: entriesIdxBeg <= largestCommonRaftIndex+1
		//    =>  entriesIdxBeg < largestCommonRaftIndex

		if entriesIdxBeg == largestCommonRaftIndex+1 {
			s.host.choice("comment: Perfectly aligned append at index %v", entriesIdxBeg)
		} else {
			s.host.choice("Warning: potential overwrite at index %v", entriesIdxBeg)
		}

		// !extends, no gap. Cannot just append.
		// possible overwrite, but have to dedup first to know.

		// THIS IS THE DEDUP IMPLEMENTATION
		//
		// Want to ignore replacement of same with same.
		// and then should we reject or accept?
		// fine to accept/reject, either way handleAEAck will
		// update us with the next thing we need. so accept.
		if entriesIdxEnd <= largestCommonRaftIndex {
			// nothing new
			s.host.choice("nothing new to append after clipping off the redundant. leaving neededEntries empty")
			ack.RejectReason = fmt.Sprintf("nothing new after clipping off redundant. largestCommonRaftIndex(%v); ae=[%v,%v)", largestCommonRaftIndex, entriesIdxBeg, entriesIdxEnd+1)
			ack.Rejected = false
			s.host.ackAE(ack, ae)
			return
		}
		// !extends. no gap. Cannot just append. entriesIdxEnd > lcp.

		s.host.choice("!extends. no gap. Cannot just append. entriesIdxEnd > lcp.")

		// INVAR:  entriesIdxEnd > lcp, so
		//                   lcp <  entriesIdxEnd
		//         entriesIdxBeg <= entriesIdxEnd, by creation of AE;
		// could there be a gap between lcp and entriesIdxBeg? nope
		//         entriesIdxBeg < lcp from above.
		// so we have
		// entriesIdxBeg < lcp < entriesIdxEnd
		// so we only need the ones from [lcp+1, entriesIdxEnd]
		// and we can discard [entriesIdxBeg, lcp]
		keepCount := largestCommonRaftIndex
		//startWritingAt := largestCommonRaftIndex + 1
		baseActuallyNew0 := largestCommonRaftIndex + 1 - entriesIdxBeg
		//vv("baseActuallyNew0 = %v = largestCommonRaftIndex(%v) +1 -entriesIdxBeg(%v)", from, largestCommonRaftIndex, entriesIdxBeg)

		neededEntries := ae.Entries[baseActuallyNew0:]
		needed := len(neededEntries)
		if needed == 0 {
			s.host.choice("nothing new to append after clipping off the redundant")
			ack.Rejected = false
			ack.RejectReason = fmt.Sprintf("nothing new after clipping off redundant. largestCommonRaftIndex(%v); ae=[%v,%v)", largestCommonRaftIndex, entriesIdxBeg, entriesIdxEnd+1)
			s.host.ackAE(ack, ae)
			return
		}
		writeBegIdx := neededEntries[0].Index
		keepCount = writeBegIdx - 1
		// use writeBegIdx just below, NOT entriesIdxBeg !!

		s.host.choice("writeBegIdx=%v, len(rlog)=%v; lcp=%v, needed=%v", writeBegIdx, len(rlog), largestCommonRaftIndex, needed)

		//numOverwrote = max(0, largestCommonRaftIndex-writeBegIdx+1)
		//numOverwrote = max(0, int64(len(rlog))-writeBegIdx+1)

		// adjust the AE? ugh. leave immutable, and just write now.

		// we might be shorter afterwards, if the AE shrinks our log.
		numTruncated = max(0, int64(len(rlog))-entriesIdxEnd) // yes
		// max(0, ) not really needed here, but is more general.
		numAppended = max(0, entriesIdxEnd-int64(len(rlog))) // yes
		numOverwrote = int64(needed) - numAppended

		s.host.choice("have %v neededEntries after clipping off the redundant. nOver=%v, nTrunc=%v, nAppend=%v", needed, numOverwrote, numTruncated, numAppended)

		err := s.wal.overwriteEntries(keepCount, neededEntries, false, s.state.CommitIndex, s.state.LastApplied, &s.state.CompactionDiscardedLast)
		panicOn(err)
		didAddData = true
		done = true // don't call s.wal.overwriteEntries() again below.

		//} // end else not perfectly aligned append/extension

	} // end !extends

	ack.Rejected = false

	if !done {
		s.host.choice("Processing %v new entries", numNew)

		if extends {
			keepCount := int64(len(rlog))
			from := keepCount + 1 - entriesIdxBeg

			switch {
			case from >= int64(len(ae.Entries)):
				s.host.choice("No useful data to append") // not true yet! have to compare the term data too.

				// These are the defaults, but stated here for
				// clarity. Keep them to make reasoning easier.
				numOverwrote = 0
				numTruncated = 0
				numAppended = 0

			case from < 0:

				//vv("keepCount = %v; from = %v; entriesIdxBeg = %v", keepCount, from, entriesIdxBeg) // keepCount = 0; from = -1; entriesIdxBeg = 2

				s.host.choice("from index negative: gap/insufficient follower log")
				ack.RejectReason = "from was negative: gap/insuffic follower log"
				s.host.ackAE(ack, ae)
				return

			default:
				entries := ae.Entries[from:]
				s.host.choice("Appending %v entries starting at index %v", len(entries), entries[0].Index)

				numOverwrote = 0
				numTruncated = 0
				numAppended = int64(len(entries))

				err := s.wal.overwriteEntries(keepCount, entries, false, s.state.CommitIndex, s.state.LastApplied, &s.state.CompactionDiscardedLast)
				panicOn(err)
				didAddData = true
			}
		} else {
			// !extends: I think this is now redundant with the DEDUP above.
			keepCount := largestCommonRaftIndex
			from := keepCount + 1 - entriesIdxBeg
			entries := ae.Entries[from:]

			//vv("Overwriting from index %v with %v entries", keepCount+1, len(entries))
			//vv("followers log = '%v'", visualizeLog(s.wal.raftLog, "follower log"))
			s.host.choice("Overwriting from index %v with %v entries", keepCount+1, len(entries))

			numOverwrote = entriesIdxEnd - keepCount // yes.
			// we might be shorter afterwards, if the AE shrinks our log.
			//numTruncated = min(0, int64(len(rlog))-entriesIdxEnd) max? getting -1
			numTruncated = max(0, int64(len(rlog))-entriesIdxEnd)
			numAppended = max(0, entriesIdxEnd-int64(len(rlog)))

			err := s.wal.overwriteEntries(keepCount, entries, false, s.state.CommitIndex, s.state.LastApplied, &s.state.CompactionDiscardedLast)
			panicOn(err)
			didAddData = true
		} // end else !extends
	} // end if !done

	s.host.resetElectionTimeout("handleAppendEntries, after disk overwriteEntries")

	prevci := s.state.CommitIndex
	if ae.LeaderCommitIndex > s.state.CommitIndex {
		lli := int64(len(s.wal.raftLog))
		s.host.choice("Updating commitIndex from %v to %v", prevci, min(ae.LeaderCommitIndex, lli))
		s.state.CommitIndex = min(ae.LeaderCommitIndex, lli)
		// note that s.state.CommitIndexEntryTerm was added much later.

	}

	s.host.commitWhatWeCan(false)

	localFirstIndex, localFirstTerm, localLastIndex,
		localLastTerm = s.host.getRaftLogSummary()

	if didAddData {
		ack.PeerLogTermsRLE = s.wal.getTermsRLE()
	}

	ack.LogsMatchExactly = localLastIndex == leaderLog.lastIndex() &&
		localLastTerm == leaderLog.lastTerm()

	ack.Rejected = false

	ack.PeerLogFirstIndex = localFirstIndex
	ack.PeerLogFirstTerm = localFirstTerm
	ack.PeerLogLastIndex = localLastIndex
	ack.PeerLogLastTerm = localLastTerm
	ack.SuppliedLeaderTermsRLE = ae.LogTermsRLE

	s.host.choice("Sending success ack")
	s.host.ackAE(ack, ae)

	s.host.resetElectionTimeout("handleAppendEntries after ackAE")
	return
}
