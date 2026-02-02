package tube

import (
	"bytes"
	"fmt"

	"iter"
	mathrand "math/rand"
	"testing"
	"time"
)

const testTubeNodeHandleAE = true // TubeNode version of handleAE
//const testTubeNodeHandleAE = false // TubeSim version of handleAE

// test runs are reproducible, but changing
// this seed will test a different set of
// reproducible (random) term assignments.
const pseudoRNGseed int64 = 0

// These control the depth of testing and
// the corresponding runtime.
// 10 and 10 => 698_885 cases tested in 25 sec.
// 5 and 5 => 18_858 cases  in 0.8 sec.
// 7 and 7 => 104_490 cases in 3.7 sec.
// 7 and 3 => 22_998 cases  in 0.75 sec. Default so can be on for general testing.
// 8 and 3 => 37_761 cases  in 1.5 sec.
// const maxFollowLogLen = 10 // max size of followers's starting log
// const addLeaderLogLen = 10 // additional max size of leader's log
const maxFollowLogLen = 7
const addLeaderLogLen = 3

func Test802_handle_append_entries_with_random_terms(t *testing.T) {
	//return

	greenCount := 0
	defer func() {
		vv("test 802 wrapping up. testTubeNodeHandleAE=%v; greenCount = %v", testTubeNodeHandleAE, greenCount)
	}()

	// Debug control - set to > 0 to run only that scenario
	//testOneScenario := int64(17840)
	testOneScenario := int64(0)

	const loud = true
	var firstRunLoud = false

	if testOneScenario > 0 {
		firstRunLoud = true // show very first run (seems backwards but is fine)
	}

	next, stop := iter.Pull2(testCaseSeq(testOneScenario))
	defer stop()

	var lcp int64 // make sure the two runs have the same

	for {

		tc, ok, valid := next()
		if !valid {
			vv("no more tests to run")
			break
		}
		if !ok {
			break
		}

		lead, ae, rng := getLeaderSetupForTest(t, tc, firstRunLoud, tc.scenario)
		foll, commonPrefixLen := getFollowerSetupForTest(t, tc, firstRunLoud, tc.scenario, lead, rng)

		lcp = commonPrefixLen
		verifySetupAgainstTestSpec(t, tc, lcp, lead, foll, ae)
		//vv("%v", visualizeLog(ae.Entries, "AE after term update"))

		aeSz := tc.appendEndxIdx - tc.appendBegin
		if aeSz != tc.appendLen {
			panic("bad setup logic--did not follow test spec")
		}
		if tc.appendEndxIdx != tc.appendLastIdx+1 {
			panic("bad setup logic-- must have appendEndxIdx == appendLastIdx+1")
		}
		beginFollowLastIdx := int64(len(foll.wal.raftLog))
		if beginFollowLastIdx != tc.followerLogLen {
			panic("bad setup logic--did not follow test spec. beginFollowLastIdx != tc.followerLogLen")
		}

		var shouldReject bool

		// compute expected length after ae has been applied.
		// We assume it is accepted for these computations.
		var xFollowLenAfter int64
		// compute exepected numOverwrite, numAppend, numTruncate
		// on AE not rejected. If a heartbeat (aeSz == 0), these
		// all stay 0. A heartbeat should be rejected, however,
		// if the followers data is different.

		var xTrunc int64
		var xOver int64
		var xAppend int64
		var rejectOnGap, ignoreTooOld, hbDetectsFollowerNeedsDataReject bool

		hbDetectsFollowerNeedsDataReject, xOver, rejectOnGap,
			ignoreTooOld, xTrunc, xAppend, xFollowLenAfter = aeUpdatesFollNoGap(ae, foll, lead)
		_ = ignoreTooOld // for state compression later
		//vv("hbReject = %v; gap=%v", hbDetectsFollowerNeedsDataReject, rejectOnGap)
		if rejectOnGap || hbDetectsFollowerNeedsDataReject {
			shouldReject = true
		}

		if aeSz > 0 {
			// Determine if this should be rejected
			// Reject if:
			// 1. Gap in log (appendBegin > followerLen)
			if tc.appendBegin > tc.followerLogLen+1 { // Gap between log and AE
				shouldReject = true
			}

			// 2. if the AE PrevLogTerm disagrees
			if ae.PrevLogTerm > 0 && int64(len(foll.wal.raftLog)) >= ae.PrevLogIndex && foll.wal.raftLog[ae.PrevLogIndex-1].Term != ae.PrevLogTerm {
				shouldReject = true
			}
		} // end if aeSz > 0

		flogCanDiverge := false

		// 3. Its an empty heartbeat and our logs have content
		// but are different.
		if aeSz == 0 {
			// unchanged follower log on heartbeat
			xFollowLenAfter = beginFollowLastIdx

			switch {
			case tc.followerLogLen == 0 && tc.leaderLogLen == 0:
				// both empty logs. That's fine for heartbeats.
				//shouldReject = false

			case tc.followerLogLen > 0 && tc.leaderLogLen == 0:
				// legit, old leader may have left remnants.
				// heartbeat should normally be rejected so we send data,
				// but here leader has no data to send; still
				// alert leader to the mismatch, maybe it should
				// tell us to delete it, but really this situation
				// is disallowed by Raft election rules. Follower
				// cannot have longer more up to date log than
				// leader. Well don't freak out. Accept the hb because
				// the server really doesn't have to send us anything.
				//shouldReject = false
				flogCanDiverge = true
			case tc.followerLogLen == 0 && tc.leaderLogLen > 0:
				// common case for new follower who
				// has nothing yet.
				shouldReject = true

			default:
				// INVAR: aeSz == 0, heartbeat
				// both logs have stuff. does it differ?

				if lead == nil || lead.wal == nil {
					panic("Wat?? lead is nil or lead.wal is nil?")
				} else {
					//vv("lead.wal.raftLog[0] = '%#v'", lead.wal.raftLog[0]) // panic
				}
				if foll == nil || foll.wal == nil {
					panic("Wat?? foll is nil or foll.wal is nil?")
				} else {
					//vv("foll.wal.raftLog = '%v'", foll.wal.raftLog)
				}
				//leadLastTerm := lead.wal.raftLog[tc.leaderLogLen-1].Term
				//follLastTerm := foll.wal.raftLog[tc.followerLogLen-1].Term
				// INVAR: heartbeat
				if tc.followerLogLen != tc.leaderLogLen {
					// well, maybe we didn't try to make them the
					// same; it was just a heartbeat.

					// heartbeats can detect out of date data and reject.
					if tc.leaderLogLen > tc.followerLogLen {
						shouldReject = true
					}
					// scenario 7285
				} else {
					// 7296 shows insufficient: leadLastTerm != follLastTerm
					if logsDiffer(lead.wal, foll.wal) {
						shouldReject = true
					}
				}
			}
		} // end if aeSz == 0

		tc.shouldReject = shouldReject
		if flogCanDiverge {
			//vv("testOneScenario= %v; shouldReject=%v; flogCanDiverge=%v", testOneScenario, shouldReject, flogCanDiverge)
		}
		if !shouldReject && !flogCanDiverge {
			// this is the accept case len of follow log afterwards.
			//vv("shouldReject=%v; tc.expectedFollowLenFinal(%v) = xFollowLenAfter(%v)", shouldReject, tc.expectedFollowLenFinal, xFollowLenAfter)
			tc.expectedFollowLenFinal = xFollowLenAfter
		} else {
			// follower stays the same on reject
			//vv("shouldReject=%v; tc.expectedFollowLenFinal(%v) = xFollowLenAfter(%v)", shouldReject, tc.expectedFollowLenFinal, tc.followerLogLen)
			tc.expectedFollowLenFinal = tc.followerLogLen
		}

		// =========================================
		// ready to run...

		var nOver, nTrunc, nAppend int64

		var s hoster = foll
		if testTubeNodeHandleAE {
			s = foll.node
		}

		var panicked any  // panic value caught by defer, in any.
		descOfPanic := "" // report if we paniced or not.
		catch := true     // both true and false are useful for debugging.
		//catch := false

		if catch {
			func() {
				defer func() {
					panicked = recover()
					if panicked != nil {
						descOfPanic = "(panic)"
					}
				}()

				// ============================
				// the test
				// ============================
				nOver, nTrunc, nAppend = s.handleAppendEntries(ae, nil)

			}()
		} else {
			// the test, without catching panics.
			// The wal will panic if we attempt to
			// overwrite an existing identical value,
			// or an already committed one. See memwal.go:75
			nOver, nTrunc, nAppend = s.handleAppendEntries(ae, nil)
		}
		failed := (panicked != nil) || testFailed(tc, foll, ae, firstRunLoud, nOver, nTrunc, nAppend, xOver, xTrunc, xAppend)

		if failed {
			if tc == nil {
				panic("Wat? tc is nil?")
			}
			alwaysPrintf("\n\n ======== scenario %v failed, re-running verbosely %v %v  ========", tc.scenario, descOfPanic, panicked)

			// same setup, just loud flag on for sure.
			lead, ae, rng = getLeaderSetupForTest(t, tc, loud, tc.scenario)
			foll2, lcp2 := getFollowerSetupForTest(t, tc, loud, tc.scenario, lead, rng)
			if lcp2 != lcp {
				panic(fmt.Sprintf("lcp2(%v) != lcp(%v). not reproducing same test if longest common prefixes differ between runs!?!", lcp2, lcp))
			}
			s = foll2
			if testTubeNodeHandleAE {
				s = foll2.node
			}

			// ============================
			// re-run the exact same test
			// ============================
			nOver2, nTrunc2, nAppend2 := s.handleAppendEntries(ae, nil)
			if nOver2 != nOver || nTrunc2 != nTrunc || nAppend2 != nAppend {
				panic(fmt.Sprintf("nOver2(%v), nTrunc2(%v), nAppend2(%v) != nOver(%v), nTrunc(%v), nAppend(%v). not reproducing same test?!?", nOver2, nTrunc2, nAppend2, nOver, nTrunc, nAppend))
			}

			// if failed, call t.Errorf(), it is *testing.T
			failed2 := (panicked != nil) || testFailedReport(t, tc, foll, lead, ae, nOver, nTrunc, nAppend, xOver, xTrunc, xAppend)

			if !failed2 {
				panic(fmt.Sprintf("testFailed said bad, testFailedReport said good. scenario=%v", tc.scenario))
			}
			break

		} else { // end if failed.
			greenCount++
		}
	} // end for

	// that's all folks.
	vv("end 802")
}

// Test case structure for append entries tests
type testCase struct {
	name string
	// Follower's log state
	followerLogBeg         int64
	followerLogLen         int64
	followerCommittedIndex int64 // Number of initial entries that are committed
	// Leader's log state
	leaderLogBeg         int64
	leaderLogLen         int64
	leaderCommittedIndex int64
	// Append request details
	appendBegin   int64
	appendLastIdx int64 // easier to think about, but can be negative for 0 span.
	appendEndxIdx int64 // easier to handle 0 length spans.
	appendLen     int64

	leaderFirstTerm int64
	followFirstTerm int64

	// reject if cannot apply the AE as sent.
	// if we dedup/do nothing, that should not be declared a reject.
	shouldReject bool

	// after append, followers log len
	expectedFollowLenFinal int64

	// We number the scenarios tested. You can set
	// testOneScenario at the top of the test to pick just one to re-play.
	// Also seeds the pseudo RNG, so each
	// scenario has a varied but reproducible term structure.
	scenario int64
}

func (c *testCase) String() string {
	return fmt.Sprintf(`testCase{
                  name: "%v",
        followerLogBeg: %v,
        followerLogLen: %v,
followerCommittedIndex: %v,
          leaderLogBeg: %v,
          leaderLogLen: %v,
  leaderCommittedIndex: %v,
           appendBegin: %v,
         appendLastIdx: %v,
         appendEndxIdx: %v,
             appendLen: %v,
       leaderFirstTerm: %v,
       followFirstTerm: %v,
          shouldReject: %v
expectedFollowLenFinal: %v,
              scenario: %v,
}
`, c.name, c.followerLogBeg, c.followerLogLen, c.followerCommittedIndex,
		c.leaderLogBeg, c.leaderLogLen, c.leaderCommittedIndex,
		c.appendBegin, c.appendLastIdx, c.appendEndxIdx,
		c.appendLen, c.leaderFirstTerm, c.followFirstTerm,
		c.shouldReject, c.expectedFollowLenFinal, c.scenario)
}

func getLeaderSetupForTest(t *testing.T, tc *testCase, loud bool, scenario int64) (lead *TubeSim, ae *AppendEntries, rng *mathrand.Rand) {
	defer func() {
		if tc.leaderLogLen != int64(len(lead.wal.raftLog)) {
			panic(fmt.Sprintf("lead log len = %v, but tc wants %v \n tc='%v'", len(lead.wal.raftLog), tc.leaderLogLen, tc))
		}
	}()
	// Set up test environment
	n := 2
	//cfg := NewTubeConfigTest(n, t.Name(), true)
	cfg := &TubeConfig{
		PeerServiceName: TUBE_REPLICA,
		ClusterID:       t.Name(), // "Test802_clusterID",
		ClusterSize:     n,
		TCPonly_no_TLS:  true,
		NoDisk:          true,
		HeartbeatDur:    time.Millisecond * 15,
		MinElectionDur:  time.Millisecond * 150,
		UseSimNet:       true,
		isTest:          true,
		testNum:         802,
	}
	cfg.Init(true, true)

	name := "leader_" + t.Name() // "test801_peerID"
	node := NewTubeNode(name, cfg)

	wal, err := cfg.newRaftWriteAheadLog("", false)
	panicOn(err)

	lead = &TubeSim{
		cfg:        node.cfg,
		ClusterID:  t.Name(), // "Test802_clusterID",
		PeerID:     name,
		wal:        wal,
		state:      node.newRaftState(),
		peers:      make(map[string]*RaftNodeInfo),
		choiceLoud: loud, // Enable choice tracking for single scenario
		role:       LEADER,
	}

	peerID := lead.PeerID
	_ = peerID

	// Initialize state with term
	lead.state.CurrentTerm = tc.leaderFirstTerm

	// Reset method call counters
	lead.resetMethodCounters()

	// Set up leader's log terms RLE
	//leaderLogRLE := newTermsRLE()

	// Add initial entries to leader's log
	for i := range tc.leaderLogLen {
		e := &RaftLogEntry{
			Index:  int64(i + 1),
			Term:   1,
			Ticket: &Ticket{}, // prevent crash in memwal/par.go log.
		}
		if i < tc.leaderCommittedIndex {
			e.committed = true
		}
		wal.saveRaftLogEntry(e)
	}

	if lead.choiceLoud {
		//vv("tc.leaderLogLen=%v, leaderLog = '%v'", tc.leaderLogLen, leaderLogRLE)
	}

	// Apply random term increases to both logs and get common prefix length.
	// These are pseudo random, repeatable sequences of mathrand
	// random numbers, seeded from the scenario number. The rng returns
	// that random number generator (RNG) to pass to the follower
	// doing the same thing.
	rng = rampTermsLeader(tc.scenario, lead, tc.leaderCommittedIndex, 0.5) // 50% chance of term increase

	// Create append entries request from the leader's log.
	ae = createAEfromLeader(tc.appendBegin, tc.appendEndxIdx, lead, tc)

	//ae, _ = lead.newAEtoPeer(peerID, entries)

	// for _, e := range lead.wal.raftLog {
	// 	ae.LogTermsRLE.AddTerm(e.Term)
	// }

	// inside createAEfromLeader now
	ae.PrevLogIndex = tc.appendBegin - 1
	if ae.PrevLogIndex > 0 {
		ae.PrevLogTerm = lead.wal.raftLog[ae.PrevLogIndex-1].Term
		//vv("wrote ae.PrevLogTerm = %v", ae.PrevLogTerm)
	}

	//ae.LogTermsRLE = leaderLogRLE
	lead.state.CommitIndex = tc.leaderCommittedIndex
	ae.LeaderCommitIndex = tc.leaderCommittedIndex

	// Visualize test setup if running single scenario
	if loud {
		ok := "should Accept"
		if tc.shouldReject {
			ok = "should Reject"
		}
		fmt.Printf("\nScenario %v leader state: (%v) the AE\n", scenario, ok)
		fmt.Printf("Leader state (commitIndex=%v, currentTerm=%v):\n%v",
			lead.state.CommitIndex, lead.state.CurrentTerm, visualizeLog(lead.wal.raftLog, "Leader log"))
		fmt.Printf("Leader's log terms RLE: %v\n", lead.wal.logIndex)
		fmt.Printf("Leader log length: %v, committed: %v\n\n", tc.leaderLogLen, tc.leaderCommittedIndex)
		fmt.Printf("Append request [beg %v, endx %v) Prev: [%v] term=%v:\n%v",
			tc.appendBegin, tc.appendEndxIdx, ae.PrevLogIndex, ae.PrevLogTerm, visualizeLog(ae.Entries, "Append entries"))
	}

	return
}

func getFollowerSetupForTest(t *testing.T, tc *testCase, loud bool, scenario int64, lead *TubeSim, rng *mathrand.Rand) (foll *TubeSim, lcp int64) {
	defer func() {
		if tc.followerLogLen != int64(len(foll.wal.raftLog)) {
			panic(fmt.Sprintf("follow log len = %v, but tc wants %v; scenario = %v; \n tc='%v'", len(foll.wal.raftLog), tc.followerLogLen, tc.scenario, tc))
		}
		if foll != nil && foll.wal != nil {
			cp := foll.wal.clone()
			foll.beforeAElog = cp
		}
	}()

	// Set up test environment
	n := 2
	cfg := &TubeConfig{
		PeerServiceName: TUBE_REPLICA,
		ClusterID:       t.Name(), // "test802_clusterID",
		ClusterSize:     n,
		TCPonly_no_TLS:  true,
		NoDisk:          true,
		HeartbeatDur:    time.Millisecond * 15,
		MinElectionDur:  time.Millisecond * 150,
		UseSimNet:       true,
		isTest:          true,
		testNum:         802,
	}
	cfg.Init(true, true)

	name := "follower_test801_peerID"
	node := NewTubeNode(name, cfg)

	wal, err := cfg.newRaftWriteAheadLog("", false)
	panicOn(err)

	foll = &TubeSim{
		cfg:        node.cfg,
		ClusterID:  t.Name(), // "test802_clusterID",
		PeerID:     name,
		wal:        wal,
		state:      node.newRaftState(),
		peers:      make(map[string]*RaftNodeInfo),
		choiceLoud: loud, // Enable choice tracking for single scenario
		role:       FOLLOWER,
		node:       node,
	}

	s := foll

	// Initialize state with term
	s.state.CurrentTerm = tc.followFirstTerm

	// Reset method call counters
	s.resetMethodCounters()

	// Set up initial log state for follower
	err = setupInitialFollowerLog(s, tc)
	panicOn(err)

	// Apply random term increases to both logs and get common prefix length
	lcp = rampTermsFollower(tc.scenario, lead, foll,
		tc.leaderCommittedIndex, tc.followerCommittedIndex, 0.5, rng) // 50% chance of term increase

	if loud {
		fmt.Printf("\nScenario %v follower:\n", scenario)
		fmt.Printf("Follower state (commitIndex=%v, currentTerm=%v):\n%v",
			s.state.CommitIndex, s.state.CurrentTerm, visualizeLog(s.wal.raftLog, "Follower log"))
		if s.wal != nil && s.wal.logIndex != nil {
			fmt.Printf("Follower's log terms RLE: %v\n", s.wal.logIndex)
		}

		fmt.Printf("Follower log length: %v, lcp=%v, committed: %v\n", tc.followerLogLen, lcp, tc.followerCommittedIndex)

	}

	if testTubeNodeHandleAE {
		// or test the tube.go version.
		// This redirect the replies that TubeNode.handleAppendEntries()
		// makes the host (TubeSim) test hosting code.
		node.host = foll
		node.state = foll.state
		node.wal = foll.wal
	} else {
		// test the raft_test.go version of handleAppendEntries()
		foll.host = foll
	}

	return
}

/// ramp terms for leader and follower

// rampTermsLeader applies random term increases to the leader's log
// while maintaining Raft safety rules. It uses the scenario number
// to seed the random number generator for deterministic test cases.
// Returns the length of the longest common prefix between the logs.
func rampTermsLeader(scenario int64, leader *TubeSim,
	leaderCommitIndex int64, termIncreaseProb float64) *mathrand.Rand {

	// start with empty log, so logIndex stays in sync
	newWal, err := leader.cfg.newRaftWriteAheadLog("", false)
	panicOn(err)

	// Seed random number generator with scenario number
	rng := mathrand.New(mathrand.NewSource(pseudoRNGseed + scenario))

	// Apply term increases to leader's log
	leaderLog := leader.wal.raftLog

	for i, e := range leaderLog {
		_ = i
		// Float64() -> half-open interval [0.0,1.0)
		if rng.Float64() < termIncreaseProb {
			leader.state.CurrentTerm++
		}
		e1 := e.shallowCloneTestOnly()
		e1.Term = leader.state.CurrentTerm
		if e1.Index <= leaderCommitIndex {
			e1.committed = true
		}
		newWal.saveRaftLogEntry(e1)
	}
	leader.wal = newWal

	return rng
}

// rampTermsFollower applies random term increases to follower logs
// while maintaining Raft safety rules. It uses the scenario number
// to seed the random number generator for deterministic test cases.
// Returns the length of the longest common prefix between the logs.
func rampTermsFollower(scenario int64, lead, foll *TubeSim,
	leaderCommitIndex int64, followerCommitIndex int64, termIncreaseProb float64, rng *mathrand.Rand) (commonPrefixLen int64) {

	leaderLog := lead.wal.raftLog
	followLog := foll.wal.raftLog

	// start with empty log, so logIndex stays in sync
	newFlog, err := foll.cfg.newRaftWriteAheadLog("", false)
	panicOn(err)

	leaderLogLen := int64(len(leaderLog))
	followLogLen := int64(len(followLog)) // can be longer or shorter

	if followerCommitIndex > leaderLogLen {
		panic(fmt.Sprintf("invalid test setup: followerCommitIndex(%v) > leaderLogLen(%v)", followerCommitIndex, leaderLogLen))
	}

	// Copy leader's terms exactly up to follower's commit index
	var followTermLastCommitted int64
	for i := int64(0); i < followerCommitIndex; i++ {
		if i < leaderLogLen && i < followLogLen {
			e := leaderLog[i]
			e1 := e.shallowCloneTestOnly()
			newFlog.saveRaftLogEntry(e1)
			term := e.Term
			followTermLastCommitted = term
		}
	}
	foll.state.CurrentTerm = followTermLastCommitted // could be 0

	// Apply independent term increases to follower's log after commit index.
	// This tests that the follower properly discards old leader
	// uncommitted entries that do not match the current leader's log.

	// foll.state.CurrentTerm is what at this point?

	// Start at random term >= 1 if no committed entries.
	// rng.Int63 gives non-negative int64.
	// The leader's term is the max possible;
	// A higher term entry can be overwritten
	// by a lower term, but only by a current
	// (higher current term) leader. The leader
	// replicates its own logs, which have a
	// max term of the leader's own current term.

	if followLogLen > 0 &&
		foll.state.CurrentTerm == 0 &&
		lead.state.CurrentTerm > 0 {

		// baseline. at least 1.
		randomTerm := (rng.Int63() % lead.state.CurrentTerm) + 1
		foll.state.CurrentTerm = max(1, randomTerm)
	}

	for i := followerCommitIndex + 1; i <= followLogLen; i++ {
		if rng.Float64() < termIncreaseProb &&
			foll.state.CurrentTerm < lead.state.CurrentTerm {
			foll.state.CurrentTerm++
		}
		e := followLog[i-1]
		e1 := e.shallowCloneTestOnly()
		e1.Term = foll.state.CurrentTerm
		e1.committed = false
		newFlog.saveRaftLogEntry(e1)
	}

	//vv("ramp follower: change from old follow log %v -> %v", len(followLog), len(newFlog.raftLog))

	// switch to new log
	foll.wal = newFlog
	followLog = foll.wal.raftLog
	followLogLen = int64(len(followLog))

	// Find longest common prefix length
	minLen := min(leaderLogLen, followLogLen)
	for i := int64(0); i < minLen; i++ {
		if leaderLog[i].Term == followLog[i].Term {
			commonPrefixLen++
		} else {
			break
		}
	}

	return commonPrefixLen
}

// testCaseSeq returns an iterator over test cases
func testCaseSeq(testOneScenario int64) iter.Seq2[*testCase, bool] {
	return func(yield func(*testCase, bool) bool) {
		var scenario int64 = 0
		//maxAppendLen := int64(20)

		var leaderFirstTerm int64 = 1
		var followFirstTerm int64 = 1
		var followerLogBeg int64 = 1
		var leaderLogBeg int64 = 1

		// Generate test cases for different initial log lengths.
		// Can be longer than leader due to previous crashed leader.
		for followLen := int64(0); followLen <= maxFollowLogLen; followLen++ {
			// Test different leader log lengths.
			// follower can have longer if new leader has taken over
			// before old leader replicated much.
			for leaderLen := int64(0); leaderLen <= followLen+addLeaderLogLen; leaderLen++ {
				// Test different commit points for leader
				for leaderCommitted := int64(0); leaderCommitted <= leaderLen; leaderCommitted++ {
					// Test different commit points for follower.
					// Can never go past leader commited of course.
					for followerCommitted := int64(0); followerCommitted <= followLen && followerCommitted <= leaderCommitted; followerCommitted++ {
						// Test appending at different positions
						for appendBegin := int64(0); appendBegin <= leaderLen; appendBegin++ {
							for appendLen := int64(0); appendLen <= leaderLen-appendBegin; appendLen++ {
								if appendBegin == 0 && appendLen != 0 {
									// heartbeats at 0 are okay.
									continue
								}
								//vv("appendLen = %v, appendBegin = %v", appendLen, appendBegin)

								// possibly negative for 0 length appends from 0.
								appendLastIdx := appendBegin + appendLen - 1
								// always >= 0. e.g. [0,0) is empty set.
								appendEndxIdx := appendLastIdx + 1
								if appendLastIdx > leaderLen {
									// we could test that this is rejected,
									// but the follower would have to have
									// a way to tell that the leader
									// lying to it... maybe it is from
									// a stale old leader?
									// We could cross reference leaderLogRLE
									// I suppose, but I'd like to keep that
									// mechanism optional in case it turns out
									// to be too heavy to use regularly.
									// TODO: add in checks that old stale
									// terms result in rejection of this
									// scenario. We'll get easier tests going 1st.
									continue
								}

								scenario++
								if testOneScenario > 0 && scenario != testOneScenario {
									continue
								}

								tc := &testCase{
									name: fmt.Sprintf("scenario=%v: followLen=%v,followerCommitted=%v,leaderLen=%v,leaderCommitted=%v,append[%v:%v)",
										scenario, followLen, followerCommitted, leaderLen, leaderCommitted, appendBegin, appendEndxIdx),
									followerLogBeg:         followerLogBeg,
									leaderLogBeg:           leaderLogBeg,
									leaderFirstTerm:        leaderFirstTerm,
									followFirstTerm:        followFirstTerm,
									followerLogLen:         followLen,
									followerCommittedIndex: followerCommitted,
									leaderLogLen:           leaderLen,
									leaderCommittedIndex:   leaderCommitted,
									appendBegin:            appendBegin,
									appendLastIdx:          appendLastIdx,
									appendLen:              appendLen,
									//appendEndxIdx == appendLastIdx + 1,
									appendEndxIdx: appendEndxIdx,
									scenario:      scenario,
								}

								if !yield(tc, true) {
									return
								}

							}
						}
					}
				}
			}
		}
	}
}

func testFailedReport(t *testing.T, tc *testCase, s, lead *TubeSim, ae *AppendEntries, nOver, nTrunc, nAppend, xOver, xTrunc, xAppend int64) (failed bool) {

	foll := s

	var didReject bool
	if foll.ackAERejectCount > 0 {
		didReject = true
	}

	if got, want := didReject, tc.shouldReject; got != want {
		failed = true
		if tc.shouldReject {
			t.Fatalf("\n *** scenario %v: expected reject from handleAppendEntries, but it was OK with request.", tc.scenario)
		} else {
			t.Fatalf("\n *** scenario %v: expected okay (handleAppendEntries to accept), but it rejected", tc.scenario)
		}
		return
	}

	if !tc.shouldReject {
		// these only good if the AE was accepted, not rejected

		switch {
		case nOver != xOver:
			failed = true
			t.Fatalf("scenario %v: want xOver = %v, but got nOver=%v   full set: (nOver=%v; xOver=%v, nAppend=%v; xAppend=%v; nTrunc=%v; xTrunc=%v)", tc.scenario, xOver, nOver, nOver, xOver, nAppend, xAppend, nTrunc, xTrunc)
		case nTrunc != xTrunc:
			failed = true
			t.Fatalf("scenario %v: want xTrunc = %v, but got nTrunc=%v  full set: (nOver=%v; xOver=%v, nAppend=%v; xAppend=%v; nTrunc=%v; xTrunc=%v)", tc.scenario, xTrunc, nTrunc, nOver, xOver, nAppend, xAppend, nTrunc, xTrunc)
		case nAppend != xAppend:
			failed = true
			t.Fatalf("scenario %v: want xAppend = %v, but got nAppend=%v  full set: (nOver=%v; xOver=%v, nAppend=%v; xAppend=%v; nTrunc=%v; xTrunc=%v)", tc.scenario, xAppend, nAppend, nOver, xOver, nAppend, xAppend, nTrunc, xTrunc)
		}
	}

	scenario := tc.scenario
	// Verify results
	if int64(len(foll.wal.raftLog)) != tc.expectedFollowLenFinal {
		failed = true
		t.Fatalf("\n *** scenario %v: Expected post append follower log length %v, got %v\n%v",
			scenario, tc.expectedFollowLenFinal, len(foll.wal.raftLog), foll.methodCallCounts())
	}

	// Verify no committed entries were overwritten
	for i := int64(0); i < tc.followerCommittedIndex; i++ {
		if i >= int64(len(foll.wal.raftLog)) {
			failed = true
			t.Fatalf("\n *** scenario %v: Expected committed entry at index %v to exist\n%v",
				scenario, i, foll.methodCallCounts())
			continue
		}
		entry := foll.wal.raftLog[i]
		if !entry.committed {
			failed = true
			t.Fatalf("Scenario %v: Entry at index %v should still be committed\n%v",
				scenario, i, foll.methodCallCounts())
		}
		xterm := foll.beforeAElog.raftLog[i].Term
		if entry.Term != xterm {
			failed = true
			t.Fatalf("\n *** scenario %v: Committed entry at index %v: expected term %v, got %v\n%v",
				scenario, i, xterm, entry.Term, foll.methodCallCounts())
		}

		expectedVal := []byte(fmt.Sprintf("value%v", i+1))
		if !bytes.Equal(entry.Ticket.Val, expectedVal) {
			failed = true
			t.Fatalf("\n *** cenario %v: Committed entry at index %v: expected value %v, got %v\n%v",
				scenario, i, expectedVal, entry.Ticket.Val, foll.methodCallCounts())
		}
	}

	// Verify no identical entries were overwritten
	for i := tc.followerCommittedIndex; i < tc.followerLogLen; i++ {
		if i >= int64(len(foll.wal.raftLog)) {
			continue // Entry might have been truncated
		}
		entry := foll.wal.raftLog[i]
		if entry.testWrote {
			failed = true
			t.Fatalf("Scenario %v: Entry at index %v was overwritten despite being identical\n%v",
				scenario, i, foll.methodCallCounts())
		}
	}
	return
}

// tc.scenario has the scenario number
func testFailed(tc *testCase, s *TubeSim, ae *AppendEntries, loud bool, nOver, nTrunc, nAppend, xOver, xTrunc, xAppend int64) (failed bool) {

	foll := s
	var didReject bool
	if foll.ackAERejectCount > 0 {
		didReject = true
	}

	if got, want := didReject, tc.shouldReject; got != want {
		failed = true
		if loud {
			fmt.Printf("\n *** scenario %v: expected reject from handleAppendEntries, but it was OK with request.\n", tc.scenario)
		} else {
			fmt.Printf("\n *** scenario %v: expected okay (handleAppendEntries to accept), but it rejected.\n", tc.scenario)
		}
	}

	if !tc.shouldReject {
		// on reject these don't apply.
		switch {
		case nOver != xOver:
			failed = true
			if loud {
				vv("expected xOver = %v, got nOver = %v\n", xOver, nOver)
			}
		case nTrunc != xTrunc:
			failed = true
			if loud {
				vv("expected xTrunc = %v, got nTrunc = %v\n", xTrunc, nTrunc)
			}
		case nAppend != xAppend:
			failed = true
			if loud {
				vv("expected xAppend = %v, got nAppend = %v ; tc.expectedFollowLenFinal = %v; tc.appendLastIdx = %v ; tc.appendLen = %v; tc = '%#v'\n", xAppend, nAppend, tc.expectedFollowLenFinal, tc.appendLastIdx, tc.appendLen, tc)
			}
		}
	}

	// Visualize results if running single scenario
	if loud {
		fmt.Printf("Follower final state:\n%v", visualizeLog(foll.wal.raftLog, "Follower final log"))
		fmt.Printf("%v", foll.methodCallCounts())
	}

	// Check for failures and enable choice tracking if any found
	if int64(len(foll.wal.raftLog)) != tc.expectedFollowLenFinal {
		if loud {
			fmt.Printf("len(foll.wal.raftLog)=%v != tc.expectedFollowLenFinal(%v)", len(foll.wal.raftLog), tc.expectedFollowLenFinal)
			failed = true
		}
	}
	for i := int64(0); i < tc.followerCommittedIndex; i++ {
		if i >= int64(len(foll.wal.raftLog)) {
			if loud {
				fmt.Printf("len(foll.wal.raftLog)=%v but tc.followerCommittedIndex(%v)", len(foll.wal.raftLog), tc.followerCommittedIndex)
			}
			failed = true
			break
		}
		entry := foll.wal.raftLog[i]
		xterm := foll.beforeAElog.raftLog[i].Term
		_ = xterm
		if !entry.committed { // || entry.Term != xterm {
			if loud {
				fmt.Printf("entry(%v) was already commited, but became uncommited", entry)
			}
			failed = true
			break
		}
		// we are not copying tickets, bah.
		//expectedVal := []byte(fmt.Sprintf("value%v", i+1))
		//if !bytes.Equal(entry.Ticket.Val, expectedVal) {
		//	failed = true
		//	break
		//}
	}
	for i := tc.followerCommittedIndex; i < tc.followerLogLen; i++ {
		if i >= int64(len(foll.wal.raftLog)) {
			continue
		}
		entry := foll.wal.raftLog[i]
		if entry.testWrote {
			failed = true
			break
		}
	}
	return failed
}

func verifySetupAgainstTestSpec(t *testing.T, tc *testCase, lcp int64, lead, foll *TubeSim, ae *AppendEntries) {
	flog := foll.wal.raftLog
	floglen := int64(len(flog))
	llog := lead.wal.raftLog
	lloglen := int64(len(llog))

	// check that the RLE indexes are up to date; scenario 1993 example
	f := newTermsRLE()
	for _, e := range flog {
		f.AddTerm(e.Term)
	}
	if !f.Equal(foll.wal.logIndex) {
		got := foll.wal.logIndex
		want := f
		t.Fatalf("foll.wal.logIndex: got %v, want %v", got, want)
	}

	// same on leader
	lrle := newTermsRLE()
	for _, e := range llog {
		lrle.AddTerm(e.Term)
	}
	if !lrle.Equal(lead.wal.logIndex) {
		got := lead.wal.logIndex
		want := lrle
		t.Fatalf("lead.wal.logIndex: got %v, want %v", got, want)
	}

	aeSz := int64(len(ae.Entries))
	isHB := aeSz == 0 // is heartbeat?
	_ = isHB
	if got, want := floglen, tc.followerLogLen; got != want {
		t.Fatalf("floglen: got %v, want %v", got, want)
	}
	if got, want := lloglen, tc.leaderLogLen; got != want {
		t.Fatalf("lloglen: got %v, want %v", got, want)
	}
	if got, want := foll.state.CommitIndex, tc.followerCommittedIndex; got != want {
		t.Fatalf("foll.commitIndex: got %v, want %v", got, want)
	}
	if got, want := lead.state.CommitIndex, tc.leaderCommittedIndex; got != want {
		t.Fatalf("lead.commitIndex: got %v, want %v", got, want)
	}
	if got, want := aeSz, tc.appendEndxIdx-tc.appendBegin; got != want {
		t.Fatalf("aeSz: %v; tc.appendEndxIdx - tc.appendBegin= %v", got, want)
	}
	if got, want := aeSz, tc.appendLen; got != want {
		t.Fatalf("aeSz: got %v; tc.appendLen: %v", got, want)
	}
	if got, want := tc.appendEndxIdx, tc.appendLastIdx+1; got != want {
		t.Fatalf("appendEndxIdx: %v; tc.appendLastIdx+1: %v", got, want)
	}
	// check the AppendEntries
	if !isHB { // aeSz > 0
		e0 := ae.Entries[0]
		eN := ae.Entries[aeSz-1]
		if got, want := e0.Index, tc.appendBegin; got != want {
			t.Fatalf("ae.Entries[0].Index: %v; tc.appendBegin: %v", got, want)
		}
		if got, want := eN.Index, tc.appendLastIdx; got != want {
			t.Fatalf("last ae.Entries[%v].Index: %v; tc.appendLastIdx: %v", aeSz-1, got, want)
		}
	}
}

func createAEfromLeader(begin, endx int64, leader *TubeSim, tc *testCase) *AppendEntries {

	// already fills in LogTermsRLE, CommitIndex properly
	ae := leader.newAE()

	//vv("createAEfromLeader begin=%v; endx=%v; leader log len='%v'; scenario = %v", begin, endx, len(leader.wal.raftLog), tc.scenario)
	var entries []*RaftLogEntry
	// Note: begin and endx are both 1-based log indices
	for i := begin; i < endx; i++ {
		// get the terms cloned from leader.
		e := leader.wal.raftLog[i-1].shallowCloneTestOnly()
		entries = append(entries, e)
	}
	//vv("createAEfromLeader len %v for beg=%v, endx=%v, should be %v", len(entries), begin, endx, endx-begin)
	ae.Entries = entries

	if begin > 1 {
		prev := leader.wal.raftLog[begin-2]
		ae.PrevLogIndex = prev.Index
		ae.PrevLogTerm = prev.Term
	}
	return ae
}

func differ(a, b *RaftLogEntry) bool {
	return a.Index != b.Index || a.Term != b.Term
}
func logsDiffer(awal, bwal *raftWriteAheadLog) bool {
	alog := awal.raftLog
	blog := bwal.raftLog
	if len(alog) != len(blog) {
		return true
	}
	for i := range alog {
		if differ(alog[i], blog[i]) {
			return true
		}
	}
	return false
}

func aeUpdatesFollNoGap(ae *AppendEntries, foll, lead *TubeSim) (hbDetectsFollowerNeedsDataReject bool, nOver int64, gap, tooOld bool, nTrunc, nAppend, xFollowLenAfter int64) {
	szAE := int64(len(ae.Entries))

	// we were getting logs still not consistent with
	// their logIndexes
	foll.wal.panicOnStaleIndex()
	lead.wal.panicOnStaleIndex()

	var f1, fN int64
	flog := foll.wal.raftLog
	nFoll := int64(len(flog))
	nLead := int64(len(lead.wal.raftLog))
	_ = nLead
	lcp := lead.wal.logIndex.lcp(foll.wal.logIndex)

	xFollowLenAfter = nFoll // default not change
	//vv("szAE = %v", szAE)
	if szAE == 0 {
		// heartbeats never update a log
		// but this doesn't reject when we
		// have data diffs between the lead and foll,
		// so do a bit more!
		if nLead > 0 {
			if lcp < nLead {
				hbDetectsFollowerNeedsDataReject = lcp < nFoll
				//vv("hbDetectsFollowerNeedsDataReject = %v", hbDetectsFollowerNeedsDataReject)
			}
		}
		return
	}
	ae1 := ae.Entries[0].Index
	aeN := ae.Entries[szAE-1].Index

	if nFoll == 0 {
		// with an empty follower log,
		// the only update we can take must start at index 1.
		// In Raft, logs never have holes.
		// BUT: after state compression, might no longer hold.
		// TODO: might need to adjust after adding state compression.
		// not sure how that mechanism will work, but this
		// could still be correct if we subtract a log index
		// of the compressed state first.
		if ae1 == 1 {
			nOver = 0
			nAppend = szAE
			xFollowLenAfter = aeN
			return
		}

		gap = true // too big a gap, > 1 on empty log
		return
	}
	// INVAR: nFoll > 0
	f1 = flog[0].Index // is usually 1, but maybe not after state compression
	fN = flog[nFoll-1].Index
	if fN != nFoll {
		panic(fmt.Sprintf("corrupt flog detected. fN(%v) != len(flog) == %v", fN, nFoll))
	}

	// handle gaps first! reject AE we are never going to apply
	// effecting the prevLogIndex/term check, never apply
	// when there really is a mismatch in logs long before
	// the current AE, yikes!

	var lcp2 int64
	for i, el := range lead.wal.raftLog {
		if i >= len(flog) {
			break
		}
		ef := foll.wal.raftLog[i]
		if ef.Index != el.Index || int64(i+1) != ef.Index {
			panic(fmt.Sprintf("bad log caught! ef.Index(%v) should be %v should be el.Indiex(%v)", ef.Index, i+1, el.Index))
		}
		if ef.Index == el.Index && ef.Term == el.Term {
			lcp2++
		} else {
			break // must stop at first mismatch.
		}
	}
	if lcp2 != lcp {
		// panic: lcp(0) != lcp2(2)
		visualizeLog(lead.wal.raftLog, "Leader log")
		visualizeLog(foll.wal.raftLog, "Follow log")
		lcp3 := lead.wal.logIndex.lcp(foll.wal.logIndex)
		panic(fmt.Sprintf("lcp(%v) != lcp2(%v)  [ lcp3 = %v should match lcp]", lcp, lcp2, lcp3))
	}
	if ae1 > lcp+1 {
		gap = true
		return
	}

	// handle apply-able AE
	if aeN < f1 {
		// update is entirely before our first log entry.
		// does not update us
		tooOld = true
		return
	}
	if ae1 > fN+1 {
		gap = true
		return
	}
	// INVAR: ae1 <= fN
	//        aeN >= f1
	// we have some intersection, is there a change?
	dup := 0
	for _, e := range ae.Entries {
		aei := e.Index
		if f1 <= aei && aei <= fN {
			// could apply it, but is it an update?
			if differ(e, flog[aei-1]) {
				//vv("differenece: Ae[%v]='%v'; versus Flog[%v]='%v'")
				break
			} else {
				dup++
			}
		}
	}
	entries := ae.Entries[dup:]
	szAE = int64(len(entries))
	if szAE == 0 {
		// all dups, no change
		return
	}
	ae1 = entries[0].Index
	aeN = entries[szAE-1].Index
	//vv("szAE=%v; f=[%v, %v]; ae[%v, %v]", szAE, f1, fN, ae1, aeN)
	switch {
	case aeN < fN:
		//vv("case aeN <= fN")
		nTrunc = fN - aeN
		nOver = szAE
		nAppend = 0
		xFollowLenAfter = aeN
	case aeN == fN:
		//vv("case aeN == fN")
		nTrunc = 0
		nOver = szAE
		nAppend = 0
		xFollowLenAfter = aeN
	case ae1 > fN: // AE extends
		//vv("case ae1 > fN")
		// can be no gaps, were handled above.
		nTrunc = 0
		nOver = 0
		nAppend = szAE
		xFollowLenAfter = aeN
	case aeN > fN: // AE overlaps
		//vv("case aeN > fN")
		nTrunc = 0
		nOver = fN - ae1 + 1
		nAppend = szAE - nOver
		xFollowLenAfter = aeN
	case ae1 > fN:
		//vv("case ae1 > fN")
		nOver = 0
		nTrunc = 0
		nAppend = szAE
		xFollowLenAfter = aeN
	default:
		panic(fmt.Sprintf("should have hit all cases above. ae1=%v; aeN=%v; fN=%v", ae1, aeN, fN))
	}

	return
}
