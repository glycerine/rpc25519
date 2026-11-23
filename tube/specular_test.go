package tube

// Regression tests for the bugs found by Specular
// (see ../bug-report.md). Each test first reproduced
// its bug (red), then went green with the fix.

import (
	"fmt"
	"testing"
	"time"
)

// ackCaptureHost lets the real TubeNode.handleAppendEntries
// run against a TubeSim host, and captures the ack
// it would have sent to the leader.
type ackCaptureHost struct {
	hoster
	acks []*AppendEntriesAck
}

func (h *ackCaptureHost) ackAE(ack *AppendEntriesAck, ae *AppendEntries, sendAck bool) {
	if sendAck {
		h.acks = append(h.acks, ack)
	}
}

// specularTestCfg returns a config for a hand-assembled
// TubeNode that is not part of a running cluster.
// testNum 802 keeps the node from using the
// regular test channels (it is not a cluster test).
func specularTestCfg(t *testing.T, clusterSize int) *TubeConfig {
	cfg := &TubeConfig{
		PeerServiceName: TUBE_REPLICA,
		ClusterID:       t.Name(),
		ClusterSize:     clusterSize,
		TCPonly_no_TLS:  true,
		NoDisk:          true,
		HeartbeatDur:    time.Millisecond * 15,
		MinElectionDur:  time.Millisecond * 150,
		UseSimNet:       true,
		isTest:          true,
		testNum:         802,
	}
	cfg.Init(true, true)
	return cfg
}

func specularMC(node *TubeNode, names ...string) *MemberConfig {
	mc := node.NewMemberConfig("specular_test")
	for _, name := range names {
		mc.PeerNames.Set(name, &PeerDetail{Name: name, PeerID: name + "_id", PeerServiceName: TUBE_REPLICA})
	}
	return mc
}

// specularNode builds a standalone TubeNode holding
// a log with the given terms (index i+1 has terms[i]).
func specularNode(t *testing.T, name string, role RaftRole, curTerm int64, terms []int64, members ...string) *TubeNode {
	cfg := specularTestCfg(t, len(members))
	node := NewTubeNode(name, cfg)
	node.PeerID = name + "_id"
	node.role = role
	wal, err := node.cfg.newRaftWriteAheadLog("", false)
	panicOn(err)
	node.wal = wal
	node.state = node.newRaftState()
	node.state.CurrentTerm = curTerm
	for i, term := range terms {
		e := &RaftLogEntry{
			Index:  int64(i + 1),
			Term:   term,
			Ticket: &Ticket{Op: NOOP, LogIndex: int64(i + 1), Term: term,
				Desc: fmt.Sprintf("%v entry %v term %v", name, i+1, term)},
		}
		_, err := wal.saveRaftLogEntry(e)
		panicOn(err)
	}
	node.state.MC = specularMC(node, members...)
	return node
}

// specularMarkApplied records that entries 1..idx
// are committed and applied.
func specularMarkApplied(node *TubeNode, idx int64) {
	for i := int64(1); i <= idx; i++ {
		e, err := node.wal.GetEntry(i)
		panicOn(err)
		e.committed = true
		e.Ticket.Committed = true
		node.state.LastApplied = i
		node.state.LastAppliedTerm = e.Term
	}
}

// Bug 1 (Critical): the commit rule counted a follower's
// divergent log suffix as replicated.
//
// n3 led term 1 and appended uncommitted entries 2..4.
// n2 wins term 2 (log [t1]) and appends its no-op at
// index 2 (log [t1, t2]). A capped catch-up AE carries
// only entry 1. n3 accepts it ("nothing new after
// clipping") and its ack reports PeerLogLastIndex = 4,
// its whole divergent log. The leader must not take
// that as MatchIndex, or it commits index 2 on the
// strength of {n2, n3} when only n2 holds it.
func Test1001_commit_does_not_count_follower_divergent_suffix(t *testing.T) {

	members := []string{"n1", "n2", "n3"}

	// leader n2, term 2: log [t1, t2(no-op)]
	lead := specularNode(t, "n2", LEADER, 2, []int64{1, 2}, members...)
	lead.state.CommitIndex = 1
	lead.state.CommitIndexEntryTerm = 1
	specularMarkApplied(lead, 1)
	lead.leaderID = lead.PeerID
	lead.leaderName = lead.name

	// n1 has only entry 1.
	n1 := lead.newRaftNodeInfo("n1_id", "n1", TUBE_REPLICA, "")
	n1.MatchIndex = 1
	n1.LargestCommonRaftIndex = 1
	lead.peers["n1_id"] = n1

	// fresh record for n3, as after an election.
	n3info := lead.newRaftNodeInfo("n3_id", "n3", TUBE_REPLICA, "")
	lead.peers["n3_id"] = n3info

	// deposed n3, now a term 2 follower, with its
	// uncommitted term 1 suffix at 2..4.
	foll := specularNode(t, "n3", FOLLOWER, 2, []int64{1, 1, 1, 1}, members...)
	foll.state.CommitIndex = 1
	foll.state.CommitIndexEntryTerm = 1
	specularMarkApplied(foll, 1)
	host := &ackCaptureHost{hoster: &TubeSim{
		cfg:   foll.cfg,
		wal:   foll.wal,
		state: foll.state,
		role:  FOLLOWER,
		node:  foll,
		peers: make(map[string]*RaftNodeInfo),
	}}
	foll.host = host

	// the capped catch-up AE: prev 0, entries [1].
	e1, err := lead.wal.GetEntry(1)
	panicOn(err)
	ae := &AppendEntries{
		ClusterID:                  lead.ClusterID,
		FromPeerID:                 lead.PeerID,
		FromPeerName:               lead.name,
		FromPeerServiceName:        TUBE_REPLICA,
		AEID:                       "bug1-ae",
		LeaderTerm:                 2,
		LeaderID:                   lead.PeerID,
		LeaderName:                 lead.name,
		PrevLogIndex:               0,
		PrevLogTerm:                0,
		Entries:                    []*RaftLogEntry{e1},
		LogTermsRLE:                lead.wal.getTermsRLE(),
		LeaderCommitIndex:          1,
		LeaderCommitIndexEntryTerm: 1,
		LeaderLLI:                  2,
		LeaderLLT:                  2,
	}
	foll.handleAppendEntries(ae, true)

	if len(host.acks) != 1 {
		t.Fatalf("expected one ack from follower, got %v", len(host.acks))
	}
	ack := host.acks[0]
	vv("follower ack: Rejected=%v, LCRI=%v, PeerLogLastIndex=%v, reason='%v'", ack.Rejected, ack.LargestCommonRaftIndex, ack.PeerLogLastIndex, ack.RejectReason)
	if ack.LargestCommonRaftIndex > 1 {
		t.Fatalf("follower reports LargestCommonRaftIndex %v but only index 1 matches the leader", ack.LargestCommonRaftIndex)
	}

	lead.handleAppendEntriesAck(ack, nil)

	if mi := lead.peers["n3_id"].MatchIndex; mi > 1 {
		t.Fatalf("Bug 1: leader set MatchIndex[n3] = %v, but n3 matches the leader's log only through index 1", mi)
	}
	if ci := lead.state.CommitIndex; ci > 1 {
		t.Fatalf("Bug 1: leader committed index %v that only the leader holds", ci)
	}
}

// Bug 2 (Critical): a stale snapshot from the current
// leader and term rolled a follower back below entries
// it had already acknowledged.
//
// The leader answers two gap rejections on two circuits:
// sid1 (log through 3), then sid2 (log through 4). They
// arrive reordered. After installing sid2 the follower
// acks last=4. Installing the older sid1 must not
// truncate the follower back to 3.
func Test1002_stale_same_term_snapshot_does_not_roll_back_follower(t *testing.T) {

	members := []string{"n1", "n2", "n3"}

	lead := specularNode(t, "n2", LEADER, 1, []int64{1, 1, 1}, members...)
	lead.state.CommitIndex = 1
	lead.state.CommitIndexEntryTerm = 1
	specularMarkApplied(lead, 1)
	lead.leaderID = lead.PeerID
	lead.leaderName = lead.name

	sid1 := lead.getStateSnapshot(false)

	e4 := &RaftLogEntry{Index: 4, Term: 1,
		Ticket: &Ticket{Op: NOOP, LogIndex: 4, Term: 1, Desc: "n2 entry 4"}}
	_, err := lead.wal.saveRaftLogEntry(e4)
	panicOn(err)
	sid2 := lead.getStateSnapshot(false)

	// follower n1 restarted with an empty log, term 1.
	foll := specularNode(t, "n1", FOLLOWER, 1, nil, members...)
	foll.leaderID = lead.PeerID
	foll.leaderName = lead.name
	host := &ackCaptureHost{hoster: foll}
	foll.host = host

	foll.applyNewStateSnapshot(sid2, "test sid2", true)
	if lli := foll.wal.LastLogIndex(); lli != 4 {
		t.Fatalf("after sid2 expected follower last index 4, got %v", lli)
	}
	if n := len(host.acks); n == 0 || host.acks[n-1].PeerLogLastIndex != 4 {
		t.Fatalf("expected follower to ack last=4 after sid2; acks=%v", len(host.acks))
	}
	ci0 := foll.state.CommitIndex
	la0 := foll.state.LastApplied

	// the stale, older snapshot arrives last.
	foll.applyNewStateSnapshot(sid1, "test sid1 (stale)", true)

	if lli := foll.wal.LastLogIndex(); lli < 4 {
		t.Fatalf("Bug 2: stale snapshot rolled follower log back to %v, below acknowledged index 4", lli)
	}
	if foll.state.CommitIndex < ci0 || foll.state.LastApplied < la0 {
		t.Fatalf("Bug 2: stale snapshot moved CommitIndex %v -> %v / LastApplied %v -> %v backwards", ci0, foll.state.CommitIndex, la0, foll.state.LastApplied)
	}
	// the leader still gets an answer reflecting our real log.
	last := host.acks[len(host.acks)-1]
	if last.Rejected || last.PeerLogLastIndex != 4 || last.LargestCommonRaftIndex != 3 {
		t.Fatalf("after stale snapshot expected accepted ack with last=4, lcri=3; got rejected=%v last=%v lcri=%v", last.Rejected, last.PeerLogLastIndex, last.LargestCommonRaftIndex)
	}
}
