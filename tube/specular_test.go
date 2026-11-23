package tube

// Regression tests for the bugs found by Specular
// (see ../bug-report.md). Each test first reproduced
// its bug (red), then went green with the fix.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glycerine/blake3"
	"github.com/glycerine/idem"
	rpc "github.com/glycerine/rpc25519"
	"github.com/glycerine/rpc25519/tube/leased"
	"github.com/glycerine/yogadb"
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
			Index: int64(i + 1),
			Term:  term,
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

// specularLeaseWrite builds a leased WRITE ticket from client c1.
func specularLeaseWrite(id string, idx int64, logTm, until time.Time) *Ticket {
	return &Ticket{
		TicketID:        id,
		Op:              WRITE,
		Table:           "t",
		Key:             "czar",
		Val:             Val([]byte("c1 is czar")),
		Leasor:          "c1",
		FromID:          "c1_id",
		FromName:        "c1",
		LeaseRequestDur: until.Sub(logTm),
		LeaseUntilTm:    until,
		RaftLogEntryTm:  logTm,
		LogIndex:        idx,
		Term:            1,
	}
}

// specularApplyAll commits and applies everything in node's log.
func specularApplyAll(node *TubeNode) {
	node.state.CommitIndex = node.wal.LastLogIndex()
	node.state.CommitIndexEntryTerm = node.wal.LastLogTerm()
	node.commitWhatWeCan(false)
}

// Bug 3 (High): a rejected lease write mutated the shared
// in-memory log entry (writeFailedSetCurrentVal overwrites
// LeaseUntilTm, Leasor, ...). A lagging follower then
// received the mutated command from the leader's in-memory
// log and applied it differently, so replicas diverged at
// the same applied index.
func Test1003_rejected_lease_write_does_not_mutate_log_entry(t *testing.T) {

	members := []string{"n1", "n2"}
	lead := specularNode(t, "n1", FOLLOWER, 1, []int64{1}, members...)
	specularMarkApplied(lead, 1)
	lead.cfg.ClockDriftBound = 500 * time.Millisecond

	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	until := t0.Add(2 * time.Second)

	// index 2: c1 takes the lease until `until`.
	// index 3: c1 "renews" with the same LeaseUntilTm: rejected.
	for _, tkt := range []*Ticket{
		specularLeaseWrite("w2", 2, t0, until),
		specularLeaseWrite("w3", 3, t0.Add(time.Second), until),
	} {
		e := &RaftLogEntry{Index: tkt.LogIndex, Term: 1, Tm: tkt.RaftLogEntryTm, Ticket: tkt}
		_, err := lead.wal.saveRaftLogEntry(e)
		panicOn(err)
	}
	specularApplyAll(lead)

	e3, err := lead.wal.GetEntry(3)
	panicOn(err)
	if !e3.Ticket.LeaseUntilTm.Equal(until) || e3.Ticket.Leasor != "c1" {
		t.Errorf("Bug 3: applying the rejected write mutated the log entry: LeaseUntilTm %v -> %v", until, e3.Ticket.LeaseUntilTm)
	}

	// the lagging follower catches up from the leader's
	// in-memory log (as a catch-up AE would carry it).
	foll := specularNode(t, "n2", FOLLOWER, 1, []int64{1}, members...)
	specularMarkApplied(foll, 1)
	foll.cfg.ClockDriftBound = 500 * time.Millisecond
	sendThese, _ := lead.wal.GetAllEntriesFrom(2)
	for _, e := range sendThese {
		bts, err := e.MarshalMsg(nil)
		panicOn(err)
		wire := &RaftLogEntry{}
		_, err = wire.UnmarshalMsg(bts)
		panicOn(err)
		_, err = foll.wal.saveRaftLogEntry(wire)
		panicOn(err)
	}
	specularApplyAll(foll)

	leafL, err := lead.state.KVstore.GetLeaf("t", "czar", false)
	panicOn(err)
	leafF, err := foll.state.KVstore.GetLeaf("t", "czar", false)
	panicOn(err)
	if leafL.Version != leafF.Version || !leafL.LeaseUntilTm.Equal(leafF.LeaseUntilTm) {
		t.Fatalf("Bug 3: replicas diverged at applied index 3: leader {ver %v, until %v} vs follower {ver %v, until %v}",
			leafL.Version, leafL.LeaseUntilTm, leafF.Version, leafF.LeaseUntilTm)
	}
}

// Bug 4 (High): a leader-local lease read deleted expired
// auto-delete keys outside the log (GetLeaf with autoDelete
// ran a write transaction using the local clock), so the
// leader's store lost a key that the followers still held
// at the same applied index.
func Test1004_read_does_not_auto_delete_outside_log(t *testing.T) {

	node := specularNode(t, "n1", LEADER, 1, []int64{1}, "n1", "n2")
	specularMarkApplied(node, 1)

	// a leased, auto-delete key whose lease has long expired.
	t0 := time.Now().Add(-time.Hour)
	w := specularLeaseWrite("w2", 2, t0, t0.Add(time.Second))
	w.LeaseAutoDel = true
	_, err := node.wal.saveRaftLogEntry(&RaftLogEntry{Index: 2, Term: 1, Tm: t0, Ticket: w})
	panicOn(err)
	specularApplyAll(node)

	if _, err := node.state.KVstore.GetLeaf("t", "czar", false); err != nil {
		t.Fatalf("setup: leased write not applied: %v", err)
	}

	// leader-local read (leaderServedLocalRead calls doReadKey)
	rd := &Ticket{TicketID: "r1", Op: READ, Table: "t", Key: "czar"}
	node.doReadKey(rd)
	// and a range scan, which has the same hazard.
	rg := &Ticket{TicketID: "r2", Op: READ_KEYRANGE, Table: "t", Key: "", KeyEndx: ""}
	node.doReadKeyRange(rg)

	if _, err := node.state.KVstore.GetLeaf("t", "czar", false); err != nil {
		t.Fatalf("Bug 4: a read mutated replicated state: expired key deleted outside the log: %v", err)
	}
	if _, err := node.state.KVstore.GetLeaf("dead", "czar", false); err == nil {
		t.Fatalf("Bug 4: a read moved the expired key to the dead table outside the log")
	}
	// readers still see an expired auto-delete key as gone.
	if rd.Err == nil {
		t.Fatalf("expired auto-delete key should read as not found, got val '%v'", string(rd.Val))
	}
	if rg.KeyValRangeScan != nil && rg.KeyValRangeScan.Size() != 0 {
		t.Fatalf("expired auto-delete key should be hidden from range scans")
	}
}

// specularSessNew builds a SESS_NEW ticket for client c1.
func specularSessNew(sessID string, idx int64, logTm time.Time) *Ticket {
	return &Ticket{
		TicketID:       "sess-" + sessID,
		Op:             SESS_NEW,
		FromID:         "c1_id",
		FromName:       "c1",
		RaftLogEntryTm: logTm,
		LogIndex:       idx,
		Term:           1,
		NewSessReq: &Session{
			SessionID:               sessID,
			CliName:                 "c1",
			CliPeerID:               "c1_id",
			SessRequestedInitialDur: 24 * time.Hour,
			SessionIndexEndxTm:      logTm.Add(24 * time.Hour),
			SessionAssignedIndex:    idx,
		},
	}
}

// Bug 5 (High): a client retry carrying the same session
// serial was admitted while the first copy was still in
// flight, and at apply time both copies executed: the
// write took effect twice and the cached reply for that
// serial was overwritten by the second execution.
func Test1005_same_serial_retry_executes_once(t *testing.T) {

	node := specularNode(t, "n1", FOLLOWER, 1, []int64{1}, "n1", "n2")
	specularMarkApplied(node, 1)
	node.cfg.ClockDriftBound = 500 * time.Millisecond

	t0 := time.Now()
	first := specularLeaseWrite("w-first", 3, t0, t0.Add(5*time.Second))
	// the designed retry: same serial, new TicketID, stamped later.
	retry := specularLeaseWrite("w-retry", 4, t0.Add(time.Second), t0.Add(6*time.Second))
	for _, w := range []*Ticket{first, retry} {
		w.SessionID = "s1"
		w.SessionSerial = 1
	}
	for _, tkt := range []*Ticket{specularSessNew("s1", 2, t0), first, retry} {
		_, err := node.wal.saveRaftLogEntry(&RaftLogEntry{Index: tkt.LogIndex, Term: 1, Tm: tkt.RaftLogEntryTm, Ticket: tkt})
		panicOn(err)
	}
	specularApplyAll(node)

	leaf, err := node.state.KVstore.GetLeaf("t", "czar", false)
	panicOn(err)
	if leaf.Version != 1 || leaf.WriteRaftLogIndex != 3 {
		t.Fatalf("Bug 5: serial 1 executed twice: key version %v, last written at index %v (want version 1, index 3)", leaf.Version, leaf.WriteRaftLogIndex)
	}
	ste, ok := node.state.SessTable["s1"]
	if !ok {
		t.Fatalf("setup: session s1 missing")
	}
	cached, ok := ste.Serial2Ticket.Get2(1)
	if !ok {
		t.Fatalf("no cached reply for serial 1")
	}
	if cached.TicketID != "w-first" || cached.Err != nil {
		t.Fatalf("Bug 5: cached reply for serial 1 was replaced by the retry's execution: TicketID=%v Err=%v", cached.TicketID, cached.Err)
	}
}

// Bug 6 (High): session expiry during apply used each
// replica's own clock (LastAppliedLogHLC was advanced with
// ReceiveMessageWithHLC, which takes the max with the local
// wall clock), so replicas whose clocks differ deleted
// different sessions at the same applied index.
func Test1006_session_gc_during_apply_uses_log_time(t *testing.T) {

	// the SESS_NEW was logged an hour ago with a 3 second
	// session; this replica is only now applying it (it
	// lagged, or its clock runs ahead). As of the log entry
	// the session is live, so every replica must keep it.
	t1 := time.Now().Add(-time.Hour)
	sess := specularSessNew("s1", 2, t1)
	sess.NewSessReq.SessRequestedInitialDur = 3 * time.Second
	sess.NewSessReq.SessionIndexEndxTm = t1.Add(3 * time.Second)

	var applied []bool
	for _, name := range []string{"r1", "r2"} {
		node := specularNode(t, name, FOLLOWER, 1, []int64{1}, "r1", "r2")
		specularMarkApplied(node, 1)
		e := &RaftLogEntry{Index: 2, Term: 1, Tm: t1, LogHLC: AssembleHLC(t1.UnixNano(), 0), Ticket: sess.applyCopy()}
		_, err := node.wal.saveRaftLogEntry(e)
		panicOn(err)
		if name == "r2" {
			// r2's apply clock was already ahead (e.g. it
			// applied other entries stamped later by
			// a previous leader); that must not matter either.
			node.state.LastAppliedLogHLC = AssembleHLC(t1.Add(-time.Second).UnixNano(), 0)
		}
		specularApplyAll(node)
		_, ok := node.state.SessTable["s1"]
		applied = append(applied, ok)
		if !ok {
			t.Errorf("Bug 6: replica %v deleted session s1 while applying its creation; as of the log entry (t1) it ends at t1+3s", name)
		}
		if got := node.state.LastAppliedLogHLC.ToTime48(); got.After(t1.Add(time.Second)) {
			t.Errorf("Bug 6: replica %v apply clock %v ran past the log's time %v (local clock mixed in)", name, got, t1)
		}
	}
}

// Bug 7 (High): the logless reconfiguration installed the
// new membership locally and acknowledged the client at
// once, before any quorum had adopted it. A newer leader's
// configuration then silently superseded the acknowledged
// change. The client must only be told "done" once the new
// configuration is loglessly committed (Q1/Q2 met for it).
func Test1007_reconfig_ack_waits_for_new_config_commit(t *testing.T) {
	onlyBubbled(t, func(t *testing.T) {

		c, _, leadi, _ := SetupTestCluster(t, 3, 0, 1007)
		defer c.Close()
		if leadi != 0 {
			t.Fatalf("expected node_0 to lead, got %v", leadi)
		}
		// let the leader cache both followers' current
		// config and term (Q1/Q2 for the current config).
		time.Sleep(3 * time.Second)

		leader := c.Nodes[0]
		goner := c.Nodes[2]

		// from now on, neither follower hears the leader,
		// so no quorum can ever adopt the new config.
		c.DeafDrop(map[int]float64{1: 1.0, 2: 1.0}, nil)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		insp, _, err := leader.RemovePeerIDFromCluster(ctx, false, false, goner.name, goner.PeerID, TUBE_REPLICA, goner.BaseServerHostPort(), leader.URL, 2*time.Second)
		if err == nil {
			mc := ""
			if insp != nil && insp.MC != nil {
				mc = insp.MC.Short()
			}
			t.Fatalf("Bug 7: removal of %v was acknowledged although no quorum adopted the new config (%v)", goner.name, mc)
		}
		vv("good: unadopted reconfig not acknowledged: %v", err)
	})
}

// stuckTransport stands in for the connection to a peer
// that has stopped reading (SIGSTOP, wedged VM, half-open
// TCP to a powered-off host). It models rpc25519's contract
// for such a peer: a blocking send (errWriteDur 0, or -1 on
// an unbuffered connection) never returns; an opted-in send
// (errWriteDur > 0 on a buffered connection) returns at once,
// here with ErrSendQueueFull.
type stuckTransport struct {
	rpc.UniversalCliSrv // only SendOneWayMessage is used.
	release             chan struct{}
	entered             atomic.Int64
	lastErrWriteDur     atomic.Int64
}

func (u *stuckTransport) SendOneWayMessage(ctx context.Context, msg *rpc.Message, errWriteDur time.Duration, forbidAutoCli bool) (bool, *rpc.LoopComm, error) {
	u.entered.Add(1)
	u.lastErrWriteDur.Store(int64(errWriteDur))
	if errWriteDur > 0 {
		return false, nil, rpc.ErrSendQueueFull
	}
	<-u.release
	return false, nil, nil
}

// Bug 8 (High): every event-loop send used SendOneWay with
// errWriteDur = -1, whose hand-off to the connection blocked
// with no timeout. One stopped peer therefore froze the
// single Tube event loop of every live replica that tried
// to send to it, and a healthy majority never elected a
// leader. Tube must opt in to rpc25519's non-blocking,
// deadline-bounded sends (errWriteDur > 0 on connections
// with SendChannelBufferSlots > 0).
func Test1008_send_to_stuck_peer_does_not_block_event_loop(t *testing.T) {

	node := specularNode(t, "n1", FOLLOWER, 1, []int64{1}, "n1", "n2", "n3")
	defer node.Halt.ReqStop.Close()

	if slots := node.cfg.RpcCfg.SendChannelBufferSlots; slots <= 0 {
		t.Fatalf("Bug 8: tube's rpc config has SendChannelBufferSlots=%v; its sends can block on a stalled peer", slots)
	}

	u := &stuckTransport{release: make(chan struct{})}
	defer close(u.release)

	ctx, canc := context.WithCancelCause(context.Background())
	defer canc(nil)
	lpb := &rpc.LocalPeer{
		Halt:     idem.NewHalterNamed("stuck_lpb"),
		Ctx:      ctx,
		U:        u,
		PeerID:   node.PeerID,
		PeerName: node.name,
	}
	defer lpb.Halt.ReqStop.Close()
	ckt := &rpc.Circuit{
		LpbFrom:        lpb,
		CircuitID:      "ckt-to-stopped-n3",
		Context:        ctx,
		Canc:           canc,
		RemotePeerID:   "n3_id",
		RemotePeerName: "n3",
		LocalPeerID:    node.PeerID,
		LocalPeerName:  node.name,
		RpbTo:          &rpc.RemotePeer{NetAddr: "simnet://n3"},
	}

	// e.g. the RequestPreVote / AppendEntries a timer
	// fires at a peer whose kernel buffer is full.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 3; i++ {
			frag := rpc.NewFragment()
			frag.FragSubject = "PreVote"
			frag.Payload = []byte("hi")
			node.SendOneWay(ckt, frag, -1, 1)
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Bug 8: the event loop is blocked sending to a stopped peer (hand-off entered %v times; errWriteDur=%v)", u.entered.Load(), time.Duration(u.lastErrWriteDur.Load()))
	}
	// generous: a deadline also ends the operation on the
	// remote, so allow 10 heartbeats.
	if d, want := time.Duration(u.lastErrWriteDur.Load()), 10*node.cfg.HeartbeatDur; d != want {
		t.Fatalf("expected tube's fire-and-forget write deadline to be 10 heartbeats (%v), got %v", want, d)
	}
}

// Bug 14 (High): a leader that removed itself kept leading
// and always counted its own log toward the commit quorum,
// although the quorum is sized to the new config C', which
// excludes it. Its own log then stood in for a member of C'.
func Test1014_removed_leader_does_not_count_itself_in_commit_quorum(t *testing.T) {

	// n1 leads term 1 and has installed C' = {n2, n3}
	// (it removed itself; C' not yet committed).
	lead := specularNode(t, "n1", LEADER, 1, []int64{1, 1}, "n2", "n3")
	lead.state.CommitIndex = 1
	lead.state.CommitIndexEntryTerm = 1
	specularMarkApplied(lead, 1)
	lead.leaderID = lead.PeerID
	lead.leaderName = lead.name

	n2 := lead.newRaftNodeInfo("n2_id", "n2", TUBE_REPLICA, "")
	n2.MatchIndex = 2
	lead.peers["n2_id"] = n2
	n3 := lead.newRaftNodeInfo("n3_id", "n3", TUBE_REPLICA, "")
	n3.MatchIndex = 1
	lead.peers["n3_id"] = n3

	// entry 2 is on n1 (not in C') and n2: one of the two
	// C' members. C' needs 2 of {n2, n3}, so it must not commit.
	lead.leaderAdvanceCommitIndex()

	if ci := lead.state.CommitIndex; ci > 1 {
		t.Fatalf("Bug 14: removed leader committed index %v held by only 1 of the 2 members of C' = {n2, n3}", ci)
	}

	// once n3 also has it, C' has a quorum.
	n3.MatchIndex = 2
	lead.leaderAdvanceCommitIndex()
	if ci := lead.state.CommitIndex; ci != 2 {
		t.Fatalf("expected index 2 committed once both C' members hold it; CommitIndex=%v", ci)
	}
}

// Bug 15 (High): peer records are keyed by PeerID, so after
// a follower restarts (new PeerID, same name) the leader
// holds a record for both incarnations until the old
// circuit's reader exits, and counted that one replica
// twice toward the commit (and read lease) quorum.
func Test1015_restarted_follower_not_counted_twice(t *testing.T) {

	members := []string{"n1", "n2", "n3", "n4", "n5"}
	lead := specularNode(t, "n1", LEADER, 1, []int64{1, 1}, members...)
	lead.state.CommitIndex = 1
	lead.state.CommitIndexEntryTerm = 1
	specularMarkApplied(lead, 1)
	lead.leaderID = lead.PeerID
	lead.leaderName = lead.name
	// n2's old incarnation (half-open circuit) and its new one.
	old := lead.newRaftNodeInfo("n2_id", "n2", TUBE_REPLICA, "")
	old.MatchIndex = 2
	lead.peers["n2_id"] = old
	cur := lead.newRaftNodeInfo("n2_id_incarnation1", "n2", TUBE_REPLICA, "")
	cur.MatchIndex = 2
	lead.peers["n2_id_incarnation1"] = cur
	now := time.Now()
	old.LastHeardAnything = now.Add(-time.Minute) // died a minute ago
	cur.LastHeardAnything = now
	// n2's new incarnation is the one connected under its name.
	lead.cktAllByName["n2"] = &cktPlus{PeerName: "n2", PeerID: "n2_id_incarnation1", ckt: &rpc.Circuit{}}
	for _, name := range []string{"n3", "n4", "n5"} {
		info := lead.newRaftNodeInfo(name+"_id", name, TUBE_REPLICA, "")
		info.MatchIndex = 1
		lead.peers[name+"_id"] = info
	}

	// entry 2 is on n1 and n2 only: 2 of 5, short of the
	// quorum of 3.
	lead.leaderAdvanceCommitIndex()
	if ci := lead.state.CommitIndex; ci > 1 {
		t.Fatalf("Bug 15: leader committed index %v held by only 2 distinct replicas of 5 (n2 counted twice)", ci)
	}
}

// Bug 16 (High): after a czar died holding a newer lease
// epoch, a member that knew only an older epoch could
// never become czar. Its CAS carries its last known epoch;
// Tube (deliberately) still enforces the epoch CAS on an
// expired lease, and the rejection's reply names the newer
// epoch but with an expired lease, which the member refused
// to learn (ErrExpiredCzarLease). It retried with the stale
// epoch forever.
func Test1016_czar_elected_after_dead_czar_with_newer_epoch(t *testing.T) {
	onlyBubbled(t, func(t *testing.T) {

		c, _, _, _ := SetupTestCluster(t, 3, 0, 1016)
		defer c.Close()
		simnet := c.Cfg.RpcCfg.GetSimnet()

		// the partition below makes the member reconnect many
		// times; drain the test-only new-circuit channels.
		stopDrain := make(chan struct{})
		defer close(stopDrain)
		drain := func(node *TubeNode) {
			go func(ch chan *cktPlus) {
				for {
					select {
					case <-ch:
					case <-stopDrain:
						return
					}
				}
			}(node.verifyPeerReplicaOrNot)
		}
		for _, node := range c.Nodes {
			drain(node)
		}

		// m0 becomes czar (lease epoch 1); m1 learns epoch 1.
		m0 := testStartOneMember(t, 0, c.Cfg)
		drain(m0.Czar.cli)
		if amCz := <-m0.TestingAmCzarCh; !amCz {
			t.Fatalf("setup: first member should become czar")
		}
		m1 := testStartOneMember(t, 1, c.Cfg)
		drain(m1.Czar.cli)
		defer func() {
			m1.Czar.Halt.RequestStop()
			<-m1.Czar.Halt.Done.Chan
		}()
		if amCz := <-m1.TestingAmCzarCh; amCz {
			t.Fatalf("setup: second member should not be czar")
		}
		time.Sleep(2 * time.Second)

		// m1 is cut off, so it misses what happens next.
		_, err := simnet.AlterHost(m1.Czar.cli.srvname, rpc.ISOLATE)
		panicOn(err)

		// the czar dies; after its lease expires another
		// leasor takes the key (epoch 2) and then dies too.
		m0.Czar.Halt.RequestStop()
		<-m0.Czar.Halt.Done.Chan
		time.Sleep(7 * time.Second)

		ghost := &ReliableMembershipList{CzarName: "ghost", PeerNames: NewOmap[string, *PeerDetailPlus]()}
		bts, err := ghost.MarshalMsg(nil)
		panicOn(err)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		tkt, err := c.Nodes[0].CAS(ctx, Key(t.Name()), "czar", nil, Val(bts), 2*time.Second, nil, ReliableMembershipListType, time.Second, leaseAutoDelFalse, 0, 1)
		cancel()
		if err != nil {
			t.Fatalf("setup: ghost could not take the expired czar key: %v", err)
		}
		if tkt.LeaseEpoch != 2 {
			t.Fatalf("setup: expected ghost at lease epoch 2, got %v", tkt.LeaseEpoch)
		}
		time.Sleep(5 * time.Second) // ghost's lease expires.

		// m1 returns, still knowing only epoch 1. With every
		// lease expired it must be able to become czar.
		_, err = simnet.AlterHost(m1.Czar.cli.srvname, rpc.UNISOLATE)
		panicOn(err)

		deadline := time.After(time.Minute)
		for {
			select {
			case amCz := <-m1.TestingAmCzarCh:
				if amCz {
					vv("good: m1 became czar")
					return
				}
			case <-deadline:
				t.Fatalf("Bug 16: no member became czar a minute after every lease expired (m1 stuck on a stale lease epoch)")
			}
		}
	})
}

// specularStalledMCTicket is a membership change ticket parked
// on stalledMembershipConfigChangeTkt, from a local client.
func specularStalledMCTicket(node *TubeNode, id string) *Ticket {
	return &Ticket{
		TicketID:       id,
		Op:             MEMBERSHIP_SET_UPDATE,
		RemovePeerName: "n3",
		FromID:         node.PeerID,
		FromName:       node.name,
		T0:             time.Now(),
		Done:           idem.NewIdemCloseChan(),
	}
}

// Bug 10 (Medium): a leader whose membership change stalled
// kept the stalled ticket after being deposed, and the next
// AE's connectToMC -> resubmitStalledTickets(true) passed it
// to the leader-only replicate path, which panics
// ("only for leader; followers should not be calling here").
func Test1010_deposed_leader_with_stalled_mc_ticket_does_not_panic(t *testing.T) {

	members := []string{"n1", "n2", "n3"}

	// (a) a follower that still holds a stalled ticket.
	foll := specularNode(t, "n1", FOLLOWER, 2, []int64{1}, members...)
	tkt := specularStalledMCTicket(foll, "stalled-a")
	foll.stalledMembershipConfigChangeTkt = append(foll.stalledMembershipConfigChangeTkt, tkt)
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Bug 10: follower panicked resubmitting a stalled membership ticket: %v", r)
			}
		}()
		foll.resubmitStalledTickets(true)
	}()
	if !tkt.Done.IsClosed() || tkt.Err == nil {
		t.Fatalf("a stalled ticket a follower cannot serve must be answered with an error (Done closed=%v, Err=%v)", tkt.Done.IsClosed(), tkt.Err)
	}

	// (b) stepping down fails the stalled tickets back to
	// their clients instead of keeping them.
	lead := specularNode(t, "n1", LEADER, 2, []int64{1}, members...)
	tkt2 := specularStalledMCTicket(lead, "stalled-b")
	lead.stalledMembershipConfigChangeTkt = append(lead.stalledMembershipConfigChangeTkt, tkt2)
	lead.becomeFollower(2, nil, false)
	if n := len(lead.stalledMembershipConfigChangeTkt); n != 0 {
		t.Fatalf("Bug 10: deposed leader kept %v stalled membership tickets", n)
	}
	if !tkt2.Done.IsClosed() || tkt2.Err == nil {
		t.Fatalf("stalled ticket not failed back to the client on step-down (Done closed=%v, Err=%v)", tkt2.Done.IsClosed(), tkt2.Err)
	}
}

// Bug 11 (Medium): the czar loop panicked ("why am I czar on
// failed write?") whenever a rejected czar-key CAS reported
// this member's own name as the current czar. Specular's
// trigger is the czar's own renewal being rejected as
// non-increasing (a failover to a leader whose clock lags),
// which simnet's shared clock cannot produce. A czar that
// restarts (same name, new PeerID) while its previous lease
// is still in force reaches the same panic deterministically.
// Neither case is impossible, and neither may crash it.
func Test1011_czar_does_not_panic_when_failed_write_names_itself(t *testing.T) {
	onlyBubbled(t, func(t *testing.T) {

		c, _, _, _ := SetupTestCluster(t, 3, 0, 1011)
		defer c.Close()

		m0 := testStartOneMember(t, 0, c.Cfg)
		if amCz := <-m0.TestingAmCzarCh; !amCz {
			t.Fatalf("setup: first member should become czar")
		}
		// the czar process restarts at once; its lease
		// (5s) is still in force under its name.
		m0.Czar.Halt.RequestStop()
		<-m0.Czar.Halt.Done.Chan
		time.Sleep(100 * time.Millisecond)

		again := testStartOneMember(t, 0, c.Cfg)
		defer func() {
			again.Czar.Halt.RequestStop()
			<-again.Czar.Halt.Done.Chan
		}()
		// it must not crash; once the old lease lapses it
		// becomes czar again.
		deadline := time.After(time.Minute)
		for {
			select {
			case amCz := <-again.TestingAmCzarCh:
				if amCz {
					return
				}
			case <-deadline:
				t.Fatalf("restarted member never became czar again")
			}
		}
	})
}

// Bug 12 (Medium): a node could lead the same term twice
// across a crash. beginElection raised CurrentTerm and voted
// for itself in memory only, sending RequestVote before
// persisting them; and on restart CurrentTerm was not
// reconciled with the WAL, so a node whose WAL held a term-1
// entry (its own no-op as term-1 leader) but whose saved
// term was still 0 campaigned for, and won, term 1 again.
func Test1012_term_and_vote_durable_before_campaign(t *testing.T) {

	// (a) the term and self-vote are on disk once
	// beginElection has sent its RequestVotes.
	path := filepath.Join(t.TempDir(), "bug12.persistor.tube.msgp")
	diskCfg := &TubeConfig{}
	saver, _, err := diskCfg.NewRaftStatePersistor(path, nil, false)
	panicOn(err)
	defer saver.close()

	cand := specularNode(t, "n3", FOLLOWER, 0, nil, "n1", "n2", "n3")
	cand.state.KVstore = newKVStore()
	cand.saver = saver
	cand.beginElection()
	if cand.state.CurrentTerm != 1 {
		t.Fatalf("setup: expected to campaign for term 1, at %v", cand.state.CurrentTerm)
	}
	saver2, onDisk, err := diskCfg.NewRaftStatePersistor(path, nil, true)
	panicOn(err)
	defer saver2.close()
	if onDisk == nil || onDisk.CurrentTerm != 1 || onDisk.VotedFor != cand.PeerID {
		var term int64
		var voted string
		if onDisk != nil {
			term, voted = onDisk.CurrentTerm, onDisk.VotedFor
		}
		t.Errorf("Bug 12: RequestVote sent before term/vote were durable: on disk CurrentTerm=%v VotedFor='%v' (want 1, '%v')", term, voted, cand.PeerID)
	}

	// (b) a restart reconciles CurrentTerm with the WAL.
	// n3 crashed as term-1 leader after fsyncing its term-1
	// no-op but before saving RaftState (term still 0).
	re := specularNode(t, "n3", FOLLOWER, 0, []int64{1}, "n1", "n2", "n3")
	panicOn(re.onRestartRecoverPersistentRaftStateFromDisk())
	if re.state.CurrentTerm < 1 {
		t.Fatalf("Bug 12: restarted with CurrentTerm %v below its WAL's last term 1; it can campaign for, and lead, term 1 again", re.state.CurrentTerm)
	}
}

// Bug 13 (Medium): the leader's read lease counted every
// TUBE_REPLICA peer with a fresh ping, including non-voting
// shadow replicas. A shadow's ack carries no promise not to
// elect another leader, so a leader cut off from the voters
// but still talking to a shadow kept serving stale reads.
func Test1013_read_lease_does_not_count_shadows(t *testing.T) {

	// voters {n1, n2, n3}; n4 is a non-voting shadow.
	lead := specularNode(t, "n1", LEADER, 1, []int64{1}, "n1", "n2", "n3")
	lead.initialNoop0HasCommitted = true
	lead.leaderID = lead.PeerID
	lead.leaderName = lead.name

	now := time.Now()
	shadow := lead.newRaftNodeInfo("n4_id", "n4", TUBE_REPLICA, "")
	shadow.MinElectionTimeoutDur = lead.cfg.MinElectionDur
	shadow.LastHeardAnything = now
	shadow.LastFullPing.Sent = now
	lead.peers["n4_id"] = shadow

	// the voters n2, n3 are unreachable: no fresh pings.
	for _, name := range []string{"n2", "n3"} {
		info := lead.newRaftNodeInfo(name+"_id", name, TUBE_REPLICA, "")
		info.MinElectionTimeoutDur = lead.cfg.MinElectionDur
		info.LastHeardAnything = now.Add(-time.Hour)
		info.LastFullPing.Sent = now.Add(-time.Hour)
		lead.peers[name+"_id"] = info
	}

	if canServe, until := lead.leaderCanServeReadsLocally(); canServe {
		t.Fatalf("Bug 13: leader granted itself a read lease (until %v) on the strength of a non-voting shadow's ack", until)
	}

	// a voter's fresh ack does give the lease.
	lead.peers["n2_id"].LastHeardAnything = now
	lead.peers["n2_id"].LastFullPing.Sent = now
	if canServe, _ := lead.leaderCanServeReadsLocally(); !canServe {
		t.Fatalf("expected a read lease once voter n2 (with the leader, 2 of 3) has a fresh ping")
	}
}

func TestRejectedHeartbeatCannotAdvanceCommit(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		leaderTerm, prevTerm int64
	}{
		{"old term", 2, 1},
		{"previous term mismatch", 3, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := specularNode(t, "f", FOLLOWER, 3, []int64{1}, "l", "f")
			host := &ackCaptureHost{hoster: &TubeSim{wal: f.wal, state: f.state, node: f}}
			f.host = host
			f.handleAppendEntries(&AppendEntries{
				LeaderName: "l", LeaderID: "l_id", LeaderTerm: tc.leaderTerm,
				PrevLogIndex: 1, PrevLogTerm: tc.prevTerm, LogTermsRLE: f.wal.getTermsRLE(),
				LeaderCommitIndex: 1, LeaderCommitIndexEntryTerm: 1,
			}, true)
			if len(host.acks) != 1 || !host.acks[0].Rejected || f.state.CommitIndex != 0 {
				t.Fatalf("rejected heartbeat must leave commit index at zero, got index %d, acks %v", f.state.CommitIndex, host.acks)
			}
		})
	}
}

func TestFollowerApplyLeavesFormerLeaderWaiterPending(t *testing.T) {
	f := specularNode(t, "f", FOLLOWER, 2, []int64{1}, "l", "f")
	e, err := f.wal.GetEntry(1)
	if err != nil {
		t.Fatal(err)
	}
	e.Ticket.FromID = f.PeerID
	e.waiter = e.Ticket.applyCopy()
	specularApplyAll(f)
	if f.state.LastApplied != 1 || e.waiter.Applied || e.waiter.Committed || e.waiter.finishTicketCalled {
		t.Fatalf("follower applied through %d, waiter committed=%v applied=%v finished=%v",
			f.state.LastApplied, e.waiter.Committed, e.waiter.Applied, e.waiter.finishTicketCalled)
	}
}

// Use the production log check while capturing the acknowledgement locally.
type diskCatchupHost struct {
	ackCaptureHost
	follower *TubeNode
}

func (h *diskCatchupHost) logsAreMismatched(ae *AppendEntries) (bool, int64, int64) {
	return h.follower.logsAreMismatched(ae)
}

func TestDiskFollowerRepairsSuffixAfterMatchingPreviousEntry(t *testing.T) {
	f := specularNode(t, "f", FOLLOWER, 3, nil, "l", "f")
	cfg := &TubeConfig{}
	w, err := cfg.newRaftWriteAheadLog(filepath.Join(t.TempDir(), "wal"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer w.close()
	f.wal = w
	for _, e := range createLogEntries(1, 4, 1) {
		e.Ticket.TicketID = rpc.NewCallID("")
		if _, err := w.saveRaftLogEntry(e); err != nil {
			t.Fatal(err)
		}
	}
	f.state.CommitIndex = 1
	f.state.LastApplied = 1
	host := &diskCatchupHost{ackCaptureHost: ackCaptureHost{hoster: &TubeSim{wal: w, state: f.state, node: f}}, follower: f}
	f.host = host
	terms := &TermsRLE{Runs: []*TermRLE{{Term: 1, Count: 1}, {Term: 3, Count: 1}}}
	terms.fixTot()
	replacement := createLogEntries(2, 3, 3)
	replacement[0].Ticket.TicketID = rpc.NewCallID("")
	f.handleAppendEntries(&AppendEntries{
		LeaderName: "l", LeaderID: "l_id", LeaderTerm: 3,
		PrevLogIndex: 1, PrevLogTerm: 1, Entries: replacement,
		LeaderCommitIndex: 1, LeaderCommitIndexEntryTerm: 1, LogTermsRLE: terms,
	}, true)
	if len(host.acks) != 1 || host.acks[0].Rejected || w.lli != 2 || w.llt != 3 {
		t.Fatalf("catch-up left WAL at (%d, %d), acks %v", w.lli, w.llt, host.acks)
	}
}

func TestFollowerCommitTermAtVerifiedPrefix(t *testing.T) {
	f := specularNode(t, "f", FOLLOWER, 4, []int64{2, 3}, "l", "f")
	f.host = &ackCaptureHost{hoster: &TubeSim{wal: f.wal, state: f.state, node: f}}
	leaderLog := &TermsRLE{Runs: []*TermRLE{{Term: 2, Count: 1}, {Term: 4, Count: 1}}}
	leaderLog.fixTot()
	f.handleAppendEntries(&AppendEntries{
		LeaderName: "l", LeaderID: "l_id", LeaderTerm: 4,
		LogTermsRLE: leaderLog, LeaderCommitIndex: 2, LeaderCommitIndexEntryTerm: 4,
	}, true)
	if f.state.CommitIndex != 1 || f.state.CommitIndexEntryTerm != 2 ||
		f.state.MC.CommitIndex != 1 || f.state.MC.CommitIndexEntryTerm != 2 {
		t.Fatalf("commit=(%d,%d), membership commit=(%d,%d), want (1,2)",
			f.state.CommitIndex, f.state.CommitIndexEntryTerm, f.state.MC.CommitIndex, f.state.MC.CommitIndexEntryTerm)
	}
}

func TestBootstrapSelfAddTakesFreshTermAndVote(t *testing.T) {
	n := specularNode(t, "f", FOLLOWER, 7, nil)
	n.state.MC.ConfigTerm = 6
	n.PeerServiceName = TUBE_REPLICA
	n.MyPeer = &rpc.LocalPeer{Ctx: context.Background(), PeerServiceName: TUBE_REPLICA, PeerID: n.PeerID}
	n.state.VotedFor = "previous_leader_id"
	n.state.HaveVotedTerm = 7
	n.state.HaveVoted = true
	n.state.KVstore = newKVStore()
	path := filepath.Join(t.TempDir(), "state")
	cfg := &TubeConfig{}
	saver, _, err := cfg.NewRaftStatePersistor(path, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	defer saver.close()
	n.saver = saver
	tkt := n.NewTicket("bootstrap self", "", "", nil, n.PeerID, n.name, MEMBERSHIP_SET_UPDATE, -1, n.MyPeer.Ctx)
	tkt.AddPeerName = n.name
	if !n.bootstrappedOrForcedMembership(tkt) {
		t.Fatal("bootstrap request was not handled")
	}
	reopened, st, err := cfg.NewRaftStatePersistor(path, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.close()
	if n.role != LEADER || st.CurrentTerm != 8 || st.VotedFor != n.PeerID || st.HaveVotedTerm != 8 {
		t.Fatalf("bootstrap role=%v durable term=%d vote=%q vote term=%d", n.role, st.CurrentTerm, st.VotedFor, st.HaveVotedTerm)
	}
}

func TestBootstrapSelfAddRefusesVisibleLeader(t *testing.T) {
	n := specularNode(t, "f", FOLLOWER, 7, nil)
	n.PeerServiceName = TUBE_REPLICA
	n.MyPeer = &rpc.LocalPeer{Ctx: context.Background(), PeerServiceName: TUBE_REPLICA, PeerID: n.PeerID}
	n.leaderID = "other_id"
	n.lastLegitAppendEntries = time.Now()
	tkt := n.NewTicket("bootstrap self", "", "", nil, n.PeerID, n.name, MEMBERSHIP_SET_UPDATE, -1, n.MyPeer.Ctx)
	tkt.AddPeerName = n.name
	n.bootstrappedOrForcedMembership(tkt)
	if n.role != FOLLOWER || n.state.MC.Len() != 0 || tkt.Err == nil {
		t.Fatalf("visible leader: self-add left role=%v members=%d err=%v", n.role, n.state.MC.Len(), tkt.Err)
	}
}

func TestInstallEmptyMCLeavesLeaderMembershipIntact(t *testing.T) {
	n := specularNode(t, "l", LEADER, 2, nil, "l", "f")
	before := n.state.MC.Clone()
	saver, _, err := n.cfg.NewRaftStatePersistor("", n, false)
	if err != nil {
		t.Fatal(err)
	}
	defer saver.close()
	n.saver = saver
	frag := &rpc.Fragment{}
	frag.SetUserArg("target", n.name)
	n.handleInstallEmptyMC(frag, nil)
	if n.state.MC.Len() != before.Len() || n.state.MC.ConfigVersion != before.ConfigVersion {
		t.Fatalf("leader config changed from %v to %v", before.Short(), n.state.MC.Short())
	}
}

func TestBatchNeedsSoleSelfVoterToCommitLocally(t *testing.T) {
	for _, members := range [][]string{nil, {"other"}} {
		n := specularNode(t, "l", LEADER, 2, nil, members...)
		tkt := n.NewTicket("probe local commit", "", "", nil, n.PeerID, n.name, NOOP, -1, context.Background())
		n.batchToSubmit = []*Ticket{tkt}
		n.replicateBatch()
		if n.state.CommitIndex != 0 || tkt.Committed {
			t.Errorf("members %v: local ticket committed at %d", members, n.state.CommitIndex)
		}
	}
}

func TestEqualVersionMembershipCollision(t *testing.T) {
	f := specularNode(t, "f", FOLLOWER, 4, nil, "f", "x")
	f.cfg.NoBackgroundConnect = true
	var err error
	f.saver, _, err = f.cfg.NewRaftStatePersistor("", f, false)
	if err != nil {
		t.Fatal(err)
	}
	defer f.saver.close()
	leaderConfig := specularMC(f, "f", "l")
	leaderConfig.IsCommitted = true
	if f.state.MC.VersionEqual(leaderConfig) {
		t.Error("different voter sets counted as the same config")
	}
	vote := &RequestVote{FromPeerName: "l", CandidatesTerm: 5, MC: leaderConfig}
	if f.logUpToDateAsOurs(vote) {
		t.Error("accepted a candidate with conflicting membership at the same version")
	}
	f.aeMemberConfigHelper(&AppendEntries{MC: leaderConfig}, 0, &AppendEntriesAck{})
	if !f.state.MC.SamePeers(leaderConfig) || !f.state.MC.IsCommitted {
		t.Errorf("did not adopt leader membership: %v", f.state.MC.Short())
	}
}

func TestForcedChangeContactingFollowerUsesLiveLeader(t *testing.T) {
	onlyBubbled(t, func(t *testing.T) {
		c, _, _, _ := SetupTestCluster(t, 3, 0, 1030)
		defer c.Close()
		leader, contact, caller := c.Nodes[0], c.Nodes[1], c.Nodes[2]
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		reply, _, err := caller.RemovePeerIDFromCluster(ctx, true, false, caller.name,
			caller.PeerID, TUBE_REPLICA, caller.BaseServerHostPort(), contact.URL, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if reply == nil || reply.ResponderPeerID != leader.PeerID {
			t.Fatalf("expected the current leader to handle the forced removal, got %v", reply)
		}
		mc := leader.Inspect().State.MC
		if _, present := mc.PeerNames.Get2(caller.name); present {
			t.Fatalf("leader retained removed voter %q", caller.name)
		}
	})
}

func TestReplicaJoinCannotReplaceLiveNamesake(t *testing.T) {
	n := specularNode(t, "node_0", FOLLOWER, 1, nil, "node_0", "node_2")
	defer n.Halt.RequestStop()
	incumbent := &cktPlus{PeerName: "node_2", PeerID: "original_id", PeerServiceName: TUBE_REPLICA,
		ckt: &rpc.Circuit{Halt: idem.NewHalter()}}
	incumbent.lastSeenTm = time.Now() // live: heard from recently
	n.cktAllByName[incumbent.PeerName] = incumbent
	n.cktall[incumbent.PeerID] = incumbent
	incoming := &rpc.Circuit{RemotePeerName: incumbent.PeerName, RemotePeerID: "new_id", RemoteServiceName: TUBE_REPLICA,
		CircuitID: "new_circuit", Halt: idem.NewHalter(), RpbTo: &rpc.RemotePeer{BaseServerAddr: "simnet://new"},
		LpbFrom: &rpc.LocalPeer{Halt: idem.NewHalter(), HandleCircuitClose: make(chan *rpc.Circuit, 1)}}
	_, rejected := n.addToCktall(incoming)
	if !rejected || n.cktAllByName[incumbent.PeerName] != incumbent || n.cktall[incoming.RemotePeerID] != nil {
		t.Fatal("a second replica took over a live replica's name")
	}
}

// Regression from d55d6692: a replica restarted under its own name
// (new PeerID) was rejected for as long as the dead process's circuit
// had not been noticed closed, so it could not rejoin or get a
// snapshot. An incumbent silent past followerDownDur gives way.
func TestRestartedReplicaReplacesSilentNamesake(t *testing.T) {
	n := specularNode(t, "node_0", LEADER, 1, nil, "node_0", "node_2")
	defer n.Halt.RequestStop()
	n.MyPeer = &rpc.LocalPeer{Halt: idem.NewHalter()} // for the circuit watchdogs
	old := n.newCktPlus("node_2", TUBE_REPLICA)
	old.PeerID = "original_id"
	old.ckt = &rpc.Circuit{RemotePeerID: "original_id", RemotePeerName: "node_2", RemoteServiceName: TUBE_REPLICA,
		CircuitID: "old_circuit", Halt: idem.NewHalter(), RpbTo: &rpc.RemotePeer{BaseServerAddr: "simnet://old"},
		LpbFrom: &rpc.LocalPeer{Halt: idem.NewHalter(), HandleCircuitClose: make(chan *rpc.Circuit, 1)}}
	old.lastSeenTm = time.Now().Add(-time.Hour) // the old process died long ago
	n.cktAllByName["node_2"] = old
	n.cktall["original_id"] = old
	incoming := &rpc.Circuit{RemotePeerName: "node_2", RemotePeerID: "restarted_id", RemoteServiceName: TUBE_REPLICA,
		CircuitID: "new_circuit", Halt: idem.NewHalter(), RpbTo: &rpc.RemotePeer{BaseServerAddr: "simnet://new"},
		LpbFrom: &rpc.LocalPeer{Halt: idem.NewHalter(), HandleCircuitClose: make(chan *rpc.Circuit, 1)}}
	if _, rejected := n.addToCktall(incoming); rejected {
		t.Fatal("restarted replica rejected because its dead predecessor's circuit lingered")
	}
	if got := n.cktAllByName["node_2"]; got == nil || got.PeerID != "restarted_id" {
		t.Fatal("restarted replica did not take over its own name")
	}
}

func TestElectionCountsEachVoterNameOnce(t *testing.T) {
	for _, phase := range []string{"pre-vote", "vote"} {
		t.Run(phase, func(t *testing.T) {
			n := specularNode(t, "node_0", FOLLOWER, 4, nil,
				"node_0", "node_1", "node_2", "node_3", "node_4", "node_5", "node_6")
			pre := phase == "pre-vote"
			if pre {
				n.beginPreVote()
			} else {
				n.beginElection()
			}

			submit := func(name, id string, granted bool) {
				n.cktall[id] = &cktPlus{node: n, PeerName: name, PeerID: id, PeerServiceName: TUBE_REPLICA}
				v := &Vote{FromPeerName: name, FromPeerID: id, FromPeerServiceName: TUBE_REPLICA,
					CandidateID: n.PeerID, CandidatesTerm: 5, FromPeerCurrentTerm: 4,
					IsPreVote: pre, VoteGranted: granted, MC: n.state.MC}
				if pre {
					n.tallyPreVote(v)
				} else {
					n.tallyVote(v)
				}
			}

			submit("node_2", "first_id", true)
			submit("node_2", "first_id", true)
			submit("node_2", "second_id", false)
			submit(n.name, "another_id", true) // self-vote already counted at the start
			yes, no := n.yesVotes, n.noVotes
			if pre {
				yes, no = n.yesPreVotes, n.noPreVotes
			}
			if yes != 2 || no != 0 {
				t.Fatalf("counted %d yes and %d no, want one yes each from node_0 and node_2", yes, no)
			}
		})
	}
}

func TestVoterKeepsFirstCandidateInSameTermAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "votes")
	cfg := &TubeConfig{}
	request := func(voter *TubeNode, name string) {
		voter.handleRequestVote(&RequestVote{
			FromPeerName: name, FromPeerID: name + "_id", FromPeerServiceName: TUBE_REPLICA,
			CandidatesTerm: 4, MC: voter.state.MC,
		}, nil)
	}
	newVoter := func() *TubeNode {
		return specularNode(t, "node_2", FOLLOWER, 4, nil, "node_0", "node_1", "node_2")
	}
	voter := newVoter()
	voter.state.KVstore = newKVStore()
	saver, _, err := cfg.NewRaftStatePersistor(path, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	voter.saver = saver
	request(voter, "node_0")
	request(voter, "node_1")
	saver.close()

	restarted := newVoter()
	saver, state, err := cfg.NewRaftStatePersistor(path, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	defer saver.close()
	if state == nil {
		t.Fatal("vote was not persisted")
	}
	restarted.saver, restarted.state = saver, state
	request(restarted, "node_1")
	if !state.HaveVoted || state.HaveVotedTerm != 4 || state.VotedFor != "node_0_id" {
		t.Fatalf("node_2 recorded vote %q for term %d, want node_0_id for term 4", state.VotedFor, state.HaveVotedTerm)
	}
}

func TestSameTermLeaderConflictClearsPendingWork(t *testing.T) {
	n := specularNode(t, "n1", LEADER, 3, []int64{2}, "n1", "n2", "n3")
	n.host = &ackCaptureHost{hoster: n}
	n.leaderName, n.leaderID = n.name, n.PeerID
	n.state.HaveVoted, n.state.VotedFor = true, n.PeerID
	stalled := specularStalledMCTicket(n, "stalled")
	n.stalledMembershipConfigChangeTkt = []*Ticket{stalled}
	n.batchToSubmit = []*Ticket{n.NewTicket("queued", "", "", nil, n.PeerID, n.name, NOOP, -1, context.Background())}
	n.handleAppendEntries(&AppendEntries{
		LeaderTerm: 3, LeaderID: "n2_id", LeaderName: "n2", LogTermsRLE: n.wal.getTermsRLE(),
	}, true)
	if n.role != FOLLOWER || n.leaderName != "n2" || !n.state.HaveVoted || n.state.VotedFor != n.PeerID {
		t.Fatalf("unexpected step-down state: role=%v leader=%q vote=%q", n.role, n.leaderName, n.state.VotedFor)
	}
	if len(n.batchToSubmit) != 0 || !stalled.Done.IsClosed() || stalled.Err == nil {
		t.Fatalf("pending batch=%d, stalled membership error=%v", len(n.batchToSubmit), stalled.Err)
	}
}

func TestFollowerCannotFlushLeaderBatch(t *testing.T) {
	n := specularNode(t, "n1", FOLLOWER, 3, nil, "n1")
	tkt := n.NewTicket("queued", "", "", nil, n.PeerID, n.name, NOOP, -1, context.Background())
	n.batchToSubmit = []*Ticket{tkt}
	n.replicateBatch()
	if n.wal.LastLogIndex() != 0 || tkt.Committed || len(n.batchToSubmit) != 0 {
		t.Fatalf("follower flushed stale batch: last index=%d committed=%v queued=%d", n.wal.LastLogIndex(), tkt.Committed, len(n.batchToSubmit))
	}
}

func TestRestartHonorsPreviousElectionTimeout(t *testing.T) {
	onlyBubbled(t, func(t *testing.T) {
		dir := t.TempDir()
		boot := func(timeout, drift time.Duration) *TubeNode {
			n := specularNode(t, "node_2", FOLLOWER, 2, nil, "node_0", "node_1", "node_2")
			n.cfg.NoDisk, n.cfg.DataDir, n.cfg.isTest = false, dir, false
			n.cfg.MinElectionDur, n.cfg.ClockDriftBound = timeout, drift
			n.t0 = time.Now()
			if err := n.onRestartRecoverPersistentRaftStateFromDisk(); err != nil {
				t.Fatal(err)
			}
			return n
		}
		closeNode := func(n *TubeNode) {
			n.saver.close()
			n.state.KVstore.Close()
		}
		const timeout = 2 * time.Second
		const drift = 300 * time.Millisecond
		first := boot(timeout, drift)
		// This node could now advertise its timeout in heartbeat acks.
		if _, err := first.saver.save(first.state); err != nil {
			t.Fatal(err)
		}
		closeNode(first)

		request := &RequestVote{IsPreVote: true, FromPeerName: "node_0", FromPeerID: "node_0_id",
			FromPeerServiceName: TUBE_REPLICA, CandidatesTerm: 3}
		for attempt := 0; attempt < 2; attempt++ {
			n := boot(200*time.Millisecond, 10*time.Millisecond)
			request.MC = n.state.MC
			time.Sleep(timeout + drift - time.Nanosecond)
			if sticky, _ := n.haveStickyLeader(request); !sticky {
				t.Error("pre-vote eligible before the previous lease window elapsed")
			}
			request.IsPreVote = false
			n.handleRequestVote(request, nil)
			if n.state.HaveVoted {
				t.Fatalf("granted vote to %s during the restart holdoff", n.state.VotedForName)
			}
			n.beginPreVote()
			if n.preVoteTerm != 0 {
				t.Fatal("started a pre-vote using our own vote during restart holdoff")
			}
			n.beginElection()
			if n.state.HaveVoted {
				t.Fatal("cast a self-vote during restart holdoff")
			}
			if attempt == 1 {
				time.Sleep(time.Nanosecond)
				if sticky, _ := n.haveStickyLeader(request); sticky {
					t.Fatal("pre-vote still refused at lease expiry")
				}
				n.handleRequestVote(request, nil)
				if n.state.VotedForName != request.FromPeerName {
					t.Fatal("eligible vote refused after lease expiry")
				}
			}
			request.IsPreVote = true
			closeNode(n)
		}
	})
}

func TestLeaseApplyUsesReplicatedDriftBound(t *testing.T) {
	for _, op := range []TicketOp{WRITE, CAS, DELETE_KEY} {
		for _, tc := range []struct {
			label                      string
			leaderBound, followerBound time.Duration
			legacy, wantAccepted       bool
		}{
			{"zero", 0, time.Second, false, true},
			{"long", time.Second, 0, false, false},
			{"legacy", 0, 0, true, false},
		} {
			t.Run(fmt.Sprintf("%v/%s", op, tc.label), func(t *testing.T) {
				leader := specularNode(t, "n1", LEADER, 1, nil, "n1", "n2")
				f := specularNode(t, "n2", FOLLOWER, 1, nil, "n1", "n2")
				leader.cfg.ClockDriftBound, f.cfg.ClockDriftBound = tc.leaderBound, tc.followerBound
				t0 := time.Now()
				until := t0.Add(time.Second)
				f.doWrite(specularLeaseWrite("first", 1, t0, until))
				now := until.Add(350 * time.Millisecond)
				tkt := specularLeaseWrite("next", 2, now, now.Add(time.Second))
				tkt.Op, tkt.Leasor, tkt.FromID = op, "c2", "c2_id"
				entry := &RaftLogEntry{Index: 2, Term: 1, Tm: now, Ticket: tkt}
				if !tc.legacy {
					entry = leader.prepOne(tkt, now, 2, 0)
				}
				wire, err := entry.MarshalMsg(nil)
				if err != nil {
					t.Fatal(err)
				}
				var recv RaftLogEntry
				if _, err := recv.UnmarshalMsg(wire); err != nil {
					t.Fatal(err)
				}
				result := recv.Ticket
				switch op {
				case WRITE:
					f.doWrite(result)
				case CAS:
					f.doCAS(result)
				case DELETE_KEY:
					f.doDeleteKey(result)
				}
				if (result.Err == nil) != tc.wantAccepted {
					t.Fatalf("accepted=%v, want %v (apply error %v)", result.Err == nil, tc.wantAccepted, result.Err)
				}
				leaf, err := f.state.KVstore.GetLeaf("t", "czar", false)
				if op == DELETE_KEY && tc.wantAccepted {
					if err != ErrKeyNotFound {
						t.Fatalf("deleted key: got %v, want missing", err)
					}
					return
				}
				wantOwner := "c1"
				if tc.wantAccepted {
					wantOwner = "c2"
				}
				if err != nil || leaf.Leasor != wantOwner {
					t.Fatalf("got leaf %v, error %v, want owner %s", leaf, err, wantOwner)
				}
			})
		}
	}
}

func TestClosedSessionWriteSkipsApplication(t *testing.T) {
	n := specularNode(t, "n2", FOLLOWER, 1, nil, "n1", "n2")
	n.state.KVstore = newKVStore()
	now := time.Now()
	appendTicket := func(tkt *Ticket) {
		t.Helper()
		e := n.prepOne(tkt, now, n.wal.LastLogIndex()+1, 0)
		if _, err := n.wal.saveRaftLogEntry(e); err != nil {
			t.Fatal(err)
		}
	}
	appendTicket(specularSessNew("s1", 1, now))
	appendTicket(&Ticket{Op: SESS_END, EndSessReq_SessionID: "s1", TicketID: "end"})
	addWrite := func(session string) {
		appendTicket(&Ticket{Op: WRITE, SessionID: session, SessionSerial: 1,
			TicketID: "write-" + session, Table: "data", Key: "count", Val: Val("value")})
	}
	addWrite("s1")
	specularApplyAll(n)
	if _, err := n.state.KVstore.GetLeaf("data", "count", false); err != ErrKeyNotFound {
		t.Errorf("write from closed session reached the store, read error %v", err)
	}
	if n.state.LastApplied != 3 {
		t.Fatalf("last applied %d, want 3 including the rejected command", n.state.LastApplied)
	}
	appendTicket(specularSessNew("s2", 4, now))
	addWrite("s2")
	specularApplyAll(n)
	leaf, err := n.state.KVstore.GetLeaf("data", "count", false)
	if err != nil || leaf.Version != 1 || leaf.WriteRaftLogIndex != 5 {
		t.Fatalf("retry in new session should apply once at index 5, got leaf %v, error %v", leaf, err)
	}
}

func TestLeaderSessionTimeoutDoesNotRemoveReplicatedState(t *testing.T) {
	n := specularNode(t, "n1", LEADER, 1, nil, "n1", "n2")
	past := time.Now().Add(-48 * time.Hour)
	n.applyNewSess(specularSessNew("s1", 1, past))
	tkt := n.NewTicket("expired", "t", "k", nil, n.PeerID, n.name, WRITE, -1, context.Background())
	tkt.SessionID, tkt.SessionSerial = "s1", 1
	done, _ := n.leaderDoneEarlyOnSessionStuff(tkt)
	if !done || tkt.Err == nil {
		t.Fatal("expired session was admitted")
	}
	if n.state.SessTable["s1"] == nil {
		t.Fatal("admission removed the session outside the Raft log")
	}
}

func TestLocalReadCannotExtendSessionOnLeaderAlone(t *testing.T) {
	n := specularNode(t, "n1", LEADER, 1, nil, "n1")
	now := time.Now()
	create := specularSessNew("s1", 1, now.Add(-23*time.Hour))
	n.applyNewSess(create)
	ste := n.state.SessTable["s1"]
	deadline := ste.SessionEndxTm
	tkt := n.NewTicket("read", "t", "k", nil, n.PeerID, n.name, READ, -1, context.Background())
	tkt.SessionID, tkt.SessionSerial = "s1", 1
	if n.leaderServedLocalRead(tkt, false) || !ste.SessionEndxTm.Equal(deadline) {
		t.Fatal("read near expiry renewed the session on the leader without replication")
	}
}

// CR-D-48: session expiry metadata survived neither
// Clone, snapshot install, nor restart.
func TestSessionTableEntryCloneKeepsExpiry(t *testing.T) {
	now := time.Now()
	ste := newSessionTableEntry(specularSessNew("s1", 1, now).NewSessReq)
	ste.SessionEndxTm = now.Add(time.Hour)
	c := ste.Clone()
	if !c.SessionEndxTm.Equal(ste.SessionEndxTm) {
		t.Fatalf("Clone dropped SessionEndxTm: got %v want %v", c.SessionEndxTm, ste.SessionEndxTm)
	}
}

func TestSnapshotInstallKeepsKnownSessionAndExpiryIndex(t *testing.T) {
	members := []string{"n1", "n2", "n3"}
	now := time.Now()

	lead := specularNode(t, "n2", LEADER, 1, []int64{1, 1, 1}, members...)
	lead.state.CommitIndex = 1
	lead.state.CommitIndexEntryTerm = 1
	specularMarkApplied(lead, 1)
	lead.leaderID = lead.PeerID
	lead.leaderName = lead.name
	lead.applyNewSess(specularSessNew("s1", 1, now))
	snap := lead.getStateSnapshot(false)

	foll := specularNode(t, "n1", FOLLOWER, 1, nil, members...)
	foll.leaderID = lead.PeerID
	foll.leaderName = lead.name
	foll.host = &ackCaptureHost{hoster: foll}
	// the follower already knew s1, plus a session the leader has since ended.
	foll.applyNewSess(specularSessNew("s1", 1, now))
	gone := specularSessNew("gone", 1, now)
	gone.NewSessReq.CliName = "c2"
	foll.applyNewSess(gone)

	foll.applyNewStateSnapshot(snap, "test", true)

	ste := foll.state.SessTable["s1"]
	if ste == nil {
		t.Fatal("snapshot install deleted the live session s1")
	}
	if ste.SessionEndxTm.IsZero() {
		t.Fatal("installed session has zero expiry")
	}
	if foll.state.SessTable["gone"] != nil {
		t.Fatal("session absent from the snapshot survived install")
	}
	if foll.sessByExpiry.Len() != 1 || foll.sessByExpiry.tree.Min().Item().(*SessionTableEntry) != ste {
		t.Fatalf("expiry index not rebuilt from installed table: len %v", foll.sessByExpiry.Len())
	}
	if foll.state.sessTableByClientName["c1"] != ste || foll.state.sessTableByClientName["c2"] != nil {
		t.Fatal("client-name index not rebuilt from installed table")
	}
}

func TestRestartRebuildsSessionExpiryIndex(t *testing.T) {
	now := time.Now()
	n := specularNode(t, "n1", FOLLOWER, 1, nil, "n1")
	ste := newSessionTableEntry(specularSessNew("s1", 1, now).NewSessReq)
	// as if just loaded from disk: in the table, no local index.
	n.state.SessTable = map[string]*SessionTableEntry{"s1": ste}
	panicOn(n.onRestartRecoverPersistentRaftStateFromDisk())
	if n.sessByExpiry.Len() != 1 || n.state.sessTableByClientName["c1"] == nil {
		t.Fatalf("restart did not rebuild session indexes: expiry len %v", n.sessByExpiry.Len())
	}
	n.state.LastAppliedLogHLC = AssembleHLC(now.Add(25*time.Hour).UnixNano(), 0)
	n.garbageCollectOldSessions()
	if n.state.SessTable["s1"] != nil {
		t.Fatal("expired session restored at restart was never collected")
	}
}

// CR-D-60: DELETE_TABLE and RENAME_TABLE destroyed live
// leases that DELETE_KEY refuses to remove.
func TestTableOpsHonorLiveLeases(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	until := t0.Add(5 * time.Second)
	for _, op := range []TicketOp{DELETE_TABLE, RENAME_TABLE} {
		n := specularNode(t, "n1", FOLLOWER, 1, []int64{1}, "n1", "n2")
		specularMarkApplied(n, 1)
		n.cfg.ClockDriftBound = 500 * time.Millisecond
		add := func(tkt *Ticket) {
			e := &RaftLogEntry{Index: tkt.LogIndex, Term: 1, Tm: tkt.RaftLogEntryTm, Ticket: tkt}
			_, err := n.wal.saveRaftLogEntry(e)
			panicOn(err)
		}
		add(specularLeaseWrite("w2", 2, t0, until))
		tableOp := func(id string, idx int64, tm time.Time) *Ticket {
			return &Ticket{TicketID: id, Op: op, Table: "t", NewTableName: "t2",
				FromID: "op_id", FromName: "op", RaftLogEntryTm: tm, LogIndex: idx, Term: 1}
		}
		// apply runs on a copy of each log ticket, so we check the store.
		add(tableOp("live", 3, t0.Add(time.Second)))
		specularApplyAll(n)
		leaf, err := n.state.KVstore.GetLeaf("t", "czar", false)
		if err != nil || leaf.Leasor != "c1" {
			t.Fatalf("%v removed the live leased key: leaf %v, err %v", op, leaf, err)
		}
		add(tableOp("expired", 4, until.Add(time.Second)))
		specularApplyAll(n)
		if _, err := n.state.KVstore.GetLeaf("t", "czar", false); err == nil {
			t.Fatalf("%v after lease expiry left the key in place", op)
		}
	}
}

// CR-D-58: the leader's dry run compared lease expiry
// against the zero RaftLogEntryTm, so a takeover of an
// expired lease never reached the log.
func TestDryRunAllowsTakeoverOfExpiredLease(t *testing.T) {
	n := specularNode(t, "n1", LEADER, 1, []int64{1}, "n1")
	specularMarkApplied(n, 1)
	n.cfg.ClockDriftBound = 500 * time.Millisecond
	logTm := time.Now().Add(-time.Minute)
	w := specularLeaseWrite("w2", 2, logTm, logTm.Add(5*time.Second))
	_, err := n.wal.saveRaftLogEntry(&RaftLogEntry{Index: 2, Term: 1, Tm: logTm, Ticket: w})
	panicOn(err)
	specularApplyAll(n)

	take := &Ticket{TicketID: "take", Op: WRITE, Table: "t", Key: "czar",
		Val: Val("c2 is czar"), Leasor: "c2", FromID: "c2_id", FromName: "c2",
		LeaseRequestDur: 5 * time.Second}
	if !n.kvstoreWouldWriteLease(take) {
		t.Fatalf("dry run rejected takeover of a lease that expired %v ago: %v", time.Since(logTm.Add(5*time.Second)), take.Err)
	}
}

// CR-D-4: a replacement circuit from the same follower
// incarnation reset the leader's replication record.
func TestReplacementCircuitKeepsFollowerProgress(t *testing.T) {
	n := specularNode(t, "n1", LEADER, 2, []int64{1, 2, 2, 2, 2}, "n1", "n2", "n3")
	n.MyPeer = &rpc.LocalPeer{Halt: idem.NewHalter()} // for the circuit watchdogs
	defer n.Halt.RequestStop()
	circuit := func(id string) *rpc.Circuit {
		return &rpc.Circuit{RemotePeerName: "n2", RemotePeerID: "n2_id", RemoteServiceName: TUBE_REPLICA,
			CircuitID: id, Halt: idem.NewHalter(), RpbTo: &rpc.RemotePeer{BaseServerAddr: "simnet://n2"},
			LpbFrom: &rpc.LocalPeer{Halt: idem.NewHalter(), HandleCircuitClose: make(chan *rpc.Circuit, 1)}}
	}
	if _, rejected := n.addToCktall(circuit("ckt_a")); rejected {
		t.Fatal("setup: first circuit rejected")
	}
	foll := n.peers["n2_id"]
	foll.MatchIndex, foll.LargestCommonRaftIndex = 5, 5
	if _, rejected := n.addToCktall(circuit("ckt_b")); rejected {
		t.Fatal("setup: replacement circuit rejected")
	}
	if got := n.peers["n2_id"]; got == nil || got.MatchIndex != 5 || got.LargestCommonRaftIndex != 5 {
		t.Fatalf("replacement circuit reset follower progress: %+v", got)
	}
}

// CR-D-4: a late accepted ack lowered the leader's record
// of the follower's common prefix within the term.
func TestStaleAckDoesNotLowerCommonPrefix(t *testing.T) {
	lead := specularNode(t, "n2", LEADER, 2, []int64{1, 2, 2, 2, 2}, "n1", "n2", "n3")
	lead.state.CommitIndex = 5
	lead.state.CommitIndexEntryTerm = 2
	specularMarkApplied(lead, 5)
	lead.leaderID, lead.leaderName = lead.PeerID, lead.name
	foll := lead.newRaftNodeInfo("n1_id", "n1", TUBE_REPLICA, "")
	foll.MatchIndex, foll.LargestCommonRaftIndex = 5, 5
	lead.peers["n1_id"] = foll
	ack := &AppendEntriesAck{ClusterID: lead.ClusterID, FromPeerID: "n1_id", FromPeerName: "n1",
		FromPeerServiceName: TUBE_REPLICA, AEID: "old-ae", Term: 2, SuppliedLeader: lead.PeerID,
		SuppliedLeaderLastTerm: 2, LargestCommonRaftIndex: 3, PeerLogLastIndex: 3, PeerLogLastTerm: 2,
		MinElectionTimeoutDur: lead.cfg.MinElectionDur}
	lead.handleAppendEntriesAck(ack, nil)
	if got := lead.peers["n1_id"].LargestCommonRaftIndex; got != 5 {
		t.Fatalf("stale accepted ack lowered LargestCommonRaftIndex 5 -> %v", got)
	}
}

// CR-D-61: an oversized scan reply panicked the leader,
// and each successive leader crashed on the retried scan.
func TestOversizedScanReplyFailsTicketInsteadOfPanicking(t *testing.T) {
	n := specularNode(t, "n1", LEADER, 1, nil, "n1")
	scan := newKVScan()
	big := make([]byte, rpc.UserMaxPayload/2+1)
	for i := 0; i < 3; i++ {
		scan.InsertLeaf(leased.NewLeaf(fmt.Sprintf("k%v", i), big, ""))
	}
	tkt := n.NewTicket("scan", "big", "", nil, "client_id", "client", READ_KEYRANGE, -1, context.Background())
	tkt.FromID, tkt.FromName = "client_id", "client"
	tkt.KeyValRangeScan = scan
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("oversized reply panicked the leader: %v", r)
			}
		}()
		n.respondToClientTicketApplied(tkt)
	}()
	if !errors.Is(tkt.Err, ErrResultTooLarge) || tkt.KeyValRangeScan != nil {
		t.Fatalf("oversized reply should fail with ErrResultTooLarge and no scan, got err %v", tkt.Err)
	}
}

// CR-12: a key over the store's limit reached the log and
// panicked every replica at apply, poisoning each leader.
func TestOversizedKeyRejectedAtAdmission(t *testing.T) {
	n := specularNode(t, "n1", LEADER, 1, []int64{1}, "n1")
	n.state.KVstore = newKVStore()
	key := Key(strings.Repeat("k", yogadb.MaxKeySize+1))
	tkt := n.NewTicket("big key", "t", key, Val("v"), n.PeerID, n.name, WRITE, -1, context.Background())
	if err := n.validateTicketKeySizes(tkt); err == nil {
		t.Fatalf("a %v byte key passed admission", len(key))
	}
	ok := n.NewTicket("ok key", "t", Key("k"), Val("v"), n.PeerID, n.name, WRITE, -1, context.Background())
	if err := n.validateTicketKeySizes(ok); err != nil {
		t.Fatalf("a short key failed admission: %v", err)
	}
}

func TestOversizedKeyInLogFailsTicketAtApply(t *testing.T) {
	n := specularNode(t, "n1", FOLLOWER, 1, []int64{1}, "n1")
	specularMarkApplied(n, 1)
	n.state.KVstore = newKVStore()
	tm := time.Now()
	tkt := &Ticket{TicketID: "w2", Op: WRITE, Table: "t", Key: Key(strings.Repeat("k", yogadb.MaxKeySize+1)),
		Val: Val("v"), FromID: "c1_id", FromName: "c1", RaftLogEntryTm: tm, LogIndex: 2, Term: 1}
	_, err := n.wal.saveRaftLogEntry(&RaftLogEntry{Index: 2, Term: 1, Tm: tm, Ticket: tkt})
	panicOn(err)
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("applying an oversized key panicked: %v", r)
			}
		}()
		specularApplyAll(n)
	}()
	if n.state.LastApplied != 2 {
		t.Fatalf("LastApplied %v, want 2", n.state.LastApplied)
	}
}

// CR-D-79: foreign, malformed or misrouted frags panicked
// the replica event loop instead of being dropped.
func TestReplicaDropsForeignAndMalformedFrags(t *testing.T) {
	n := specularNode(t, "n1", FOLLOWER, 1, nil, "n1", "n2", "n3")
	n.cktall["c_id"] = &cktPlus{node: n, PeerName: "c", PeerID: "c_id", PeerServiceName: TUBE_CLIENT}
	frag := func(op int, payload []byte) *rpc.Fragment {
		f := n.newFrag()
		f.Typ = rpc.CallPeerTraffic
		f.FromPeerID, f.FromPeerName = "c_id", "c"
		f.FragOp = op
		f.Payload = payload
		return f
	}
	foreignVote, err := (&RequestVote{ClusterID: "456", FromPeerID: "c_id"}).MarshalMsg(nil)
	panicOn(err)
	foreign := frag(AppendEntriesMsg, nil)
	foreign.SetUserArg("ClusterID", "456")
	cases := map[string]*rpc.Fragment{
		"foreign ClusterID":                  foreign,
		"unknown FragOp":                     frag(9999, nil),
		"NotifyClientNewLeader at replica":   frag(NotifyClientNewLeader, nil),
		"undecodable AppendEntries":          frag(AppendEntriesMsg, []byte("garbage")),
		"foreign ClusterID in payload":       frag(RequestVoteMsg, foreignVote),
		"PeerListReply without fragCallID":   frag(PeerListReply, nil),
		"undecodable RedirectTicketToLeader": frag(RedirectTicketToLeaderMsg, []byte{0xc1}),
	}
	for name, f := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("%v: replica panicked instead of dropping: %v", name, r)
				}
			}()
			if err := n.handleArrivingFrag(f, nil); err != nil {
				t.Errorf("%v: dropping a frag asked Start to return: %v", name, err)
			}
		}()
	}
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("circuit with empty RemotePeerName panicked: %v", r)
			}
		}()
		ckt := &rpc.Circuit{RemotePeerID: "x_id", CircuitID: "x", Halt: idem.NewHalter(),
			RpbTo:   &rpc.RemotePeer{BaseServerAddr: "simnet://x"},
			LpbFrom: &rpc.LocalPeer{Halt: idem.NewHalter(), HandleCircuitClose: make(chan *rpc.Circuit, 1)}}
		n.handleNewCircuit(ckt, nil, nil, nil, nil)
	}()
}

// CR-D-80: a frag whose PeerID differs from its circuit's
// (a stale client URL now served by a new process) hit a
// debug panic in peerJoin on the new leader.
func TestPeerJoinDropsFragFromUnexpectedPeerID(t *testing.T) {
	n := specularNode(t, "n1", LEADER, 1, nil, "n1")
	ckt := &rpc.Circuit{RemotePeerID: "stale_id", RemotePeerName: "c", RemoteServiceName: TUBE_CLIENT,
		CircuitID: "stale_ckt", Halt: idem.NewHalter(), RpbTo: &rpc.RemotePeer{BaseServerAddr: "simnet://c"},
		LpbFrom: &rpc.LocalPeer{Halt: idem.NewHalter(), HandleCircuitClose: make(chan *rpc.Circuit, 1)}}
	frag := n.newFrag()
	frag.FromPeerID, frag.FromPeerName = "new_id", "c"
	frag.FragSubject = "RedirectTicketToLeader"
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("peerJoin panicked on a mismatched PeerID: %v", r)
		}
	}()
	if n.peerJoin(frag, ckt) {
		t.Fatal("peerJoin accepted a frag whose PeerID differs from its circuit")
	}
}

// CR-D-34: a remote circuit-start for a stale PeerID made
// rpc25519 run a second TubeNode.Start on a client node.
func TestClientServicesLimitedToOneLocalPeer(t *testing.T) {
	cfg := specularTestCfg(t, 1)
	for _, name := range []string{TUBE_REPLICA, TUBE_CLIENT, "hermes"} {
		if got := cfg.RpcCfg.GetLimitMax(name); got != 1 {
			t.Errorf("service %q limit %v per process, want 1", name, got)
		}
	}
}

func TestStartRefusesToRunTwice(t *testing.T) {
	n := specularNode(t, "c1", CLIENT, 0, nil, "n1")
	n.startRunning.Store(true) // as if a first Start loop owned the node
	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("panic: %v", r)
			}
		}()
		done <- n.Start(&rpc.LocalPeer{PeerID: "second_id", Halt: idem.NewHalter()}, context.Background(), nil)
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrAlreadyStarted) {
			t.Fatalf("second Start returned %v, want ErrAlreadyStarted", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second Start kept running on the same node")
	}
	if n.PeerID == "second_id" {
		t.Fatal("second Start overwrote the node's PeerID")
	}
}

// CR-D-84: an add ticket carrying a restarted node's old
// PeerID panicked the leader in changeMembership.
func TestAddWithStalePeerIDUsesLiveCircuit(t *testing.T) {
	n := specularNode(t, "n1", LEADER, 1, []int64{1}, "n1")
	n.state.CommitIndex, n.state.CommitIndexEntryTerm = 1, 1
	specularMarkApplied(n, 1)
	n.leaderID, n.leaderName = n.PeerID, n.name
	n.MyPeer = &rpc.LocalPeer{Halt: idem.NewHalter(), PeerServiceName: TUBE_REPLICA}
	ckt := &rpc.Circuit{RemotePeerID: "n2_live", RemotePeerName: "n2", RemoteServiceName: TUBE_REPLICA,
		CircuitID: "ckt_n2", Halt: idem.NewHalter(), RpbTo: &rpc.RemotePeer{BaseServerAddr: "simnet://n2"}}
	n.cktAllByName["n2"] = &cktPlus{node: n, PeerName: "n2", PeerID: "n2_live", PeerServiceName: TUBE_REPLICA, ckt: ckt}
	tkt := n.NewTicket("add n2", "", "", nil, n.PeerID, n.name, MEMBERSHIP_SET_UPDATE, -1, context.Background())
	tkt.AddPeerName, tkt.AddPeerID = "n2", "n2_stale"
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("stale AddPeerID panicked the leader: %v", r)
			}
		}()
		n.changeMembership(tkt)
	}()
	if det, ok := n.state.Known.PeerNames.Get2("n2"); !ok || det.PeerID != "n2_live" {
		t.Fatalf("expected the live circuit's PeerID for n2, got %v", det)
	}
}

// CR-D-78: CAS panicked (panicf before its return) on a
// cancelled context or a closed session.
func TestCASReturnsErrorsForCancelledContextAndSession(t *testing.T) {
	n := specularNode(t, "c1", CLIENT, 0, nil, "n1")
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	try := func(name string, ctx context.Context, sess *Session, want error) {
		t.Helper()
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("%v: CAS panicked: %v", name, r)
			}
		}()
		_, err := n.CAS(ctx, "t", "k", nil, Val("v"), 0, sess, "", 0, false, 0, 0)
		if !errors.Is(err, want) {
			t.Errorf("%v: CAS returned %v, want %v", name, err, want)
		}
	}
	try("cancelled ctx", cancelled, nil, context.Canceled)
	try("closed session", context.Background(), &Session{ctx: cancelled}, ErrNeedNewSession)
}

// CR-D-30: the watchdog's periodic liveness check never ran
// (pack was never assigned), so a circuit that went silent was
// never reconnected by the watchdog.
func TestWatchdogReconnectsSilentReplicaCircuit(t *testing.T) {
	onlyBubbled(t, func(t *testing.T) {
		c, _, leadi, _ := SetupTestCluster(t, 3, 0, 1030)
		defer c.Close()
		simnet := c.Cfg.RpcCfg.GetSimnet()
		lead := c.Nodes[leadi]
		// drain any reconnects from startup.
		for len(lead.testWatchdogTimeoutReconnectCh) > 0 {
			<-lead.testWatchdogTimeoutReconnectCh
		}
		// Isolate the leader: its circuits to the followers go
		// silent, and only its watchdog can notice.
		_, err := simnet.AlterHost(lead.srvname, rpc.ISOLATE)
		panicOn(err)
		time.Sleep(30 * time.Second) // well past followerDownDur
		_, err = simnet.AlterHost(lead.srvname, rpc.UNISOLATE)
		panicOn(err)
		time.Sleep(5 * time.Second)
		if len(lead.testWatchdogTimeoutReconnectCh) == 0 {
			t.Fatalf("%v's watchdog never tried to reconnect its silent replica circuits", lead.name)
		}
	})
}

// CR-D-35: once a follower's single dial to a peer failed,
// nothing dialed that peer again without a leader, so a live,
// connected majority could stay leaderless.
func TestFollowersRedialEachOtherWithoutLeader(t *testing.T) {
	onlyBubbled(t, func(t *testing.T) {
		c, _, leadi, _ := SetupTestCluster(t, 3, 0, 1035)
		defer c.Close()
		// C, the leader, is cut off; the others elect X.
		c.IsolateNode(leadi)
		var x int = -1
		deadline := time.Now().Add(time.Minute)
		for x < 0 && time.Now().Before(deadline) {
			time.Sleep(time.Second)
			for i, n := range c.Nodes {
				if i != leadi && n.Inspect().Role == LEADER {
					x = i
				}
			}
		}
		if x < 0 {
			t.Fatal("setup: the majority did not elect a new leader")
		}
		// X crashes while C is still isolated, then the network heals.
		c.Nodes[x].SimCrash()
		time.Sleep(20 * time.Second)
		c.AllHealthyAndPowerOn(false)
		deadline = time.Now().Add(2 * time.Minute)
		for time.Now().Before(deadline) {
			time.Sleep(time.Second)
			for i, n := range c.Nodes {
				if i != x && n.Inspect().Role == LEADER {
					return // the live majority elected a leader
				}
			}
		}
		t.Fatalf("live majority of nodes %v and %v never elected a leader after the heal", leadi, 3-leadi-x)
	})
}

// CR-D-36: at an unchanged config version, a follower ignored
// the leader's new address for a restarted peer and kept
// dialing its stale address from a pending entry.
func TestFollowerAdoptsLeadersNewAddressAtEqualVersion(t *testing.T) {
	n := specularNode(t, "n1", FOLLOWER, 3, nil, "n0", "n1", "n2")
	old := &PeerDetail{Name: "n2", PeerID: "n2_old", PeerServiceName: TUBE_REPLICA,
		Addr: "simnet://old", URL: "simnet://old/tube-replica/n2_old"}
	n.state.MC.setNameDetail("n2", old, n)
	pending := n.newCktPlus("n2", TUBE_REPLICA)
	pending.PeerBaseServerAddr, pending.Addr = old.Addr, old.Addr
	n.cktAllByName["n2"] = pending

	mc := n.state.MC.Clone()
	mc.setNameDetail("n2", &PeerDetail{Name: "n2", PeerID: "n2_new", PeerServiceName: TUBE_REPLICA,
		Addr: "simnet://new", URL: "simnet://new/tube-replica/n2_new"}, n)
	if !mc.VersionEqual(n.state.MC) {
		t.Fatal("setup: expected an equal config version")
	}
	n.aeMemberConfigHelper(&AppendEntries{MC: mc}, 0, &AppendEntriesAck{})

	if det, _ := n.state.MC.PeerNames.Get2("n2"); det.Addr != "simnet://new" {
		t.Fatalf("follower kept n2's stale address %q", det.Addr)
	}
	pending.requestReconnectMut.Lock()
	req := pending.requestReconnect
	pending.requestReconnectMut.Unlock()
	if req == nil || req.addr != "simnet://new" {
		t.Fatalf("pending entry for n2 not redialed at the new address: %+v", req)
	}
}

// CR-D-39: when two replicas dialed each other at once, each
// kept its own circuit, and the 20 s audit GC then closed the
// circuit its peer was sending on, black-holing the pair.
func TestAuditGCKeepsUnchosenReplicaCircuits(t *testing.T) {
	n := specularNode(t, "n1", FOLLOWER, 1, nil, "n1", "n2")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mk := func(id string) *rpc.Circuit {
		return &rpc.Circuit{RemotePeerID: "n2_id", RemotePeerName: "n2", RemoteServiceName: TUBE_REPLICA,
			LocalServiceName: TUBE_REPLICA, CircuitID: id, Context: ctx, Halt: idem.NewHalter(),
			RpbTo:   &rpc.RemotePeer{BaseServerAddr: "simnet://n2"},
			LpbFrom: &rpc.LocalPeer{Halt: idem.NewHalter(), HandleCircuitClose: make(chan *rpc.Circuit, 1)}}
	}
	plus := func(id string) *cktPlus {
		p := n.newCktPlus("n2", TUBE_REPLICA)
		p.PeerID, p.ckt = "n2_id", mk(id)
		return p
	}
	chosen, peers := plus("ours"), plus("theirs")
	n.cktall["n2_id"] = chosen
	n.cktAuditByCID.Set("ours", chosen)
	n.cktAuditByCID.Set("theirs", peers) // the circuit n2 sends on
	n.globalPruneCktCheck()
	if peers.ckt.IsClosed() {
		t.Fatal("audit GC closed the replica circuit the peer sends on")
	}
}

// CR-D-43: a "connection refused" on an old circuit deleted
// whatever circuit cktall now held for that peer, including a
// healthy replacement.
func TestConnRefusedOnlyDropsTheFailedCircuit(t *testing.T) {
	n := specularNode(t, "n1", FOLLOWER, 1, nil, "n1", "n2")
	mk := func(id string) *rpc.Circuit {
		return &rpc.Circuit{RemotePeerID: "n2_id", RemotePeerName: "n2", RemoteServiceName: TUBE_REPLICA,
			CircuitID: id, Halt: idem.NewHalter(), RpbTo: &rpc.RemotePeer{BaseServerAddr: "simnet://n2"},
			LpbFrom: &rpc.LocalPeer{Halt: idem.NewHalter(), HandleCircuitClose: make(chan *rpc.Circuit, 1)}}
	}
	old := mk("old")
	replacement := n.newCktPlus("n2", TUBE_REPLICA)
	replacement.PeerID, replacement.ckt = "n2_id", mk("new")
	n.cktall["n2_id"] = replacement
	n.cktAllByName["n2"] = replacement
	if !n.wasConnRefused(fmt.Errorf("dial tcp: connect: connection refused"), old) {
		t.Fatal("setup: refusal not recognized")
	}
	if n.cktall["n2_id"] != replacement {
		t.Fatal("a refusal on the old circuit removed the healthy replacement")
	}
	if !old.IsClosed() {
		t.Fatal("the failing circuit itself was left open")
	}
}

// CR-D-45: closing a circuit with no watchdog (every non-
// replica circuit) always waited the full 10ms on the event
// loop; pruning hundreds of them stalled heartbeats.
func TestCloseWithoutWatchdogDoesNotWait(t *testing.T) {
	onlyBubbled(t, func(t *testing.T) {
		n := specularNode(t, "n1", LEADER, 1, nil, "n1")
		defer n.Halt.RequestStop()
		var elapsed time.Duration
		for i := 0; i < 100; i++ {
			c := n.newCktPlus(fmt.Sprintf("client%v", i), TUBE_CLIENT)
			t0 := time.Now()
			c.Close()
			elapsed += time.Since(t0)
		}
		if elapsed > 0 {
			t.Fatalf("closing 100 circuits without watchdogs took %v on the event loop", elapsed)
		}
	})
}

// CR-D-22: a member that reconnected but never acked (its
// cktPlus.MC still nil) made changeMembership stall every
// membership change forever, before any quorum check.
func TestSilentReconnectedMemberDoesNotStallMembershipChange(t *testing.T) {
	n := specularNode(t, "n0", LEADER, 2, []int64{1, 2}, "n0", "n1", "n2")
	n.state.CommitIndex, n.state.CommitIndexEntryTerm = 2, 2
	specularMarkApplied(n, 2)
	n.state.MC.IsCommitted = true
	n.initialNoop0HasCommitted = true
	n.leaderID, n.leaderName = n.PeerID, n.name
	n.MyPeer = &rpc.LocalPeer{Halt: idem.NewHalter(), PeerServiceName: TUBE_REPLICA}
	mkckt := func(name string) *rpc.Circuit {
		return &rpc.Circuit{RemotePeerID: name + "_id", RemotePeerName: name, RemoteServiceName: TUBE_REPLICA,
			CircuitID: name + "_ckt", Halt: idem.NewHalter(), RpbTo: &rpc.RemotePeer{BaseServerAddr: "simnet://" + name}}
	}
	// n1 is current; n2 reconnected but has never acked.
	n1 := &cktPlus{node: n, PeerName: "n1", PeerID: "n1_id", PeerServiceName: TUBE_REPLICA,
		ckt: mkckt("n1"), MC: n.state.MC.Clone(), CurrentTerm: 2}
	n2 := &cktPlus{node: n, PeerName: "n2", PeerID: "n2_id", PeerServiceName: TUBE_REPLICA, ckt: mkckt("n2")}
	n.cktAllByName["n1"], n.cktAllByName["n2"] = n1, n2
	// n1 holds the committed prefix (the P1 reconfiguration guard).
	n1info := n.newRaftNodeInfo("n1_id", "n1", TUBE_REPLICA, "")
	n1info.MatchIndex, n1info.LargestCommonRaftIndex = 2, 2
	n.peers["n1_id"] = n1info

	tkt := n.NewTicket("remove n2", "", "", nil, n.PeerID, n.name, MEMBERSHIP_SET_UPDATE, -1, context.Background())
	tkt.RemovePeerName = "n2"
	func() {
		defer func() { recover() }() // replication past the decision is not under test
		n.changeMembership(tkt)
	}()
	for _, stalled := range n2.stalledOnSeenTkt {
		if stalled == tkt && len(n1.stalledOnSeenTkt) == 0 && n.state.MC.ConfigVersion == 0 {
			t.Fatal("membership change stalled on a silent member before checking quorum")
		}
	}
	for _, stalled := range n.stalledMembershipConfigChangeTkt {
		if stalled == tkt {
			t.Fatal("membership change with a quorum of current members was stalled")
		}
	}
}

// CR-D-21: a membership change that timed out (its client told
// it failed) stayed in the stalledOnSeenTkt queues and could be
// re-run and applied later.
func TestTimedOutMembershipChangeIsForgotten(t *testing.T) {
	n := specularNode(t, "n0", LEADER, 2, []int64{1, 2}, "n0", "n1", "n2")
	n.state.CommitIndex, n.state.CommitIndexEntryTerm = 2, 2
	specularMarkApplied(n, 2)
	n.state.MC.IsCommitted = true
	n.leaderID, n.leaderName = n.PeerID, n.name
	n.MyPeer = &rpc.LocalPeer{Halt: idem.NewHalter(), PeerServiceName: TUBE_REPLICA}
	// no initial noop committed yet: the quorum check (P1) fails.
	stale := &cktPlus{node: n, PeerName: "n1", PeerID: "n1_id", PeerServiceName: TUBE_REPLICA,
		ckt: &rpc.Circuit{RemotePeerID: "n1_id", RemotePeerName: "n1", RemoteServiceName: TUBE_REPLICA,
			CircuitID: "n1_ckt", Halt: idem.NewHalter(), RpbTo: &rpc.RemotePeer{BaseServerAddr: "simnet://n1"}}}
	n.cktAllByName["n1"] = stale

	tkt := n.NewTicket("remove n2", "", "", nil, n.PeerID, n.name, MEMBERSHIP_SET_UPDATE, -1, context.Background())
	tkt.RemovePeerName = "n2"
	n.changeMembership(tkt) // stalls, queued on n1
	if len(stale.stalledOnSeenTkt) == 0 {
		t.Fatal("setup: expected the change to stall on n1")
	}
	n.stalledMembershipConfigChangeTkt = nil
	tkt.T0 = time.Now().Add(-time.Minute)
	n.changeMembership(tkt) // times out: the client is told it failed
	if tkt.Err == nil {
		t.Fatal("setup: expected the timeout error")
	}
	if !tkt.finishTicketCalled {
		t.Error("timed-out membership ticket was not finished")
	}
	for _, q := range stale.stalledOnSeenTkt {
		if q == tkt {
			t.Fatal("timed-out membership ticket is still queued for a later re-run")
		}
	}
}

// CR-D-57: tickets with LogIndex 0 (not yet in the log) were
// invisible to imap.all(), so leader takeover, resubmission and
// step-down notices missed them and their callers hung.
func TestImapIteratesUnindexedTickets(t *testing.T) {
	m := newImap()
	m.set("indexed", &Ticket{TicketID: "indexed", LogIndex: 4})
	m.set("pending", &Ticket{TicketID: "pending"}) // LogIndex 0
	seen := map[string]bool{}
	for _, tkt := range m.allIncludingUnindexed() {
		seen[tkt.TicketID] = true
	}
	if !seen["indexed"] || !seen["pending"] || len(seen) != 2 {
		t.Fatalf("iteration saw %v, want both tickets", seen)
	}
}

// CR-D-59: tickets dropped without being finished left their
// callers hanging: a failed redirect, a leader's own tickets on
// step-down or batch clear, and client tickets whose leader
// stepped down.
func TestDroppedTicketsAreFinished(t *testing.T) {
	newLocal := func(n *TubeNode, id string) *Ticket {
		tkt := n.NewTicket(id, "t", "k", Val("v"), n.PeerID, n.name, WRITE, -1, context.Background())
		return tkt
	}
	t.Run("failed redirect", func(t *testing.T) {
		n := specularNode(t, "n1", FOLLOWER, 1, nil, "n0", "n1")
		n.leaderID, n.leaderName = "n0_id", "n0"
		halted := idem.NewHalter()
		halted.ReqStop.Close()
		n.cktall["n0_id"] = &cktPlus{node: n, PeerName: "n0", PeerID: "n0_id", PeerServiceName: TUBE_REPLICA,
			ckt: &rpc.Circuit{RemotePeerID: "n0_id", CircuitID: "c", Halt: idem.NewHalter(),
				Context: context.Background(), LpbFrom: &rpc.LocalPeer{Halt: halted, Ctx: context.Background()}}}
		tkt := newLocal(n, "redirect")
		if !n.redirectToLeader(tkt) {
			t.Fatal("setup: expected a redirect")
		}
		if !tkt.finishTicketCalled || tkt.Err == nil {
			t.Fatalf("failed redirect left the ticket unfinished (err %v)", tkt.Err)
		}
	})
	t.Run("step-down", func(t *testing.T) {
		n := specularNode(t, "n0", FOLLOWER, 2, nil, "n0", "n1")
		tkt := newLocal(n, "local")
		n.WaitingAtLeader.set(tkt.TicketID, tkt)
		n.tellClientsImNotLeader()
		if !tkt.finishTicketCalled || !errors.Is(tkt.Err, ErrLeaderStepDown) {
			t.Fatalf("deposed leader dropped its own ticket (err %v)", tkt.Err)
		}
	})
	t.Run("batch clear", func(t *testing.T) {
		n := specularNode(t, "n0", FOLLOWER, 2, nil, "n0", "n1")
		tkt := newLocal(n, "batched")
		n.batchToSubmit = []*Ticket{tkt}
		n.clearBatchToSubmit(true)
		if !tkt.finishTicketCalled || !errors.Is(tkt.Err, ErrLeaderStepDown) {
			t.Fatalf("batch clear dropped a local ticket (err %v)", tkt.Err)
		}
	})
	t.Run("client told leader stepped down", func(t *testing.T) {
		n := specularNode(t, "c1", CLIENT, 0, nil, "n0")
		n.cktall["n0_id"] = &cktPlus{node: n, PeerName: "n0", PeerID: "n0_id", PeerServiceName: TUBE_REPLICA}
		tkt := newLocal(n, "client")
		n.WaitingAtFollow.set(tkt.TicketID, tkt)
		f := n.newFrag()
		f.Typ = rpc.CallPeerTraffic
		f.FromPeerID, f.FromPeerName = "n0_id", "n0"
		f.FragOp = TellClientSteppedDown
		f.Payload, _ = tkt.MarshalMsg(nil)
		n.handleArrivingFrag(f, nil)
		if !tkt.finishTicketCalled || !errors.Is(tkt.Err, ErrLeaderStepDown) {
			t.Fatalf("client ticket left waiting after its leader stepped down (err %v)", tkt.Err)
		}
	})
}

// CR-D-59 (M4): waiting tickets never expired; a caller that
// gave up (ctx done) left its ticket in the waiting maps forever.
func TestAbandonedWaitingTicketsExpire(t *testing.T) {
	n := specularNode(t, "c1", CLIENT, 0, nil, "n0")
	ctx, cancel := context.WithCancel(context.Background())
	tkt := n.NewTicket("abandoned", "t", "k", Val("v"), n.PeerID, n.name, WRITE, -1, ctx)
	n.WaitingAtFollow.set(tkt.TicketID, tkt)
	live := n.NewTicket("live", "t", "k", Val("v"), n.PeerID, n.name, WRITE, -1, context.Background())
	n.WaitingAtFollow.set(live.TicketID, live)
	cancel()
	n.globalPruneCktCheck()
	if !tkt.finishTicketCalled {
		t.Fatal("ticket of a caller that gave up was never expired")
	}
	if _, ok := n.WaitingAtFollow.get2(tkt.TicketID); ok {
		t.Fatal("expired ticket still in WaitingAtFollow")
	}
	if live.finishTicketCalled {
		t.Fatal("a live caller's ticket was expired")
	}
}

// CR-D-26: a node restarted with -zap stamped its empty member
// config with the current term, so it outranked the live config
// and spread to the leader; nobody could be elected again.
func TestZappedMemberConfigIsMinimal(t *testing.T) {
	n := specularNode(t, "n2", FOLLOWER, 5, []int64{5}, "n0", "n1", "n2")
	n.state.MC.ConfigTerm, n.state.MC.ConfigVersion = 5, 2
	n.zapMemberConfig()
	if n.state.MC.ConfigTerm != 0 || n.state.MC.ConfigVersion != 0 || n.state.MC.PeerNames.Len() != 0 {
		t.Fatalf("zapped config is (%v,%v) with %v members, want an empty (0,0)",
			n.state.MC.ConfigTerm, n.state.MC.ConfigVersion, n.state.MC.PeerNames.Len())
	}
	live := specularMC(n, "n0", "n3", "n4")
	live.ConfigTerm, live.ConfigVersion = 4, 3
	if !live.VersionGT(n.state.MC) {
		t.Fatal("the live config does not outrank a zapped one")
	}

	// and a populated set is never replaced by an empty one from a peer.
	m := specularNode(t, "n0", LEADER, 4, []int64{4}, "n0", "n3", "n4")
	m.state.MC.ConfigTerm, m.state.MC.ConfigVersion = 4, 3
	empty := m.NewMemberConfig("peer")
	empty.ConfigTerm, empty.ConfigVersion = 5, 0
	m.becomeFollower(4, empty, false) // same term: the empty config still ranks higher
	if m.state.MC.PeerNames.Len() != 3 {
		t.Fatal("a peer's empty config replaced our populated member set")
	}
}

// specularParDiskFixture writes count identified WAL entries
// (as par_test.go's parDiskFixture, which needs -tags par).
func specularParDiskFixture(t *testing.T, count int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tube.wal.msgp")
	wal, err := (&TubeConfig{}).newRaftWriteAheadLog(path, false)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < count; i++ {
		e := &RaftLogEntry{Index: int64(i + 1), Term: 1, Ticket: &Ticket{TicketID: fmt.Sprintf("ticket-%d", i),
			ClusterID: "par-test", Op: WRITE, Key: Key(fmt.Sprintf("k%d", i)), Val: Val("original")}}
		if _, err := wal.saveRaftLogEntry(e); err != nil {
			t.Fatal(err)
		}
	}
	if err := wal.close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// CR-D-49 / CR-9: a crash between a WAL append's fsync and the
// publication of its identifiers left an unidentified tail that
// fenced the node forever (no vote, no campaign, no donor).
// Entries above every protected floor were never acknowledged:
// the node may identify its own checksum-valid tail locally.
func TestInterruptedAppendTailIsIdentifiedLocally(t *testing.T) {
	for _, floor := range []int64{2, 4} {
		t.Run(fmt.Sprintf("floor%v", floor), func(t *testing.T) {
			path := specularParDiskFixture(t, 4)
			// crash image: WAL fsynced, only 2 of 4 identifiers published.
			if err := os.Truncate(path+".parlog", 2*maxParRecord); err != nil {
				t.Fatal(err)
			}
			cfg := &TubeConfig{ClusterID: "par-test"}
			wal, err := cfg.openPARWAL(path)
			if err != nil {
				t.Fatal(err)
			}
			mc := &MemberConfig{PeerNames: NewOmap[string, *PeerDetail]()}
			mc.PeerNames.Set("solo", &PeerDetail{Name: "solo", PeerID: "id-solo"})
			node := &TubeNode{cfg: *cfg, name: "solo", PeerID: "id-solo", ClusterID: "par-test",
				PeerServiceName: TUBE_REPLICA, wal: wal,
				state: &RaftState{CurrentTerm: 1, CommitIndex: floor, LastApplied: 2, LastAppliedTerm: 1, MC: mc}}
			defer func() { node.wal.close() }()
			node.parAfterRestore()
			if !node.parBlocked() {
				t.Fatal("setup: expected the unidentified tail to fence the node")
			}
			for i := 0; i < 10 && node.parBlocked(); i++ {
				node.parRecoveryStep()
			}
			if floor == 2 {
				if node.parBlocked() || node.wal.lli != 4 {
					t.Fatalf("single node stayed fenced by its own interrupted append (lli %v, err %v)", node.wal.lli, node.par.err)
				}
			} else if !node.parBlocked() {
				t.Fatal("identified locally a tail at or below the protected floor; that needs a checked peer")
			}
		})
	}
}

// Regression from ee975fec: VersionEqual started comparing voter
// sets and dereferenced a nil PeerNames, which crashed callers
// that compare bare member configs (e.g. PAR snapshot admission).
func TestVersionEqualToleratesNilPeerNames(t *testing.T) {
	a := &MemberConfig{ConfigTerm: 4, ConfigVersion: 2}
	b := &MemberConfig{ConfigTerm: 4, ConfigVersion: 2}
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("comparing member configs without PeerNames panicked: %v", r)
		}
	}()
	if !a.VersionEqual(b) {
		t.Fatal("two empty configs at the same version should be equal")
	}
	b.PeerNames = NewOmap[string, *PeerDetail]()
	b.PeerNames.Set("x", &PeerDetail{Name: "x"})
	if a.VersionEqual(b) {
		t.Fatal("an empty set equals a populated one")
	}
}

// CR-D-50: a lagging follower copies MC.CommitIndex from a newer
// leader before catching up. Restarted then, it was fenced as PAR
// unknown (no vote, no campaign) although its disk was intact.
// Fence only on a locally earned witness of durable appends.
func TestLaggingFollowerRestartIsNotFenced(t *testing.T) {
	for _, tc := range []struct {
		name   string
		high   int64 // highest index this replica durably appended
		fenced bool
	}{
		{"never held the entries", 2, false},
		{"lost a suffix it had appended", 5, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := specularParDiskFixture(t, 2)
			cfg := &TubeConfig{ClusterID: "par-test"}
			wal, err := cfg.openPARWAL(path)
			if err != nil {
				t.Fatal(err)
			}
			mc := &MemberConfig{PeerNames: NewOmap[string, *PeerDetail](), CommitIndex: 5}
			mc.PeerNames.Set("f", &PeerDetail{Name: "f", PeerID: "id-f"})
			node := &TubeNode{cfg: *cfg, name: "f", PeerID: "id-f", ClusterID: "par-test",
				PeerServiceName: TUBE_REPLICA, wal: wal,
				state: &RaftState{CurrentTerm: 1, CommitIndex: 2, LastApplied: 2, LastAppliedTerm: 1, MC: mc,
					DurableAppendHigh: tc.high}}
			defer func() { node.wal.close() }()
			node.parAfterRestore()
			if node.parBlocked() != tc.fenced {
				t.Fatalf("fenced=%v, want %v (lli %v, MC.CommitIndex 5, durable high %v)",
					node.parBlocked(), tc.fenced, node.wal.lli, tc.high)
			}
		})
	}
}

// CR-D-47: two multipart snapshots arriving interleaved (two
// circuits) kept discarding each other in the single reassembly
// buffer, so neither was ever installed.
func TestInterleavedMultipartSnapshotsBothAssemble(t *testing.T) {
	members := []string{"n1", "n2", "n3"}
	lead := specularNode(t, "n2", LEADER, 1, []int64{1, 1, 1}, members...)
	lead.state.CommitIndex, lead.state.CommitIndexEntryTerm = 1, 1
	specularMarkApplied(lead, 1)
	lead.leaderID, lead.leaderName = lead.PeerID, lead.name
	older := lead.getStateSnapshot(false)
	_, err := lead.wal.saveRaftLogEntry(&RaftLogEntry{Index: 4, Term: 1,
		Ticket: &Ticket{Op: NOOP, LogIndex: 4, Term: 1, Desc: "n2 entry 4"}})
	panicOn(err)
	newer := lead.getStateSnapshot(false)

	foll := specularNode(t, "n1", FOLLOWER, 1, nil, members...)
	foll.leaderID, foll.leaderName = lead.PeerID, lead.name
	foll.host = &ackCaptureHost{hoster: foll}

	parts := func(snap *Snapshot) []*rpc.Fragment {
		bts, err := snap.MarshalMsg(nil)
		panicOn(err)
		h := blake3.New(64, nil)
		h.Write(bts)
		whole := blake3ToString33B(h)
		third := len(bts) / 3
		chunks := [][]byte{bts[:third], bts[third : 2*third], bts[2*third:]}
		var frags []*rpc.Fragment
		for i, c := range chunks {
			f := lead.newFrag()
			f.FromPeerID, f.FromPeerName = lead.PeerID, lead.name
			f.FragPart = int64(i + 1)
			if i == len(chunks)-1 {
				f.FragPart = -f.FragPart
			}
			f.Payload = c
			f.SetUserArg("b3checksumWhole", whole)
			frags = append(frags, f)
		}
		return frags
	}
	a, b := parts(newer), parts(older)
	cktA := &rpc.Circuit{CircuitID: "ckt_a", RemotePeerID: lead.PeerID}
	cktB := &rpc.Circuit{CircuitID: "ckt_b", RemotePeerID: lead.PeerID}
	for i := range a {
		foll.handleStateSnapshotEnclosed(a[i], cktA, "test a")
		foll.handleStateSnapshotEnclosed(b[i], cktB, "test b")
	}
	if lli := foll.wal.LastLogIndex(); lli != 4 {
		t.Fatalf("interleaved multipart snapshot never installed: follower lli %v, want 4", lli)
	}
}

// CR-D-27: the P1 prefix-quorum reconfiguration guard returned
// true whenever s.par was nil (every NoDisk node), so the default
// suite never exercised production reconfiguration admission.
func TestReconfigurationGuardAppliesWithoutPAR(t *testing.T) {
	n := specularNode(t, "n0", LEADER, 1, []int64{1, 1, 1, 1, 1}, "n0", "n1", "n2")
	n.state.CommitIndex, n.state.CommitIndexEntryTerm = 5, 1
	if n.par != nil {
		t.Fatal("setup: expected a NoDisk node without PAR state")
	}
	// no voter but the leader holds the committed prefix through 5.
	if n.parReconfigurationReady(n.state.MC) {
		t.Fatal("reconfiguration admitted without a quorum holding the committed prefix")
	}
	for _, name := range []string{"n1"} {
		cktP := &cktPlus{node: n, PeerName: name, PeerID: name + "_id", PeerServiceName: TUBE_REPLICA,
			ckt: &rpc.Circuit{CircuitID: name}, MC: n.state.MC.Clone()}
		n.cktAllByName[name] = cktP
		info := n.newRaftNodeInfo(name+"_id", name, TUBE_REPLICA, "")
		info.MatchIndex, info.LargestCommonRaftIndex = 5, 5
		n.peers[name+"_id"] = info
	}
	if !n.parReconfigurationReady(n.state.MC) {
		t.Fatal("reconfiguration refused although a quorum holds the committed prefix")
	}
}
