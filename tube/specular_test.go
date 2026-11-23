package tube

// Regression tests for the bugs found by Specular
// (see ../bug-report.md). Each test first reproduced
// its bug (red), then went green with the fix.

import (
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glycerine/idem"
	rpc "github.com/glycerine/rpc25519"
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
