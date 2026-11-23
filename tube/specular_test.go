package tube

// Regression tests for the bugs found by Specular
// (see ../bug-report.md). Each test first reproduced
// its bug (red), then went green with the fix.

import (
	"context"
	"fmt"
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
// that has stopped (SIGSTOP, wedged VM, half-open TCP to a
// powered-off host): the hand-off of a message to that
// connection's writer never completes.
type stuckTransport struct {
	rpc.UniversalCliSrv // only SendOneWayMessage is used.
	release             chan struct{}
	entered             atomic.Int64
}

func (u *stuckTransport) SendOneWayMessage(ctx context.Context, msg *rpc.Message, errWriteDur time.Duration, forbidAutoCli bool) (bool, *rpc.LoopComm, error) {
	u.entered.Add(1)
	<-u.release
	return false, nil, nil
}

// Bug 8 (High): every event-loop send used SendOneWay with
// errWriteDur = -1, whose hand-off to the connection blocks
// with no timeout. One stopped peer therefore froze the
// single Tube event loop of every live replica that tried
// to send to it, and a healthy majority never elected a
// leader. A fire-and-forget send must never block the
// event loop.
func Test1008_send_to_stuck_peer_does_not_block_event_loop(t *testing.T) {

	node := specularNode(t, "n1", FOLLOWER, 1, []int64{1}, "n1", "n2", "n3")
	defer node.Halt.ReqStop.Close()

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
		t.Fatalf("Bug 8: the event loop is blocked sending to a stopped peer (hand-off entered %v times)", u.entered.Load())
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
