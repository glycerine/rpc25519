package hermes

import (
	"errors"
	"testing"
	"time"

	"github.com/glycerine/rpc25519/tube"
)

func Test011_local_requests_require_operating_lease(t *testing.T) {
	cfg := &HermesConfig{
		ReplicationDegree:  1,
		MessageLossTimeout: time.Second,
		TCPonly_no_TLS:     true,
		testName:           t.Name(),
	}
	n := NewHermesNode("lease_gate", cfg)
	n.PeerID = "lease_gate_peer"

	done := make(chan error, 1)
	go func() {
		done <- n.Write("k", []byte("v"), 0)
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrNoOperatingLease) {
			t.Fatalf("Write without an operating lease returned %v, want %v", err, ErrNoOperatingLease)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("Write without an operating lease blocked")
	}

	readDone := make(chan error, 1)
	go func() {
		_, err := n.Read("k", 0)
		readDone <- err
	}()
	select {
	case err := <-readDone:
		if !errors.Is(err, ErrNoOperatingLease) {
			t.Fatalf("Read without an operating lease returned %v, want %v", err, ErrNoOperatingLease)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("Read without an operating lease blocked")
	}

	rmwDone := make(chan error, 1)
	go func() {
		_, err := n.ReadModifyWrite("k", func(old Val) Val {
			return []byte("next")
		}, 0)
		rmwDone <- err
	}()
	select {
	case err := <-rmwDone:
		if !errors.Is(err, ErrNoOperatingLease) {
			t.Fatalf("ReadModifyWrite without an operating lease returned %v, want %v", err, ErrNoOperatingLease)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("ReadModifyWrite without an operating lease blocked")
	}
}

func Test012_single_replica_write_and_rmw_complete_locally(t *testing.T) {
	cfg := &HermesConfig{
		ReplicationDegree:  1,
		MessageLossTimeout: time.Second,
		TCPonly_no_TLS:     true,
		testName:           t.Name(),
	}
	n := NewHermesNode("single_replica", cfg)
	n.PeerID = "single_replica_peer"
	n.operLeaseUntilTm = time.Now().Add(time.Minute)

	first := n.NewHermesTicket(WRITE, "k", []byte("one"), n.PeerID, 0)
	n.writeReq(first)
	select {
	case <-first.Done.Chan:
		if first.Err != nil {
			t.Fatalf("initial Write failed: %v", first.Err)
		}
	default:
		t.Fatalf("initial single-replica Write did not complete immediately")
	}

	second := n.NewHermesTicket(WRITE, "k", []byte("two"), n.PeerID, 0)
	n.writeReq(second)
	select {
	case <-second.Done.Chan:
		if second.Err != nil {
			t.Fatalf("second Write failed: %v", second.Err)
		}
	case <-time.After(10 * time.Millisecond):
		t.Fatalf("second single-replica Write blocked waiting for ACKs")
	}

	keym := n.store["k"]
	if keym.TS.Version != 2 {
		t.Fatalf("second write version = %v, want 2", keym.TS.Version)
	}

	rmw := n.NewHermesTicket(RMW, "k", nil, n.PeerID, 0)
	rmw.RMWFunc = func(old Val) Val {
		if string(old) != "two" {
			t.Errorf("RMW saw %q, want %q", string(old), "two")
		}
		return []byte("three")
	}
	n.writeReq(rmw)
	select {
	case <-rmw.Done.Chan:
		if rmw.Err != nil {
			t.Fatalf("ReadModifyWrite failed: %v", rmw.Err)
		}
	case <-time.After(10 * time.Millisecond):
		t.Fatalf("single-replica ReadModifyWrite blocked waiting for ACKs")
	}
	if string(rmw.Val) != "three" {
		t.Fatalf("ReadModifyWrite returned %q, want %q", string(rmw.Val), "three")
	}
	if keym.TS.Version != 3 {
		t.Fatalf("RMW version = %v, want 3", keym.TS.Version)
	}
}

func Test013_equal_timestamp_inv_is_ack_only(t *testing.T) {
	cfg := &HermesConfig{
		ReplicationDegree:  2,
		MessageLossTimeout: time.Second,
		TCPonly_no_TLS:     true,
		testName:           t.Name(),
	}
	n := NewHermesNode("equal_inv", cfg)
	n.PeerID = "node_a"
	n.operLeaseUntilTm = time.Now().Add(time.Minute)
	ts := TS{Version: 2, CoordID: "node_b"}
	n.store["k"] = &KeyMeta{
		Key:          "k",
		TS:           ts,
		State:        sInvalid,
		LastWriterID: "node_b",
		Val:          []byte("installed"),
	}

	err := n.recvInvalidate(&INV{
		Key:      "k",
		FromID:   "node_b",
		EpochV:   n.EpochV,
		TS:       ts,
		Val:      []byte("duplicate-but-different"),
		TicketID: "duplicate-ticket",
	})
	if err != nil {
		t.Fatalf("recvInvalidate returned %v", err)
	}

	keym := n.store["k"]
	if string(keym.Val) != "installed" {
		t.Fatalf("equal timestamp INV rewrote value to %q", string(keym.Val))
	}
	if keym.State != sInvalid {
		t.Fatalf("equal timestamp INV changed state to %v", stateString(keym.State))
	}
}

func Test014_overwritten_write_completion_does_not_publish_lower_value(t *testing.T) {
	orig := useBcastAckOptimization
	useBcastAckOptimization = false
	defer func() {
		useBcastAckOptimization = orig
	}()

	cfg := &HermesConfig{
		ReplicationDegree:  2,
		MessageLossTimeout: time.Second,
		TCPonly_no_TLS:     true,
		testName:           t.Name(),
	}
	n := NewHermesNode("overwritten", cfg)
	n.PeerID = "node_a"
	n.operLeaseUntilTm = time.Now().Add(time.Minute)
	lowTS := TS{Version: 2, CoordID: "node_a"}
	highTS := TS{Version: 4, CoordID: "node_b"}
	keym := &KeyMeta{
		Key:          "k",
		TS:           highTS,
		State:        sInvalidWR,
		LastWriterID: "node_b",
		Val:          []byte("higher"),
	}
	n.store["k"] = keym

	tkt := n.NewHermesTicket(WRITE, "k", []byte("lower"), n.PeerID, 0)
	tkt.TS = lowTS
	tkt.keym = keym
	tkt.ackVector[n.PeerID] = true
	n.actionAbRecordPending(tkt)

	err := n.recvAck(&ACK{
		Key:      "k",
		FromID:   "node_b",
		EpochV:   n.EpochV,
		TS:       lowTS,
		TicketID: tkt.TicketID,
	})
	if err != nil {
		t.Fatalf("recvAck returned %v", err)
	}
	select {
	case <-tkt.Done.Chan:
		if tkt.Err != nil {
			t.Fatalf("overwritten write completed with error: %v", tkt.Err)
		}
	default:
		t.Fatalf("overwritten lower write did not complete after full ACK set")
	}
	if keym.TS != highTS {
		t.Fatalf("overwritten completion changed key TS to %v, want %v", keym.TS, highTS)
	}
	if string(keym.Val) != "higher" {
		t.Fatalf("overwritten completion published lower value %q", string(keym.Val))
	}
	if len(n.sentVALIDATEs) != 0 {
		t.Fatalf("overwritten completion sent %v VALIDATE messages", len(n.sentVALIDATEs))
	}
}

func Test015_validate_current_does_not_complete_lower_pending(t *testing.T) {
	cfg := &HermesConfig{
		ReplicationDegree:  2,
		MessageLossTimeout: time.Second,
		TCPonly_no_TLS:     true,
		testName:           t.Name(),
	}
	n := NewHermesNode("validate_lower_pending", cfg)
	n.PeerID = "node_a"
	n.operLeaseUntilTm = time.Now().Add(time.Minute)
	lowTS := TS{Version: 2, CoordID: "node_a"}
	highTS := TS{Version: 4, CoordID: "node_b"}
	keym := &KeyMeta{
		Key:          "k",
		TS:           highTS,
		State:        sInvalidWR,
		LastWriterID: "node_b",
		Val:          []byte("higher"),
	}
	n.store["k"] = keym

	tkt := n.NewHermesTicket(WRITE, "k", []byte("lower"), n.PeerID, 0)
	tkt.TS = lowTS
	tkt.keym = keym
	tkt.ackVector[n.PeerID] = true
	n.actionAbRecordPending(tkt)

	err := n.recvValidate(&VALIDATE{
		Key:      "k",
		FromID:   "node_b",
		EpochV:   n.EpochV,
		TS:       highTS,
		TicketID: "higher-ticket",
	})
	if err != nil {
		t.Fatalf("recvValidate returned %v", err)
	}
	if keym.State != sValid {
		t.Fatalf("VALIDATE current left key state %v, want sValid", stateString(keym.State))
	}
	select {
	case <-tkt.Done.Chan:
		t.Fatalf("VALIDATE for higher timestamp completed lower pending write")
	default:
	}
	if _, ok := n.getTicket(tkt.TicketID); !ok {
		t.Fatalf("VALIDATE for higher timestamp deleted lower pending write")
	}
	if string(keym.Val) != "higher" {
		t.Fatalf("VALIDATE changed key value to %q", string(keym.Val))
	}
}

func Test016_membership_change_drains_write_waiting_only_on_removed_member(t *testing.T) {
	orig := useBcastAckOptimization
	useBcastAckOptimization = false
	defer func() {
		useBcastAckOptimization = orig
	}()

	cfg := &HermesConfig{
		ReplicationDegree:  2,
		MessageLossTimeout: time.Second,
		TCPonly_no_TLS:     true,
		testName:           t.Name(),
	}
	n := NewHermesNode("membership_drain", cfg)
	n.PeerID = "node_a"
	n.liveNodes = []string{"node_a", "node_b"}
	n.operLeaseUntilTm = time.Now().Add(time.Minute)
	ts := TS{Version: 2, CoordID: "node_a"}
	keym := &KeyMeta{
		Key:          "k",
		TS:           ts,
		State:        sWrite,
		LastWriterID: "node_a",
		Val:          []byte("value"),
	}
	n.store["k"] = keym

	tkt := n.NewHermesTicket(WRITE, "k", []byte("value"), n.PeerID, 0)
	tkt.TS = ts
	tkt.keym = keym
	tkt.ackVector[n.PeerID] = true
	n.actionAbRecordPending(tkt)

	reply := &tube.PingReply{
		Members: &tube.ReliableMembershipList{
			PeerNames: tube.NewOmap[string, *tube.PeerDetailPlus](),
		},
		Vers: &tube.RMVersionTuple{
			CzarLeaseEpoch:    7,
			WithinCzarVersion: 3,
		},
	}
	reply.Members.PeerNames.Set("node_a", &tube.PeerDetailPlus{
		Det: &tube.PeerDetail{Name: "node_a", PeerID: "node_a"},
	})

	n.applyMembershipChange(reply)

	select {
	case <-tkt.Done.Chan:
		if tkt.Err != nil {
			t.Fatalf("write completed with error after membership change: %v", tkt.Err)
		}
	default:
		t.Fatalf("membership change did not drain ready write")
	}
	if keym.State != sValid {
		t.Fatalf("membership change left key state %v, want sValid", stateString(keym.State))
	}
	if !n.EpochV.Equal(&EpochVers{Epoch: 7, Version: 3}) {
		t.Fatalf("EpochV = %v, want epoch 7 version 3", n.EpochV.String())
	}
}

func Test017_completed_rmw_version_conflict_aborts_ready_rmw(t *testing.T) {
	cfg := &HermesConfig{
		ReplicationDegree:  1,
		MessageLossTimeout: time.Second,
		TCPonly_no_TLS:     true,
		testName:           t.Name(),
	}
	n := NewHermesNode("rmw_conflict", cfg)
	n.PeerID = "node_a"
	ts := TS{Version: 1, CoordID: "node_a"}
	keym := &KeyMeta{
		Key:          "k",
		TS:           ts,
		State:        sWrite,
		LastWriterID: "node_a",
		Val:          []byte("candidate"),
		IsRMW:        true,
	}
	n.store["k"] = keym
	n.completedRMWVersion = map[int64]TS{
		1: {Version: 1, CoordID: "node_b"},
	}

	tkt := n.NewHermesTicket(RMW, "k", []byte("candidate"), n.PeerID, 0)
	tkt.TS = ts
	tkt.keym = keym
	tkt.ackVector[n.PeerID] = true
	tkt.Ready = true
	n.actionAbRecordPending(tkt)

	if !n.completeReady(tkt) {
		t.Fatalf("completeReady did not consume conflicted RMW")
	}
	select {
	case <-tkt.Done.Chan:
	default:
		t.Fatalf("conflicted RMW ticket was not completed")
	}
	if !errors.Is(tkt.Err, ErrAbortRMW) {
		t.Fatalf("conflicted RMW error = %v, want %v", tkt.Err, ErrAbortRMW)
	}
	if keym.State == sValid {
		t.Fatalf("conflicted RMW made key valid")
	}
	if string(keym.Val) != "candidate" {
		t.Fatalf("conflicted RMW unexpectedly rewrote value")
	}
}

func Test018_o3_pseudotickets_are_disabled_by_default(t *testing.T) {
	orig := useBcastAckOptimization
	useBcastAckOptimization = true
	defer func() {
		useBcastAckOptimization = orig
	}()

	cfg := &HermesConfig{
		ReplicationDegree:  3,
		MessageLossTimeout: time.Second,
		TCPonly_no_TLS:     true,
		testName:           t.Name(),
	}
	n := NewHermesNode("o3_disabled", cfg)
	n.PeerID = "node_a"

	err := n.recvAck(&ACK{
		Key:      "k",
		FromID:   "node_b",
		EpochV:   n.EpochV,
		TS:       TS{Version: 2, CoordID: "node_c"},
		TicketID: "foreign-ticket",
	})
	if err != ErrKeyNotFound {
		t.Fatalf("unknown ACK returned %v, want %v while O3 is disabled", err, ErrKeyNotFound)
	}
	if len(n.tkt2item) != 0 {
		t.Fatalf("O3 disabled by default but stored %v pseudo-ticket(s)", len(n.tkt2item))
	}
}

func Test019_timeout_does_not_complete_while_missing_member_is_still_live(t *testing.T) {
	orig := useBcastAckOptimization
	useBcastAckOptimization = false
	defer func() {
		useBcastAckOptimization = orig
	}()

	cfg := &HermesConfig{
		ReplicationDegree:  2,
		MessageLossTimeout: time.Millisecond,
		TCPonly_no_TLS:     true,
		testName:           t.Name(),
	}
	n := NewHermesNode("timeout_membership", cfg)
	n.PeerID = "node_a"
	n.liveNodes = []string{"node_a", "node_b"}
	n.operLeaseUntilTm = time.Now().Add(time.Minute)
	ts := TS{Version: 2, CoordID: "node_a"}
	keym := &KeyMeta{
		Key:          "k",
		TS:           ts,
		State:        sWrite,
		LastWriterID: "node_a",
		Val:          []byte("value"),
	}
	n.store["k"] = keym

	tkt := n.NewHermesTicket(WRITE, "k", []byte("value"), n.PeerID, 0)
	tkt.TS = ts
	tkt.keym = keym
	tkt.ackVector[n.PeerID] = true
	n.actionAbRecordPending(tkt)
	tkt.messageLossTimeout = time.Now().Add(-time.Millisecond)
	if item, ok := n.tkt2item[tkt.TicketID]; ok {
		item.priority = tkt.messageLossTimeout
	}

	n.checkCoordOrFollowerFailed()

	select {
	case <-tkt.Done.Chan:
		t.Fatalf("timeout completed write while node_b is still live")
	default:
	}
	if keym.State == sValid {
		t.Fatalf("timeout made key valid while node_b is still live")
	}
	if len(n.sentINVs) == 0 {
		t.Fatalf("timeout did not retransmit INV")
	}
}

func Test020_o3_waits_for_matching_ack_quorum_before_valid(t *testing.T) {
	orig := useBcastAckOptimization
	useBcastAckOptimization = true
	defer func() {
		useBcastAckOptimization = orig
	}()

	cfg := &HermesConfig{
		ReplicationDegree:  2,
		MessageLossTimeout: time.Second,
		TCPonly_no_TLS:     true,
		EnableO3:           true,
		testName:           t.Name(),
	}
	n := NewHermesNode("o3_guarded", cfg)
	n.PeerID = "node_b"
	n.liveNodes = []string{"node_a", "node_b"}
	n.operLeaseUntilTm = time.Now().Add(time.Minute)
	ts := TS{Version: 2, CoordID: "node_a"}
	inv := &INV{
		Key:      "k",
		FromID:   "node_a",
		EpochV:   n.EpochV,
		TS:       ts,
		Val:      []byte("value"),
		TicketID: "write-ticket",
	}

	if err := n.recvInvalidate(inv); err != nil {
		t.Fatalf("recvInvalidate returned %v", err)
	}
	keym := n.store["k"]
	if keym == nil {
		t.Fatalf("INV did not install key metadata")
	}
	if keym.State == sValid {
		t.Fatalf("O3 made key valid before matching ACK quorum")
	}

	if err := n.recvAck(&ACK{
		Key:      "k",
		FromID:   "node_b",
		EpochV:   n.EpochV,
		TS:       ts,
		TicketID: inv.TicketID,
	}); err != nil {
		t.Fatalf("recvAck returned %v", err)
	}
	if keym.State != sValid {
		t.Fatalf("O3 did not make key valid after matching ACK quorum; state %v", stateString(keym.State))
	}
	if string(keym.Val) != "value" {
		t.Fatalf("O3 changed value to %q", string(keym.Val))
	}
}

func Test021_o3_completes_when_inv_arrives_after_ack_quorum(t *testing.T) {
	orig := useBcastAckOptimization
	useBcastAckOptimization = true
	defer func() {
		useBcastAckOptimization = orig
	}()

	cfg := &HermesConfig{
		ReplicationDegree:  2,
		MessageLossTimeout: time.Second,
		TCPonly_no_TLS:     true,
		EnableO3:           true,
		testName:           t.Name(),
	}
	n := NewHermesNode("o3_reordered", cfg)
	n.PeerID = "node_b"
	n.liveNodes = []string{"node_a", "node_b"}
	n.operLeaseUntilTm = time.Now().Add(time.Minute)
	ts := TS{Version: 2, CoordID: "node_a"}
	ticketID := "write-ticket"

	if err := n.recvAck(&ACK{
		Key:      "k",
		FromID:   "node_b",
		EpochV:   n.EpochV,
		TS:       ts,
		TicketID: ticketID,
	}); err != nil {
		t.Fatalf("recvAck before INV returned %v", err)
	}
	if _, ok := n.store["k"]; ok {
		t.Fatalf("ACK before INV created key metadata")
	}

	if err := n.recvInvalidate(&INV{
		Key:      "k",
		FromID:   "node_a",
		EpochV:   n.EpochV,
		TS:       ts,
		Val:      []byte("value"),
		TicketID: ticketID,
	}); err != nil {
		t.Fatalf("recvInvalidate returned %v", err)
	}
	keym := n.store["k"]
	if keym == nil {
		t.Fatalf("INV did not install key metadata")
	}
	if keym.State != sValid {
		t.Fatalf("O3 did not complete after reordered ACK quorum and INV; state %v", stateString(keym.State))
	}
}

func Test022_distributed_rmw_replicates_and_advances_by_one(t *testing.T) {
	hermesBubble(t, func(t *testing.T) {
		n := 3
		cfg := &HermesConfig{
			ReplicationDegree:  n,
			MessageLossTimeout: time.Second * 5,
			TCPonly_no_TLS:     true,
			testName:           t.Name(),
		}
		c := newHermesTestCluster(cfg)
		nodes := c.Nodes
		c.Start()
		defer c.Close()

		if err := nodes[0].Write("k", []byte("one"), 0); err != nil {
			t.Fatalf("initial write failed: %v", err)
		}
		base := nodes[0].store["k"].TS
		if base.Version != 2 {
			t.Fatalf("initial write version = %v, want 2", base.Version)
		}

		got, err := nodes[1].ReadModifyWrite("k", func(old Val) Val {
			if string(old) != "one" {
				t.Fatalf("RMW saw %q, want one", string(old))
			}
			return []byte("two")
		}, 0)
		if err != nil {
			t.Fatalf("ReadModifyWrite failed: %v", err)
		}
		if string(got) != "two" {
			t.Fatalf("ReadModifyWrite returned %q, want two", string(got))
		}

		for i, node := range nodes {
			val, err := node.Read("k", 0)
			if err != nil {
				t.Fatalf("node %v read after RMW failed: %v", i, err)
			}
			if string(val) != "two" {
				t.Fatalf("node %v read %q after RMW, want two", i, string(val))
			}
			keym := node.store["k"]
			if keym.TS.Version != base.Version+1 {
				t.Fatalf("node %v RMW version = %v, want %v", i, keym.TS.Version, base.Version+1)
			}
			if !keym.IsRMW {
				t.Fatalf("node %v key metadata did not retain RMW flag", i)
			}
		}
	})
}
