//go:build par

package tube

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc64"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glycerine/blake3"
	rpc "github.com/glycerine/rpc25519"
	"github.com/glycerine/vfs"
	"github.com/glycerine/yogadb"
)

// PAR paper, figure 1 test scenarios
type parTestServer struct {
	serverNum int
	crashed   bool
	lagging   bool
	faulty    [3]bool // wal has 3 entries
}

// PAR paper, figure 1 test scenarios
type parTestScenario struct {
	scenario int
	node     []*parTestServer
}

// PAR paper, figure 1 test scenarios
func genParTestScenarios() (r []*parTestScenario) {

	for i := range 6 {
		s := &parTestScenario{
			scenario: i,
		}
		r = append(r, s)
		for j := range 5 {
			srv := &parTestServer{serverNum: j}
			s.node = append(s.node, srv)
		}
		switch i {
		case 0:
			s.node[0].faulty[0] = true
			s.node[3].crashed = true
			s.node[4].crashed = true
		case 1:
			s.node[0].faulty[0] = true
			s.node[3].lagging = true
			s.node[4].lagging = true
		case 2:
			s.node[0].faulty[0] = true
			s.node[1].faulty[1] = true
			s.node[4].crashed = true
		case 3:
			s.node[0].faulty[0] = true
			s.node[1].faulty[1] = true
			s.node[4].lagging = true
		case 4:
			s.node[0].faulty[0] = true
			s.node[1].faulty[1] = true
			s.node[2].faulty[2] = true
		case 5:
			s.node[0].faulty[0] = true
			s.node[1].faulty[1] = true
			s.node[2].faulty[2] = true
			s.node[3].faulty[0] = true
			s.node[4].faulty[1] = true
		}
	}
	return
}

func TestPar_crash_vs_disk_fault(t *testing.T) {
	for _, which := range []string{"first", "middle", "last", "missing_suffix", "partial_identifier", "unbounded_frame"} {
		t.Run(which, func(t *testing.T) {
			bubbleOrNot(t, func(t *testing.T) {
				path, entries := parDiskFixture(t, 4)
				before, err := scanParWAL(path, "par-test")
				if err != nil || before.Err != nil {
					t.Fatalf("initial inventory: %v, %v", err, before)
				}
				for i, e := range before.Entries {
					if e.Status != parIntact {
						t.Fatalf("entry %d: %v", i, e.Err)
					}
				}
				bad := 0
				switch which {
				case "middle":
					bad = 2
				case "last", "missing_suffix":
					bad = 3
				}
				record := *before.Entries[bad].Record
				original, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if which == "partial_identifier" {
					if err := os.Truncate(path+".parlog", 3*maxParRecord+19); err != nil {
						t.Fatal(err)
					}
					inv, err := scanParWAL(path, "par-test")
					if err != nil || inv.Err == nil || len(inv.Entries) != 3 {
						t.Fatalf("partial identifier: %#v, %v", inv, err)
					}
					return
				}
				if which == "missing_suffix" {
					if err := os.Truncate(path, record.Offset); err != nil {
						t.Fatal(err)
					}
				} else {
					fd, err := os.OpenFile(path, os.O_RDWR, 0)
					if err != nil {
						t.Fatal(err)
					}
					b := []byte{0}
					if which == "unbounded_frame" {
						b = []byte{bin32, 255, 255, 255, 255}
					}
					_, err = fd.WriteAt(b, record.Offset)
					fd.Close()
					if err != nil {
						t.Fatal(err)
					}
				}
				inv, err := scanParWAL(path, "par-test")
				if err != nil {
					t.Fatal(err)
				}
				if len(inv.Entries) != 4 || inv.Entries[bad].Status != parDamaged {
					t.Fatalf("lost damaged identity: %#v", inv)
				}
				for i, e := range inv.Entries {
					if i != bad && e.Entry == nil {
						t.Fatalf("lost healthy entry %d: %v", i, e.Err)
					}
				}
				for range 2 { // repeated repair is idempotent
					if err := repairParEntry(path, int64(bad), record, entries[bad]); err != nil {
						t.Fatal(err)
					}
				}
				after, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(after, original) {
					t.Fatalf("repair changed healthy bytes: %v", err)
				}
				wal, err := (&TubeConfig{}).newRaftWriteAheadLog(path, false)
				if err != nil {
					t.Fatal(err)
				}
				if len(wal.raftLog) != 4 {
					t.Fatal("lost history after reopen")
				}
				if err := wal.close(); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

func TestPar_append_batch_crash_images(t *testing.T) {
	for _, cut := range []string{"before_data", "during_entry", "during_checksum", "wal_synced", "partial_identifier", "one_identifier", "all_identifiers", "acknowledged_payload_loss", "acknowledged_identifier_loss"} {
		t.Run(cut, func(t *testing.T) {
			bubbleOrNot(t, func(t *testing.T) {
				path, _ := parDiskFixture(t, 4)
				donor, err := scanParWAL(path, "par-test")
				if err != nil || donor.Err != nil {
					t.Fatal("invalid donor fixture", err)
				}
				walSize, parSize := donor.WALSize, donor.ParSize
				second := donor.Entries[1].Record
				switch cut {
				case "before_data":
					walSize, parSize = second.Offset, maxParRecord
				case "during_entry":
					walSize, parSize = second.Offset+second.Len0/2, maxParRecord
				case "during_checksum":
					walSize, parSize = second.Offset+second.Len0+second.Len2/2, maxParRecord
				case "wal_synced":
					parSize = maxParRecord
				case "partial_identifier":
					parSize = maxParRecord + 19
				case "one_identifier":
					parSize = 2 * maxParRecord
				case "acknowledged_payload_loss":
					walSize = second.Offset
				case "acknowledged_identifier_loss":
					// The same observation as wal_synced, caused by corruption
					// after acknowledgment. Never infer safe truncation from it.
					parSize = maxParRecord
				}
				if err := os.Truncate(path, walSize); err != nil {
					t.Fatal(err)
				}
				if err := os.Truncate(path+".parlog", parSize); err != nil {
					t.Fatal(err)
				}
				beforeWAL, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				beforePar, err := os.ReadFile(path + ".parlog")
				if err != nil {
					t.Fatal(err)
				}
				inv, err := scanParWAL(path, "par-test")
				if err != nil {
					t.Fatal(err)
				}
				clean := cut == "before_data" || cut == "all_identifiers"
				identifiedLoss := cut == "acknowledged_payload_loss"
				if parInventoryUnknown(inv) != (!clean && !identifiedLoss) {
					t.Fatal("wrong ambiguity classification")
				}
				wal, err := (&TubeConfig{ClusterID: "par-test"}).openPARWAL(path)
				if err != nil {
					t.Fatal(err)
				}
				if (wal.parInv == nil) != clean {
					t.Fatal("ambiguous or identified damage admitted as a healthy WAL")
				}
				if err := wal.close(); err != nil {
					t.Fatal(err)
				}
				for i, suffix := range []string{"", ".parlog"} {
					got, err := os.ReadFile(path + suffix)
					if err != nil || !bytes.Equal(got, [][]byte{beforeWAL, beforePar}[i]) {
						t.Fatal("opening/closing crash image changed recovery evidence", err)
					}
				}
				if identifiedLoss {
					for slot := 1; slot < len(donor.Entries); slot++ {
						if inv.Entries[slot].Status != parDamaged || inv.Entries[slot].Record.Index != int64(slot+1) {
							t.Fatal("lost acknowledged suffix identity")
						}
						if err := repairParEntry(path, int64(slot), *donor.Entries[slot].Record, donor.Entries[slot].Bytes); err != nil {
							t.Fatal(err)
						}
					}
				} else if walSize == donor.WALSize {
					for repaired := parSize / maxParRecord; repaired < 4; repaired++ {
						slot, candidate, err := parIdentifierCandidate(path, "par-test")
						if err != nil || slot != repaired {
							t.Fatalf("candidate slot %d/%d: %v", slot, repaired, err)
						}
						if err := parInstallIdentifier(path, slot, *candidate, donor.Entries[slot].Bytes); err != nil {
							t.Fatal(err)
						}
					}
				} else {
					return // No peer proof can make incomplete local bytes intact.
				}
				recovered, err := scanParWAL(path, "par-test")
				if err != nil || recovered.Err != nil || len(recovered.Entries) != 4 {
					t.Fatal("checked recovery did not restore the batch", err)
				}
				for i, entry := range recovered.Entries {
					if entry.Status != parIntact || !bytes.Equal(entry.Bytes, donor.Entries[i].Bytes) {
						t.Fatal("batch recovery changed original command bytes")
					}
				}
			})
		})
	}
}

func TestPar_global_commitment_determination(t *testing.T) {
	tests := []struct {
		name  string
		n     int
		kinds []parEvidence
		floor int64
		want  parDecision
		err   error
	}{
		{"two_absences_wait", 5, []parEvidence{parAbsent, parAbsent}, 0, parWait, nil},
		{"three_absences_discard", 5, []parEvidence{parAbsent, parAbsent, parAbsent}, 0, parDiscard, nil},
		{"damaged_not_absent", 5, []parEvidence{parAbsent, parAbsent, parKnownDamaged}, 0, parWait, nil},
		{"compacted_not_absent", 5, []parEvidence{parAbsent, parAbsent, parCompacted}, 0, parWait, nil},
		{"one_donor_repairs", 5, []parEvidence{parValid}, 0, parRepair, nil},
		{"all_damaged_wait", 3, []parEvidence{parKnownDamaged, parKnownDamaged}, 0, parWait, nil},
		{"three_node_absence", 3, []parEvidence{parAbsent, parAbsent}, 0, parDiscard, nil},
		{"even_quorum_wait", 4, []parEvidence{parAbsent, parAbsent}, 0, parWait, nil},
		{"even_quorum_discard", 4, []parEvidence{parAbsent, parAbsent, parAbsent}, 0, parDiscard, nil},
		{"single_node_wait", 1, nil, 0, parWait, nil},
		{"committed_never_discard", 3, []parEvidence{parAbsent, parAbsent}, 1, parWait, ErrPARConflict},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bubbleOrNot(t, func(t *testing.T) {
				pr, data := parDecisionFixture(t)
				fence := parFence{Leader: "node0", Term: 2, ConfigTerm: 1, ConfigVersion: 1, Round: "query"}
				members := make(map[string]string)
				for i := 0; i < tt.n; i++ {
					members[fmt.Sprintf("node%d", i)] = fmt.Sprintf("peer%d", i)
				}
				r, err := newParRound(fence, pr, members, tt.floor)
				if err != nil {
					t.Fatal(err)
				}
				for i, kind := range tt.kinds {
					reply := parReply{Fence: fence, Member: fmt.Sprintf("node%d", i+1), PeerID: fmt.Sprintf("peer%d", i+1), Kind: kind, Record: pr}
					if kind == parValid {
						reply.Bytes = data
					}
					for range 2 {
						if err := r.add(fence, reply); err != nil {
							t.Fatal(err)
						}
					} // deduplicate
				}
				got, gotBytes, err := r.decide(fence)
				if got != tt.want || !errors.Is(err, tt.err) {
					t.Fatalf("decision=%v err=%v; want %v, %v", got, err, tt.want, tt.err)
				}
				if got == parRepair && !bytes.Equal(gotBytes, data) {
					t.Fatal("wrong repair data")
				}
				stale := fence
				stale.Term++
				if _, _, err := r.decide(stale); !errors.Is(err, ErrPARStale) {
					t.Fatal("stale round accepted")
				}
			})
		})
	}
}

func TestPar_nonvoting_donor_cannot_vote_absent(t *testing.T) {
	bubbleOrNot(t, func(t *testing.T) {
		target, data := parDecisionFixture(t)
		fence := parFence{Leader: "leader", Term: 2, Round: "shadow"}
		members := map[string]string{"leader": "l", "a": "a", "b": "b", "c": "c", "d": "d"}
		donors := map[string]string{"shadow": "s"}
		round, err := newParRound(fence, target, members, 0, donors)
		if err != nil {
			t.Fatal(err)
		}
		for _, member := range []string{"a", "b"} {
			if err := round.add(fence, parReply{Fence: fence, Member: member, PeerID: member, Kind: parAbsent, Record: target}); err != nil {
				t.Fatal(err)
			}
		}
		shadow := parReply{Fence: fence, Member: "shadow", PeerID: "s", Kind: parAbsent, Record: target}
		if err := round.add(fence, shadow); !errors.Is(err, ErrPARStale) {
			t.Fatal("nonvoting absence counted toward quorum", err)
		}
		if decision, _, err := round.decide(fence); err != nil || decision != parWait {
			t.Fatal("two voters and one shadow authorized discard", decision, err)
		}
		donors["shadow"] = "new-incarnation" // Inputs cannot mutate a pinned round.
		shadow.Kind, shadow.Bytes = parValid, data
		if err := round.add(fence, shadow); err != nil {
			t.Fatal("checked nonvoting donor rejected", err)
		}
		if decision, got, err := round.decide(fence); err != nil || decision != parRepair || !bytes.Equal(got, data) {
			t.Fatal("checked shadow copy did not repair the entry", decision, err)
		}
		shadow.PeerID = "new-incarnation"
		if err := round.add(fence, shadow); !errors.Is(err, ErrPARStale) {
			t.Fatal("unpinned shadow incarnation accepted", err)
		}
	})
}

func TestPar_traffic_does_not_establish_remote_configuration(t *testing.T) {
	bubbleOrNot(t, func(t *testing.T) {
		current := &MemberConfig{ConfigTerm: 2, ConfigVersion: 3, PeerNames: NewOmap[string, *PeerDetail]()}
		current.PeerNames.Set("peer", &PeerDetail{Name: "peer", PeerID: "peer-id"})
		old := &MemberConfig{ConfigTerm: 1, ConfigVersion: 1, PeerNames: NewOmap[string, *PeerDetail]()}
		peer := &cktPlus{MC: old, CurrentTerm: 1}
		node := &TubeNode{ClusterID: "cluster", role: FOLLOWER, par: &parNode{}, state: &RaftState{CurrentTerm: 2, MC: current},
			cktall: map[string]*cktPlus{"peer-id": peer}}
		ckt := &rpc.Circuit{RemotePeerID: "peer-id", RemotePeerName: "peer", RemoteServiceName: TUBE_REPLICA}
		msg := parWire{Version: 1, Cluster: "cluster", Member: "peer", PeerID: "peer-id",
			Fence: parFence{Leader: "requester", Term: 99, ConfigTerm: 2, ConfigVersion: 3}}
		payload, err := json.Marshal(msg)
		if err != nil {
			t.Fatal(err)
		}
		node.parHandle(&rpc.Fragment{FragOp: PARReplyMsg, Payload: payload}, ckt)
		if peer.MC != old || peer.CurrentTerm != 1 {
			t.Fatal("PAR request fence was counted as the remote peer's configuration or term")
		}
		if peer.lastSeenTm.IsZero() {
			t.Fatal("verified PAR traffic did not refresh circuit liveness")
		}
	})
}

func TestParRecord(t *testing.T) {

	// make sure our ParRecord always fits in <= maxParRecord bytes.

	finPRbytes := make([]byte, maxParRecord)

	entry := &RaftLogEntry{}
	b, err := entry.MarshalMsg(nil)
	panicOn(err)
	_ = b

	for i := range 2 {
		checkEach := blake3.New(64, nil)

		var e *ParRecord
		switch i {
		case 0:
			e = &ParRecord{
				Offset:    math.MinInt64,
				Len0:      math.MinInt64,
				Len2:      math.MinInt64,
				Index:     math.MinInt64,
				Term:      math.MinInt64,
				Epoch:     math.MinInt64,
				ClusterID: rpc.NewCallID(""),
				TicketID:  rpc.NewCallID(""),
			}
		case 1:
			e = &ParRecord{
				Offset:    math.MaxInt64,
				Len0:      math.MaxInt64,
				Len2:      math.MaxInt64,
				Index:     math.MaxInt64,
				Term:      math.MaxInt64,
				Epoch:     math.MaxInt32,
				ClusterID: rpc.NewCallID(""),
				TicketID:  rpc.NewCallID(""),
			}
		}

		by, err := e.MarshalMsg(nil)
		panicOn(err)

		// really the hash will be of the RaftLogEntry bytes we protect.
		// but anything will do for checking size of final bytes.
		checkEach.Write(by)
		e.RLEblake3 = blake3ToString33B(checkEach)

		pre, err := e.MarshalMsg(nil)
		panicOn(err)

		bs := ByteSlice(pre)
		took, err := bs.MarshalMsg(finPRbytes[:0])
		panicOn(err)

		if len(took) > maxParRecord-8 { // -8 for the 8 bytes of crc64.ECMA
			panic(fmt.Sprintf("took %v must be <= maxParRecord(%v)", len(took), maxParRecord)) // panic: took 327 must be <= maxParRecord(320)
		}
		//vv("took len %v", len(took)) // took len 274 + 8 = 280 < 320. good.
		//vv("e = '%v'", e.Gstring())
		maxsz := e.Msgsize()
		vv("e.Msgsize = '%v', and maxsize+8 < maxParRecord(%v) == %v", maxsz, maxParRecord, maxsz+8 < maxParRecord) // 282 + 8 = 290 < 320. good.
		if maxsz > maxParRecord-8 {
			panic(fmt.Sprintf("maxsz+8= %v must be <= maxParRecord(%v)", maxsz+8, maxParRecord))
		}
	}
}

// Local directories deliberately survive failing tests for post-mortem inspection.
func parTestDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp(".", "test.par.")
	if err != nil {
		t.Fatal(err)
	}
	dir, err = filepath.Abs(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("PAR fixture: %s", dir)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("retained PAR fixture: %s", dir)
			return
		}
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	return dir
}

func parDiskFixture(t *testing.T, count int) (string, [][]byte) {
	t.Helper()
	path := filepath.Join(parTestDir(t), "tube.wal.msgp")
	wal, err := (&TubeConfig{}).newRaftWriteAheadLog(path, false)
	if err != nil {
		t.Fatal(err)
	}
	var data [][]byte
	for i := 0; i < count; i++ {
		e := &RaftLogEntry{Index: int64(i + 1), Term: 1, Ticket: &Ticket{TicketID: fmt.Sprintf("ticket-%d", i), ClusterID: "par-test", Op: WRITE, Key: Key(fmt.Sprintf("k%d", i)), Val: Val("original")}}
		if _, err := wal.saveRaftLogEntry(e); err != nil {
			t.Fatal(err)
		}
		data = append(data, append([]byte(nil), e.msgbytes...))
	}
	if err := wal.close(); err != nil {
		t.Fatal(err)
	}
	return path, data
}

func TestPar_destructive_publication_crash_cuts(t *testing.T) {
	for _, cut := range []string{"prepared", "wal", "sidecar", "both", "committed", "bad_backup", "bad_manifest", "missing_manifest", "repeat_restore"} {
		t.Run(cut, func(t *testing.T) {
			bubbleOrNot(t, func(t *testing.T) {
				path, _ := parDiskFixture(t, 3)
				oldWAL, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				oldPar, err := os.ReadFile(path + ".parlog")
				if err != nil {
					t.Fatal(err)
				}
				undo, err := parBeginUndo(path)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := parBeginUndo(path); !errors.Is(err, ErrPARRecovering) {
					t.Fatalf("nested publication: %v", err)
				}
				if cut != "prepared" {
					if cut != "sidecar" {
						if err := os.WriteFile(path, nil, 0600); err != nil {
							t.Fatal(err)
						}
					}
					if cut != "wal" {
						if err := os.WriteFile(path+".parlog", nil, 0600); err != nil {
							t.Fatal(err)
						}
					}
				}
				switch cut {
				case "committed":
					if err := undo.finish(); err != nil {
						t.Fatal(err)
					}
				case "bad_backup":
					if err := os.WriteFile(filepath.Join(path+".par-undo", "1"), []byte("damaged"), 0600); err != nil {
						t.Fatal(err)
					}
				case "bad_manifest":
					if err := os.WriteFile(filepath.Join(path+".par-undo", "manifest"), []byte("damaged"), 0600); err != nil {
						t.Fatal(err)
					}
				case "missing_manifest":
					if err := os.Remove(filepath.Join(path+".par-undo", "manifest")); err != nil {
						t.Fatal(err)
					}
				case "repeat_restore":
					// A first restore installed only one file before another crash.
					if err := os.WriteFile(path, oldWAL, 0600); err != nil {
						t.Fatal(err)
					}
				}
				err = parRecoverUndo(path)
				if cut == "bad_backup" || cut == "bad_manifest" || cut == "missing_manifest" {
					if err == nil {
						t.Fatal("unusable recovery evidence accepted")
					}
					got, _ := os.ReadFile(path)
					if len(got) != 0 {
						t.Fatal("changed live data before checking all recovery evidence")
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if err := parRecoverUndo(path); err != nil {
					t.Fatalf("repeat recovery: %v", err)
				}
				for i, suffix := range []string{"", ".parlog"} {
					got, err := os.ReadFile(path + suffix)
					want := [][]byte{oldWAL, oldPar}[i]
					if cut == "committed" {
						want = nil
					}
					if err != nil || !bytes.Equal(got, want) {
						t.Fatalf("pair member %d: %v; size %d, want %d", i, err, len(got), len(want))
					}
				}
				cfg := &TubeConfig{ClusterID: "par-test"}
				wal, err := cfg.openPARWAL(path)
				if err != nil {
					t.Fatal(err)
				}
				defer wal.close()
				want := 3
				if cut == "committed" {
					want = 0
				}
				if len(wal.raftLog) != want || wal.parInv != nil {
					t.Fatalf("reopened incomplete publication: %d entries, inventory %v", len(wal.raftLog), wal.parInv)
				}
			})
		})
	}
}

func TestPar_publication_IO_failures(t *testing.T) {
	// Enumerate every actual sync and rename, then fail immediately before and
	// after each. An error after a syscall also models uncertain publication.
	for _, stage := range []string{"prepare", "commit", "restore", "prepare_snapshot", "commit_snapshot", "restore_snapshot"} {
		phase := strings.TrimSuffix(stage, "_snapshot")
		steps := 0
		run := func(t *testing.T, failAt int, after bool) {
			bubbleOrNot(t, func(t *testing.T) {
				path, _ := parDiskFixture(t, 2)
				oldWAL, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				oldPar, err := os.ReadFile(path + ".parlog")
				if err != nil {
					t.Fatal(err)
				}
				var states []*RaftState
				var oldState, newState []byte
				statePath := filepath.Join(filepath.Dir(path), "persistor.raftstate.tube.msgp")
				if phase != stage {
					state := &RaftState{CurrentTerm: 7, LastApplied: 2, LastAppliedTerm: 1, CommitIndex: 2,
						ClusterID: "par-test", PeerName: "node", VotedFor: "local-vote", VotedForName: "candidate",
						HaveVoted: true, HaveVotedTerm: 7, KVstore: newKVStore()}
					defer state.KVstore.Close()
					encode := func() []byte {
						image := state.clone()
						defer image.KVstore.Close()
						data, err := parEncodeState(image)
						if err != nil {
							t.Fatal(err)
						}
						return data
					}
					if err := state.KVstore.MakeTable("old"); err != nil {
						t.Fatal(err)
					}
					oldState = encode()
					if err := state.KVstore.MakeTable("new"); err != nil {
						t.Fatal(err)
					}
					newState = encode()
					if err := state.KVstore.DeleteTable("new"); err != nil {
						t.Fatal(err)
					}
					states = []*RaftState{state}
					if err := os.WriteFile(statePath, oldState, 0600); err != nil {
						t.Fatal(err)
					}
				}
				calls, hit := 0, false
				invoke := func(op string, fn func() error) error {
					calls++
					fail := calls == failAt
					if fail {
						hit = true
						t.Logf("injecting %s failure at operation %d, after=%v", op, calls, after)
						if !after {
							return errParTestIO
						}
					}
					if err := fn(); err != nil {
						return err
					}
					if fail {
						return errParTestIO
					}
					return nil
				}
				disk := parUndoIO{
					renameFile: func(from, to string) error {
						return invoke("rename", func() error { return os.Rename(from, to) })
					},
					syncFile: func(fd *os.File) error { return invoke("sync", fd.Sync) },
				}
				var undo *parUndo
				if phase == "prepare" {
					undo, err = parBeginUndoWithIO(path, disk, states...)
				} else {
					undo, err = parBeginUndo(path, states...)
					if err != nil {
						t.Fatal(err)
					}
					for _, suffix := range []string{"", ".parlog"} {
						if err := os.WriteFile(path+suffix, nil, 0600); err != nil {
							t.Fatal(err)
						}
					}
					if len(states) > 0 {
						if err := os.WriteFile(statePath, newState, 0600); err != nil {
							t.Fatal(err)
						}
					}
					if phase == "commit" {
						undo.io = disk
						err = undo.finish()
					} else {
						err = parRecoverUndoWithIO(path, disk)
					}
				}
				if failAt == 0 {
					if err != nil {
						t.Fatal(err)
					}
					steps = calls
				} else if !hit || !errors.Is(err, errParTestIO) {
					t.Fatalf("fault not returned: hit=%v, error=%v", hit, err)
				}
				// Pending evidence selects the old pair. Once commit retired the
				// journal, the complete replacement is valid even if sync errored.
				_, pendingErr := os.Stat(path + ".par-undo")
				wantOld := phase != "commit" || pendingErr == nil
				for range 2 {
					if err := parRecoverUndo(path); err != nil {
						t.Fatal("retry did not complete recovery", err)
					}
				}
				for i, suffix := range []string{"", ".parlog"} {
					want := [][]byte{oldWAL, oldPar}[i]
					if !wantOld {
						want = nil
					}
					got, err := os.ReadFile(path + suffix)
					if err != nil || !bytes.Equal(got, want) {
						t.Fatalf("mixed/incorrect recovered pair member %d: %v", i, err)
					}
				}
				if len(states) > 0 {
					data, err := os.ReadFile(statePath)
					if err != nil {
						t.Fatal(err)
					}
					state, err := parDecodeState(data)
					if err != nil {
						t.Fatal(err)
					}
					defer state.KVstore.Close()
					tables, _, _, err := state.KVstore.snapshotData()
					wantTables := 1
					if !wantOld {
						wantTables = 2
					}
					if err != nil || len(tables) != wantTables || state.VotedFor != "local-vote" || state.CurrentTerm != 7 {
						t.Fatal("snapshot journal lost application rows or local metadata", err)
					}
				}
				wal, err := (&TubeConfig{ClusterID: "par-test"}).openPARWAL(path)
				if err != nil {
					t.Fatal(err)
				}
				defer wal.close()
				if wal.parInv != nil || (wantOld && len(wal.raftLog) != 2) || (!wantOld && len(wal.raftLog) != 0) {
					t.Fatal("reopened incomplete publication")
				}
			})
		}
		t.Run(stage+"/baseline", func(t *testing.T) { run(t, 0, false) })
		for step := 1; step <= steps; step++ {
			for _, after := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/step_%d/after_%v", stage, step, after), func(t *testing.T) { run(t, step, after) })
			}
		}
	}
}

func TestPar_snapshot_before_image_restores_application_and_local_vote(t *testing.T) {
	for _, cut := range []string{"application", "state", "wal", "both_logs", "committed"} {
		t.Run(cut, func(t *testing.T) {
			bubbleOrNot(t, func(t *testing.T) {
				path, _ := parDiskFixture(t, 3)
				statePath := filepath.Join(filepath.Dir(path), "persistor.raftstate.tube.msgp")
				cfg := &TubeConfig{}
				saver, _, err := cfg.NewRaftStatePersistor(statePath, nil, false)
				if err != nil {
					t.Fatal(err)
				}
				defer saver.close()
				state := &RaftState{CurrentTerm: 7, LastApplied: 3, LastAppliedTerm: 1, CommitIndex: 3,
					ClusterID: "par-test", VotedFor: "local-vote", VotedForName: "candidate",
					HaveVoted: true, HaveVotedTerm: 7, KVstore: newKVStore()}
				node := &TubeNode{cfg: *cfg, state: state}
				writeIndex := int64(3)
				write := func(value string) {
					writeIndex++
					if !node.kvstoreWrite(&Ticket{Table: "data", Key: "key", Val: Val(value), LogIndex: writeIndex, RaftLogEntryTm: time.Now()}, false, false) {
						t.Fatal("fixture application write rejected")
					}
				}
				write("old")
				if _, err := saver.save(state); err != nil {
					t.Fatal(err)
				}
				undo, err := parBeginUndo(path, state)
				if err != nil {
					t.Fatal(err)
				}
				// Simulate separate durable components of snapshot installation.
				write("new")
				if err := state.KVstore.db.Sync(); err != nil {
					t.Fatal(err)
				}
				if cut != "application" {
					state.LastApplied, state.CommitIndex = 9, 9
					if _, err := saver.save(state); err != nil {
						t.Fatal(err)
					}
				}
				if cut == "wal" || cut == "both_logs" || cut == "committed" {
					if err := os.WriteFile(path, nil, 0600); err != nil {
						t.Fatal(err)
					}
				}
				if cut == "both_logs" || cut == "committed" {
					if err := os.WriteFile(path+".parlog", nil, 0600); err != nil {
						t.Fatal(err)
					}
				}
				state.KVstore.Close()
				if cut == "committed" {
					if err := undo.finish(); err != nil {
						t.Fatal(err)
					}
				}
				if err := parRecoverUndo(path); err != nil {
					t.Fatal(err)
				}
				for restart := 0; restart < 2; restart++ {
					reopened, restored, err := cfg.NewRaftStatePersistor(statePath, nil, false)
					if err != nil {
						t.Fatal(err)
					}
					want, applied := "old", int64(3)
					if cut == "committed" {
						want, applied = "new", 9
					}
					got, _, err := restored.KVStoreRead(&Ticket{}, "data", "key")
					if err != nil || string(got) != want || restored.LastApplied != applied || restored.CommitIndex != applied {
						t.Fatalf("restart %d: value %q/%q, applied %d/%d, err %v", restart, got, want, restored.LastApplied, applied, err)
					}
					if restored.CurrentTerm != 7 || restored.VotedFor != "local-vote" || !restored.HaveVoted || restored.HaveVotedTerm != 7 {
						t.Fatalf("lost local voting history: %v", restored)
					}
					if _, err := reopened.save(restored); err != nil {
						t.Fatal(err)
					}
					restored.KVstore.Close()
					reopened.close()
				}
			})
		})
	}
}

func TestPar_unreadable_application_before_image(t *testing.T) {
	for _, layout := range []string{"directory", "missing", "file"} {
		for _, cut := range []string{"prepared", "application_removed", "application_replaced", "both_logs", "committed", "restore_rename_error", "restore_copy_sync_error", "restore_dir_sync_error", "damaged_backup"} {
			t.Run(layout+"/"+cut, func(t *testing.T) {
				bubbleOrNot(t, func(t *testing.T) {
					path, _ := parDiskFixture(t, 2)
					kvPath := filepath.Join(filepath.Dir(path), "tube.yogadb")
					if layout == "directory" {
						if err := os.MkdirAll(filepath.Join(kvPath, "nested", "empty"), 0700); err != nil {
							t.Fatal(err)
						}
						if err := os.WriteFile(filepath.Join(kvPath, "nested", "broken"), []byte("unreadable page bytes"), 0600); err != nil {
							t.Fatal(err)
						}
					} else if layout == "file" {
						if err := os.WriteFile(kvPath, []byte("not a database directory"), 0600); err != nil {
							t.Fatal(err)
						}
					}
					state := &RaftState{CurrentTerm: 7, ClusterID: "par-test", PeerName: "node", LastApplied: 2, LastAppliedTerm: 1, CommitIndex: 2,
						VotedFor: "local-vote", HaveVoted: true, HaveVotedTerm: 7, KVstore: newKVStore()}
					defer state.KVstore.Close()
					state.KVstore.parFault, state.KVstore.parFaultPath = ErrPARChecksum, kvPath
					statePath := filepath.Join(filepath.Dir(path), "persistor.raftstate.tube.msgp")
					local := &raftStatePersistor{path: statePath, parMetaEnabled: true}
					if err := local.parSaveLocalMeta(state); err != nil {
						t.Fatal(err)
					}
					old, err := (parUndoIO{}).copyApplication(kvPath, "")
					if err != nil {
						t.Fatal(err)
					}
					undo, err := parBeginUndo(path, state)
					if err != nil {
						t.Fatal(err)
					}
					if cut != "prepared" {
						if err := os.RemoveAll(kvPath); err != nil {
							t.Fatal(err)
						}
						if cut != "application_removed" {
							if err := os.Mkdir(kvPath, 0700); err != nil {
								t.Fatal(err)
							}
							if err := os.WriteFile(filepath.Join(kvPath, "replacement"), []byte("new snapshot"), 0600); err != nil {
								t.Fatal(err)
							}
						}
					}
					if cut == "both_logs" || cut == "committed" {
						for _, suffix := range []string{"", ".parlog"} {
							if err := os.WriteFile(path+suffix, nil, 0600); err != nil {
								t.Fatal(err)
							}
						}
					}
					if cut == "committed" {
						if err := undo.finish(); err != nil {
							t.Fatal(err)
						}
						old, err = (parUndoIO{}).copyApplication(kvPath, "")
						if err != nil {
							t.Fatal(err)
						}
					}
					if cut == "damaged_backup" {
						backup := filepath.Join(path+".par-undo", "3")
						if old.Missing {
							backup = filepath.Join(path+".par-undo", "manifest")
						}
						if err := os.RemoveAll(backup); err != nil {
							t.Fatal(err)
						}
						if err := os.WriteFile(backup, []byte("bad backup"), 0600); err != nil {
							t.Fatal(err)
						}
						before, _ := (parUndoIO{}).copyApplication(kvPath, "")
						if err := parRecoverUndo(path); err == nil {
							t.Fatal("damaged raw application evidence accepted")
						}
						after, err := (parUndoIO{}).copyApplication(kvPath, "")
						if err != nil || before != after {
							t.Fatal("changed the application before validating all evidence", err)
						}
						return
					}
					if strings.HasPrefix(cut, "restore_") && !old.Missing {
						renamed := false
						disk := parUndoIO{
							renameFile: func(from, to string) error {
								if to == kvPath && cut == "restore_rename_error" {
									return errParTestIO
								}
								err := os.Rename(from, to)
								renamed = renamed || to == kvPath
								return err
							},
							syncFile: func(fd *os.File) error {
								if (cut == "restore_copy_sync_error" && strings.Contains(fd.Name(), ".par-app-restore-")) ||
									(cut == "restore_dir_sync_error" && renamed && fd.Name() == filepath.Dir(kvPath)) {
									return errParTestIO
								}
								return fd.Sync()
							},
						}
						if err := parRecoverUndoWithIO(path, disk); !errors.Is(err, errParTestIO) {
							t.Fatal("restore rename error hidden", err)
						}
					}
					for range 2 {
						if err := parRecoverUndo(path); err != nil {
							t.Fatal("repeated application restoration failed", err)
						}
					}
					got, err := (parUndoIO{}).copyApplication(kvPath, "")
					if err != nil || got != old {
						t.Fatal("rollback replaced damaged evidence with invented application rows", err)
					}
					if cut != "committed" {
						data, err := os.ReadFile(filepath.Join(filepath.Dir(path), "persistor.raftstate.tube.msgp"))
						if err != nil {
							t.Fatal(err)
						}
						restored, err := parDecodeState(data)
						if err != nil {
							t.Fatal(err)
						}
						defer restored.KVstore.Close()
						if restored.KVstore.SnapshotValid || restored.LastApplied != 2 || restored.CurrentTerm != 7 || restored.VotedFor != "local-vote" {
							t.Fatal("raw rollback changed the application boundary or local voting history")
						}
						node := &TubeNode{name: "node", PeerServiceName: TUBE_REPLICA, cfg: TubeConfig{DataDir: filepath.Dir(path)}}
						saver, restarted, err := (&TubeConfig{}).newRaftStatePersistor(statePath, node, false, true)
						if err != nil {
							t.Fatal(err)
						}
						defer saver.close()
						defer restarted.KVstore.Close()
						if restarted.KVstore.parFault == nil || restarted.LastApplied != 2 || restarted.VotedFor != "local-vote" {
							t.Fatal("rollback made an unreadable application look successfully applied")
						}
					}
				})
			})
		}
	}
}

func TestPar_publication_restores_originally_missing_files(t *testing.T) {
	for _, suffix := range []string{"", ".parlog"} {
		t.Run(fmt.Sprintf("missing%q", suffix), func(t *testing.T) {
			bubbleOrNot(t, func(t *testing.T) {
				path, _ := parDiskFixture(t, 3)
				other := ".parlog"
				if suffix != "" {
					other = ""
				}
				before, err := os.ReadFile(path + other)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(path + suffix); err != nil {
					t.Fatal(err)
				}
				if _, err := parBeginUndo(path); err != nil {
					t.Fatal(err)
				}
				for _, name := range []string{path, path + ".parlog"} {
					if err := os.WriteFile(name, nil, 0600); err != nil {
						t.Fatal(err)
					}
				}
				if err := parRecoverUndo(path); err != nil {
					t.Fatal(err)
				}
				if _, err := os.Stat(path + suffix); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("rollback fabricated originally missing evidence", err)
				}
				got, err := os.ReadFile(path + other)
				if err != nil || !bytes.Equal(got, before) {
					t.Fatal("rollback lost surviving evidence", err)
				}
			})
		})
	}
}

func TestPar_identifier_reconstruction(t *testing.T) {
	for _, fault := range []string{"first", "middle", "last", "missing", "partial", "bad_peer", "changed_WAL", "conflicting_identifier", "unbounded_frame"} {
		t.Run(fault, func(t *testing.T) {
			bubbleOrNot(t, func(t *testing.T) {
				path, _ := parDiskFixture(t, 3)
				original, err := scanParWAL(path, "par-test")
				if err != nil {
					t.Fatal(err)
				}
				slot := int64(0)
				if fault == "middle" {
					slot = 1
				}
				if fault == "last" || fault == "partial" {
					slot = 2
				}
				if fault == "missing" || fault == "unbounded_frame" {
					if err := os.Remove(path + ".parlog"); err != nil {
						t.Fatal(err)
					}
				} else {
					fd, err := os.OpenFile(path+".parlog", os.O_RDWR, 0)
					if err != nil {
						t.Fatal(err)
					}
					if fault == "partial" {
						err = fd.Truncate(slot*maxParRecord + maxParRecord/2)
					} else if fault == "conflicting_identifier" {
						forged := *original.Entries[slot].Record
						forged.TicketID = "different-ticket"
						data, encodeErr := encodeParSlot(&forged)
						if encodeErr != nil {
							t.Fatal(encodeErr)
						}
						_, err = fd.WriteAt(data, slot*maxParRecord)
					} else {
						_, err = fd.WriteAt([]byte{0}, slot*maxParRecord)
					}
					fd.Close()
					if err != nil {
						t.Fatal(err)
					}
				}
				if fault == "unbounded_frame" {
					fd, err := os.OpenFile(path, os.O_RDWR, 0)
					if err != nil {
						t.Fatal(err)
					}
					_, err = fd.WriteAt([]byte{bin32, 255, 255, 255, 255}, 0)
					fd.Close()
					if err != nil {
						t.Fatal(err)
					}
				}
				for repaired := 0; repaired < 3; repaired++ {
					candidateSlot, candidate, err := parIdentifierCandidate(path, "par-test")
					if fault == "unbounded_frame" || fault == "conflicting_identifier" {
						if err == nil {
							t.Fatal("proposed reconstruction from unusable evidence")
						}
						return
					}
					if err != nil || candidateSlot != slot || !parSameIdentity(candidate, original.Entries[slot].Record) {
						t.Fatalf("candidate slot %d/%d: %v", candidateSlot, slot, err)
					}
					data := original.Entries[slot].Bytes
					if fault == "bad_peer" || fault == "changed_WAL" {
						before, _ := os.ReadFile(path + ".parlog")
						if fault == "bad_peer" {
							data = append([]byte(nil), data...)
							data[0] ^= 1
						} else {
							fd, err := os.OpenFile(path, os.O_RDWR, 0)
							if err != nil {
								t.Fatal(err)
							}
							_, err = fd.WriteAt([]byte{0}, candidate.Offset)
							fd.Close()
							if err != nil {
								t.Fatal(err)
							}
						}
						if err := parInstallIdentifier(path, slot, *candidate, data); err == nil {
							t.Fatal("installed identifier without matching checked local and peer data")
						}
						after, _ := os.ReadFile(path + ".parlog")
						if !bytes.Equal(before, after) {
							t.Fatal("failed reconstruction changed identifier evidence")
						}
						return
					}
					if err := parInstallIdentifier(path, slot, *candidate, data); err != nil {
						t.Fatal(err)
					}
					if err := parInstallIdentifier(path, slot, *candidate, data); err != nil {
						t.Fatal("repeat identifier install", err)
					}
					if fault != "missing" {
						break
					}
					slot++
				}
				inv, err := scanParWAL(path, "par-test")
				if err != nil || inv.Err != nil || len(inv.Entries) != 3 {
					t.Fatalf("reconstructed inventory: %v, %v", inv, err)
				}
				for i, entry := range inv.Entries {
					if entry.Status != parIntact || !bytes.Equal(entry.Bytes, original.Entries[i].Bytes) {
						t.Fatalf("reconstruction changed WAL entry %d", i)
					}
				}
				wal, err := (&TubeConfig{ClusterID: "par-test"}).openPARWAL(path)
				if err != nil {
					t.Fatal(err)
				}
				defer wal.close()
				if _, err := wal.saveRaftLogEntry(&RaftLogEntry{Index: 4, Term: 2, Ticket: &Ticket{TicketID: "next", ClusterID: "par-test"}}); err != nil {
					t.Fatal("append after reconstructed identifiers", err)
				}
			})
		})
	}
}

func TestPar_compacted_identifier_recovery_preserves_snapshot_anchor(t *testing.T) {
	bubbleOrNot(t, func(t *testing.T) {
		path, _ := parDiskFixture(t, 4)
		cfg := &TubeConfig{ClusterID: "par-test"}
		wal, err := cfg.newRaftWriteAheadLog(path, false)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := wal.Compact(3, nil, nil); err != nil {
			t.Fatal(err)
		}
		if err := wal.close(); err != nil {
			t.Fatal(err)
		}
		before, err := scanParWAL(path, "par-test")
		if err != nil || len(before.Entries) != 2 || before.Entries[0].Record.Index != 3 {
			t.Fatal("invalid compacted fixture", err)
		}
		if err := os.Remove(path + ".parlog"); err != nil {
			t.Fatal(err)
		}
		wal, err = cfg.openPARWAL(path)
		if err != nil {
			t.Fatal(err)
		}
		node := &TubeNode{cfg: *cfg, ClusterID: "par-test", PeerServiceName: TUBE_REPLICA, wal: wal,
			state: &RaftState{CommitIndex: 4, LastApplied: 2, LastAppliedTerm: 1, CompactionDiscardedLast: IndexTerm{Index: 2, Term: 1}}}
		defer func() { node.wal.close() }()
		node.parAfterRestore()
		// Match Start's reconciliation after parAfterRestore.
		node.state.CompactionDiscardedLast = IndexTerm{Index: node.wal.logIndex.BaseC, Term: node.wal.logIndex.CompactTerm}
		if node.state.CompactionDiscardedLast.Index != 2 || !node.par.unknown || !node.parBlocked() {
			t.Fatal("missing identifiers erased the durable snapshot anchor")
		}
		for slot := range before.Entries {
			got, candidate, err := parIdentifierCandidate(path, "par-test")
			if err != nil || got != int64(slot) {
				t.Fatal("compacted identifier candidate", got, err)
			}
			if err := parInstallIdentifier(path, got, *candidate, before.Entries[slot].Bytes); err != nil {
				t.Fatal(err)
			}
		}
		node.par.unknown = false
		node.parFinish()
		if node.parBlocked() || node.wal.logIndex.BaseC != 2 || node.wal.lli != 4 || node.state.LastApplied != 2 {
			t.Fatalf("reconstructed compacted WAL changed its application boundary: %v", node.par.err)
		}
		node.assertCompactOK()
	})
}

func TestPar_recovery_snapshot_admission(t *testing.T) {
	for _, change := range []string{"none", "cluster", "leader", "name", "term", "floor", "boundary", "boundary_term", "configuration", "metadata_loss", "candidate", "no_application", "no_portable_rows", "unavailable_rows"} {
		t.Run(change, func(t *testing.T) {
			bubbleOrNot(t, func(t *testing.T) {
				node := &TubeNode{ClusterID: "cluster", role: FOLLOWER, leaderID: "leader", leaderName: "leader-name", par: &parNode{active: true},
					state: &RaftState{CurrentTerm: 4, CommitIndex: 5, LastApplied: 3, MC: &MemberConfig{ConfigTerm: 4, ConfigVersion: 2}}}
				state := &RaftState{ClusterID: "cluster", PeerID: "leader", PeerName: "leader-name", CurrentTerm: 4, CommitIndex: 6,
					LastApplied: 6, LastAppliedTerm: 3, CompactionDiscardedLast: IndexTerm{Index: 6, Term: 3},
					KVstore: &KVStore{SnapshotValid: true}, MC: &MemberConfig{ConfigTerm: 4, ConfigVersion: 2}}
				switch change {
				case "cluster":
					state.ClusterID = "foreign"
				case "leader":
					state.PeerID = "old-leader"
				case "name":
					state.PeerName = "old-name"
				case "term":
					state.CurrentTerm--
				case "floor":
					state.LastApplied = 4
				case "boundary":
					state.CompactionDiscardedLast.Index--
				case "boundary_term":
					state.CompactionDiscardedLast.Term--
				case "configuration":
					state.MC.ConfigVersion--
				case "metadata_loss":
					node.par.metaFault = true
				case "candidate":
					node.role = CANDIDATE
				case "no_application":
					state.KVstore = nil
				case "no_portable_rows":
					state.KVstore.SnapshotValid = false
				case "unavailable_rows":
					state.KVstore.parFault = ErrPAREvidence
				}
				if got := node.parSnapshotAllowed(&Snapshot{State: state}); got != (change == "none") {
					t.Fatalf("snapshot admission %v for %s", got, change)
				}
				if node.state.CommitIndex != 5 || !node.par.active {
					t.Fatal("admission changed protected history or service readiness")
				}
			})
		})
	}
}

func TestPar_delayed_snapshot_cannot_restore_older_configuration(t *testing.T) {
	for _, recovering := range []bool{false, true} {
		t.Run(fmt.Sprintf("recovering_%v", recovering), func(t *testing.T) {
			bubbleOrNot(t, func(t *testing.T) {
				node := parApplicationNode(t, parTestDir(t))
				node.state.MC.ConfigVersion = 3
				if _, err := node.saver.save(node.state); err != nil {
					t.Fatal(err)
				}
				incoming := node.state.clone()
				defer incoming.KVstore.Close()
				incoming.MC.ConfigVersion = 2
				incoming.PeerID, incoming.PeerName = "leader-id", "leader-name"
				node.leaderID, node.leaderName, node.leaderTerm = incoming.PeerID, incoming.PeerName, node.state.CurrentTerm
				node.par.active = recovering
				before, err := os.ReadFile(node.GetPersistorPath())
				if err != nil {
					t.Fatal(err)
				}
				node.applyNewStateSnapshot(&Snapshot{State: incoming}, "delayed same-term snapshot", false)
				if node.state.MC.ConfigVersion != 3 {
					t.Fatal("delayed snapshot revived an older voter configuration")
				}
				after, err := os.ReadFile(node.GetPersistorPath())
				if err != nil || !bytes.Equal(before, after) {
					t.Fatal("rejected snapshot changed durable local state", err)
				}
				if _, err := os.Stat(node.wal.path + ".par-undo"); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("rejected snapshot published an undo journal", err)
				}
			})
		})
	}
}

func TestPar_local_vote_metadata_redundancy(t *testing.T) {
	for _, fault := range []string{"none", "copy0", "copy1", "both", "both_missing", "interrupted_update", "conflicting_vote", "wrong_identity", "fresh"} {
		t.Run(fault, func(t *testing.T) {
			bubbleOrNot(t, func(t *testing.T) {
				path := filepath.Join(parTestDir(t), "persistor.raftstate.tube.msgp")
				if fault == "fresh" {
					meta, err := parLoadLocalMeta(path)
					if err != nil || meta != nil {
						t.Fatal("empty store fabricated local metadata", err)
					}
					return
				}
				saver := &raftStatePersistor{path: path, parMetaEnabled: true}
				state := &RaftState{CurrentTerm: 5, ClusterID: "cluster", PeerName: "node", HaveVoted: true, HaveVotedTerm: 5, VotedFor: "candidate", VotedForName: "candidate-name"}
				if err := saver.parSaveLocalMeta(state); err != nil {
					t.Fatal(err)
				}
				files, err := os.ReadDir(filepath.Dir(path))
				if err != nil || len(files) != 2 {
					t.Fatalf("metadata save must create exactly two copies: %v, %v", files, err)
				}
				data, err := parEncodeState(state)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
				prior, err := os.ReadFile(path + ".par-meta1")
				if err != nil {
					t.Fatal(err)
				}
				seq := saver.parMeta.Sequence
				// Same-term state rewrites cannot forget an already durable vote.
				state.VotedFor, state.VotedForName, state.HaveVoted, state.HaveVotedTerm = "", "", false, 0
				if err := saver.parSaveLocalMeta(state); err != nil || state.VotedFor != "candidate" || saver.parMeta.Sequence != seq {
					t.Fatalf("same-term save forgot vote or rewrote unchanged metadata: %v", err)
				}
				switch fault {
				case "copy0", "copy1", "both", "both_missing":
					for i, suffix := range []string{".par-meta0", ".par-meta1"} {
						if (fault == "copy0" && i == 1) || (fault == "copy1" && i == 0) {
							continue
						}
						var err error
						if fault == "both_missing" {
							err = os.Remove(path + suffix)
						} else {
							err = os.WriteFile(path+suffix, []byte("damage"), 0600)
						}
						if err != nil {
							t.Fatal(err)
						}
					}
				case "interrupted_update":
					state.CurrentTerm, state.VotedFor, state.VotedForName, state.HaveVoted, state.HaveVotedTerm = 6, "", "", false, 0
					if err := saver.parSaveLocalMeta(state); err != nil {
						t.Fatal(err)
					}
					// Only the first new copy was published when the node crashed.
					if err := os.WriteFile(path+".par-meta1", prior, 0600); err != nil {
						t.Fatal(err)
					}
				case "conflicting_vote":
					state.VotedFor = "another-candidate"
					if err := saver.parSaveLocalMeta(state); !errors.Is(err, ErrPARConflict) {
						t.Fatalf("double vote: %v", err)
					}
				case "wrong_identity":
					state.PeerName = "another-node"
					if err := saver.parSaveLocalMeta(state); !errors.Is(err, ErrPARIdentity) {
						t.Fatalf("foreign local metadata: %v", err)
					}
				}
				meta, err := parLoadLocalMeta(path)
				if fault == "both_missing" {
					// Saved state always requires the two-copy format. Missing
					// copies cannot trigger initialization from an older format.
					_, _, err = (&TubeConfig{}).NewRaftStatePersistor(path, &TubeNode{name: "node"}, false)
				}
				if fault == "both" || fault == "both_missing" {
					if !errors.Is(err, ErrPAREvidence) {
						t.Fatalf("lost all local evidence without fencing: %v", err)
					}
					return
				}
				if err != nil || meta == nil {
					t.Fatal("redundancy did not recover", err)
				}
				restored := &RaftState{ClusterID: "cluster", PeerName: "node", CurrentTerm: 4}
				if err := meta.merge(restored); err != nil {
					t.Fatal(err)
				}
				wantTerm, wantVote := int64(5), "candidate"
				if fault == "interrupted_update" {
					wantTerm, wantVote = 6, ""
				}
				if restored.CurrentTerm != wantTerm || restored.VotedFor != wantVote {
					t.Fatalf("restored term %d vote %q; want term %d vote %q", restored.CurrentTerm, restored.VotedFor, wantTerm, wantVote)
				}
			})
		})
	}
}

func TestPar_local_metadata_publication_IO_failures(t *testing.T) {
	for step := 1; step <= 6; step++ {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("step_%d/after_%v", step, after), func(t *testing.T) {
				bubbleOrNot(t, func(t *testing.T) {
					path := filepath.Join(parTestDir(t), "state")
					saver := &raftStatePersistor{path: path, parMetaEnabled: true}
					state := &RaftState{ClusterID: "cluster", PeerName: "node", CurrentTerm: 1}
					if err := saver.parSaveLocalMeta(state); err != nil {
						t.Fatal(err)
					}
					state.CurrentTerm, state.HaveVotedTerm = 2, 2
					state.VotedFor, state.VotedForName, state.HaveVoted = "candidate", "candidate", true
					calls := 0
					invoke := func(fn func() error) error {
						calls++
						if calls == step && !after {
							return errParTestIO
						}
						if err := fn(); err != nil {
							return err
						}
						if calls == step {
							return errParTestIO
						}
						return nil
					}
					saver.parIO = parUndoIO{
						syncFile: func(fd *os.File) error { return invoke(fd.Sync) },
						renameFile: func(from, to string) error {
							return invoke(func() error { return os.Rename(from, to) })
						},
					}
					if err := saver.parSaveLocalMeta(state); !errors.Is(err, errParTestIO) || calls != step {
						t.Fatalf("metadata publication error hidden: %v calls=%d", err, calls)
					}
					if saver.parMeta.Term != 1 || saver.parMeta.Sequence != 1 {
						t.Fatal("failed metadata publication advanced its durable cache")
					}
					meta, _, err := parLoadLocalMetaDetail(path)
					if err != nil || meta == nil {
						t.Fatal("interrupted metadata publication lost both copies", err)
					}
					newCopy := step > 2 || (step == 2 && after)
					if (meta.Term == 2) != newCopy || (newCopy && (meta.Vote != "candidate" || !meta.Voted || meta.VoteTerm != 2)) {
						t.Fatal("restart selected the wrong local voting history")
					}
					// Retry also handles an earlier successful rename whose following
					// directory sync failed, without erasing an acknowledged old vote.
					saver.parIO = parUndoIO{}
					if err := saver.parSaveLocalMeta(state); err != nil {
						t.Fatal(err)
					}
					meta, dirty, err := parLoadLocalMetaDetail(path)
					if err != nil || dirty || meta.Term != 2 || meta.Vote != "candidate" {
						t.Fatal("retry did not finish both local metadata copies", err)
					}
				})
			})
		}
	}
}

func TestPar_application_completion_waits_for_state_save(t *testing.T) {
	for _, failSave := range []bool{false, true} {
		t.Run(fmt.Sprintf("save_fails_%v", failSave), func(t *testing.T) {
			bubbleOrNot(t, func(t *testing.T) {
				cfg := NewTubeConfigTest(1, "par-completion", faketime)
				cfg.NoDisk, cfg.NoLogCompaction, cfg.DataDir = false, true, parTestDir(t)
				node := NewTubeNode("node", cfg)
				node.PeerID, node.state.PeerID, node.role = "local", "local", FOLLOWER
				node.state.CurrentTerm = 1
				node.par = &parNode{}
				defer func() {
					if node.state.KVstore != nil {
						node.state.KVstore.Close()
					}
				}()
				var err error
				node.wal, err = node.cfg.newRaftWriteAheadLog(filepath.Join(cfg.DataDir, "tube.wal.msgp"), false)
				if err != nil {
					t.Fatal(err)
				}
				defer node.wal.close()
				node.saver, _, err = node.cfg.NewRaftStatePersistor(node.GetPersistorPath(), node, false)
				if err != nil {
					t.Fatal(err)
				}
				defer node.saver.close()
				if _, err := node.saver.save(node.state); err != nil {
					t.Fatal(err)
				}
				ticket := node.NewTicket("durable completion", "base", "key", Val("value"), node.PeerID, node.name, WRITE, time.Second, context.Background())
				ticket.LogIndex, ticket.Term, ticket.RaftLogEntryTm = 1, 1, time.Now()
				if _, err := node.wal.saveRaftLogEntry(&RaftLogEntry{Index: 1, Term: 1, Ticket: ticket, Tm: ticket.RaftLogEntryTm}); err != nil {
					t.Fatal(err)
				}
				node.WaitingAtFollow.set(ticket.TicketID, ticket)
				node.state.CommitIndex, node.state.CommitIndexEntryTerm = 1, 1
				statePath := node.saver.path
				if failSave {
					// A deterministic filesystem failure after application and before
					// the replacement state file can be created/published.
					node.saver.path = filepath.Join(cfg.DataDir, "missing-directory", "state")
				}
				panicked := false
				func() {
					defer func() { panicked = recover() != nil }()
					node.commitWhatWeCan(false)
				}()
				if failSave {
					if !panicked || ticket.Done.IsClosed() || ticket.finishTicketCalled {
						t.Fatal("failed state save acknowledged an applied command")
					}
					return
				}
				if panicked || !ticket.Done.IsClosed() || !ticket.finishTicketCalled || node.par.applying || len(node.par.completions) != 0 {
					t.Fatal("successful durable batch did not release completion")
				}
				node.state.KVstore.Close()
				reopened, restored, err := node.cfg.NewRaftStatePersistor(statePath, node, false)
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.close()
				defer restored.KVstore.Close()
				value, _, err := restored.KVStoreRead(&Ticket{}, "base", "key")
				if err != nil || string(value) != "value" || restored.LastApplied != 1 {
					t.Fatal("completion preceded durable application and LastApplied", err)
				}
			})
		})
	}
}

func parApplicationNode(t *testing.T, dir string) *TubeNode {
	t.Helper()
	cfg := NewTubeConfigTest(1, "par-atomic", faketime)
	cfg.NoDisk, cfg.NoLogCompaction, cfg.DataDir = false, true, dir
	node := NewTubeNode("node", cfg)
	node.PeerID, node.state.PeerID, node.role = "local", "local", FOLLOWER
	node.state.CurrentTerm, node.par = 1, &parNode{}
	node.state.MC = node.NewMemberConfig("application checkpoint fixture")
	node.state.MC.ConfigTerm, node.state.MC.ConfigVersion = 1, 1
	node.state.MC.PeerNames.Set(node.name, &PeerDetail{Name: node.name, PeerID: node.PeerID})
	var err error
	node.wal, err = node.cfg.newRaftWriteAheadLog(filepath.Join(dir, "tube.wal.msgp"), false)
	if err != nil {
		t.Fatal(err)
	}
	node.saver, _, err = node.cfg.NewRaftStatePersistor(node.GetPersistorPath(), node, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := node.saver.save(node.state); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		node.wal.close()
		node.saver.close()
		if node.state.KVstore != nil {
			node.state.KVstore.Close()
		}
	})
	return node
}

// Capture a particular permitted crash image while the source owner is paused.
// Closing the source first would flush/commit a different image and hide bugs.
func parCopyCrashImage(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.WalkDir(src, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		out := filepath.Join(dst, rel)
		if entry.IsDir() {
			return os.MkdirAll(out, 0700)
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("unexpected crash fixture file type: %s", path)
		}
		_, err = parCopyChecked(path, out)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPar_application_transaction_crash_cuts(t *testing.T) {
	for _, cut := range []string{"before_rows", "after_rows", "after_checkpoint", "after_rows_flushed", "after_checkpoint_flushed", "after_commit", "after_state_save", "nested_save_rollback", "nested_save_commit", "newer_vote", "corrupt_checkpoint", "missing_checkpoint"} {
		t.Run(cut, func(t *testing.T) {
			bubbleOrNot(t, func(t *testing.T) {
				root := parTestDir(t)
				node := parApplicationNode(t, filepath.Join(root, "source"))
				now := time.Now()
				ste := newSessionTableEntry(&Session{SessionID: "session", T0: now, SessionIndexEndxTm: now.Add(time.Hour),
					SessRequestedInitialDur: time.Hour, CliName: node.name, CliPeerID: node.PeerID})
				node.state.SessTable[ste.SessionID] = ste
				write := node.NewTicket("initial", "base", "key", Val("old"), node.PeerID, node.name, WRITE, time.Second, context.Background())
				write.LogIndex, write.Term, write.RaftLogEntryTm = 1, 1, now
				if _, err := node.wal.saveRaftLogEntry(&RaftLogEntry{Index: 1, Term: 1, Ticket: write, Tm: now}); err != nil {
					t.Fatal(err)
				}
				node.state.CommitIndex, node.state.CommitIndexEntryTerm = 1, 1
				node.commitWhatWeCan(false)
				cas := node.NewTicket("cas", "base", "key", Val("new"), node.PeerID, node.name, CAS, time.Second, context.Background())
				cas.LogIndex, cas.Term, cas.RaftLogEntryTm, cas.OldVal = 2, 1, now.Add(time.Nanosecond), Val("old")
				cas.SessionID, cas.SessionSerial = ste.SessionID, 1
				if _, err := node.wal.saveRaftLogEntry(&RaftLogEntry{Index: 2, Term: 1, Ticket: cas, Tm: cas.RaftLogEntryTm}); err != nil {
					t.Fatal(err)
				}
				node.state.CommitIndex = 2
				var application *parApplication
				committed := false
				if cut != "before_rows" {
					var err error
					application, err = node.parBeginApplication()
					if err != nil {
						t.Fatal(err)
					}
					defer application.abort()
					node.doCAS(cas)
					if cas.Err != nil || !cas.CASwapped {
						t.Fatal("fixture CAS did not apply", cas.Err)
					}
					cas.Applied, cas.Committed = true, true
					ste.Serial2Ticket.Set(1, cas)
					ste.ticketID2tkt[cas.TicketID], ste.MaxAppliedSerial = cas, 1
					node.state.LastApplied, node.state.LastAppliedTerm = 2, 1
					if cut == "nested_save_rollback" || cut == "nested_save_commit" {
						node.state.CurrentTerm, node.state.VotedFor, node.state.HaveVoted, node.state.HaveVotedTerm = 2, "local-vote", true, 2
						node.state.MC.ConfigTerm, node.state.MC.ConfigVersion = 2, 2
						if _, err := node.saver.save(node.state); err != nil {
							t.Fatal(err)
						}
					}
					if cut == "after_checkpoint" || cut == "after_checkpoint_flushed" {
						data, err := parApplicationBytes(node.state)
						if err != nil {
							t.Fatal(err)
						}
						if _, err := application.tx.Put(parApplicationKey, data, 0); err != nil {
							t.Fatal(err)
						}
					}
					if cut == "after_rows_flushed" || cut == "after_checkpoint_flushed" {
						// Cross YogaDB's 8 MiB MEMWAL buffer while leaving the
						// transaction open. The captured file now contains actual
						// uncommitted BEGIN/mutation records, not just RAM changes.
						for i := 0; i < 9000; i++ {
							key := "bulk\x00" + strings.Repeat("k", 1024) + fmt.Sprint(i)
							if _, err := application.tx.Put(key, []byte("x"), 0); err != nil {
								t.Fatal(err)
							}
						}
						fi, err := os.Stat(filepath.Join(node.GetKVStorePath(), "FLEXDB.MEMWAL"))
						if err != nil || fi.Size() < 7<<20 {
							t.Fatal("fixture did not flush an uncommitted MEMWAL group", err)
						}
					}
					committed = cut != "after_rows" && cut != "after_checkpoint" && cut != "after_rows_flushed" && cut != "after_checkpoint_flushed" && cut != "nested_save_rollback"
					if committed {
						if err := application.commit(node.state); err != nil {
							t.Fatal(err)
						}
					}
					if cut == "after_state_save" {
						if _, err := node.saver.save(node.state); err != nil {
							t.Fatal(err)
						}
					}
					if cut == "newer_vote" {
						local := *node.state
						local.CurrentTerm, local.VotedFor, local.HaveVoted, local.HaveVotedTerm = 3, "newer-local-vote", true, 3
						if err := node.saver.parSaveLocalMeta(&local); err != nil {
							t.Fatal(err)
						}
					}
					if cut == "corrupt_checkpoint" || cut == "missing_checkpoint" {
						if err := node.state.KVstore.db.Update(func(tx *yogadb.WriteTx) error {
							if cut == "missing_checkpoint" {
								return tx.Delete(parApplicationKey)
							}
							_, err := tx.Put(parApplicationKey, []byte("damaged"), 0)
							return err
						}); err != nil {
							t.Fatal(err)
						}
					}
				}
				image := filepath.Join(root, "crash-image")
				parCopyCrashImage(t, node.cfg.DataDir, image)
				if application != nil {
					application.abort()
				}
				cfg := node.cfg
				cfg.DataDir = image
				restarted := NewTubeNode(node.name, &cfg)
				restarted.PeerID, restarted.role, restarted.par = "local", FOLLOWER, &parNode{}
				saver, state, err := cfg.NewRaftStatePersistor(restarted.GetPersistorPath(), restarted, false)
				if cut == "corrupt_checkpoint" || cut == "missing_checkpoint" {
					if err == nil {
						t.Fatal("unusable application checkpoint did not fence startup")
					}
					return
				}
				if err != nil {
					t.Fatal("reopening crash image", err)
				}
				defer saver.close()
				defer state.KVstore.Close()
				want, applied := "old", int64(1)
				if committed {
					want, applied = "new", 2
				}
				got, _, err := state.KVStoreRead(&Ticket{}, "base", "key")
				if err != nil || string(got) != want || state.LastApplied != applied {
					t.Fatalf("rows/checkpoint diverged after %s: %q/%q, applied %d/%d, err %v", cut, got, want, state.LastApplied, applied, err)
				}
				if cut == "newer_vote" && (state.CurrentTerm != 3 || state.VotedFor != "newer-local-vote") {
					t.Fatal("application checkpoint overwrote newer local voting history")
				}
				if (cut == "nested_save_rollback" || cut == "nested_save_commit") &&
					(state.CurrentTerm != 2 || state.VotedFor != "local-vote" || state.MC.ConfigVersion != 2) {
					t.Fatal("intermediate application save lost local metadata")
				}
				restarted.state, restarted.saver = state, saver
				restarted.wal, err = cfg.openPARWAL(filepath.Join(image, "tube.wal.msgp"))
				if err != nil {
					t.Fatal(err)
				}
				defer restarted.wal.close()
				restarted.state.CommitIndex, restarted.state.CommitIndexEntryTerm = 2, 1
				restarted.commitWhatWeCan(false)
				leaf, err := state.KVstore.GetLeaf("base", "key", false)
				prior, cached := state.SessTable[ste.SessionID].Serial2Ticket.Get2(1)
				if err != nil || string(leaf.Value) != "new" || leaf.Version != 2 || state.LastApplied != 2 || !cached || !prior.CASwapped {
					t.Fatal("crash/replay did not apply CAS exactly once", err)
				}
				restarted.role = LEADER
				retry := restarted.NewTicket("retry CAS", "base", "key", Val("different"), restarted.PeerID, restarted.name, CAS, time.Second, context.Background())
				retry.OldVal, retry.SessionID, retry.SessionSerial, retry.MinSessSerialWaiting = Val("old"), ste.SessionID, 1, 1
				done, _ := restarted.leaderDoneEarlyOnSessionStuff(retry)
				if !done || !retry.DupDetected || !retry.CASwapped || retry.Err != nil || !retry.Done.IsClosed() {
					t.Fatal("retry did not return the durable CAS success", retry.Err)
				}
				leaf, err = state.KVstore.GetLeaf("base", "key", false)
				if err != nil || string(leaf.Value) != "new" || leaf.Version != 2 {
					t.Fatal("retried session command executed a second time", err)
				}
			})
		})
	}
}

func TestPar_application_table_transaction_crash_cuts(t *testing.T) {
	for _, op := range []TicketOp{MAKE_TABLE, DELETE_TABLE, RENAME_TABLE, DELETE_KEY} {
		for _, committed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/committed_%v", op, committed), func(t *testing.T) {
				bubbleOrNot(t, func(t *testing.T) {
					root := parTestDir(t)
					node := parApplicationNode(t, filepath.Join(root, "source"))
					write := node.NewTicket("initial", "base", "key", Val("old"), node.PeerID, node.name, WRITE, time.Second, context.Background())
					write.LogIndex, write.Term, write.RaftLogEntryTm = 1, 1, time.Now()
					if _, err := node.wal.saveRaftLogEntry(&RaftLogEntry{Index: 1, Term: 1, Ticket: write, Tm: write.RaftLogEntryTm}); err != nil {
						t.Fatal(err)
					}
					node.state.CommitIndex, node.state.CommitIndexEntryTerm = 1, 1
					node.commitWhatWeCan(false)
					ticket := node.NewTicket("table operation", "base", "key", nil, node.PeerID, node.name, op, time.Second, context.Background())
					ticket.LogIndex, ticket.Term, ticket.RaftLogEntryTm, ticket.NewTableName = 2, 1, time.Now(), "renamed"
					if op == MAKE_TABLE {
						ticket.Table = "fresh"
					}
					if _, err := node.wal.saveRaftLogEntry(&RaftLogEntry{Index: 2, Term: 1, Ticket: ticket, Tm: ticket.RaftLogEntryTm}); err != nil {
						t.Fatal(err)
					}
					node.state.CommitIndex = 2
					application, err := node.parBeginApplication()
					if err != nil {
						t.Fatal(err)
					}
					defer application.abort()
					switch op {
					case MAKE_TABLE:
						node.doMakeTable(ticket)
					case DELETE_TABLE:
						node.doDeleteTable(ticket)
					case RENAME_TABLE:
						node.doRenameTable(ticket)
					case DELETE_KEY:
						node.doDeleteKey(ticket)
					}
					if ticket.Err != nil {
						t.Fatal("table operation fixture", ticket.Err)
					}
					node.state.LastApplied, node.state.LastAppliedTerm = 2, 1
					if committed {
						if err := application.commit(node.state); err != nil {
							t.Fatal(err)
						}
					}
					image := filepath.Join(root, "crash-image")
					parCopyCrashImage(t, node.cfg.DataDir, image)
					application.abort()
					cfg := node.cfg
					cfg.DataDir = image
					restarted := NewTubeNode(node.name, &cfg)
					restarted.PeerID, restarted.role, restarted.par = "local", FOLLOWER, &parNode{}
					saver, state, err := cfg.NewRaftStatePersistor(restarted.GetPersistorPath(), restarted, false)
					if err != nil {
						t.Fatal("table crash recovery", err)
					}
					defer saver.close()
					defer state.KVstore.Close()
					wantApplied := int64(1)
					if committed {
						wantApplied = 2
					}
					if state.LastApplied != wantApplied {
						t.Fatalf("table mutation/checkpoint diverged: %d/%d", state.LastApplied, wantApplied)
					}
					if !committed {
						value, _, err := state.KVStoreRead(&Ticket{}, "base", "key")
						if err != nil || string(value) != "old" {
							t.Fatal("uncommitted table mutation destroyed the old application", err)
						}
					}
					restarted.state, restarted.saver = state, saver
					restarted.wal, err = cfg.openPARWAL(filepath.Join(image, "tube.wal.msgp"))
					if err != nil {
						t.Fatal(err)
					}
					defer restarted.wal.close()
					restarted.state.CommitIndex, restarted.state.CommitIndexEntryTerm = 2, 1
					restarted.commitWhatWeCan(false)
					tables, _, _, err := state.KVstore.snapshotData()
					if err != nil {
						t.Fatal(err)
					}
					sort.Slice(tables, func(i, j int) bool { return tables[i] < tables[j] })
					want := []Key{"base"}
					switch op {
					case MAKE_TABLE:
						want = []Key{"base", "fresh"}
					case DELETE_TABLE:
						want = nil
					case RENAME_TABLE:
						want = []Key{"renamed"}
					}
					if fmt.Sprint(tables) != fmt.Sprint(want) || state.LastApplied != 2 {
						t.Fatalf("table replay/checkpoint mismatch: %v/%v, applied %d", tables, want, state.LastApplied)
					}
					if op == DELETE_KEY {
						if _, _, err := state.KVStoreRead(&Ticket{}, "base", "key"); !errors.Is(err, ErrKeyNotFound) {
							t.Fatal("deleted key reappeared", err)
						}
					} else if op == MAKE_TABLE || op == RENAME_TABLE {
						table := Key("base")
						if op == RENAME_TABLE {
							table = "renamed"
						}
						leaf, err := state.KVstore.GetLeaf(table, "key", false)
						if err != nil || string(leaf.Value) != "old" || leaf.Version != 1 {
							t.Fatal("table operation lost or replayed the existing leaf", err)
						}
					}
				})
			})
		}
	}
}

func TestPar_first_state_publication_IO_failures(t *testing.T) {
	// Discover the first save's actual publication operations, then interrupt
	// each one before/after the syscall. Each subtest owns its synctest bubble.
	run := func(t *testing.T, failAt int, after bool) int {
		cfg := NewTubeConfigTest(1, "par-first-save", faketime)
		cfg.NoDisk, cfg.NoLogCompaction, cfg.DataDir = false, true, parTestDir(t)
		node := NewTubeNode("node", cfg)
		node.PeerID, node.state.PeerID = "local", "local"
		node.state.CurrentTerm, node.state.HaveVotedTerm = 2, 2
		node.state.VotedFor, node.state.VotedForName, node.state.HaveVoted = "candidate", "candidate-name", true
		node.state.MC = node.NewMemberConfig("first publication")
		node.state.MC.ConfigVersion = 7
		node.state.MC.PeerNames.Set(node.name, &PeerDetail{Name: node.name, PeerID: node.PeerID})
		if !node.kvstoreWrite(&Ticket{Table: "base", Key: "seed", Val: Val("initial"), RaftLogEntryTm: time.Now()}, false, false) {
			t.Fatal("initial application fixture")
		}
		defer node.state.KVstore.Close()
		var err error
		node.saver, _, err = node.cfg.NewRaftStatePersistor(node.GetPersistorPath(), node, false)
		if err != nil {
			t.Fatal(err)
		}
		defer node.saver.close()
		calls := 0
		invoke := func(fn func() error) error {
			calls++
			if calls == failAt && !after {
				return errParTestIO
			}
			if err := fn(); err != nil {
				return err
			}
			if calls == failAt {
				return errParTestIO
			}
			return nil
		}
		node.saver.parIO = parUndoIO{
			syncFile: func(fd *os.File) error { return invoke(fd.Sync) },
			renameFile: func(from, to string) error {
				return invoke(func() error { return os.Rename(from, to) })
			},
		}
		var failure any
		func() {
			defer func() { failure = recover() }()
			_, _ = node.saver.save(node.state)
		}()
		if failAt == 0 {
			if failure != nil || calls == 0 {
				t.Fatal("initial publication baseline", failure, calls)
			}
		} else if err, ok := failure.(error); !ok || !errors.Is(err, errParTestIO) || calls != failAt {
			t.Fatalf("wrong publication failure: %v, calls=%d want=%d", failure, calls, failAt)
		}
		// Capture the interrupted disk image before closing the application.
		crashDir := parTestDir(t)
		parCopyCrashImage(t, cfg.DataDir, crashDir)
		crashCfg := *cfg
		crashCfg.DataDir = crashDir
		restarted := NewTubeNode(node.name, &crashCfg)
		defer restarted.state.KVstore.Close()
		for attempt := 0; attempt < 2; attempt++ {
			saver, state, err := crashCfg.newRaftStatePersistor(restarted.GetPersistorPath(), restarted, false, true)
			if err != nil {
				t.Fatalf("first-save interruption prevents restart: %v", err)
			}
			defer saver.close()
			if state == nil {
				// A cut before any initialization evidence was published may
				// leave a fresh store, but must never forget a published vote.
				if saver.parMeta != nil || attempt != 0 || failAt == 0 {
					t.Fatal("published local history was replaced with empty state")
				}
				state = node.state.clone() // Retry the unacknowledged initial save.
			} else {
				if state.CurrentTerm != 2 || state.VotedFor != "candidate" || state.VotedForName != "candidate-name" || !state.HaveVoted || state.HaveVotedTerm != 2 || state.MC == nil || state.MC.ConfigVersion != 7 {
					t.Fatal("initial publication lost local vote or membership")
				}
				leaf, err := state.KVstore.GetLeaf("base", "seed", false)
				if err != nil || leaf == nil || string(leaf.Value) != "initial" || leaf.Version != 1 {
					t.Fatal("initial publication lost or replayed its application", err)
				}
			}
			if _, err := saver.save(state); err != nil {
				t.Fatal(err)
			}
			state.KVstore.Close()
		}
		leftovers, err := filepath.Glob(restarted.GetPersistorPath() + ".pre_rename.*")
		if err != nil || len(leftovers) != 0 {
			t.Fatal("successful initialization retained staging evidence", leftovers, err)
		}
		return calls
	}
	var operations int
	t.Run("baseline", func(t *testing.T) {
		bubbleOrNot(t, func(t *testing.T) { operations = run(t, 0, false) })
	})
	for step := 1; step <= operations; step++ {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("step_%d/after_%v", step, after), func(t *testing.T) {
				bubbleOrNot(t, func(t *testing.T) { run(t, step, after) })
			})
		}
	}
}

func TestPar_initial_state_requires_checked_staging(t *testing.T) {
	for _, fault := range []string{"missing", "checksum", "identity", "nonportable", "conflicting_vote", "completed_state_lost"} {
		t.Run(fault, func(t *testing.T) {
			bubbleOrNot(t, func(t *testing.T) {
				cfg := NewTubeConfigTest(1, "par-initial-evidence", faketime)
				cfg.NoDisk, cfg.DataDir = false, parTestDir(t)
				node := NewTubeNode("node", cfg)
				defer node.state.KVstore.Close()
				node.state.CurrentTerm, node.state.HaveVotedTerm = 2, 2
				node.state.VotedFor, node.state.HaveVoted = "candidate", true
				saver, _, err := cfg.NewRaftStatePersistor(node.GetPersistorPath(), node, false)
				if err != nil {
					t.Fatal(err)
				}
				defer saver.close()
				want := ErrPAREvidence
				if fault == "completed_state_lost" {
					if _, err := saver.save(node.state); err != nil {
						t.Fatal(err)
					}
					node.state.KVstore.Close()
					if err := os.Truncate(saver.path, 0); err != nil {
						t.Fatal(err)
					}
				} else {
					// Construct a first-publication cut with durable voting copies
					// and a missing or invalid initial recovery image.
					if err := saver.parSaveLocalMeta(node.state); err != nil {
						t.Fatal(err)
					}
					if fault != "missing" {
						image := node.state.clone()
						defer image.KVstore.Close()
						switch fault {
						case "identity":
							image.PeerName, want = "another-node", ErrPARIdentity
						case "nonportable":
							image.KVstore.SnapshotValid = false
						case "conflicting_vote":
							image.VotedFor, want = "another-candidate", ErrPARConflict
						}
						data, err := parEncodeState(image)
						if err != nil {
							t.Fatal(err)
						}
						if fault == "checksum" {
							body, _, err := parFrame(data)
							if err != nil {
								t.Fatal(err)
							}
							body[len(body)/2] ^= 1
							want = ErrPARChecksum
						}
						if err := parWriteAtomic(saver.path+parInitialStateSuffix, data); err != nil {
							t.Fatal(err)
						}
					}
				}
				evidence := make(map[string][]byte)
				for _, suffix := range []string{"", ".par-meta0", ".par-meta1"} {
					data, err := os.ReadFile(saver.path + suffix)
					if err != nil {
						t.Fatal(err)
					}
					evidence[suffix] = data
				}
				reopened, state, err := cfg.newRaftStatePersistor(saver.path, node, false, true)
				if reopened != nil || state != nil || !errors.Is(err, want) {
					t.Fatalf("invalid initial evidence accepted: saver=%v state=%v err=%v; want %v", reopened, state, err, want)
				}
				for suffix, before := range evidence {
					after, err := os.ReadFile(saver.path + suffix)
					if err != nil || !bytes.Equal(before, after) {
						t.Fatal("rejected initialization changed local history", suffix, err)
					}
				}
			})
		})
	}
}

func TestPar_state_publication_IO_failures(t *testing.T) {
	for step := 1; step <= 3; step++ {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("step_%d/after_%v", step, after), func(t *testing.T) {
				bubbleOrNot(t, func(t *testing.T) {
					node := parApplicationNode(t, parTestDir(t))
					node.state.Serial = 1
					if _, err := node.saver.save(node.state); err != nil {
						t.Fatal(err)
					}
					node.state.Serial, node.state.CurrentTerm = 2, 2
					node.state.VotedFor, node.state.VotedForName = "candidate", "candidate"
					node.state.HaveVoted, node.state.HaveVotedTerm = true, 2
					if err := node.saver.parSaveLocalMeta(node.state); err != nil {
						t.Fatal(err)
					}
					calls := 0
					invoke := func(fn func() error) error {
						calls++
						if calls == step && !after {
							return errParTestIO
						}
						if err := fn(); err != nil {
							return err
						}
						if calls == step {
							return errParTestIO
						}
						return nil
					}
					node.saver.parIO = parUndoIO{
						syncFile: func(fd *os.File) error { return invoke(fd.Sync) },
						renameFile: func(from, to string) error {
							return invoke(func() error { return os.Rename(from, to) })
						},
					}
					var failure any
					func() {
						defer func() { failure = recover() }()
						_, _ = node.saver.save(node.state)
					}()
					if err, ok := failure.(error); !ok || !errors.Is(err, errParTestIO) || calls != step {
						t.Fatalf("publication error was not surfaced: %v, calls=%d", failure, calls)
					}
					node.state.KVstore.Close()
					reopened, state, err := node.cfg.NewRaftStatePersistor(node.GetPersistorPath(), node, false)
					if err != nil {
						t.Fatal(err)
					}
					defer reopened.close()
					defer state.KVstore.Close()
					wantSerial := int64(1)
					if step > 2 || (step == 2 && after) {
						wantSerial = 2
					}
					if state.Serial != wantSerial || state.CurrentTerm != 2 || state.VotedFor != "candidate" || !state.HaveVoted || state.HaveVotedTerm != 2 {
						t.Fatal("state publication selected a mixed image or forgot durable voting history")
					}
					leftovers, err := filepath.Glob(node.GetPersistorPath() + ".pre_rename.*")
					if err != nil || len(leftovers) != 0 {
						t.Fatal("failed state publication leaked a staging file", err)
					}
				})
			})
		}
	}
}

func TestPar_application_fault_preserves_local_metadata(t *testing.T) {
	for _, fault := range []string{"corrupt_checkpoint", "missing_checkpoint", "future_voting_term", "conflicting_applied_term"} {
		t.Run(fault, func(t *testing.T) {
			bubbleOrNot(t, func(t *testing.T) {
				node := parApplicationNode(t, parTestDir(t))
				node.state.CurrentTerm, node.state.VotedFor, node.state.VotedForName = 2, "local-vote", "candidate"
				node.state.HaveVoted, node.state.HaveVotedTerm = true, 2
				if _, err := node.saver.save(node.state); err != nil {
					t.Fatal(err)
				}
				data := []byte("damaged checkpoint")
				if fault == "future_voting_term" || fault == "conflicting_applied_term" {
					copy := *node.state
					copy.KVstore = nil
					if fault == "future_voting_term" {
						copy.CurrentTerm = 3
					} else {
						copy.LastAppliedTerm++
					}
					var err error
					data, err = parEncodeState(&copy)
					if err != nil {
						t.Fatal(err)
					}
				}
				if err := node.state.KVstore.db.Update(func(tx *yogadb.WriteTx) error {
					if fault == "missing_checkpoint" {
						return tx.Delete(parApplicationKey)
					}
					_, err := tx.Put(parApplicationKey, data, 0)
					return err
				}); err != nil {
					t.Fatal(err)
				}
				node.state.KVstore.Close()
				saver, state, err := node.cfg.newRaftStatePersistor(node.GetPersistorPath(), node, false, true)
				if fault == "future_voting_term" || fault == "conflicting_applied_term" {
					if !errors.Is(err, ErrPARConflict) || saver != nil || state != nil {
						t.Fatal("local history contradiction was made peer-recoverable", err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				defer saver.close()
				defer state.KVstore.Close()
				if state.KVstore.parFault == nil || state.CurrentTerm != 2 || state.VotedFor != "local-vote" || state.LastApplied != 0 {
					t.Fatal("recovery-only startup forgot local metadata")
				}
				if err := state.KVstore.Attach(node.GetKVStorePath(), false); !errors.Is(err, ErrPARRecovering) || state.KVstore.SnapshotValid {
					t.Fatal("unavailable application became an empty portable snapshot", err)
				}
				if err := state.KVstore.MakeTable("unsafe"); !errors.Is(err, ErrPARRecovering) {
					t.Fatal("unavailable application accepted a mutation", err)
				}
				if err := saver.parSaveApplication(state); !errors.Is(err, ErrPARRecovering) {
					t.Fatal("unavailable application could publish a checkpoint", err)
				}
				image := state.KVstore.clone()
				if image.SnapshotValid || image.parFault == nil {
					t.Fatal("inspection invented portable application rows")
				}
				image.Close()
				// A later term/vote must persist without attaching the placeholder
				// to disk or creating a valid checkpoint for its empty rows.
				state.CurrentTerm, state.VotedFor, state.VotedForName = 3, "new-local-vote", "new-candidate"
				state.HaveVoted, state.HaveVotedTerm = true, 3
				if _, err := saver.save(state); err != nil {
					t.Fatal(err)
				}
				state.KVstore.Close()
				again, restored, err := node.cfg.newRaftStatePersistor(node.GetPersistorPath(), node, false, true)
				if err != nil {
					t.Fatal(err)
				}
				defer again.close()
				defer restored.KVstore.Close()
				if restored.KVstore.parFault == nil || restored.CurrentTerm != 3 || restored.VotedFor != "new-local-vote" || restored.HaveVotedTerm != 3 {
					t.Fatal("metadata save initialized unavailable application rows or lost the new vote")
				}
			})
		})
	}
}

func TestPar_application_fault_retains_portable_before_image(t *testing.T) {
	bubbleOrNot(t, func(t *testing.T) {
		node := parApplicationNode(t, parTestDir(t))
		write := node.NewTicket("portable before-image", "base", "key", Val("retained"), node.PeerID, node.name, WRITE, time.Second, context.Background())
		write.LogIndex, write.Term, write.RaftLogEntryTm = 1, 1, time.Now()
		if _, err := node.wal.saveRaftLogEntry(&RaftLogEntry{Index: 1, Term: 1, Ticket: write, Tm: write.RaftLogEntryTm}); err != nil {
			t.Fatal(err)
		}
		node.state.CommitIndex, node.state.CommitIndexEntryTerm = 1, 1
		node.commitWhatWeCan(false)
		node.state.CurrentTerm, node.state.VotedFor, node.state.VotedForName = 2, "local-vote", "candidate"
		node.state.HaveVoted, node.state.HaveVotedTerm = true, 2
		if _, err := node.saver.save(node.state); err != nil {
			t.Fatal(err)
		}
		portable := node.state.clone()
		defer portable.KVstore.Close()
		data, err := parEncodeState(portable)
		if err != nil {
			t.Fatal(err)
		}
		if err := parWriteAtomic(node.GetPersistorPath(), data); err != nil {
			t.Fatal(err)
		}
		node.state.KVstore.Close()
		kvPath := node.GetKVStorePath()
		if err := os.Rename(kvPath, kvPath+".preserved"); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(kvPath, []byte("unreadable store"), 0600); err != nil {
			t.Fatal(err)
		}
		saver, state, err := node.cfg.newRaftStatePersistor(node.GetPersistorPath(), node, false, true)
		if err != nil {
			t.Fatal(err)
		}
		defer saver.close()
		defer state.KVstore.Close()
		if state.KVstore.parFault == nil || !state.KVstore.SnapshotValid {
			t.Fatal("unreadable store lost the checked portable before-image")
		}
		if _, err := saver.save(state); err != nil {
			t.Fatal(err)
		}
		if _, err := parBeginUndo(node.wal.path, state); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(kvPath); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(kvPath, 0700); err != nil {
			t.Fatal(err)
		}
		if err := parRecoverUndo(node.wal.path); err != nil {
			t.Fatal(err)
		}
		again, restored, err := node.cfg.newRaftStatePersistor(node.GetPersistorPath(), node, false, true)
		if err != nil {
			t.Fatal(err)
		}
		defer again.close()
		defer restored.KVstore.Close()
		if restored.KVstore.parFault == nil || !restored.KVstore.SnapshotValid || restored.LastApplied != 1 {
			t.Fatal("raw rollback lost portable evidence or cleared the unavailable gate")
		}
		leaf, err := restored.KVstore.GetLeaf("base", "key", false)
		if err != nil || string(leaf.Value) != "retained" {
			t.Fatal("raw rollback lost rows from the checked local before-image", err)
		}
		// Fail local recovery after the new application is durable, but before
		// its ordinary state-file publication. A boot must restore the complete
		// before-image and retry, rather than admitting that mixed generation.
		node.saver.close()
		node.saver, node.state = again, restored
		again.parIO.renameFile = func(from, to string) error {
			if to == node.GetPersistorPath() {
				return errParTestIO
			}
			return os.Rename(from, to)
		}
		var failure any
		func() {
			defer func() {
				if caught := recover(); caught != nil {
					failure = caught
				}
			}()
			if err := node.parRestorePortableApplication(); err != nil {
				failure = err
			}
		}()
		if err, ok := failure.(error); !ok || !errors.Is(err, errParTestIO) {
			t.Fatal("local application publication error was hidden", failure)
		}
		node.state.KVstore.Close()
		if err := node.wal.close(); err != nil {
			t.Fatal(err)
		}
		walPath := filepath.Join(node.cfg.DataDir, "tube.wal.msgp")
		node.wal, err = node.cfg.openPARWAL(walPath)
		if err != nil {
			t.Fatal(err)
		}
		retry, retryState, err := node.cfg.newRaftStatePersistor(node.GetPersistorPath(), node, false, true)
		if err != nil {
			t.Fatal(err)
		}
		again.close()
		node.saver, node.state = retry, retryState
		if retryState.KVstore.parFault == nil || !retryState.KVstore.SnapshotValid {
			t.Fatal("failed local publication did not restore the unavailable before-image")
		}
		if err := node.parRestorePortableApplication(); err != nil {
			t.Fatal("local application recovery could not retry", err)
		}
		value, _, err := node.state.KVStoreRead(&Ticket{}, "base", "key")
		if err != nil || string(value) != "retained" || node.state.LastApplied != 1 || node.state.KVstore.parFault != nil ||
			node.state.CurrentTerm != 2 || node.state.VotedFor != "local-vote" || node.state.HaveVotedTerm != 2 {
			t.Fatal("local recovery retry lost the saved application boundary", err)
		}
	})
}

// The real backend's per-instance VFS reaches writes inside snapshot row
// installation, not just Tube's surrounding journal/publication syscalls.
// Background flushing is disabled by KVStore.Open, so the owner serializes it.
type parApplicationFS struct {
	vfs.FS
	root         string
	at, seen     int
	after, fired bool
	trace        []string
	files        []vfs.File
}

func (s *parApplicationFS) mutation(name string, op string, do func() error) error {
	relative, err := filepath.Rel(s.root, name)
	if err != nil {
		return err
	}
	s.seen++
	s.trace = append(s.trace, filepath.Base(relative)+":"+op)
	fail := s.at == s.seen && !s.fired
	if fail {
		s.fired = true
		if !s.after {
			return errParTestIO
		}
	}
	if err := do(); err != nil {
		return err
	}
	if fail {
		return errParTestIO
	}
	return nil
}

func (s *parApplicationFS) wrap(fd vfs.File, err error) (vfs.File, error) {
	if err != nil {
		return nil, err
	}
	s.files = append(s.files, fd)
	return &parApplicationFile{File: fd, fs: s}, nil
}

func (s *parApplicationFS) Create(path string, category vfs.DiskWriteCategory) (vfs.File, error) {
	return s.wrap(s.FS.Create(path, category))
}
func (s *parApplicationFS) Open(path string, opts ...vfs.OpenOption) (vfs.File, error) {
	return s.wrap(s.FS.Open(path, opts...))
}
func (s *parApplicationFS) OpenReadWrite(path string, category vfs.DiskWriteCategory, opts ...vfs.OpenOption) (vfs.File, error) {
	return s.wrap(s.FS.OpenReadWrite(path, category, opts...))
}
func (s *parApplicationFS) OpenDir(path string) (vfs.File, error) {
	return s.wrap(s.FS.OpenDir(path))
}

type parApplicationFile struct {
	vfs.File
	fs *parApplicationFS
}

func (f *parApplicationFile) Write(p []byte) (n int, err error) {
	err = f.fs.mutation(f.Name(), "write", func() error { n, err = f.File.Write(p); return err })
	return
}
func (f *parApplicationFile) WriteAt(p []byte, off int64) (n int, err error) {
	err = f.fs.mutation(f.Name(), "writeAt", func() error { n, err = f.File.WriteAt(p, off); return err })
	return
}
func (f *parApplicationFile) Sync() error {
	return f.fs.mutation(f.Name(), "sync", f.File.Sync)
}
func (f *parApplicationFile) SyncData() error {
	return f.fs.mutation(f.Name(), "syncData", f.File.SyncData)
}
func (f *parApplicationFile) SyncTo(length int64) (full bool, err error) {
	err = f.fs.mutation(f.Name(), "syncTo", func() error { full, err = f.File.SyncTo(length); return err })
	return
}
func (f *parApplicationFile) Truncate(length int64) error {
	return f.fs.mutation(f.Name(), "truncate", func() error { return f.File.Truncate(length) })
}

func TestPar_application_backend_publication_failures(t *testing.T) {
	var trace []string
	template := filepath.Join(parTestDir(t), "template")
	var prepared TubeConfig
	// Prepare the acknowledged state once. Replaying initialization for every
	// cut adds thousands of irrelevant syncs to this disk-fault matrix.
	bubbleOrNot(t, func(t *testing.T) {
		if err := os.Mkdir(template, 0700); err != nil {
			t.Fatal(err)
		}
		node := parApplicationNode(t, template)
		write := node.NewTicket("backend before-image", "base", "key", Val("retained"), node.PeerID, node.name, WRITE, time.Second, context.Background())
		write.LogIndex, write.Term, write.RaftLogEntryTm = 1, 1, time.Now()
		if _, err := node.wal.saveRaftLogEntry(&RaftLogEntry{Index: 1, Term: 1, Ticket: write, Tm: write.RaftLogEntryTm}); err != nil {
			t.Fatal(err)
		}
		node.state.CommitIndex, node.state.CommitIndexEntryTerm = 1, 1
		node.commitWhatWeCan(false)
		node.state.CurrentTerm, node.state.VotedFor, node.state.HaveVoted, node.state.HaveVotedTerm = 2, "local-vote", true, 2
		if _, err := node.saver.save(node.state); err != nil {
			t.Fatal(err)
		}
		portable := node.state.clone()
		defer portable.KVstore.Close()
		data, err := parEncodeState(portable)
		if err != nil {
			t.Fatal(err)
		}
		if err := parWriteAtomic(node.GetPersistorPath(), data); err != nil {
			t.Fatal(err)
		}
		node.state.KVstore.Close()
		if err := os.RemoveAll(node.GetKVStorePath()); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(node.GetKVStorePath(), []byte("unreadable before-image"), 0600); err != nil {
			t.Fatal(err)
		}
		prepared = node.cfg
	})
	if t.Failed() {
		return
	}
	// Establish the actual backend operation sequence, then cut every write,
	// truncate and sync before and after the syscall. Each case has its own
	// bubble and retained-on-failure local directory.
	run := func(t *testing.T, peer bool, at int, after bool) {
		bubbleOrNot(t, func(t *testing.T) {
			caseCfg := prepared
			caseCfg.DataDir = filepath.Join(parTestDir(t), "node")
			parCopyCrashImage(t, template, caseCfg.DataDir)
			node := NewTubeNode("node", &caseCfg)
			node.role, node.par = FOLLOWER, &parNode{}
			var err error
			node.wal, err = caseCfg.openPARWAL(filepath.Join(caseCfg.DataDir, "tube.wal.msgp"))
			if err != nil {
				t.Fatal(err)
			}
			node.saver, node.state, err = caseCfg.newRaftStatePersistor(node.GetPersistorPath(), node, false, true)
			if err != nil {
				node.wal.close()
				t.Fatal(err)
			}
			t.Cleanup(func() {
				node.wal.close()
				node.saver.close()
				if node.state.KVstore != nil {
					node.state.KVstore.Close()
				}
			})
			if node.state.KVstore.parFault == nil || !node.state.KVstore.SnapshotValid {
				t.Fatal("prepared fixture lost its unreadable store or portable evidence")
			}
			fs := &parApplicationFS{FS: vfs.Default, root: node.cfg.DataDir, at: at, after: after}
			node.state.KVstore.parFS = fs
			var incoming *RaftState
			if peer {
				incoming = node.state.clone()
				incoming.KVstore.parFault, incoming.KVstore.parFaultPath = nil, ""
				defer func() { incoming.KVstore.Close() }()
				node.par.active = true
				node.leaderID, node.leaderName, node.leaderTerm = "donor", "donor-name", node.state.CurrentTerm
				incoming.PeerID, incoming.PeerName = node.leaderID, node.leaderName
				incoming.VotedFor = "donor-vote"
				incoming.CompactionDiscardedLast = IndexTerm{Index: incoming.LastApplied, Term: incoming.LastAppliedTerm}
				incoming.KVstore.parFS = fs
			}
			// A backend may panic on a failed internal write. That is a fail-stop
			// outcome; the published journal must still support restart.
			var failed any
			func() {
				defer func() {
					if v := recover(); v != nil {
						failed = v
					}
				}()
				if peer {
					node.applyNewStateSnapshot(&Snapshot{State: incoming}, "backend failure matrix", false)
				} else {
					if err := node.parRestorePortableApplication(); err != nil {
						failed = err
					}
				}
			}()
			if at == 0 {
				if failed != nil {
					t.Fatal("baseline snapshot installation failed", failed)
				}
				trace = append([]string(nil), fs.trace...)
				if len(trace) == 0 {
					t.Fatal("backend seam observed no writes")
				}
				t.Logf("%d backend mutations; testing both failure outcomes for each", len(trace))
				for _, name := range []string{"LARGE.VLOG", "FLEXDB.MEMWAL", "FLEXSPACE.KV.SLOT_BLOCKS", "FLEXTREE.PAGES", "FLEXTREE.COMMIT"} {
					found := false
					for _, op := range trace {
						found = found || strings.HasPrefix(op, name+":")
					}
					if !found {
						t.Fatal("snapshot backend matrix missed", name, trace)
					}
				}
				return
			}
			if !fs.fired {
				t.Fatal("backend fault was not exercised", at, fs.trace)
			}
			if fs.trace[at-1] != trace[at-1] {
				t.Fatal("backend cut did not exercise the baseline operation", trace[at-1], fs.trace[at-1])
			}
			// Stop observing before cleanup. Closing is not the crash oracle:
			// copy the image first, then reopen only that interrupted image.
			crash := filepath.Join(parTestDir(t), "crash")
			parCopyCrashImage(t, node.cfg.DataDir, crash)
			fs.at = 0
			func() { defer func() { _ = recover() }(); node.state.KVstore.Close() }()
			for _, fd := range fs.files {
				_ = fd.Close()
			}
			// The fixture's handles are discarded as at process exit.
			node.state.KVstore = nil
			cfg := node.cfg
			cfg.DataDir = crash
			restarted := NewTubeNode("node", &cfg)
			var failedErr error
			restarted.wal, failedErr = cfg.openPARWAL(filepath.Join(crash, "tube.wal.msgp"))
			if failedErr != nil {
				t.Fatal("backend crash prevented journal rollback", failedErr, failed)
			}
			defer restarted.wal.close()
			saver, state, err := cfg.newRaftStatePersistor(restarted.GetPersistorPath(), restarted, false, true)
			if err != nil {
				t.Fatal("cannot reopen backend crash image", err, failed)
			}
			restarted.saver, restarted.state = saver, state
			defer saver.close()
			defer func() { restarted.state.KVstore.Close() }()
			if err := restarted.parRestorePortableApplication(); err != nil {
				t.Fatal("backend recovery could not retry", err)
			}
			leaf, err := restarted.state.KVstore.GetLeaf("base", "key", false)
			if err != nil || leaf == nil || string(leaf.Value) != "retained" || leaf.Version != 1 ||
				restarted.state.LastApplied != 1 || restarted.state.CurrentTerm != 2 || restarted.state.VotedFor != "local-vote" ||
				!restarted.state.HaveVoted || restarted.state.HaveVotedTerm != 2 {
				t.Fatal("backend cut lost the acknowledged row, applied boundary, or local vote", err, failed)
			}
		})
	}
	for _, peer := range []bool{false, true} {
		t.Run(fmt.Sprintf("peer_%v", peer), func(t *testing.T) {
			trace = nil
			t.Run("baseline", func(t *testing.T) { run(t, peer, 0, false) })
			for i, op := range trace {
				for _, after := range []bool{false, true} {
					t.Run(fmt.Sprintf("%03d_%s_after_%v", i+1, op, after), func(t *testing.T) { run(t, peer, i+1, after) })
				}
			}
		})
	}
}

func TestPar_application_open_failure_preserves_evidence(t *testing.T) {
	bubbleOrNot(t, func(t *testing.T) {
		node := parApplicationNode(t, parTestDir(t))
		node.state.KVstore.Close()
		paths := []string{node.GetPersistorPath(), node.wal.path, node.wal.path + ".parlog",
			node.GetPersistorPath() + ".par-meta0", node.GetPersistorPath() + ".par-meta1"}
		before := make([][]byte, len(paths))
		for i, path := range paths {
			var err error
			before[i], err = os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
		}
		// Keep the application files for inspection; make the live pathname
		// unreadable as a database without damaging Raft's independent evidence.
		if err := os.Rename(node.saver.kvPath, node.saver.kvPath+".preserved"); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(node.saver.kvPath, []byte("not a database directory"), 0600); err != nil {
			t.Fatal(err)
		}
		saver, state, err := node.cfg.NewRaftStatePersistor(node.GetPersistorPath(), node, false)
		if err == nil || saver != nil || state != nil {
			t.Fatalf("unreadable application storage accepted: saver=%v state=%v err=%v", saver, state, err)
		}
		for i, path := range paths {
			got, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(got, before[i]) {
				t.Fatal("application open failure changed Raft recovery evidence", err)
			}
		}
	})
}

func TestPar_application_batch_sync_count(t *testing.T) {
	var single int64
	for _, count := range []int{1, 16} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			bubbleOrNot(t, func(t *testing.T) {
				node := parApplicationNode(t, parTestDir(t))
				for i := 1; i <= count; i++ {
					ticket := node.NewTicket(fmt.Sprint(i), "base", "key", Val(fmt.Sprint(i)), node.PeerID, node.name, WRITE, time.Second, context.Background())
					ticket.LogIndex, ticket.Term, ticket.RaftLogEntryTm = int64(i), 1, time.Now()
					if _, err := node.wal.saveRaftLogEntryDoFsync(&RaftLogEntry{Index: int64(i), Term: 1, Ticket: ticket, Tm: ticket.RaftLogEntryTm}, false); err != nil {
						t.Fatal(err)
					}
				}
				if err := node.wal.sync(false); err != nil {
					t.Fatal(err)
				}
				before := atomic.LoadInt64(&node.state.KVstore.db.MemWALFsyncs)
				node.state.CommitIndex, node.state.CommitIndexEntryTerm = int64(count), 1
				if !node.commitWhatWeCan(false) {
					t.Fatal("batch was not saved")
				}
				syncs := atomic.LoadInt64(&node.state.KVstore.db.MemWALFsyncs) - before
				t.Logf("%d entries: %d application MEMWAL syncs", count, syncs)
				if count == 1 {
					single = syncs
				}
				if syncs <= 0 || syncs > 4 || syncs != single {
					t.Fatalf("application sync count grows per entry: single=%d batch=%d", single, syncs)
				}
				node.state.KVstore.Close()
				reopened, state, err := node.cfg.NewRaftStatePersistor(node.GetPersistorPath(), node, false)
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.close()
				defer state.KVstore.Close()
				leaf, err := state.KVstore.GetLeaf("base", "key", false)
				if err != nil || string(leaf.Value) != fmt.Sprint(count) || leaf.Version != int64(count) || state.LastApplied != int64(count) {
					t.Fatalf("batch did not read its earlier writes or persist its final result: leaf=%v err=%v applied=%d", leaf, err, state.LastApplied)
				}
			})
		})
	}
}

func TestPar_application_validation_vs_storage_failure(t *testing.T) {
	for _, storageFailure := range []bool{false, true} {
		t.Run(fmt.Sprintf("storage_failure_%v", storageFailure), func(t *testing.T) {
			bubbleOrNot(t, func(t *testing.T) {
				node := parApplicationNode(t, parTestDir(t))
				write := node.NewTicket("initial", "base", "key", Val("old"), node.PeerID, node.name, WRITE, time.Second, context.Background())
				write.LogIndex, write.Term, write.RaftLogEntryTm = 1, 1, time.Now()
				if _, err := node.wal.saveRaftLogEntry(&RaftLogEntry{Index: 1, Term: 1, Ticket: write, Tm: write.RaftLogEntryTm}); err != nil {
					t.Fatal(err)
				}
				node.state.CommitIndex, node.state.CommitIndexEntryTerm = 1, 1
				node.commitWhatWeCan(false)
				node.state.CommitIndex = 2
				application, err := node.parBeginApplication()
				if err != nil {
					t.Fatal(err)
				}
				defer application.abort()
				// Rejected preflight validation does not poison the enclosing batch.
				if err := node.state.KVstore.RenameTable("missing", "renamed"); err == nil {
					t.Fatal("missing rename source accepted")
				}
				if node.state.KVstore.parTxFailure != nil {
					t.Fatal("normal command validation poisoned the application batch")
				}
				if err := node.state.KVstore.MakeTable("fresh"); err != nil {
					t.Fatal(err)
				}
				if storageFailure {
					injected := errors.New("injected failure after a partial mutation")
					err := node.state.KVstore.parUpdate(func(tx *yogadb.WriteTx) error {
						if err := tx.Delete(compositeKey("base", "key")); err != nil {
							return err
						}
						return injected
					})
					if err != injected || node.state.KVstore.parTxFailure != injected {
						t.Fatal("storage failure was not retained")
					}
					node.state.LastApplied, node.state.LastAppliedTerm = 2, 1
					if err := application.commit(node.state); err != injected {
						t.Fatal("partially failed transaction was allowed to commit", err)
					}
					application.abort()
					value, _, err := node.state.KVStoreRead(&Ticket{}, "base", "key")
					if err != nil || string(value) != "old" {
						t.Fatal("failed transaction did not restore the previous row", err)
					}
					data, _, _, _, err := node.state.KVstore.db.Get(parApplicationKey)
					if err != nil {
						t.Fatal(err)
					}
					checkpoint, err := parDecodeState(data)
					if err != nil || checkpoint.LastApplied != 1 {
						t.Fatal("failed application advanced the durable checkpoint", err)
					}
					return
				}
				node.state.LastApplied, node.state.LastAppliedTerm = 2, 1
				if err := application.commit(node.state); err != nil {
					t.Fatal("valid batch after a rejected command could not commit", err)
				}
				if _, err := node.saver.save(node.state); err != nil {
					t.Fatal(err)
				}
				node.state.KVstore.Close()
				reopened, state, err := node.cfg.NewRaftStatePersistor(node.GetPersistorPath(), node, false)
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.close()
				defer state.KVstore.Close()
				tables, _, _, err := state.KVstore.snapshotData()
				if err != nil || len(tables) != 2 || state.LastApplied != 2 {
					t.Fatal("normal validation failure prevented durable progress", err)
				}
			})
		})
	}
}

func parDecisionFixture(t *testing.T) (ParRecord, []byte) {
	t.Helper()
	e := &RaftLogEntry{Index: 1, Term: 1, Ticket: &Ticket{TicketID: "ticket", ClusterID: "par-test", Op: WRITE}}
	data, err := e.MarshalMsg(nil)
	if err != nil {
		t.Fatal(err)
	}
	hash, _ := parHash(data, nil)
	return ParRecord{Index: 1, Term: 1, Len0: 2, Len2: 2, TicketID: "ticket", ClusterID: "par-test", RLEblake3: hash}, data
}

func TestPar_slot_bounds_and_errors(t *testing.T) {
	bubbleOrNot(t, func(t *testing.T) {
		pr, _ := parDecisionFixture(t)
		b, err := encodeParSlot(&pr)
		if err != nil {
			t.Fatal(err)
		}
		for _, bad := range [][]byte{nil, {bin32, 255, 255, 255, 255}, make([]byte, maxParRecord-1)} {
			if _, err := decodeParSlot(bad); err == nil {
				t.Fatal("accepted invalid slot")
			}
		}
		bad := append([]byte(nil), b...)
		bad[10] ^= 1
		if _, err := decodeParSlot(bad); !errors.Is(err, ErrPARChecksum) {
			t.Fatalf("CRC error: %v", err)
		}
		pr.TicketID = string(make([]byte, maxParRecord))
		if _, err := encodeParSlot(&pr); !errors.Is(err, ErrPARBounds) {
			t.Fatalf("oversize: %v", err)
		}
		path := filepath.Join(parTestDir(t), "sidecar")
		p, err := newParLog(path, false)
		if err != nil {
			t.Fatal(err)
		}
		defer p.close()
		if _, err := p.at(-1); !errors.Is(err, ErrPARBounds) {
			t.Fatal(err)
		}
		for _, n := range []int64{-1, 1, math.MaxInt64} {
			if !errors.Is(p.truncate(n), ErrPARBounds) {
				t.Fatal("invalid truncate accepted")
			}
		}
		if _, err := p.loadone(math.MaxInt64); !errors.Is(err, ErrPARBounds) {
			t.Fatal(err)
		}
		p.fd.Close()
		e := &RaftLogEntry{Index: 1, Term: 1, Ticket: &Ticket{TicketID: "t"}}
		if err := p.append1(e, 0, 2, 2, "h"); err == nil {
			t.Fatal("closed file append succeeded")
		}
		if p.n != 0 || len(p.e) != 0 {
			t.Fatal("failed append advanced memory")
		}
	})
}

func TestPar_inventory_checks_metadata(t *testing.T) {
	for _, field := range []string{"index", "term", "ticket", "cluster", "offset", "length", "overflow"} {
		t.Run(field, func(t *testing.T) {
			bubbleOrNot(t, func(t *testing.T) {
				path, _ := parDiskFixture(t, 3)
				fd, err := os.OpenFile(path+".parlog", os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				defer fd.Close()
				var slot [maxParRecord]byte
				if _, err := fd.ReadAt(slot[:], maxParRecord); err != nil {
					t.Fatal(err)
				}
				pr, err := decodeParSlot(slot[:])
				if err != nil {
					t.Fatal(err)
				}
				switch field {
				case "index":
					pr.Index++
				case "term":
					pr.Term++
				case "ticket":
					pr.TicketID = "wrong"
				case "cluster":
					pr.ClusterID = "wrong"
				case "offset":
					pr.Offset++
				case "length":
					pr.Len0++
				case "overflow":
					pr.Len0 = math.MaxInt64
				}
				b, err := encodeParSlot(pr)
				if err != nil {
					t.Fatal(err)
				}
				// Recomputed CRC deliberately makes this a semantic validation test.
				payload, n, err := parFrame(b[:maxParRecord-8])
				_ = payload
				if err != nil {
					t.Fatal(err)
				}
				binary.BigEndian.PutUint64(b[n:n+8], crc64.Checksum(b[:n], crc64table))
				if _, err := fd.WriteAt(b, maxParRecord); err != nil {
					t.Fatal(err)
				}
				inv, err := scanParWAL(path, "par-test")
				if err != nil {
					t.Fatal(err)
				}
				if inv.Entries[1].Status == parIntact {
					t.Fatal("trusted hash masked invalid metadata")
				}
			})
		})
	}
}

func TestPar_batch_identifiers_follow_WAL_sync(t *testing.T) {
	bubbleOrNot(t, func(t *testing.T) {
		path := filepath.Join(parTestDir(t), "tube.wal.msgp")
		wal, err := (&TubeConfig{}).newRaftWriteAheadLog(path, false)
		if err != nil {
			t.Fatal(err)
		}
		defer wal.close()
		file := &parFaultFile{File: wal.parlog.fd.(*os.File)}
		wal.parlog.fd = file
		for i := int64(1); i <= 4; i++ {
			e := &RaftLogEntry{Index: i, Term: 1, Ticket: &Ticket{TicketID: fmt.Sprint(i), ClusterID: "par-test"}}
			if _, err := wal.saveRaftLogEntryDoFsync(e, false); err != nil {
				t.Fatal(err)
			}
			fi, err := wal.parlog.fd.Stat()
			if err != nil {
				t.Fatal(err)
			}
			if fi.Size() != 0 || wal.parlog.durable != 0 || file.syncs != 0 {
				t.Fatal("identifier published before WAL flush")
			}
		}
		if err := wal.sync(false); err != nil {
			t.Fatal(err)
		}
		fi, err := wal.parlog.fd.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if fi.Size() != 4*maxParRecord || wal.parlog.durable != 4 || file.syncs != 1 {
			t.Fatal("batch identifiers not published")
		}
		inv, err := scanParWAL(path, "par-test")
		if err != nil || inv.Err != nil {
			t.Fatalf("batch: %v, %#v", err, inv)
		}
		for _, e := range inv.Entries {
			if e.Status != parIntact {
				t.Fatal(e.Err)
			}
		}
	})
}

func TestPar_scenario_recovery_core(t *testing.T) {
	for _, scenario := range genParTestScenarios() {
		for leader, srv := range scenario.node {
			if srv.crashed || srv.lagging {
				continue
			}
			t.Run(fmt.Sprintf("scenario%d/leader%d", scenario.scenario, leader), func(t *testing.T) {
				bubbleOrNot(t, func(t *testing.T) {
					paths := make([]string, 5)
					var originals [][]byte
					members := make(map[string]string)
					for node, srv := range scenario.node {
						members[fmt.Sprintf("node%d", node)] = fmt.Sprintf("peer%d", node)
						count := 3
						if srv.lagging {
							count = 0
						}
						path, data := parDiskFixture(t, count)
						paths[node] = path
						if count == 3 {
							originals = data
						}
						inv, err := scanParWAL(path, "par-test")
						if err != nil {
							t.Fatal(err)
						}
						fd, err := os.OpenFile(path, os.O_RDWR, 0)
						if err != nil {
							t.Fatal(err)
						}
						for slot, bad := range srv.faulty {
							if !bad {
								continue
							}
							if _, err := fd.WriteAt([]byte{0}, inv.Entries[slot].Record.Offset); err != nil {
								t.Fatal(err)
							}
						}
						fd.Close()
					}
					inv, err := scanParWAL(paths[leader], "par-test")
					if err != nil {
						t.Fatal(err)
					}
					for slot, hole := range inv.Entries {
						if hole.Status == parIntact {
							continue
						}
						fence := parFence{Leader: fmt.Sprintf("node%d", leader), Term: 2, ConfigVersion: 1, Round: fmt.Sprint(slot)}
						round, err := newParRound(fence, *hole.Record, members, 3)
						if err != nil {
							t.Fatal(err)
						}
						for node, srv := range scenario.node {
							if node == leader || srv.crashed {
								continue
							}
							peer, err := scanParWAL(paths[node], "par-test")
							if err != nil {
								t.Fatal(err)
							}
							reply := parReply{Fence: fence, Member: fmt.Sprintf("node%d", node), PeerID: fmt.Sprintf("peer%d", node), Record: *hole.Record, Kind: parAbsent}
							if slot < len(peer.Entries) {
								entry := peer.Entries[slot]
								reply.Kind = parKnownDamaged
								if entry.Entry != nil {
									reply.Kind, reply.Bytes = parValid, entry.Bytes
								}
							}
							if err := round.add(fence, reply); err != nil {
								t.Fatal(err)
							}
						}
						decision, data, err := round.decide(fence)
						if err != nil || decision != parRepair {
							t.Fatalf("committed hole not repaired: %v %v", decision, err)
						}
						if err := repairParEntry(paths[leader], int64(slot), *hole.Record, data); err != nil {
							t.Fatal(err)
						}
					}
					wal, err := (&TubeConfig{}).newRaftWriteAheadLog(paths[leader], false)
					if err != nil {
						t.Fatal(err)
					}
					defer wal.close()
					if len(wal.raftLog) != 3 {
						t.Fatal("committed entries lost")
					}
					for i, e := range wal.raftLog {
						if !bytes.Equal(e.msgbytes, originals[i]) {
							t.Fatalf("entry %d changed", i)
						}
					}
				})
			})
		}
	}
}

func TestPar_round_rejects_invalid_evidence(t *testing.T) {
	bubbleOrNot(t, func(t *testing.T) {
		pr, data := parDecisionFixture(t)
		fence := parFence{Leader: "leader", Term: 2, Round: "query", ConfigVersion: 1}
		members := map[string]string{"leader": "l", "one": "1", "two": "2"}
		makeRound := func() *parRound {
			r, err := newParRound(fence, pr, members, 0)
			if err != nil {
				t.Fatal(err)
			}
			return r
		}
		good := parReply{Fence: fence, Member: "one", PeerID: "1", Kind: parValid, Record: pr, Bytes: data}
		for _, mutate := range []func(*parReply){
			func(p *parReply) { p.PeerID = "old-incarnation" },
			func(p *parReply) { p.Member = "shadow" },
			func(p *parReply) { p.Member = "leader"; p.PeerID = "l" },
			func(p *parReply) { p.Fence.Round = "old-query" },
			func(p *parReply) { p.Fence.Term++ },
			func(p *parReply) { p.Fence.ConfigVersion++ },
			func(p *parReply) { p.Fence.Generation++ },
			func(p *parReply) { p.Record.ClusterID = "other" },
			func(p *parReply) { p.Record.Term++ },
			func(p *parReply) { p.Record.TicketID = "other" },
			func(p *parReply) { p.Record.Index++ },
			func(p *parReply) { p.Record.RLEblake3 = "other" },
			func(p *parReply) { p.Bytes = []byte("corrupt") },
		} {
			r := makeRound()
			reply := good
			mutate(&reply)
			if err := r.add(fence, reply); err == nil {
				t.Fatalf("accepted invalid evidence: %#v", reply)
			}
			if decision, _, err := r.decide(fence); decision != parWait || err != nil {
				t.Fatal("invalid reply changed round")
			}
		}
		r := makeRound()
		absence := good
		absence.Kind = parAbsent
		absence.Bytes = nil
		if err := r.add(fence, absence); err != nil {
			t.Fatal(err)
		}
		if err := r.add(fence, good); !errors.Is(err, ErrPARConflict) {
			t.Fatal("contradictory evidence accepted")
		}
		if _, _, err := r.decide(fence); !errors.Is(err, ErrPARConflict) {
			t.Fatal("round failed open")
		}
	})
}

// All injected failures operate on actual local files, so a red test retains
// the partial durable image as well as the assertions that exposed it.
type parFaultFile struct {
	*os.File
	fail  string
	syncs int
}

var errParTestIO = errors.New("injected PAR I/O failure")

func (f *parFaultFile) WriteAt(b []byte, off int64) (int, error) {
	if f.fail == "write" {
		return 0, errParTestIO
	}
	if f.fail == "short" {
		return f.File.WriteAt(b[:len(b)-1], off)
	}
	return f.File.WriteAt(b, off)
}
func (f *parFaultFile) ReadAt(b []byte, off int64) (int, error) {
	if f.fail == "read" {
		return 0, errParTestIO
	}
	return f.File.ReadAt(b, off)
}
func (f *parFaultFile) Sync() error {
	f.syncs++
	if f.fail == "sync" {
		return errParTestIO
	}
	return f.File.Sync()
}
func (f *parFaultFile) Truncate(n int64) error {
	if f.fail == "truncate" {
		return errParTestIO
	}
	return f.File.Truncate(n)
}
func (f *parFaultFile) Close() error {
	err := f.File.Close()
	if f.fail == "close" {
		return errParTestIO
	}
	return err
}

func TestPar_sidecar_IO_failures(t *testing.T) {
	for _, op := range []string{"read", "write", "short", "sync", "truncate", "close"} {
		t.Run(op, func(t *testing.T) {
			bubbleOrNot(t, func(t *testing.T) {
				path := filepath.Join(parTestDir(t), "sidecar")
				p, err := newParLog(path, false)
				if err != nil {
					t.Fatal(err)
				}
				file := p.fd.(*os.File)
				f := &parFaultFile{File: file}
				p.fd = f
				e := &RaftLogEntry{Index: 1, Term: 1, Ticket: &Ticket{TicketID: "t"}}
				if err := p.append1(e, 0, 2, 2, "hash"); err != nil {
					t.Fatal(err)
				}
				f.fail = op
				switch op {
				case "read":
					if _, err := p.loadone(0); !errors.Is(err, errParTestIO) {
						t.Fatalf("read: %v", err)
					}
				case "write", "short", "sync":
					if err := p.append1(e, 4, 2, 2, "hash"); err == nil {
						t.Fatal("append succeeded despite I/O failure")
					}
					if p.n != 1 || p.durable != 1 || len(p.e) != 1 {
						t.Fatal("failed append changed committed count")
					}
				case "truncate":
					if err := p.truncate(0); !errors.Is(err, errParTestIO) {
						t.Fatal(err)
					}
					if p.n != 1 || p.durable != 1 || len(p.e) != 1 {
						t.Fatal("failed truncate changed count")
					}
				case "close":
					if err := p.close(); !errors.Is(err, errParTestIO) {
						t.Fatal(err)
					}
					if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) {
						t.Fatal("failed close leaked descriptor")
					}
					return
				}
				f.fail = ""
				if err := p.close(); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

func TestPar_failed_WAL_flush_does_not_publish_identifiers(t *testing.T) {
	bubbleOrNot(t, func(t *testing.T) {
		path := filepath.Join(parTestDir(t), "tube.wal.msgp")
		wal, err := (&TubeConfig{}).newRaftWriteAheadLog(path, false)
		if err != nil {
			t.Fatal(err)
		}
		e := &RaftLogEntry{Index: 1, Term: 1, Ticket: &Ticket{TicketID: "t", ClusterID: "par-test"}}
		if _, err := wal.saveRaftLogEntryDoFsync(e, false); err != nil {
			t.Fatal(err)
		}
		wal.fd.Close() // force the data flush to fail, including during close
		if err := wal.close(); err == nil {
			t.Fatal("closed WAL sync succeeded")
		}
		fi, err := os.Stat(path + ".parlog")
		if err != nil {
			t.Fatal(err)
		}
		if fi.Size() != 0 {
			t.Fatal("published persist record without data sync")
		}
	})
}

func TestPar_evidence_arrival_permutations(t *testing.T) {
	bubbleOrNot(t, func(t *testing.T) {
		pr, data := parDecisionFixture(t)
		for _, n := range []int{1, 3, 4, 5} {
			members := make(map[string]string)
			for i := 0; i < n; i++ {
				members[fmt.Sprint(i)] = fmt.Sprint(i)
			}
			fence := parFence{Leader: "0", Term: 2, Round: "query"}
			// Enumerate all evidence states, then all arrival permutations.
			kinds := make([]parEvidence, n-1)
			var enumerate func(int)
			enumerate = func(i int) {
				if i < len(kinds) {
					for k := parUnavailable; k <= parCompacted; k++ {
						kinds[i] = k
						enumerate(i + 1)
					}
					return
				}
				positive, negative := false, 0
				for _, k := range kinds {
					positive = positive || k == parValid
					if k == parAbsent {
						negative++
					}
				}
				want := parWait
				if positive {
					want = parRepair
				} else if negative >= n/2+1 {
					want = parDiscard
				}
				order := make([]int, n-1)
				for i := range order {
					order[i] = i
				}
				var permute func(int)
				permute = func(i int) {
					if i < len(order) {
						for j := i; j < len(order); j++ {
							order[i], order[j] = order[j], order[i]
							permute(i + 1)
							order[i], order[j] = order[j], order[i]
						}
						return
					}
					r, err := newParRound(fence, pr, members, 0)
					if err != nil {
						t.Fatal(err)
					}
					for _, node := range order {
						reply := parReply{Fence: fence, Member: fmt.Sprint(node + 1), PeerID: fmt.Sprint(node + 1), Kind: kinds[node], Record: pr}
						if reply.Kind == parValid {
							reply.Bytes = data
						}
						if err := r.add(fence, reply); err != nil {
							t.Fatal(err)
						}
					}
					got, _, err := r.decide(fence)
					if err != nil || got != want {
						t.Fatalf("N=%d kinds=%v order=%v: %v %v; want %v", n, kinds, order, got, err, want)
					}
				}
				permute(0)
			}
			enumerate(0)
		}
	})
}

func TestPar_missing_files_preserve_evidence(t *testing.T) {
	for _, missing := range []string{"wal", "sidecar", "both"} {
		t.Run(missing, func(t *testing.T) {
			bubbleOrNot(t, func(t *testing.T) {
				path, original := parDiskFixture(t, 3)
				if missing != "sidecar" {
					if err := os.Remove(path); err != nil {
						t.Fatal(err)
					}
				}
				if missing != "wal" {
					if err := os.Remove(path + ".parlog"); err != nil {
						t.Fatal(err)
					}
				}
				inv, err := scanParWAL(path, "par-test")
				if err != nil {
					t.Fatal(err)
				}
				if inv.Err == nil {
					t.Fatal("missing history treated as healthy")
				}
				if missing == "wal" {
					if len(inv.Entries) != 3 {
						t.Fatal("lost independent identifiers")
					}
					for i, entry := range inv.Entries {
						if entry.Status != parDamaged || entry.Record == nil {
							t.Fatal("missing bytes lost identity")
						}
						if err := repairParEntry(path, int64(i), *entry.Record, original[i]); err != nil {
							t.Fatal(err)
						}
					}
					wal, err := (&TubeConfig{}).newRaftWriteAheadLog(path, false)
					if err != nil {
						t.Fatal(err)
					}
					if err := wal.close(); err != nil {
						t.Fatal(err)
					}
				} else {
					if _, err := os.Stat(path + ".parlog"); !errors.Is(err, os.ErrNotExist) {
						t.Fatal("scan fabricated missing identifiers")
					}
				}
			})
		})
	}
}

func TestPar_static_errors_do_not_allocate(t *testing.T) {
	p := &parLog{nodisk: true}
	var got error
	allocs := testing.AllocsPerRun(1000, func() { _, got = p.at(-1) })
	if got != ErrPARBounds || allocs != 0 {
		t.Fatalf("bounds lookup: error=%v allocs=%v", got, allocs)
	}
}

func TestPar_WAL_IO_failure_fences_future_appends(t *testing.T) {
	for _, op := range []string{"data_write", "sidecar_write", "sidecar_sync"} {
		t.Run(op, func(t *testing.T) {
			bubbleOrNot(t, func(t *testing.T) {
				path := filepath.Join(parTestDir(t), "tube.wal.msgp")
				wal, err := (&TubeConfig{}).newRaftWriteAheadLog(path, false)
				if err != nil {
					t.Fatal(err)
				}
				if op == "data_write" {
					wal.fd.Close()
					wal.fd, err = os.Open(path) // read-only fd: seek succeeds, data write fails
					if err != nil {
						t.Fatal(err)
					}
				} else {
					failure := "write"
					if op == "sidecar_sync" {
						failure = "sync"
					}
					wal.parlog.fd = &parFaultFile{File: wal.parlog.fd.(*os.File), fail: failure}
				}
				e := &RaftLogEntry{Index: 1, Term: 1, Ticket: &Ticket{TicketID: "t", ClusterID: "par-test"}}
				_, first := wal.saveRaftLogEntry(e)
				if first == nil || wal.failure != first {
					t.Fatal("failure not retained")
				}
				if op == "data_write" && (len(wal.raftLog) != 0 || wal.lli != 0 || wal.logIndex.lastIndex() != 0 || wal.parlog.n != 0) {
					t.Fatal("failed data write advanced logical history")
				}
				before, _ := os.ReadFile(path)
				if _, next := wal.saveRaftLogEntry(e); next != first {
					t.Fatal("later append did not return original failure")
				}
				after, _ := os.ReadFile(path)
				if !bytes.Equal(before, after) {
					t.Fatal("later append wrote through failed WAL")
				}
				if err := wal.close(); err == nil {
					t.Fatal("failed WAL close hid I/O failure")
				}
				if wal.parlog.durable != 0 {
					t.Fatal("failed append published identifier")
				}
			})
		})
	}
}

// These are actual disk-backed Tube nodes using the ordinary circuit/event-loop
// paths. Their local log holes exist before InitAndStart opens any storage.
func parLiveCluster(t *testing.T, faults [][]int) (*TubeCluster, [][]byte) {
	return parLiveClusterState(t, faults, nil, nil)
}

// Nonnegative faults damage WAL slots. Fixture flags: -1 missing WAL, -2 missing
// sidecar, -3 compacted snapshot, -4 uncommitted third entry, -5 two-entry prefix,
// -6 nonvoting shadow; -7 corrupt application checkpoint, -8 missing store,
// -9 non-directory store path; -100 damages a VLOG application value, -101 a
// slotted page, -102 a tree commit record; -103 retains a portable local state
// image with an unreadable destination. Other flags -10 and below damage
// identifier slot -flag-10.
func parLiveClusterState(t *testing.T, faults [][]int, down, lagging map[int]bool, payload ...[]byte) (*TubeCluster, [][]byte) {
	t.Helper()
	root := parTestDir(t)
	cfg := NewTubeConfigTest(len(faults), "par-live", faketime)
	cfg.NoDisk = false
	cfg.NoLogCompaction = true
	cfg.testNum = 707
	cfg.HeartbeatDur = 50 * time.Millisecond
	cfg.MinElectionDur = 300 * time.Millisecond
	cfg.ClockDriftBound = 50 * time.Millisecond
	c := NewCluster(t.Name(), cfg)
	c.NoInitialLeaderTimeout = true
	t.Cleanup(c.Close)
	mc := c.Nodes[0].NewMemberConfig("PAR live test")
	mc.ConfigTerm, mc.ConfigVersion, mc.IsCommitted = 1, 1, true
	shadows := c.Nodes[0].NewMemberConfig("PAR shadow donors")
	shadows.ConfigTerm, shadows.ConfigVersion, shadows.IsCommitted = 1, 1, true
	for i, node := range c.Nodes {
		nonvoting := false
		for _, slot := range faults[i] {
			nonvoting = nonvoting || slot == -6
		}
		det := &PeerDetail{Name: node.name, PeerID: "old_" + node.name, URL: "boot.blank"}
		if nonvoting {
			shadows.PeerNames.Set(node.name, det)
		} else {
			mc.PeerNames.Set(node.name, det)
		}
	}
	var original [][]byte
	value := Val("committed")
	if len(payload) > 0 {
		value = payload[0]
	}
	for i, node := range c.Nodes {
		node.cfg.DataDir = filepath.Join(root, node.name)
		if err := os.MkdirAll(node.cfg.DataDir, 0700); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(node.cfg.DataDir, "tube.wal.msgp")
		wal, err := node.cfg.newRaftWriteAheadLog(path, false)
		if err != nil {
			t.Fatal(err)
		}
		var thisOriginal [][]byte
		count := int64(3)
		compacted := false
		uncommitted := false
		for _, slot := range faults[i] {
			compacted = compacted || slot == -3
			uncommitted = uncommitted || slot == -4
			if slot == -5 {
				count = 2
			}
		}
		if lagging[i] || compacted {
			count = 0
		}
		for index := int64(1); index <= count; index++ {
			tm := time.Date(2000, time.January, 1, 0, 0, 0, int(index), time.UTC)
			tkt := &Ticket{TicketID: fmt.Sprintf("par-live-ticket-%d", index), ClusterID: cfg.ClusterID,
				Op: WRITE, Table: "base", Key: Key(fmt.Sprint(index)), Val: value, LogIndex: index, Term: 1, FromName: "par-client", FromID: "par-client", RaftLogEntryTm: tm}
			e := &RaftLogEntry{Index: index, Term: 1, Ticket: tkt, Tm: tm}
			if _, err := wal.saveRaftLogEntry(e); err != nil {
				t.Fatal(err)
			}
			thisOriginal = append(thisOriginal, append([]byte(nil), e.msgbytes...))
		}
		if count == 3 {
			original = thisOriginal
		}
		if err := wal.close(); err != nil {
			t.Fatal(err)
		}
		st := node.state
		st.CurrentTerm, st.CommitIndex, st.CommitIndexEntryTerm = 1, count, 1
		if uncommitted {
			st.CommitIndex = min(count, 2)
		}
		st.MC = mc.Clone()
		st.ShadowReplicas = shadows.Clone()
		if compacted {
			for index := int64(1); index <= 3; index++ {
				tkt := &Ticket{Table: "base", Key: Key(fmt.Sprint(index)), Val: value, LogIndex: index,
					RaftLogEntryTm: time.Date(2000, time.January, 1, 0, 0, 0, int(index), time.UTC)}
				if !node.kvstoreWrite(tkt, false, false) {
					t.Fatal("snapshot fixture write failed", tkt.Err)
				}
			}
			st.CommitIndex, st.LastApplied, st.LastAppliedTerm = 3, 3, 1
			st.CompactionDiscardedLast = IndexTerm{Index: 3, Term: 1}
		}
		saver, _, err := node.cfg.NewRaftStatePersistor(node.GetPersistorPath(), node, false)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := saver.save(st); err != nil {
			t.Fatal(err)
		}
		saver.close()
		inv, err := scanParWAL(path, cfg.ClusterID)
		if err != nil {
			t.Fatal(err)
		}
		fd, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		missingWAL := false
		missingSidecar := false
		for _, slot := range faults[i] {
			if slot == -1 {
				missingWAL = true
				continue
			}
			if slot == -2 {
				missingSidecar = true
				continue
			}
			if slot == -3 {
				continue // This node's entries are represented by its durable snapshot.
			}
			if slot == -4 || slot == -5 || slot == -6 {
				continue // Fixture commitment floor / shorter known prefix.
			}
			if (slot >= -9 && slot <= -7) || (slot >= -103 && slot <= -100) {
				if slot == -103 {
					image := st.clone()
					data, err := parEncodeState(image)
					image.KVstore.Close()
					if err != nil {
						t.Fatal(err)
					}
					if err := parWriteAtomic(node.GetPersistorPath(), data); err != nil {
						t.Fatal(err)
					}
				}
				if slot == -7 {
					if err := st.KVstore.db.Update(func(tx *yogadb.WriteTx) error {
						_, err := tx.Put(parApplicationKey, []byte("damaged checkpoint"), 0)
						return err
					}); err != nil {
						t.Fatal(err)
					}
				}
				st.KVstore.Close()
				if slot >= -102 && slot <= -100 {
					file := map[int]string{-100: "LARGE.VLOG", -101: "FLEXSPACE.KV.SLOT_BLOCKS", -102: "FLEXTREE.COMMIT"}[slot]
					path := filepath.Join(node.GetKVStorePath(), file)
					data, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					marker := map[int]string{-100: "committed", -101: "base", -102: "FTCOW003"}[slot]
					changed := 0
					for from := 0; from < len(data); {
						offset := bytes.Index(data[from:], []byte(marker))
						if offset < 0 {
							break
						}
						offset += from
						data[offset] ^= 0x80
						changed++
						from = offset + len(marker)
						if slot == -100 {
							break
						}
						// Slotted pages and tree commits retain older versions. Damage
						// all matching copies, rather than just an unused old page.
					}
					if changed == 0 {
						t.Fatal("fixture has no target application bytes", file)
					}
					if err := os.WriteFile(path, data, 0600); err != nil {
						t.Fatal(err)
					}
				}
				if slot == -8 || slot == -9 || slot == -103 {
					if err := os.RemoveAll(node.GetKVStorePath()); err != nil {
						t.Fatal(err)
					}
				}
				if slot == -9 || slot == -103 {
					if err := os.WriteFile(node.GetKVStorePath(), []byte("not a database directory"), 0600); err != nil {
						t.Fatal(err)
					}
				}
				continue
			}
			if slot <= -10 {
				ids, err := os.OpenFile(path+".parlog", os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				_, err = ids.WriteAt([]byte{0}, int64(-slot-10)*maxParRecord)
				ids.Close()
				if err != nil {
					t.Fatal(err)
				}
				continue
			}
			if _, err := fd.WriteAt([]byte{0}, inv.Entries[slot].Record.Offset); err != nil {
				t.Fatal(err)
			}
		}
		fd.Close()
		if missingWAL {
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
		}
		if missingSidecar {
			if err := os.Remove(path + ".parlog"); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Cluster.Start supports an initially down prefix. Reorder only the test
	// fixture's node slice; persisted names and membership identities stay put.
	if len(down) > 0 {
		isDown := make(map[string]bool)
		for i := range down {
			isDown[fmt.Sprintf("node_%d", i)] = true
		}
		sort.SliceStable(c.Nodes, func(i, j int) bool { return isDown[c.Nodes[i].name] && !isDown[c.Nodes[j].name] })
		c.Cfg.deadOnStartCount = len(down)
		for i, node := range c.Nodes {
			c.Name2num[node.name] = i
		}
	}
	c.Start()
	return c, original
}

// Start an initially down fixture replica and connect the missing mesh edges.
// Its saved bootstrap URLs are placeholders, so automatic discovery alone
// cannot establish these connections.
func parStartDownReplica(t *testing.T, ctx context.Context, c *TubeCluster, name string) *TubeNode {
	t.Helper()
	index := c.Name2num[name]
	old := c.Nodes[index]
	node := NewTubeNode(old.name, &old.cfg)
	c.Nodes[index] = node
	c.Halt.AddChild(node.Halt)
	if err := node.InitAndStart(); err != nil {
		t.Fatal(err)
	}
	for _, other := range c.Nodes {
		if other == node {
			continue
		}
		frag := other.newFrag()
		frag.ToPeerName = node.name
		frag.SetUserArg("fromPeerName", other.name)
		frag.SetUserArg("toPeerName", node.name)
		ckt, _, _, _, err := other.MyPeer.PreferExtantRemotePeerGetCircuit(ctx, "tube-ckt", "", frag, string(TUBE_REPLICA), binVersion, node.URL, 0, nil, waitForAckTrue)
		if err != nil {
			t.Fatal(err)
		}
		other.MyPeer.NewCircuitCh <- ckt
	}
	return node
}

func parWaitForRecoveredRead(t *testing.T, node *TubeNode) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var last error
	for ctx.Err() == nil {
		ticket, err := node.Read(ctx, "base", "1", time.Second, nil)
		if err == nil {
			if string(ticket.Val) != "committed" {
				t.Fatal("wrong recovered value")
			}
			return
		}
		last = err
		time.Sleep(20 * time.Millisecond)
	}
	look := node.Inspect()
	defer look.State.KVstore.Close()
	t.Fatalf("recovery did not finish: last=%v role=%v leader=%s term=%d applied=%d commit=%d log=%d", last, look.Role, look.CurrentLeaderName, look.State.CurrentTerm, look.State.LastApplied, look.State.CommitIndex, look.LastLogIndex)
}

func TestPar_live_clean_suffix_loss_recovers_from_saved_boundary(t *testing.T) {
	onlyBubbled(t, func(t *testing.T) {
		c, _ := parLiveClusterState(t, [][]int{nil, nil, nil}, map[int]bool{2: true}, nil)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		leader, old := c.Nodes[c.Name2num["node_0"]], c.Nodes[c.Name2num["node_2"]]
		leader.testBeginElection <- true
		select {
		case <-c.LeaderNoop0committedCh:
		case <-ctx.Done():
			t.Fatal("healthy leader did not commit its no-op")
		}
		path := filepath.Join(old.cfg.DataDir, "tube.wal.msgp")
		inv, err := scanParWAL(path, old.ClusterID)
		if err != nil || len(inv.Entries) != 3 {
			t.Fatal("invalid original WAL fixture", err)
		}
		// Both files still agree, but the saved commitment through entry 3
		// independently proves the loss. No individual slot looks damaged.
		if err := os.Truncate(path, inv.Entries[1].Record.Offset); err != nil {
			t.Fatal(err)
		}
		if err := os.Truncate(path+".parlog", maxParRecord); err != nil {
			t.Fatal(err)
		}
		inv, err = scanParWAL(path, old.ClusterID)
		if err != nil || inv.Err != nil || len(inv.Entries) != 1 || inv.Entries[0].Status != parIntact {
			t.Fatal("shortened pair must be internally intact", err)
		}
		node := parStartDownReplica(t, ctx, c, old.name)
		parWaitForRecoveredRead(t, node)
		look := node.Inspect()
		defer look.State.KVstore.Close()
		if look.Role != FOLLOWER || look.CurrentLeaderName != leader.name || look.State.LastApplied < 3 || look.LogIndexBaseC < 3 {
			t.Fatal("saved boundary was not restored by the accepted leader's snapshot")
		}
		for index := 1; index <= 3; index++ {
			leaf, err := look.State.KVstore.GetLeaf("base", Key(fmt.Sprint(index)), false)
			if err != nil || leaf == nil || string(leaf.Value) != "committed" || leaf.Version != 1 {
				t.Fatal("recovery lost or replayed an acknowledged row", index, err)
			}
		}
	})
}

func TestPar_live_nodes_recover_before_command(t *testing.T) {
	for _, which := range []string{"follower_hole", "every_node_has_a_hole", "identifier_holes_every_node", "missing_WAL", "missing_sidecar", "compacted_donors"} {
		t.Run(which, func(t *testing.T) {
			bubbleOrNot(t, func(t *testing.T) {
				faults := [][]int{nil, {1}, nil}
				if which == "every_node_has_a_hole" {
					faults = [][]int{{0}, {1}, {2}}
				}
				if which == "identifier_holes_every_node" {
					faults = [][]int{{-10}, {-11}, {-12}}
				}
				if which == "missing_WAL" {
					faults = [][]int{{-1}, nil, nil}
				}
				if which == "missing_sidecar" {
					faults = [][]int{{-2}, nil, nil}
				}
				if which == "compacted_donors" {
					faults = [][]int{{0}, {-3}, {-3}}
				}
				c, original := parLiveCluster(t, faults)
				// A damaged but logically current node is allowed to win; its normal
				// leader no-op and client work remain gated until its bytes are repaired.
				leader := 0
				if which == "missing_sidecar" || which == "compacted_donors" {
					leader = 1
				}
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				if which == "identifier_holes_every_node" {
					// No node may run an election with an unknown identifier. They
					// must confirm each other's intact WAL bytes before electing.
					for {
						ready := true
						for _, node := range c.Nodes {
							look := node.Inspect()
							ready = ready && look.LastLogIndex >= 3
							if look.State != nil && look.State.KVstore != nil {
								look.State.KVstore.Close()
							}
						}
						if ready {
							break
						}
						select {
						case <-ctx.Done():
							t.Fatal("identifier reconstruction did not restore election freshness")
						case <-time.After(20 * time.Millisecond):
						}
					}
				}
				c.Nodes[leader].testBeginElection <- true
				// Wait for the real election/recovery readiness event before testing
				// successful service. Sending a read during the initial election
				// also tests Tube's unrelated pre-leader ticket queue.
				select {
				case <-c.LeaderNoop0committedCh:
				case <-ctx.Done():
					t.Fatal("leader did not commit its initial no-op after recovery")
				}
				for nodeIndex, node := range c.Nodes {
					for {
						tkt, err := node.Read(ctx, "base", "1", time.Second, nil)
						if err == nil {
							if string(tkt.Val) != "committed" {
								t.Fatalf("node %d lost acknowledged data: %q", nodeIndex, tkt.Val)
							}
							break
						}
						select {
						case <-ctx.Done():
							t.Fatalf("node %d did not recover: %v", nodeIndex, err)
						case <-time.After(50 * time.Millisecond):
						}
					}
					inv, err := scanParWAL(filepath.Join(node.cfg.DataDir, "tube.wal.msgp"), node.ClusterID)
					if err != nil {
						t.Fatalf("node %d: %v %#v", nodeIndex, err, inv)
					}
					insp := node.Inspect()
					base := insp.LogIndexBaseC
					if insp.State != nil && insp.State.KVstore != nil {
						insp.State.KVstore.Close()
					}
					for i := 0; i < 3; i++ {
						index := int64(i + 1)
						found := false
						for _, entry := range inv.Entries {
							if entry.Record != nil && entry.Record.Index == index {
								found = entry.Status == parIntact && bytes.Equal(entry.Bytes, original[i])
								if !found {
									t.Fatalf("node %d entry %d damaged after recovery", nodeIndex, i)
								}
							}
						}
						if !found && index > base {
							t.Fatalf("node %d entry %d not recovered", nodeIndex, i)
						}
					}
				}
			})
		})
	}
}

func TestPar_reconfiguration_requires_current_quorum_prefix(t *testing.T) {
	bubbleOrNot(t, func(t *testing.T) {
		var version int64
		config := func(names ...string) *MemberConfig {
			version++
			mc := &MemberConfig{ConfigTerm: 2, ConfigVersion: version, PeerNames: NewOmap[string, *PeerDetail]()}
			for _, name := range names {
				mc.PeerNames.Set(name, &PeerDetail{Name: name, PeerID: "id-" + name})
			}
			return mc
		}
		mc := config("a", "b", "c", "d", "e")
		node := &TubeNode{name: "a", PeerID: "id-a", role: LEADER, par: &parNode{},
			initialNoop0HasCommitted: true, wal: &raftWriteAheadLog{lli: 10},
			state: &RaftState{CurrentTerm: 2, CommitIndex: 10, LastApplied: 10, MC: mc},
			peers: map[string]*RaftNodeInfo{}, cktAllByName: map[string]*cktPlus{}}
		for _, name := range []string{"b", "c", "d", "e"} {
			index := int64(0)
			if name == "b" || name == "c" {
				index = 10
			}
			node.peers["id-"+name] = &RaftNodeInfo{PeerName: name, PeerID: "id-" + name, MatchIndex: index, LargestCommonRaftIndex: index}
			node.cktAllByName[name] = &cktPlus{PeerName: name, PeerID: "id-" + name, ckt: &rpc.Circuit{}, MC: mc}
		}
		setConfig := func(next *MemberConfig) {
			node.state.MC, mc = next, next
			for name, current := range node.cktAllByName {
				if _, voter := next.PeerNames.Get2(name); voter {
					current.MC = next
				}
			}
		}
		next := config("a", "b", "d", "e")
		if err := node.mongoLeaderCanReconfig(mc, next, 5, 5); err != nil {
			t.Fatal("initial committed quorum rejected", err)
		}
		// Only a,b retain the prefix in this four-member set. Q1/Q2 may be
		// satisfied by heartbeats from d,e; the old committed no-op is unchanged.
		setConfig(next)
		next = config("a", "d", "e")
		if err := node.mongoLeaderCanReconfig(mc, next, 4, 4); !errors.Is(err, ErrPARPrefixQuorum) {
			t.Fatal("old no-op permitted a second change without a current quorum holding the prefix", err)
		}
		node.peers["id-d"].MatchIndex = 10
		node.peers["id-d"].LargestCommonRaftIndex = 10
		if err := node.mongoLeaderCanReconfig(mc, next, 4, 4); err != nil {
			t.Fatal("caught-up current quorum rejected", err)
		}
		setConfig(next)
		next = config("a", "e")
		if err := node.mongoLeaderCanReconfig(mc, next, 3, 3); err != nil {
			t.Fatal("transferred prefix rejected", err)
		}
		setConfig(next)
		next = config("a", "e", "f")
		if err := node.mongoLeaderCanReconfig(mc, next, 2, 2); !errors.Is(err, ErrPARPrefixQuorum) {
			t.Fatal("removed replicas counted toward the next prefix quorum", err)
		}
		node.peers["id-e"].MatchIndex = 10
		node.peers["id-e"].LargestCommonRaftIndex = 10
		if err := node.mongoLeaderCanReconfig(mc, next, 2, 2); err != nil {
			t.Fatal("two-member prefix quorum rejected", err)
		}
	})
}

func TestPar_current_prefix_quorum_evidence(t *testing.T) {
	for _, count := range []int{1, 3, 4, 5} {
		for _, fault := range []string{"none", "lagging", "restart", "disconnected", "stale_config", "duplicate", "nonvoter", "applied_floor", "config_floor", "local_short", "recovering", "mismatched_suffix", "unknown_prefix"} {
			t.Run(fmt.Sprintf("n%d_%s", count, fault), func(t *testing.T) {
				bubbleOrNot(t, func(t *testing.T) {
					mc := &MemberConfig{ConfigTerm: 2, ConfigVersion: 3, PeerNames: NewOmap[string, *PeerDetail]()}
					node := &TubeNode{name: "0", PeerID: "id-0", role: LEADER, par: &parNode{}, wal: &raftWriteAheadLog{lli: 10},
						state: &RaftState{CurrentTerm: 2, CommitIndex: 10, LastApplied: 10, MC: mc}, peers: map[string]*RaftNodeInfo{}, cktAllByName: map[string]*cktPlus{}}
					for i := 0; i < count; i++ {
						name := fmt.Sprint(i)
						mc.PeerNames.Set(name, &PeerDetail{Name: name, PeerID: "id-" + name})
						if i == 0 {
							continue
						}
						index := int64(0)
						if i < count/2+1 {
							index = 10
						}
						node.peers["id-"+name] = &RaftNodeInfo{PeerName: name, PeerID: "id-" + name, MatchIndex: index, LargestCommonRaftIndex: index}
						node.cktAllByName[name] = &cktPlus{PeerName: name, PeerID: "id-" + name, ckt: &rpc.Circuit{}, MC: mc}
					}
					expect := fault == "none"
					switch fault {
					case "lagging":
						if count == 1 {
							node.wal.lli = 9
						} else {
							node.peers["id-1"].MatchIndex = 9
						}
					case "restart":
						if count == 1 {
							node.par.active = true
						} else {
							node.cktAllByName["1"] = &cktPlus{PeerID: "restarted-1"}
						}
					case "disconnected":
						if count == 1 {
							node.wal.lli = 9
						} else {
							delete(node.cktAllByName, "1")
						}
					case "stale_config":
						if count == 1 {
							node.wal.lli = 9
						} else {
							old := mc.Clone()
							old.ConfigVersion--
							node.cktAllByName["1"].MC = old
						}
					case "duplicate", "nonvoter":
						if count == 1 {
							node.wal.lli = 9
						} else {
							node.peers["id-1"].MatchIndex = 9
						}
						name := "outside"
						if fault == "duplicate" {
							name = "0"
						}
						node.peers["extra"] = &RaftNodeInfo{PeerName: name, PeerID: "extra", MatchIndex: 100, LargestCommonRaftIndex: 100}
					case "applied_floor":
						node.state.LastApplied = 11
					case "config_floor":
						mc.CommitIndex = 11
					case "local_short":
						node.wal.lli = 9
					case "recovering":
						node.par.active = true
					case "mismatched_suffix", "unknown_prefix":
						if count == 1 {
							node.wal.lli = 9
						} else {
							node.peers["id-1"].MatchIndex = 100
							node.peers["id-1"].LargestCommonRaftIndex = 9
							if fault == "unknown_prefix" {
								node.peers["id-1"].LargestCommonRaftIndex = -1
							}
						}
					}
					if got := node.parReconfigurationReady(mc); got != expect {
						t.Fatal("incorrect current-prefix quorum", got, expect)
					}
				})
			})
		}
	}
}

func TestPar_live_membership_transition_and_damaged_restart(t *testing.T) {
	bubbleOrNot(t, func(t *testing.T) {
		c, original := parLiveCluster(t, [][]int{nil, nil, nil})
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()
		leader := c.Nodes[0]
		leader.testBeginElection <- true
		select {
		case <-c.LeaderNoop0committedCh:
		case <-ctx.Done():
			t.Fatal("initial leader did not become ready")
		}
		removed := c.Nodes[2]
		inspection, _, err := leader.RemovePeerIDFromCluster(ctx, false, false, removed.name, removed.PeerID, removed.PeerServiceName, removed.BaseServerHostPort(), leader.URL, 0)
		if err != nil {
			t.Fatal("safe three-to-two reconfiguration failed", err)
		}
		if inspection.State != nil && inspection.State.KVstore != nil {
			inspection.State.KVstore.Close()
		}
		waitConfig := func(node *TubeNode, count int, minVersion int64) {
			for {
				look := node.Inspect()
				mc := look.State.MC
				ready := mc != nil && mc.PeerNames.Len() == count && mc.ConfigVersion >= minVersion
				look.State.KVstore.Close()
				if ready {
					return
				}
				select {
				case <-ctx.Done():
					t.Fatalf("%s did not adopt %d-member configuration", node.name, count)
				case <-time.After(20 * time.Millisecond):
				}
			}
		}
		for _, node := range c.Nodes {
			waitConfig(node, 2, 2)
		}
		// Restart from disk, with a new peer incarnation and a hole in history
		// acknowledged under the preceding three-member configuration.
		old := c.Nodes[1]
		old.Close()
		old.Srv.Close()
		if old.cfg.UseSimNet {
			for {
				if _, present := c.SimnetSnapshot(true).DNS[old.srvname]; !present {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal("old server did not finish unregistering")
				case <-time.After(20 * time.Millisecond):
				}
			}
		}
		path := filepath.Join(old.cfg.DataDir, "tube.wal.msgp")
		fd, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		_, err = fd.WriteAt([]byte{0}, 0)
		err = errors.Join(err, fd.Sync(), fd.Close())
		if err != nil {
			t.Fatal(err)
		}
		restarted := NewTubeNode(old.name, &old.cfg)
		c.Nodes[1] = restarted
		c.Halt.AddChild(restarted.Halt)
		if err := restarted.InitAndStart(); err != nil {
			t.Fatal(err)
		}
		if restarted.PeerID == old.PeerID {
			t.Fatal("restart reused the old peer incarnation")
		}
		for {
			ticket, err := restarted.Read(ctx, "base", "1", time.Second, nil)
			if err == nil {
				if string(ticket.Val) != "committed" {
					t.Fatal("reconfiguration/restart lost an acknowledged result")
				}
				break
			}
			select {
			case <-ctx.Done():
				t.Fatal("damaged voter did not recover after membership change", err)
			case <-time.After(20 * time.Millisecond):
			}
		}
		waitConfig(restarted, 2, 2)
		inv, err := scanParWAL(path, restarted.ClusterID)
		if err != nil || inv.Err != nil {
			t.Fatal("restarted voter retains unusable history", err)
		}
		for i := 0; i < 3; i++ {
			if inv.Entries[i].Status != parIntact || !bytes.Equal(inv.Entries[i].Bytes, original[i]) {
				t.Fatal("reconfiguration recovery rewrote acknowledged history")
			}
		}
		inspection, _, err = leader.AddPeerIDToCluster(ctx, false, false, removed.name, removed.PeerID, removed.PeerServiceName, removed.BaseServerHostPort(), leader.URL, 0)
		if err != nil {
			t.Fatal("safe two-to-three reconfiguration after recovery failed", err)
		}
		if inspection.State != nil && inspection.State.KVstore != nil {
			inspection.State.KVstore.Close()
		}
		for _, node := range c.Nodes {
			waitConfig(node, 3, 3)
		}
	})
}

func TestPar_live_recovering_leader_crosses_partition_and_reconfiguration(t *testing.T) {
	onlyBubbled(t, func(t *testing.T) {
		c, original := parLiveClusterState(t, [][]int{{2}, nil, nil, {-5}, {-5}}, map[int]bool{1: true, 2: true}, nil)
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
		defer cancel()
		byName := func(name string) *TubeNode {
			for _, node := range c.Nodes {
				if node.name == name {
					return node
				}
			}
			t.Fatal("missing fixture node", name)
			return nil
		}
		view := func(node *TubeNode) *Inspection {
			look := node.Inspect()
			look.State.KVstore.Close()
			return look
		}
		wait := func(node *TubeNode, ready func(*Inspection) bool, why string) *Inspection {
			for {
				look := view(node)
				if ready(look) {
					return look
				}
				select {
				case <-ctx.Done():
					t.Fatalf("%s: role=%v term=%d leader=%s config=%s applied=%d log=%d base=%d", why, look.Role, look.State.CurrentTerm, look.CurrentLeaderName, look.State.MC.Short(), look.State.LastApplied, look.LastLogIndex, look.LogIndexBaseC)
				case <-time.After(20 * time.Millisecond):
				}
			}
		}
		old := byName("node_0")
		old.testBeginElection <- true
		look := wait(old, func(i *Inspection) bool { return i.Role == LEADER }, "damaged logical leader was not elected")
		oldTerm := look.State.CurrentTerm
		time.Sleep(40 * time.Millisecond) // Let its recovery query reach the lagging voters.
		if _, err := old.Read(ctx, "base", "3", time.Second, nil); !errors.Is(err, ErrPARRecovering) {
			t.Fatal("unresolved leader served a command", err)
		}
		path := filepath.Join(old.cfg.DataDir, "tube.wal.msgp")
		checkHole := func() {
			inv, err := scanParWAL(path, old.ClusterID)
			if err != nil || len(inv.Entries) != 3 || inv.Entries[2].Status != parDamaged || inv.Entries[2].Record.Index != 3 {
				t.Fatal("lagging voters erased the protected hole", err)
			}
		}
		checkHole()
		c.IsolateNode(c.Name2num[old.name])
		// The only intact copies of entry 3 have not been started yet. Bring
		// them up after isolating the unresolved old leader, then connect the
		// four healthy nodes with the same circuit API used by Cluster.Start.
		for _, name := range []string{"node_1", "node_2"} {
			previous := byName(name)
			node := NewTubeNode(name, &previous.cfg)
			c.Nodes[c.Name2num[name]] = node
			c.Halt.AddChild(node.Halt)
			if err := node.InitAndStart(); err != nil {
				t.Fatal(err)
			}
		}
		healthy := []*TubeNode{byName("node_1"), byName("node_2"), byName("node_3"), byName("node_4")}
		connect := func(from, to *TubeNode) {
			frag := from.newFrag()
			frag.ToPeerName = to.name
			frag.SetUserArg("fromPeerName", from.name)
			frag.SetUserArg("toPeerName", to.name)
			ckt, _, _, _, err := from.MyPeer.PreferExtantRemotePeerGetCircuit(ctx, "tube-ckt", "", frag, string(TUBE_REPLICA), binVersion, to.URL, 0, nil, waitForAckTrue)
			if err != nil {
				t.Fatal(err)
			}
			from.MyPeer.NewCircuitCh <- ckt
		}
		for i, from := range healthy {
			for _, to := range healthy[i+1:] {
				connect(from, to)
			}
		}
		leader := healthy[0]
		leader.testBeginElection <- true
		wait(leader, func(i *Inspection) bool {
			return i.Role == CANDIDATE || i.Role == LEADER || i.State.CurrentTerm > oldTerm
		}, "healthy candidate did not start")
		// The lagging voters already voted for the old leader in oldTerm.
		// Starting a new pre-vote lets the live protocol elect a higher-term
		// leader without directly mutating any replica's owner state.
		if view(leader).State.CurrentTerm <= oldTerm {
			leader.testBeginPreVote <- true
		}
		select {
		case name := <-c.LeaderNoop0committedCh:
			if name != "node_1" && name != "node_2" {
				t.Fatal("a replica without the protected entry committed the no-op", name)
			}
			leader = byName(name)
		case <-ctx.Done():
			t.Fatal("new leader failed to establish its committed prefix")
		}
		if look := view(leader); look.Role != LEADER || look.State.CurrentTerm <= oldTerm {
			t.Fatal("healthy leader did not replace the partitioned recovering leader")
		}
		var version int64
		for _, removed := range []*TubeNode{old, healthy[3]} {
			inspection, _, err := leader.RemovePeerIDFromCluster(ctx, false, false, removed.name, removed.PeerID, removed.PeerServiceName, removed.BaseServerHostPort(), leader.URL, 0)
			if err != nil {
				t.Fatal("safe transition while old leader was isolated failed", err)
			}
			version = inspection.MC.ConfigVersion
			if inspection.State != nil && inspection.State.KVstore != nil {
				inspection.State.KVstore.Close()
			}
		}
		checkHole()
		c.AllHealthyAndPowerOn(true) // Deliver queued old-term traffic too.
		// Initially down nodes carry placeholder bootstrap URLs. Restore the
		// missing mesh edge explicitly, as Cluster.Start does for live nodes.
		connect(leader, old)
		wait(old, func(i *Inspection) bool {
			return i.Role == FOLLOWER && i.State.CurrentTerm > oldTerm && i.CurrentLeaderName == leader.name && i.State.MC.ConfigVersion >= version && i.State.MC.PeerNames.Len() == 3 && i.State.LastApplied >= 3
		}, "old recovery did not adopt the new leader/configuration")
		for {
			ticket, err := old.Read(ctx, "base", "3", time.Second, nil)
			if err == nil {
				if string(ticket.Val) != "committed" {
					t.Fatal("old replica resumed with the wrong value")
				}
				break
			}
			if !errors.Is(err, ErrPARRecovering) {
				t.Fatal("old replica failed to resume normal service", err)
			}
			select {
			case <-ctx.Done():
				t.Fatal("old recovery barrier did not clear")
			case <-time.After(20 * time.Millisecond):
			}
		}
		look = old.Inspect()
		for index := 1; index <= 3; index++ {
			leaf, err := look.State.KVstore.GetLeaf("base", Key(fmt.Sprint(index)), false)
			if err != nil || leaf == nil || string(leaf.Value) != "committed" || leaf.Version != 1 {
				t.Fatal("partition/configuration recovery lost or replayed an acknowledged row", index, err)
			}
		}
		look.State.KVstore.Close()
		inv, err := scanParWAL(path, old.ClusterID)
		if err != nil || inv.Err != nil {
			t.Fatal("recovered history remains unusable", err)
		}
		for index := 1; index <= 3; index++ {
			if int64(index) <= look.LogIndexBaseC {
				continue
			}
			found := false
			for _, entry := range inv.Entries {
				if entry.Record != nil && entry.Record.Index == int64(index) {
					found = entry.Status == parIntact && bytes.Equal(entry.Bytes, original[index-1])
				}
			}
			if !found {
				t.Fatal("recovery changed historical command bytes", index)
			}
		}
	})
}

func TestPar_live_removed_replica_snapshot_after_reconfiguration(t *testing.T) {
	bubbleOrNot(t, func(t *testing.T) {
		c, _ := parLiveCluster(t, [][]int{nil, nil, nil})
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		leader, old := c.Nodes[0], c.Nodes[2]
		leader.testBeginElection <- true
		select {
		case <-c.LeaderNoop0committedCh:
		case <-ctx.Done():
			t.Fatal("initial leader did not become ready")
		}
		old.Close()
		old.Srv.Close()
		if old.cfg.UseSimNet {
			for {
				if _, present := c.SimnetSnapshot(true).DNS[old.srvname]; !present {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal("old server did not finish unregistering")
				case <-time.After(20 * time.Millisecond):
				}
			}
		}
		inspection, _, err := leader.RemovePeerIDFromCluster(ctx, false, false, old.name, old.PeerID, old.PeerServiceName, old.BaseServerHostPort(), leader.URL, 0)
		if err != nil {
			t.Fatal("reconfiguration while a follower is down failed", err)
		}
		version := inspection.MC.ConfigVersion
		if inspection.State != nil && inspection.State.KVstore != nil {
			inspection.State.KVstore.Close()
		}
		path := filepath.Join(old.cfg.DataDir, "tube.wal.msgp")
		fd, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		_, err = fd.WriteAt([]byte{0}, 0)
		err = errors.Join(err, fd.Sync(), fd.Close())
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(path + ".parlog"); err != nil {
			t.Fatal(err)
		}
		// Neither the first payload nor its identifier can be reconstructed.
		// This replica needs a snapshot but still has the old saved voter set.
		restarted := NewTubeNode(old.name, &old.cfg)
		c.Nodes[2] = restarted
		c.Halt.AddChild(restarted.Halt)
		if err := restarted.InitAndStart(); err != nil {
			t.Fatal(err)
		}
		for {
			look := restarted.Inspect()
			mc := look.State.MC
			ready := mc != nil && mc.ConfigVersion == version && mc.PeerNames.Len() == 2 && look.LogIndexBaseC >= 3
			lastVersion := mc.ConfigVersion
			look.State.KVstore.Close()
			if ready {
				break
			}
			select {
			case <-ctx.Done():
				t.Fatalf("snapshot recovery stranded behind old membership: version=%d want=%d", lastVersion, version)
			case <-time.After(20 * time.Millisecond):
			}
		}
		ticket, err := restarted.Read(ctx, "base", "1", time.Second, nil)
		if err != nil || string(ticket.Val) != "committed" {
			t.Fatal("snapshot after reconfiguration lost an acknowledged result", err)
		}
	})
}

func TestPar_live_application_store_recovery(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		faults [][]int
	}{
		{"checkpoint", [][]int{nil, {-3, -7}, nil}},
		{"missing_store", [][]int{nil, {-3, -8}, nil}},
		{"non_directory", [][]int{nil, {-3, -9}, nil}},
		{"vlog_value", [][]int{nil, {-3, -100}, nil}},
		{"slotted_pages", [][]int{nil, {-3, -101}, nil}},
		{"tree_commits", [][]int{nil, {-3, -102}, nil}},
		{"two_unavailable_applications", [][]int{nil, {-3, -7}, {-3, -8}}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			bubbleOrNot(t, func(t *testing.T) {
				// The damaged replica already compacted through LastApplied=3;
				// rebuilding an empty application from its remaining WAL is unsafe.
				c, _ := parLiveCluster(t, scenario.faults)
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				node := c.Nodes[1]
				if _, err := node.Read(ctx, "base", "1", time.Second, nil); !errors.Is(err, ErrPARRecovering) {
					t.Fatal("unreadable application reached ordinary service", err)
				}
				node.testBeginElection <- true
				for len(node.testBeginElection) != 0 {
					select {
					case <-ctx.Done():
						t.Fatal("unavailable replica did not process election request")
					case <-time.After(20 * time.Millisecond):
					}
				}
				before := node.Inspect()
				if before.Role != FOLLOWER || before.State.CurrentTerm != 1 {
					t.Fatal("unavailable application campaigned before recovery")
				}
				before.State.KVstore.Close()
				c.Nodes[0].testBeginElection <- true
				select {
				case <-c.LeaderNoop0committedCh:
				case <-ctx.Done():
					t.Fatal("healthy leader did not become ready")
				}
				for {
					ticket, err := node.Read(ctx, "base", "1", time.Second, nil)
					if err == nil {
						if string(ticket.Val) != "committed" {
							t.Fatal("application recovery lost an acknowledged value")
						}
						break
					}
					select {
					case <-ctx.Done():
						t.Fatal("application snapshot recovery did not finish", err)
					case <-time.After(20 * time.Millisecond):
					}
				}
				look := node.Inspect()
				if look.State.LastApplied < 3 || look.LogIndexBaseC < 3 || look.State.PeerName != node.name {
					t.Fatal("application recovery rolled back a protected boundary or copied donor identity")
				}
				look.State.KVstore.Close()
				if scenario.name == "two_unavailable_applications" {
					for {
						ticket, err := c.Nodes[2].Read(ctx, "base", "1", time.Second, nil)
						if err == nil {
							if string(ticket.Val) != "committed" {
								t.Fatal("second unavailable application lost its acknowledged value")
							}
							break
						}
						select {
						case <-ctx.Done():
							t.Fatal("sole healthy application could not recover the other voters", err)
						case <-time.After(20 * time.Millisecond):
						}
					}
				}
				node.Close()
				saver, state, err := node.cfg.NewRaftStatePersistor(node.GetPersistorPath(), node, false)
				if err != nil {
					t.Fatal("completed application recovery did not reopen strictly", err)
				}
				defer saver.close()
				defer state.KVstore.Close()
				for index := 1; index <= 3; index++ {
					value, _, err := state.KVStoreRead(&Ticket{}, "base", Key(fmt.Sprint(index)))
					if err != nil || string(value) != "committed" || state.LastApplied < 3 || state.KVstore.parFault != nil {
						t.Fatal("reopened application checkpoint/rows did not match the snapshot", index, err)
					}
				}
			})
		})
	}
}

func TestPar_live_portable_application_recovers_without_peer(t *testing.T) {
	bubbleOrNot(t, func(t *testing.T) {
		c, _ := parLiveCluster(t, [][]int{{-3, -103}})
		node := c.Nodes[0]
		look := node.Inspect()
		if look.State.KVstore.parFault != nil || look.State.LastApplied != 3 || look.LogIndexBaseC != 3 {
			t.Fatal("portable local application did not recover before election")
		}
		look.State.KVstore.Close()
		if _, err := os.Stat(filepath.Join(node.cfg.DataDir, "tube.wal.msgp.par-undo")); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("local recovery did not finish publication", err)
		}
		node.testBeginElection <- true
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		select {
		case <-c.LeaderNoop0committedCh:
		case <-ctx.Done():
			t.Fatal("single replica could not serve after local application recovery")
		}
		for index := 1; index <= 3; index++ {
			ticket, err := node.Read(ctx, "base", Key(fmt.Sprint(index)), time.Second, nil)
			if err != nil || string(ticket.Val) != "committed" {
				t.Fatal("local recovery lost a saved snapshot row", index, err)
			}
		}
		look = node.Inspect()
		ownTerm, ownVote := look.State.CurrentTerm, look.State.VotedFor
		ownHaveVoted, ownVoteTerm := look.State.HaveVoted, look.State.HaveVotedTerm
		look.State.KVstore.Close()
		node.Close()
		saver, state, err := node.cfg.NewRaftStatePersistor(node.GetPersistorPath(), node, false)
		if err != nil {
			t.Fatal(err)
		}
		defer saver.close()
		defer state.KVstore.Close()
		if state.LastApplied < 3 || state.KVstore.parFault != nil || state.CurrentTerm != ownTerm || state.VotedFor != ownVote ||
			state.HaveVoted != ownHaveVoted || state.HaveVotedTerm != ownVoteTerm {
			t.Fatal("locally recovered application checkpoint did not survive restart")
		}
		for index := 1; index <= 3; index++ {
			leaf, err := state.KVstore.GetLeaf("base", Key(fmt.Sprint(index)), false)
			if err != nil || string(leaf.Value) != "committed" || leaf.Version != 1 {
				t.Fatal("local restore replayed or lost an applied row", index, err)
			}
		}
	})
}

func TestPar_chunked_reply_bounds(t *testing.T) {
	bubbleOrNot(t, func(t *testing.T) {
		pr, data := parDecisionFixture(t)
		pr.Len0 = int64(len(data) + 5)
		fence := parFence{Leader: "leader", Term: 2, Round: "chunks"}
		members := map[string]string{"leader": "l", "donor": "d", "other": "o"}
		makeNode := func() *parNode {
			r, err := newParRound(fence, pr, members, 0)
			if err != nil {
				t.Fatal(err)
			}
			return &parNode{round: r}
		}
		part := parWire{Version: 1, Cluster: pr.ClusterID, Fence: fence, Record: pr, Member: "donor", PeerID: "d", Kind: parValid, Total: int64(len(data))}
		p := makeNode()
		half := len(data) / 2
		part.Bytes = data[:half]
		if _, done, err := p.joinReply(fence, part); done || err != nil {
			t.Fatal(done, err)
		}
		part.Offset = int64(half)
		part.Bytes = data[half:]
		got, done, err := p.joinReply(fence, part)
		if !done || err != nil || !bytes.Equal(got, data) {
			t.Fatal("chunk reassembly failed", err)
		}
		if len(p.chunks) != 0 {
			t.Fatal("complete reply retained assembly")
		}
		for _, mutate := range []func(*parWire){
			func(w *parWire) { w.Member = "shadow" },
			func(w *parWire) { w.PeerID = "old" },
			func(w *parWire) { w.Fence.Term++ },
			func(w *parWire) { w.Record.ClusterID = "other" },
			func(w *parWire) { w.Total = math.MaxInt64 },
			func(w *parWire) { w.Offset = -1 },
			func(w *parWire) { w.Total = 1 },
			func(w *parWire) { w.Total = 0 },
		} {
			p := makeNode()
			bad := part
			bad.Offset = 0
			mutate(&bad)
			if _, _, err := p.joinReply(fence, bad); err == nil {
				t.Fatal("bad chunk accepted")
			}
			if len(p.chunks) != 0 {
				t.Fatal("invalid chunk allocated reassembly state")
			}
		}
		p = makeNode()
		if _, _, err := p.joinReply(fence, part); err == nil {
			t.Fatal("out-of-order chunk accepted")
		}
	})
}

func TestPar_live_six_scenarios(t *testing.T) {
	for _, scenario := range genParTestScenarios() {
		for lead, srv := range scenario.node {
			if srv.crashed || srv.lagging {
				continue
			}
			t.Run(fmt.Sprintf("scenario%d/leader%d", scenario.scenario, lead), func(t *testing.T) {
				bubbleOrNot(t, func(t *testing.T) {
					faults := make([][]int, len(scenario.node))
					down := make(map[int]bool)
					lagging := make(map[int]bool)
					for node, srv := range scenario.node {
						if srv.crashed {
							down[node] = true
						}
						if srv.lagging {
							lagging[node] = true
						}
						for slot, bad := range srv.faulty {
							if bad {
								faults[node] = append(faults[node], slot)
							}
						}
					}
					c, original := parLiveClusterState(t, faults, down, lagging)
					leaderName := fmt.Sprintf("node_%d", lead)
					for _, node := range c.Nodes {
						if node.name == leaderName {
							node.testBeginElection <- true
						}
					}
					ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
					defer cancel()
					select {
					case <-c.LeaderNoop0committedCh:
					case <-ctx.Done():
						t.Fatal("recovered leader did not commit its initial no-op")
					}
					for i, node := range c.Nodes {
						if i < c.Cfg.deadOnStartCount {
							continue
						}
						for {
							tkt, err := node.Read(ctx, "base", "1", time.Second, nil)
							if err == nil {
								if string(tkt.Val) != "committed" {
									t.Fatal("acknowledged value changed")
								}
								break
							}
							select {
							case <-ctx.Done():
								t.Fatalf("%s did not recover: %v", node.name, err)
							case <-time.After(20 * time.Millisecond):
							}
						}
						inv, err := scanParWAL(filepath.Join(node.cfg.DataDir, "tube.wal.msgp"), node.ClusterID)
						if err != nil {
							t.Fatalf("%s lost history: %v", node.name, err)
						}
						look := node.Inspect()
						defer look.State.KVstore.Close()
						for slot := 0; slot < 3; slot++ {
							index := int64(slot + 1)
							leaf, err := look.State.KVstore.GetLeaf("base", Key(fmt.Sprint(index)), false)
							if err != nil || string(leaf.Value) != "committed" {
								t.Fatalf("%s lost applied entry %d: %v", node.name, index, err)
							}
							if look.LogIndexBaseC >= index {
								// Lagging nodes may receive the acknowledged prefix
								// as a state snapshot rather than literal old entries.
								continue
							}
							found := false
							for _, entry := range inv.Entries {
								if entry.Record != nil && entry.Record.Index == index {
									found = entry.Status == parIntact && bytes.Equal(entry.Bytes, original[slot])
									break
								}
							}
							if !found {
								t.Fatalf("%s entry %d not recovered or covered by snapshot", node.name, index)
							}
						}
					}
				})
			})
		}
	}
}

func TestPar_live_large_entry_chunk_recovery(t *testing.T) {
	bubbleOrNot(t, func(t *testing.T) {
		payload := bytes.Repeat([]byte("p"), 1_000_000)
		c, _ := parLiveClusterState(t, [][]int{{0}, {1}, {2}}, nil, nil, payload)
		c.Nodes[0].testBeginElection <- true
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		select {
		case <-c.LeaderNoop0committedCh:
		case <-ctx.Done():
			t.Fatal("large entry recovery did not finish")
		}
		for _, node := range c.Nodes {
			tkt, err := node.Read(ctx, "base", "1", time.Second, nil)
			if err != nil || !bytes.Equal(tkt.Val, payload) {
				t.Fatalf("%s recovered wrong large value: %v", node.name, err)
			}
		}
	})
}

func TestPar_live_follower_replaces_damaged_uncommitted_suffix(t *testing.T) {
	for _, fault := range []struct {
		name string
		slot int
	}{{"payload", 2}, {"identifier", -12}} {
		t.Run(fault.name, func(t *testing.T) {
			onlyBubbled(t, func(t *testing.T) {
				// The down replica has an uncommitted term-1 entry 3. The healthy
				// replicas elect a leader and commit a different term-2 no-op there.
				c, _ := parLiveClusterState(t, [][]int{{-5}, {-5}, {fault.slot, -4}}, map[int]bool{2: true}, nil)
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				leader := c.Nodes[c.Name2num["node_0"]]
				leader.testBeginElection <- true
				select {
				case <-c.LeaderNoop0committedCh:
				case <-ctx.Done():
					t.Fatal("healthy leader did not commit its replacement no-op")
				}
				node := parStartDownReplica(t, ctx, c, "node_2")
				parWaitForRecoveredRead(t, node)
				look := node.Inspect()
				defer look.State.KVstore.Close()
				if look.Role != FOLLOWER || look.CurrentLeaderName != leader.name || look.State.LastApplied < 3 || look.LogIndexBaseC < 3 {
					t.Fatal("follower did not install the accepted leader's replacement snapshot")
				}
				for _, key := range []Key{"1", "2"} {
					leaf, err := look.State.KVstore.GetLeaf("base", key, false)
					if err != nil || leaf == nil || string(leaf.Value) != "committed" || leaf.Version != 1 {
						t.Fatal("replacement lost or replayed the committed prefix", key, err)
					}
				}
				if _, _, err := look.State.KVStoreRead(&Ticket{}, "base", "3"); !errors.Is(err, ErrKeyNotFound) {
					t.Fatal("obsolete uncommitted entry was applied", err)
				}
			})
		})
	}
}

func TestPar_live_uncommitted_suffix_discard(t *testing.T) {
	bubbleOrNot(t, func(t *testing.T) {
		c, original := parLiveCluster(t, [][]int{{2, -4}, {-5}, {-5}})
		c.Nodes[0].testBeginElection <- true
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		select {
		case <-c.LeaderNoop0committedCh:
		case <-ctx.Done():
			t.Fatal("absence quorum did not release the recovering leader")
		}
		for _, node := range c.Nodes {
			for _, key := range []Key{"1", "2"} {
				tkt, err := node.Read(ctx, "base", key, time.Second, nil)
				if err != nil || string(tkt.Val) != "committed" {
					t.Fatalf("node %s lost committed prefix key %s: %v", node.name, key, err)
				}
			}
			look := node.Inspect()
			if look.State == nil || look.State.KVstore == nil {
				t.Fatal("missing recovered application state")
			}
			_, _, err := look.State.KVStoreRead(&Ticket{}, "base", "3")
			look.State.KVstore.Close()
			if !errors.Is(err, ErrKeyNotFound) {
				t.Fatalf("discarded command was applied: %v", err)
			}
			inv, err := scanParWAL(filepath.Join(node.cfg.DataDir, "tube.wal.msgp"), node.ClusterID)
			if err != nil || inv.Err != nil || len(inv.Entries) < 3 {
				t.Fatalf("recovered suffix inventory: %v, %v", inv, err)
			}
			for i := 0; i < 2; i++ {
				if inv.Entries[i].Status != parIntact || !bytes.Equal(inv.Entries[i].Bytes, original[i]) {
					t.Fatal("discard changed a committed prefix entry")
				}
			}
			entry := inv.Entries[2]
			if entry.Status != parIntact || entry.Entry.Term <= 1 || entry.Entry.Ticket.Op != NOOP {
				t.Fatal("leader did not durably replace the uncommitted suffix with its new-term no-op")
			}
		}
	})
}

func TestPar_live_shadow_is_the_only_intact_donor(t *testing.T) {
	bubbleOrNot(t, func(t *testing.T) {
		c, original := parLiveCluster(t, [][]int{{0}, {0}, {0}, {-6}})
		c.Nodes[0].testBeginElection <- true
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		select {
		case <-c.LeaderNoop0committedCh:
		case <-ctx.Done():
			t.Fatal("recovering voters could not use the intact shadow copy")
		}
		for _, node := range c.Nodes[:3] {
			for {
				tkt, err := node.Read(ctx, "base", "1", time.Second, nil)
				if err == nil && string(tkt.Val) == "committed" {
					break
				}
				if !errors.Is(err, ErrPARRecovering) {
					t.Fatalf("voter %s lost acknowledged value: %v", node.name, err)
				}
				select {
				case <-ctx.Done():
					t.Fatal("voter did not finish shadow donor recovery")
				case <-time.After(20 * time.Millisecond):
				}
			}
			inv, err := scanParWAL(filepath.Join(node.cfg.DataDir, "tube.wal.msgp"), node.ClusterID)
			if err != nil || len(inv.Entries) < 3 || inv.Entries[0].Status != parIntact || !bytes.Equal(inv.Entries[0].Bytes, original[0]) {
				t.Fatalf("voter %s did not install the exact shadow donor bytes: %v", node.name, err)
			}
		}
	})
}

func TestPar_repair_round_transition_discards_delayed_evidence(t *testing.T) {
	for _, change := range []string{"term", "config_term", "config_version", "incarnation", "members", "generation", "leadership"} {
		t.Run(change, func(t *testing.T) {
			bubbleOrNot(t, func(t *testing.T) {
				path, _ := parDiskFixture(t, 3)
				intact, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				inv, err := scanParWAL(path, "par-test")
				if err != nil {
					t.Fatal(err)
				}
				target, data := *inv.Entries[1].Record, inv.Entries[1].Bytes
				fd, err := os.OpenFile(path, os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				_, err = fd.WriteAt([]byte{0}, target.Offset)
				if err := errors.Join(err, fd.Close()); err != nil {
					t.Fatal(err)
				}
				cfg := &TubeConfig{ClusterID: "par-test", NoLogCompaction: true}
				wal, err := cfg.openPARWAL(path)
				if err != nil {
					t.Fatal(err)
				}
				defer wal.close()
				mc := &MemberConfig{ConfigTerm: 1, ConfigVersion: 1, PeerNames: NewOmap[string, *PeerDetail]()}
				for _, name := range []string{"leader", "a", "b"} {
					mc.PeerNames.Set(name, &PeerDetail{Name: name, PeerID: "id-" + name})
				}
				node := &TubeNode{cfg: *cfg, name: "leader", PeerID: "id-leader", ClusterID: "par-test", role: LEADER,
					leaderID: "id-leader", leaderTerm: 2, wal: wal, par: &parNode{active: true, slot: 1},
					state: &RaftState{CurrentTerm: 2, CommitIndex: 1, LastApplied: 1, MC: mc}}
				fence := node.parFenceNow("before-transition")
				node.par.round, err = newParRound(fence, target, node.parMembers(), node.parProtected())
				if err != nil {
					t.Fatal(err)
				}
				old := node.par.round
				if err := old.add(fence, parReply{Fence: fence, Member: "a", PeerID: "id-a", Kind: parValid, Record: target, Bytes: data}); err != nil {
					t.Fatal(err)
				}
				partial := parWire{Version: 1, Cluster: "par-test", Fence: fence, Member: "b", PeerID: "id-b", Kind: parValid, Record: target, Total: int64(len(data)), Bytes: data[:len(data)/2]}
				if _, complete, err := node.par.joinReply(fence, partial); err != nil || complete {
					t.Fatal("partial reply fixture", err)
				}
				switch change {
				case "term":
					node.state.CurrentTerm++
					node.leaderTerm++
				case "config_term":
					mc.ConfigTerm++
				case "config_version":
					mc.ConfigVersion++
				case "incarnation":
					mc.PeerNames.Set("a", &PeerDetail{Name: "a", PeerID: "restarted-a"})
				case "members":
					mc.PeerNames.Delkey("b")
				case "generation":
					node.par.generation++
				case "leadership":
					node.state.CurrentTerm++
					node.role = FOLLOWER
					node.leaderID, node.leaderName, node.leaderTerm = "id-a", "a", node.state.CurrentTerm
				}
				before, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				node.parRecoveryStep()
				fresh := node.par.round
				if fresh == nil || fresh == old || len(fresh.replies) != 0 || len(node.par.chunks) != 0 {
					t.Fatal("transition retained old complete or partial evidence")
				}
				packet := parWire{Version: 1, Cluster: "par-test", Fence: fence, Member: "a", PeerID: "id-a", Kind: parValid, Record: target, Total: int64(len(data)), Bytes: data}
				deliver := func() {
					payload, err := json.Marshal(packet)
					if err != nil {
						t.Fatal(err)
					}
					node.parHandle(&rpc.Fragment{FragOp: PARReplyMsg, Payload: payload}, &rpc.Circuit{RemotePeerName: packet.Member, RemotePeerID: packet.PeerID, RemoteServiceName: TUBE_REPLICA})
				}
				deliver() // An already decoded old response arrives after the transition.
				if len(fresh.replies) != 0 || len(node.par.chunks) != 0 {
					t.Fatal("delayed old packet reentered the fresh round")
				}
				after, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(before, after) {
					t.Fatal("stale evidence repaired disk", err)
				}
				packet.Fence, packet.PeerID = fresh.fence, fresh.donors["a"]
				deliver()
				node.parRecoveryStep()
				after, err = os.ReadFile(path)
				if err != nil || !bytes.Equal(intact, after) {
					t.Fatal("fresh checked reply failed to repair the exact original bytes", err, node.par.err)
				}
			})
		})
	}
}

func TestPar_discard_certificate_install_fences(t *testing.T) {
	for _, change := range []string{"none", "term", "config_term", "config_version", "incarnation", "commit", "applied", "config_commit", "follower", "leader", "unknown", "missing_vote", "prefix_damage", "identifier_damage"} {
		t.Run(change, func(t *testing.T) {
			bubbleOrNot(t, func(t *testing.T) {
				path, _ := parDiskFixture(t, 3)
				inv, err := scanParWAL(path, "par-test")
				if err != nil {
					t.Fatal(err)
				}
				fd, err := os.OpenFile(path, os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				_, err = fd.WriteAt([]byte{0}, inv.Entries[1].Record.Offset)
				fd.Close()
				if err != nil {
					t.Fatal(err)
				}
				cfg := &TubeConfig{ClusterID: "par-test", NoLogCompaction: true}
				wal, err := cfg.openPARWAL(path)
				if err != nil {
					t.Fatal(err)
				}
				mc := &MemberConfig{ConfigTerm: 1, ConfigVersion: 1, IsCommitted: true, PeerNames: NewOmap[string, *PeerDetail]()}
				for _, name := range []string{"leader", "a", "b"} {
					mc.PeerNames.Set(name, &PeerDetail{Name: name, PeerID: "id-" + name})
				}
				node := &TubeNode{cfg: *cfg, name: "leader", PeerID: "id-leader", ClusterID: "par-test", role: LEADER,
					leaderID: "id-leader", leaderTerm: 2, wal: wal, par: &parNode{active: true, slot: 1},
					state: &RaftState{CurrentTerm: 2, CommitIndex: 1, LastApplied: 1, MC: mc}}
				defer func() { node.wal.close() }()
				fence := node.parFenceNow("discard")
				node.par.round, err = newParRound(fence, *inv.Entries[1].Record, node.parMembers(), node.parProtected())
				if err != nil {
					t.Fatal(err)
				}
				for _, name := range []string{"a", "b"} {
					if err := node.par.round.add(fence, parReply{Fence: fence, Member: name, PeerID: "id-" + name, Record: node.par.round.target, Kind: parAbsent}); err != nil {
						t.Fatal(err)
					}
				}
				switch change {
				case "term":
					node.state.CurrentTerm++
				case "config_term":
					mc.ConfigTerm++
				case "config_version":
					mc.ConfigVersion++
				case "incarnation":
					mc.PeerNames.Set("a", &PeerDetail{Name: "a", PeerID: "restarted-a"})
				case "commit":
					node.state.CommitIndex = 2
				case "applied":
					node.state.LastApplied = 2
				case "config_commit":
					mc.CommitIndex = 2
				case "follower":
					node.role = FOLLOWER
				case "leader":
					node.leaderID = "another-leader"
				case "unknown":
					node.par.unknown = true
				case "missing_vote":
					delete(node.par.round.replies, "b")
				case "prefix_damage", "identifier_damage":
					name, offset := path, int64(0)
					if change == "identifier_damage" {
						name, offset = path+".parlog", maxParRecord
					}
					fd, err := os.OpenFile(name, os.O_RDWR, 0)
					if err != nil {
						t.Fatal(err)
					}
					_, err = fd.WriteAt([]byte{0}, offset)
					fd.Close()
					if err != nil {
						t.Fatal(err)
					}
				}
				beforeWAL, _ := os.ReadFile(path)
				beforeIDs, _ := os.ReadFile(path + ".parlog")
				err = node.parDiscardSuffix()
				if change == "none" {
					if err != nil {
						t.Fatal(err)
					}
					node.parFinish()
					if node.parBlocked() || len(node.wal.raftLog) != 1 || node.wal.lli != 1 || node.state.CommitIndex != 1 {
						t.Fatal("discard did not preserve and reopen the protected prefix")
					}
					return
				}
				if err == nil {
					t.Fatal("installed an invalidated discard certificate")
				}
				afterWAL, _ := os.ReadFile(path)
				afterIDs, _ := os.ReadFile(path + ".parlog")
				if !bytes.Equal(beforeWAL, afterWAL) || !bytes.Equal(beforeIDs, afterIDs) {
					t.Fatal("rejected discard changed durable history")
				}
			})
		})
	}
}

func TestPar_live_unrecoverable_entry_preserves_history(t *testing.T) {
	bubbleOrNot(t, func(t *testing.T) {
		c, _ := parLiveCluster(t, [][]int{{0}, {0}, {0}})
		c.Nodes[0].testBeginElection <- true
		// Give several actual recovery query/reply rounds time to complete.
		time.Sleep(500 * time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		for _, node := range c.Nodes {
			if _, err := node.Read(ctx, "base", "1", time.Second, nil); !errors.Is(err, ErrPARRecovering) {
				t.Fatalf("%s served a read across a committed hole: %v", node.name, err)
			}
			if _, err := node.Write(ctx, "base", "new", Val("unsafe"), time.Second, nil, "", 0, false); !errors.Is(err, ErrPARRecovering) {
				t.Fatalf("%s accepted a write across a committed hole: %v", node.name, err)
			}
			look := node.Inspect()
			if look.LastLogIndex != 3 || look.State.LastApplied != 0 {
				t.Fatalf("%s changed protected history: last=%d applied=%d", node.name, look.LastLogIndex, look.State.LastApplied)
			}
			look.State.KVstore.Close()
			inv, err := scanParWAL(filepath.Join(node.cfg.DataDir, "tube.wal.msgp"), node.ClusterID)
			if err != nil || len(inv.Entries) != 3 || inv.Entries[0].Status != parDamaged {
				t.Fatalf("%s discarded or executed a protected hole: %v", node.name, err)
			}
		}
		leader, removed := c.Nodes[0], c.Nodes[2]
		look := leader.Inspect()
		role := look.Role
		look.State.KVstore.Close()
		if role != LEADER {
			t.Fatal("test did not elect a leader with identified damage")
		}
		for _, force := range []bool{false, true} {
			inspection, _, err := leader.RemovePeerIDFromCluster(ctx, force, false, removed.name, removed.PeerID, removed.PeerServiceName, removed.BaseServerHostPort(), leader.URL, 0)
			if inspection != nil && inspection.State != nil && inspection.State.KVstore != nil {
				inspection.State.KVstore.Close()
			}
			if !errors.Is(err, ErrPARRecovering) {
				t.Fatalf("unresolved recovery allowed membership change, force=%v: %v", force, err)
			}
			look := leader.Inspect()
			unchanged := look.State.MC.ConfigVersion == 1 && look.State.MC.PeerNames.Len() == 3 && look.LastLogIndex == 3
			look.State.KVstore.Close()
			if !unchanged {
				t.Fatal("membership request changed configuration or history during recovery")
			}
		}
		select {
		case <-c.LeaderNoop0committedCh:
			t.Fatal("recovering leader committed a new no-op")
		default:
		}
	})
}
