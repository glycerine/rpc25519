# Bug Report — mysys (Tube Raft, rpc25519/tube @ a66e9b9c)

## Summary

- Scenarios tested: 11 (S1 term durability, S2 MatchIndex/commit, S3 Extends/compaction, S4 quorum identity, S5 reconfiguration, S6 blocking sends, S7 connection topology, S8 snapshots, S9 PAR / storage faults, S10 replicated KV state machine, S11 czar), plus liveness configs for S3, S4, S6, S7, S8, S9 and S11
- Bugs found: 16 (Critical 2, High 9, Medium 5)
- Configs run: MC.cfg (convergence); safety: MC_hunt_S1_term_durability, MC_hunt_S1_term_durability_dataloss, MC_hunt_S1_diag, MC_hunt_S2_matchindex, MC_hunt_S2_matchindex_dataloss, MC_hunt_S2_matchindex_quorum, MC_hunt_S2_diag, MC_hunt_S3_extends_compaction, MC_hunt_S3_extends_compaction_noacked, MC_hunt_S3_diag, MC_hunt_S3_diag_nobase, MC_hunt_S4_quorum_identity, MC_hunt_S4_quorum_identity_nopanic, MC_hunt_S4_diag, MC_hunt_S4_diag_noshadow, MC_hunt_S4_diag_nodup, MC_hunt_S5_reconfig, MC_hunt_S5_reconfig_nopanic, MC_hunt_S5_reconfig_nopanic_noacked, MC_hunt_S6_blocking, MC_hunt_S6_blocking_nostall, MC_hunt_S7_topology, MC_hunt_S7_diag, MC_hunt_S7_diag_nod38, MC_hunt_S8_snapshot, MC_hunt_S8_snapshot_noacked, MC_hunt_S9_par, MC_hunt_S10_kv, MC_hunt_S10_kv_nosess, MC_hunt_S10_kv_nosess_noreply, MC_hunt_S10_kv_det, MC_hunt_S10_kv_lease, MC_hunt_S10_kv_drift, MC_hunt_S10_kv_drift_det, MC_hunt_S10_kv_drift_nolocal, MC_hunt_S11_czar, MC_hunt_S11_czar_nopanic; liveness: MC_hunt_S3_catchup_live, MC_hunt_S4_stepdown_live, MC_hunt_S6_leader_live (+ _buf2, _buf2q3 bound rechecks), MC_hunt_S6_unblock_live, MC_hunt_S7_blackhole_live, MC_hunt_S7_connectivity_live, MC_hunt_S8_snapshot_live, MC_hunt_S9_par_live, MC_hunt_S11_czar_live (+ _restart1 bound recheck)
- Spec status: converged in round 1 (11/11 traces fully matched; MC.cfg clean). The trace set was re-validated (11/11) after the one spec fix made during hunting (see "Spec changes during bug hunting").
- Coverage caveat (applies to every "depth" below): in this TLC build, multi-worker BFS is not level-synchronous. "depth" for a time-limited BFS is the deepest level any worker reached, not a level explored exhaustively. Only runs marked "exhausted" are complete.

**Answer to the priority question.** Yes. Within Raft's failure bounds:
- **Committed-data loss (classic Raft safety).** A committed entry can end up held by fewer than a majority, so one later crash loses it:
  - Bug 1: the commit rule counts a follower's divergent log suffix.
  - Bug 2: a reordered stale snapshot rolls a follower back below entries it acknowledged. The model also shows the leader committing an entry only it holds.

  Three quorum-identity defects point the same way:
  - Bug 14: a self-removed leader counts its own log in the new configuration's quorum.
  - Bug 15: a restarted follower is counted twice.
  - Bug 13: shadows are counted for the read lease.

  For these three the model reached the defective counting, not the end-to-end loss.
- **Replica divergence in the KV state machine.** Three bugs make replicas apply different state at the same log index (Bugs 3, 4, 6), and one breaks exactly-once execution (Bug 5).
- **Configuration rollback.** An acknowledged membership change can be rolled back (Bug 7).
- **Liveness loss and crashes.**
  - Connection management: one crashed or stopped peer freezes every live replica's event loop, so a healthy majority never elects a leader (Bug 8, confirmed as a liveness lasso).
  - Connection management: one stale-circuit frame tears down a fresh connection to a restarted peer (Bug 9).
  - The czar election can stall forever after a czar dies holding a newer epoch (Bug 16).
  - Leadership changes during a pending reconfiguration panic the deposed leader (Bug 10).
  - The czar can panic on its own rejected renewal (Bug 11).
- **Same term led twice.** One node can lead the same term twice across a crash (Bug 12). No downstream log divergence was found from this.
- **Storage faults.** Loss of the WAL and `.parlog`, a lost last PAR identifier, and torn appends did **not** produce a finding: the PAR fence held in the code and the model (S9, re-run after one spec fix).

| Bug | Severity | Title (short) | Ledger |
|-----|----------|---------------|--------|
| 1 | Critical | Commit counts a follower's divergent suffix | D-3 |
| 2 | Critical | Stale same-term snapshot rolls a follower back | D-5 |
| 3 | High | Rejected lease write mutates the shared log entry | D-52/D-53 |
| 4 | High | Leader-local read auto-deletes outside the log | D-51 |
| 5 | High | Same-serial retry executes twice | D-54 |
| 6 | High | Session GC uses each replica's clock | D-55 |
| 7 | High | Acknowledged reconfiguration rolled back | D-19 |
| 8 | High | One dead/stopped peer freezes all event loops | D-28 |
| 9 | Medium | Stale-circuit frame tears down a fresh connection | D-38 |
| 10 | Medium | Deposed leader with stalled ticket panics | D-20 |
| 11 | Medium | Czar panics on its own rejected renewal | D-63 |
| 12 | Medium | Same node leads a term twice after a crash | D-1/D-2 |
| 13 | Medium | Read lease counts non-voting shadows | D-15 |
| 14 | High | Self-removed leader counts itself in C' quorum | D-14 |
| 15 | High | Restarted follower counted twice | D-17 |
| 16 | High | Czar election stalls forever on a stale epoch | D-62 |

**Liveness configs** (BFS only, about 25 min each):
- **Violated** (Case C, each checked with a bound raised by one):
  - EventuallyLeaderWithHealthyMajority and SenderEventuallyUnblocked (Bug 8).
  - EventuallyCzar (Bug 16).
- **Inconclusive**: BFS did not finish within its budget, and no violation was found in the explored part:
  - CatchUpEventually (S3)
  - StaleLeaderStepsDown (S4)
  - PeerConnectivityRestored and NoPersistentBlackHole (S7)
  - SnapshotEventuallyInstalled (S8)
  - EventuallyLeaderAfterCrashes (S9)

---

## Bug 1: The commit rule counts a follower's divergent log suffix as replicated

- **Scenario**: S2 (MatchIndex / commit soundness), D-3
- **Severity**: Critical
- **Invariant violated**: CommittedOnQuorum (also MatchIndexSound)
- **Config**: MC_hunt_S2_matchindex_quorum.cfg (CommittedOnQuorum); MC_hunt_S2_diag.cfg (MatchIndexSound)
- **Counterexample**: 112 states, spec/output/hunt_S2_matchindex_quorum_SIM.out. Second witness: 113 states, spec/output/hunt_S2_diag_SIM.out. In the S2_diag continue-mode simulation (spec/output/hunt_S2_diag_SIM_C_condensed.out), 8 of the 12 counterexamples collected are this mechanism.

### Trace Summary

1. **Setup.** n3 leads term 1 and appends client entries at indexes 2..4 that only it holds (uncommitted). Entry 1, the term-1 no-op, is on every node.
2. **New leader.** Links flap and n3 is deposed. n2 wins term 2 with the votes of n1 and n2 (both have log `[1]`) and appends its no-op `id5` at index 2.
3. **Fresh follower record.** n2's record for n3 is fresh after the election (MatchIndex 0, LargestCommonRaftIndex 0), so n2 sends a catch-up AE from index 1: `prev 0`, entries `[1]`. The batch cap trims it to one entry.
4. **Wrong acceptance.** n3 is now a term-2 follower. It computes a common prefix of 1, sees the AE adds nothing beyond it, and accepts (`Rejected=false`) on the "nothing new after clipping" branch. The ack reports `PeerLogLastIndex = 4`, which includes its divergent term-1 suffix.
5. **Wrong commit.** n2 sets `MatchIndex[n3] = 4` and runs `leaderAdvanceCommitIndex`. It commits index 2 (`id5`) because the quorum {n2, n3} appears to hold it. In fact only n2 holds `id5`; n3's index 2 is the old term-1 entry and n1 never received it. CommittedOnQuorum fails: a committed entry is on 1 of 3 nodes.
6. **Data loss.** If n2 now crashes, n3 (longer term-1 log) wins with n1's vote and overwrites index 2. A committed entry, already applied on n2, is lost: a Leader Completeness / State Machine Safety failure.

In the S2_diag witness the same steps (102 and 113) give a leader MatchIndex of 4 while the leader's own log has only 2 entries.

### Root Cause

- **Ack reports the whole log.** Every AppendEntries ack is filled from the follower's local log summary, so `PeerLogLastIndex` is the follower's whole last index (`tube/tube.go:8075-8076`, `tube/tube.go:8100`). This includes any uncommitted suffix from an older term that does not match the leader.
- **Accept without truncation.** The "nothing new after clipping" branch accepts an AE whose entries end at or before the common prefix (`tube/tube.go:8377-8385`). It never truncates the divergent suffix and leaves `PeerLogLastIndex` unchanged.
- **MatchIndex taken from the ack.** On an accepted ack the leader sets `MatchIndex = max(MatchIndex, PeerLogLastIndex)` and recomputes the commit index (`tube/tube.go:10109-10116`).
- **Commit from MatchIndex.** `leaderAdvanceCommitIndex` commits the quorum-th largest MatchIndex (`tube/tube.go:10186`, `10262-10263`). Raft requires MatchIndex to cover only entries known to match the leader's log, i.e. `prevLogIndex + len(entries)` of the AE that was acknowledged.
- **How the precondition arises.** A new leader's record starts at 0, and every new or duplicate replica circuit installs a fresh record (`addToCktall`, `tube/tube.go:16832`). Catch-up AEs are capped to `rpc.UserMaxPayload/4` bytes (`tube/tube.go:9756-9768`), so the first catch-up AE often ends inside the common prefix.
- **Already noticed in code.** The developers noted the hazard in `parReconfigurationReady` (`tube/par.go:2160-2162`: "MatchIndex can be raised from PeerLogLastIndex. A longer divergent suffix is not evidence for this leader's prefix") but guarded only the reconfiguration path, not the commit path.

### Affected Code

- `tube/tube.go:8075-8076, 8100`: the ack's `PeerLogLastIndex` is the follower's full local last index
- `tube/tube.go:8377-8385`: "nothing new" branch accepts without truncating a divergent suffix
- `tube/tube.go:10109-10116`: `MatchIndex := PeerLogLastIndex` on accept, then commit advance
- `tube/tube.go:10186, 10262-10263`: commit index computed from MatchIndex
- `tube/tube.go:16832`: every new replica circuit resets the follower record (`newRaftNodeInfo`)
- `tube/tube.go:9756-9768`: catch-up batch cap that makes AE end inside the common prefix
- `tube/par.go:2160-2162`: in-code acknowledgment, applied only to reconfiguration

### Recommendation

Set MatchIndex only from what the acknowledged AE proves: `ae.PrevLogIndex + len(ae.Entries)`, matched to the ack by AEID. Alternatively use the follower's LargestCommonRaftIndex, computed against the leader's current log. Do not use the follower's self-reported last index. Standard Raft never truncates a follower's log beyond the AE's last entry, so this leader-side fix is the essential one. Add a regression test with a deposed leader holding an uncommitted tail.

---

## Bug 2: A stale snapshot from the same leader and term rolls a follower back below entries it acknowledged

- **Scenario**: S8 / S2 / S3 (snapshot install), D-5
- **Severity**: Critical
- **Invariant violated**: AckedStays (an unfenced follower keeps every entry it acknowledged in its current term); also MatchIndexSound
- **Config**: MC_hunt_S2_matchindex.cfg; also MC_hunt_S3_extends_compaction.cfg, MC_hunt_S8_snapshot.cfg and MC_hunt_S2_diag.cfg
- **Counterexample**: 75 states, spec/output/hunt_S2_matchindex_SIM.out. Further witnesses:
  - 104 states, spec/output/hunt_S3_extends_compaction_SIM.out (two circuits on one TCP connection)
  - 97 states, spec/output/hunt_S8_snapshot_SIM.out (multipart snapshot)
  - 123 states, cx01 in spec/output/hunt_S2_diag_SIM_C_condensed.out: the leader commits an entry only it holds

### Trace Summary

1. n1 restarts with an empty log while n2 leads term 1. Two replica circuits exist between them: ckt11 (dialed by n1) and ckt12 (dialed by n2).
2. n1 rejects two AEs with a gap (`NeedSnapshotGap`). The leader answers each rejection on the circuit the ack came in on:
   - snapshot sid1 (state 50, tail AE covering 2..3) on ckt11;
   - snapshot sid2 (state 62, tail AE covering 2..4) on ckt12.
3. n1 installs sid2 first and acks `last = 4` (state 74). The leader's MatchIndex for n1 becomes 4.
4. The older sid1 arrives last (state 75). It comes from the current leader in the current term, so n1 installs it. n1's log goes back to `[1, 2, 3]` and its commit index and last applied move backwards.
5. The leader still counts n1 as holding entry 4, so it can commit index 4 on the leader plus a node that no longer holds it.
   - S3 witness: two circuits on the same TCP connection reorder, and commit index goes from 2 to 1.
   - S8 witness: a multipart snapshot, and commit index goes from 3 to 1.
   - S2_diag witness cx01 shows the consequence end to end:
     1. Follower n2 installs snapshot sid2 (entries to 4) and acks `last 4` (step 110).
     2. The older sid1 rolls n2 back to `[1, 2]` (step 114).
     3. Leader n3 then processes the earlier ack and advances its commit index to 4 (step 123), counting itself and n2.

     Entry 4 is held only by n3: n1 has `[1, 2, 3]` and n2 has `[1, 2]`. A single crash of n3, within Raft's fault tolerance, lets n1 and n2 elect a leader without the committed entry.

### Root Cause

- **Snapshot on the ack's circuit.** A gap rejection sets `NeedSnapshotGap` (`tube/tube.go:8231-8233`, `8282-8288`). The leader then builds and sends a full snapshot on the circuit the ack arrived on (`tube/tube.go:10010-10017` → `handleRequestStateSnapshot`, `tube/tube.go:17464`). AEs go out on `cktall[F]` instead. Duplicate replica circuits are not pruned, so two snapshots for one follower can be in flight on different circuits and be read in either order: per-circuit readers do not keep send order, even across one TCP connection.
- **Minimal receiver checks.** On receipt, `handleStateSnapshotEnclosed` checks only that the sender is the current leader (`tube/tube.go:17562-17568`).
- **Disabled guards.** `applyNewStateSnapshot` has its CommitIndex-rollback guard and its log-length guard disabled with `if false` (`tube/tube.go:17755-17773`). It requires only an equal term (`tube/tube.go:17779-17782`).
- **Unconditional overwrite.** It then assigns CommitIndex, LastApplied and their terms unconditionally (`tube/tube.go:17800-17805`), and `installedSnapshot` truncates the WAL to the snapshot base (`tube/wal.go:1525-1566`, `raftLog[:0]` and `Truncate(0)`). Finally it replays the snapshot's embedded AE (`tube/tube.go:17882-17883`).

### Affected Code

- `tube/tube.go:10010-10017`: snapshot sent on the ack's circuit for every gap rejection
- `tube/tube.go:17562-17568`: only leader identity is checked on receipt
- `tube/tube.go:17755-17773`: monotonicity guards disabled (`if false`)
- `tube/tube.go:17779-17782, 17800-17805`: same-term snapshot overwrites commit/applied state
- `tube/wal.go:1525-1566`: `installedSnapshot` truncates the log
- `tube/tube.go:17882-17883`: the snapshot's AE is replayed after the rollback

### Recommendation

Reject a snapshot whose last included index, plus its embedded AE, does not extend past the follower's current commit index and acknowledged last index. Tag each snapshot with a per-leader sequence number and drop older ones. Send at most one snapshot per follower at a time, keyed by follower rather than by circuit, and prune duplicate replica circuits.

---

## Bug 3: A rejected lease write mutates the shared in-memory log entry, so replicas apply different commands at the same index

- **Scenario**: S10 (KV determinism), D-52 with D-53
- **Severity**: High
- **Invariant violated**: KVDeterminism
- **Config**: MC_hunt_S10_kv_drift_nolocal.cfg (leader-local reads disabled, so Bug 4 cannot fire)
- **Counterexample**: 13 states, spec/output/hunt_S10_kv_drift_nolocal_BFS.out

### Trace Summary

The Raft log is abstracted as one committed order held by the leader r1. Follower r2 lags and receives entries from the leader's in-memory copy.

1. Client c1 creates a session (index 1). Both replicas apply it.
2. c1 makes a leased write at index 2 (`LeaseUntilTm = 2`). Both replicas apply it: version 1, until 2.
3. c1 sends a renewal at index 3 with the same `LeaseUntilTm = 2`.
4. r1 applies index 3 and rejects it as a non-increasing lease. The rejection path writes the current leaf back into the ticket: `LeaseUntilTm := 2 + ClockDriftBound = 3`. That ticket is the in-memory log entry.
5. r2 catches up from r1's in-memory log, receives the mutated entry (until 3), and applies it as a valid renewal: version 2, until 3.
6. At applied index 3, r1 holds `{ver 1, until 2}` and r2 holds `{ver 2, until 3}`. One replica rejected and the other accepted the same log entry. After a failover the lease and its holder differ by replica, which undermines the single-czar guarantee the hermes czar election relies on.

### Root Cause

- **Ticket mutated on rejection.** `writeFailedSetCurrentVal` (`tube/actions.go:766-790`) is called on rejection (`tube/actions.go:679-684`). It overwrites fields of the ticket: `LeaseUntilTm = leaf.LeaseUntilTm + ClockDriftBound` at `tube/actions.go:784`, plus Leasor, Vtype, LeaseEpoch and VersionRead.
- **Ticket shared with the log.** In the apply loop the ticket is `do.Ticket` of the entry returned by `s.wal.GetEntry(applyIdx)` (`tube/tube.go:10428`, `10453`). `GetEntry` returns a pointer into the in-memory `raftLog` (`tube/wal.go:1198-1215`).
- **Mutated entries are shipped.** The leader builds catch-up AEs from the same shared slice (`GetAllEntriesFrom`, `tube/wal.go:1168-1182`, used at `tube/tube.go:9733`). Followers that receive an entry after the leader applied it get the mutated command. A restarted leader reloads the unmutated entry from disk.
- **Clock bound differs by replica.** Each replica's own `ClockDriftBound` (D-53) feeds the mutation, so even the mutated value can differ between leaders.

### Affected Code

- `tube/actions.go:679-684`: rejection calls `writeFailedSetCurrentVal` on the log's ticket
- `tube/actions.go:766-790`: mutates `LeaseUntilTm` (784), Leasor, Vtype, epoch and version fields of that ticket
- `tube/tube.go:10428, 10453`: apply uses the in-memory entry's ticket directly
- `tube/wal.go:1168-1182, 1198-1215`: in-memory entries shared by apply and replication
- `tube/tube.go:9733`: catch-up AE built from the shared entries

### Recommendation

Never mutate a log entry's ticket during apply. Copy the ticket, or keep apply results (`CurValOnFailedWrite`, lease info for the reply) in a separate per-apply result object. Make AE construction send immutable encoded entries, for example the bytes read from the WAL.

---

## Bug 4: A leader-local lease read deletes expired keys outside the log

- **Scenario**: S10 (KV determinism), D-51
- **Severity**: High
- **Invariant violated**: KVDeterminism
- **Config**: MC_hunt_S10_kv_det.cfg; also MC_hunt_S10_kv_drift_det.cfg
- **Counterexample**: 13 states, spec/output/hunt_S10_kv_det_BFS.out. Second witness: 14 states, spec/output/hunt_S10_kv_drift_det_BFS.out.

### Trace Summary

1. c1 creates a session, then writes a leased, auto-delete key at index 2. Both replicas apply it.
2. Time passes beyond the lease expiry.
3. Leader r1 serves a local lease read of the key without a log entry. Reading the leaf with `autoDelete=true` finds it expired, moves it to the "dead" table and deletes it from r1's store.
4. r2 never sees this mutation. Both replicas have applied index 2, but r1's store lacks the key that r2 still holds.
5. Any later CAS, write or read on that key now produces different results on the two replicas for the same log entries.

### Root Cause

- **Read path reaches auto-delete.** `leaderServedLocalRead` (`tube/tube.go:11802`) serves reads under the leader lease without replicating them. It calls `doReadKey` (`tube/tube.go:12153-12159`), which reads through `KVStoreReadLeaf` → `GetLeaf(table, key, autoDelete=true)` (`tube/kv.go:815-820`).
- **Read mutates the store.** `GetLeaf` with `autoDelete` runs a write transaction (`parUpdate`, `tube/par.go:498-506`). It moves an expired auto-delete leaf to the "dead" table and deletes it, using the replica's own `time.Now()` (`tube/kv.go:483-500`).
- **Same hazard on applied reads.** A logged READ applied on each replica deletes according to each replica's own clock.

### Affected Code

- `tube/tube.go:11802`: leader-local read path
- `tube/tube.go:12153-12159`: `doReadKey` reads through the auto-deleting leaf accessor
- `tube/kv.go:815-820`: `KVStoreReadLeaf` passes `autoDelete=true`
- `tube/kv.go:483-500`: expired leaf moved to "dead" and deleted using `time.Now()`
- `tube/par.go:498-506`: the read runs a write transaction

### Recommendation

Reads must not mutate replicated state. Return "expired / not found" without deleting, and perform auto-deletion only through a replicated command (for example a leader-proposed DELETE at a log-determined time). Decide expiry from the log entry's timestamp, not the local clock.

---

## Bug 5: A client retry with the same session serial executes twice, and the first execution's reply is overwritten

- **Scenario**: S10 (exactly-once sessions), D-54
- **Severity**: High
- **Invariant violated**: KVReplyMatchesApply; with that disabled, KVExactlyOnce
- **Config**: MC_hunt_S10_kv_nosess.cfg; MC_hunt_S10_kv_nosess_noreply.cfg
- **Counterexample**: 7 states, spec/output/hunt_S10_kv_nosess_BFS.out. 9 states, spec/output/hunt_S10_kv_nosess_noreply_BFS.out.

### Trace Summary

1. c1 issues a leased write with session serial 1. Before it is applied, c1 retries with the same serial, which is the designed retry pattern. The retry carries a new TicketID.
2. Both copies pass the leader's admission check, because only already-applied serials are rejected, and both are appended to the log.
3. On apply the first copy executes and succeeds. The second copy also executes:
   - in the 7-state trace it is rejected (non-increasing lease) and its result overwrites the cached reply for serial 1, so the client sees "rejected" although its write took effect;
   - in the 9-state trace the retry carries a later timestamp and is applied as a renewal, so the key's version goes from 1 to 2 and the write is applied twice.

### Root Cause

- **Admission checks only applied serials.** `leaderDoneEarlyOnSessionStuff` rejects only `SessionSerial <= MaxAppliedSerial` (`tube/tube.go:16485-16501`). Duplicates that are in flight but not yet applied are admitted.
- **No duplicate check in apply.** The apply loop executes the command first (`switch tkt.Op`, `tube/tube.go:10467`). Only afterwards does it record the session result: `Serial2Ticket.Set(serial, tkt)` and `MaxAppliedSerial` (`tube/tube.go:10607-10636`). Nothing skips a second entry with an already-applied serial, so it re-executes and replaces the cached ticket.
- **Retries reuse the serial.** Clients deliberately retry with the same serial (`tube/fuzz_test.go:1665`, "try again with same serial"), and each call creates a new ticket (`tube/actions.go:114`, `123-124`).

### Affected Code

- `tube/tube.go:16485-16501`: admission dedups only against applied serials
- `tube/tube.go:10467`: command executed before session dedup
- `tube/tube.go:10607-10636`: session result recorded after execution; no duplicate skip
- `tube/actions.go:114, 123-124`: a new ticket per call with the same session serial
- `tube/fuzz_test.go:1665`: the client retry pattern that triggers it

### Recommendation

At apply time, before executing, check `tkt.SessionSerial <= ste.MaxAppliedSerial` (or presence in `Serial2Ticket`). If found, skip execution and answer with the cached result of the first execution. This is the standard Raft session dedup (Ongaro §6.3). Optionally also reject at admission when the same serial is already pending in the log.

---

## Bug 6: Session expiry during apply uses each replica's own clock, so session tables diverge

- **Scenario**: S10 (session determinism), D-55
- **Severity**: High
- **Invariant violated**: KVSessDeterminism
- **Config**: MC_hunt_S10_kv.cfg (also found under drift: MC_hunt_S10_kv_drift.cfg)
- **Counterexample**: 8 states, spec/output/hunt_S10_kv_BFS.out. Drift variant: spec/output/hunt_S10_kv_drift_BFS_condensed.out.

### Trace Summary

1. c1 creates a session at log time 1 with duration 3, so the session ends at time 4.
2. Replica r1 applies that session-creation entry while its clock reads 3 and keeps the session.
3. Replica r2's clock runs one tick ahead. It applies the same entry at local time 4, and the post-apply session GC deletes the session it just created.
4. At the same applied index, r1 has the session and r2 does not. After a failover to r2, c1's next request fails with "unknown SessionID" or is treated without dedup, depending on which replica leads.

### Root Cause

- **Apply clock mixes in local time.** The apply loop advances `LastAppliedLogHLC` with `ReceiveMessageWithHLC(do.LogHLC)` (`tube/tube.go:10440`). That HLC function takes the max with the local wall clock `PhysicalTime48()` (`tube/hlc.go:221-248`, at 233).
- **GC runs during apply.** After each apply, `garbageCollectOldSessions` (`tube/tube.go:10657`) computes `lnow := LastAppliedLogHLC.ToTime()` and deletes sessions whose end time has passed (`tube/tube.go:16267-16295`). The result depends on each replica's clock, not on the log.
- **Leader GC outside the log.** The leader also runs the GC on its election timer (`tube/tube.go:1570`).

### Affected Code

- `tube/tube.go:10440`: apply-time HLC advanced with the local clock
- `tube/hlc.go:221-248`: `ReceiveMessageWithHLC` takes the max with `PhysicalTime48()`
- `tube/tube.go:10657, 16267-16295`: session GC driven by that clock during apply
- `tube/tube.go:1570`: leader-side GC on the election timer

### Recommendation

Use only log-carried time (the entry's `LogHLC` or `RaftLogEntryTm`) for any decision made during apply. Expire sessions through a replicated SESS_END command, or deterministically from the log timestamp, never from `PhysicalTime48()`.

---

## Bug 7: An acknowledged membership change is rolled back after a leader change

- **Scenario**: S5 (logless reconfiguration), D-19
- **Severity**: High
- **Invariant violated**: AckedReconfigDurable
- **Config**: MC_hunt_S5_reconfig_nopanic.cfg (NoPanic / NoFollowerLeaderOnlyPath disabled after Bug 10)
- **Counterexample**: 74 states, spec/output/hunt_S5_reconfig_nopanic_SIM.out

### Trace Summary

1. n2 leads term 1 with configuration (1,1). A client asks it to add n4. The change stalls because a peer's configuration is not yet confirmed, and the ticket is parked on that peer's circuit.
2. n1 wins term 2. On election it rewrites its configuration term, so it holds (2,1), which does not contain n4.
3. n1's first term-2 AE reaches n2. Before n2 processes the term, `suppressWatchdogs` calls `seen()` on n1's circuit. n2 still has role LEADER, so `seen()` resumes the parked ticket and calls `changeMembership`.
4. The Q2 check counts peers by their cached terms, and n3's cached term is stale (1). The check passes, so n2 installs the new configuration locally with `setMC` and immediately acks the client: "n4 added".
5. n2 then processes the AE, steps down to follower in term 2, and adopts n1's configuration (2,1). That configuration is newer by term-major ordering and lacks n4. The acknowledged addition has silently disappeared.

### Root Cause

- **Retry before the term check.** At the top of `handleAppendEntries`, `suppressWatchdogs` (`tube/tube.go:7959`) calls `seen()` before the term check. `seen()` resumes stalled membership tickets whenever `role == LEADER` (`tube/watchdog.go:54-62`).
- **Q2 uses stale cached terms.** `changeMembership` counts peers whose cached `CurrentTerm` equals ours (`tube/tube.go:14521-14524`); it never confirms a current-term round trip.
- **Ack on local install.** The logless reconfiguration installs locally and answers the client at once (`setMC` then `respondToClientTicketApplied`, `tube/tube.go:14743-14757`), before a quorum has adopted it in the current term.
- **Newer leader supersedes it.** A newer leader's election-time ConfigTerm rewrite (`tube/tube.go:7336-7356`) makes its configuration supersede the acknowledged one (`VersionGT` is term-major, `tube/tube.go:13769`).

### Affected Code

- `tube/tube.go:7959`: `suppressWatchdogs(ae)` runs `seen()` before the AE's term is processed
- `tube/watchdog.go:54-62`: `seen()` resumes a stalled `changeMembership` while still leader
- `tube/tube.go:14521-14524`: Q2 counts stale cached peer terms
- `tube/tube.go:14705-14715`: stall and resume path
- `tube/tube.go:14743-14757`: local install acknowledged to the client immediately
- `tube/tube.go:7336-7356, 13769`: ConfigTerm rewrite and term-major ordering supersede it

### Recommendation

Do not resume reconfiguration from `seen()` before the incoming message's term is handled. Do not count a peer for Q2 from cached terms; require a current-term response. Acknowledge the client only after the new configuration is known to be adopted by a quorum in the leader's current term, i.e. logless-committed. Clear stalled reconfiguration tickets, with an error, on step-down (see also Bug 10).

---

## Bug 8: One crashed or stopped peer freezes the event loop of every live replica

- **Scenario**: S6 (blocking sends), D-28
- **Severity**: High
- **Invariant violated**: NoTotalStall. Liveness: EventuallyLeaderWithHealthyMajority and SenderEventuallyUnblocked.
- **Config**: MC_hunt_S6_blocking.cfg; liveness: MC_hunt_S6_leader_live.cfg, MC_hunt_S6_unblock_live.cfg
- **Counterexample**: 6 states, spec/output/hunt_S6_blocking_BFS.out. Liveness lassos:
  - 7 states, spec/output/hunt_S6_leader_live_BFS.out
  - 7 states, spec/output/hunt_S6_unblock_live_BFS.out

### Trace Summary

1. n1 crashes without its sockets being reset (power loss, cable pull, hung VM). The peers' TCP connections to n1 stay half-open.
2. n2's election timer fires and it sends RequestPreVote to all peers. The one to n1 fills the socket buffer that n1 never drains.
3. n2's next send to n1 blocks the event loop inside `SendOneWay`.
4. n3 does the same, and its event loop also blocks sending to n1.
5. Every live replica's event loop is blocked. No election, heartbeat, AE processing or client handling happens anywhere: the healthy majority {n2, n3} is unavailable.
   - A crashed peer frees them only when TCP aborts the connection (minutes, ~15 by default).
   - A stopped-but-ACKing peer (SIGSTOP, wedged process with a live kernel, zero window) frees them never.

**Liveness confirmation (fair specification).**
- **EventuallyLeaderWithHealthyMajority** (MC_hunt_S6_leader_live): a 7-state lasso.
  1. n3 is stopped; its kernel keeps ACKing.
  2. n1's election timer fires twice, and its second RequestPreVote to n3 blocks n1's event loop.
  3. n2 handles n1's pre-vote request, then its own election timer fires and its send to n3 blocks too.
  4. The behavior stutters forever: the healthy majority {n1, n2} never elects a leader.
- **Is the stall real?** In the stuttering state the only enabled actions without fairness are resuming n3 (`Cont`) and new faults. No fair action can release the sends: a stopped peer never drains, and its kernel never aborts. The implementation cannot guarantee that a stopped or wedged peer resumes, so this is Case C.
- **Bound check.** Raising the kernel send buffer from 1 to 2 (MC_hunt_S6_leader_live_buf2.cfg): inconclusive. Timer sends require every queue < MaxQueue, so with MaxQueue = 2 = SendBufCap the blocked state is unreachable by construction; the stuck point moves to MaxQueue (spec/output/hunt_S6_leader_live_buf2_BFS{,_r2}.out). With SendBufCap 2 and MaxQueue 3 (MC_hunt_S6_leader_live_buf2q3.cfg, spec/output/hunt_S6_leader_live_buf2q3_BFS.out), BFS ran 18 min (797,899 distinct states, max depth 12) without finishing and without finding the longer (~10-step) lasso. The stall itself does not depend on the buffer size, because a real socket buffer is finite and a stopped peer never drains it. That is shown by the code (`srv.go:2538-2546`, `common.go:438-450`) and by the NoTotalStall safety counterexample.
- **SenderEventuallyUnblocked** (MC_hunt_S6_unblock_live): violated by the same mechanism (n1 stopped; n2 and n3 block sending to it). The modeling brief predicted this.

### Root Cause

- **No timeout on the send.** Tube calls `SendOneWay` with `errWriteDur = -1` (e.g. `tube/tube.go:1632, 6441, 6893, 7509`). In rpc25519 that takes the blocking branch of `sendOneWayMessage`: a `select` on the connection's send channel with only halt or context-cancel exits, no timeout and no default (`srv.go:2538-2546`). A nearby developer comment reads "paused 17 minutes here!?" (`srv.go:2532`).
- **Unbuffered channels.** The channels are unbuffered by design ("not buffered! synchronous so we get back-pressure", `cli.go:1914-1915`; `srv.go:1975`).
- **No write deadline.** The writer goroutine blocks in `writeFull` with no deadline when none is given (`common.go:438-450`). A send to one unresponsive peer therefore blocks the single Tube event loop.

### Affected Code

- `srv.go:2538-2546`: blocking hand-off to the per-connection send channel
- `cli.go:1914-1915`, `srv.go:1975`: unbuffered send channels
- `common.go:438-450`: `writeFull` without a write deadline
- `tube/tube.go:1632, 6441, 6893, 7509`: event-loop sends with `errWriteDur = -1`

### Recommendation

Never block the Raft event loop on a network send. Give each peer a bounded outbound queue drained by its own goroutine, dropping or coalescing on overflow (Raft tolerates message loss). Or set a write deadline and use a non-blocking hand-off with a timeout, closing the circuit when it expires. Enable TCP_USER_TIMEOUT/keepalive so half-open connections to dead peers are detected promptly.

---

## Bug 9: A frame for a restarted peer's old circuit tears down the new connection to that peer

- **Scenario**: S7 (connection topology), D-38
- **Severity**: Medium
- **Invariant violated**: NoCollateralTeardown (action property)
- **Config**: MC_hunt_S7_diag.cfg
- **Counterexample**: 7 states, spec/output/hunt_S7_diag_BFS.out

### Trace Summary

1. n2 crashes. n1's connection conn2 to n2 is aborted, but n1 has not yet removed circuit ckt2, which belongs to n2's old incarnation.
2. n2 restarts at the same address with a new PeerID (incarnation 1).
3. n1's election timer fires. It sends RequestPreVote on its stale ckt2, addressed to n2 incarnation 0. Because conn2 is closed, the frame is routed by address: n1 dials n2's address and gets a fresh connection conn10 to the new n2.
4. n2 (incarnation 1) reads a frame whose ToPeerID/circuit it does not know. Its read loop returns, closing conn10 and every circuit on it, although both processes are up and the link is healthy.

### Root Cause

- **Unknown circuit closes the connection.** In rpc25519, when an incoming `CallPeerTraffic`/`CallPeerTrafficWithServiceFallback`/`CallPeerError` frame is not claimed by any local circuit (unknown ToPeerID or CallID), the server read loop returns and tears down the whole connection (`srv.go:803-813`). The client read loop does the same (`cli.go:532-538`). A developer comment there (`srv.go:806-809`) notes seeing AppendEntries here after a leader restart.
- **Stale circuits route to the new process.** Stale circuits are routed by address onto the connection to the peer's new incarnation.
- **Can repeat.** With zombie circuits the teardown can repeat on every send, causing connection churn.

### Affected Code

- `srv.go:803-813`: unknown-target traffic frame makes the server read loop return
- `cli.go:532-538`: the same on the client side

### Recommendation

Drop, and optionally answer with a CallPeerEndCircuit for, a frame addressed to an unknown local peer or circuit, instead of returning from the read loop. Tear the connection down only on protocol or authentication errors. On the sending side, retire all circuits bound to a peer incarnation when its connection fails, before routing new traffic to the same address.

---

## Bug 10: A deposed leader with a stalled membership ticket panics

- **Scenario**: S4 / S5 (stalled reconfiguration across a leader change), D-20
- **Severity**: Medium
- **Invariant violated**: NoPanic (panic "only for leader; followers should not be calling here"); NoFollowerLeaderOnlyPath
- **Config**: MC_hunt_S4_quorum_identity.cfg; MC_hunt_S5_reconfig.cfg
- **Counterexample**: 83 states, spec/output/hunt_S4_quorum_identity_SIM.out. Second witness: 76 states, spec/output/hunt_S5_reconfig_SIM.out.

### Trace Summary

1. Leader n1 accepts a membership change (remove a node) that stalls because a peer's configuration is not yet confirmed. The ticket goes to `stalledMembershipConfigChangeTkt`.
2. n1 is deposed: another node wins term 2. Its election-time ConfigTerm rewrite gives it a newer configuration.
3. n1 becomes a follower but keeps its stalled-ticket queue.
4. The new leader's AE carries the newer configuration. `aeMemberConfigHelper` calls `connectToMC("AE")`, whose deferred `resubmitStalledTickets(true)` pops the stalled ticket. It calls `commandSpecificLocalActionsThenReplicateTicket`, which panics because n1 is no longer leader.
   - In the S5 witness, n3's stalled "remove n2" panics the same way.

### Root Cause

- **Queue survives step-down.** Stalled membership tickets are only removed when resubmitted or forced (`tube/tube.go:14357-14385`, `6052`). `becomeFollower` (`tube/tube.go:6572`) does not clear them.
- **Resubmit without a role check.** `connectToMC` always defers `resubmitStalledTickets(true)` (`tube/tube.go:15233-15237`), from the AE path (`tube/tube.go:7833`) and the election-timeout path (`tube/tube.go:1451`). `resubmitStalledTickets` (`tube/tube.go:6031-6072`) calls the leader-only `commandSpecificLocalActionsThenReplicateTicket` without checking the role. That function panics for non-leaders (`tube/tube.go:17118-17121`).

### Affected Code

- `tube/tube.go:14715`: ticket parked on `stalledMembershipConfigChangeTkt`
- `tube/tube.go:6572`: `becomeFollower` does not clear or fail stalled tickets
- `tube/tube.go:7833, 1451, 15233-15237`: `connectToMC` defers `resubmitStalledTickets(true)` on followers too
- `tube/tube.go:6031-6072`: resubmit without a role check
- `tube/tube.go:17118-17121`: panic "only for leader"

### Recommendation

On step-down, fail all stalled membership tickets back to their clients with a redirect or "not leader" error. Guard `resubmitStalledTickets` with `if s.role != LEADER { redirect/fail; return }`.

---

## Bug 11: The czar panics when its own lease renewal is rejected

- **Scenario**: S11 (czar / reliable membership), D-63
- **Severity**: Medium
- **Invariant violated**: CzNoPanic
- **Config**: MC_hunt_S11_czar.cfg
- **Counterexample**: 7 states, spec/output/hunt_S11_czar_BFS.out

### Trace Summary

1. Member m1 wins the czar lease (`LeaseUntilTm` T).
2. m1 renews in the same clock tick. In production the renewal fires every 1 s against a 5 s lease, so this needs the stamping leader's clock to be at least 1 s behind the previous stamp: failover to a lagging leader, or a backward clock step.
3. The renewal's `LeaseUntilTm` is not greater than the stored one, so Tube rejects the write. The failed-write reply carries the current value, which names m1 as czar.
4. m1's czar loop sees itself named as czar in a failed write and panics ("why am I czar on failed write?"), crashing the hermes member that holds the czar role.

### Root Cause

- **Equal timestamps rejected.** Tube rejects non-increasing `LeaseUntilTm` writes (`tube/actions.go:679-684`) and returns the current leaf in the reply (`tube/actions.go:766-790`).
- **Czar treats it as impossible.** `tube/czar.go` panics when the current czar in a failed write is itself (`tube/czar.go:1161-1166`), instead of treating it as "still czar, lease unchanged".
- **Renewal cadence.** Renewal every `leaseDurCzar/5` (`tube/czar.go:155-156`, `1125-1129`) makes equal or backwards stamps possible after a leader change.

### Affected Code

- `tube/czar.go:1161-1166`: panic when a failed write names this member as czar
- `tube/actions.go:679-684`: non-increasing lease rejected
- `tube/czar.go:155-156, 1125-1129`: renewal cadence

### Recommendation

When a renewal fails and the current value names this member with an unexpired lease, keep czar status and retry later instead of panicking. On the Tube side, consider letting the current leasor's renewal with an equal `LeaseUntilTm` succeed as a no-op.

---

## Bug 12: A node can lead the same term twice after a crash (term and vote are saved too late)

- **Scenario**: S1 (term durability), D-1 / D-2
- **Severity**: Medium
- **Invariant violated**: ElectionSafetyInc (incarnation-aware election safety); diagnostic DurableTermCoversLog
- **Config**: MC_hunt_S1_term_durability.cfg; MC_hunt_S1_diag.cfg
- **Counterexample**: 62 states, spec/output/hunt_S1_term_durability_SIM.out. Diagnostic: 7 states, spec/output/hunt_S1_diag_BFS.out.

### Trace Summary

1. n3 campaigns for term 1. `beginElection` increments the term and votes for itself in memory, then sends RequestVote, without saving term or vote.
2. n3 wins with n2's vote and becomes leader of term 1. `becomeLeader` replicates and fsyncs its term-1 no-op before saving `RaftState`.
3. n3 crashes in that window. Its WAL holds a term-1 entry, but its saved `CurrentTerm` is still 0 and its vote was never recorded.
4. On restart n3 takes `CurrentTerm = 0` from `RaftState` and does not reconcile it with the WAL's last term (1). It campaigns for term 1 again, n1 grants the vote, and n3 leads term 1 a second time as a new incarnation.
   - The diagnostic run shows the precondition in 7 states: a term-1 entry durable while the durable term is 0.

A 30-minute downstream simulation without ElectionSafetyInc (10,968,456 states, spec/output/hunt_S1_term_durability_dataloss_SIM.out) found no LogMatching, LeaderCompleteness or StateMachineSafety violation. The second leadership is by the same node over the same WAL, and other nodes cannot win the same term without the term-1 entry. Severity is therefore Medium: an invariant breach of Raft's persistence rules without observed data loss.

### Root Cause

- **No save before campaigning.** `beginElection` increments `CurrentTerm` and sets `VotedFor` (`tube/tube.go:6790-6795`) and sends RequestVote (`tube/tube.go:6893`) without a prior `saver.save`.
- **Save after the fsync.** `becomeLeader` replicates the no-op (`tube/tube.go:7461`, fsynced to the WAL) before `saver.save(s.state)` (`tube/tube.go:7473-7475`).
- **No reconciliation on restart.** Restart deliberately does not reconcile `CurrentTerm` with the WAL (`tube/tube.go:9376-9383`).

### Affected Code

- `tube/tube.go:6790-6795`: term and self-vote changed in memory only
- `tube/tube.go:6893`: RequestVote sent before persistence
- `tube/tube.go:7461, 7473-7475`: no-op fsynced before `RaftState` save
- `tube/tube.go:9376-9383`: no reconciliation of `CurrentTerm` with the WAL's last term on restart

### Recommendation

Persist `CurrentTerm` and `VotedFor` (fsync) before sending RequestVote, as Raft requires. On restart set `CurrentTerm = max(saved term, WAL last term)`.

---

## Bug 13: The leader's read lease counts acknowledgments from non-voting shadow replicas

- **Scenario**: S4 (quorum identity), D-15
- **Severity**: Medium
- **Invariant violated**: QuorumCountsDistinctVoters (conjunct: every replica counted for the read lease is a voting member)
- **Config**: MC_hunt_S4_diag.cfg
- **Counterexample**: 61 states, spec/output/hunt_S4_diag_SIM.out

### Trace Summary

1. The cluster has voters {n1, n2, n3} and a shadow (non-voting) replica n4. n1 crashes and restarts; links flap.
2. n3 wins term 1 with its own vote and n2's (the shadow n4 also answers the RequestVote, but its reply arrives after the election). n3's no-op is broadcast to n1, n2 and the shadow n4.
3. The shadow n4 accepts n3's AppendEntries and acks it.
4. n3 records a fresh ping for n4 and gives it read-lease credit. Its lease quorum (1 of 2 needed besides itself, for 3 voters) is now met by a node that is not in its configuration and never votes.

### Root Cause

- **No voter filter.** `leaderCanServeReadsLocally` counts every `s.peers` record whose service is `TUBE_REPLICA` and whose `LastFullPing` is inside the lease window (`tube/tube.go:12341-12368`). It has no membership filter and no per-name de-duplication.
- **Shadows match that filter.** Non-voting shadows run as `TUBE_REPLICA` (`tube/tube.go:399-401`).
- **The commit path does filter.** The commit path filters by the current configuration (`tube/tube.go:10255-10261`); the lease path does not.
- **Why it matters.** A Raft read lease is safe only if the acknowledging nodes are voters that refuse to elect another leader during the lease. A shadow's ack carries no such promise.

**Consequence.** Take a leader L that the voters can no longer reach but that still talks to a shadow (asymmetric partition, or a shadow left connected). L keeps renewing its read lease from the shadow's acks. Meanwhile the voters elect a new leader and commit writes, so L serves stale local reads: a linearizability violation. D-16 also keeps L from stepping down in this situation, because pongs from any acker count.

The model did **not** reproduce that end-to-end consequence. LeaseSafety held in MC_hunt_S4_quorum_identity BFS (30 min, max depth 11) and in its derived SIM run (7,084,934 states). The finding is the confirmed counting defect.

### Affected Code

- `tube/tube.go:12341-12368`: lease loop counts any `TUBE_REPLICA` peer with a fresh ping
- `tube/tube.go:399-401`: shadows are `TUBE_REPLICA`
- `tube/tube.go:10255-10261`: the commit path's membership filter, absent from the lease path

### Recommendation

Count only voting members of the current committed configuration toward the read lease, de-duplicated by name and limited to the current PeerID. Apply the same filter to the §6.2 pong step-down check.

---

## Bug 14: A leader that removes itself keeps leading and counts its own log toward the new configuration's commit quorum

- **Scenario**: S4 (quorum identity) / S5 (reconfiguration), D-14
- **Severity**: High
- **Invariant violated**: QuorumCountsDistinctVotersExceptShadowLease (conjunct: a leader is a member of its own configuration)
- **Config**: MC_hunt_S4_diag_noshadow.cfg
- **Counterexample**: 91 states, spec/output/hunt_S4_diag_noshadow_SIM.out

### Trace Summary

1. The cluster has voters {n1, n2, n3} and shadow n4. n2 crashes and restarts, links flap, and n1 wins term 1 (steps 2-55).
2. Step 56: the operator asks leader n1 to remove itself. Under the logless reconfiguration, n1 installs C' = {n2, n3} (version (1,2), not yet committed) and stays leader.
3. Steps 57-91: n1 keeps heartbeating. n2 (incarnation 1) acks up to index 1 and n3 has acked nothing.
4. At step 91 n1 is still leader of a configuration that excludes it. Its commit computation uses its own last index as one of the votes for C'.

### Root Cause

- **The leader always counts itself.** `leaderAdvanceCommitIndex` always adds the leader's own `lastLogIndex()` ("add our own, since we are not in peers", `tube/tube.go:10244-10247`). It filters only the *peers* by the current configuration (`tube/tube.go:10255-10261`).
- **Quorum sized to C'.** The quorum is `clusterSize()/2 + 1` with `clusterSize()` = the size of the current configuration C' (`tube/tube.go:6904-6914`, `6938-6949`). For a leader outside C', its own log therefore stands in for a member of C'.
- **No step-down after self-removal.** The step-down Raft prescribes once the new configuration commits (dissertation §4.2.2, "If we were removed, step down") is commented out (`tube/tube.go:14765-14768`).
- **Elections already handle this.** Elections exclude a non-member self (`weAreMemberOfCurrentMC`, `tube/tube.go:6808`).

**Consequence (argued from the code; the model reached the precondition but not the loss).**
- **Removal 2 → 1.** C' = {n2}, quorum 1. The removed leader commits any entry it holds, even one n2 lacks. When the removed node is shut down, n2 leads alone without those committed entries.
- **Removal 4 → 3.** Quorum 2 is met by the leader plus one C' member. An entry on 1 of 3 C' members is committed, and the other two can elect a leader without it.

### Affected Code

- `tube/tube.go:10244-10247`: leader's own last index always counted
- `tube/tube.go:10255-10261`: membership filter applied to peers only
- `tube/tube.go:6904-6914, 6938-6949`: quorum sized to the new configuration
- `tube/tube.go:14765-14768`: step-down after self-removal commented out

### Recommendation

Count the leader's own log only when `weAreMemberOfCurrentMC()`, as elections already do. Step down once the configuration that removes the leader is committed. Until then, the removed leader replicates without counting itself.

---

## Bug 15: A restarted follower is counted twice (old and new incarnation) in the leader's commit and lease quorums

- **Scenario**: S4 (quorum identity), D-17
- **Severity**: High
- **Invariant violated**: QuorumCountsNoDuplicateReplica (no replica name counted twice for commit or lease)
- **Config**: MC_hunt_S4_diag_nodup.cfg
- **Counterexample**: 53 states, spec/output/hunt_S4_diag_nodup_SIM.out

### Trace Summary

1. The cluster has voters {n1, n2, n3} and shadow n4. n1 crashes (power loss: its old TCP connection and circuit `ckt2` stay half-open at the peers) and restarts with a new PeerID (incarnation 1).
2. n1 dials the others, and n2 wins term 1 (steps 43-53).
3. Leader n2's peer records now hold both `<<n1, 0>>`, the dead incarnation reached over the half-open `ckt2`, and `<<n1, 1>>`, the new incarnation over `ckt17`. Both carry the name n1, which is in the configuration, so both pass the commit-quorum membership filter and n1 can be counted twice.

### Root Cause

- **Records keyed by PeerID, no name de-duplication.** `s.peers` and `cktall` are keyed by PeerID. `leaderAdvanceCommitIndex` iterates all records and filters them only by *name* membership (`tube/tube.go:10250-10266`). The read-lease loop does the same (`tube/tube.go:12341-12368`), as does the §6.2 pong check.
- **Old record never replaced.** `addToCktall` looks up an existing record by the *new* PeerID (`tube/tube.go:16666`). The branch meant to replace an old incarnation's record (`tube/tube.go:16715-16718`) therefore never fires for a restart. The old record, with whatever MatchIndex the old incarnation reached, lingers until the old circuit's reader exits. After a power loss that is only when TCP gives up, about 15 minutes (see Bug 8).
- **Votes are de-duplicated; commits are not.** Votes are de-duplicated by name, and `parReconfigurationReady` de-duplicates by name and checks the current PeerID (`tube/par.go:2159-2172`); the commit path does not.

**Consequence (argued; the 3-voter model reaches the precondition but double counting cannot change a 2-of-3 quorum).** In a 5-voter cluster, the leader plus both records of one restarted follower meet the quorum of 3. An entry held by only 2 distinct nodes is then committed and is lost if those 2 fail, which Raft's 2-failure tolerance must survive. The same double count inflates the read lease.

### Affected Code

- `tube/tube.go:10250-10266`: commit quorum over PeerID-keyed records, no per-name de-duplication
- `tube/tube.go:12341-12368`: read-lease count, same issue
- `tube/tube.go:16666, 16715-16718`: old incarnation's record never replaced on restart

### Recommendation

Key replication progress by peer name. On a new incarnation (new PeerID for the same name), delete the old record immediately. Count at most one record per name, the current PeerID's, for commit, lease and step-down, as `parReconfigurationReady` already does.

---

## Bug 16: After a czar dies holding a newer lease epoch, members that know only the older epoch can never become czar

- **Scenario**: S11 (czar / reliable membership), D-62
- **Severity**: High
- **Invariant violated**: EventuallyCzar (the cfg's liveness `PROPERTY`)
- **Config**: MC_hunt_S11_czar_live.cfg
- **Counterexample**: 17-state lasso (states 15-17 repeat), spec/output/hunt_S11_czar_live_BFS.out

### Trace Summary

1. Members m1 and m2 both submit a CAS on the czar key, each expecting lease epoch 0.
2. m1's reply is lost, so it never learns the outcome. Tube applies m1's CAS: the key now names m1 at epoch 1 with a 3-tick lease (state 7).
3. m1 crashes and stays down (state 8).
4. m2's CAS (expecting epoch 0) is rejected. The reply names m1 at epoch 1 (state 9).
5. Time passes and m1's lease expires (states 10-14).
6. m2 handles the rejection and stays NotCzar (state 15). It resubmits, still expecting epoch 0, and is rejected again (states 16-17). The behavior returns to state 15 and repeats forever: m2 is alive the whole time but never becomes czar, and every pass appends another CAS entry to the Raft log.

### Root Cause

- **Stale epoch reused.** The czar loop's CAS carries the member's last known epoch (`prevLeaseEpoch = vers.CzarLeaseEpoch`, `tube/czar.go:1013-1016`).
- **The reply's epoch is never learned.** The rejected reply carries the holder's already-expired lease, which the version update refuses (`ErrExpiredCzarLease`, `tube/czar.go:226-229`).
- **CAS enforced on an expired lease.** Tube enforces the epoch CAS even when the stored lease has expired; the expired-lease bypass is commented out (`tube/tube.go:12083-12089`).

A member that missed epoch E+1 is therefore locked out until the dead holder comes back. If every surviving member is in that state, nobody ever becomes czar.

**Why this is a real stall (Case C).**
- The repeating part uses only fair actions: submit, Tube apply, handle result. m2 is up throughout.
- The faults are one lost response and one crash, both within the configured fault bounds.
- The clock floor `CzClamp` only saturates an expired lease; it does not block progress.
- Bound check, raising `MaxCzRestart` 0 → 1 (MC_hunt_S11_czar_live_restart1.cfg): the same 17-state lasso is found (spec/output/hunt_S11_czar_live_restart1_BFS.out), so the stall does not depend on the dead czar staying down.

### Affected Code

- `tube/czar.go:1013-1016`: CAS uses the member's stale known epoch
- `tube/czar.go:226-229`: expired lease in a reply refused, so the newer epoch is never learned
- `tube/tube.go:12083-12089`: expired-lease bypass of the epoch CAS commented out

### Recommendation

When a CAS is rejected, learn the holder's epoch from the reply even if its lease has expired, then retry with that epoch. Alternatively, let Tube's epoch CAS succeed (with epoch+1) once the stored lease has expired. Add backoff to the retry loop so a stuck member does not flood the Raft log.

---

## Not Reproduced

Every config run during bug hunting is listed with its coverage. BFS runs were limited to 30 minutes and simulations to 30 minutes (`-S -n 999999999`, depth 200). "Not exhausted" BFS results give the deepest level any worker reached (see the coverage caveat in the Summary). Rows that produced a counterexample name the Bug they support. Liveness rows are BFS-only.

| Scenario | Config | States Explored | Result |
|------------|--------|-----------------|--------|
| Convergence | MC.cfg | BFS 30 min: 34,057,780 distinct (150.3M generated), max depth 15, not exhausted; SIM 30 min: 29,842,660 states, 197,579 traces | No violation |
| S1 term durability | MC_hunt_S1_term_durability.cfg | BFS: 14,441,000 distinct, max depth 13, not exhausted; SIM: 4,357,321 states | BFS: no violation. SIM: ElectionSafetyInc violated, 62 states (Bug 12) |
| S1 | MC_hunt_S1_term_durability_dataloss.cfg (ElectionSafetyInc off) | SIM: 10,968,456 states, 67,048 traces | No violation (LogMatching, LeaderCompleteness, StateMachineSafety hold) |
| S1 | MC_hunt_S1_diag.cfg | BFS: 175,326 distinct when stopped | DurableTermCoversLog violated, 7 states (Bug 12 diagnostic) |
| S2 MatchIndex | MC_hunt_S2_matchindex.cfg | BFS: 11,613,390 distinct, max depth 12; SIM: 2,630,373 states | BFS: no violation. SIM: AckedStays violated, 75 states (Bug 2) |
| S2 | MC_hunt_S2_matchindex_dataloss.cfg (AckedStays off, MaxCrash 2) | SIM: 14,734,973 states, 188,708 traces | No violation of LeaderCompleteness / StateMachineSafety |
| S2 | MC_hunt_S2_matchindex_quorum.cfg | SIM: 6,067,129 states | CommittedOnQuorum violated, 112 states (Bug 1) |
| S2 | MC_hunt_S2_diag.cfg | BFS: 13,654,935 distinct, max depth 13; SIM: 649,030 states; SIM with -C: 8,416,533 states | BFS: no violation. SIM: MatchIndexSound violated, 113 states (Bug 1). -C: 12 counterexamples, all Bug 1 or Bug 2 |
| S3 Extends / compaction | MC_hunt_S3_extends_compaction.cfg | BFS: 13,085,740 distinct, max depth 13; SIM: 3,629,793 states | BFS: no violation. SIM: AckedStays violated, 104 states (Bug 2) |
| S3 | MC_hunt_S3_extends_compaction_noacked.cfg | SIM: 9,264,364 states | No violation |
| S3 | MC_hunt_S3_diag.cfg | BFS: 13,504,068 distinct, max depth 13; SIM: 889,737 states | BFS: no violation. SIM: LcriSound violated, 128 states: harmless LCRI past a stale AE's end, up to the follower's own committed base (Case A; see spec changes) |
| S3 | MC_hunt_S3_diag_nobase.cfg (LcriSoundExceptBase) | SIM: 8,342,080 states | No violation: D-6 proper (leader base inside a follower's divergent tail) not reproduced |
| S4 quorum identity | MC_hunt_S4_quorum_identity.cfg | BFS: 10,573,294 distinct, max depth 11; SIM: 3,251,493 states | BFS: no violation (incl. LeaseSafety). SIM: NoPanic violated, 83 states (Bug 10) |
| S4 | MC_hunt_S4_quorum_identity_nopanic.cfg | SIM: 7,084,934 states | No violation: LeaseSafety, LeaderCompleteness, StateMachineSafety hold. The stale read Bug 13 implies was not reproduced |
| S4 | MC_hunt_S4_diag.cfg | BFS: 10,845,360 distinct, max depth 11; SIM: 577 states | BFS: no violation. SIM: QuorumCountsDistinctVoters violated, 61 states (Bug 13) |
| S4 | MC_hunt_S4_diag_noshadow.cfg | SIM: 2,232 states | Violated, 91 states (Bug 14) |
| S4 | MC_hunt_S4_diag_nodup.cfg | SIM: 3,503 states | Violated, 53 states (Bug 15) |
| S5 reconfiguration | MC_hunt_S5_reconfig.cfg | BFS: 11,360,537 distinct, max depth 12; SIM: 21,610 states | BFS: no violation. SIM: NoFollowerLeaderOnlyPath violated, 76 states (Bug 10) |
| S5 | MC_hunt_S5_reconfig_nopanic.cfg | SIM: 432,458 states | AckedReconfigDurable violated, 74 states (Bug 7) |
| S5 | MC_hunt_S5_reconfig_nopanic_noacked.cfg | SIM: 8,673,344 states | No violation (ConfigUnique and the classic invariants hold) |
| S6 blocking | MC_hunt_S6_blocking.cfg | BFS: 5,063 distinct when stopped | NoTotalStall violated, 6 states (Bug 8) |
| S6 | MC_hunt_S6_blocking_nostall.cfg | BFS: 15,389,973 distinct, max depth 13; SIM: 11,551,013 states | No violation |
| S7 topology | MC_hunt_S7_topology.cfg | BFS: 11,494,247 distinct, max depth 12; SIM: 11,137,302 states | No violation |
| S7 | MC_hunt_S7_diag.cfg | BFS: 57,536 distinct when stopped | NoCollateralTeardown violated, 7 states (Bug 9) |
| S7 | MC_hunt_S7_diag_nod38.cfg (NoCollateralTeardownExceptD38) | BFS: 8,105,601 distinct, max depth 12; SIM: 8,080,791 states | No violation: D-37 (close-with-reason sent as CallPeerError with an empty ToPeerID) not reproduced |
| S8 snapshots | MC_hunt_S8_snapshot.cfg | BFS: 11,285,793 distinct, max depth 13; SIM: 4,526,987 states | BFS: no violation. SIM: AckedStays violated, 97 states (Bug 2) |
| S8 | MC_hunt_S8_snapshot_noacked.cfg | SIM: 9,233,755 states | No violation |
| S9 PAR / storage faults (before walGone fix) | MC_hunt_S9_par.cfg | BFS run 1: AckedStays, 10 states (Case A, invariant revised); BFS run 2: 14,559,391 distinct, max depth 14; SIM: 15,028,665 states | No violation after the Case A revision |
| S9 (after walGone fix) | MC_hunt_S9_par.cfg | BFS: 18,467,181 distinct, max depth 13; SIM: 20,032,887 states, 496,525 traces | No violation: storage faults (lost PAR identifiers, torn appends, WAL+parlog loss) did not break the fence or the classic invariants |
| S10 KV | MC_hunt_S10_kv.cfg | BFS: 6,572 distinct when stopped | KVSessDeterminism violated, 8 states (Bug 6) |
| S10 | MC_hunt_S10_kv_nosess.cfg | BFS: 25,292 distinct when stopped | KVReplyMatchesApply violated, 7 states (Bug 5) |
| S10 | MC_hunt_S10_kv_nosess_noreply.cfg | BFS: 110,873 distinct when stopped | KVExactlyOnce violated, 9 states (Bug 5) |
| S10 | MC_hunt_S10_kv_det.cfg | BFS: 9,196,403 distinct | KVDeterminism violated, 13 states (Bug 4) |
| S10 | MC_hunt_S10_kv_lease.cfg (only KVLeaseMutex) | BFS: 57,932,787 distinct, max depth 19, not exhausted; SIM: 475,880,876 states, 23,299,492 traces | No violation: at most one lease holder per key in every replica's applied state |
| S10 | MC_hunt_S10_kv_drift.cfg | BFS with -C, 10.5 min: 3,538,275 distinct | KVSessDeterminism, KVReplyMatchesApply, KVExactlyOnce violated (Bugs 5, 6) |
| S10 | MC_hunt_S10_kv_drift_det.cfg | BFS: 11,609,841 distinct | KVDeterminism violated, 14 states (Bug 4) |
| S10 | MC_hunt_S10_kv_drift_nolocal.cfg | BFS: 6,284,891 distinct | KVDeterminism violated, 13 states (Bug 3) |
| S11 czar | MC_hunt_S11_czar.cfg | BFS: 745 distinct when stopped | CzNoPanic violated, 7 states (Bug 11) |
| S11 | MC_hunt_S11_czar_nopanic.cfg | BFS **exhausted**: 37,365,110 distinct, depth 67 | No violation: CzTypeOK and CzAtMostOneCzarBelief (at most one live member believes it is czar with an unexpired lease) hold exhaustively |
| S6 liveness | MC_hunt_S6_leader_live.cfg | BFS: 85,708 distinct when stopped | EventuallyLeaderWithHealthyMajority violated, 7-state lasso (Bug 8) |
| S6 liveness | MC_hunt_S6_leader_live_buf2.cfg (SendBufCap 2) | BFS 15 min + 22 min: 159,641 / 880,955 distinct, max depth 9 / 12, not finished | Inconclusive: MaxQueue = SendBufCap makes the blocked state unreachable (bound moved); see Bug 8 |
| S6 liveness | MC_hunt_S6_unblock_live.cfg | BFS: 54,651 distinct when stopped | SenderEventuallyUnblocked violated, 7-state lasso (Bug 8; predicted by the brief) |
| S7 liveness | MC_hunt_S7_connectivity_live.cfg | BFS 25 min: 359,198 distinct, max depth 10, not finished | Inconclusive (BFS not finished; no violation in the explored part) |
| S7 liveness | MC_hunt_S7_blackhole_live.cfg | BFS 25 min: 300,321 distinct, max depth 10, not finished | Inconclusive (BFS not finished; no violation in the explored part) |
| S9 liveness | MC_hunt_S9_par_live.cfg | BFS 25 min: 439,874 distinct, max depth 9, not finished | Inconclusive (BFS not finished; no violation in the explored part) |
| S3 liveness | MC_hunt_S3_catchup_live.cfg | BFS 25 min: 307,308 distinct, max depth 9, not finished | Inconclusive (BFS not finished; no violation in the explored part) |
| S4 liveness | MC_hunt_S4_stepdown_live.cfg | BFS 25 min: 61,690 distinct, max depth 8, not finished | Inconclusive (BFS not finished; no violation in the explored part) |
| S8 liveness | MC_hunt_S8_snapshot_live.cfg | BFS 25 min: 121,585 distinct, max depth 10, not finished | Inconclusive (BFS not finished; no violation in the explored part) |
| S11 liveness | MC_hunt_S11_czar_live.cfg | BFS: 121,893 distinct when stopped | EventuallyCzar violated, 17-state lasso (Bug 16) |
| S6 liveness | MC_hunt_S6_leader_live_buf2q3.cfg (SendBufCap 2, MaxQueue 3) | BFS 18 min: 797,899 distinct, max depth 12, not finished | Inconclusive (the ~10-step lasso was not reached); see Bug 8 |
| S11 liveness | MC_hunt_S11_czar_live_restart1.cfg (MaxCzRestart 1) | BFS: 229,531 distinct when stopped | EventuallyCzar violated, the same 17-state lasso (Bug 16 bound recheck) |
| Probe (storage) | MC.tla + UnfencedAckLoss, tiny bounds (before / after walGone fix) | BFS: 21,015 distinct (before); 958,571 distinct, max depth 19 (after, 5 min) | Before: 10-state unfenced WAL loss (spec gap, Case B). After the fix: no violation (see spec changes) |

---

## Spec changes during bug hunting

- **[fix-inv] AckedStays (Case A).**
  - Trigger: the first S9 BFS (spec/output/hunt_S9_par_BFS_r1_ackedstays_caseA.out, 10 states) violated it only through a storage fault. A lost last PAR identifier left the node with an empty identified log, but the node was correctly fenced (`par = "unknown"`).
  - Change: the invariant now requires `par[n] = "ok"`, since under storage faults the code promises the PAR fence, not stable storage. It still catches Bug 2.
- **[fix] Restart / CrashDiskFault("wal") (Case B), the only change to the system model during hunting.**
  - Trigger: a targeted probe (spec/output/probe_S9_walloss_unfenced_prefix_BFS.out) reached a follower that restarted unfenced after losing its WAL and `.parlog`.
  - Why that was wrong: the code fences such a node whenever `RaftState` exists (`openPARWAL`, `tube/par.go:2016-2034`; `scanParWAL`, `tube/par.go:1390-1410`; `parInventoryUnknown`, `tube/par.go:2012`), independent of the commit index.
  - Change: a durable flag `walGone` (set by the fault, cleared by the PAR snapshot install that recreates the files, `tube/tube.go:17792-17797`).
  - Re-checks: all 11 traces re-validated; the probe no longer reaches the state (spec/output/probe_S9_walloss_unfenced_postfix_BFS.out); S9, the only safety config with storage faults, was re-run (BFS and SIM). No reported counterexample uses that fault. So **storage-fault loss of a WAL is not a finding**: the PAR fence holds in the code.
- **[fix-inv] LcriSound (Case A, for the witness found).**
  - Trigger: the S3_diag SIM counterexample (128 states). A stale AE whose TermsRLE covers only index 1 was answered after a snapshot install with base 4, and `Extends` reported LCRI 4.
  - Why it is harmless: entries at or below the follower's compaction base are committed, so every current leader holds them; neither MatchIndex nor CommitIndex moved.
  - Change: `LcriSoundExceptBase` allows LCRI up to the follower's base. It still flags D-6 proper; in 8.3M SIM states it found nothing.
- **[analysis] MC.tla additions (no change to the system model).**
  - `CommittedOnQuorum`: measures the impact of Bugs 1 and 2 (cfgs without reconfiguration).
  - Filtered diagnostics that keep exploring a config after its first counterexample:
    - `NoCollateralTeardownExceptD38` (after Bug 9)
    - `QuorumCountsDistinctVotersExceptShadowLease` (after Bug 13)
    - `QuorumCountsNoDuplicateReplica` (after Bug 14)
  - Liveness premise checks: `NeverFencedWhileHealthy`, `NeverDeafLeader`, `NeverLaggingWhileHealthy`, `NeverPairDisconnected`, `NeverBlackHole`, `NeverBlockedWithHealthyMajority`. None was needed: no liveness BFS both finished and passed.
- **Derived cfgs.** All are in spec/, each with a header comment.
  - These disable or replace a property after its counterexample so the rest of the config keeps being explored:
    - MC_hunt_S1_term_durability_dataloss
    - MC_hunt_S2_matchindex_dataloss
    - MC_hunt_S2_matchindex_quorum
    - MC_hunt_S3_extends_compaction_noacked
    - MC_hunt_S3_diag_nobase
    - MC_hunt_S4_quorum_identity_nopanic
    - MC_hunt_S4_diag_noshadow
    - MC_hunt_S4_diag_nodup
    - MC_hunt_S5_reconfig_nopanic
    - MC_hunt_S5_reconfig_nopanic_noacked
    - MC_hunt_S6_blocking_nostall
    - MC_hunt_S7_diag_nod38
    - MC_hunt_S8_snapshot_noacked
    - MC_hunt_S10_kv_nosess
    - MC_hunt_S10_kv_nosess_noreply
    - MC_hunt_S10_kv_det
    - MC_hunt_S10_kv_lease
    - MC_hunt_S10_kv_drift_det
    - MC_hunt_S10_kv_drift_nolocal
    - MC_hunt_S11_czar_nopanic
  - Liveness bound rechecks, each raising one bound:
    - MC_hunt_S6_leader_live_buf2
    - MC_hunt_S6_leader_live_buf2q3
    - MC_hunt_S11_czar_live_restart1
- **Counterexample presentation.** TLC trace-exploration modules written into spec/ were moved to spec/output/ttrace/. The `-C` logs for S10_kv_drift and S2_diag were condensed to the first counterexample(s) plus a classification; the originals were 7.3 GB and 25 MB.
