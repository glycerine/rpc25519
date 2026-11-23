# Specular TODO — open findings from part 2

Distilled from `specular-part2-confirmed-bugs.md` (95 entries; 2.8 MB, mostly tool
transcripts). This keeps only findings **not already fixed** by the first round
(`fixed-specular-bug-report.md`, Bugs 1–16, commits `ad569420`…`22e2bef4`).

## Dropped

- **Entries 1–16 (MC-1…MC-16):** these are Bugs 1–16, all fixed.
- **Entry 81, CR-D-82: false positive.** The 10-minute clock-skew fence is intended.
  Optional: document the NTP requirement and rate-limit the per-fragment log flood.
- **Entry 90, CR-D-91: false positive.** The "terms should be monotone up" panic was
  removed by Bug 1 (`ad569420`).

## How to read

- **St:** R = reproduced; M = masked (the defect is real, but another bug or setting
  currently hides its consequence).
- **Commit tested:** entries 80, 83, 85, 87–89 and 92–94 were confirmed at `e36dfdac`,
  after the Bug 1–16 fixes but before the Bug 8 rework. All others were confirmed at the
  pre-fix base `a66e9b9c`, or the report names no commit for them.
- **Overlap** notes say how far a round-1 fix already reaches.
- Priority is my own. The report gives no severities.

---

## 1. Safety: consensus and replicated state

| Entry | ID | St | Problem | Fix direction |
|---|---|---|---|---|
| 18, 93 | CR-D-6, CR-3 | R FIXED | `TermsRLE.Extends` assumes agreement up to the leader's compaction base. When that base lies inside a follower's divergent tail, the LCRI is overstated. `handleAppendEntries` raises `CommitIndex` from it *before* the term and prev-index checks, so the follower applies never-committed entries. **New consequence of Bug 3's design (seen by CR-3):** if the follower created those entries as leader, the apply completes the original `waiter` ticket and `Write()` reports success. | Never infer agreement over (followerBase, leaderBase]: return the follower base or request a snapshot. Raise `CommitIndex` only after the term and prev checks, to `min(leaderCommit, last verified index)`. Never complete a `waiter` from a follower-side apply. |
| 23 | CR-D-11 | R FIXED | The "overwriteEntries would kill a committed entry" guards only run when `NoLogCompaction` is set, i.e. never in production. Applied entries are overwritten silently and `LastApplied` isn't rolled back, so the state machine stays wrong for good. Guard G2 can never fire. | Ungate the guards. Fail-stop, or reject and force a snapshot install. Never overwrite at or below `LastApplied`. |
| 21 | CR-D-9 | R FIXED | In disk mode, `overwriteEntries` sets `s.llt = 0` after a snapshot install, so the next append panics in `saveRaftLogEntryDoFsync` and the follower crash-loops. memwal hides it from tests. | `s.llt = s.logIndex.lastTerm()` in both WALs. Add a disk-mode regression test. |
| 22 | CR-D-10 | R FIXED | `getTermIndexStartingAt(BaseC)` is off by one: it never advances `nextIdx`. The compacted branch of `logsAreMismatched` then rejects correct AppendEntries. | Advance `nextIdx` after yielding the compact term. Add a unit test. |
| 19 | CR-D-7 | R FIXED | In disk mode below the compaction threshold, `logsAreMismatched` rejects instead of truncating at a conflict just past the LCRI. The leader resends the identical AE forever, so an idle cluster never repairs the follower. | Stop rejecting once the prev term matches, and let the overwrite branch truncate. Don't resend an identical AE after an identical reject. |
| 20 | CR-D-8 | M FIXED | `TermsRLE.Truncate` overwrites `CompactTerm` (and `CompactionDiscardedLast.Term`) when `keepIndex > BaseC`. | Leave `CompactTerm` alone in those arms. Strengthen `assertCompactOK`. |
| 24 | CR-D-12 | M FIXED | `if largestCommonRaftIndexTerm > 0` (tube.go:8053) tests itself, so `CommitIndexEntryTerm` is stored as 0 and spreads to saved state, the PAR checkpoint, Inspect and AEs. | Test `largestCommonRaftIndex > 0`. Handle a `-1` from `getTermForIndex`. |
| 32 | CR-D-25 | R FIXED | An empty-config self-add (`bootstrappedOrForcedMembership`) calls `becomeLeader()` with no election and no term bump, which can give two leaders in one term. | Take a fresh, persisted term and self-vote first. Refuse the self-add when a live cluster is visible. |
| 31 | CR-D-24 | R FIXED | `InstallEmptyMC` (`tuberm -e`) has no role, term or sender check. On the leader, the empty config outranks every follower's, and the leader keeps leading and commits writes alone. **Overlap:** Bug 14 stops `leaderAdvanceCommitIndex` counting a non-member self, but `replicateBatch` still takes its local-commit path whenever `clusterSz > 1` is false, including size 0. | Make the handler role-aware (refuse on, or demote, a leader). Allow local commit only when there is exactly one member and it is self. |
| 30 | CR-D-23 | R FIXED | Forced changes (`-f`) bump `ConfigVersion` in place on whichever node receives them, followers included, so two member sets can share one (ConfigTerm, ConfigVersion). Everything compares only the version. | Forward forced tickets to a live leader. Include the member set (or a hash of it) in config identity. |
| 80 | CR-D-81 | R FIXED | Peer identity is self-asserted (`FromPeerName`, and all nodes share one TLS key), so two processes started with the same name can both vote and elect two leaders in one term. | Persist a per-replica ID and record name→ID in the config; reject mismatches. Use per-node certificates bound to the name. |
| 25 | CR-D-13 | M FIXED | A *leader* that receives an AppendEntries carrying its own term. Correct Raft never does this: Election Safety allows one leader per term, so it can only happen after another bug has already produced a second same-term leader (e.g. CR-D-25, CR-D-81; Bug 12 was another, now fixed). That's why it is masked. The `ae.LeaderTerm == CurrentTerm` branch of `handleAppendEntries` (tube.go ~8255) is right for a *candidate* (Raft §3.4: a candidate that hears from the winner becomes a follower). For a leader, though, it only flips `role = FOLLOWER` and skips `becomeFollower`: the pending batch survives and is broadcast later, and the step-down cleanup added for Bugs 7 and 10 (`mcAwaitingCommitTkt`, `failStalledMembershipTickets`) is skipped. | Primary: when the receiver is a leader, treat this as an Election Safety violation and log or alarm loudly rather than stepping down silently; keep the plain role flip for candidates only. Defense in depth: on a leader, also run `becomeFollower`'s cleanup, and have `replicateBatch` bounce when not leader. |
| 27 | CR-D-18 | R FIXED | Follower stickiness (the vote refusal that read leases rely on) is volatile, so a restarted follower votes at once while the leader still counts its pre-restart ack for its read lease. **Overlap:** Bug 15 fixed the old-incarnation counting half. The volatile-stickiness half remains. | After boot, refuse votes and pre-votes until boot time + MinElectionTimeout + drift, using a persisted timeout. |
| 26 | CR-D-16 | R FIXED | §6.2 step-down quorum. **Overlap:** Bug 13 filters non-voter pongs when they are added, and Bug 15 keeps one pong per name. Remaining: the voter filter isn't applied when the check runs, so a pong from a peer removed by a later membership change still counts. The check also uses `quorum()-1` even when the leader isn't a voter, and is skipped when the leader is not in the config. | Filter by current voters at check time. Use `quorum()` when self isn't a voter. Don't skip the check when `notInMC`. |
| 56 | CR-D-53 | R FIXED | The lease decisions inside the replicated apply (`kvstoreWrite`, `validLease`, `doDeleteKey`) use each node's own `ClockDriftBound` config, so mixed values diverge state. Bug 3 removed only the mutation channel. | Stamp the bound into the log entry in `prepOne` (falling back to a constant for old entries). Check for uniformity among peers. |
| 57 | CR-D-56 | R FIXED | An operation is applied before its session is looked up. If the session vanished, the client hears "unknown SessionID" for an applied operation, and its retry applies it twice. **Overlap:** Bug 5 dedups by serial before applying; this missing-session case is separate. | Check that the session exists before `switch tkt.Op`; if it's gone, skip deterministically. All session removals must come from the log (remove the leader-only timeout delete). |
| 53 | CR-D-48 | R FIXED | `SessionTableEntry.Clone` drops `SessionEndxTm`. Snapshot install and restart don't rebuild `sessByExpiry` or `sessTableByClientName`, and `updateSessTableByClientName` then deletes live sessions, so session tables diverge. | Copy the field. Rebuild the session indexes after install and after restart. |
| 61 | CR-D-60 | R FIXED | `DELETE_TABLE` and `RENAME_TABLE` ignore leases. Deleting or renaming `hermes` wipes a live czar key, and the key returns at epoch 1, giving two czars and an epoch regression. | Apply the `DeleteKey` lease rule to every key, using log time. Keep a monotone epoch floor. |
| 59 | CR-D-58 | R FIXED | The leader's dry run for a leased write uses a zero `RaftLogEntryTm` (actions.go `expiredDur`), so taking over an expired lease is always rejected locally and never reaches the log. | `expiredDur := now.Sub(okAfter)`. |
| 17 | CR-D-4 | M FIXED | A follower's replication record is reset (MatchIndex and LCRI = 0) on every new or duplicate circuit, and stale acks overwrite LCRI downward. **Overlap:** Bug 1 changed where MatchIndex comes from; this reset and overwrite remain. | Keep the record across replacement circuits from the same incarnation. Raise LCRI only upward within a term. |

## 2. Crashes: panics reachable from remote or operator input

| Entry | ID | St | Problem | Fix direction |
|---|---|---|---|---|
| 62 | CR-D-61 | R FIXED | A range scan or ShowKeys reply larger than `UserMaxPayload` panics the leader (`respondToClientTicketApplied`). Each new leader crashes on the same retried request, so quorum is lost. (Bug 4 stopped the unbounded growth of the `dead` table, but large tables still trigger it.) | Paginate or chunk scan replies, or fail the ticket with an error; never panic. |
| 95 | CR-12 | R FIXED | No key-length check before replication. A key longer than 2048 bytes (yogadb's limit) panics at apply, and because the entry is durable it poisons every leader. | Validate key and table length at admission. At apply, turn storage validation errors into `tkt.Err`. |
| 78 | CR-D-79 | R FIXED | A foreign ClusterID, unknown FragOp, bad payload, empty `RemotePeerName`, or `NotifyClientNewLeader` arriving at a replica all panic the replica event loop (e.g. a stale `tubels` config). | Log and drop instead of panicking. |
| 79 | CR-D-80 | R FIXED | A stale client URL dialed after a leader change hits an `if true { panic }` debug check in `peerJoin` on the new leader. | Remove the panic. Dial clients by address and adopt the PeerID from the acknowledgement. |
| 40 | CR-D-34 | R FIXED | A remote circuit-start for a stale PeerID makes rpc25519 run a second `TubeNode.Start` on the same tube-client or hermes node (`LimitedServiceNames` covers only replicas). The leader then panics with "mis-directed fragment". | Limit tube-client and hermes to one instance. Make `TubeNode.Start` refuse to run twice. Don't panic in `peerJoin`. |
| 83 | CR-D-84 | R FIXED | `changeMembership` panics when the ticket's `AddPeerID` is stale for a restarted node. | The live circuit's PeerID wins; log the stale value. |
| 77 | CR-D-78 | R FIXED | `TubeNode.CAS` panics (`panicf` before its `return`) on a cancelled ctx, a closed session, or after shutdown. | Delete the two `panicf` calls so the existing error returns run. |
| 82 | CR-D-83 | R FIXED | (rpc25519) The server read loop has no `recover()`. An unknown compression byte (`magic7`) panics the whole process, and with TCP-only it needs no authentication. | Return an error from `handleDecompress`/`handleCompress`. Add a recover to the server read loop. |
| 92 | CR-D-93 | R | (rpc25519) An empty, non-nil `frag.Args` becomes a nil map in `ToMessage`, and `newCircuit` then panics writing into it. | `copyArgsMap` should return an empty map for empty input; add nil checks after `ToMessage`. |
| 84 | CR-D-85 | R | (rpc25519) The QUIC client never sets `c.cpair`, so keepalives and `PingStats` dereference nil. Separately, the keepalive's per-`writeFull` deadline can split a frame and desynchronize the stream. Bug 8's `writeFrameByDeadline` covers only `HDR.Deadline` messages, not keepalives. | Set `c.cpair`. Send keepalives through one deadline per frame, and close the connection on a partial frame. |
| 85 | CR-D-86 | R | (rpc25519) A repeated `Close()` on a QUIC client closes the shared UDP socket; a negative count panics with the mutex still locked. | Release the shared socket once per holder; remove the `< 0` panic. |

## 3. Liveness and availability: connections and circuits

| Entry | ID | St | Problem | Fix direction |
|---|---|---|---|---|
| 35 | CR-D-29 | R | A send on a dead circuit makes `Server.SendOneWayMessage` dial an auto-client synchronously on the Tube event loop, with no connect timeout, sometimes to an ephemeral port. **Not covered by the Bug 8 rework**, which bounds only the hand-off and write. | Circuit sends must never auto-dial (`forbidAutoCli=true`, or fail once the circuit is halted). Cancel circuit contexts before `deletePair`. |
| 36 | CR-D-30 | R | The watchdog's periodic liveness and reconnect branch is dead code (`pack` is never assigned), so silent or half-open replica circuits stay forever and block every rebuild path. | Assign `pack`; close circuits silent for longer than `followerDownDur`; retry pending entries. |
| 41 | CR-D-35 | R | Follower-to-follower circuits are never re-dialed after one failed attempt (absent-only dialing, a one-shot watchdog, re-pulsing only by a leader), so a connected majority can stay leaderless. | Pending entries retry with backoff, or followers re-pulse pending peers on every election timeout. |
| 42 | CR-D-36 | R | At an unchanged config version, followers never adopt a restarted peer's new address (`aeMemberConfigHelper` and `setMC` return early). | Copy changed address details at equal version (with a freshness marker); redial when a pending entry's address is stale. |
| 44 | CR-D-39 | R | `addToCktall` keeps the newest circuit, chosen independently on each side, and the 20 s audit GC closes the other one, so the circuits between two healthy replicas can black-hole in both directions. | Pick deterministically (lowest circuit ID); never GC replica-to-replica circuits for being unchosen. |
| 46 | CR-D-41 | R | (rpc25519) The `PreferExtant` hand-off can lose its race against `callCtx`, returning `err == nil` with an orphaned circuit that the remote treats as current; the watchdog marks the peer up. | Every path that doesn't hand off must set `err` and close the circuit. |
| 47 | CR-D-42 | R | (rpc25519) An auto-client that dies before its cleanup hook is installed stays in `remote2pair` as a tombstone, so its address is never re-dialed; `deletePair` can also reorder a circuit's messages. | Remove the entry on both early returns; compare-and-delete in `deletePair`; serialize auto-client creation per destination. |
| 43 | CR-D-37 | R | `Circuit.Close(reason)` is retyped to `CallPeerError`, which Tube ignores, so the remote end stays open. **Overlap:** Bug 9 fixed the half where the whole connection was torn down. | Keep the `CallPeerEndCircuit` type and carry the reason in `JobErrs`; stop ignoring `cktHasError` in Tube. |
| 48 | CR-D-43 | R | `wasConnRefused` deletes `cktall[RemotePeerID]` without checking that it is the circuit that failed, which can remove a healthy replacement. | Only delete when the CircuitIDs match; close the failing circuit itself. |
| 39 | CR-D-33 | R | (rpc25519) `bootstrapCircuit` holds the node-wide `chanmut` lock while sending `CircuitEstablishedAck`. **Overlap with Bug 8:** on tube's buffered connections `-1` no longer blocks, but under a full queue the ack is now *refused* (`ErrSendQueueFull`), failing circuit setup. Default unbuffered users still block. | Release the lock before any network send; send the ack in `-2` mode with background queueing. |
| 91 | CR-D-92 | R | (rpc25519) `Circuit.Close` blocks on the per-connection `cktServedDel` channel (100 slots) once that connection's send loop is gone, and can stall the Raft event loop through the audit GC. | Add a send-loop-gone signal to the select; post the removal once; keep blocking closes off the event loop. |
| 50 | CR-D-45 | R | Closing a non-replica circuit always waits the full 10 ms on the event loop (no watchdog marks it done). A prune of hundreds of circuits stalls heartbeats long enough to cause an election. | Skip the wait when no watchdog was started; bound or offload the prune. |
| 38 | CR-D-32 | M | (rpc25519) Receive path: the pump drops frags and tears down a *live* circuit after 2 s of a full `ckt.Reads`; new circuits reset that timer; the read loop drops silently every 5 s; untimed waits elsewhere. | Close only on evidence a circuit is dead; otherwise apply back-pressure or report the error. |
| 37 | CR-D-31 | M | (rpc25519) The client read loop keeps reading after framing, decrypt or sticky TLS/QUIC errors, unlike the server, and spins or reads mid-frame. | Return on all non-timeout errors, like the server; check total frame length on the send side. |

## 4. Liveness: membership, tickets and PAR

| Entry | ID | St | Problem | Fix direction |
|---|---|---|---|---|
| 29 | CR-D-22 | R | A member that reconnects but never acks (`cktPlus.MC == nil`) stalls every membership change forever. The early return skips quorum, the 10 s timeout and `-f`. | Treat it as a stale member (not counted) and let Q1/Q2 decide; enforce the ticket deadline on every re-stall. |
| 28 | CR-D-21 | M | A membership change that timed out (client told it failed) leaves copies in the `stalledOnSeenTkt` queues, which are re-run later. **Overlap:** Bug 10 clears the queues only on step-down. | On every error reply, finish the ticket and remove all copies; don't queue duplicates. |
| 58 | CR-D-57 | R | Tickets with `LogIndex == 0` are invisible to `imap.all()`, so `resubmitStalledTickets`, `becomeLeader`'s takeover of `WaitingAtFollow`, and `tellClientsImNotLeader` miss them, and callers hang. | Add `imap.allIncludingUnindexed()` and use it at those sites. |
| 60 | CR-D-59 | R | Tickets leak and clients hang: failed redirects are neither finished nor queued, a leader's own local tickets are dropped on step-down and batch clear, and waiting tickets never expire. | Finish or re-queue on every failure path; re-forward or fail follower tickets when the leader changes; add expiry. |
| 33 | CR-D-26 | R | A node restarted with `-zap` stamps its empty config with the current term, so it outranks the live config and the node never re-adopts the real member set. | A zapped config should be (0,0); never replace a populated set with an empty one learned from a peer. |
| 54, 94 | CR-D-49, CR-9 | R | A crash between a WAL append and its PAR identifier publication leaves an unidentified tail. The node is fenced `unknown` (no vote, no campaign) with no donor, and a single node or a correlated crash loses availability. | Rebuild identifiers locally for a checksum-valid, chained tail above every protected floor (`parInterrupted`); give fenced nodes a way out. |
| 55 | CR-D-50 | R | A lagging follower that restarts below the `MC.CommitIndex` it copied from a newer leader is fenced as PAR `unknown` with no disk fault. | Base the fence on a locally earned witness, `min(MC.CommitIndex, highest index durably appended here)`. |
| 52 | CR-D-47 | M | Interleaved multipart snapshots (two circuits) keep discarding each other in the single reassembly buffer. **Related to Bug 2:** snapshots are still sent per ack circuit. | Keep a buffer per (circuit, checksum) or per transfer ID; allow one snapshot in flight per follower. |
| 34 | CR-D-27 | M | The P1 prefix-quorum reconfiguration guard is a no-op whenever `s.par == nil` (all NoDisk tests), so the default suite never exercises production reconfiguration admission. | Make P1 unconditional; run the PAR membership tests in the default suite. |
| 51 | CR-D-46 | M | Membership observers are never notified (`isPending()` is always true for non-replicas), and idle observer circuits are closed after 60 s. | Select recipients by open circuit; keep observers alive or document a heartbeat requirement. |

## 5. Resource leaks and data races

| Entry | ID | St | Problem | Fix direction |
|---|---|---|---|---|
| 49 | CR-D-44 | R | Parked `cktPlus` entries and pending watchdogs leak without bound under circuit churn. | Remove from `parked` in `deleteFromCktAll`; clean up a replaced pending entry in `addToCktall`. |
| 86 | CR-D-87 | R | (rpc25519) idem halter children are never removed, for connections, auto-client dials and circuits. | `RemoveChild` at each teardown; fix idem's `RemoveChild` to clear both lists. |
| 87 | CR-D-88 | R | (rpc25519) A rejected or failed circuit setup leaks the registered circuit and its goroutine. | Close it on both failure paths, or register only after success. |
| 88 | CR-D-89 | R | (rpc25519) Data races on `ckt.loopy` and `ckt.RemotePeerID`/`RemotePeerName` after the circuit is handed to the pump. | Make `loopy` an `atomic.Pointer`; freeze identity before registration. |
| 45 | CR-D-40 | R | (rpc25519) Close-notification registration looks up the connection by exact address string, so hostnames, aliases, NAT or reformatted IPv6 leave zombie circuits. | Return and use the connection actually used for sending; normalize address keys. |
| 89 | CR-D-90 | M | A shadowed `netAddr` in `connectInBackgroundIfNoCircuitTo` throws away the corrected reconnect address, and the auto-client reuse lookup never matches (random name suffix). | Assign with `=`; match on the name prefix; add the `go vet` shadow check. |

## 6. Czar and hermes

| Entry | ID | St | Problem | Fix direction |
|---|---|---|---|---|
| 63 | CR-D-64 | R | The czar panics if its lease expires between `handlePing`'s lease check and `setVers`. | Handle `ErrExpiredCzarLease` like the other callers; pass `now` into `setVers`. |
| 64 | CR-D-65 | R | `expireSilentNodes` ignores its `fromList` argument and drops a newcomer handled in the same call; repeated, the member side panics. | `newlist = fromList.Clone()`; don't panic on the member side. |
| 65 | CR-D-66 | R | A member whose first Ping reply is rejected locally wedges forever (a stale `rpcClientToCzar`, no timer armed). | Do the normal NotCzar cleanup before `continue fullRestart`, at both sites. |
| 66 | CR-D-67 | R | `setVers` resets `t0` on every membership change, so dead members inherited from the czar-key snapshot are never expired under churn. | Reset `t0` only when the czar epoch changes. |
| 67 | CR-D-68 | M | The member lease (6 s) can outlive the czar lease (5 s), so a member dropped by a new czar can serve stale hermes reads. | Cap member leases at `CzarLeaseUntilTm - drift`. |
| 68 | CR-D-69 | R | `HelperFindLeader` returns the last leader in random map order, not the highest-term one; the czar `panicf`s when it disagrees with the client. | Return the highest-term choice; don't panic on disagreement. |
| 69 | CR-D-70 | R | A member that can't reach the Tube cluster panics (with no `InitialLeaderName`) or hot-loops without delay. | Handle the error; add a halt-aware jittered backoff. |
| 74 | CR-D-75 | R | The member's heartbeat `Czar.Ping` uses a nil context, so the 20 s default timeout exceeds both lease durations. | Bound it with a ctx, e.g. `2*memberHeartBeatDur`, cancelled on halt. |
| 76 | CR-D-77 | R | The member's czar ping client dials with no connect timeout (`ConnectTimeout` 0; `runClientTCP` ignores it) and blocks about 135 s against a dead host. | Set a connect timeout below the lease; use `StartWithCtx`; honor the timeout in rpc25519. |
| 75 | CR-D-76 | R | A czar that reaches Tube but not its members renews forever and can never be deposed. | Record member-unreachable evidence in Tube; the czar steps down on it. |
| 70 | CR-D-71 | R | Multi-machine hermes advertises the internal Tube client's PeerIDs and URLs and binds to 127.0.0.1, so writes never complete and remote Tube clients panic. | Publish the hermes peer's own ID and URL; bind to the external host. |
| 71 | CR-D-72 | R | Hermes peer circuits are never pruned or re-dialed, so one transient link drop loses traffic to that peer permanently. | Prune on goroutine exit (compare-and-delete); re-dial dead circuits of current members. |
| 72 | CR-D-73 | R | Data race on hermes `operLeaseUntilTm` (a torn `time.Time` read in the lease gate). | Check the lease on the event loop only, or store it atomically. |
| 73 | CR-D-74 | R | A lost VALIDATE (or a lost RMW ACK) blocks a hermes follower read forever. | Replay after the mlt expires even when the writer is live; retransmit RMW INVs. |

---

## Suggested order

1. **Section 1 safety items**, starting with the CR-D-6/CR-3 Extends and early-commit
   cluster: it shares its root cause with CR-D-11 and CR-D-7 and touches the waiter
   path from Bug 3. Then CR-D-9, CR-D-10, CR-D-25, CR-D-24, CR-D-23, CR-D-53,
   CR-D-56, CR-D-48 and CR-D-60.
2. **Section 2 crash items.** Most are one-line `panic`-to-error changes; CR-D-61 and
   CR-12 are cluster-wide poison pills.
3. **Sections 3 and 4.** The availability items that need design: CR-D-29 (dialing on
   the event loop), CR-D-30, CR-D-35 and CR-D-39 (circuit lifecycle), and CR-D-49/CR-9
   (PAR fencing).
4. **Sections 5 and 6** (leaks, races, czar and hermes).
