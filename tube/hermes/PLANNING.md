# Hermes implementation planning

This note summarizes the current state of the Tube Hermes implementation and
spells out the protocol transitions we should implement. It is based on:

- `hermes/original-reference/Hermes/tla/Hermes.tla`
- `hermes/original-reference/Hermes/tla/HermesRMWs.tla`
- `hermes/original-reference/Hermes/tla/protocol-actions.png`
- `hermes/original-reference/Hermes/src/hermes/hermesKV.c`
- `hermes/original-reference/Hermes/src/hermes/hermes_worker.c`
- the checked Ivy model in `hermes/hermes.ivy`
- the current Go implementation in `hermes/` and `cmd/hermes/`

The TLA specs are the primary safety reference. The original C code is useful
for the production optimizations and for details the TLA does not model, such
as client request completion and operation buffers. The PNG is a compact
cheat-sheet, but it appears to contain or hide several important edge cases.
The Ivy model is now the local proof artifact for the corrected protocol shape.
It is intentionally one-key, but the implementation can apply it independently
per key because Hermes composes across keys.

## Current Go state

The implementation currently lives almost entirely in `hermes/hermes.go`.

Implemented pieces:

- Per-key state machine with `sValid`, `sInvalid`, `sWrite`, `sReplay`, and
  `sInvalidWR`.
- In-memory `map[Key]*KeyMeta` store.
- `INV`, `ACK`, and `VALIDATE` messages with key, value, timestamp, epoch, RMW
  flag, and ticket id.
- Local `Read` and `Write` API using `HermesTicket` and a single event loop in
  `HermesNode.Start`.
- Buffered pending operations in `timeoutPQ`, indexed by ticket id and key.
- Broadcast invalidations and validations.
- ACK sending, with the O3 broadcast-ACK optimization enabled globally by
  `useBcastAckOptimization`.
- Basic message epoch filtering through `EpochV.Equal`.
- Basic tests for single-node operation, normal writes, reads waiting behind
  writes, lost `VALIDATE`, and lost `ACK`.

Half-finished or missing pieces:

- There is no public RMW API. `readModifyWriteReq` is commented out and
  `replayRMW` panics.
- Local `Read` and `Write` requests do not currently reject work when the
  operating membership lease is expired. Incoming network messages are checked,
  but local requests must be checked too.
- `cmd/hermes/main.go` wires Tube reliable membership into Hermes, but then the
  main loop also reads from `mem.UpcallMembershipChangeCh`, competing with the
  Hermes node for the same upcalls.
- `cmd/hermes/main.go` does not yet use membership updates to form Hermes peer
  circuits.
- Failure handling is currently timer-driven. The protocol needs timers to
  suspect lost messages, but safety depends on Tube/RM membership changes and
  lease expiration before completing operations without the failed member.
- The RMW timestamp increment in Go is reversed relative to `HermesRMWs.tla`:
  RMW should advance by 1, and writes should advance by 2 when RMW is enabled.
  Current `actionW` advances writes by 1 and RMWs by 2.
- RMW invalidation handling currently ACKs and returns for `compare >= 0`,
  which means it does not apply a greater incoming RMW invalidation. The TLA
  requires applying greater RMW invalidations and only treating equal RMW
  invalidations as ACK-only.
- Stale RMW invalidations should be answered by sending this node's local
  invalidation back, not by ACKing. This is modeled in `HermesRMWs.tla` and in
  the C `ST_OP_INV_ABORT` path.
- `sInvalidWR` completion is too eager in some paths. In particular, a pending
  lower timestamp replay/read should be retried or kept blocked after it
  finishes in `sInvalidWR`; it must not return the higher timestamp value until
  that higher value is actually valid.
- `recvValidate` currently completes writers in `sInvalidWR` immediately on a
  higher validation. The C implementation allows the key to become valid while
  an older local operation is still waiting for its own ACK completion.
- The code relies on `tkt.TS` as the local write timestamp. The TLA has an
  explicit `nodeLastWriteTS`, separate from the key's current `nodeTS`. That
  distinction matters after a local write is overwritten by a higher
  invalidation.
- Tests fake the operating lease and do not exercise real membership changes,
  peer discovery, RMW, or multiple concurrent writers at high contention.

## Protocol model

Each key stores:

- `TS`: the key's current logical timestamp, ordered by `(version, tie-breaker)`.
- `Val`: the value associated with `TS`.
- `State`: one of the Hermes states below.
- `LastWriterID`: the node currently responsible for completing the write or
  replay for `TS`. This is the message sender, not necessarily `TS.CoordID`.
- `IsRMW`: whether the current `TS` belongs to an RMW.

Each local pending update also needs:

- `LocalWriteTS`: the timestamp of the write or replay this node initiated.
  This must remain available even if `KeyMeta.TS` later advances to a higher
  timestamp.
- An ACK set for the current membership epoch.
- The original client ticket, if any.
- The operation kind: read, write, RMW, or replay.

States:

- `sValid`: local value is readable.
- `sInvalid`: local value has been invalidated by an update that is not yet
  known valid.
- `sWrite`: this node is coordinating a local write or RMW.
- `sReplay`: this node is replaying an already-invalidated update to unblock
  the key.
- `sInvalidWR`: transient. This node had a local write or replay in progress,
  but it has also accepted a higher timestamp invalidation for the same key.
  The older local operation may still need enough ACKs to complete its client
  request, but this node must not broadcast `VALIDATE` for it.

Messages:

- `INV`: invalidation carrying key, value, timestamp, epoch, sender,
  ticket/op id, and RMW flag.
- `ACK`: acknowledgement for a specific invalidation timestamp and epoch.
- `VALIDATE`: notification that an update/replay gathered all required ACKs and
  the timestamp is now readable.

All network message handling must be idempotent and tolerate duplicates and
reordering. Messages with a stale or future membership epoch are dropped.

## Checked Ivy model and implementation contract

`hermes/hermes.ivy` verifies the single-key protocol with Ivy 1.7:

```bash
ivy_check hermes/hermes.ivy
```

The current checked result is `OK`. The model proves:

- Safety: any two live replicas in `hs_valid` have the same timestamp and value.
- Safety: once a timestamp is completed, every live node has advanced to at
  least that timestamp.
- RMW safety: at most one RMW derived from the same base timestamp can
  complete.
- Liveness: if `complete_ready(n)` is attempted infinitely often for a node,
  then every ready completion epoch for that node is eventually marked done.

The liveness theorem is written in "no permanent bad suffix" form:

```ivy
forall N,E.
  (globally eventually complete_try(N))
  -> globally ~(ready(N) & ready_epoch(N) = E & globally ~epoch_done(E))
```

Operationally, this means implementation progress must be split into two
observable phases:

1. `mark_ready`: after all live members in the current membership snapshot have
   ACKed a pending operation, record that the operation is ready to complete.
2. `complete_ready`: in a fair event-loop step, consume that ready operation and
   perform either current-timestamp completion or overwritten-timestamp
   completion.

The proof assumes the implementation keeps trying to complete ready work. In Go
terms, the Hermes event loop should call a `completeReady`/`drainReady` helper
after every ACK, membership change, replay start, validation, timeout wakeup,
and before blocking on the next select. A ready ticket must not depend on an
external client retry to finish.

The proof also makes these implementation obligations explicit:

- Local pending metadata is separate from key metadata. A pending operation has
  its own `pending_ts`, `pending_rmw`, ACK set, and ready bit even if the key's
  current `TS` advances to a higher timestamp.
- A ready bit is a certificate for one ACK set and one membership generation.
  If a later ACK or reconfiguration mutates the ACK set, clear/recompute the
  ready bit rather than treating the previous ready certificate as still live.
- `complete_current` validates only when `pending_ts == key.TS` and the key is
  in `sWrite` or `sReplay`.
- `complete_overwritten` completes only the older local operation when
  `pending_ts < key.TS`. It marks that lower timestamp completed for the local
  client, clears pending metadata, and must not broadcast `VALIDATE`.
- A `VALIDATE` for the current key timestamp may make the key valid while an
  older overwritten local pending operation remains present. That older pending
  operation still completes only through its own ACK/ready path or through
  membership change.
- Replays after failure are permitted only when the key is `sInvalid`, there is
  no local pending operation for the key, and `LastWriterID` is no longer live
  in the current membership.
- The O3 broadcast-ACK optimization and follower pseudo-tickets are not modeled
  by the proof. The base protocol to implement first is `INV -> ACK ->
  VALIDATE`; O3 should stay disabled until we either prove or heavily test the
  optimized path separately.

## Corrected rules from the references

The following are the corrections and clarifications to use when the PNG is
ambiguous.

1. `INV TS == local.TS` should normally be ACK-only.

   `Hermes.tla` updates the key only when the incoming timestamp is greater.
   The PNG lists `A_I` for several equal-timestamp cells, but reapplying the
   value and overwriting `LastWriterID` on equal timestamps is not needed for
   safety. Equal timestamp messages are duplicates, replay echoes, or
   out-of-order messages for a timestamp already installed locally.

2. `sWrite + INV TS == local.TS` is not a hard "impossible" case.

   The PNG marks this as `X`, but duplicate/replay traffic can produce an equal
   timestamp. The safe behavior is ACK-only or drop-self-echo. It must not
   advance state or reapply a different value for the same timestamp.

3. Stale write invalidations and stale RMW invalidations differ.

   For a stale non-RMW write INV, followers still ACK. This lets writes finish
   even if they lose a race with a higher timestamp write.

   For a stale RMW INV, followers must not ACK. They respond with their local
   newer invalidation. This aborts the stale RMW coordinator.

4. RMW timestamp spacing is intentional.

   With RMW enabled, RMW uses `local.version + 1`; write uses
   `local.version + 2`. This guarantees a blind write racing with an RMW from
   the same base value gets a higher timestamp, so the RMW aborts and the write
   commits.

5. Timeout alone is not a membership change.

   A message-loss timeout should cause retransmission or replay attempts. It
   should not remove a missing node from the ACK requirement. Completing with a
   smaller ACK set is safe only after Tube/RM publishes a new membership epoch
   and the old membership lease has expired.

6. `A_RR` in the PNG should be read as "retry/resume the read when safe", not
   "return a value immediately".

   In `sInvalidWR`, a local replay may finish after a higher timestamp has
   already invalidated the key. The waiting read must not return the higher
   value until that higher timestamp becomes valid.

7. The TLA does not model all client completion details.

   For example, the base TLA can leave `invalid_write` until a higher
   validation arrives. The C implementation tracks operation buffers so an
   overwritten local write can still complete its client request without
   broadcasting `VALIDATE`. The Go implementation needs the C-style local
   operation tracking in addition to the TLA state safety.

8. ACK completion and protocol completion are separate phases.

   The Ivy model does not immediately complete a write/replay inside
   `receive_ack`. After the ACK set covers all live members, `mark_ready`
   creates a ready completion record. A later fair `complete_ready` step
   consumes it. The Go implementation can call both helpers in the same event
   loop turn, but the data model should still make the distinction explicit.

9. `sInvalidWR` is about a lower pending operation, not the higher current key.

   In the proof, `sInvalidWR` means `pending_ts < cur_ts`. Completion of that
   pending timestamp never validates the current key timestamp and never sends
   `VALIDATE`. It only completes or retries the lower local operation and then
   leaves the key `sInvalid` unless a matching higher `VALIDATE` has already
   made it `sValid`.

   If the lower pending operation was already marked ready before the higher
   invalidation arrived, keep that ready certificate so `complete_overwritten`
   can drain it. Clear a ready certificate during invalidation only when the
   pending operation itself is being aborted, such as a lower pending RMW.

10. A higher `VALIDATE` does not complete a lower overwritten operation.

   If `VALIDATE.TS == key.TS`, the key becomes valid. If a pending operation
   exists for exactly that timestamp, it can be cleared. If the pending
   operation is for an older timestamp, leave it in place and let its own
   ACK/ready path finish it.

11. Stale or conflicting RMW invalidations must not be ACKed.

   Besides the ordinary `TS < local.TS` stale case, the Ivy model has an
   explicit completed-conflict path: if a different RMW from the same parent
   timestamp has already completed, respond with the local invalidation and do
   not ACK the incoming RMW. This is how "at most one RMW per base" is
   maintained.

12. Scheduler fairness is part of the liveness contract.

   The proof assumes `complete_try(node)` happens infinitely often for a node.
   Implementation must approximate this by draining ready completions whenever
   the node is operational. A ready operation that sits in a map or queue with
   no wakeup path is outside the proved behavior.

## Transition prompts and actions

This section is the target behavior for Tube's Hermes implementation.

### Local read request

Prompt: a client calls `Read(key, waitForDur)`.

Common preconditions:

- The node must currently hold a valid operating membership lease.
- The read is evaluated against the node's current membership epoch.

By current key state:

- Missing key:
  - If the caller requested no wait, return `ErrKeyNotFound`.
  - If this is a single-replica configuration, return `ErrKeyNotFound`.
  - Otherwise create an `sInvalid` placeholder only if the caller is willing to
    wait for the value to arrive or be replayed.

- `sValid`:
  - Return the local value immediately.
  - Do not change state.

- `sInvalid`:
  - If `LastWriterID` is still in the current live membership, buffer the read
    and wait for `VALIDATE` or a later replay. An O3 broadcast-ACK completion
    path can be added later, but it is not part of the checked Ivy model.
  - If `LastWriterID` has been removed by a membership change, start a replay:
    keep the same `TS`, value, and `IsRMW`; set state to `sReplay`; initialize
    the ACK set for the current live membership; broadcast `INV`; buffer the
    read ticket until replay completion.

- `sWrite`, `sReplay`, or `sInvalidWR`:
  - Buffer the read.
  - The read completes only after the key becomes `sValid`.

### Local write request

Prompt: a client calls `Write(key, value, waitForDur)`.

Common preconditions:

- The node must currently hold a valid operating membership lease.
- The write uses a stable snapshot of the current membership epoch.
- The write creates a local pending update ticket before broadcasting `INV`.

By current key state:

- Missing key:
  - Create `KeyMeta` with version 0 and local coordinator id.
  - In a single-replica configuration, apply the value, set `sValid`, and
    complete immediately.
  - Otherwise proceed as for `sValid`.

- `sValid`:
  - Compute a new timestamp.
    - If RMW support is disabled, simple writes may advance by 1.
    - If RMW support is enabled, simple writes advance by 2.
  - Set `TS.CoordID` to this node's id.
  - Apply the new value locally.
  - Set `LastWriterID` to this node's id.
  - Set `IsRMW=false`.
  - Save `LocalWriteTS` on the pending ticket.
  - Initialize the ACK set for the current membership, with self already
    accounted for.
  - Set state to `sWrite`.
  - Broadcast `INV`.
  - Reset the message-loss timer.

- `sInvalid`:
  - This is the "writes while invalid" optimization from the PNG and C code.
  - It is safe only when the key has no local in-progress operation that still
    owns the operation buffer for this key.
  - If enabled, perform the same actions as the `sValid` write case, using the
    invalid key's current timestamp as the base.
  - If not enabled or a local op is already pending, buffer the write.

- `sWrite`, `sReplay`, or `sInvalidWR`:
  - Buffer the write until the key leaves the in-progress state.

Write completion:

- When ACKs have been received from every live member in the current epoch,
  mark the pending write ready.
- A fair completion step consumes the ready write and transitions
  `sWrite -> sValid`.
- Complete the client write successfully.
- Broadcast `VALIDATE` in the base protocol. Skipping `VALIDATE` under O3 is a
  separate optimization and is not part of the current proof.

### Local RMW request

Prompt: a future client API calls an atomic read-modify-write operation.

RMW is not currently implemented in Go, but the target protocol is:

- RMW may start only from `sValid`.
- Compute the new timestamp as `local.version + 1`.
- Set `IsRMW=true`, apply the candidate value locally, enter `sWrite`, and
  broadcast an RMW `INV`.
- If this local RMW observes a higher timestamp for the same key before it
  gathers all ACKs, abort the RMW ticket.
- If the RMW gathers all ACKs first, mark it ready. A fair completion step then
  transitions to `sValid`, completes the client RMW, and broadcasts
  `VALIDATE` in the base protocol.
- If a different RMW from the same base timestamp has already completed, abort
  this RMW and reply with local state instead of ACKing.
- After a membership reconfiguration, replaying an in-progress RMW must reset
  its ACK set before rebroadcasting. This differs from non-RMW write replay,
  which may keep already gathered ACKs when safe.

### Receiving a non-RMW write INV

Prompt: `recvInvalidate` receives `INV{IsRMW:false}` for the current epoch.

Always send an `ACK` for a non-RMW write INV, regardless of timestamp
comparison. Then apply the timestamp-specific rule:

- Incoming `TS > local.TS`:
  - Install the incoming `TS`, value, `IsRMW=false`, and `LastWriterID=INV.FromID`.
  - If the local state was `sValid` or `sInvalid`, transition to `sInvalid`.
  - If the local state was `sWrite` or `sReplay` for a non-RMW local op,
    transition to `sInvalidWR`, preserving the local pending operation's
    `LocalWriteTS` so it can still complete without broadcasting `VALIDATE`.
  - If the local pending operation is an RMW, abort it, clear its ACK/ready
    metadata, and transition to `sInvalid`.
  - If the local state was already `sInvalidWR`, remain `sInvalidWR` while
    there is still a lower local non-RMW op to complete; otherwise transition
    to `sInvalid`.
  - If the overwritten non-RMW operation had already been marked ready, keep
    that ready generation attached to the lower pending operation so
    `complete_overwritten` can finish it.
  - Buffered reads remain blocked.

- Incoming `TS == local.TS`:
  - ACK only.
  - Do not change state.
  - Do not overwrite the local value unless we deliberately add a duplicate
    recovery path that verifies the value matches the existing timestamp.

- Incoming `TS < local.TS`:
  - ACK only.
  - Do not change state.

### Receiving an RMW INV

Prompt: `recvInvalidate` receives `INV{IsRMW:true}` for the current epoch.

Timestamp-specific rules:

- Incoming `TS > local.TS`:
  - Install the incoming `TS`, value, `IsRMW=true`, and `LastWriterID=INV.FromID`.
  - Abort any lower local RMW for the key.
  - If the local state was `sValid`, `sInvalid`, or has no pending lower write
    to complete, transition to `sInvalid`.
  - If there is a lower local non-RMW write/replay that still needs to complete,
    transition to `sInvalidWR` and retain that pending ticket.
  - If the overwritten non-RMW operation had already been marked ready, keep
    that ready generation attached to the lower pending operation so
    `complete_overwritten` can finish it.
  - If the overwritten pending operation is an RMW, abort it; if it was already
    ready, retire its ready generation while clearing the pending RMW metadata.
  - Send `ACK`.

- Incoming `TS == local.TS`:
  - Send `ACK`.
  - Do not change state or value.

- Incoming `TS < local.TS`:
  - Do not send `ACK`.
  - Send a local `INV` back to `INV.FromID` using this node's current local
    timestamp, value, and RMW flag. This is the RMW abort path.

- Incoming RMW conflicts with an already completed different RMW from the same
  base timestamp:
  - Do not send `ACK`.
  - Send a local `INV` back to `INV.FromID` using this node's current local
    timestamp, value, and RMW flag.

### Receiving ACK

Prompt: `recvAck` receives `ACK` for the current epoch.

Common rules:

- The ACK only counts if its timestamp equals a local pending update's
  `LocalWriteTS`.
- The ACK should update the pending operation's ACK set, not the key's current
  timestamp directly. The key may already have advanced to a higher timestamp.
- Duplicate ACKs are ignored.
- Stale ACKs for older completed or abandoned tickets are ignored.
- If the ticket was already marked ready, clear or retire that ready generation
  before mutating its ACK set. Then recompute readiness.
- Under O3, ACKs may be broadcast to all replicas, so a node may need a
  pseudo-ticket to accumulate ACKs before the corresponding `INV` arrives.
- In the base protocol proved in Ivy, ignore O3 pseudo-tickets and count ACKs
  only for local pending operations.

When the ACK set covers every live member in the current membership:

- Check the RMW conflict guard. A pending RMW can be marked ready only if no
  different RMW from the same base timestamp has completed.
- Mark the pending operation ready with a fresh ready generation tied to the
  current membership snapshot.
- Schedule or immediately call `complete_ready`. The proof's liveness
  assumption is that this completion action is attempted fairly.

By current key state when the ACK completes the live membership set:

- `sWrite`:
  - If `LocalWriteTS == key.TS`, `complete_ready` transitions to `sValid`.
  - Complete the local write or RMW ticket.
  - Broadcast `VALIDATE` in the base protocol.
  - Under O3, skip `VALIDATE` only if every live replica can independently
    infer validity from the broadcast ACK set. This path is outside the current
    proof.

- `sReplay`:
  - If `LocalWriteTS == key.TS`, `complete_ready` transitions to `sValid`.
  - Restart or complete the blocked read that triggered replay.
  - Broadcast `VALIDATE` in the base protocol.
  - Under O3, skip `VALIDATE` under the same all-ACK visibility condition.
    This path is outside the current proof.

- `sInvalidWR`:
  - The ACK completion is for a lower timestamp local write or replay.
  - If it was a write, complete that write's client ticket successfully.
  - If it was a replay triggered by a read, restart or keep the read blocked
    against the key's current higher timestamp.
  - Do not broadcast `VALIDATE` for the lower timestamp.
  - If the key is still not known valid for the higher timestamp, transition to
    `sInvalid` after clearing the lower pending operation.
  - If a matching higher `VALIDATE` was already processed, the key may remain
    `sValid`, but the lower ticket completion must not overwrite the higher
    value or timestamp.

- `sValid` or `sInvalid`:
  - Normally ignore. This can happen for stale ACKs or because O3 collected a
    pseudo-ticket out of order.
  - In a future O3 variant, if pseudo-ticket collection proves that the current
    timestamp has all live ACKs and the value is installed locally, transition
    `sInvalid -> sValid` and unblock reads. This is outside the current proof.

### Receiving VALIDATE

Prompt: `recvValidate` receives `VALIDATE` for the current epoch.

Rules:

- If `VALIDATE.TS != local.TS`, ignore it.
- If `VALIDATE.TS == local.TS`, set the key state to `sValid`.
- Unblock reads waiting on this timestamp.
- If there is a pending operation for exactly `VALIDATE.TS`, clear it; the
  timestamp has been completed elsewhere.
- Do not complete older local write/replay tickets merely because a higher
  timestamp was validated. Those older tickets complete only through their own
  ACK/ready path or through a membership change that removes the missing ACKs.
- If state was `sInvalidWR` with `LocalWriteTS < key.TS`, the key becomes
  `sValid` for the higher timestamp while the lower pending operation remains
  pending.

### Message-loss timeout

Prompt: a pending ticket's message-loss timer fires.

Rules:

- If this node is coordinating `sWrite` or `sReplay`, rebroadcast the same
  `INV` with the same timestamp and value, then reset the timer. Do not mark
  the operation ready unless the ACK set already covers the current live
  membership.
- If the key is `sInvalid` and `LastWriterID` is still in the current live
  membership, keep the read/write buffered and wait.
- If the key is `sInvalid` and `LastWriterID` has been removed from the current
  live membership, enter `sReplay` and broadcast `INV` with the existing
  timestamp and value.
- Do not shrink the ACK requirement because of timeout alone. The live set
  changes only through Tube/RM membership updates.

### Membership change

Prompt: Tube/RM publishes a new membership epoch after the old membership lease
has expired.

Rules:

- Update `EpochV` and the live node set atomically in the Hermes event loop.
- Drop future messages from older epochs.
- Recompute every pending ACK set against the new live membership.
- Clear any ready generation that was tied to the old membership snapshot, then
  recompute readiness against the new live set.
- For `sWrite`, if the pending write now has all ACKs from live members:
  mark it ready; `complete_ready` transitions to `sValid`, completes the client
  ticket, and broadcasts `VALIDATE`.
- For `sReplay`, if the replay now has all ACKs from live members:
  mark it ready; `complete_ready` transitions to `sValid`, unblocks/retries
  reads, and broadcasts `VALIDATE`.
- For `sInvalidWR`, if the lower local write/replay now has all required ACKs:
  mark it ready; `complete_ready` completes or retries that local ticket, but
  does not broadcast `VALIDATE`.
- For pending RMWs after reconfiguration:
  reset ACKs and replay the RMW to re-check conflicts.
- For keys in `sInvalid` whose `LastWriterID` is no longer live:
  the next blocked request or maintenance scan may start a replay.
- After processing the membership change, drain ready completions before the
  event loop blocks again.

## Implementation plan

1. Default to the proved base protocol while bringing correctness up.
   - Disable O3 broadcast-ACK pseudo-ticket completion by default.
   - Keep `VALIDATE` broadcasts on current timestamp write/replay completion.
   - Re-enable O3 only after separate tests or a separate proof cover
     ACK-before-INV, follower-side all-ACK inference, and pseudo-ticket
     lifetime.

2. Make membership and leases first-class in Hermes.
   - Store the current live member set in `HermesNode`.
   - Reject local reads/writes/RMWs when the operating lease is expired.
   - Stop `cmd/hermes/main.go` from consuming membership upcalls that belong to
     the Hermes node.
   - Use membership upcalls to connect/disconnect Hermes peer circuits.

3. Split key timestamp from local pending update timestamp.
   - Keep `KeyMeta.TS` as the current key timestamp.
   - Add explicit per-key pending operation metadata, or extend
     `HermesTicket`, so overwritten local writes can complete against their
     original `LocalWriteTS`.
   - Track `pending_ts`, `pending_rmw`, ACK set, ready bit, and ready
     generation separately from `KeyMeta.TS`, `KeyMeta.IsRMW`, and
     `KeyMeta.State`.
   - Store the parent/base timestamp for RMW conflict checks.

4. Add proof-shaped ready/completion helpers.
   - `markReady`: requires all current live members are ACKed and the RMW
     conflict guard passes.
   - `completeReady`: handles the two proved completion cases:
     `LocalWriteTS == KeyMeta.TS` and `LocalWriteTS < KeyMeta.TS`.
   - Current completion sets `sValid`, completes the local ticket, and
     broadcasts `VALIDATE` in the base protocol.
   - Overwritten completion clears the lower pending op and completes/retries
     the local ticket, but does not broadcast `VALIDATE` and does not overwrite
     the higher key timestamp/value.
   - Drain ready completions after ACKs, VALIDATE, membership changes, replay
     starts, and timeout processing.

5. Fix the write/RMW timestamp rules.
   - Write: `+2` when RMW is enabled.
   - RMW: `+1`.
   - Keep the current deterministic tie-breaker only if every node computes it
     identically from the same `(version, CoordID)` pair.

6. Implement RMW fully.
   - Add public API.
   - Implement RMW start, ACK completion, abort, stale-RMW-INV response, and
     membership replay.
   - Track RMW parent/base timestamps.
   - Refuse to mark a pending RMW ready if a different RMW from the same base
     has completed.
   - On receiving a conflicting completed RMW invalidation, send local state
     back and do not ACK.
   - Add tests for write-vs-RMW and RMW-vs-RMW races.

7. Correct INV handling.
   - Equal timestamp: ACK-only.
   - Greater timestamp: apply value/state and abort lower RMWs.
   - Stale write INV: ACK-only.
   - Stale RMW INV: send local INV back instead of ACK.
   - Preserve lower non-RMW pending metadata when entering `sInvalidWR`.
   - If a higher INV overwrites a non-RMW pending operation, preserve its ready
     generation if one exists so `completeReady` can drain the lower operation.
   - If a higher INV aborts a pending RMW, retire any ready generation while
     clearing the RMW's pending metadata.

8. Correct ACK and VALIDATE handling around `sInvalidWR`.
   - Completing a lower overwritten operation must not publish a lower value.
   - Validation of a higher timestamp must not prematurely complete a lower
     operation.
   - Reads should only return after the timestamp they observe is valid.
   - A matching higher `VALIDATE` can make the key `sValid` while the lower
     pending operation remains tracked.

9. Move failure completion onto membership-change semantics.
   - Timers retransmit or start replay only when membership says the old
     writer is gone.
   - Completion without a missing ACK happens only after the missing node is no
     longer live in the current epoch.
   - Remove timeout paths that complete `sWrite`, `sReplay`, or `sInvalidWR`
     merely because a deadline expired.

10. Expand tests.
   - Lease expiry rejects local requests.
   - Membership change completes writes/replays waiting only on removed nodes.
   - Coordinator failure before any INV, after some INVs, and after all INVs.
   - Lost INV, lost ACK, lost VALIDATE, duplicate messages, and reordered
     messages.
   - Concurrent writes from every node to the same key.
   - Concurrent write vs RMW and RMW vs RMW.
   - `sInvalidWR` read safety: no read returns an unvalidated higher value or
     an overwritten lower value.
   - Ready/completion fairness: once a ticket becomes ready, the event loop
     drains it without requiring another client operation.
   - VALIDATE while lower pending exists: higher reads may complete, but lower
     write/replay completion must not overwrite the higher value.
