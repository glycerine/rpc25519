# Hermes implementation planning

This note summarizes the current state of the Tube Hermes implementation and
spells out the protocol transitions we should implement. It is based on:

- `hermes/original-reference/Hermes/tla/Hermes.tla`
- `hermes/original-reference/Hermes/tla/HermesRMWs.tla`
- `hermes/original-reference/Hermes/tla/protocol-actions.png`
- `hermes/original-reference/Hermes/src/hermes/hermesKV.c`
- `hermes/original-reference/Hermes/src/hermes/hermes_worker.c`
- the current Go implementation in `hermes/` and `cmd/hermes/`

The TLA specs are the primary safety reference. The original C code is useful
for the production optimizations and for details the TLA does not model, such
as client request completion and operation buffers. The PNG is a compact
cheat-sheet, but it appears to contain or hide several important edge cases.

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
    and wait for `VALIDATE`, O3 broadcast-ACK completion, or a later replay.
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
  transition `sWrite -> sValid`.
- Complete the client write successfully.
- Broadcast `VALIDATE` unless the O3 broadcast-ACK optimization is enabled and
  all replicas can independently observe the all-ACK condition.

### Local RMW request

Prompt: a future client API calls an atomic read-modify-write operation.

RMW is not currently implemented in Go, but the target protocol is:

- RMW may start only from `sValid`.
- Compute the new timestamp as `local.version + 1`.
- Set `IsRMW=true`, apply the candidate value locally, enter `sWrite`, and
  broadcast an RMW `INV`.
- If this local RMW observes a higher timestamp for the same key before it
  gathers all ACKs, abort the RMW ticket.
- If the RMW gathers all ACKs first, transition to `sValid`, complete the
  client RMW, and broadcast `VALIDATE` unless O3 is active.
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
  - If the local state was `sWrite` for a local RMW, abort the local RMW and
    transition to `sInvalid`.
  - If the local state was `sInvalidWR`, remain `sInvalidWR` if there is still a
    lower local non-RMW op to complete; otherwise transition to `sInvalid`.
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
  - Send `ACK`.

- Incoming `TS == local.TS`:
  - Send `ACK`.
  - Do not change state or value.

- Incoming `TS < local.TS`:
  - Do not send `ACK`.
  - Send a local `INV` back to `INV.FromID` using this node's current local
    timestamp, value, and RMW flag. This is the RMW abort path.

### Receiving ACK

Prompt: `recvAck` receives `ACK` for the current epoch.

Common rules:

- The ACK only counts if its timestamp equals a local pending update's
  `LocalWriteTS`.
- Duplicate ACKs are ignored.
- Stale ACKs for older completed or abandoned tickets are ignored.
- Under O3, ACKs may be broadcast to all replicas, so a node may need a
  pseudo-ticket to accumulate ACKs before the corresponding `INV` arrives.

By current key state when the ACK completes the live membership set:

- `sWrite`:
  - Transition to `sValid`.
  - Complete the local write or RMW ticket.
  - Broadcast `VALIDATE` in the base protocol.
  - Under O3, skip `VALIDATE` only if every live replica can independently
    infer validity from the broadcast ACK set.

- `sReplay`:
  - Transition to `sValid`.
  - Restart or complete the blocked read that triggered replay.
  - Broadcast `VALIDATE` in the base protocol.
  - Under O3, skip `VALIDATE` under the same all-ACK visibility condition.

- `sInvalidWR`:
  - The ACK completion is for a lower timestamp local write or replay.
  - If it was a write, complete that write's client ticket successfully.
  - If it was a replay triggered by a read, restart or keep the read blocked
    against the key's current higher timestamp.
  - Do not broadcast `VALIDATE` for the lower timestamp.
  - If the higher timestamp is not valid yet, transition to or remain
    `sInvalid`.
  - If a matching higher `VALIDATE` was already processed, the key may remain
    `sValid`, but the lower ticket completion must not overwrite the higher
    value.

- `sValid` or `sInvalid`:
  - Normally ignore. This can happen for stale ACKs or because O3 collected a
    pseudo-ticket out of order.
  - If O3 pseudo-ticket collection proves that the current timestamp has all
    live ACKs and the value is installed locally, transition `sInvalid -> sValid`
    and unblock reads.

### Receiving VALIDATE

Prompt: `recvValidate` receives `VALIDATE` for the current epoch.

Rules:

- If `VALIDATE.TS != local.TS`, ignore it.
- If `VALIDATE.TS == local.TS`, set the key state to `sValid`.
- Unblock reads waiting on this timestamp.
- Do not complete older local write/replay tickets merely because a higher
  timestamp was validated. Those older tickets complete only through their own
  ACK set or through a membership change that removes the missing ACKs.
- If state was `sInvalidWR`, keep enough pending-operation metadata to complete
  the overwritten lower operation later without broadcasting `VALIDATE`.

### Message-loss timeout

Prompt: a pending ticket's message-loss timer fires.

Rules:

- If this node is coordinating `sWrite` or `sReplay`, rebroadcast the same
  `INV` with the same timestamp and value, then reset the timer.
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
- For `sWrite`, if the pending write now has all ACKs from live members:
  transition to `sValid`, complete the client ticket, and broadcast `VALIDATE`.
- For `sReplay`, if the replay now has all ACKs from live members:
  transition to `sValid`, unblock/retry reads, and broadcast `VALIDATE`.
- For `sInvalidWR`, if the lower local write/replay now has all required ACKs:
  complete or retry that local ticket, but do not broadcast `VALIDATE`.
- For pending RMWs after reconfiguration:
  reset ACKs and replay the RMW to re-check conflicts.
- For keys in `sInvalid` whose `LastWriterID` is no longer live:
  the next blocked request or maintenance scan may start a replay.

## Implementation plan

1. Make membership and leases first-class in Hermes.
   - Store the current live member set in `HermesNode`.
   - Reject local reads/writes/RMWs when the operating lease is expired.
   - Stop `cmd/hermes/main.go` from consuming membership upcalls that belong to
     the Hermes node.
   - Use membership upcalls to connect/disconnect Hermes peer circuits.

2. Split key timestamp from local pending update timestamp.
   - Keep `KeyMeta.TS` as the current key timestamp.
   - Add explicit pending operation metadata, or extend `HermesTicket`, so
     overwritten local writes can complete against their original
     `LocalWriteTS`.

3. Fix the write/RMW timestamp rules.
   - Write: `+2` when RMW is enabled.
   - RMW: `+1`.
   - Keep the current deterministic tie-breaker only if every node computes it
     identically from the same `(version, CoordID)` pair.

4. Implement RMW fully.
   - Add public API.
   - Implement RMW start, ACK completion, abort, stale-RMW-INV response, and
     membership replay.
   - Add tests for write-vs-RMW and RMW-vs-RMW races.

5. Correct INV handling.
   - Equal timestamp: ACK-only.
   - Greater timestamp: apply value/state and abort lower RMWs.
   - Stale write INV: ACK-only.
   - Stale RMW INV: send local INV back instead of ACK.

6. Correct ACK and VALIDATE handling around `sInvalidWR`.
   - Completing a lower overwritten operation must not publish a lower value.
   - Validation of a higher timestamp must not prematurely complete a lower
     operation.
   - Reads should only return after the timestamp they observe is valid.

7. Move failure completion onto membership-change semantics.
   - Timers retransmit or start replay only when membership says the old
     writer is gone.
   - Completion without a missing ACK happens only after the missing node is no
     longer live in the current epoch.

8. Revisit O3 broadcast-ACK optimization after the base protocol is correct.
   - Default to the base `ACK -> VALIDATE` protocol during correctness work.
   - Re-enable O3 only with tests covering out-of-order ACK-before-INV,
     duplicate ACKs, and follower-side all-ACK validity.

9. Expand tests.
   - Lease expiry rejects local requests.
   - Membership change completes writes/replays waiting only on removed nodes.
   - Coordinator failure before any INV, after some INVs, and after all INVs.
   - Lost INV, lost ACK, lost VALIDATE, duplicate messages, and reordered
     messages.
   - Concurrent writes from every node to the same key.
   - Concurrent write vs RMW and RMW vs RMW.
   - `sInvalidWR` read safety: no read returns an unvalidated higher value or
     an overwritten lower value.

