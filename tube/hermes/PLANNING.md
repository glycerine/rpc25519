# Hermes implementation planning

This note summarizes the current state of the Tube Hermes implementation and
spells out the protocol transitions we should implement. It is based on:

- `hermes/original-reference/Hermes/tla/Hermes.tla`
- `hermes/original-reference/Hermes/tla/HermesRMWs.tla`
- `hermes/original-reference/Hermes/tla/protocol-actions.png`
- `hermes/original-reference/Hermes/src/hermes/hermesKV.c`
- `hermes/original-reference/Hermes/src/hermes/hermes_worker.c`
- the checked Ivy models in `hermes/hermes.ivy`, `hermes/hermes_o3.ivy`, and
  `hermes/hermes_rmw_o3.ivy`
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

`hermes/hermes.ivy` verifies the base single-key protocol with Ivy 1.7.
`hermes/hermes_o3.ivy` verifies the same protocol plus the proved non-RMW O3
broadcast-ACK path. `hermes/hermes_rmw_o3.ivy` extends O3 to RMW timestamps:
RMW O3 completion must re-check the completed-RMW conflict guard, and RMW O3
liveness is "finish or observe a conflicting completed RMW" rather than
unconditional finish.

```bash
ivy_check hermes/hermes.ivy
ivy_check hermes/hermes_o3.ivy
ivy_check hermes/hermes_rmw_o3.ivy
```

The current checked result for all three files is `OK`. The models prove:

- Safety: any two live replicas in `hs_valid` have the same timestamp and value.
- Safety: once a timestamp is completed, every live node has advanced to at
  least that timestamp.
- RMW safety: at most one RMW derived from the same base timestamp can
  complete.
- Liveness: if `complete_ready(n)` is attempted infinitely often for a node,
  then every ready completion epoch for that node is eventually marked done.
- O3 liveness in `hermes_o3.ivy`: if `o3_complete(n)` is attempted fairly, a
  recorded non-RMW O3 quorum for an installed invalid timestamp cannot remain
  unfinished forever.
- RMW O3 liveness in `hermes_rmw_o3.ivy`: if `o3_complete(n)` is attempted
  fairly, a recorded RMW O3 quorum for an installed invalid timestamp cannot
  remain unfinished forever while the RMW conflict marker is clear. It either
  completes, stops matching the installed timestamp, becomes valid by another
  path, or is marked conflicted by a completed sibling RMW.

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
- The base proof in `hermes.ivy` models `INV -> ACK -> VALIDATE`.
  `hermes_o3.ivy` separately models the O3 broadcast-ACK optimization for
  non-RMW writes/replays. `hermes_rmw_o3.ivy` adds RMW O3 by recording the
  timestamp parent in `parent_ts`, marking sibling RMW conflict through
  `rmw_conflict`, and requiring `o3_complete` to re-check both
  `~rmw_conflict(t)` and the original completed-RMW conflict guard.

## `hermes_o3.ivy` action traceability

Line references in this section are to `hermes/hermes_o3.ivy`. The Ivy actions
are atomic transitions. The Go implementation can queue network sends outside
the state lock/event-loop turn, but it must not expose an intermediate key,
pending-ticket, ACK-set, ready-generation, or O3 pseudo-ticket state that no Ivy
action can reach.

Implementation-critical invariant groups:

- Initialization and metadata shape: lines 99-130 define one initial valid
  timestamp, no pending operation, no ACKs, no O3 quorums, and the initial
  timestamp completed. New Go state should start in this shape for every key.
- Timestamp/message well-formedness in `hermes_o3.ivy`: lines 411-430 require
  each timestamp to have one value/parent meaning, require INV/ACK/VALIDATE/O3
  records to imply known timestamps, and restrict this proof's O3 quorums to
  non-RMW timestamps. The RMW extension below removes that restriction and adds
  conflict bookkeeping.
- Pending and ready metadata: lines 432-452 require pending metadata to remain
  separate from the key's current timestamp, require self-ACK on pending work,
  and require ready certificates to imply full live ACK coverage.
- Completion/read safety: lines 457-464 are the core read-safety contract:
  completed timestamps have reached every live node, and two live valid replicas
  agree on timestamp and value.
- RMW safety: lines 467-477 enforce write/RMW timestamp spacing and at most one
  completed RMW per parent timestamp.
- Liveness bookkeeping: lines 552-559 keep the fair-attempt pulses temporary.
  Lines 561-575 prove ready work eventually completes under fair
  `complete_ready` attempts. Lines 577-592 prove non-RMW O3 quorums eventually
  finish under fair `o3_complete` attempts.

Action-to-implementation mapping:

| Ivy action | Proof lines | Implementation mapping |
| --- | --- | --- |
| `init` | 99-130 | Per-key/default node state. Initialize `sValid`, initial timestamp/value, no pending ticket, no ready bit, no ACK set, no pseudo-ticket, initial timestamp completed. |
| `local_write` | 133-162 | `Write` start from live lease, no pending op, `sValid` or `sInvalid`. Allocate a fresh non-RMW timestamp, record parent/value metadata, install key state as `sWrite`, create pending metadata with self-ACK, then publish/broadcast non-RMW `INV`. |
| `local_rmw` | 164-192 | Future RMW API start from live lease, no pending op, and `sValid` only. Allocate a fresh RMW timestamp below any concurrent write from the same base, record parent/value/RMW metadata, install `sWrite`, create pending metadata with self-ACK, then broadcast RMW `INV`. |
| `receive_write_inv` | 194-224 | Non-RMW `INV` handling. Always record/send ACK for the sender/timestamp. If incoming timestamp is higher, abort a lower pending RMW, otherwise preserve lower non-RMW pending metadata as `sInvalidWR`; install timestamp/value as non-RMW and set `LastWriterID` to the sender. Equal/stale non-RMW INV is ACK-only. |
| `receive_rmw_inv` | 226-268 | RMW `INV` handling. First enforce the completed-RMW conflict guard. If incoming timestamp is higher, install it, abort any lower RMW, preserve lower non-RMW pending metadata as needed, and ACK. If equal, ACK only. If stale, send this node's current INV back and do not ACK. |
| `receive_rmw_inv_completed_conflict` | 270-288 | Explicit RMW abort path. When a different RMW from the same parent has already completed, send this node's current INV back to the stale coordinator and do not ACK. |
| `receive_ack` | 290-300 | ACK handling for local pending operations only. Count an ACK only when it matches `LocalWriteTS`; if a ready certificate already exists, retire its generation before mutating the ACK set, then clear `ready` and recompute readiness. |
| `mark_ready` | 302-313 | Ready-certificate creation. Wait until every live member in the current membership is ACKed. For pending RMW, also wait for the completed-RMW conflict guard. Allocate a fresh ready generation and mark the pending operation ready. |
| `complete_current` | 315-332 | Base current-timestamp completion. Wait for live node, pending operation, `LocalWriteTS == KeyMeta.TS`, state `sWrite` or `sReplay`, and a ready certificate. Mark the timestamp completed, broadcast/record `VALIDATE`, set `sValid`, retire ready generation, and clear pending metadata. |
| `complete_overwritten` | 334-352 | Lower overwritten-operation completion. Wait for live node, pending operation, `LocalWriteTS < KeyMeta.TS`, and ready certificate. Mark only the lower timestamp completed for the local ticket; do not broadcast `VALIDATE`; clear pending metadata; turn `sInvalidWR` into `sInvalid` unless a matching higher validation already made the key valid. |
| `receive_validate` | 354-369 | `VALIDATE` handling. If the validate timestamp equals the key timestamp, set `sValid`. Clear pending metadata only when the pending timestamp exactly matches the key timestamp. Do not complete or clear older overwritten work. |
| `replay_after_failure` | 371-393 | Replay start. Wait until the node is live, the key has no pending op, state is `sInvalid`, and `LastWriterID` is no longer live in the current membership. Preserve timestamp/value/RMW kind, enter `sReplay`, create self-ACKed pending metadata, retire stale ready generation, and rebroadcast the matching INV kind. |
| `fail` | 395-405 | Membership removal/local failure transition. Remove the node from the live set and clear that node's pending, ACK, and ready metadata. Retire any ready generation owned by the failed node. |
| `complete_ready` | 483-513 | Fair event-loop completion driver. This is the liveness-facing implementation helper. It performs the same two cases as `complete_current` and `complete_overwritten`, and must be attempted whenever ready work may exist. |
| `o3_observe_quorum` | 515-524 | O3 pseudo-ticket observation. For a live node, record a pseudo-ticket only after ACKs from every currently live member are visible for a non-RMW timestamp. Do not mutate key state here. |
| `o3_complete` | 526-550 | O3 follower completion. Wait until the matching non-RMW timestamp is already installed locally (`KeyMeta.TS == T`), the key is not valid, and the O3 pseudo-ticket has all live ACKs. Then mark completed, set `sValid`, and clear same-timestamp pending metadata without broadcasting `VALIDATE`. |

`hermes_rmw_o3.ivy` extends those O3 rows to RMW timestamps. It records
`parent_ts` and `rmw_conflict` at lines 82-85 and 123-126, records parent
metadata for local writes/RMWs at lines 151-186, marks sibling RMW conflicts
when timestamps complete at lines 334-353, 498-511, and 549, and requires
`o3_complete` to see `~rmw_conflict(t)` plus the original completed-RMW conflict
guard at lines 540-549. Its temporal properties at lines 591-621 prove O3
quorums finish or become conflicted.

Ordering notes that must not be loosened:

- Do not merge `receive_ack` and completion into one unstructured path. The
  proof separates ACK counting (`receive_ack`, lines 290-300), ready-certificate
  creation (`mark_ready`, lines 302-313), and fair completion (`complete_ready`,
  lines 483-513). Go may call helpers back-to-back in one event-loop turn, but
  the metadata boundary must remain explicit.
- When a higher INV overwrites a local pending operation, install the higher key
  timestamp/value and preserve a lower non-RMW pending ticket only as lower
  pending metadata. Completion of that lower ticket must follow
  `complete_overwritten` lines 334-352 and must not publish `VALIDATE`.
- A ready generation belongs to one live-membership snapshot. Retire or
  recompute it before changing ACK membership, processing a matching validate,
  aborting an RMW, or clearing pending metadata.
- O3 completion is two-step: observe all live ACKs first (`o3_observe_quorum`,
  lines 515-524), then wait until the matching non-RMW INV/value is installed
  locally before setting `sValid` (`o3_complete`, lines 526-550). ACK quorum
  alone is not enough to expose a value.
- Network sends should be ordered after the local event-loop state transition is
  durably represented in memory. The Ivy action is atomic; real Go code must not
  let a sent ACK/INV/VALIDATE race ahead of the local state that justifies it.

Absolute wait points:

- Wait for a valid operating membership lease before local reads, writes, RMWs,
  replay starts, and ACK/quorum decisions. This is the Go counterpart of the
  `live(n)` preconditions throughout the proof.
- Wait for every currently live member to ACK before `mark_ready` lines 302-313,
  `complete_current` lines 315-332, `complete_overwritten` lines 334-352, or O3
  quorum observation lines 515-524.
- Wait for Tube/RM membership change and old-lease expiry before replaying or
  completing around a missing writer. Timeout alone cannot satisfy
  `~live(last_writer(n))` in `replay_after_failure` lines 371-376.
- Wait for the RMW conflict guard before accepting, replaying, or marking ready
  an RMW. The guard appears on RMW receive lines 226-230, replay lines 371-376,
  and ready creation lines 302-308.
- Wait for the matching INV/value to be installed locally before O3 completion.
  `o3_complete` requires `cur_ts(n) = t` at lines 528-533; a pseudo-ticket for a
  timestamp not yet installed must remain blocked.
- Wait for fair event-loop progress before considering liveness satisfied. Ready
  work and O3-completable work must be drained before the event loop blocks
  again; otherwise the implementation is outside lines 561-592.

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
    and wait for `VALIDATE`, a later replay, or the proved O3 path for an
    installed non-RMW timestamp with all live ACKs observed.
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
- Broadcast `VALIDATE` in the base protocol. Under the proved O3 variant, a
  non-RMW write may skip `VALIDATE` only when the O3 pseudo-ticket condition is
  met; RMW completion must still use the base path.

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
- In the base protocol, ignore O3 pseudo-tickets and count ACKs only for local
  pending operations. In the O3 protocols, retain pseudo-tickets and complete
  them only through the `o3_observe_quorum` and `o3_complete` sequence. For RMW
  pseudo-tickets, also track parent/conflict state and treat a completed sibling
  RMW as a terminal conflict outcome.

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
  - Under O3, a write/replay may skip `VALIDATE` only if every live replica can
    independently infer validity from the broadcast ACK set. For RMW timestamps,
    the RMW conflict guard must also remain clear.

- `sReplay`:
  - If `LocalWriteTS == key.TS`, `complete_ready` transitions to `sValid`.
  - Restart or complete the blocked read that triggered replay.
  - Broadcast `VALIDATE` in the base protocol.
  - Under O3, replay may skip `VALIDATE` under the same all-ACK visibility
    condition. RMW replay additionally requires the conflict guard to remain
    clear.

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
  - Under the proved O3 variants, if pseudo-ticket collection proves that the
    current timestamp has all live ACKs and the value is installed locally,
    transition `sInvalid -> sValid` and unblock reads. For an RMW timestamp, do
    this only while the conflict marker is clear; a completed sibling RMW is a
    terminal conflict outcome for that pseudo-ticket.

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
   - Proof anchors: base current completion lines 315-332, overwritten
     completion lines 334-352, fair completion lines 483-513, validation lines
     354-369.
   - Keep O3 disabled until the implementation matches `hermes_o3.ivy` lines
     515-550 for non-RMW and `hermes_rmw_o3.ivy` lines 531-549 for RMW.
   - Once enabled, O3 pseudo-ticket completion is allowed only for timestamps
     with all live ACKs observed and the matching value installed.
   - For RMW timestamps, also require the conflict marker to be clear and
     re-check the completed-RMW conflict guard before completing.
   - Keep `VALIDATE` broadcasts on current timestamp write/replay completion.

2. Make membership and leases first-class in Hermes.
   - Proof anchors: `live(n)` preconditions on local/write/RMW/receive/replay
     actions at lines 133-138, 164-170, 194-197, 226-230, 290-294,
     302-308, 315-320, 334-338, 354-356, 371-376, and failure removal lines
     395-405.
   - Store the current live member set in `HermesNode`.
   - Reject local reads/writes/RMWs when the operating lease is expired.
   - Stop `cmd/hermes/main.go` from consuming membership upcalls that belong to
     the Hermes node.
   - Use membership upcalls to connect/disconnect Hermes peer circuits.

3. Split key timestamp from local pending update timestamp.
   - Proof anchors: initialization lines 99-130, local pending writes at
     lines 155-159 and 185-189, pending invariants lines 436-452, and
     overwritten completion lines 334-352.
   - Keep `KeyMeta.TS` as the current key timestamp.
   - Add explicit per-key pending operation metadata, or extend
     `HermesTicket`, so overwritten local writes can complete against their
     original `LocalWriteTS`.
   - Track `pending_ts`, `pending_rmw`, ACK set, ready bit, and ready
     generation separately from `KeyMeta.TS`, `KeyMeta.IsRMW`, and
     `KeyMeta.State`.
   - Store the parent/base timestamp for RMW conflict checks.

4. Add proof-shaped ready/completion helpers.
   - Proof anchors: ACK count lines 290-300, ready creation lines 302-313,
     current completion lines 315-332, overwritten completion lines 334-352,
     fair `complete_ready` lines 483-513, and liveness theorem lines 561-575.
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
   - Proof anchors: write timestamp guard lines 133-147, RMW timestamp guard
     lines 164-177, and write/RMW spacing invariants lines 466-477.
   - Write: `+2` when RMW is enabled.
   - RMW: `+1`.
   - Keep the current deterministic tie-breaker only if every node computes it
     identically from the same `(version, CoordID)` pair.

6. Implement RMW fully.
   - Proof anchors: `local_rmw` lines 164-192, `receive_rmw_inv` lines
     226-268, completed-conflict response lines 270-288, RMW guards in
     `mark_ready` lines 302-308 and replay lines 371-376, and RMW safety
     invariants lines 467-477.
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
   - Proof anchors: non-RMW INV lines 194-224, RMW INV lines 226-268, and
     completed-conflict RMW INV lines 270-288.
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
   - Proof anchors: ACK lines 290-300, `mark_ready` lines 302-313,
     overwritten completion lines 334-352, validate lines 354-369, and
     pending/ready invariants lines 436-452.
   - Completing a lower overwritten operation must not publish a lower value.
   - Validation of a higher timestamp must not prematurely complete a lower
     operation.
   - Reads should only return after the timestamp they observe is valid.
   - A matching higher `VALIDATE` can make the key `sValid` while the lower
     pending operation remains tracked.

9. Move failure completion onto membership-change semantics.
   - Proof anchors: `replay_after_failure` lines 371-393 and `fail` lines
     395-405.
   - Timers retransmit or start replay only when membership says the old
     writer is gone.
   - Completion without a missing ACK happens only after the missing node is no
     longer live in the current epoch.
   - Remove timeout paths that complete `sWrite`, `sReplay`, or `sInvalidWR`
     merely because a deadline expired.

10. Implement the proved O3 path only after the base/RMW/membership work above.
   - Proof anchors: non-RMW O3 quorum observation lines 515-524 and completion
     lines 526-550 in `hermes_o3.ivy`; RMW O3 quorum observation lines 531-535,
     guarded completion lines 540-549, conflict bookkeeping lines 82-85,
     123-126, 151-186, 334-353, 498-511, and liveness lines 591-621 in
     `hermes_rmw_o3.ivy`.
   - Absolutely wait before enabling this by default until the implementation
     has pseudo-tickets keyed by `(key, coordinator, timestamp)`, can prove all
     current live members ACKed, and can prove the matching value is installed
     locally.
   - Do not complete from a pseudo-ticket if `KeyMeta.TS != ticket.TS`, if the
     key is already valid, or if the membership epoch changed without
     recomputing the ACK requirement.
   - For RMW pseudo-tickets, do not complete if a sibling RMW from the same
     parent has completed. The implementation needs the equivalent of
     `rmw_conflict` and must treat conflict as a terminal O3 outcome for that
     candidate, not as an O3 success.
   - O3 completion sets `sValid` and may clear same-timestamp pending metadata,
     but it does not broadcast `VALIDATE`.

11. Expand tests.
   - Proof anchors: safety invariants lines 411-477, base liveness lines
     561-575, and O3 liveness lines 577-592.
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
