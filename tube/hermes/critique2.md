2026 Sept 17: critique of the ivy_check passed hermes_rmw_o3_testing.ivy spec.

https://chatgpt.com/c/6aac3c51-7f54-83ea-8460-dffeeacd5dd4

The hermes_rwm_o3_testing.ivy is an Ivy formal spec for the hermes protocol described in the attached pdf. While the Ivy proof tool "ivy_check" has agreed that this spec proves its safety and liveness properties, I am still worried that the model spec does not faithfully capture the hermes protocol. So please analyze the spec against the textual protcol description in the paper and look for unwaranteed assumptions and gaps in the model.

here are the TLA+ specs, if they help.


A: Yes. The TLA+ specs help a great deal because they let us distinguish three things: the protocol described in the paper, the authors’ own formal abstraction, and your Ivy abstraction. I do not see new TLA+ files as separate chat attachments yet, but I found and checked the authors’ official `HermesRMWs.tla` in the Hermes repository. That already exposes several important discrepancies.

The main conclusion is that your Ivy model proves some valuable invariants, but I would not yet regard it as a faithful proof of the full Hermes protocol described in the paper. The strongest concerns are the reliable-membership abstraction, the treatment of failures/message loss, the limited meaning of the liveness properties, and especially the extra oracle-like conditions around O3 + RMW.

### What the Ivy proof actually establishes

Your own header accurately says that this is a single-key model proving “read-state agreement, not a full history linearizability theorem.”  The central read safety property is essentially:

> if two live replicas are both `hs_valid`, their timestamp and value agree.

That is exactly what the invariants at lines 601–608 establish. 

That is a strong and useful invariant. But it is materially narrower than “every client-visible execution of Hermes is linearizable.”

Here is my gap analysis, roughly in decreasing order of concern.

| Concern                                                            | Paper / concrete protocol                                                                                                                                                        | Ivy model                                                                                                                                                   | Why it matters                                                                                                                                |                                                              |
| ------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------ |
| **Reliable membership is effectively an oracle**                   | Nodes have leases, local membership state and `epoch_id`; messages are epoch-tagged, and stale-epoch messages are dropped. Reconfiguration happens only after lease expiration.  | One global `live(N)` relation; `fail(n)` atomically changes the whole system. There is no membership epoch in INV/ACK/VAL.                                  | This removes one of Hermes's most delicate correctness boundaries.                                                                            |                                                              |
| **Failure and reconfiguration are collapsed into one atomic step** | Crash/partition → wait for lease expiry → RM majority update → different replicas learn the new epoch at different times.                                                        | `fail` immediately removes the node, deletes/reset messages and ACK state, changes surviving coordinators into replay state, etc.                           | Many races between protocol traffic and membership transition simply cannot occur.                                                            |                                                              |
| **Message-loss recovery is not really modeled**                    | INV/ACK loss causes retransmission after `mlt`; a missing VAL can cause a follower to replay even when the original coordinator is still alive.                                  | Messages are permanent relations “never removed”; follower replay requires `~writer_live(n)`.                                                               | Safety against delayed/duplicate packets is exercised, but recovery from genuine loss is not.                                                 |                                                              |
| **The liveness theorem is much weaker than protocol liveness**     | Paper discusses eventual completion/replay under partial synchrony and reconfiguration.                                                                                          | Temporal properties begin only after `ready` or an O3 quorum already exists, and assume the corresponding completion driver is invoked infinitely often.    | No theorem says an INV arrives, ACK quorum is reached, an invalid read unblocks, a membership transition finishes, etc.                       |                                                              |
| **No client-visible read history**                                 | Linearizability constrains invocation/completion order.                                                                                                                          | There is no `read` protocol action or read-result history at all.                                                                                           | Valid-state agreement is necessary but not literally a linearizability proof.                                                                 |                                                              |
| **RMW semantics are only arbitration semantics**                   | Hermes presents RMWs such as CAS as atomic read-modify-write operations.                                                                                                         | `local_rmw(n,t,v)` receives an arbitrary new `v`; nothing relates it to the value read, a predicate, or a client-visible result.                            | You prove “which RMW may commit,” not linearizable semantics of a particular RMW.                                                             |                                                              |
| **Writes may start from Invalid state**                            | Paper says a coordinator issues a write only when the key is Valid.                                                                                                              | `local_write` explicitly permits `hs_valid                                                                                                                  | hs_invalid`.                                                                                                                                  | This is an optimization/extension, not the textual protocol. |
| **O3+RMW contains hidden global knowledge**                        | O3 says a follower may proceed after receiving all broadcast ACKs.                                                                                                               | `o3_observe_quorum` additionally inspects the coordinator's internal RMW/pending/parent state; `o3_complete` tests global `rmw_conflict`/`write_conflict`.  | Those predicates are not information an actual follower possesses. They can suppress precisely the executions you want the checker to attack. |                                                              |

## 1. The biggest abstraction gap is membership/epochs

The paper's safety argument is not simply “wait for all nodes that happen to be alive.” It is “wait for all members of a **reliably established membership**, while only operational nodes holding valid leases may serve requests.”

Messages carry an `epoch_id`, and a receiver with a different epoch drops them. The paper specifically relies on this to survive network partitions and staggered membership updates. 

The authors’ TLA+ spec confirms that this is intended protocol state, not merely prose. `HermesRMWs.tla` has `aliveNodes`, `epochID`, and `nodeWriteEpochID`; RMW invalidations explicitly contain an `epochID`, and receive actions require the message epoch to equal the current epoch. ([GitHub][1])

Your Ivy model contains a type called `epoch`, but this is important: **it is not the Hermes membership epoch**. `mark_ready` chooses a fresh `e`, stores it in `ready_epoch`, and uses it to label a liveness episode.  It never appears in INV/ACK/VAL messages.

So an easy reader of the Ivy model could mistakenly think epoch handling has been formalized when it has not.

I would characterize the current proof as:

**Hermes protocol conditional on an ideal, globally consistent membership oracle.**

That is useful, but considerably weaker than the paper's end-to-end failure model.

## 2. `fail` performs actions the real system cannot perform atomically

This is one of the most important modeling choices to revisit.

Your `fail(n)` does all of these in one state transition:

* removes or regenerates INV relations,
* destroys every ACK message,
* clears O3 quorums,
* turns surviving pending operations into `hs_replay`,
* resets gathered ACKs,
* marks the failed writer dead everywhere,
* finally sets `live(n) := false`. 

There is no single Hermes protocol event corresponding to that.

In the paper, a crash may occur while old-epoch packets are still in flight. Then leases expire. Then the RM chooses a membership. Then individual replicas receive the m-update at different times. Updated replicas can already serve some reads, while writes stall because still-old replicas reject their new-epoch INVs. 

The atomic Ivy `fail` therefore removes a substantial class of intermediate states.

The authors’ TLA+ RMW spec is more explicit about reconfiguration. In particular, after an epoch change a blind-write replay keeps already collected ACKs, whereas an RMW replay deliberately resets its ACK set. ([GitHub][1]) Your `fail` resets the ACK set to self-only for every current pending operation. That is probably conservative for safety, but it is another concrete divergence from the authors' model.

## 3. The network model tests duplication/reordering much better than loss

This comment is revealing:

> “Persistent network buffers. Messages are never removed…”



The official TLA+ RMW model does the same thing: it explicitly says messages are only appended, intentionally testing duplicate delivery and reordering. ([GitHub][1])

That is good for one purpose: once a message has been sent, the model checker may deliver it arbitrarily late and repeatedly.

But it does not faithfully express packet loss followed by Hermes's timeout machinery. In the paper, an INV or ACK really can disappear, causing the coordinator's `mlt` to expire and the INV to be retransmitted. A lost VAL can leave a follower Invalid until a request triggers its own timeout and replay. 

Your follower replay condition is instead:

```ivy
state(n) = hs_invalid &
~writer_live(n)
```



So consider this perfectly ordinary paper execution:

1. coordinator writes successfully;
2. all ACKs arrive;
3. coordinator commits;
4. VAL to replica B is lost;
5. coordinator remains healthy indefinitely;
6. B later receives a read.

The paper says B eventually times out and **replays the write despite the writer being alive**.

Your model has no such transition.

A permanently nondelivered `val_msg` can represent the lost packet, but then B remains stuck forever because the replay action is disabled. None of your temporal properties detects this.

That is a concrete liveness hole between model and paper.

## 4. The current “liveness proof” should be interpreted very narrowly

This may be the largest source of false confidence from `ivy_check OK`.

For `ready_epochs_eventually_finish`, the hypothesis is:

```ivy
globally eventually complete_try(N)
```

and the proof starts with an operation that is *already* `ready`. 

Likewise the O3 theorems assume `o3_quorum` has already been obtained and that `o3_try` keeps occurring. 

There is no temporal obligation saying that:

* an INV will eventually be retransmitted,
* an INV will eventually reach every current member,
* ACKs will eventually reach a coordinator,
* RM will eventually finish a membership change,
* an Invalid replica will eventually replay,
* a pending client request will eventually start,
* a stalled read will eventually return.

So I would rename the meaning in your head from:

**“Hermes is live”**

to something closer to:

**“once an operation has obtained the required completion evidence, a fair completion driver does not remain internally stuck.”**

That is still worth proving, but it is a different theorem.

## 5. Valid-replica agreement is not a full linearizability specification

Your model itself warns about this. 

The invariants

```ivy
valid(N1) & valid(N2) -> cur_ts(N1) = cur_ts(N2)
valid(N1) & valid(N2) -> cur_value(N1) = cur_value(N2)
```

are excellent protocol invariants. 

But linearizability is a property of histories. You need to know that if operation A completes before operation B is invoked, their abstract ordering respects that real-time order.

There are no read invocation/completion events in this model at all. `completed(T)` is protocol metadata, not a client history.

I don't immediately see an execution violating linearizability given your other invariants; the point is that the theorem being checked cannot state such a violation. A bug could preserve “all currently valid replicas agree” while still mishandling an operation's client response boundary.

A small abstract atomic-register refinement model would close this gap much more convincingly.

## 6. RMW correctness is weaker than the term “RMW linearizability” suggests

The paper motivates RMWs with operations like compare-and-swap. It says only the winning concurrent RMW may commit and writes outrank concurrent RMWs. 

Your model accurately captures much of that timestamp/arbitration machinery. It also closely follows the official TLA+ design: writes increment by two, RMWs by one; stale RMW INVs receive a newer INV rather than an ACK. ([GitHub][1])

But:

```ivy
local_rmw(n,t,v)
```

takes `v` from the environment. Nothing says:

```text
v = f(old_value)
```

or that a CAS predicate held, or records the old value/result returned to the client. 

Interestingly, this is largely inherited from the authors' TLA+ spec: its `HRSemanticsRMW` invariant is principally about preventing committed RMW/write version combinations that would imply two conflicting operations read the same predecessor. ([GitHub][1])

So this is partly a limitation of the original proof methodology, not just your Ivy port.

## 7. Starting a blind write while Invalid is not the paper protocol

This is one very clean textual mismatch.

Paper:

> coordinator issues a write only if Valid; otherwise the write is stalled.



Ivy:

```ivy
require state(n) = hs_valid | state(n) = hs_invalid;
```



The authors' repository actually acknowledges such an optimization separately: its TLA directory README warns that some protocol-action material contains optimizations such as issuing writes from Invalid state. ([GitHub][2]) And in the official `HermesRMWs.tla`, the permissive condition is commented out while the active `HRWrite` requires `"valid"`. ([GitHub][1])

This doesn't necessarily make your protocol unsafe. In fact, proving the larger transition relation safe can be stronger.

But it means you are no longer answering the exact question “is the ASPLOS textual protocol correct?” You are answering “is this modified Hermes variant correct?”

I would separate that optimization into a second model or feature flag.

## 8. The O3/RMW conditions worry me the most in the custom part of this spec

This deserves close attention.

The paper's O3 rule is local and implementable: ACKs are broadcast, and after a follower has received all the ACKs it can service reads without waiting for VAL. 

Your `o3_observe_quorum` starts with the expected ACK condition, but then has conditions such as:

```ivy
live(c) & ts_rmw(t) & parent(t,B) &
    cur_rmw(c) & parent(cur_ts(c),B)
        -> cur_ts(c) = t

live(c) & ts_rmw(t) & parent(t,B) &
    ~cur_rmw(c) & parent(cur_ts(c),B)
        -> false
```

and similar tests against `pending(c)`. 

Then `o3_complete` additionally requires:

```ivy
~rmw_conflict(t) & ~write_conflict(t)
```



Those are **global semantic facts**, not protocol information present in the broadcast ACK messages.

That is dangerous in a model checker. A `require` does not prove that the real implementation knows or satisfies the fact; it simply prevents the checker from exploring the transition when the fact is false.

This is exactly the sort of condition capable of turning a real protocol counterexample into an unreachable model state.

The official `HermesRMWs.tla` I checked contains no O3 machinery at all—the RMW extension's action sets are the baseline write/RMW/ACK/VAL/replay operations. ([GitHub][1]) So this is genuinely new territory in your Ivy model rather than a straightforward translation of their formal proof.

### What I would change first

I would make these changes in this order:

1. **Attack O3 immediately.** Remove every non-locally-observable condition from the *precondition* of `o3_observe_quorum` and `o3_complete`. Keep only what a real replica knows: its state, its TS, membership, and ACK messages it received. Convert the `rmw_conflict`, `write_conflict`, and coordinator-internal-state conditions into `assert`s or invariants. If Ivy produces a counterexample, that counterexample is extremely interesting.

2. **Split `fail` into protocol-realistic steps:** `crash`, lease expiration, RM decision, per-node receipt of the new membership, and replay. Add a real per-node membership epoch and tag INV/ACK/VAL with it. Do not delete old messages; make receivers reject them by epoch, as the paper says.

3. **Model actual message loss and timeout recovery.** Let individual message deliveries disappear, explicitly retransmit INVs after `mlt`, and permit an Invalid follower to replay after timeout even when `last_writer` is still alive.

4. **Add an abstract client history/refinement specification.** A single atomic register plus an atomic RMW reference object is enough. Record invocation and response. Then prove the implementation refines that rather than using valid/valid agreement as the final safety theorem.

5. **Make the baseline match the paper exactly first:** writes start only in Valid, concrete timestamp = `(version,cid)`, no O3. Once that verifies, add Invalid-state writes and O3 as separately named refinements/optimizations.

A smaller secondary issue is timestamp abstraction. The paper and TLA+ specs explicitly represent a timestamp as `(version, coordinator/tieBreaker)` and set the tie-breaker from the coordinator identity. Your Ivy model instead has an uninterpreted totally ordered `ts` and separately attaches a `ts_version`; `local_write` is handed an arbitrary fresh `t`.   This is plausibly a safe abstraction, but I would want an explicit abstraction argument showing that every concrete `[version,cid]` execution maps into it. The model also assumes unbounded versions, so integer wraparound is outside its theorem.

So the short version is: **the core timestamp/invalidation logic looks substantially aligned with Hermes, and your RMW translation has clearly captured a lot of the original TLA+ structure. The proof gap is mostly at the boundaries—the membership service, failure/reconfiguration timing, loss/retry machinery, client histories—and there is one particularly suspicious new boundary inside O3/RMW where global ghost facts are used as transition guards.**

That O3 point is the first thing I would try to break. It has the highest chance of turning the current “OK” into a useful counterexample.

[1]: https://github.com/ease-lab/Hermes/blob/master/tla/HermesRMWs.tla "Hermes/tla/HermesRMWs.tla at master · ease-lab/Hermes · GitHub"
[2]: https://github.com/ease-lab/Hermes/tree/master/tla "Hermes/tla at master · ease-lab/Hermes · GitHub"

