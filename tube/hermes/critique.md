https://chatgpt.com/c/6aa92ce0-e660-83e9-bcb1-e98d802e9314

No. I would **not** regard `hermes_rmw_o3.ivy` as a complete proof that Hermes + RMW + O3 is safe and live.

It does prove some nontrivial invariants of its abstract transition system, so I would not say the safety proof is *entirely* vacuous. But I found three serious problems: the liveness proofs are vacuous; the model admits a concrete RMW/write execution that the supplied TLA+ model explicitly forbids; and several RMW safety conditions are put into `require` clauses as global-oracle assumptions rather than derived from the actual Hermes mechanism.

### 1. The liveness proofs are vacuous

This one is decisive.

The model asserts:

```ivy
invariant ~complete_try(N)
invariant ~o3_try(N)
```

and then states temporal properties with antecedents

```ivy
globally eventually complete_try(N)
```

and

```ivy
globally eventually o3_try(N)
```

respectively.  

So the model proves, at all externally visible states,

$$
G\neg complete\_try(N)
$$

while the theorem assumes

$$
GF\ complete\_try(N).
$$

Those cannot both be true. Therefore the implication is true because its antecedent is false.

The actions themselves reinforce this. `complete_ready` does:

```ivy
complete_try(n) := true;
complete_try(N) := false;
```

and `o3_complete` does the same thing to `o3_try`.  

In Ivy, a capitalized parameterized update such as `r(x,Y) := false` updates the relation for all `Y`; therefore the second statement wipes out the first before the exported action finishes. ([Worldwide Computing Lab][1])

So **all three explicit temporal properties can succeed even if the protocol never makes any progress whatsoever**.

There is a second conceptual problem even if that bug were fixed: `complete_try` is set only when `complete_ready` actually performs a successful completion. Thus `GF complete_try(N)` is not a scheduler-fairness assumption; it is essentially an assumption that successful completions keep happening forever. A one-shot write that should complete once need not satisfy that premise.

The supplied TLA+ specifications don't provide a liveness reference to rescue this. Their `Spec`s have no weak/strong fairness clauses, and their stated theorems are invariants only.   The Hermes paper says the TLA+ work checked safety and absence of deadlocks, which is substantially weaker than proving eventual completion. ([Informatics Homepages][2])

### 2. More seriously: the Ivy model misses an RMW/write safety condition that the TLA+ model checks

`HermesRMWs.tla` explicitly requires that a committed RMW cannot coexist with a committed write at the same relevant version or the immediately following write version:

```tla
\A x \in committedRMWs:
    \A y \in committedWrites:
        /\ x.version /= y.version
        /\ x.version /= y.version - 1
```

Since an RMW increments the base version by 1 and a write by 2, the second condition is exactly what rules out **an RMW and a concurrent write based on the same old value both committing**. 

Your Ivy model has the timestamp-ordering invariant

```ivy
parent(R,B) & parent(W,B) & ts_rmw(R) & ~ts_rmw(W) -> lt(R,W)
```

but that only says the write's timestamp is greater. It does **not** say that the RMW cannot also complete. Its explicit completion exclusion only covers two RMWs with the same parent. 

I walked the following execution through the Ivy guards. Let `r < w`, and let both have the initial timestamp `b` as parent:

1. `A` executes `local_rmw(A,r,vr)`.
2. Concurrently, `B` executes `local_write(B,w,vw)`. The Ivy timestamp-spacing rule permits this provided `r < w`. 
3. `C` receives `r`, advances to `r`, and ACKs `A`.
4. `C` then receives `w`, advances from `r` to `w`, and ACKs `B`. 
5. `A` receives `C`'s ACK for `r`.
6. `B` fails **before `A` sees `w`**. Ivy simply makes `B` non-live; it does not reset `A`'s ACKs or force `A`'s pending RMW to restart. 
7. The remaining live set is `{A,C}`. `A` now has an ACK from itself and from `C`, so `mark_ready(A,...)` succeeds and `complete_current(A)` marks RMW `r` completed. `C` being at the higher timestamp `w` satisfies the completion assertion `r <= cur_ts(C)`. 
8. `C` is sitting in `invalid` at `w`, whose last writer `B` is now dead. Because `w` is a normal write, `replay_after_failure(C)` is permitted. 
9. `C` replays `w`; `A` receives it, advances from `r` to `w`, and ACKs it.
10. `C` gathers the live ACKs and completes `w`.

The end state contains both

```text
completed(r)   ts_rmw(r)    parent(r,b)
completed(w)   !ts_rmw(w)   parent(w,b)
```

and nothing among the listed Ivy safety invariants forbids that.

That is exactly the same-parent RMW/write situation that `HRSemanticsRMW` rules out.

The reason the original Hermes protocol avoids this execution is revealing. On a membership reconfiguration, a pending RMW coordinator must throw away its old ACK evidence and replay the RMW:

```tla
HRRMWReplay(n) ==
    ...
    /\ nodeWriteEpochID[n] < epochID
    ...
    /\ hr_actions_for_upd_replay(n, {})
```



And `nodeFailure` increments the epoch and resets the ACK sets for the nodes.  The paper explicitly calls this rule out: after an RM reconfiguration, a pending RMW resets gathered ACKs and replays specifically to ensure it is not conflicting. ([Informatics Homepages][2])

**The Ivy model has omitted that mechanism.**

This is not merely “less detailed than TLA+.” It allows a behavior contrary to one of the safety properties the TLA+ model checks.

### 3. Some RMW safety is being assumed through global oracle state

There is another reason I would distrust an apparent safety success.

For example, ordinary RMW invalidation reception has this requirement:

```ivy
require parent(t,B) & completed(R) & ts_rmw(R) &
        parent(R,B) -> R = t;
```

and similar global `completed`-based requirements appear in `mark_ready`, `replay_after_failure`, and `o3_complete`.    

There is even a separate action `receive_rmw_inv_completed_conflict` that rejects the incoming invalidation based on the global fact that another sibling RMW has completed. 

But a Hermes replica does not possess a global `completed(R)` oracle. The actual RMW rule is local: compare the received timestamp with the local timestamp, ACK it if it is equal/higher, otherwise respond with the local INV. The supplied TLA model does exactly that and does not consult `committedRMWs` while handling an incoming RMW. 

This matters particularly in Ivy because an exported action's `require` is an environmental precondition: the abstract environment is required to call the action only when that condition holds. Ivy's documentation explicitly describes such abstract exported-action preconditions as assumptions about the allowed calls. ([Microsoft GitHub][3])

Therefore the proof is allowed to assume:

> “Don't ACK/process this RMW if doing so would conflict with a globally completed sibling.”

That is perilously close to assuming the RMW safety property you are trying to derive from Hermes.

It *might* be legitimate as a refinement abstraction if you separately prove that this global condition always follows from the concrete replica-local state. But there is no such refinement proof in this file. I would remove these `completed`/`rmw_conflict` guards from the concrete protocol actions and see whether the desired invariant can still be proved from timestamp processing alone.

### 4. The O3 fast path appears unreachable in the very case O3 is supposed to optimize

This is another concrete modeling bug.

The only places that create `ack_msg(n,s,t)` are invalidation-receive actions, and those actions require `n ~= s`. Thus the coordinator never generates

```ivy
ack_msg(c,c,t)
```

for its own update.  

But `o3_observe_quorum` requires:

```ivy
require live(A) -> ack_msg(A,c,t);
```

for every `A`. 

If coordinator `c` is live, instantiate `A = c`. You require:

```text
ack_msg(c,c,t)
```

which is unreachable.

Consequently I believe your current model implies:

```ivy
o3_quorum(N,C,T) -> ~live(C)
```

because an O3 quorum can only first be observed after coordinator `C` has ceased being live.

That is the opposite of the intended O3 scenario. The Hermes paper describes O3 specifically as a **failure-free** optimization in which followers broadcast their ACKs and can unblock reads after seeing all ACKs. ([Informatics Homepages][2])

The likely correction is that the coordinator's own contribution must be implicit, e.g. conceptually:

```ivy
require live(A) & A ~= c -> ack_msg(A,c,t)
```

or you must explicitly create the coordinator's self-ACK.

So the current safety proof barely exercises the intended O3 fast path at all.

### 5. Membership reconfiguration has been abstracted too aggressively

The TLA+ model has a protocol epoch, messages carry that epoch, receivers reject wrong-epoch messages, failures increment the epoch, and pending updates can be replayed in the new epoch.  

The Ivy model has none of this. Its `epoch`/`ready_epoch` state is only synthetic bookkeeping for the liveness proof; INV/ACK/VAL messages have no Hermes membership epoch at all. 

This means old ACKs survive a membership removal, old INVs can be delivered after a membership change, and a pending coordinator need not execute the new-epoch replay required by Hermes. The concrete RMW/write counterexample above comes directly from this omission.

At the system level, `fail(n)` also assumes that once a node is declared non-live it instantaneously stops all behavior. There is no modeling of leases, stale membership views, minority partitions, or the transient m-update period. Those are fundamental assumptions behind Hermes local reads. The paper relies on RM leases and epoch-tagged messages precisely so removed/minority nodes stop serving before a new membership can make progress. ([Informatics Homepages][2])

That abstraction can be valid if `fail` really means **“a safe RM reconfiguration has completed”**, rather than “node crashed.” But if that is the intended meaning, the missing epoch/ACK-reset/RMW-replay effects still need to be part of the action.

### 6. It isn't actually specifying linearizability

The comment calls this:

> “Linearizability safety for the one modeled key”

but the two invariants merely say that live replicas in `hs_valid` agree on timestamp and value. 

There is no read action in the Ivy interface at all, no client invocation/response history, no return values, and no real-time-before relation. 

And `local_rmw` accepts an arbitrary `v`; there is no relation such as

```text
v_new = f(v_old)
```

or CAS success/failure semantics.

So the model can establish a very useful **replica-state consistency invariant**, but that isn't formally the Herlihy/Wing linearizability property. The supplied TLA model has essentially the same limitation: `HRead` is only a stuttering transition enabled when the key is valid, and `HConsistent` checks replica timestamp agreement.  

### Where I would rate the model now

| Claim                                                               | Status                                                                            |
| ------------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| Valid live replicas agree                                           | **Real, useful safety proof**                                                     |
| Completed timestamp has reached/been surpassed by all live replicas | **Real internal invariant**                                                       |
| At most one same-base RMW completes                                 | **Proved only in a model with suspicious global conflict guards**                 |
| RMW vs concurrent write semantics                                   | **Not proved; Ivy admits a counterexample forbidden by `HermesRMWs.tla`**         |
| O3 failure-free fast path                                           | **Effectively unreachable as modeled**                                            |
| Linearizability                                                     | **Not formally specified as a history property**                                  |
| Write/RMW eventual completion                                       | **Not proved**                                                                    |
| Three explicit liveness properties                                  | **Vacuous**                                                                       |
| Reconfiguration/network-partition safety end-to-end                 | **Assumed via an abstract `live` oracle, and weaker than the supplied TLA model** |

Two very revealing litmus invariants to try immediately are:

```ivy
# I expect this to FAIL:
invariant completed(R) & ts_rmw(R) & parent(R,B) &
          completed(W) & ~ts_rmw(W) & parent(W,B) -> false

# I expect these to PASS:
invariant ~ack_msg(N,N,T)
invariant o3_quorum(N,C,T) -> ~live(C)
```

If Ivy behaves as I expect on those, they independently confirm the two largest safety/O3 holes above.

So my conclusion is stronger than merely “the model could use more detail”: **an `ivy_check` success on this particular file is currently not evidence that Hermes RMW+O3 is safe and live.** The safety proof contains useful pieces, but it misses at least one property the reference TLA+ model explicitly verifies, and the liveness portion presently proves nothing.

[1]: https://wcl.cs.rpi.edu/pilots/library/papers/fp/ivy_sas18.pdf?utm_source=chatgpt.com "Deductive Verification in Decidable Fragments with Ivy 3"
[2]: https://homepages.inf.ed.ac.uk/bgrot/pubs/HERMES_ASPLOS20.pdf "Hermes: a Fast, Fault-Tolerant and Linearizable Replication Protocol"
[3]: https://microsoft.github.io/ivy/examples/client_server_example.html?utm_source=chatgpt.com "Invariants"

