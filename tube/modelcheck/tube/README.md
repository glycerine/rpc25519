# Model checking Tube

[tube.tla](tube.tla) models the Tube implementation at `44d0b71d` with the
subsequent fix for session renewal after a local read. It started
from Specular run `20261122-010209-d9f6`, which analyzed `a66e9b9c`. The
reference module uses only the standard TLA+ modules. The
[coverage table](coverage.md) maps the 119 intervening commits to Go functions,
model actions and abstraction limits. Inherited line numbers and finding IDs in
the module refer to the historical run. Use the Go function names to navigate
the current implementation.

## Run a check

Set `TLA2TOOLS_JAR` to a local `tla2tools.jar`. A Specular installation includes
one in its `lib/` directory. From the repository root, run

```sh
cd tube/modelcheck/tube
python3 check.py quick packet-storage
python3 check.py --list
```

The first command explores a two-voter election and checks bounded packet,
snapshot and WAL scenarios. The runner uses a temporary directory for TLC state
and returns a failing exit status when a safety or progress check fails. Each
profile defines its actions, properties, bounds and any assumed scheduling.
[Profile definitions](checks/profiles.ini)

| To check | Run with `python3 check.py` |
| --- | --- |
| Replication, client writes and Raft safety across all interleavings within the two-voter bounds | `raft --workers 4 --heap 2g` |
| A committed write applied at both voters | `--witness raft` |
| Vote persistence, deferred campaigns and post-restart commitment | `restart-vote restart-delay singleton-wal post-restart-progress` |
| Membership acknowledgements, step-down and the startup unanimity deadline | `membership startup-unanimity` |
| Operator overrides, bootstrap and restart with a cleared configuration | `admin-cases bootstrap` |
| Plain and extra-ChaCha framing, circuit identity and redirected ticket outcomes | `transport transport-psk handshake identity redirect caller-timeout` |
| Deterministic KV apply, duplicate requests, CAS and leased tables | `kv kv-cases` |
| Session renewal when a local read is retried through the log after failover | `kv-session-diagnostic` |
| Czar grants, leadership handoff and Hermes recovery after message loss | `czar-contacts czar-handoff hermes-replay hermes-publication` |
| Sample three-voter schedules with reconfiguration, crash, restart, compaction and link failure | `raft-faults` |

Use `--show-config PROFILE` to see the exact TLC input. A witness check succeeds
only when TLC reaches the named condition. A profile with `simulation` samples
schedules. Other profiles exhaust their bounded state graphs or search for a
named witness. `packet-storage` starts from selected boundary states.
`post-restart-progress` starts from initialization, elects a singleton, crashes
and restarts it, and checks that it commits again.

For direct use with TLC or the Toolbox, [tube.cfg](tube.cfg) starts two
connected voters, permits one election, one heartbeat and one client request,
and checks the five classic Raft properties, WAL and quorum consistency,
and ticket and transport properties. The fault and reconfiguration budgets
are zero. Deadlock checking is disabled because a quiet cluster can exhaust
its timer budget. Run the full two-voter check with

```sh
java -XX:+UseParallelGC -Xmx2g -cp "$TLA2TOOLS_JAR" tlc2.TLC \
  -workers 4 -config tube.cfg tube.tla
```

## What each specification covers

| Entry point | Behavior and assumptions |
| --- | --- |
| `Spec`, `MCSpec` | Raft elections, quorum evidence, replication and compaction, multipart snapshots, PAR recovery, logless membership, client completion and circuit transport interact in one state machine. `MCSpec` adds timer, fault and resource bounds. |
| `MCSpecAdmin` | Adds forced membership changes, empty configuration, self-bootstrap and operator restart with `-zap`. Operator intervention can replace the ordinary voting configuration. |
| `KV_Spec`, `MCSpecKV` | Replicas receive one committed command order. Shipping, apply, snapshot restore, local reads and replies can occur at different times. The model has a representative key and a table rename destination, and models clocks in ticks. |
| `Cz_Spec`, `MCSpecCzar` | Czar and member clients use one linearizable leased CAS register. Requests, application, replies, direct pings, member grants, reports and expiration are separate steps. |
| `MCSpecHermes` | Czar publishes membership and grants to Hermes. Replicas consume them independently. The model covers one key, unicast acknowledgements and nonconflicting reads, writes and read-modify-write operations. |
| `LiveSpec`, `LiveSpecCzar` | Add explicit scheduling and time assumptions. Choose properties and bounds appropriate to the proposed progress guarantee. |

For progress checks, inspect the healthy-majority premise and the terminal
state or repeating cycle. Exhausted terms, circuits or queue capacity can
prevent recovery within a bounded model. The historical `VIEW MCView`
projection merges states with different remaining fault budgets. The supplied
safety profiles use full state.

## Replay the Go implementation

The collector exports the selected Git revision into a fresh directory, applies
the trace hooks there, and runs the instrumented Go scenarios with the
repository’s `synctest` build tag. The replay uses `tla2tools.jar` and
`CommunityModules-deps.jar` from the same Specular `lib/` directory.

```sh
# From tube/modelcheck/tube
export TLA_LIB="$(dirname "$TLA2TOOLS_JAR")"
trace_output="$(mktemp -d)/tube-run"
bash trace/collect.sh ../../.. 44d0b71d "$trace_output" cold_start_election
bash trace/replay.sh tube.tla "$trace_output/traces/cold_start_election.ndjson"
```

Omit `cold_start_election` from the collector command to run all twelve Go
scenarios. Each produces an NDJSON file in `traces/` and a Go log in `logs/`
under the output directory. Replay checks the recorded event order and
post-state against actions in `tube.tla`. Observations include term, vote, log
tail, applied and committed indexes, commit term, membership, circuit identity,
and the durable vote on events that report it. The trace adapter allows
unobserved transport and clock steps between logged events. For a later source
revision, rebase the hooks when an instrumented Go function changes.
[Trace adapter](trace/Trace.tla)

## Validation at `44d0b71d`

TLC matched 5,978 recorded events across all twelve Go scenarios at
`44d0b71d`. The trace fixtures stagger restored process starts by 20 ms. In
the interrupted-uncommitted-tail case, they heal the partition before restart
so the test observes local WAL recovery and the later leader overwrite. A
control trace with an altered commit term failed validation.

The full two-voter configuration in `tube.cfg` completed at depth 40 across
1,266,052 distinct states with all configured properties satisfied. The separate
administrative profile checked 940,460 distinct states at depth 29.
The Go regression checks covered 85 Tube, 25 RPC/transport and five Hermes
top-level tests. The modeled `Extends` result also agreed with
`TermsRLE.Extends` on 10,000 bounded leader/follower log pairs, including
compaction boundaries. The three-voter `raft-faults` profile sampled 300
schedules of 100 steps. The KV, czar and Hermes checks exercise their own
model interfaces. The implementation traces cover Raft and transport.

## Session renewal after failover

`python3 check.py kv-session-diagnostic` checks a leader that serves a session
read locally. After failover the client retries the same serial through the
log. When both replicas have applied the retry at index 3, the model checks
that each records serial 1, the deadline from the logged read, and a reply
produced by log application. Use `--witness` to see the schedule reach that state.
[TLC scenario](checks/SessionProjection.tla)

`leaderServedLocalRead` caches the original reply for client retries.
`appliedSessionDuplicate` reuses a cached reply during apply only when the
ticket is committed and carries no leader-local read timestamp. A fresh
lease read stamps its reply, and a fresh log execution clears that stamp.
One Go test checks that a replica can cache a local reply while the committed
entry for that serial still awaits application. A second test commits an
unrelated write past the original deadline and checks that all three replicas
retain the renewed session. Run both from the repository root.
[Commit and apply test](../../specular_test.go) and [failover test](../../client_test.go)

```sh
go test -tags synctest ./tube -run '^Test(LocalReplyForCommittedSerialStillNeedsLogApply|LocalReadRetryRenewsSessionOnEveryReplica)$' -count=1
```

## Extend the model

To add a property, create a sibling module. The retained logical log includes
the compacted prefix for these checks.

```tla
------------------------- MODULE TubeExtension -------------------------
EXTENDS tube

AppliedWithinLog == \A n \in Server : lastApplied[n] <= Len(log[n])
========================================================================
```

Copy `tube.cfg` to `extension.cfg`, add `AppliedWithinLog` to the
`INVARIANTS` list, then run

```sh
java -cp "$TLA2TOOLS_JAR" tlc2.TLC -config extension.cfg TubeExtension.tla
```

To experiment with a new Raft action in a sibling module, define
`MyCoreAction` over the existing `coreVars`. Assign or preserve every core
variable. The following composition lets TLC interleave that action with the
ordinary bounded protocol and keep the other layers at rest.

```tla
ExtensionNext == MCNext \/ (MyCoreAction /\ BaseOnly /\ NoFc)
ExtensionSpec == MCInit /\ [][ExtensionNext]_mcVars
```

Select `SPECIFICATION ExtensionSpec` in the copied configuration. Supply a
bound when the new action can create arbitrarily many states.

For a behavior change, edit the corresponding action in `tube.tla`. Connect
new actions to the relevant reference and bounded dispatchers, such as
`CoreNext` and `MCNext` for Raft. Include each new state variable in its tuple,
initialization, updates and unchanged clauses. Add a focused profile that
checks the property and reaches the changed path. [Update properties](Update.tla)
