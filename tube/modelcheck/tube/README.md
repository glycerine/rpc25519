# Model checking Tube

[tube.tla](tube.tla) models Tube at Go commit `a66e9b9c`. It consolidates
the Raft/transport, KV, czar and model-checking modules from Specular run
`20261122-010209-d9f6`. Source line references refer to that commit. Later
Go changes need corresponding model updates. The run's `Trace.tla` and
NDJSON traces serve a separate purpose of replaying recorded executions.
This model uses only the standard TLA+ modules.

## Run the safety check

From the repository root, with `TLA2TOOLS_JAR` set to the path of
`tla2tools.jar`, run

```sh
cd tube/modelcheck/tube
java -cp "$TLA2TOOLS_JAR" tlc2.TLC -config tube.cfg tube.tla
```

[tube.cfg](tube.cfg) explores two connected voters with budgets for one
election timeout, one heartbeat and one client write. Both voters can apply
the write. TLC checks election safety, log matching, leader completeness,
state machine safety, leader append-only behavior and structural invariants.
Crash, partition and reconfiguration budgets start at zero. A quiet cluster
can exhaust its timer budgets, so the configuration allows terminal states.

## Check a different behavior

Copy `tube.cfg`, choose the specification, and set the bounds and properties
for that behavior. Each entry point advances the subsystem in its row.

| TLC specification | Behavior | Example check |
| --- | --- | --- |
| `MCSpec` | Raft elections and replication over circuits, membership changes, snapshots and PAR recovery | `INVARIANT ElectionSafety` |
| `MCSpecKV` | KV replicas independently apply one committed log order, with leases and sessions | `INVARIANT KVDeterminism` |
| `MCSpecCzar` | Czar clients acquire and renew leases through a linearizable CAS register | `INVARIANT CzAtMostOneCzarBelief` |
| `LiveSpec` | Raft and transport with the run's fairness assumptions | `PROPERTY PeerConnectivityRestored` |
| `LiveSpecCzar` | Czar with fair client steps, CAS processing and clock ticks | `PROPERTY EventuallyCzar` |

Replace the default Raft checks when selecting KV or czar. Some diagnostic
properties have known counterexamples at the modeled revision. For liveness,
use exhaustive search and check the property's premises and any `BoundHit`
escape against the chosen bounds.

The original run's `spec/MC.cfg` and `spec/MC_hunt_*.cfg` also select these
entry points. To reuse one, pass its path to `-config` and use `tube.tla` as
the module. Add `-deadlock` for those archived configurations to allow
quiescence when the budgets run out. Some contain `VIEW MCView`, which
merges states with different remaining budgets. Omit that line to check
the full bounded state graph.

## Check an extension

For a new property, put a module next to `tube.tla`. For example,
`TubeExtension.tla` can check whether every applied index fits in the modeled
log, which retains its compacted prefix as history.

```tla
------------------------- MODULE TubeExtension -------------------------
EXTENDS tube

AppliedWithinLog == \A n \in Server : lastApplied[n] <= Len(log[n])
========================================================================
```

Copy `tube.cfg` to `extension.cfg`, add `AppliedWithinLog` to its
`INVARIANTS` list, and run

```sh
java -cp "$TLA2TOOLS_JAR" tlc2.TLC -config extension.cfg TubeExtension.tla
```

For a Raft behavior change, wire a new transition into both `CoreNext` and
the corresponding bounded dispatcher under `MCNext`. For new state, update
`CoreInit`, its variable tuple, and each action that updates or preserves
it. KV and czar have corresponding `KV_Next` / `MCKVNext` and
`Cz_Next` / `MCCzNext` pairs.
