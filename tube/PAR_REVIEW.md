# PAR implementation review

Reviewed 2026-10-17. Three confirmed findings: two recovery-progress failures and one first-publication crash-recovery failure. The reproductions below did not demonstrate committed-data loss, but they leave replicas unavailable despite recoverable conditions.

The numbered findings describe the implementation before the fixes. All three now have regression tests that failed before their respective fixes and passed afterward; see the TDD resolution below. Source links point to the corresponding current locations.

Scope: `par.go` and its integration with startup, elections, AppendEntries, membership admission, WAL replacement, application transactions, snapshots, and state persistence. The original review did not modify Go source files. The subsequent fixes add no recurring online auditing, migration support, or changes to YogaDB's API.

## 1. [P1] A cleanly truncated WAL enters recovery but never requests repair

Primary location: [parRecoveryStep](/Users/jaten/go/src/github.com/glycerine/rpc25519/tube/par.go:2312), originally the unconditional return when `s.wal.parInv == nil`.

If both the WAL and its sidecar lose the same complete suffix, the remaining files form an internally valid pair. [openPARWAL](/Users/jaten/go/src/github.com/glycerine/rpc25519/tube/par.go:2017) therefore uses the ordinary opener, leaving `parInv` nil. The saved commit/applied/configuration boundary can still prove that history is missing: [parAfterRestore](/Users/jaten/go/src/github.com/glycerine/rpc25519/tube/par.go:2178) correctly sets `active` and `unknown`.

However, every recovery step returns at the nil-inventory check, before reaching the snapshot-request path. Ordinary AppendEntries cannot repair the suffix either: [tube.go:7861](/Users/jaten/go/src/github.com/glycerine/rpc25519/tube/tube.go:7861) sends recovering replicas through `parObserveAE` and returns without accepting entries or acknowledging them. Consequently, a healthy connected leader can keep sending heartbeats indefinitely while the replica continues returning `ErrPARRecovering`.

**Reproduced:** start two healthy replicas from a three-member configuration, elect their leader, and bring up the third replica after truncating its WAL and identifiers to entry 1 while retaining its saved commitment through entry 3. After five seconds of simulated connected operation, the restarted node remained `FOLLOWER`, recognized `node_0` in term 2, and reported `commit=3`, `log=1`, and `PAR: log recovery in progress`. This is detectable loss with an intact independent boundary witness, not the inherently undetectable loss of every witness.

**Correction direction:** let metadata-detected missing history reach verified snapshot recovery even when the byte-level WAL scan was clean and no sparse inventory was created. Preserve the existing protected-floor and snapshot-admission checks.

**Missing regression:** a live startup test with a complete, valid shortened WAL/sidecar pair and a larger saved commit/applied boundary. Require eventual normal service under a stable connected leader with log compaction disabled.

## 2. [P1] A follower cannot recover a damaged obsolete, uncommitted suffix

Primary locations: [the leader-only discard branch](/Users/jaten/go/src/github.com/glycerine/rpc25519/tube/par.go:2412) and [the reply-triggered snapshot fallback](/Users/jaten/go/src/github.com/glycerine/rpc25519/tube/par.go:2568), which originally did not handle `parAbsent`.

A recovering follower may retain the checked identity of an uncommitted entry from a previous leader whose payload is damaged. The current leader can legitimately have a different entry at that index. Its peers then return `parAbsent` for the old identity. Above the protected floor, a majority of those replies permits `parDiscard`. The leader's newer configuration commitment can instead raise the floor over that index, producing `ErrPARConflict` for the obsolete identity. The permanent regression tests exposed this latter case too.

Keeping direct certificate-based truncation exclusive to the leader is sensible, but the original follower did nothing with either outcome. It neither asked for a replacement snapshot nor entered a path that could reconcile its log with the accepted leader. It continued querying for bytes that no other replica had. The snapshot fallback handled only `parCompacted`, while the recovery gate suppressed ordinary AppendEntries reconciliation.

**Reproduced:** two replicas contain committed entries 1–2. A third, initially down replica also has a damaged, uncommitted term-1 entry 3. Elect a healthy leader, which commits its term-2 no-op at index 3, then start and connect the damaged replica to both healthy nodes. After five simulated seconds it remained a follower of that leader with `commit=2`, `log=3`, `applied=0`, and `ErrPARRecovering`. No intact copy of its obsolete entry is needed to restore the committed prefix, yet recovery cannot finish.

**Correction direction:** provide a follower recovery route through the accepted leader's verified snapshot when the obsolete identity cannot be repaired, retaining the existing protected-boundary checks. Merely allowing followers to call `parDiscardSuffix` would bypass its intended leadership precondition.

**Missing regression:** the follower counterpart of `TestPar_live_uncommitted_suffix_discard`. Keep the healthy leader stable and compaction disabled, replace the old suffix with a different current-term entry, and require the follower to resume service without applying the obsolete command. The existing test elects the damaged node as leader, so it exercises the branch that already works.

## 3. [P2] Interruption of the first state save makes a fresh node unable to restart

Primary locations: [first state publication](/Users/jaten/go/src/github.com/glycerine/rpc25519/tube/persistor.go:245), which publishes voting metadata before the state file, and [startup selection](/Users/jaten/go/src/github.com/glycerine/rpc25519/tube/persistor.go:109), which originally rejected an empty state file whenever voting metadata existed.

The persistor constructor creates an empty state file. On the first `save`, `parSaveLocalMeta` durably publishes both metadata copies, then application attachment/checkpoint publication occurs, and only afterward is the new state file renamed into place. A crash between those publications leaves an empty state file alongside valid local metadata. Every subsequent startup returns `ErrPAREvidence` at the empty-state check, before the node can enter PAR recovery.

**Reproduced:** create a fresh disk-backed node and inject an error immediately before the first rename to `saver.path`, allowing the metadata publications to complete normally. Reopening through the integrated recovery opener fails with `PAR: insufficient trustworthy evidence`. A crash at the same publication boundary has the same selected state; unpublished temporary state files are not considered by the opener. No storage corruption or upgrade is involved.

**Correction direction:** make first publication recoverable from its own checked local evidence, or coordinate the initial state/metadata publication so this ordinary crash window cannot produce an unreopenable store. Preserve fail-closed behavior for genuinely lost voting history; accepting arbitrary empty state files would be unsafe.

**Missing regression:** exercise first-ever publication cuts, including before/after the first and second metadata copies and before/after initial state-file rename. Existing metadata/state publication tests establish a prior successful save before injecting failures, so their old-state fallback does not cover initialization.

## Verification and review limits

The temporary probe source is [probes_test.go](/private/tmp/par-review-PfXNdI/probes_test.go). It was added to the test build using a Go overlay, without changing repository Go files. Each live scenario uses its own synctest bubble. Failed fixtures remain in local `tube/test.par.*` directories for inspection, following the existing test helper's policy.

The current `par_test.go` has the `par` build tag, so these commands explicitly include it:

```sh
GOCACHE=/private/tmp/rpc25519-go-build go test -tags 'par synctest' \
  -overlay /private/tmp/par-review-PfXNdI/overlay.json ./tube \
  -run '^TestParReview_' -count=1 -timeout=90s

GOCACHE=/private/tmp/rpc25519-go-build go test -tags 'par synctest' ./tube \
  -run '^TestPar_(live_nodes_recover_before_command|local_metadata_publication_IO_failures|state_publication_IO_failures)$' \
  -count=1 -timeout=120s
```

The obsolete-suffix and first-save probes failed with the conditions described above. The initial clean-prefix probe stopped at a fixture reconnection error; after changing it to start the affected node offline and explicitly connect it after startup, its focused rerun reproduced finding 1. That corrected source is the version retained in the overlay. Logs: [first probe run](/private/tmp/par-review-PfXNdI/probes.log), [corrected prefix probe](/private/tmp/par-review-PfXNdI/prefix.log).

The three existing control tests passed together in 8.957 seconds ([log](/private/tmp/par-review-PfXNdI/controls.log)). They establish that the existing recovery and later-publication cases still work in the same environment; they do not cover the newly identified cases. No full-suite rerun or formal consensus proof was performed for this review.

## TDD resolution — 2026-10-17

Each finding was handled separately: add its permanent regression, observe the expected failure, then implement the fix and rerun it with related existing checks.

1. **Clean suffix loss:** [TestPar_live_clean_suffix_loss_recovers_from_saved_boundary](/Users/jaten/go/src/github.com/glycerine/rpc25519/tube/par_test.go:3717) failed with a connected follower stuck at `commit=3`, `log=1`, and `ErrPARRecovering` ([red log](/private/tmp/par-tdd-ruVxZe/1-red.log)). Recovery now requests a verified snapshot when the saved boundary proves missing history even though there is no sparse WAL inventory. The test verifies restored service, the snapshot boundary, and all three committed rows at version 1. It and the snapshot-admission/configuration-fence regressions passed with `-race` ([green log](/private/tmp/par-tdd-ruVxZe/1-green.log)).

2. **Obsolete follower suffix:** [TestPar_live_follower_replaces_damaged_uncommitted_suffix](/Users/jaten/go/src/github.com/glycerine/rpc25519/tube/par_test.go:4691) failed for both damaged payload and damaged identifier cases ([red log](/private/tmp/par-tdd-ruVxZe/2-red.log)). A matching, current recovery reply from the accepted leader now requests its verified snapshot on `parAbsent` as well as `parCompacted`. Waiting for `parDiscard` alone would miss the raised-protected-floor case described above. Followers still cannot perform certificate-based suffix discard. Both variants and the existing leader-discard/admission checks passed with `-race`; the new tests verify the committed prefix is preserved and the obsolete command is absent ([green log](/private/tmp/par-tdd-ruVxZe/2-green.log)).

3. **First state publication:** [TestPar_first_state_publication_IO_failures](/Users/jaten/go/src/github.com/glycerine/rpc25519/tube/par_test.go:1903) failed at 12 before/after publication cuts with `ErrPAREvidence` on restart ([red log](/private/tmp/par-tdd-ruVxZe/3-red.log)). Before publishing either metadata copy, initialization now stages a checksummed portable initial state at `.pre_rename.initial`. If the selected state file is still empty, startup validates that image, merges the local voting metadata, restores the application, and publishes the ordinary state file. The staging image survives an interrupted recovery and is removed after publication is durable. Successful initialization leaves the ordinary database-backed state format intact. This is temporary evidence for the first save; normal subsequent saves do not create it, and healthy startup without it adds no directory syncs.

   The test discovers the publication operations and injects an error immediately before and after each sync/rename, copies the crash image before closing its source, then reopens twice. It checks the vote, configuration, application value/version, and staging-file cleanup. [TestPar_initial_state_requires_checked_staging](/Users/jaten/go/src/github.com/glycerine/rpc25519/tube/par_test.go:2011) additionally rejects missing/checksum-invalid/foreign/nonportable/conflicting images and loss of the main state after completed initialization, without changing the surviving state or voting files. The first-publication, existing application-crash, and live application-recovery checks passed ([green log](/private/tmp/par-tdd-ruVxZe/3-green.log)); the first-publication, evidence-rejection, metadata, and persistor checks also passed with `-race` in 16.349 seconds ([race log](/private/tmp/par-tdd-ruVxZe/3-race.log)).

All new subtests create their own synctest bubble. Fixtures use local `parTestDir` directories retained on failure. Production changes are confined to `par.go` and `persistor.go`; regression tests are in `par_test.go`.

Final regression: the full `tube` package suite passed with the race detector and both build tags, including 232 passing top-level tests, in 475.208 seconds. No races were reported. [Full test log](/private/tmp/par-tdd-ruVxZe/full-tube-race.log).

```sh
GOCACHE=/private/tmp/rpc25519-go-build go test -v -race -tags 'par synctest' ./tube \
  -count=1 -timeout=20m
```

`gofmt -l` reported no formatting changes for the three modified Go files. Validation covers deterministic crash images, injected I/O failures, and simulated cluster behavior; it is not a physical power-loss test or formal consensus proof.
