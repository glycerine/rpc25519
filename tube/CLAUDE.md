# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Tube** is a production-grade Raft consensus implementation in Go, located in the `rpc25519/tube/` subdirectory of the rpc25519 project. It implements Diego Ongaro's Raft algorithm with pre-voting, sticky-leader, leader read-leases, log compaction (ch. 5), client sessions for linearizability (ch. 6), and the Mongo-Raft-Reconfig algorithm for membership changes (ch. 4). It includes a built-in sorted key-value store as its replicated state machine. The values stored under a key can be written or, more commonly, CAS-ed to obtain a time-limited exclusive lease on a key. Under faultless operation the leasing czar (e.g. leader of a hermes set of replicas) regularly renews its lease to maintain leadership (czar-ship). If or when the czar fails, all other members of the hermes replicas will content to be czar. For safety correctness, the Raft implementation must guarantee only one at a time obtains the lease in its key-value state machine, similar to how in Raft only a single leader per term may be elected.

**Module:** `github.com/glycerine/rpc25519` (root go.mod, requires Go 1.24.3+)

## Build Commands

From `tube/`:
```bash
make all              # Install all tube CLI tools (tube, tup, tubels, tuberm, tubeadd, member, hermes)
```

From project root (`rpc25519/`):
```bash
make all              # Build parent-level tools (cli, srv, selfy, samesame, jcp, jsrv) + go generate
make githash          # Regenerate gitcommit.go with current hash/tag/branch
```

Serialization code generation uses greenpack (`//go:generate greenpack` annotations).

## Testing

```bash
# Standard tests (from tube/) with and without race detector, should
use the synctest framework to allow in memory simulation of a network.

GOEXPERIMENT=synctest go test -v
GOEXPERIMENT=synctest go test -v -race
GOEXPERIMENT=synctest go test -v -run TestXXX   # Single simnet test

# From project root
make test              # Tests rpc25519, jsync, jcdc, bytes packages
make synctest          # Deterministic simnet tests at root level
```

Tests use `testing/synctest` and an embedded gosimnet network simulator (`simnet.go`) for deterministic simulation testing (DST). Linearizability is verified with the porcupine library (`porc_test.go`). Model checking specs exist in `modelcheck/` (TLA+, Spin, Ivy).

## Architecture

### Core Files (tube/)

- **tube.go** (~18K lines) — Central Raft node implementation (`TubeNode` struct). All core Raft logic (elections, log replication, pre-voting, sticky-leader, leader leases) deliberately kept in one file.
- **admin.go** — Client-facing APIs and cluster administration. Configuration uses Zygo Lisp (zygomys).
- **wal.go** — Write-ahead log persistence using msgp/greenpack serialization with Blake3 hash verification. Supports disk and in-memory (`nodisk`) modes.
- **actions.go** — FSM operations: Read, Write, CAS, ReadKeyRange, DeleteKey, ShowKeys, MakeTable, DeleteTable, RenameTable. Key versioning for linearizability.
- **czar.go** + **czar_gen.go** — Membership management. The Czar is elected via Raft to maintain the ReliableMembershipList with lease-based member detection.
- **persistor.go** — Persistent state (CurrentTerm, VotedFor) storage and recovery.
- **rle.go** — Run-length encoding of term history (`TermsRLE`) for efficient log communication.
- **kv.go** + **kv_gen.go** — Key-value store with multiple sorted tables backed by ART (Adaptive Radix Tree in `art/`).
- **hermes/** — this sub-packate is a client of tube. Hermes provides an in-memory key-value store replication protocol. Inspired by hardward cache corherency protocols, hermes allows both local reads and concurrent writes from any node initiating a write.

### key components of Raft: the default/included Raft state-machine is a key/value store that uses an Adaptive Radix Tree from `rpc25519/tube/art/`, and the `art/leaf.go` file therein contains the important variables for the leasing of keys (which is critical for the czar election in `tube/hermes`).

### Network & RPC

Built on the parent `rpc25519` package which provides ed25519-authenticated RPC. Circuit management handles peer connections. Message types include AppendEntries, RequestVote, PreVote, InstallSnapshot, etc.

### CLI Tools (tube/cmd/)

| Tool | Purpose |
|------|---------|
| `tube` | Main Raft replica server |
| `tup` | Interactive CLI for key-value operations |
| `tubels` | List cluster membership |
| `tubeadd` | Add node to cluster |
| `tuberm` | Remove node from cluster |
| `member` | Demonstrates the reliable group membership capabilities of rpc25519/tube/czar.go; uses leases on Raft keys |
| `hermes` | Hermes in-memory key-value replica server (builds on relaible group membership czar.go functionality) |

### Key Design Decisions

- **Pre-voting always on** — Prevents zombie node disruption; enables leader read-leases for fast local reads.
- **Mongo-Raft-Reconfig** — Membership kept separate from Raft log ("logless"); formally proven safe. Single-server-at-a-time (SSAAT) changes only.
- **greenpack serialization** — MessagePack-based code generation (`_gen.go` files). Run `go generate` to regenerate.
- **Deterministic simulation** — `GOEXPERIMENT=synctest` enables reproducible distributed system testing via Master Event Queue pattern.

## Code Generation

Files ending in `_gen.go` are auto-generated by greenpack. Do not edit them by hand. To regenerate after modifying structs with `//go:generate greenpack` annotations:
```bash
go generate ./...
```
