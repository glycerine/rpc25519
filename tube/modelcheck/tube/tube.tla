-------------------------------- MODULE tube --------------------------------
\* Tube protocol model, revised against glycerine/rpc25519 at 44d0b71d.
\* Specular run 20261122-010209-d9f6 at a66e9b9c supplied the original
\* model. S and D numbers and inherited numeric line references locate that
\* run. Named Go functions on revised actions locate the current behavior.
\*
\* tube.cfg selects MCSpec for a bounded Raft/transport safety check.
\* MCSpecAdmin also enables operator overrides, configuration reset and bootstrap.
\* MCSpecHermes joins the czar client to replica views, leases and one Hermes key.
\* MCSpecKV models a replicated KV store over a single committed order.
\* MCSpecCzar models the czar client over a linearizable CAS register.
\* LiveSpec and LiveSpecCzar add the run's scheduling assumptions.
\* Init, Next, vars and Spec expose the Raft/transport behavior without
\* the MC fault and timer budgets. See README.md for an extension example.

EXTENDS Integers, Sequences, FiniteSets, TLC

\* ========================================================================
\* Raft, WAL, snapshots, PAR, membership and circuit transport (S1-S9)
\* ========================================================================
\* One event-loop action can modify both consensus and transport state.
\* Peer names persist across restarts and PeerIDs include the incarnation.
\* Go panics are recorded in the ghost set panics. The affected action skips
\* the panicking operation. Crash is a separate transition.

\* ========================================================================
\* CONSTANTS
\* ========================================================================

CONSTANTS
    Server,           \* replica process names (PeerName); stable across restarts
    InitMembers,      \* initial MemberConfig (MC) members
    InitShadows,      \* initial shadow replicas (non-voting, receive AEs)
    Addrs,            \* possible listen addresses of a node
    InitAddr,         \* initial listen address of every node (element of Addrs)
    MaxInc,           \* incarnations per node are 0..MaxInc (PeerID per process start)
    SendBufCap,       \* abstract per-direction capacity for the send channel and kernel buffers
    AEBatchCap,       \* max entries per catch-up AE (abstracts the 300 KB cap, tube/tube.go:9757-9768)
    SnapParts,        \* 1: state snapshots fit one frame; 2: two-part (multipart) snapshots
    InitMeshed,       \* TRUE: Init starts from a full mesh of open replica circuits
    TimelyHeartbeats, \* TRUE: partial-synchrony assumption (see ElectionTimerMayFire)
    Nil

ASSUME InitMembers \subseteq Server /\ InitShadows \subseteq Server
ASSUME InitMembers \cap InitShadows = {}
ASSUME InitAddr \in Addrs
ASSUME MaxInc \in Nat /\ SendBufCap \in Nat \ {0} /\ AEBatchCap \in Nat \ {0}
ASSUME SnapParts \in {1, 2}
ASSUME InitMeshed \in BOOLEAN /\ TimelyHeartbeats \in BOOLEAN

\* ------------------------------------------------------------------------
\* Value names
\* ------------------------------------------------------------------------
Follower  == "Follower"
Candidate == "Candidate"
Leader    == "Leader"

\* circuit endpoint states at one side:
\*   "new"    created by the transport, not yet registered by Tube's event loop
\*            (sits in NewCircuitCh, tube/tube.go:2006-2010)
\*   "open"   registered by handleNewCircuit; per-circuit reader running
\*   "closed" halted (Close, EndCircuit, connection death); reader exit pending
\*   "gone"   reader exit processed (cleanupGoneCktP + deleteFromCktAll) or
\*            the owning process died
EpDialWait == "awaiting-setup-ack"
EpNew == "new"
EpOpen == "open"
EpClosed == "closed"
EpGone == "gone"

\* cktAllByName entries: 0 = absent, -1 = pending cktPlus (ckt == nil), c > 0 = circuit
NoCkt == 0
Pending == -1

Incs == 0..MaxInc
Keys == Server \X Incs                 \* PeerID abstracted as <<PeerName, incarnation>>
KName(k) == k[1]
KInc(k) == k[2]

Max(a, b) == IF a >= b THEN a ELSE b
Min(a, b) == IF a <= b THEN a ELSE b
MaxSet(S) == CHOOSE x \in S : \A y \in S : x >= y
MinSet(S) == CHOOSE x \in S : \A y \in S : x <= y

\* fixed total order on Server (TLA+ model values are not ordered)
Ord == CHOOSE f \in [Server -> 1..Cardinality(Server)] :
          \A x, y \in Server : x /= y => f[x] /= f[y]

\* ========================================================================
\* VARIABLES
\* ========================================================================

\* --- processes and the environment -------------------------------------
VARIABLES
    up,          \* up[n]: process is alive
    inc,         \* inc[n]: incarnation; PeerID of the running process is <<n, inc[n]>>
    addr,        \* addr[n]: current listen (BaseServer) address
    stopped,     \* stopped[n]: process alive but not running (SIGSTOP/wedged); kernel still ACKs
    link         \* link[x][y] \in {"ok","drop"}: directional reachability x -> y

\* --- rpc25519 transport ---------------------------------------------------
VARIABLES
    conn,        \* conn[k]: TCP connection record (dialer d, acceptor a), see ConnRec
    ckt,         \* ckt[c]: circuit record (multiplexed on a connection), see CktRec
    nextConn,    \* next connection id
    nextCkt,     \* next circuit id (crypto-random CallID in the code)
    blk          \* blk[n]: outgoing send continuation and deferred leader save

\* --- Tube connection views (all volatile, per process) ---------------------
VARIABLES
    knownPeers,  \* locally remembered PeerDetails retained after removal
    cktall,      \* cktall[n][key] \in circuit ids \cup {NoCkt}   (s.cktall map[PeerID])
    byName,      \* byName[n][m] \in circuit ids \cup {NoCkt, Pending} (s.cktAllByName)
    pendAddr,    \* pendAddr[n][m]: address stored in a pending cktPlus (PeerBaseServerAddr)
    wdReq,       \* explicit reconnect request or a failed pending watchdog's retry address
    watchSilent, \* IDs of selected circuits whose last observation expired at followerDownDur
    observer,    \* one representative external membership observer per node (when enabled)
    cktReplica,  \* cktReplica[n] \subseteq Keys (s.cktReplica map[PeerID])
    audit        \* audit[n]: circuits registered by handleNewCircuit (s.cktAuditByCID)

\* --- Raft volatile state ----------------------------------------------------
VARIABLES
    role,            \* Follower / Candidate / Leader
    currentTerm,     \* s.state.CurrentTerm (in memory)
    votedFor,        \* s.state.VotedFor (a key) or Nil
    leaderKey,       \* s.leaderID (key) or Nil
    sticky,          \* lastLegitAppendEntries within MinElection+ClockDriftBound (abstract time)
    campaign,        \* Per-process restart deadline, deferred request and post-fsync continuation.
    pvTerm,          \* s.preVoteTerm (phase 1), 0 if none
    pv2Term,         \* s.preVoteOkLeaderElecTerm (phase-2 timer armed), 0 if none
    pvYes, pvNo,     \* pre-votes recorded by voter name (s.preVotes)
    vYes, vNo,       \* votes recorded by voter name (s.votes)
    peers,           \* peers[n][key]: RaftNodeInfo [mi, lcri, credit] or Nil (s.peers)
    pong,            \* pong[n] = [all, fresh]: leaderFullPongPQ entries by PeerID
    leaderAged,      \* leader for at least followerDownDur (abstract time)
    startupPhase,    \* process start, NoFaultTolDur deadline, one-shot ACK commit check
    noop0,           \* noop0[n] = [id, done]: initialNoop0Tkt / initialNoop0HasCommitted
    stallQ,          \* FIFO of membership tickets waiting for the admission guard
    mcPending,       \* tickets awaiting commitment, per-circuit retries, deadline and client outcomes
    config,          \* s.state.MC
    shadows,         \* s.state.ShadowReplicas names
    commitIndex, commitIndexTerm,  \* RaftState.CommitIndex and CommitIndexEntryTerm
    lastApplied,     \* s.state.LastApplied (durable with the KV checkpoint)
    lastAppliedTerm, \* s.state.LastAppliedTerm
    snapSlot         \* ordered pending (circuit, checksum) assemblies at each node, oldest first

\* --- WAL and PAR -------------------------------------------------------------
VARIABLES
    log,         \* log[n]: Seq of [term, id, kind]; FULL history (entries <= base are compacted:
                 \* not visible to the code, kept for the invariants; after a snapshot install
                 \* the prefix is the leader's history)
    base,        \* base[n] = [idx, term]: CompactionDiscardedLast (wal BaseC/CompactTerm)
    idLen,       \* PAR sidecar: number of entries with published identifiers
    torn,        \* torn[n]: intact WAL bytes beyond the open recovery handle, in index order
    par          \* "unknown" fences elections. "ready" has checked identities and awaits WAL reopen.

\* --- durable RaftState (persistor.go save: temp file + fsync + rename) ---------
VARIABLES
    dTerm, dVote, dCfg, dShadows, dCommit, dCommitTerm, dKnownPeers,
    appendWitness, \* [current, saved] DurableAppendHigh at WAL sync and at RaftState save
    walGone      \* WAL and .parlog missing while RaftState exists (openPARWAL,
                 \* par.go:2016-2034; scanParWAL 1390-1410): set by the "wal"
                 \* storage fault, cleared when a PAR snapshot install recreates
                 \* both files (parPrepareSnapshotWAL, tube.go:17792-17797)

\* --- ghosts --------------------------------------------------------------------
VARIABLES
    adminEvents, \* processed recovery requests and their local outcomes
    nextId,      \* fresh log entry ids
    committed,   \* entries a leader declared committed (coded commit rule): [idx, term, id]
    applied,     \* applied[n]: Seq of [id, term] applied to the state machine
    leaders,     \* history of becomeLeader: [term, name, inc]
    ackedCfg,    \* membership changes acknowledged to the client: [ct, cv, op, x]
    cfgHist,     \* every installed config: [ct, cv, mem]
    panics,      \* reachable panic sites (strings)
    ackHigh,     \* ackHigh[n] = [term, claims]: entries n acked (accept) in that term
    clientTickets,  \* ordinary write submissions and their client-visible outcomes
    nextSid      \* fresh snapshot ids (whole-payload checksum in the code)

envVars   == <<up, inc, addr, stopped, link>>
netVars   == <<conn, ckt, nextConn, nextCkt, blk>>
viewVars  == <<knownPeers, cktall, byName, pendAddr, wdReq, watchSilent, observer, cktReplica, audit>>
raftWithoutCampaign == <<role, currentTerm, votedFor, leaderKey, sticky, pvTerm, pv2Term,
               pvYes, pvNo, vYes, vNo, peers, pong, leaderAged, startupPhase, noop0, stallQ,
               config, shadows, commitIndex, commitIndexTerm, lastApplied, lastAppliedTerm, snapSlot>>
raftProtocolVars == <<raftWithoutCampaign, campaign>>
raftVars == <<raftProtocolVars, mcPending>>
logVars   == <<log, base, idLen, torn, par>>
durVars   == <<dKnownPeers, dTerm, dVote, dCfg, dShadows, dCommit, dCommitTerm, appendWitness, walGone>>
raftHistoryVars == <<adminEvents, nextId, committed, applied, leaders, ackedCfg, cfgHist, panics, ackHigh, nextSid>>
ghostVars == <<raftHistoryVars, clientTickets>>

coreVars == <<envVars, netVars, viewVars, raftVars, logVars, durVars, ghostVars>>

\* ========================================================================
\* RECORD SHAPES (documentation; used by TypeOK)
\* ========================================================================

\* PeerDetail in MemberConfig.PeerNames: PeerID (incarnation, -1 = unknown /
\* "pending" URL) and Addr.
Det == [inc: Incs \cup {-1}, addr: Addrs \cup {"none"}]

\* MemberConfig (tube/tube.go:13651-13709): version (ConfigTerm, ConfigVersion),
\* members with details, IsCommitted, CommitIndex.
CfgRec == [mem: SUBSET Server, ct: Nat, cv: Nat, com: BOOLEAN, ci: Nat, cit: Nat,
           det: [Server -> Det \cup {Nil}]]

\* RaftNodeInfo kept by a leader per PeerID (tube/tube.go:9952-9996)
PeerRec == [mi: Nat, lcri: Nat, credit: BOOLEAN, heard: Nat, recentAck: BOOLEAN, ping: Nat, pingFresh: BOOLEAN]

\* ========================================================================
\* MEMBER CONFIG HELPERS
\* ========================================================================

\* MemberConfig.VersionGT orders version numbers. VersionEqual also checks voter names.
VersionGT(a, b)  == a.ct > b.ct \/ (a.ct = b.ct /\ a.cv > b.cv)
SameVersionNumber(a, b) == a.ct = b.ct /\ a.cv = b.cv
VersionEq(a, b)  == SameVersionNumber(a, b) /\ a.mem = b.mem
ConfigConflicts(a,b) == SameVersionNumber(a,b) /\ a.mem /= b.mem
VersionGTE(a, b) == VersionGT(a, b) \/ SameVersionNumber(a, b)

\* MemberConfig.majority(): 0 for an empty config (tube.go, majority())
CfgMajority(c) == IF c.mem = {} THEN 0 ELSE Cardinality(c.mem) \div 2 + 1

\* TubeNode.Start, quorum, quorumAndIsNoFaultTolDur, advanceCheckForced.
\* Override with the set of replicas configured with positive NoFaultTolDur.
\* The interval restarts on each process start. At exact deadline quorum()
\* already uses a majority, but advanceCheckForced waits until strictly after.
NoFaultTolServers == {}
ASSUME NoFaultTolServers \subseteq Server
StartQuorumPhase(n) == IF n \in NoFaultTolServers THEN "unanimous" ELSE "majority"
RequiredVotes(members, phase) ==
    IF phase = "unanimous" THEN Cardinality(members) ELSE Cardinality(members) \div 2 + 1
ClusterSize(n) == Cardinality(config[n].mem)
Quorum(n) == RequiredVotes(config[n].mem, startupPhase[n])

IsMember(n) == n \in config[n].mem                    \* weAreMemberOfCurrentMC (17943)
IsShadow(n) == n \in shadows[n] /\ n \notin config[n].mem   \* amShadowReplica (17977)

\* quorumsOverlap (tube/tube.go:17280-17317)
QuorumsOverlap(c0, c1) ==
    LET need0 == CfgMajority(c0)
        need1 == CfgMajority(c1)
        inter == c0.mem \cap c1.mem
        unionSz == Cardinality(c0.mem) + Cardinality(c1.mem) - Cardinality(inter)
    IN IF need0 = 0 \/ need1 = 0 THEN TRUE ELSE (need0 + need1) > unionSz

\* ========================================================================
\* LOG HELPERS (full-history log; the code sees only entries > base)
\* ========================================================================

LastIdxL(lg) == Len(lg)
LastTermL(lg, bs) ==
    IF Len(lg) = 0 THEN 0
    ELSE IF Len(lg) = bs.idx THEN bs.term
    ELSE lg[Len(lg)].term

LastIdx(n)  == Len(log[n])
LastTerm(n) == LastTermL(log[n], base[n])

\* getRaftLogSummary (tube/tube.go) reports only physical entries. An
\* installed snapshot alone therefore reports 0/0 in an acknowledgement.

PhysLastIdx(lg, bs)  == IF Len(lg) > bs.idx THEN Len(lg) ELSE 0
PhysLastTerm(lg, bs) == IF Len(lg) > bs.idx THEN lg[Len(lg)].term ELSE 0

\* term at index i as the code can see it (i >= base.idx)
TermAtL(lg, bs, i) == IF i = 0 THEN 0 ELSE IF i = bs.idx THEN bs.term ELSE lg[i].term

VisibleTerm(lg, bs, i) ==
    IF i < bs.idx \/ i > Len(lg) THEN -1 ELSE TermAtL(lg, bs, i)

Ids(lg) == [i \in 1..Len(lg) |-> lg[i].id]

\* TermsRLE of a node's WAL (tube/rle.go): compaction base, CompactTerm and
\* the terms of the physical entries (equivalent to the run-length form).
RLEOf(lg, bs) == [b |-> bs.idx, bt |-> bs.term,
                  t |-> [j \in 1..(Len(lg) - bs.idx) |-> lg[bs.idx + j].term]]

EndiR(X)     == X.b + Len(X.t)
TermR(X, i)  == X.t[i - X.b]                    \* X.b < i <= EndiR(X)
LastTermR(X) == IF Len(X.t) = 0 THEN X.bt ELSE X.t[Len(X.t)]

ERes(e, l, s) == [ext |-> e, lcri |-> l, snap |-> s]

\* rle.go:723-744 (and 836-857): pull both term iterators from beginOverlap;
\* the first term mismatch at idxF returns (false, idxF-1); otherwise the
\* shorter side ends the scan and the result is (L.Endi >= F.Endi, min end).
OverlapScan(L, F, bo) ==
    LET eo  == Min(EndiR(F), EndiR(L))
        div == {i \in bo..eo : TermR(F, i) /= TermR(L, i)}
    IN IF div /= {} THEN ERes(FALSE, MinSet(div) - 1, FALSE)
       ELSE ERes(EndiR(L) >= EndiR(F), eo, FALSE)

\* rle.go:748-858 extends_aware_but_no_compaction_here (both bases 0)
ExtendsNoCompaction(L, F) ==
    IF Len(L.t) = 0 THEN (IF Len(F.t) = 0 THEN ERes(TRUE, 0, FALSE)
                          ELSE ERes(FALSE, 0, FALSE))              \* 759-766
    ELSE IF Len(F.t) = 0 THEN ERes(TRUE, 0, FALSE)                  \* 768-770
    ELSE OverlapScan(L, F, 1)                                      \* 810-857

\* tube/rle.go, TermsRLE.Extends. A leader with physical entries must
\* establish the term at its compacted boundary when that index is still
\* present in the follower's physical tail (50500547).

Extends(L, F) ==
    IF F.b = 0 /\ Len(F.t) = 0 THEN ERes(TRUE, 0, FALSE)             \* 460-463
    ELSE IF Len(L.t) = 0 THEN                                        \* 472
        IF L.b = 0 THEN ERes(FALSE, 0, FALSE)                          \* 473-476
        ELSE IF F.b = L.b THEN
             (IF Len(F.t) = 0 THEN ERes(TRUE, L.b, FALSE)              \* 479-482
              ELSE ERes(FALSE, L.b, FALSE))                            \* 485
        ELSE IF F.b > L.b THEN ERes(FALSE, L.b, FALSE)                 \* 489-492
        ELSE ERes(FALSE, F.b, TRUE)                                    \* 502 NEED_LEADER_SNAPSHOT
    ELSE IF L.b > F.b /\ EndiR(F) >= L.b /\ TermR(F, L.b) /= L.bt
         THEN ERes(FALSE, F.b, TRUE)
    ELSE IF L.b = 0 /\ F.b = 0 THEN ExtendsNoCompaction(L, F)         \* 519-522
    ELSE IF EndiR(F) <= L.b THEN                                     \* 533
        IF EndiR(F) > F.b THEN                                         \* 540
            (IF EndiR(F) = L.b /\ LastTermR(F) = L.bt
             THEN ERes(TRUE, EndiR(F), FALSE)                          \* 544-546
             ELSE ERes(FALSE, F.b, TRUE))                              \* 551
        ELSE IF F.b < L.b THEN ERes(FALSE, F.b, TRUE)                  \* 554-555
        ELSE IF F.bt = L.bt THEN ERes(TRUE, F.b, FALSE)                \* 561-562
        ELSE ERes(FALSE, F.b, FALSE)                                   \* 564
    ELSE IF Len(F.t) = 0 THEN ERes(TRUE, F.b, FALSE)                  \* 587-603
    ELSE IF EndiR(L) <= F.b THEN ERes(FALSE, EndiR(L), FALSE)         \* 621-623
    ELSE OverlapScan(L, F, Max(F.b, L.b) + 1)                         \* 641-744

\* ------------------------------------------------------------------------
\* tube/tube.go, logsAreMismatched. The WAL iterator yields the compacted
\* boundary exactly once, then consecutive physical indices (rle.go).

LAMRes(r, ct, cti) == [rej |-> r, ct |-> ct, cti |-> cti]

LogsAreMismatched(lg, bs, ae) ==
    LET nes   == Len(ae.ents)
        lli   == Len(lg)
        baseC == bs.idx
    IN
    IF nes = 0 THEN LAMRes(FALSE, -1, -1)                              \* 7578-7581
    ELSE IF ae.prev <= 0 THEN LAMRes(FALSE, -1, -1)                    \* 7584-7587
    ELSE IF lli < ae.prev THEN LAMRes(TRUE, -1, -1)                    \* 7593-7595
    ELSE IF baseC > 0 /\ ae.prev <= baseC THEN                         \* 7604-7608
        IF ae.prev = baseC THEN
            (IF ae.prevTerm /= bs.term THEN LAMRes(TRUE, ae.prevTerm, ae.prev)  \* 7611-7613
             ELSE LAMRes(FALSE, -1, -1))                               \* 7618
        ELSE
          LET e0 == ae.ents[1].idx
              eL == ae.ents[nes].idx
          IN IF e0 > lli + 1 THEN LAMRes(TRUE, -1, -1)                 \* 7633-7636
             ELSE IF eL < baseC THEN LAMRes(FALSE, -1, -1)              \* 7637-7640
             ELSE
               \* 7649-7685: walk follower indices from baseC; compare with
               \* the AE entry at the same index; stop at eL.
               LET hi   == Min(lli, eL)
                   bad  == {f \in baseC..hi :
                              TermAtL(lg, bs, f) /= ae.ents[f - ae.prev].term}
               IN IF bad /= {} THEN
                     LET f == MinSet(bad) IN
                     LAMRes(TRUE, ae.ents[f - ae.prev].term, f)         \* 7676-7678
                  ELSE LAMRes(FALSE, -1, -1)                            \* 7679-7685
    ELSE
      \* 7704-7708: the entry at PrevLogIndex exists (lli >= prev > baseC)
      LET prevTerm == lg[ae.prev].term IN
      IF prevTerm = ae.prevTerm THEN LAMRes(FALSE, -1, -1)

      ELSE
          \* 7745-7764: conflict at PrevLogIndex; report first index of that term
          LET same == {i \in 1..ae.prev : i > baseC /\ lg[i].term = prevTerm /\
                          \A j \in i..ae.prev : j > baseC /\ lg[j].term = prevTerm}
          IN LAMRes(TRUE, prevTerm, IF same = {} THEN ae.prev ELSE MinSet(same))

\* ========================================================================
\* TRANSPORT (rpc25519) HELPERS
\* ========================================================================
\*

\* A circuit retains its setup connection k for lifecycle notification
\* (ckt.go setLoopyIfUnset, ckt2.go setLoopy). SendOneWay still looks up the
\* destination address on each send. A missing mapping immediately fails.
\* Each connection has two FIFO send channels (out), active writes (writer),
\* complete frames awaiting dispatch (q), and reverse-link ACK debt (u).
\* Once written, distinct circuit readers can dispatch out of order.
\* A full send channel refuses an ordinary Raft frame. On accepted writes
\* LocalPeer.SendOneWay waits for DoneCh or circuit/peer cancellation.
\* END notices from the pump can await capacity in background goroutines.

Dirs == {"da", "ad"}

EpStOf(cm, c, n) ==
    IF cm[c].d = n /\ cm[c].di = inc[n] THEN cm[c].dSt
    ELSE IF cm[c].a = n /\ cm[c].ai = inc[n] THEN cm[c].aSt
    ELSE EpGone

EpSt(c,n) == EpStOf(ckt,c,n)

SetEp(cm, c, n, st) ==
    [cm EXCEPT ![c] = IF cm[c].d = n THEN [@ EXCEPT !.dSt = st]
                                     ELSE [@ EXCEPT !.aSt = st]]

\* PeerID (key) of the remote end of circuit c as seen from n
PeerKeyOf(c, n) == IF ckt[c].d = n THEN <<ckt[c].a, ckt[c].ai>>
                   ELSE <<ckt[c].d, ckt[c].di>>
PeerNameOf(c, n) == KName(PeerKeyOf(c, n))
\* cktP.PeerBaseServerAddr = ckt.RpbTo.BaseServerAddr (tube/tube.go:16742)
PeerAddrOf(c, n) == IF ckt[c].d = n THEN ckt[c].ad ELSE ckt[c].dd

\* Absolute addresses name the most recently registered pair. Removing
\* that pair does not reinstall an older one (srv.go deletePair). Synthetic
\* auto-client addresses also allow the reverse incoming connection to be
\* found by its suffixed name (destAddrToSendCh/findAutoCliPair).
SyntheticAutoClientAddrs == FALSE
EnableSyntheticAddrs == TRUE

AddressRoute(cn,n,m,a) ==
    LET direct == {k \in DOMAIN cn : cn[k].d = n /\ cn[k].di = inc[n]
                                     /\ cn[k].a = m /\ cn[k].ad = a}
        key    == IF direct = {} THEN 0 ELSE MaxSet(direct)
        rev    == {k \in DOMAIN cn : cn[k].a = n /\ cn[k].ai = inc[n] /\ cn[k].aOpen
                                     /\ cn[k].d = m /\ cn[k].dd = a}
    IN IF key /= 0 /\ cn[key].dOpen THEN key
       ELSE IF SyntheticAutoClientAddrs /\ rev /= {} THEN CHOOSE k \in rev : TRUE ELSE 0

RouteOf(cn,n,c) ==
    LET ck == ckt[c]
        k  == ck.k
    IN IF ck.d = n THEN AddressRoute(cn,n,ck.a,ck.ad)
       ELSE IF cn[k].a = n /\ cn[k].ai = inc[n] /\ cn[k].aOpen THEN k
       ELSE IF cn[k].d = n \/ SyntheticAutoClientAddrs THEN AddressRoute(cn,n,ck.d,ck.dd) ELSE 0

SendDir(cn, k, n) == IF cn[k].d = n /\ cn[k].di = inc[n] THEN "da" ELSE "ad"
DirOpen(p, dir) == IF dir = "da" THEN p.dOpen ELSE p.aOpen
OtherDir(dir) == IF dir = "da" THEN "ad" ELSE "da"
Occ(cn, k, dir) == Len(cn[k].q[dir]) + cn[k].u[dir]

Absorbing(cn, k, dir) ==
    LET r  == IF dir = "da" THEN cn[k].a ELSE cn[k].d
        sd == IF dir = "da" THEN cn[k].d ELSE cn[k].a
    IN /\ DirOpen(cn[k], OtherDir(dir)) /\ up[r] /\ ~stopped[r]
       /\ link[sd][r] = "ok" /\ link[r][sd] = "ok"
       /\ blk[r].pend = <<>>

WireRoom(cn, k, dir) == Occ(cn, k, dir) < SendBufCap \/ Absorbing(cn, k, dir)

\* The plaintext workspace checks HDR.Deadline before writing and across
\* all frame parts (common.go sendMessage/writeFrameByDeadline). The optional
\* extra ChaCha layer uses writeFull with the send loop's explicit timeout,
\* nil for ordinary data (chacha.go blabber.sendMessage/encoder.sendMessage).
EnforceWriteDeadline == TRUE
ExtraChaChaFraming == FALSE

\* Config.ClientSendKeepAlive and Config.ServerSendKeepAlive default to
\* zero. Include "da" (client) or "ad" (server) in this set for profiles
\* that enable application keepalives. Each write has its own explicit
\* timeout, including with extra ChaCha framing.
KeepAliveDirections == {}

NoBlk == [pend |-> <<>>, wait |-> Nil, save |-> FALSE]

CanEnqueueAt(cn, cm, n, c) ==
    IF c = NoCkt \/ c = Pending THEN FALSE
    ELSE IF EpStOf(cm, c, n) /= EpOpen THEN FALSE
    ELSE LET k == RouteOf(cn, n, c) IN
         IF k = 0 THEN FALSE ELSE Len(cn[k].out[SendDir(cn, k, n)]) < SendBufCap

CanEnqueue(cn,n,c) == CanEnqueueAt(cn,ckt,n,c)

\* Redirects pass Ticket.waitForValid to TubeNode.SendOneWay. Zero waits
\* for queue admission as well as the write. Ordinary protocol sends have
\* positive deadlines; the pump's END has a background handoff only.
TimedMessage(m) == IF "timed" \in DOMAIN m THEN m.timed ELSE m.type /= "END"

CanAttemptAt(cn,cm,n,c,m) ==
    IF TimedMessage(m) THEN CanEnqueueAt(cn,cm,n,c)
    ELSE IF c = NoCkt \/ c = Pending THEN FALSE
    ELSE EpStOf(cm,c,n) = EpOpen /\ RouteOf(cn,n,c) /= 0
CanAttempt(cn,n,c,m) == CanAttemptAt(cn,ckt,n,c,m)

\* TubeNode.SendOneWay gives fire-and-forget data ten heartbeats to write.
\* An accepted send synchronously waits in ckt.go LocalPeer.SendOneWay.
\* Finish a prefix of ready writes immediately or expose the first delayed
\* write and save the unsent tail. Each subsequent send runs the route and
\* capacity checks again after its predecessor has completed.
RECURSIVE SendBurstAt(_, _, _, _, _)
SendBurstAt(cn, cm, n, lst, slowAt) ==
    IF lst = <<>> THEN [cn |-> cn, bl |-> NoBlk]
    ELSE LET item == Head(lst)
             c == item.c
         IN IF ~CanAttemptAt(cn, cm, n, c, item.m)
            THEN SendBurstAt(cn, cm, n, Tail(lst), slowAt - 1)
            ELSE LET k == RouteOf(cn, n, c)
                     dir == SendDir(cn, k, n)
                     f == [cid |-> c, to |-> PeerKeyOf(c, n), m |-> item.m]
                     ready == cn[k].out[dir] = <<>> /\ cn[k].writer[dir] = Nil /\ WireRoom(cn,k,dir)
                     full == Len(cn[k].out[dir]) >= SendBufCap
                     waiting(status) == [NoBlk EXCEPT !.pend = lst,
                                                       !.wait = [k |-> k, dir |-> dir, status |-> status]]
                 IN IF full THEN [cn |-> cn, bl |-> waiting("slot")]
                    ELSE IF ready /\ slowAt /= 1
                    THEN SendBurstAt([cn EXCEPT ![k].q[dir] = Append(@, f)], cm, n, Tail(lst), slowAt - 1)
                    ELSE [cn |-> [cn EXCEPT ![k].out[dir] = Append(@, f)],
                          bl |-> waiting("write")]

SendBurst(cn,n,lst,slowAt) == SendBurstAt(cn,ckt,n,lst,slowAt)

\* The pump's -2 mode uses a separate waiter when the send channel is full.
\* Each notice has at most one waiter per circuit end (pump.go cleanupCkt).
QueueNotice(cn, k, dir, f) ==
    IF Len(cn[k].out[dir]) < SendBufCap THEN [cn EXCEPT ![k].out[dir] = Append(@, f)]
    ELSE [cn EXCEPT ![k].bg[dir] = @ \cup {f}]

BgSendOne(cn, n, c, msg) ==
    LET k == RouteOf(cn, n, c) IN
    IF k = 0 THEN cn
    ELSE QueueNotice(cn, k, SendDir(cn, k, n), [cid |-> c, to |-> PeerKeyOf(c,n), m |-> msg])

SeqElems(seq) == {seq[j] : j \in DOMAIN seq}
PipeFrames(p, dir) == SeqElems(p.out[dir]) \cup SeqElems(p.q[dir]) \cup p.bg[dir]
                      \cup (IF p.writer[dir] = Nil THEN {} ELSE {p.writer[dir].f})
QueuedMessages ==
    (UNION {UNION {{f.m : f \in PipeFrames(conn[k], dir)} : dir \in Dirs} : k \in DOMAIN conn})
    \cup (UNION {{blk[n].pend[j].m : j \in DOMAIN blk[n].pend} : n \in Server})

RemoveAt(s, i) == SubSeq(s, 1, i - 1) \o SubSeq(s, i + 1, Len(s))

Snd(k, dir) == IF dir = "da" THEN conn[k].d ELSE conn[k].a
Rcv(k, dir) == IF dir = "da" THEN conn[k].a ELSE conn[k].d
RcvOpen(k, dir) == IF dir = "da" THEN conn[k].aOpen ELSE conn[k].dOpen

\* The receiving process reads frame i (srv.go:619+ read loop, pump.go:200-280).
\* Requires the forward link; the TCP ACK reaches the sender only if the
\* reverse link is up, otherwise the frame still occupies the sender's
\* buffer (u counter).  Per-circuit FIFO.
CanRead(k, dir, i) ==
    /\ i \in 1..Len(conn[k].q[dir])
    /\ RcvOpen(k, dir)
    /\ LET r == Rcv(k, dir) IN up[r] /\ ~stopped[r] /\ link[Snd(k, dir)][r] = "ok"
    /\ \A j \in 1..(i - 1) : conn[k].q[dir][j].cid /= conn[k].q[dir][i].cid

FrameAt(k, dir, i) == conn[k].q[dir][i]

Consume(cn, k, dir, i) ==
    [cn EXCEPT ![k].q[dir] = RemoveAt(@, i),
               ![k].u[dir] = IF link[Rcv(k, dir)][Snd(k, dir)] = "ok" THEN @ ELSE @ + 1]

ConsumeTube(cn, k, dir, i) ==
    [Consume(cn,k,dir,i) EXCEPT ![k].read[dir] = @ \cup {cn[k].q[dir][i].cid}]

\* ToPeerID of the frame is not the receiving process (stale incarnation
\* routed by address onto a connection to a newer process, D-38).
StaleTarget(k, dir, i) == FrameAt(k, dir, i).to /= <<Rcv(k, dir), inc[Rcv(k, dir)]>>

\* Replica-to-replica addToCktall refuses a local name collision or a
\* different PeerID whose open owner has been seen within followerDownDur.
NameInUse(n,c) ==
    LET m == PeerNameOf(c,n)
        key == PeerKeyOf(c,n)
        owner == byName[n][m]
    IN \/ m = n /\ key /= <<n,inc[n]>>
       \/ IF owner > 0 THEN /\ EpSt(owner,n) = EpOpen /\ PeerKeyOf(owner,n) /= key
                            /\ owner \notin watchSilent[n]
          ELSE FALSE

PeerJoinRefused(n,c) == cktall[n][PeerKeyOf(c,n)] = NoCkt /\ NameInUse(n,c)

\* frame i is a Tube message for an endpoint Tube has registered
TubeFrame(k, dir, i) ==
    /\ CanRead(k, dir, i)
    /\ ~StaleTarget(k, dir, i)
    /\ EpSt(FrameAt(k, dir, i).cid, Rcv(k, dir)) = EpOpen
    /\ FrameAt(k, dir, i).m.from = PeerKeyOf(FrameAt(k, dir, i).cid, Rcv(k, dir))
    /\ ~PeerJoinRefused(Rcv(k,dir), FrameAt(k,dir,i).cid)

\* ========================================================================
\* NODE-STATE RECORD
\* Handlers are composed of several Go functions (becomeFollower, setMC,
\* connectToMC, commitWhatWeCan, ...).  They are modeled as operators on a
\* record holding one node's volatile + WAL state, written back at the end
\* of the action (WriteBack).  `out` collects frames to send (in order),
\* `closes` circuit closes, `save` whether saver.save ran.
\* ========================================================================

FreshPeer == [mi |-> 0, lcri |-> 0, credit |-> FALSE, heard |-> 0, recentAck |-> FALSE, ping |-> 0, pingFresh |-> FALSE]
RecentAck(s, key) == IF s.peers[key] = Nil THEN FALSE ELSE s.peers[key].recentAck

NS(n) ==
    [ self |-> <<n, inc[n]>>, book |-> clientTickets,
      node |-> n, known |-> knownPeers[n], newAdmin |-> {}, role |-> role[n], term |-> currentTerm[n], vote |-> votedFor[n],
      leader |-> leaderKey[n], sticky |-> sticky[n], campaign |-> campaign[n],
      pvTerm |-> pvTerm[n], pv2Term |-> pv2Term[n], pvYes |-> pvYes[n], pvNo |-> pvNo[n],
      vYes |-> vYes[n], vNo |-> vNo[n], peers |-> peers[n], pong |-> pong[n],
      aged |-> leaderAged[n], startup |-> startupPhase[n], noop0 |-> noop0[n], stallQ |-> stallQ[n], mc |-> mcPending[n],
      cfg |-> config[n], sh |-> shadows[n], ci |-> commitIndex[n], cit |-> commitIndexTerm[n],
      la |-> lastApplied[n], lat |-> lastAppliedTerm[n], slot |-> snapSlot[n],
      log |-> log[n], base |-> base[n], idLen |-> idLen[n],
      high |-> appendWitness[n].current, savedHigh |-> appendWitness[n].saved,
      saved |-> [term |-> dTerm[n], vote |-> dVote[n], cfg |-> dCfg[n], sh |-> dShadows[n],
                 known |-> dKnownPeers[n], ci |-> dCommit[n], cit |-> dCommitTerm[n]],
      ca |-> cktall[n], bn |-> byName[n], pa |-> pendAddr[n], wd |-> wdReq[n],
      rep |-> cktReplica[n], aud |-> audit[n], silent |-> watchSilent[n], obs |-> observer[n],
      appl |-> applied[n], ackH |-> ackHigh[n], par |-> par[n],
      seenOv |-> {},                   \* cktPlus.seen updates made in this step
      \* accumulators
      out |-> <<>>, closes |-> <<>>, save |-> FALSE, save2 |-> FALSE,
      newCommitted |-> {}, newPanics |-> {}, newCfgHist |-> {},
      newAcked |-> {}, newLeaders |-> {}, nid |-> nextId, sid |-> nextSid ]

\* persistor.go save and par.go parSaveLocalMeta. A save captures the
\* configuration and commitment at this call, including intermediate saves
\* during a handler. The PAR metadata also restores an existing same-term
\* vote in memory and prevents recording two votes for different candidates.
SaveRaftState(s) ==
    LET prior == s.saved
        conflict == prior.term = s.term /\ prior.vote /= Nil /\ s.vote /= Nil /\ prior.vote /= s.vote
        tm == Max(prior.term, s.term)
        vt == IF prior.term > s.term \/ (prior.term = s.term /\ prior.vote /= Nil)
              THEN prior.vote ELSE s.vote
    IN IF conflict THEN [s EXCEPT !.newPanics = @ \cup {"PARMetaVoteConflict"}]
       ELSE [s EXCEPT !.term = tm, !.vote = vt, !.save = TRUE,
                      !.saved = [term |-> tm, vote |-> vt, cfg |-> s.cfg, sh |-> s.sh,
                                 known |-> s.known, ci |-> s.ci, cit |-> s.cit],
                      !.savedHigh = s.high, !.campaign.persisted = s.campaign.promise]

AddOut(s, c, msg) == [s EXCEPT !.out = Append(@, [c |-> c, m |-> msg])]

\* RaftState.Known.merge (tube.go, MemberConfig.merge). An established
\* directory entry survives a pending address in a received membership.
MergeKnown(directory, details, names) ==
    [m \in Server |-> IF m \in names /\ details[m] /= Nil /\
                         (directory[m] = Nil \/ details[m].inc /= -1)
                      THEN details[m] ELSE directory[m]]

RECURSIVE AddOutSet(_, _)
\* broadcast: one frame per element of S (S: set of [c, m]); deterministic order
AddOutSet(s, S) ==
    IF S = {} THEN s
    ELSE LET x == CHOOSE x \in S : TRUE IN AddOutSet(AddOut(s, x.c, x.m), S \ {x})

Panic(s, why) == [s EXCEPT !.newPanics = @ \cup {why}]

\* Ordinary data tickets. TicketID is a sequence index here. The original
\* belongs to one PeerID (including its incarnation). A forwarded submission
\* can have a separate waiting copy at each leader. Each WAL append records
\* its own immutable entry ID and the process that holds its in-memory waiter.
\* The KV model gives the return values for individual command types.
NewDataTicket(origin, waitForLeader) ==
    [origin |-> origin, wait |-> waitForLeader, result |-> Nil, returned |-> Nil, contextDone |-> FALSE,
     stashed |-> FALSE, dispatch |-> FALSE,
     command |-> [kind |-> "data"], timed |-> TRUE, old |-> FALSE, mcs |-> [k \in Keys |-> Nil],
     stage |-> [k \in Keys |-> "none"], stamp |-> [k \in Keys |-> 0],
     entries |-> {}]

RegisterDataTicket(s, waitForLeader) ==
    [s EXCEPT !.book = Append(@, NewDataTicket(s.self, waitForLeader))]

\* FinishTicket closes Done once. The caller sees that result if still waiting.
\* A prior context cancellation or membership API timeout retains its own return.
FinishDataTicket(s, t, status, entry, via) ==
    IF s.book[t].origin /= s.self THEN s
    ELSE IF s.book[t].result /= Nil THEN [s EXCEPT !.book[t].stage[s.self] = "none"]
    ELSE LET r == [status |-> status, entry |-> entry, via |-> via]
         IN [s EXCEPT !.book[t].result = r, !.book[t].returned = IF @ = Nil THEN r ELSE @,
                      !.book[t].stage[s.self] = "none"]

\* Follower redirectToLeader uses the same failure handling for a rejected
\* queue handoff, a failed write or cancellation before it observes success.
RedirectFailed(s,t) ==
    IF s.book[t].wait
    THEN [s EXCEPT !.book[t].stashed = TRUE,
                   !.book[t].stage[s.self] = IF s.book[t].result = Nil THEN "follow" ELSE "none"]
    ELSE FinishDataTicket(s,t,"redirectError",0,"local")

\* WaitingAtLeader before noop0, then batchToSubmit until the batch fires.
\* Preserve the order stamped on admission (replicateTicket, resubmitStalledTickets).
AdmitDataTicket(s, t) ==
    LET ord == 1 + MaxSet({0} \cup {s.book[i].stamp[s.self] : i \in DOMAIN s.book})
    IN [s EXCEPT !.book[t].stage[s.self] =
                     IF s.book[t].command.kind /= "data" THEN "adminAdmission"
                     ELSE IF s.noop0.done THEN "batch" ELSE "unindexed",
                 !.book[t].stamp[s.self] = IF @ = 0 THEN ord ELSE @]

\* prepOne separates the immutable wire command from the local waiting ticket.
BindDataEntry(s, t) ==
    [s EXCEPT !.book[t].stage[s.self] = "indexed",
              !.book[t].entries = @ \cup {[id |-> s.nid, maker |-> s.self, idx |-> Len(s.log) + 1]}]

TicketReply(s, t, status, entry) ==
    LET dest == s.book[t].origin
        direct == s.ca[dest]
        c == IF direct /= NoCkt THEN direct
             ELSE IF status \in {"applied","configCommitted","unchanged"} /\ KName(dest) /= KName(s.self) THEN s.bn[KName(dest)]
             ELSE NoCkt
    IN IF c \in {NoCkt, Pending} THEN s
       ELSE AddOut(s, c, [type |-> "TKTRES", from |-> s.self, tid |-> t,
                          status |-> status, entry |-> entry])

\* commitWhatWeCan applies a local waiter only on the leader. At its origin,
\* a follower may complete a replicated copy, but cannot use a waiter left
\* in its WAL by its own prior leadership. parCompleteApply publishes the
\* successful result after saving the application checkpoint in this step.
CompleteAppliedDataTicket(s, e, onLeader) ==
    LET ids == {t \in DOMAIN s.book : \E r \in s.book[t].entries : r.id = e.id}
    IN IF ids = {} THEN s
       ELSE LET t == CHOOSE j \in ids : TRUE
                q == s.book[t]
                owner == CHOOSE r \in q.entries : r.id = e.id
                liveCaller == q.stage[s.self] /= "none"
            IN IF q.origin = s.self
               THEN IF liveCaller /\ (onLeader \/ owner.maker /= s.self)
                    THEN FinishDataTicket(s, t, "applied", e.id,
                                          IF onLeader THEN "leader" ELSE "follower")
                    ELSE s
               ELSE IF onLeader
                    THEN TicketReply([s EXCEPT !.book[t].stage[s.self] = "none"], t, "applied", e.id)
                    ELSE s

\* clearBatchToSubmit and tellClientsImNotLeader walk both pending and indexed
\* tickets. A local caller sees ErrLeaderStepDown; a remote caller can receive
\* that result once its notice is delivered (tube.go, TellClientSteppedDown).
RECURSIVE DeposeDataTickets(_, _)
DeposeDataTickets(s, t) ==
    IF t > Len(s.book) THEN s
    ELSE LET s1 == IF s.book[t].stage[s.self] \notin {"unindexed", "batch", "indexed"} THEN s
                   ELSE IF s.book[t].origin = s.self
                        THEN FinishDataTicket(s, t, "stepdown", 0, "local")
                        ELSE TicketReply([s EXCEPT !.book[t].stage[s.self] = "none"],
                                         t, "stepdown", 0)
         IN DeposeDataTickets(s1, t + 1)

\* becomeLeader transfers WaitingAtFollow, including entries with LogIndex 0.
RECURSIVE TakeOverDataTickets(_, _)
TakeOverDataTickets(s, t) ==
    IF t > Len(s.book) THEN s
    ELSE LET s1 == IF s.book[t].origin = s.self /\
                      (s.book[t].stage[s.self] = "follow" \/ s.book[t].stashed)
                   THEN AdmitDataTicket([s EXCEPT !.book[t].stashed = FALSE, !.book[t].dispatch = FALSE], t) ELSE s
         IN TakeOverDataTickets(s1, t + 1)

\* ------------------------------------------------------------------------
\* deleteFromCktAll (tube/tube.go:2552-2593) for the cktPlus of PeerID key
\* whose circuit is c (c = Pending for a pending cktPlus).  It removes map
\* entries only if they still refer to that cktPlus (matches()), and does
\* NOT close the circuit.  It also deletes s.peers[PeerID] (2570).
\* ------------------------------------------------------------------------
DeleteFromCktAll(s, key, c) ==
    LET m == KName(key)
        isCur == key \in DOMAIN s.ca /\ s.ca[key] = c /\ c > 0
    IN [s EXCEPT
          !.ca    = IF isCur THEN [@ EXCEPT ![key] = NoCkt] ELSE @,
          !.peers = IF isCur THEN [@ EXCEPT ![key] = Nil] ELSE @,
          !.rep   = IF isCur THEN @ \ {key} ELSE @,
          !.bn    = IF s.bn[m] = c THEN [@ EXCEPT ![m] = NoCkt] ELSE @,
          !.wd    = IF s.bn[m] = c THEN [@ EXCEPT ![m] = Nil] ELSE @,
          !.pa    = IF s.bn[m] = c THEN [@ EXCEPT ![m] = "none"] ELSE @,
          !.silent = @ \ {c}]

\* ------------------------------------------------------------------------
\* Tube identifies a replica by both its stable name and process PeerID.
\* addToCktall rejects a different incarnation while the name's owner is
\* open and recently heard from. A silent open owner is retired first
\* (tube/tube.go, addToCktall). Identity checks run again in peerJoin.
AddToCktall(s, n, c) ==
    LET key == PeerKeyOf(c, n)
        m == KName(key)
        owner == s.bn[m]
        other == IF owner > 0 THEN owner /= c /\ PeerKeyOf(owner, n) /= key /\ EpSt(owner,n) = EpOpen
                              ELSE FALSE
        s0 == IF other THEN DeleteFromCktAll(s, PeerKeyOf(owner,n), owner) ELSE s
        old == s0.ca[key]
        keep == IF old /= NoCkt THEN s0.peers[key] ELSE Nil
        s1 == IF old /= NoCkt THEN DeleteFromCktAll(s0, key, old) ELSE s0
        detail == [inc |-> KInc(key), addr |-> PeerAddrOf(c,n)]
        s2 == [s1 EXCEPT !.ca[key] = c, !.bn[m] = c, !.wd[m] = Nil, !.pa[m] = "none",
                         !.silent = @ \ {c},
                         !.seenOv = {x \in @ : x.c /= c} \cup {[c |-> c, v |-> Nil]}]
    IN IF m \in s2.cfg.mem
       THEN [s2 EXCEPT !.cfg.det[m] = detail, !.known[m] = detail, !.rep = @ \cup {key},
                       !.peers[key] = IF keep /= Nil THEN keep ELSE FreshPeer]
       ELSE s2

\* cfg.det contains the details for both voters and shadows. Removing a
\* member puts its old detail in ShadowReplicas.PeerNames. A new direct
\* circuit can change a member's details at the same config version.
\* Addresses represent canonical base-server addresses after normalization
\* in setAddrURL and connectInBackgroundIfNoCircuitTo (tube/tube.go).
CIB(s, n, m) ==
    IF m = n THEN s
    ELSE LET d == s.cfg.det[m]
             known == IF d = Nil THEN FALSE ELSE m \in s.cfg.mem \cup s.sh /\ d.addr \in Addrs
             a == IF s.bn[m] > 0 THEN PeerAddrOf(s.bn[m],n)
                  ELSE IF known THEN d.addr ELSE s.pa[m]
         IN IF s.bn[m] <= 0
            THEN [s EXCEPT !.bn[m] = Pending, !.pa[m] = a, !.wd[m] = a]
            ELSE [s EXCEPT !.wd[m] = a]

RECURSIVE CIBSet(_, _, _)
CIBSet(s, n, S) ==
    IF S = {} THEN s
    ELSE LET m == CHOOSE m \in S : TRUE IN CIBSet(CIB(s, n, m), n, S \ {m})

\* connectToMC (tube/tube.go:15226-15307): dial MC members and shadows that
\* are absent from cktAllByName. A pending watchdog handles its own retries.
\* Its deferred resubmitStalledTickets(true) (15228-15230) is applied by
\* the caller through ResubmitStalledMC.
ConnectToMC(s, n) ==
    LET want == {m \in (s.cfg.mem \cup s.sh) \ {n} : s.bn[m] = NoCkt}
    IN IF s.cfg.mem = {} THEN s ELSE CIBSet(s, n, want)

\* ------------------------------------------------------------------------
\* Optional representative non-replica subscriber. Registers no Raft
\* watchdog. notice records the last attempted ObserveMembershipChange
\* send, as its asynchronous transport may lose the notification.
ModelObservers == FALSE
ClosedObserver == [open |-> FALSE, notice |-> Nil]
NotifyObserver(s) ==
    IF s.obs.open
    THEN [s EXCEPT !.obs.notice = [ct |-> s.cfg.ct, cv |-> s.cfg.cv, mem |-> s.cfg.mem,
                                    leader |-> s.leader]]
    ELSE s

\* adjustCktReplicaForNewMembership calls serviceMembershipObservers,
\* then selects the PeerID specified in each member/shadow PeerDetail.
AdjustCktReplica(s, n) ==
    LET grp == (s.cfg.mem \cup s.sh) \ {n}
        keep == {k \in s.rep : KName(k) \in grp}
        todo == grp \ {KName(k) : k \in keep}
        known == {m \in todo : IF s.cfg.det[m] = Nil THEN FALSE ELSE s.cfg.det[m].inc /= -1}
        key(m) == <<m, s.cfg.det[m].inc>>
        present == {m \in known : s.ca[key(m)] /= NoCkt}
        s1 == [NotifyObserver(s) EXCEPT !.rep = keep \cup {key(m) : m \in present}]
    IN CIBSet(s1, n, known \ present)

\* putRemovedReplicasIntoShadows (tube/tube.go:14876-14896)
ShadowsAfter(sh, old, new) == (sh \cup (old.mem \ new.mem)) \ (new.mem \ old.mem)

WithShadowDetails(s, newCfg) ==
    [newCfg EXCEPT !.det = [m \in Server |-> IF m \in (s.cfg.mem \cup s.sh) \ newCfg.mem
                                            THEN s.cfg.det[m] ELSE newCfg.det[m]]]

ShadowsFromAE(s, ae) ==
    [s EXCEPT !.sh = ae.sh, !.cfg.det = [m \in Server |-> IF m \in ae.sh \ s.cfg.mem
                                                        THEN ae.cfg.det[m] ELSE @[m]]]

\* installMemberConfig, tube/tube.go. A leader's authoritative AE can
\* resolve two configurations with equal numbers and different voters.
InstallMC(s, n, newCfg, authoritative) ==
    IF (~authoritative /\ newCfg.mem = {} /\ s.cfg.mem /= {})
       \/ (VersionGTE(s.cfg, newCfg) /\ ~(authoritative /\ ConfigConflicts(s.cfg,newCfg)))
    THEN s
    ELSE LET s1 == [s EXCEPT !.sh = ShadowsAfter(@, s.cfg, newCfg),
                             !.cfg = WithShadowDetails(s,newCfg),
                             !.newCfgHist = @ \cup {[ct |-> newCfg.ct, cv |-> newCfg.cv, mem |-> newCfg.mem]}]
         IN AdjustCktReplica(SaveRaftState(s1), n)

SetMC(s, n, newCfg) == InstallMC(s, n, newCfg, FALSE)

EmptyMCTickets == [issued |-> 0, direct |-> {}, expired |-> {}, awaiting |-> {}, watch |-> <<>>,
                   failed |-> {}, noChange |-> {}, installed |-> {}]
TicketSet(q) == {q[i] : i \in DOMAIN q}
MCError(t, why) == [id |-> t.id, op |-> t.op, x |-> t.x, reason |-> why]

TicketFinished(s,t) ==
    \/ t.id \in s.mc.noChange
    \/ \E f \in s.mc.failed : f.id = t.id
    \/ \E a \in ackedCfg \cup s.newAcked : a.id = t.id

\* A stepdown fails stalled proposals and reports uncertain outcomes for
\* changes already installed. The configuration itself stays installed.
\* tube.go: becomeFollower, failStalledMembershipTickets, failMCAwaitingCommit.
AbortMembership(s) ==
    LET stalled == {MCError(t,"notLeader") : t \in {u \in TicketSet(s.stallQ) : ~TicketFinished(s,u)}}
        uncertain == {MCError(t,"unconfirmed") : t \in s.mc.awaiting}
    IN [s EXCEPT !.stallQ = <<>>, !.mc.watch = <<>>, !.mc.awaiting = {},
                 !.mc.failed = @ \cup stalled \cup uncertain]

\* Snapshot/WAL replacement can leave an indexed waiter with an unobservable
\* outcome. ifLostTicketTellClient reports that uncertainty when apply advances.
RECURSIVE CheckLostDataTickets(_, _)
CheckLostDataTickets(s, t) ==
    IF t > Len(s.book) THEN s
    ELSE LET recs == {r \in s.book[t].entries : r.maker = s.self}
             ids == {r.id : r \in s.book[t].entries}
             last == IF recs = {} THEN 0 ELSE MaxSet({r.id : r \in recs})
             lost == IF s.book[t].stage[s.self] /= "indexed" \/ last = 0 THEN FALSE
                     ELSE LET idx == (CHOOSE r \in recs : r.id = last).idx IN
                          /\ idx <= s.la
                          /\ idx <= s.base.idx \/ (idx <= Len(s.log) /\ s.log[idx].id \notin ids)
             s1 == IF ~lost THEN s
                   ELSE IF s.book[t].origin = s.self THEN FinishDataTicket(s, t, "outcomeLost", 0, "local")
                   ELSE TicketReply([s EXCEPT !.book[t].stage[s.self] = "none"], t, "outcomeLost", 0)
         IN CheckLostDataTickets(s1, t + 1)

\* resetPreVoteState (tube/tube.go): clears phase 1 and phase 2 state.
ResetPreVote(s) == [s EXCEPT !.pvTerm = 0, !.pv2Term = 0, !.pvYes = {}, !.pvNo = {}]

\* becomeFollower(term, mc, save) (tube/tube.go:6572-6665)
BecomeFollower(s, n, t, mc, doSave) ==
    LET mcUpd == mc /= Nil /\ VersionGT(mc, s.cfg) /\ (mc.mem /= {} \/ s.cfg.mem = {})                       \* 6599-6604
        retiring == IF s.role = Leader THEN DeposeDataTickets(s, 1) ELSE s
        s1 == [AbortMembership(retiring) EXCEPT !.role = Follower,                               \* 6583
                        !.noop0 = [id |-> 0, done |-> FALSE]]            \* 6595-6596
        s2 == IF mcUpd THEN SetMC(s1, n, mc) ELSE s1                     \* 6605-6607
        s3 == IF t > s.term \/ mcUpd                                     \* 6609-6619
              THEN [s2 EXCEPT !.term = t, !.vote = Nil, !.leader = Nil]
              ELSE s2
        s4 == ResetPreVote(s3)                                           \* 6621
    IN IF doSave THEN SaveRaftState(s4) ELSE s4                  \* 6625-6627

\* ========================================================================
\* APPLY AND COMMIT
\* ========================================================================

\* commitWhatWeCan apply loop (tube/tube.go:10408-10669), one entry at a time.
\* A shadow is a replication destination without a vote. Its add/remove is
\* a WAL command (tube.go, commandSpecificLocalActionsThenReplicateTicket,
\* commitWhatWeCan, doAddShadow, doRemoveShadow).
ForgetShadowCircuit(s,x) ==
    LET c == s.bn[x] IN
    IF c = NoCkt THEN s
    ELSE IF c = Pending THEN [s EXCEPT !.bn[x] = NoCkt, !.wd[x] = Nil, !.pa[x] = "none"]
    ELSE DeleteFromCktAll(s, PeerKeyOf(c,s.node), c)

ApplyShadow(s,e) ==
    CASE e.kind = "shadowAdd" ->
           [s EXCEPT !.sh = @ \cup {e.x}, !.known[e.x] = e.detail,
                     !.cfg.det[e.x] = IF e.x \in s.cfg.mem THEN @ ELSE e.detail]
      [] e.kind = "shadowRemove" ->
           ForgetShadowCircuit([s EXCEPT !.sh = @ \ {e.x},
                         !.cfg.det[e.x] = IF e.x \in s.cfg.mem THEN @ ELSE Nil], e.x)
      [] OTHER -> s

RECURSIVE ApplyFrom(_, _)
ApplyFrom(s, onLeader) ==
    IF s.la >= s.ci THEN s
    ELSE LET i == s.la + 1 IN
         IF i > Len(s.log) THEN Panic(s, "CommitIndexBeyondLog")          \* 10414-10417
         ELSE IF i <= s.base.idx THEN ApplyFrom([s EXCEPT !.la = i], onLeader)  \* 10425-10427
         ELSE LET e == s.log[i] IN
              \* 10447-10449: panic "terms should be monotone up" (D-91)
              IF e.term < s.lat THEN Panic(s, "TermsMonotoneUp")
              ELSE LET s1 == [s EXCEPT !.la = i, !.lat = e.term,          \* 10412, 10450
                                       !.appl = Append(@, [id |-> e.id, term |-> e.term])]
                       \* 10504-10537: the leader's own noop0 applied
                       s2 == IF onLeader /\ e.kind = "noop" /\ s.noop0.id = e.id /\ ~s.noop0.done
                             THEN [s1 EXCEPT !.noop0.done = TRUE] ELSE s1
                   IN ApplyFrom(CompleteAppliedDataTicket(ApplyShadow(s2,e), e, onLeader), onLeader)

\* commitWhatWeCan defers resubmitStalledTickets until the first noop has
\* applied. Retry its oldest pending membership change after the state save.
RECURSIVE ResubmitStalledMC(_, _)
CommitWhatWeCan(s, onLeader) ==
    IF s.par /= "ok" THEN s                                          \* 10350-10352 parBlocked
    ELSE LET s1 == ApplyFrom(s, onLeader)
             \* 10672-10678: MC.CommitIndex follows CommitIndex
             s2 == IF s1.cfg.ci < s1.ci THEN [s1 EXCEPT !.cfg.ci = s1.ci, !.cfg.cit = s1.cit] ELSE s1
             saved == IF s2.la > s.la THEN SaveRaftState(IF onLeader THEN CheckLostDataTickets(s2,1) ELSE s2) ELSE s2
         IN IF onLeader /\ ~s.noop0.done /\ saved.noop0.done
            THEN ResubmitStalledMC(saved,KName(saved.leader)) ELSE saved

\* peersOnePerName and preferPeerRecord, tube/tube.go. A live named circuit
\* selects its own incarnation, even if another record has a higher index.
\* With no such circuit the most recently heard record wins. Never-heard
\* ties prefer the smaller MI, then a fixed PeerID order (incarnation here).
PeerRecords(s, m) == {k \in Keys : KName(k) = m /\ s.peers[k] /= Nil}
PreferredPeer(s, a, b) ==
    \/ s.peers[a].heard > s.peers[b].heard
    \/ /\ s.peers[a].heard = s.peers[b].heard
       /\ \/ s.peers[a].mi < s.peers[b].mi
          \/ s.peers[a].mi = s.peers[b].mi /\ KInc(a) < KInc(b)
CurrentRecords(s, n, m) ==
    LET ks == PeerRecords(s,m) IN
    IF s.bn[m] > 0 THEN ks \cap {PeerKeyOf(s.bn[m],n)}
    ELSE {k \in ks : ~\E j \in ks : PreferredPeer(s,j,k)}

\* pongpq.go: pongPQ.add replaces any earlier pong of this name, even if a
\* restarted replica now uses a different PeerID. Record only voting pongs.
RecordPong(p,k) ==
    [all |-> {old \in p.all : KName(old) /= KName(k)} \cup {k},
     fresh |-> {old \in p.fresh : KName(old) /= KName(k)} \cup {k}]

\* LastHeardAnything changes only on an addressed current-term AEA. Relative
\* ranks per name encode recency without an unbounded wall clock.
MarkPeerHeard(s, key) ==
    LET others == {k \in PeerRecords(s,KName(key)) \ {key} : s.peers[k].heard > 0}
        rank(k) == 1 + Cardinality({j \in others : s.peers[j].heard < s.peers[k].heard})
        old == IF s.peers[key] = Nil THEN FreshPeer ELSE s.peers[key]
    IN [s EXCEPT !.peers = [k \in Keys |->
           IF k = key THEN [old EXCEPT !.heard = Cardinality(others) + 1, !.recentAck = TRUE]
           ELSE IF k \in others THEN [s.peers[k] EXCEPT !.heard = rank(k)]
           ELSE s.peers[k]]]

VotingPeers(s,n) == UNION {CurrentRecords(s,n,m) : m \in s.cfg.mem \ {n}}
CountGE(s, n, v) ==
    (IF n \in s.cfg.mem /\ Len(s.log) >= v THEN 1 ELSE 0) +
    Cardinality({k \in VotingPeers(s,n) : s.peers[k].mi >= v})

\* tube.go: leaderAdvanceCommitIndex sorts all counted indexes before its
\* logical-length and current-term guards. A removed leader may replicate
\* for the new configuration until it commits, without contributing a vote.
LeaderAdvanceCommitIndex(s, n) ==
    LET quor == RequiredVotes(s.cfg.mem, s.startup)
        values == {s.peers[k].mi : k \in VotingPeers(s,n)} \cup
                  (IF n \in s.cfg.mem THEN {Len(s.log)} ELSE {})
        cands == {v \in values : CountGE(s,n,v) >= quor}
        newCI == IF cands = {} THEN 0 ELSE MaxSet(cands)
    IN IF newCI <= s.ci \/ newCI > Len(s.log) THEN s
       ELSE IF newCI <= s.base.idx \/ s.log[newCI].term /= s.term THEN s
       ELSE LET s1 == [s EXCEPT !.ci = newCI, !.cit = s.log[newCI].term,
                         !.newCommitted = @ \cup
                            {[idx |-> i, term |-> s.log[i].term, id |-> s.log[i].id] : i \in (s.ci + 1)..newCI}]
            IN CommitWhatWeCan(s1, TRUE)

\* ========================================================================
\* APPEND-ENTRIES CONSTRUCTION AND SENDING (leader side)
\* ========================================================================

WireEntry(entry, i) == ("idx" :> i) @@ entry
LogEntry(entry) == [f \in DOMAIN entry \ {"idx"} |-> entry[f]]
EntsFrom(lg, b, e) ==
    [j \in 1..(e - b + 1) |-> WireEntry(lg[b + j - 1], b + j - 1)]

\* newAE (tube/tube.go:8903-8948): term, LLI/LLT, commit, TermsRLE, MC,
\* ShadowReplicas and watchdog gossip. An AE identifies the PeerIDs
\* recently heard from by its leader, a projection of PeerID2LastHeard.
AEMsg(s, n, prev, prevTerm, ents) ==
    [type |-> "AE", from |-> <<n, inc[n]>>, term |-> s.term, commit |-> s.ci,
     commitTerm |-> s.cit, ping |-> 0,
     lli |-> Len(s.log), llt |-> LastTermL(s.log, s.base),
     prev |-> prev, prevTerm |-> prevTerm, ents |-> ents,
     rle |-> RLEOf(s.log, s.base), cfg |-> s.cfg, sh |-> s.sh,
     heard |-> {k \in Keys : RecentAck(s,k)},
     gids |-> Ids(s.log)]                     \* ghost: leader entry ids (for LcriSound)

\* AEID identifies the round trip on a RaftNodeInfo. Snapshot-enclosed AEs
\* have no corresponding UnackedPing. Ordinary sends choose a reusable
\* positive color absent from all queues and this handler's outbound batch.
FreshPing(s) ==
    LET messages == QueuedMessages \cup {s.out[i].m : i \in DOMAIN s.out}
        carrying == {m \in messages : m.type \in {"AE", "AEA"}}
        used == {m.ping : m \in carrying}
    IN MinSet((1..(Cardinality(used) + 1)) \ used)

StartPing(s, key, ping) ==
    LET p == IF s.peers[key] = Nil THEN FreshPeer ELSE s.peers[key]
    IN [s EXCEPT !.peers[key] = [p EXCEPT !.ping = ping, !.pingFresh = TRUE]]

\* skipAEBecauseNotReplica (tube/tube.go:9661-9690): only members and shadows
SkipNotReplica(s, m) == m \notin (s.cfg.mem \cup s.sh)

\* tube/tube.go, sendAppendEntriesTo. A rejection retries only when the
\* next AE would differ in term, previous entry, range, commit or log end.
SendAppendEntriesAfter(s, n, key, beginIndex, ack) ==
    IF s.par /= "ok" \/ SkipNotReplica(s, KName(key)) THEN s
    ELSE IF beginIndex < 1 \/ Len(s.log) = 0 \/ beginIndex > Len(s.log) THEN s
    ELSE LET b == Max(beginIndex, s.base.idx + 1)
             e == Min(Len(s.log), b + AEBatchCap - 1)
             ents == IF b > Len(s.log) THEN <<>> ELSE EntsFrom(s.log, b, e)
             prev == IF b > 1 /\ b <= Len(s.log) THEN b - 1 ELSE 0
             prvT == IF prev = 0 THEN 0 ELSE TermAtL(s.log, s.base, prev)
             repeat == IF ack = Nil \/ ents = <<>> THEN FALSE
                       ELSE /\ ack.rej /\ ack.aeTerm = s.term
                            /\ ack.prev = prev /\ ack.prevTerm = prvT
                            /\ ack.beg = b /\ ack.end = e
                            /\ ack.commit = s.ci /\ ack.lli = Len(s.log)
             id == FreshPing(s)
             msg == [AEMsg(s,n,prev,prvT,ents) EXCEPT !.ping = id]
             c == s.ca[key]
         IN IF repeat THEN s
            ELSE LET sx == StartPing(s, key, id) IN
                 IF c = NoCkt THEN sx ELSE AddOut(sx, c, msg)

SendAppendEntriesTo(s, n, key, beginIndex) ==
    SendAppendEntriesAfter(s, n, key, beginIndex, Nil)

\* sendAppendEntriesEmptyHeartbeat (tube/tube.go:8974-9012); no MC filter
SendHeartbeat(s, n, key) ==
    IF s.par /= "ok" \/ KName(key) = n THEN s                        \* 8975-8981
    ELSE LET id == FreshPing(s) IN
         AddOut(StartPing(s, key, id), s.ca[key], [AEMsg(s,n,0,0,<<>>) EXCEPT !.ping = id])

\* bcastAppendEntries (tube/tube.go:9046-9154): to every s.peers record
\* (except self) whose name is a member or shadow and that has a cktall entry.
BcastAppendEntries(s, n, ents, prev, prevTerm) ==
    IF s.cfg.mem = {n} THEN s                                            \* bcastAppendEntries: soleVoter
    ELSE LET tgt == {k \in Keys : s.peers[k] /= Nil /\ KName(k) /= n     \* 9087-9090
                       /\ ~SkipNotReplica(s, KName(k))                   \* 9110-9118
                       /\ s.ca[k] /= NoCkt}                              \* 9124-9128
             id == FreshPing(s)
             msg == [AEMsg(s, n, prev, prevTerm, ents) EXCEPT !.ping = id]
             sx == [s EXCEPT !.peers = [k \in Keys |-> IF k \in tgt
                                       THEN [@[k] EXCEPT !.ping = id, !.pingFresh = TRUE]
                                       ELSE @[k]]]
         IN AddOutSet(sx, {[c |-> s.ca[k], m |-> msg] : k \in tgt})

\* ========================================================================
\* STATE SNAPSHOTS (leader side)
\* ========================================================================

\* getStateSnapshot (tube/tube.go:17908-17942): state with
\* CompactionDiscardedLast = (LastApplied, LastAppliedTerm) and an AE holding
\* the log past LastApplied.  `hist` and `appl` are ghosts (the history the
\* snapshot's KV embodies).
SnapState(s, n) ==
    [term |-> s.term, ci |-> s.ci, cit |-> s.cit, la |-> s.la, lat |-> s.lat,
     base |-> [idx |-> s.la, term |-> s.lat], cfg |-> s.cfg, sh |-> s.sh, known |-> s.known,
     hist |-> SubSeq(s.log, 1, s.la), appl |-> s.appl]

\* handleRequestStateSnapshot (tube/tube.go:17464-17551): sent on the circuit
\* the request / gap-ack arrived on (not necessarily cktall[peer]).
HandleRequestStateSnapshot(s, n, c) ==
    IF s.par /= "ok" THEN s                                          \* 17465-17467
    ELSE LET s1 == IF s.la < s.ci THEN CommitWhatWeCan(s, s.role = Leader) ELSE s   \* 17468-17473
             st == SnapState(s1, n)
             ae == AEMsg(s1, n, s1.la, s1.lat,
                         IF s1.la < Len(s1.log) THEN EntsFrom(s1.log, s1.la + 1, Len(s1.log)) ELSE <<>>)
             part(p, payload) ==
                 [type |-> "SNAP", from |-> <<n, inc[n]>>, sid |-> s1.sid, part |-> p,
                  st |-> IF payload THEN st ELSE Nil, ae |-> IF payload THEN ae ELSE Nil]
             s2 == [s1 EXCEPT !.sid = @ + 1]
         IN IF SnapParts = 1
            THEN AddOut(s2, c, part(0, TRUE))                            \* 17538-17550 single part
            \* 17504-17537: multipart (parts 1..k, last part negated); the
            \* model carries the whole payload in the last part.
            ELSE AddOut(AddOut(s2, c, part(1, FALSE)), c, part(-2, TRUE))

\* ========================================================================
\* MEMBERSHIP (MongoRaftReconfig, logless)
\* ========================================================================

\* cktPlus.MC / CurrentTerm as last reported to n on circuit c
SeenOf(s, n, c) ==
    LET ov == {x \in s.seenOv : x.c = c}
    IN IF ov /= {} THEN (CHOOSE x \in ov : TRUE).v
       ELSE IF ckt[c].d = n THEN ckt[c].dSeen ELSE ckt[c].aSeen

LiveByName(s, m) == s.bn[m] > 0                \* ok && cktP0.ckt != nil

\* parProtected (par.go:2127-2133)
ParProtected(s) == Max(Max(s.ci, s.la), Max(s.base.idx, s.cfg.ci))

\* par.go: parReconfigurationReady enforces the protected-prefix majority in
\* disk and NoDisk modes. Only the connected incarnation reporting this
\* voter set can supply its match index and largest common index.
ParReconfigReady(s, n, mc) ==
    IF s.par /= "ok" THEN FALSE                                      \* 2144-2146
    ELSE LET floor == ParProtected(s)
             selfC == IF n \in mc.mem /\ Len(s.log) >= floor THEN 1 ELSE 0     \* 2149-2153
             ok(m) == LET c == s.bn[m] IN
                        /\ c > 0                                          \* 2155-2158
                        /\ SeenOf(s, n, c) /= Nil
                        /\ VersionEq(SeenOf(s, n, c), mc)
                        /\ \E k \in Keys : /\ KName(k) = m /\ s.peers[k] /= Nil  \* 2159-2172
                                           /\ s.peers[k].mi >= floor /\ s.peers[k].lcri >= floor
                                           /\ k = PeerKeyOf(c, n)
         IN selfC + Cardinality({m \in mc.mem \ {n} : ok(m)}) >= CfgMajority(mc)

\* Q1/Q2 counters (changeMembership 14479-14538, onLeaderIsCurrentMCcommitted
\* 17367-17392): self counts 1 only if a member; others by cktAllByName.
CfgCount(s, n, cur) ==
    (IF n \in cur.mem THEN 1 ELSE 0) +
    Cardinality({m \in cur.mem \ {n} : LiveByName(s, m) /\ SeenOf(s, n, s.bn[m]) /= Nil
                                       /\ VersionEq(SeenOf(s, n, s.bn[m]), cur)})
TermCount(s, n, cur) ==
    (IF n \in cur.mem THEN 1 ELSE 0) +
    Cardinality({m \in cur.mem \ {n} : LiveByName(s, m) /\ SeenOf(s, n, s.bn[m]) /= Nil
                                       /\ SeenOf(s, n, s.bn[m]).term = s.term})

\* mongoLeaderCanReconfig (tube/tube.go:17213-17253): Q1, Q2, P1
\* (initialNoop0HasCommitted && parReconfigurationReady), QuorumsOverlap.
MongoCanReconfig(s, n, cur, new) ==
    LET need == CfgMajority(cur) IN
    /\ CfgCount(s, n, cur) >= need                                       \* 17224-17227 Q1
    /\ TermCount(s, n, cur) >= need                                      \* 17231-17233 Q2
    /\ s.noop0.done                                                      \* 17240-17242 P1
    /\ ParReconfigReady(s, n, cur)                                       \* 17243-17245
    /\ (new = Nil \/ QuorumsOverlap(cur, new))                           \* 17247-17251

\* tube.go: resolveMCAwaitingCommit. A positive confirmation takes
\* precedence over the 10-second deadline when both are observed together.
ResolveAwaitingMC(s) ==
    LET current == {t \in s.mc.awaiting : VersionEq(t,s.cfg)}
        done == IF s.cfg.com THEN current ELSE {}
        late == {t \in current \ done : t.id \in s.mc.expired}
        superseded == s.mc.awaiting \ current
    IN [s EXCEPT !.mc.awaiting = current \ (done \cup late),
                 !.newAcked = @ \cup done,
                 !.mc.failed = @ \cup {MCError(t,"unconfirmedTimeout") : t \in late}
                                \cup {MCError(t,"superseded") : t \in superseded}]

OnLeaderIsCurrentMCcommitted(s, n) ==
    LET promoted == ~s.cfg.com /\ MongoCanReconfig(s, n, s.cfg, Nil)
        s1 == IF promoted THEN [s EXCEPT !.cfg.com = TRUE, !.cfg.ci = Max(@,s.ci),
                      !.cfg.cit = IF s.ci > s.cfg.ci THEN s.cit ELSE @] ELSE s
    IN ResolveAwaitingMC(s1)
CurMCCommitted(s, n) == s.cfg.com \/ MongoCanReconfig(s, n, s.cfg, Nil)

\* Each stalled ticket may also be subscribed to the circuits whose MC was
\* absent or stale during Q1. Preserve FIFO order on each circuit.
RECURSIVE WatchMembership(_,_,_)
WatchMembership(q,t,cs) ==
    IF cs = {} THEN q
    ELSE LET c == CHOOSE c \in cs : TRUE
             w == [c |-> c, t |-> t]
             q1 == IF w \in TicketSet(q) THEN q ELSE Append(q,w)
         IN WatchMembership(q1,t,cs \ {c})

ForgetMCWatch(s,t) ==
    [s EXCEPT !.mc.watch = SelectSeq(@,LAMBDA w : w.t.id /= t.id)]
FailMC(s,t,why) ==
    [ForgetMCWatch(s,t) EXCEPT !.stallQ = SelectSeq(@,LAMBDA q : q.id /= t.id),
                                !.mc.failed = @ \cup {MCError(t,why)}]

\* tube.go: changeMembership. Ordinary requests queue behind the oldest
\* stalled one. A locally submitted forced change clears the main queue
\* and overrides the admission failure. The reply still awaits commitment.
ChangeMembershipWithForce(s, n, tkt0, force) ==
    IF TicketFinished(s,tkt0) THEN s ELSE
    LET q == IF force THEN <<>> ELSE s.stallQ
        s0 == IF q = <<>> THEN [s EXCEPT !.stallQ = <<>>]
              ELSE IF tkt0 = Head(q) THEN [s EXCEPT !.stallQ = Tail(q)]
              ELSE [s EXCEPT !.stallQ = Tail(Append(q,tkt0))]
        tkt == IF q /= <<>> THEN Head(q) ELSE tkt0
        cur == s0.cfg
        new0 == [cur EXCEPT !.cv = @ + 1, !.com = FALSE]                 \* 14424-14426
        \* changeMembership: an unreported member contributes no vote. Watch
        \* its circuit for a retry only if quorum or another admission test fails.
        live == {m \in cur.mem \ {n} : LiveByName(s0, m)}
        watch == {s0.bn[m] : m \in {p \in live :
                     LET seen == SeenOf(s0,n,s0.bn[p]) IN seen = Nil \/ ~VersionEq(seen,cur)}}
        new1 == [new0 EXCEPT !.det = [m \in Server |->
                    IF m \in live THEN [inc |-> KInc(PeerKeyOf(s0.bn[m], n)),
                                        addr |-> PeerAddrOf(s0.bn[m], n)]
                    ELSE IF m = n /\ n \in cur.mem THEN [inc |-> inc[n], addr |-> addr[n]]  \* 14539-14554
                    ELSE @[m]]]
        x == tkt.x
        \* 14560-14634 add; 14636-14661 remove (removed members become shadows in setMC)
        addCase ==
            IF s0.bn[x] > 0
            THEN [cfg |-> [new1 EXCEPT !.mem = @ \cup {x},
                             !.det[x] = [inc |-> KInc(PeerKeyOf(s0.bn[x], n)), addr |-> PeerAddrOf(s0.bn[x], n)]],
                  s |-> s0]                                              \* 14573-14592
            ELSE IF x = n
            THEN [cfg |-> [new1 EXCEPT !.mem = @ \cup {x}, !.det[x] = [inc |-> inc[n], addr |-> addr[n]]],
                  s |-> s0]                                              \* 14597-14610
            ELSE \* 14612-14631: pending cktPlus at AddPeerBaseServerHostPort, pulse it
                 [cfg |-> [new1 EXCEPT !.mem = @ \cup {x}, !.det[x] = [inc |-> -1, addr |-> "none"]],
                  s |-> [s0 EXCEPT !.bn[x] = Pending, !.pa[x] = tkt.addr, !.wd[x] = tkt.addr]]
        newCfg == IF tkt.op = "add" THEN addCase.cfg
                  ELSE [new1 EXCEPT !.mem = @ \ {x}, !.det[x] = Nil]     \* 14661
        refreshed == live \cup ({n} \cap cur.mem)
        known == MergeKnown(s0.known,new1.det,refreshed)
        s1 == IF tkt.op = "add"
              THEN [addCase.s EXCEPT !.known = IF LiveByName(s0,x) \/ x = n
                    THEN MergeKnown(known,newCfg.det,{x}) ELSE known]
              ELSE [s0 EXCEPT !.known = known]
    IN
    IF tkt.op = "add" /\ x \in cur.mem THEN FailMC(s0,tkt,"occupiedName")
    ELSE IF tkt.op = "remove" /\ x \notin cur.mem THEN
        [ForgetMCWatch(s1,tkt) EXCEPT !.mc.noChange = @ \cup {tkt.id}]
    ELSE IF ~tkt.force /\ ~MongoCanReconfig(s1,n,cur,newCfg)
         THEN IF tkt.id \in s1.mc.expired THEN FailMC(s1,tkt,"admissionTimeout")
              ELSE [s1 EXCEPT !.stallQ = IF tkt \in TicketSet(@) THEN @ ELSE Append(@,tkt),
                              !.mc.watch = WatchMembership(@,tkt,watch)]
    ELSE LET s2 == SetMC(ForgetMCWatch(s1,tkt),n,newCfg)
             pending == [id |-> tkt.id, op |-> tkt.op, x |-> x,
                         ct |-> newCfg.ct, cv |-> newCfg.cv, mem |-> newCfg.mem]
         IN OnLeaderIsCurrentMCcommitted([s2 EXCEPT !.mc.awaiting = @ \cup {pending},
                  !.mc.installed = IF VersionEq(s2.cfg,newCfg) THEN @ \cup {pending} ELSE @],n)

ChangeMembership(s,n,t) == ChangeMembershipWithForce(s,n,t,t.force)

\* unstallChangeMC and resubmitStalledTickets pop before resubmission.
ChangeMembershipRetry(s, n) ==
    IF s.role = Leader /\ s.stallQ /= <<>>
    THEN ChangeMembership([s EXCEPT !.stallQ = Tail(@)],n,Head(s.stallQ)) ELSE s
ResubmitStalledMC(s, n) ==
    IF s.role = Leader THEN ChangeMembershipRetry(s,n) ELSE AbortMembership(s)

\* watchdog.go: cktPlus.seen remembers the reporting voter set and the
\* latest nonzero term. It can release one circuit-specific waiter unless
\* a higher-term message is about to depose this leader.
SeenOn(s, n, c, cfgV, t) ==
    IF c = NoCkt \/ c = Pending THEN s
    ELSE LET old == SeenOf(s,n,c)
             term == IF t > 0 \/ old = Nil THEN t ELSE old.term
             v == [ct |-> cfgV.ct, cv |-> cfgV.cv, mem |-> cfgV.mem, term |-> term]
             s1 == [s EXCEPT !.silent = @ \ {c}, !.seenOv = {x \in @ : x.c /= c} \cup {[c |-> c, v |-> v]}]
             waiters == SelectSeq(s1.mc.watch,LAMBDA w : w.c = c)
         IN IF waiters = <<>> THEN
                IF s1.role = Leader /\ ~s1.cfg.com THEN OnLeaderIsCurrentMCcommitted(s1,n) ELSE s1
            ELSE IF s1.role /= Leader THEN
                [s1 EXCEPT !.mc.watch = SelectSeq(@,LAMBDA w : w.c /= c)]
            ELSE IF t > s1.term THEN s1
            ELSE LET w == Head(waiters)
                     s2 == [s1 EXCEPT !.mc.watch = SelectSeq(@,LAMBDA p : p /= w)]
                 IN ChangeMembership(s2,n,w.t)

SeenFrom(s, n, fromKey, cfgV, t) == SeenOn(s,n,s.ca[fromKey],cfgV,t)

\* handleAppendEntriesAck additionally retries the global stall queue after
\* seen on a known circuit, before checking the message term.
SeenAck(s,n,key,cfg,t) ==
    IF s.ca[key] = NoCkt THEN s ELSE ChangeMembershipRetry(SeenFrom(s,n,key,cfg,t),n)

\* suppressWatchdogs uses the leader's LastHeardAnything timestamps for
\* indirect observations; only peers for which this replica has a circuit
\* are refreshed. Direct reports also record the sender's term and MC.
SuppressWatchdogs(s, n, ae) ==
    LET direct == SeenFrom(s, n, ae.from, ae.cfg, ae.term)
    IN [direct EXCEPT !.silent = @ \ {direct.ca[k] : k \in ae.heard}]

\* ========================================================================
\* ELECTIONS
\* ========================================================================

\* tube.go: onRestartRecoverPersistentRaftStateFromDisk persists the larger
\* of the old and newly configured min-election-timeout + drift before this
\* incarnation advertises it. A fresh process has no restart deadline.
\* Delay values are relative model ticks. Override RestartVoteDelays with
\* {1,2} to allow either duration when restarting a disk-backed replica.
InitialVoteDelay == 1
RestartVoteDelays == {InitialVoteDelay}
ASSUME InitialVoteDelay \in Nat \ {0} /\ RestartVoteDelays # {} /\ RestartVoteDelays \subseteq (Nat \ {0})

NewCampaign(restored, old, configured) ==
    LET delay == Max(old, configured)
        held == restored \/ old > 0
    IN [held |-> held, remaining |-> IF held THEN delay ELSE 0,
        configured |-> configured, promise |-> delay, persisted |-> delay,
        releaseOnAE |-> held, deferred |-> "none", resume |-> "none"]

\* handleAppendEntries, before the PAR/term checks. After the deadline the
\* first AE releases the old in-memory promise. The next saver.save persists
\* this reduction. A crash before that save still restores the old window.
ObserveRestartDeadline(s) ==
    IF s.campaign.releaseOnAE /\ ~s.campaign.held
    THEN [s EXCEPT !.campaign.promise = s.campaign.configured, !.campaign.releaseOnAE = FALSE]
    ELSE s

RememberCampaign(s, kind) ==
    [s EXCEPT !.campaign.deferred = IF @ = "election" THEN @ ELSE kind]

\* recentlyHeardLeader and haveStickyLeader. Leadership transfer is outside
\* this model. Empty-MC self-add tests recent contact independently of held.
RecentLeader(s) == s.role = Leader \/ (s.leader /= Nil /\ s.sticky)
HaveStickyLeader(s) == s.campaign.held \/ RecentLeader(s)

\* logUpToDateAsOurs (tube/tube.go:9275-9333)
LogUpToDateAsOurs(s, n, rv) ==
    IF s.par = "unknown" THEN FALSE                                      \* 9276-9278 PAR fence
    ELSE IF VersionGT(s.cfg, rv.cfg) \/ ConfigConflicts(s.cfg, rv.cfg) THEN FALSE
    ELSE IF KName(rv.from) /= n /\ rv.cterm < s.term THEN FALSE           \* 9286-9290
    ELSE IF Len(s.log) < 1 THEN TRUE                                     \* 9291-9294
    ELSE IF rv.llt > LastTermL(s.log, s.base) THEN TRUE                  \* 9295-9297
    ELSE IF rv.llt < LastTermL(s.log, s.base) THEN FALSE                 \* 9298-9300
    ELSE rv.lli >= Len(s.log)                                            \* 9301-9304

\* tallyPreVote (tube/tube.go:6978-7134)
TallyPreVote(s0, n, v) ==
    LET s == SeenFrom(s0, n, v.from, v.cfg, v.vterm) IN                  \* 6980-6983
    IF v.cterm /= s.pvTerm THEN s                                        \* 6985-6987
    ELSE IF s.role = Leader \/ s.role = Candidate THEN s                 \* 6988-6993
    ELSE IF s.pvTerm /= s.term + 1 THEN ResetPreVote(s)                  \* 6994-6997
    ELSE IF VersionGT(v.cfg, s.cfg) \/ ConfigConflicts(v.cfg, s.cfg) THEN s
    ELSE IF KName(v.from) \notin s.cfg.mem THEN s                        \* 7010-7013
    ELSE LET voter == KName(v.from)
             rec == /\ v.cand = <<n, inc[n]>>                            \* 7015
                    /\ voter \notin (s.pvYes \cup s.pvNo)                \* 7016-7017 de-dup by NAME
                    /\ (s.ca[v.from] /= NoCkt \/ v.from = <<n, inc[n]>>) \* 7018-7020 alive
             s1 == IF ~rec THEN s
                   ELSE IF v.granted THEN [s EXCEPT !.pvYes = @ \cup {voter}]
                   ELSE [s EXCEPT !.pvNo = @ \cup {voter}]
             q == RequiredVotes(s1.cfg.mem, s1.startup)
         IN IF Cardinality(s1.pvYes) >= q                                \* 7029-7041 phase 2 armed
            THEN [s1 EXCEPT !.pv2Term = s1.pvTerm, !.pvTerm = 0]
            ELSE IF Cardinality(s1.pvNo) >= q THEN ResetPreVote(s1)      \* 7042-7044
            ELSE IF Cardinality(s1.pvYes) + Cardinality(s1.pvNo) >= Cardinality(s1.cfg.mem)
                 THEN ResetPreVote(s1)                                   \* 7045-7047
            ELSE s1

\* beginPreVote (tube/tube.go:11167-11335).  The phase-1 deadline equals the
\* election timer that fires it, so phase 1 is never "going" here; phase 2
\* is going while its timer is armed (pv2Term > 0).
BeginPreVote(s, n) ==
    IF s.campaign.held THEN RememberCampaign(s, "preVote")
    ELSE IF s.par = "unknown" THEN s                                          \* 11168-11170
    ELSE IF n \in s.sh THEN s                                            \* 11171-11173 amShadowReplica
    ELSE IF n \notin s.cfg.mem THEN s                                    \* 11174-11179
    ELSE
      LET pt  == s.term + 1                                              \* 11180
          s1  == IF s.role = Candidate THEN [s EXCEPT !.role = Follower] ELSE s   \* 11181-11183
          phase2going == s1.pv2Term > 0
          newer == pt > s1.pvTerm /\ pt > s1.pv2Term                     \* 11188-11190
      IN IF phase2going /\ ~newer THEN s1                                \* 11191-11198
         ELSE
           LET s2 == [s1 EXCEPT !.pvTerm = pt, !.pv2Term = 0,            \* 11201-11207
                                !.pvYes = {n}, !.pvNo = {}]              \* 11208-11216 member: self pre-vote
               rv == [type |-> "RPV", from |-> <<n, inc[n]>>, cterm |-> pt,
                      lli |-> Len(s2.log), llt |-> LastTermL(s2.log, s2.base), cfg |-> s2.cfg]
           IN IF Cardinality(s2.cfg.mem) <= 1                            \* 11230-11254 single node
              THEN TallyPreVote(s2, n, [type |-> "PV", from |-> <<n, inc[n]>>, cand |-> <<n, inc[n]>>,
                                        cterm |-> pt, granted |-> TRUE, cfg |-> s2.cfg, vterm |-> s2.term])
              ELSE AddOutSet(s2, {[c |-> s2.ca[k], m |-> rv] : k \in s2.rep})   \* 11265-11324 over cktReplica only

\* candidateReinitFollowerInfo (tube/tube.go:9617-9622)
ReinitPeers(s) == [s EXCEPT !.peers = [k \in Keys |-> IF k \in s.rep THEN FreshPeer ELSE Nil]]

\* replicateTicket for a leader-created entry (tube/tube.go:6147-6349) once
\* it is allowed through (noop0, or after noop0 committed): stamp, WAL
\* append + fsync + identifiers (6262; wal.go:486-588), then broadcast
\* (6343-6345) or, for a single-node cluster, commit at once (6324-6340).
\* replicateTicket: sole-voter commit runs after saveRaftLogEntry returns.
CommitLocalTail(s) ==
    LET idx == Len(s.log)
        e == s.log[idx]
    IN CommitWhatWeCan([s EXCEPT !.ci = idx, !.cit = s.term,
                         !.newCommitted = @ \cup {[idx |-> idx, term |-> e.term, id |-> e.id]}], TRUE)

ReplicateCommand(s, n, e) ==
    LET idx  == Len(s.log) + 1
        prev == Len(s.log)
        prvT == LastTermL(s.log, s.base)
        s1   == [s EXCEPT !.log = Append(@, e), !.idLen = idx, !.high = idx, !.nid = @ + 1,
                          !.noop0 = IF e.kind = "noop" THEN [id |-> s.nid, done |-> FALSE] ELSE @]
    IN IF s1.cfg.mem /= {n}
       THEN BcastAppendEntries(s1, n, <<WireEntry(e, idx)>>, prev, prvT)
       ELSE IF e.kind = "noop" THEN [s1 EXCEPT !.campaign.resume = "commitNoop"]
       ELSE CommitLocalTail(s1)

ReplicateEntry(s,n,kind) ==
    ReplicateCommand(s,n,[term |-> s.term, id |-> s.nid, kind |-> kind])

\* becomeLeader (tube/tube.go:7298-7479), up to and including the noop0
\* broadcast.  The state save (7473-7475) runs AFTER the broadcast: modeled
\* by blk[n].save and BecomeLeaderSave. Multi-voter candidates have already
\* persisted their term/vote before collecting votes.
BecomeLeader(s, n) ==
    IF s.par = "unknown" THEN s
    ELSE IF s.par = "ready" THEN                                         \* parBecomeLeader
        [ResetPreVote(s) EXCEPT !.role = Leader, !.leader = <<n, inc[n]>>,
                                !.newLeaders = @ \cup {[term |-> s.term, name |-> n, inc |-> inc[n]]}]
    ELSE IF s.role = Leader THEN Panic(s, "BecomeLeaderAlreadyLeader")    \* 7302-7304
    ELSE
      LET ct == s.cfg.ct
          how == IF s.term < ct THEN "panic"                             \* 7317-7319
                 ELSE IF ct /= s.term THEN "rewrite"                     \* 7321-7329
                 ELSE IF n \in s.cfg.mem /\ Cardinality(s.cfg.mem) = 1 THEN "single"  \* 7331-7334
                 ELSE "panic"                                            \* 7335-7337 "already equal?"
      IN IF how = "panic" THEN Panic(s, "BecomeLeaderConfigTerm")
         ELSE
           LET t1 == IF how = "single" THEN s.term + 1 ELSE s.term
               s1 == [s EXCEPT !.role = Leader, !.leader = <<n, inc[n]>>, !.term = t1,   \* 7307-7313
                               !.cfg.ct = t1,                            \* 7328 MC.ConfigTerm := CurrentTerm
                               !.pvTerm = 0, !.pv2Term = 0, !.pvYes = {}, !.pvNo = {},   \* 7306
                               !.pong = [all |-> {}, fresh |-> {}],     \* 7357 leaderFullPongPQ.deleteAll
                               !.aged = FALSE,
                               !.newLeaders = @ \cup {[term |-> t1, name |-> n, inc |-> inc[n]]},
                               !.newCfgHist = @ \cup {[ct |-> t1, cv |-> s.cfg.cv, mem |-> s.cfg.mem]}]
               s2 == ReplicateEntry(s1, n, "noop")                       \* 7372-7378 noop0
           IN IF s2.campaign.resume = "commitNoop" THEN s2
              ELSE [TakeOverDataTickets(s2, 1) EXCEPT !.save2 = TRUE]                                 \* 7473-7475 deferred save

\* tube.go: beginElection/persistSelfVote. A singleton goes directly to
\* becomeLeader. A multi-voter campaign durably records the term and vote
\* before sending any RequestVote. A crash may occur immediately after fsync.
BeginElection(s, n) ==
    IF s.campaign.held THEN RememberCampaign(s, "election")
    ELSE IF s.par = "unknown" \/ n \notin s.cfg.mem THEN s
    ELSE LET s0 == ResetPreVote(s) IN
         IF s0.cfg.mem = {n} THEN BecomeLeader(s0, n)
         ELSE SaveRaftState([s0 EXCEPT
                    !.term = IF s0.role = Follower THEN @ + 1 ELSE @,
                    !.role = Candidate, !.vote = <<n, inc[n]>>,
                    !.campaign.resume = "requestVotes"])

\* The rest of the same event-loop call after persistSelfVote.
SendRequestVotes(s, n) ==
    LET s1 == ReinitPeers([s EXCEPT !.leader = Nil, !.vYes = {n}, !.vNo = {}])
        rv == [type |-> "RV", from |-> <<n, inc[n]>>, cterm |-> s1.term,
               lli |-> Len(s1.log), llt |-> LastTermL(s1.log, s1.base), cfg |-> s1.cfg]
    IN AddOutSet(s1, {[c |-> s1.ca[k], m |-> rv] : k \in s1.rep})

\* tallyVote (tube/tube.go:7136-7271)
TallyVote(s0, n, v) ==
    LET s == SeenFrom(s0, n, v.from, v.cfg, v.vterm) IN                  \* 7138-7141
    IF v.cterm /= s.term THEN                                            \* 7146-7152
        IF v.cterm < s.term THEN s ELSE BecomeFollower(s, n, v.cterm, v.cfg, TRUE)
    ELSE IF s.role = Leader THEN s                                       \* 7153-7155
    ELSE IF s.role = Follower THEN s                                     \* 7156-7159
    ELSE IF KName(v.from) \notin s.cfg.mem THEN s                        \* 7205-7209
    ELSE IF (VersionGT(v.cfg, s.cfg) \/ ConfigConflicts(v.cfg, s.cfg)) /\ v.granted THEN s
    ELSE IF v.vterm >= s.term /\ v.granted THEN s                        \* 7218-7223 voter's term must be < ours
    ELSE LET voter == KName(v.from)
             rec == /\ v.cand = <<n, inc[n]>>                            \* 7228
                    /\ voter \notin (s.vYes \cup s.vNo)                  \* 7231-7233 de-dup by NAME
                    /\ (s.ca[v.from] /= NoCkt \/ v.from = <<n, inc[n]>>) \* 7235-7237
             s1 == IF ~rec THEN s
                   ELSE [s EXCEPT !.vYes = IF v.granted THEN @ \cup {voter} ELSE @,
                                  !.vNo  = IF v.granted THEN @ ELSE @ \cup {voter},
                                  !.peers[v.from] = IF @ = Nil THEN FreshPeer ELSE @]  \* 7246-7254
             q == RequiredVotes(s1.cfg.mem, s1.startup)
         IN IF Cardinality(s1.vYes) >= q THEN BecomeLeader(s1, n)        \* 7260-7263
            ELSE IF Cardinality(s1.vNo) >= q /\ s1.role /= Follower      \* 7264-7269
                 THEN BecomeFollower(s1, n, s1.term, Nil, FALSE)
            ELSE s1

\* ========================================================================
\* APPEND ENTRIES (follower side)
\* ========================================================================

AdoptLeaderAddresses(s, n, cfg) ==
    LET targets == {m \in (cfg.mem \cap s.cfg.mem) \ {n} :
                       /\ IF cfg.det[m] = Nil THEN FALSE ELSE cfg.det[m].addr \in Addrs /\ s.cfg.det[m] /= cfg.det[m]
                       /\ IF s.bn[m] > 0 THEN EpSt(s.bn[m],n) /= EpOpen ELSE TRUE}
        s1 == [s EXCEPT !.cfg.det = [m \in Server |-> IF m \in targets THEN cfg.det[m] ELSE @[m]]]
    IN CIBSet(s1, n, targets)

\* tube.go: aeMemberConfigHelper. Copy usable addresses for equivalent
\* membership, ignore older numbers, and accept the leader's set on a tie.
AEMemberConfigHelper(before,n,ae) ==
    LET s == [before EXCEPT !.known = MergeKnown(@,ae.cfg.det,ae.cfg.mem)] IN
    IF VersionEq(s.cfg,ae.cfg)
    THEN AdoptLeaderAddresses([s EXCEPT !.cfg.com = @ \/ ae.cfg.com],n,ae.cfg)
    ELSE IF VersionGT(s.cfg,ae.cfg) THEN s
    ELSE LET s1 == InstallMC(ShadowsFromAE(s,ae),n,ae.cfg,TRUE)
         IN ResubmitStalledMC(ConnectToMC(s1,n),n)

\* tube/wal.go, overwriteEntries. Clamp at the compacted boundary before
\* testing the first physical entry that would be killed. The caller sends
\* a snapshot-gap rejection if it is committed or already applied.
OverwriteProtected(s, keep, endIdx, priorCommit) ==
    LET k == Max(keep, s.base.idx)
    IN endIdx > s.base.idx /\ k < Len(s.log) /\ k < Max(priorCommit, s.la)

OverwriteEntries(lg, bs, keepIdx, es) ==
    LET barrier == bs.idx
        nes == Len(es)
        conv(x) == LogEntry(x)
        write(k, xs) == SubSeq(lg, 1, Min(k, Len(lg))) \o [j \in 1..Len(xs) |-> conv(xs[j])]
    IN IF keepIdx < barrier THEN
          IF keepIdx + nes <= barrier THEN lg                            \* 834-841
          ELSE write(barrier, SubSeq(es, barrier - keepIdx + 1, nes))    \* 846-856
       ELSE write(keepIdx, es)

\* AppendEntriesAck sent by handleAppendEntries
\* gx: LCRI computed by TermsRLE.Extends; gt: true common prefix by entry id
\* with the leader log the AE was built from (ghosts for LcriSound).
AckMsg(s, n, ae, rej, needSnap, lcri, last, lastTerm, cfg, gx, gt) ==
    [type |-> "AEA", from |-> <<n, inc[n]>>, term |-> s.term, rej |-> rej,
     needSnap |-> needSnap, lcri |-> lcri, last |-> last, lastTerm |-> lastTerm,
     cfg |-> cfg, leader |-> ae.from, aeTerm |-> ae.term, ping |-> ae.ping,
     prev |-> ae.prev, prevTerm |-> ae.prevTerm,
     beg |-> IF ae.ents = <<>> THEN 0 ELSE ae.ents[1].idx,
     end |-> IF ae.ents = <<>> THEN 0 ELSE ae.ents[Len(ae.ents)].idx,
     commit |-> ae.commit, lli |-> ae.lli, gx |-> gx, gt |-> gt]

CommonPrefix(lg, ids) ==
    MaxSet({j \in 0..Min(Len(lg), Len(ids)) : \A i \in 1..j : lg[i].id = ids[i]})

\* An accepting acknowledgement proves the common prefix capped by the
\* reported physical last index (handleAppendEntriesAck, ad569420).
ClaimAck(s, last) ==
    LET cl == {[idx |-> i, id |-> s.log[i].id] : i \in 1..Min(last, Len(s.log))}
    IN [s EXCEPT !.ackH = IF @.term = s.term THEN [@ EXCEPT !.claims = @ \cup cl]
                          ELSE [term |-> s.term, claims |-> cl]]

\* tube/tube.go, advanceFollowerCommit. Use the term at the actual commit
\* point. Only the leader's own commit point has a supplied fallback term.
AdvanceFollowerCommit(s, ae, verified) ==
    LET ci == Min(ae.commit, verified)
    IN IF ci <= s.ci THEN s
       ELSE LET local == VisibleTerm(s.log, s.base, ci)
                ct == IF local < 0 /\ ci = ae.commit THEN ae.commitTerm ELSE local
            IN IF ct <= 0 THEN s
               ELSE [s EXCEPT !.ci = ci, !.cit = ct,
                         !.cfg.ci = IF ci > s.cfg.ci THEN ci ELSE @,
                         !.cfg.cit = IF ci > s.cfg.ci THEN ct ELSE @]

\* tube/tube.go, handleAppendEntries. Term and predecessor rejection happen
\* before either commit advance. A verified prefix can be applied on return
\* even when a heartbeat or later write path requests more data.
AEStep(raw, n, ae) ==
    LET s0 == ObserveRestartDeadline(raw) IN
    LET s1 == SuppressWatchdogs(s0, n, ae)
        X == Extends(ae.rle, RLEOf(s1.log, s1.base))
        last0 == PhysLastIdx(s1.log, s1.base)
        lastT0 == PhysLastTerm(s1.log, s1.base)
        gt0 == CommonPrefix(s1.log, ae.gids)
        s2 == IF ae.term > s1.term THEN [s1 EXCEPT !.leader = ae.from] ELSE s1
    IN IF ae.term < 1 THEN [s |-> s1, branch |-> "InvalidTerm", pre |-> s1, wlog |-> Nil, ack |-> Nil]
    ELSE IF ae.term < s2.term THEN
        LET lc == IF s2.base.idx > 0 THEN s2.base.idx ELSE X.lcri IN
        [s |-> s2, branch |-> "RejectTerm", pre |-> s2, wlog |-> Nil,
         ack |-> AckMsg(s2,n,ae,TRUE,FALSE,lc,last0,lastT0,s2.cfg,X.lcri,gt0)]
    ELSE
    LET stickyState == [s2 EXCEPT !.sticky = TRUE]
        stepped == IF ae.term = s2.term /\ s2.role = Leader
                   THEN BecomeFollower([stickyState EXCEPT !.leader = ae.from], n, ae.term, Nil, FALSE)
                   ELSE stickyState
        cfgState == AEMemberConfigHelper(stepped,n,ae)
        follow == IF ae.term > cfgState.term THEN BecomeFollower(cfgState,n,ae.term,ae.cfg,TRUE)
                  ELSE IF cfgState.role = Candidate THEN [cfgState EXCEPT !.role = Follower]
                  ELSE cfgState
        sx == [ResetPreVote(follow) EXCEPT !.leader = ae.from]
        nes == Len(ae.ents)
        eBeg == IF nes > 0 THEN ae.ents[1].idx ELSE 0
        eEnd == IF nes > 0 THEN ae.ents[nes].idx ELSE 0
        M == LogsAreMismatched(sx.log, sx.base, ae)
        prevWrong == ae.prev > 0 /\ ae.prev >= sx.base.idx /\
                        VisibleTerm(sx.log, sx.base, ae.prev) /= ae.prevTerm
        first == AdvanceFollowerCommit(sx, ae, X.lcri)
        from == Len(sx.log) + 1 - eBeg
        ackE(rej, ns) == AckMsg(sx,n,ae,rej,ns,X.lcri,last0,lastT0,sx.cfg,X.lcri,gt0)
        early(b, a) ==
            LET sa == IF first.ci > sx.ci THEN CommitWhatWeCan(first,FALSE) ELSE first IN
            [s |-> IF a.rej THEN sa ELSE ClaimAck(sa,Min(a.lcri,a.last)), branch |-> b,
             ack |-> a, pre |-> first, wlog |-> Nil]
        tail(b, lg2) ==
            LET sw == [first EXCEPT !.log = lg2, !.idLen = Len(lg2),
                                    !.high = IF b = "ExtendsNoData" THEN @ ELSE PhysLastIdx(lg2,first.base)]
                sa == CommitWhatWeCan(AdvanceFollowerCommit(sw,ae,Max(X.lcri,eEnd)),FALSE)
                l1 == PhysLastIdx(sa.log, sa.base)
                t1 == PhysLastTerm(sa.log, sa.base)
                exact == l1 = EndiR(ae.rle) /\ t1 = LastTermR(ae.rle)
                X2 == Extends(ae.rle, RLEOf(sa.log, sa.base))
                lc == IF exact THEN l1 ELSE IF X2.lcri = 0 THEN sa.base.idx ELSE X2.lcri
                a == AckMsg(sa,n,ae,FALSE,IF exact THEN X.snap ELSE X2.snap,lc,l1,t1,sx.cfg,
                            IF exact THEN l1 ELSE X2.lcri,CommonPrefix(sa.log,ae.gids))
            IN [s |-> ClaimAck(sa,Min(lc,l1)), branch |-> b, ack |-> a,
                pre |-> sx, wlog |-> lg2]
    IN IF M.rej \/ prevWrong THEN
                [s |-> sx, branch |-> "Mismatch", pre |-> sx, wlog |-> Nil,
                 ack |-> ackE(TRUE, X.snap \/ ae.prev > X.lcri)]
       ELSE IF X.lcri = EndiR(ae.rle) THEN early("UpToDate", ackE(FALSE,X.snap))
       ELSE IF nes = 0 THEN early("HeartbeatReject", ackE(TRUE,X.snap))
       ELSE IF ~X.ext /\ ae.prev > X.lcri THEN early("GapReject", ackE(TRUE,X.snap))
       ELSE IF ~X.ext /\ eBeg > X.lcri + 1 THEN early("CannotGap", ackE(TRUE,X.snap))
       ELSE IF ~X.ext /\ eEnd <= X.lcri THEN early("NothingNew", ackE(FALSE,X.snap))
       ELSE IF ~X.ext THEN
              IF OverwriteProtected(first,X.lcri,eEnd,s1.ci)
              THEN early("Protected", ackE(TRUE,TRUE))
              ELSE tail("Overwrite",OverwriteEntries(first.log,first.base,X.lcri,
                                                      SubSeq(ae.ents,X.lcri+2-eBeg,nes)))
       ELSE IF from >= nes THEN tail("ExtendsNoData",first.log)
       ELSE IF from < 0 THEN early("ExtendsGap",ackE(FALSE,X.snap))
       ELSE tail("ExtendsAppend",OverwriteEntries(first.log,first.base,Len(first.log),
                                                           SubSeq(ae.ents,from+1,nes)))

\* parObserveAE (par.go:2835-2851): the only AE processing of a PAR-blocked node
ParObserveAE(raw, n, ae) ==
    LET s == ObserveRestartDeadline(raw) IN
    IF ae.term < s.term THEN s
    ELSE LET conf == IF VersionGT(ae.cfg, s.cfg) THEN ae.cfg ELSE Nil
             s1 == IF ae.term > s.term \/ s.role /= Follower \/ conf /= Nil
                   THEN BecomeFollower(s, n, ae.term, conf, TRUE) ELSE s
         IN [s1 EXCEPT !.leader = ae.from, !.sticky = TRUE]

\* ========================================================================
\* APPEND ENTRIES ACK (leader side)
\* ========================================================================

\* tube/tube.go, handleAppendEntriesAck. LastHeardAnything includes stale
\* correlations. Lease credit needs the matching ping and its send-time
\* window; the pong queue uses its receipt time and current voting membership.

AckStep(s0, n, ack, c) ==
    IF s0.par /= "ok" THEN                                           \* 9848-9853
        (IF ack.term > s0.term THEN BecomeFollower(s0, n, ack.term, ack.cfg, TRUE) ELSE s0)
    ELSE
    LET s1 == SeenAck(s0, n, ack.from, ack.cfg, ack.term) IN            \* 9875-9882
    IF ack.term > s1.term THEN BecomeFollower(s1, n, ack.term, ack.cfg, TRUE)   \* 9887-9893
    ELSE IF ack.term < s1.term THEN s1                                   \* 9894-9902
    ELSE IF s1.role /= Leader THEN s1                                    \* 9903-9905
    ELSE IF n \notin s1.cfg.mem /\ s1.cfg.mem /= {} /\ CurMCCommitted(s1, n)     \* 9912-9932 removed leader
         THEN BecomeFollower([OnLeaderIsCurrentMCcommitted(s1, n) EXCEPT !.leader = Nil],
                             n, s1.term, Nil, TRUE)
    ELSE IF ack.leader /= <<n, inc[n]>> THEN s1                          \* 9938-9942
    ELSE
      LET k == ack.from
          heard == MarkPeerHeard(s1,k)
          prev == heard.peers[k]
          match == ack.ping /= 0 /\ ack.ping = prev.ping
          s2 == [heard EXCEPT
                    !.peers[k].lcri = IF ack.rej THEN ack.lcri ELSE Max(@,ack.lcri),
                    !.peers[k].credit = IF match THEN prev.pingFresh ELSE @,
                    !.peers[k].pingFresh = IF match THEN FALSE ELSE @,
                    !.pong = IF match /\ KName(k) \in heard.cfg.mem THEN RecordPong(@,k) ELSE @]
          proof == IF ack.rej THEN ack.lcri ELSE Min(ack.lcri, ack.last)
          grow == ack.aeTerm = s2.term /\ proof > prev.mi
          forceCheck == ~ack.rej /\ s2.startup = "recheck"
          progress == [s2 EXCEPT !.peers[k].mi = IF grow THEN proof ELSE @,
                                 !.startup = IF forceCheck THEN "majority" ELSE @]
          s3 == IF grow \/ forceCheck THEN LeaderAdvanceCommitIndex(progress, n) ELSE progress
      IN IF ack.rej /\ ack.needSnap THEN HandleRequestStateSnapshot(s2,n,c)
         ELSE IF ack.rej THEN SendAppendEntriesAfter(s3,n,k,ack.lcri+1,ack)
         ELSE IF s3.role = Leader /\ s3.peers[k] /= Nil /\
                 PhysLastIdx(s3.log,s3.base) > s3.peers[k].lcri
              THEN SendAppendEntriesTo(s3,n,k,s3.peers[k].lcri+1) ELSE s3


\* ========================================================================
\* STATE SNAPSHOT INSTALL (follower side)
\* ========================================================================

\* parSnapshotAllowed (par.go:2656-2680)
ParSnapshotAllowed(s, n, sn) ==
    /\ ~VersionGT(s.cfg, sn.st.cfg)                                      \* 2660-2662
    /\ (s.par /= "ok" =>                                             \* 2663-2678
          /\ s.role = Follower /\ s.leader /= Nil /\ sn.from = s.leader
          /\ sn.st.term = s.term /\ sn.st.la >= ParProtected(s)
          /\ sn.st.ci >= sn.st.la
          /\ VersionEq(sn.st.cfg, s.cfg))

\* tube.go applyNewStateSnapshot uses the embedded AE to distinguish an older
\* complete prefix from a snapshot that must actually replace the WAL.
CoveredSnapshot(s, sn) ==
    /\ s.par = "ok"
    /\ EndiR(sn.ae.rle) <= Len(s.log)
    /\ Extends(sn.ae.rle, RLEOf(s.log, s.base)).lcri >= EndiR(sn.ae.rle)

ApplySnapshot(s, n, sn) ==
    IF ~ParSnapshotAllowed(s, n, sn) THEN [s |-> s, ack |-> Nil, branch |-> "Refused"]   \* 17696-17698
    ELSE IF sn.st.term /= s.term THEN [s |-> s, ack |-> Nil, branch |-> "TermMismatch"]   \* 17779-17782
    ELSE IF CoveredSnapshot(s, sn) THEN
      LET r == AEStep(s, n, sn.ae)
      IN [s |-> r.s, ack |-> r.ack, branch |-> "Covered"]
    ELSE
      LET st == sn.st
          inst == [s EXCEPT !.ci = st.ci, !.cit = st.cit, !.la = st.la, !.lat = st.lat,    \* 17800-17805
                          !.appl = st.appl,                              \* 17819-17832 KV replaced
                          !.known = MergeKnown(MergeKnown(@, st.known, Server), st.cfg.det, st.cfg.mem),
                          !.cfg = st.cfg, !.sh = st.sh,                  \* 17834-17839
                          !.base = st.base,                              \* 17857
                          !.log = st.hist, !.idLen = Len(st.hist),       \* 17879 wal.installedSnapshot
                          !.par = "ok",                                  \* 17793-17797 PAR recovered
                          !.high = IF s.par = "ok" THEN PhysLastIdx(s.log, s.base) ELSE s.high,
                          !.newCfgHist = @ \cup {[ct |-> st.cfg.ct, cv |-> st.cfg.cv, mem |-> st.cfg.mem]}]
          saved == SaveRaftState(inst)
          s1 == IF s.par = "ok" THEN saved ELSE [saved EXCEPT !.high = 0]
          r  == AEStep(s1, n, sn.ae)                                     \* 17882-17887 handleAppendEntries(snap.AE, true)
      IN [s |-> r.s, ack |-> r.ack, branch |-> "Installed"]

\* ========================================================================
\* WRITE-BACK OF ONE EVENT-LOOP STEP
\* ========================================================================

RECURSIVE ApplyCloses(_, _, _, _)

\* A close with a reason also sends END. Fragment.ToMessage preserves the
\* type and transmits the reason separately in JobErrs (ckt.go; pump.go).

ApplyCloses(cn, ck, n, cl) ==
    IF cl = <<>> THEN [cn |-> cn, ck |-> ck]
    ELSE LET x == Head(cl)
             openNow == EpStOf(ck, x.c, n) \in {EpNew,EpOpen}
             msg == [type |-> "END", from |-> <<n, inc[n]>>]
         IN ApplyCloses(IF openNow THEN BgSendOne(cn, n, x.c, msg) ELSE cn,
                        IF openNow THEN SetEp(ck, x.c, n, IF EpStOf(ck,x.c,n) = EpNew THEN "newdead" ELSE EpClosed) ELSE ck, n, Tail(cl))

ApplySeen(ck, n, ov) ==
    [c \in DOMAIN ck |->
        IF \E x \in ov : x.c = c
        THEN LET v == (CHOOSE x \in ov : x.c = c).v IN
             IF ck[c].d = n THEN [ck[c] EXCEPT !.dSeen = v] ELSE [ck[c] EXCEPT !.aSeen = v]
        ELSE ck[c]]

RECURSIVE SetToSeq(_)
SetToSeq(S) == IF S = {} THEN <<>>
               ELSE LET x == CHOOSE x \in S : TRUE IN <<x>> \o SetToSeq(S \ {x})

\* Publish the final volatile state and the last saved RaftState separately.
PublishAdminOutcome(s,t,status) ==
    IF s.book[t].origin = s.self THEN FinishDataTicket(s,t,status,0,"local")
    ELSE TicketReply([s EXCEPT !.book[t].stage[s.self] = "none"],t,status,0)

\* SingleUpdateClusterMemberConfig submits voting requests through changeMembership.
\* Shadow requests instead go through writeReqCh, ordinary batching and Raft apply.
\* A forwarded ticket retains its origin time, command and address.
AdmitAdminRequest(s,t) ==
    LET q == s.book[t]
        cmd == q.command
        n == KName(s.self)
    IN IF cmd.kind \in {"add","remove"}
       THEN LET fresh == q.mcs[s.self] = Nil
                id == IF fresh THEN <<n,inc[n],s.mc.issued+1>> ELSE q.mcs[s.self]
                proposal == [op |-> cmd.kind, x |-> cmd.x, addr |-> cmd.addr, force |-> FALSE, id |-> id]
                ready == [s EXCEPT !.mc.issued = IF fresh THEN @+1 ELSE @,
                                   !.mc.expired = IF q.old THEN @ \cup {id} ELSE @,
                                   !.book[t].mcs[s.self] = id, !.book[t].stage[s.self] = "logless"]
            IN IF TicketFinished(ready,proposal) THEN ready ELSE ChangeMembership(ready,n,proposal)
       ELSE IF (cmd.kind = "shadowAdd" /\ cmd.x \in s.cfg.mem) \/
               (cmd.kind = "shadowRemove" /\ cmd.x \notin s.sh)
            THEN PublishAdminOutcome(s,t,"invalidShadow")
            ELSE LET ready == IF cmd.kind = "shadowRemove" THEN ForgetShadowCircuit(s,cmd.x) ELSE s
                 IN [ready EXCEPT !.book[t].stage[s.self] = IF s.noop0.done THEN "batch" ELSE "unindexed"]

\* Publish the result of the underlying logless state machine as a local
\* return or a response frame. A timeout leaves the installed config intact.
RECURSIVE DispatchAdminRequests(_, _)
DispatchAdminRequests(s,t) ==
    IF t > Len(s.book) THEN s
    ELSE LET s1 == IF s.book[t].stage[s.self] = "adminAdmission" THEN AdmitAdminRequest(s,t) ELSE s
             id == s1.book[t].mcs[s1.self]
             errs == {f \in s1.mc.failed : f.id = id}
             s2 == IF s1.book[t].stage[s1.self] /= "logless" THEN s1
                   ELSE IF \E a \in s1.newAcked \cup ackedCfg : a.id = id
                        THEN PublishAdminOutcome(s1,t,"configCommitted")
                   ELSE IF id \in s1.mc.noChange THEN PublishAdminOutcome(s1,t,"unchanged")
                   ELSE IF errs /= {} THEN PublishAdminOutcome(s1,t,(CHOOSE f \in errs : TRUE).reason)
                   ELSE s1
         IN DispatchAdminRequests(s2,t+1)

WriteBack(n, rawState, cn0, ck0) ==
    LET s == DispatchAdminRequests(rawState,1) IN
    \E slowAt \in 0..Len(s.out) :
    LET st == SendBurstAt(cn0, ck0, n, s.out, slowAt)
        cl == ApplyCloses(st.cn, ck0, n, s.closes)
    IN
    /\ role'            = [role EXCEPT ![n] = s.role]
    /\ currentTerm'     = [currentTerm EXCEPT ![n] = s.term]
    /\ votedFor'        = [votedFor EXCEPT ![n] = s.vote]
    /\ leaderKey'       = [leaderKey EXCEPT ![n] = s.leader]
    /\ sticky'          = [sticky EXCEPT ![n] = s.sticky]
    /\ campaign'        = [campaign EXCEPT ![n] = s.campaign]
    /\ pvTerm'          = [pvTerm EXCEPT ![n] = s.pvTerm]
    /\ pv2Term'         = [pv2Term EXCEPT ![n] = s.pv2Term]
    /\ pvYes'           = [pvYes EXCEPT ![n] = s.pvYes]
    /\ pvNo'            = [pvNo EXCEPT ![n] = s.pvNo]
    /\ vYes'            = [vYes EXCEPT ![n] = s.vYes]
    /\ vNo'             = [vNo EXCEPT ![n] = s.vNo]
    /\ peers'           = [peers EXCEPT ![n] = s.peers]
    /\ pong'            = [pong EXCEPT ![n] = s.pong]
    /\ leaderAged'      = [leaderAged EXCEPT ![n] = s.aged]
    /\ startupPhase' = [startupPhase EXCEPT ![n] = s.startup]
    /\ noop0'           = [noop0 EXCEPT ![n] = s.noop0]
    /\ stallQ'          = [stallQ EXCEPT ![n] = s.stallQ]
    /\ mcPending'       = [mcPending EXCEPT ![n] = s.mc]
    /\ config'          = [config EXCEPT ![n] = s.cfg]
    /\ knownPeers'      = [knownPeers EXCEPT ![n] = s.known]
    /\ shadows'         = [shadows EXCEPT ![n] = s.sh]
    /\ commitIndex'     = [commitIndex EXCEPT ![n] = s.ci]
    /\ commitIndexTerm' = [commitIndexTerm EXCEPT ![n] = s.cit]
    /\ lastApplied'     = [lastApplied EXCEPT ![n] = s.la]
    /\ lastAppliedTerm' = [lastAppliedTerm EXCEPT ![n] = s.lat]
    /\ snapSlot'        = [snapSlot EXCEPT ![n] = s.slot]
    /\ log'             = [log EXCEPT ![n] = s.log]
    /\ base'            = [base EXCEPT ![n] = s.base]
    /\ idLen'           = [idLen EXCEPT ![n] = s.idLen]
    /\ par'             = [par EXCEPT ![n] = s.par]
    /\ torn'            = IF s.par = "ok" THEN [torn EXCEPT ![n] = <<>>] ELSE torn
    /\ cktall'          = [cktall EXCEPT ![n] = s.ca]
    /\ byName'          = [byName EXCEPT ![n] = s.bn]
    /\ pendAddr'        = [pendAddr EXCEPT ![n] = s.pa]
    /\ wdReq'           = [wdReq EXCEPT ![n] = s.wd]
    /\ watchSilent'     = [watchSilent EXCEPT ![n] = s.silent]
    /\ observer'        = [observer EXCEPT ![n] = s.obs]
    /\ cktReplica'      = [cktReplica EXCEPT ![n] = s.rep]
    /\ audit'           = [audit EXCEPT ![n] = s.aud]
    /\ applied'         = [applied EXCEPT ![n] = s.appl]
    /\ ackHigh'         = [ackHigh EXCEPT ![n] = s.ackH]
    /\ conn'            = cl.cn
    /\ ckt'             = ApplySeen(cl.ck, n, s.seenOv)
    /\ blk'             = [blk EXCEPT ![n] = [st.bl EXCEPT !.save = s.save2]]
    /\ dTerm'           = IF s.save THEN [dTerm EXCEPT ![n] = s.saved.term] ELSE dTerm
    /\ dVote'           = IF s.save THEN [dVote EXCEPT ![n] = s.saved.vote] ELSE dVote
    /\ dCfg'            = IF s.save THEN [dCfg EXCEPT ![n] = s.saved.cfg] ELSE dCfg
    /\ dKnownPeers' = IF s.save THEN [dKnownPeers EXCEPT ![n] = s.saved.known] ELSE dKnownPeers
    /\ dShadows'        = IF s.save THEN [dShadows EXCEPT ![n] = s.saved.sh] ELSE dShadows
    /\ dCommit'         = IF s.save THEN [dCommit EXCEPT ![n] = s.saved.ci] ELSE dCommit
    /\ appendWitness'   = [appendWitness EXCEPT ![n] = [current |-> s.high, saved |-> s.savedHigh]]
    /\ dCommitTerm' = IF s.save THEN [dCommitTerm EXCEPT ![n] = s.saved.cit] ELSE dCommitTerm
    /\ walGone'         = [walGone EXCEPT ![n] = @ /\ s.par = "unknown"]   \* 17792-17797 files recreated
    /\ committed'       = committed \cup s.newCommitted
    /\ panics'          = panics \cup s.newPanics
    /\ adminEvents'     = adminEvents \cup s.newAdmin
    /\ cfgHist'         = cfgHist \cup s.newCfgHist
    /\ ackedCfg'        = ackedCfg \cup s.newAcked
    /\ leaders'         = leaders \cup s.newLeaders
    /\ nextId'          = s.nid
    /\ clientTickets'   = s.book
    /\ nextSid'         = s.sid
    /\ UNCHANGED <<up, inc, addr, stopped, link, nextConn, nextCkt>>

\* LastHeardAnything is a timestamp sampled at AE construction. Each
\* reported observation can cease to be recent before the queued AE or
\* its enclosing snapshot reaches the handler. Abstracting thresholds
\* independently admits both timely and delayed indirect observations.
AgeGossip(k,dir,i,key) ==
    /\ k \in DOMAIN conn /\ dir \in Dirs /\ i \in 1..Len(conn[k].q[dir])
    /\ LET m == conn[k].q[dir][i].m IN
       CASE m.type = "AE" ->
              /\ key \in m.heard
              /\ conn' = [conn EXCEPT ![k].q[dir][i].m.heard = @ \ {key}]
         [] m.type = "SNAP" ->
              IF m.ae = Nil THEN FALSE
              ELSE /\ key \in m.ae.heard
                   /\ conn' = [conn EXCEPT ![k].q[dir][i].m.ae.heard = @ \ {key}]
         [] OTHER -> FALSE
    /\ UNCHANGED <<envVars, ckt, nextCkt, nextConn, blk, viewVars, raftVars, logVars, durVars, ghostVars>>

\* handleNewCircuit and handleAppendEntries dispatch ticketsAwaitingLeader
\* before returning to the event loop. The separate send steps allow the
\* transport to drain during the dispatch while other Tube events wait.
WriteBackDispatch(n, s, cn, ck) ==
    LET book == [t \in DOMAIN s.book |->
                     IF s.book[t].origin = s.self /\ s.book[t].stashed
                     THEN [s.book[t] EXCEPT !.dispatch = TRUE] ELSE s.book[t]]
    IN WriteBack(n, [s EXCEPT !.book = book], cn, ck)

DataDispatchDue(n) ==
    {t \in DOMAIN clientTickets : clientTickets[t].origin = <<n, inc[n]>> /\ clientTickets[t].dispatch}

\* The event loop can run a handler: alive, not stopped, not inside a
\* pending accepted write, not between becomeLeader's broadcast and save.
CanRunEvent(n) == up[n] /\ ~stopped[n] /\ blk[n].pend = <<>> /\ ~blk[n].save /\ campaign[n].resume = "none"
Running(n) == CanRunEvent(n) /\ DataDispatchDue(n) = {}

\* ========================================================================
\* INITIAL STATE
\* ========================================================================

InitNodes == InitMembers \cup InitShadows

InitCfg == [mem |-> InitMembers, ct |-> 0, cv |-> 1, com |-> TRUE, ci |-> 0, cit |-> 0,
            det |-> [m \in Server |-> IF m \in InitNodes
                                      THEN [inc |-> 0, addr |-> InitAddr] ELSE Nil]]
\* NewMemberConfig for a node that was never part of the cluster
EmptyCfg == [mem |-> {}, ct |-> 0, cv |-> 0, com |-> FALSE, ci |-> 0, cit |-> 0,
             det |-> [m \in Server |-> Nil]]
CfgOf0(n) == IF n \in InitNodes THEN InitCfg ELSE EmptyCfg
ShOf0(n)  == IF n \in InitNodes THEN InitShadows ELSE {}

EmptyFn == [x \in {} |-> 0]
NN == Cardinality(Server)
PairId(x, y) == (Ord[x] - 1) * NN + Ord[y]
MeshCkt(x, y) == IF Ord[x] < Ord[y] THEN PairId(x, y) ELSE PairId(y, x)
MeshPairs == {p \in InitNodes \X InitNodes : Ord[p[1]] < Ord[p[2]]}
MeshIds == {PairId(p[1], p[2]) : p \in MeshPairs}
PairOf(c) == CHOOSE p \in MeshPairs : PairId(p[1], p[2]) = c
Meshed(n, m) == InitMeshed /\ n \in InitNodes /\ m \in InitNodes /\ n /= m
\* startup connectToMC (tube/tube.go:1036) for a cold start
ColdWant(n) == IF ~InitMeshed /\ n \in InitNodes /\ CfgOf0(n).mem /= {}
               THEN (CfgOf0(n).mem \cup ShOf0(n)) \ {n} ELSE {}
ColdAddr(n, m) == IF CfgOf0(n).det[m] /= Nil
                  THEN CfgOf0(n).det[m].addr ELSE "none"

InitialKnown(n) ==
    [m \in Server |-> IF n \in InitNodes /\ (m = n \/ Meshed(n,m))
                      THEN [inc |-> 0, addr |-> InitAddr] ELSE Nil]

CoreInitWithRestored(restored) ==
    /\ restored \subseteq InitNodes
    /\ up      = [n \in Server |-> n \in InitNodes]
    /\ inc     = [n \in Server |-> 0]
    /\ addr    = [n \in Server |-> InitAddr]
    /\ stopped = [n \in Server |-> FALSE]
    /\ link    = [x \in Server |-> [y \in Server |-> "ok"]]
    /\ conn = IF InitMeshed
              THEN [k \in MeshIds |-> [d |-> PairOf(k)[1], di |-> 0, a |-> PairOf(k)[2], ai |-> 0,
                                       ad |-> InitAddr, dd |-> InitAddr, dOpen |-> TRUE, aOpen |-> TRUE,
                                       q |-> [x \in Dirs |-> <<>>], u |-> [x \in Dirs |-> 0],
                                       out |-> [x \in Dirs |-> <<>>], writer |-> [x \in Dirs |-> Nil],
                                       bg |-> [x \in Dirs |-> {}], read |-> [x \in Dirs |-> {}],
                                       closedBy |-> [x \in Dirs |-> Nil]]]
              ELSE EmptyFn
    /\ ckt  = IF InitMeshed
              THEN [c \in MeshIds |-> [d |-> PairOf(c)[1], di |-> 0, dd |-> InitAddr,
                                       a |-> PairOf(c)[2], ai |-> 0, ad |-> InitAddr, k |-> c,
                                       dSt |-> EpOpen, aSt |-> EpOpen, dSeen |-> Nil, aSeen |-> Nil]]
              ELSE EmptyFn
    /\ nextConn = NN * NN + 1
    /\ nextCkt  = NN * NN + 1
    /\ blk = [n \in Server |-> NoBlk]
    /\ cktall = [n \in Server |-> [k \in Keys |->
                    IF Meshed(n, KName(k)) /\ KInc(k) = 0 THEN MeshCkt(n, KName(k)) ELSE NoCkt]]
    /\ byName = [n \in Server |-> [m \in Server |->
                    IF Meshed(n, m) THEN MeshCkt(n, m)
                    ELSE IF m \in ColdWant(n) THEN Pending ELSE NoCkt]]
    /\ pendAddr = [n \in Server |-> [m \in Server |->
                    IF m \in ColdWant(n) THEN ColdAddr(n, m) ELSE "none"]]
    /\ wdReq = [n \in Server |-> [m \in Server |->
                    IF m \in ColdWant(n) THEN ColdAddr(n, m) ELSE Nil]]
    /\ watchSilent = [n \in Server |-> {}]
    /\ observer = [n \in Server |-> ClosedObserver]
    /\ cktReplica = [n \in Server |-> {<<m, 0>> : m \in {x \in Server : Meshed(n, x)}}]
    /\ audit = [n \in Server |-> {MeshCkt(n, m) : m \in {x \in Server : Meshed(n, x)}}]
    /\ role = [n \in Server |-> Follower]
    /\ currentTerm = [n \in Server |-> 0]
    /\ votedFor = [n \in Server |-> Nil]
    /\ leaderKey = [n \in Server |-> Nil]
    /\ sticky = [n \in Server |-> FALSE]
    /\ campaign = [n \in Server |-> NewCampaign(n \in restored, 0, IF n \in InitNodes THEN InitialVoteDelay ELSE 0)]
    /\ pvTerm = [n \in Server |-> 0]
    /\ pv2Term = [n \in Server |-> 0]
    /\ pvYes = [n \in Server |-> {}]
    /\ pvNo = [n \in Server |-> {}]
    /\ vYes = [n \in Server |-> {}]
    /\ vNo = [n \in Server |-> {}]
    /\ peers = [n \in Server |-> [k \in Keys |->
                    IF Meshed(n, KName(k)) /\ KInc(k) = 0 /\ KName(k) \in InitMembers
                    THEN FreshPeer ELSE Nil]]
    /\ pong = [n \in Server |-> [all |-> {}, fresh |-> {}]]
    /\ leaderAged = [n \in Server |-> FALSE]
    /\ startupPhase = [n \in Server |-> IF n \in InitNodes THEN StartQuorumPhase(n) ELSE "majority"]
    /\ noop0 = [n \in Server |-> [id |-> 0, done |-> FALSE]]
    /\ stallQ = [n \in Server |-> <<>>]
    /\ mcPending = [n \in Server |-> EmptyMCTickets]
    /\ config = [n \in Server |-> CfgOf0(n)]
    /\ shadows = [n \in Server |-> ShOf0(n)]
    /\ commitIndex = [n \in Server |-> 0]
    /\ commitIndexTerm = [n \in Server |-> 0]
    /\ lastApplied = [n \in Server |-> 0]
    /\ lastAppliedTerm = [n \in Server |-> 0]
    /\ snapSlot = [n \in Server |-> <<>>]
    /\ log = [n \in Server |-> <<>>]
    /\ base = [n \in Server |-> [idx |-> 0, term |-> 0]]
    /\ idLen = [n \in Server |-> 0]
    /\ torn = [n \in Server |-> <<>>]
    /\ par = [n \in Server |-> "ok"]
    /\ dTerm = [n \in Server |-> 0]
    /\ dVote = [n \in Server |-> Nil]
    /\ dCfg = [n \in Server |-> CfgOf0(n)]
    /\ knownPeers = [n \in Server |-> InitialKnown(n)]
    /\ dKnownPeers = [n \in Server |-> InitialKnown(n)]
    /\ adminEvents = {}
    /\ dShadows = [n \in Server |-> ShOf0(n)]
    /\ dCommit = [n \in Server |-> 0]
    /\ appendWitness = [n \in Server |-> [current |-> 0, saved |-> 0]]
    /\ dCommitTerm = [n \in Server |-> 0]
    /\ walGone = [n \in Server |-> FALSE]
    /\ nextId = 1
    /\ committed = {}
    /\ applied = [n \in Server |-> <<>>]
    /\ leaders = {}
    /\ ackedCfg = {}
    /\ cfgHist = {[ct |-> 0, cv |-> 1, mem |-> InitMembers]}
    /\ panics = {}
    /\ ackHigh = [n \in Server |-> [term |-> 0, claims |-> {}]]
    /\ clientTickets = <<>>
    /\ nextSid = 1

\* ========================================================================
CoreInit == CoreInitWithRestored({})

\* ACTIONS: PROCESS AND NETWORK FAULTS
\* ========================================================================

\* A process dies (kill -9, OOM, power loss, or a panic such as a persistor
\* I/O error, persistor.go:245-291).  Its sockets die; the peers' ends stay
\* open (half-open) until they detect it (ConnAbort).  Volatile state is
\* lost (reloaded by Restart).

CloseSide(p, dir, why) ==
    [p EXCEPT !.dOpen = IF dir = "da" THEN FALSE ELSE @,
              !.aOpen = IF dir = "ad" THEN FALSE ELSE @,
              !.writer[dir] = Nil, !.out[dir] = <<>>, !.bg[dir] = {},
              !.q[OtherDir(dir)] = <<>>, !.u[dir] = 0, !.closedBy[dir] = why]

CrashConn(n) == [k \in DOMAIN conn |->
    IF conn[k].d = n /\ conn[k].di = inc[n] THEN CloseSide(conn[k],"da","process")
    ELSE IF conn[k].a = n /\ conn[k].ai = inc[n] THEN CloseSide(conn[k],"ad","process")
    ELSE conn[k]]

CrashCkt(n) == [c \in DOMAIN ckt |-> [ckt[c] EXCEPT
                    !.dSt = IF ckt[c].d = n /\ ckt[c].di = inc[n] THEN EpGone ELSE @,
                    !.aSt = IF ckt[c].a = n /\ ckt[c].ai = inc[n] THEN EpGone ELSE @]]

Crash(n) ==
    /\ up[n]
    /\ up' = [up EXCEPT ![n] = FALSE]
    /\ stopped' = [stopped EXCEPT ![n] = FALSE]
    /\ conn' = CrashConn(n)
    /\ ckt' = CrashCkt(n)
    /\ blk' = [blk EXCEPT ![n] = NoBlk]
    /\ UNCHANGED <<inc, addr, link, nextConn, nextCkt, viewVars, raftVars, logVars, durVars, ghostVars>>

\* log is the open WAL's view. During recovery torn carries the surviving
\* frames past that view. Re-identification changes the on-disk identity
\* count before parFinish replaces the open handle.
PhysicalHistory(n) == log[n] \o [j \in 1..Len(torn[n]) |-> torn[n][j].e]

\* parLossFloor (tube/par.go) distinguishes the local, saved append witness
\* from the commit index copied in a leader's member configuration.
LossFloor(ci, la, anchor, cfgCI, high) ==
    Max(Max(ci, la), Max(anchor, Min(cfgCI, high)))

\* Recovery starts a new incarnation. A WAL term newer than RaftState is
\* adopted with a durable self-vote before serving. See
\* onRestartRecoverPersistentRaftStateFromDisk and parAfterRestore.
RestartWithMode(n, a, zap) ==
    /\ ~up[n] /\ inc[n] < MaxInc /\ a \in Addrs
    /\ LET disk  == PhysicalHistory(n)
           lg    == SubSeq(disk, 1, idLen[n])
           tseq  == [j \in 1..(Len(disk) - idLen[n]) |->
                        [idx |-> idLen[n] + j, e |-> disk[idLen[n] + j]]]
           ci    == Max(dCommit[n], lastApplied[n])
           cit   == IF ci = dCommit[n] THEN dCommitTerm[n] ELSE lastAppliedTerm[n]
           cfg   == IF zap THEN [EmptyCfg EXCEPT !.ci = ci, !.cit = cit] ELSE dCfg[n]
           sh    == IF zap THEN {} ELSE dShadows[n]
           me    == [inc |-> inc[n] + 1, addr |-> a]
           floor == LossFloor(ci, lastApplied[n], base[n].idx, dCfg[n].ci, appendWitness[n].saved)
           unk   == tseq /= <<>> \/ floor > Len(lg) \/ walGone[n]
           walTerm == IF walGone[n] THEN 0 ELSE LastTermL(lg, base[n])
           recoverTerm == Max(dTerm[n], walTerm)
           recoverVote == IF walTerm > dTerm[n] THEN <<n, inc[n]+1>> ELSE dVote[n]
           want  == IF cfg.mem = {} THEN {} ELSE (cfg.mem \cup sh) \ {n}   \* 15233-15294
           dA(m) == IF cfg.det[m] /= Nil THEN cfg.det[m].addr ELSE "none"
       IN
       /\ up' = [up EXCEPT ![n] = TRUE]
       /\ inc' = [inc EXCEPT ![n] = @ + 1]
       /\ addr' = [addr EXCEPT ![n] = a]
       /\ role' = [role EXCEPT ![n] = Follower]
       /\ currentTerm' = [currentTerm EXCEPT ![n] = recoverTerm]
       /\ votedFor' = [votedFor EXCEPT ![n] = recoverVote]
       /\ dTerm' = [dTerm EXCEPT ![n] = recoverTerm]
       /\ dVote' = [dVote EXCEPT ![n] = recoverVote]
       /\ \E delay \in RestartVoteDelays :
            campaign' = [campaign EXCEPT ![n] =
                NewCampaign(n \in InitNodes \/ inc[n] > 0, campaign[n].persisted, delay)]
       /\ leaderKey' = [leaderKey EXCEPT ![n] = Nil]
       /\ sticky' = [sticky EXCEPT ![n] = FALSE]                         \* stickiness is volatile (D-18)
       /\ pvTerm' = [pvTerm EXCEPT ![n] = 0]
       /\ pv2Term' = [pv2Term EXCEPT ![n] = 0]
       /\ pvYes' = [pvYes EXCEPT ![n] = {}]
       /\ pvNo' = [pvNo EXCEPT ![n] = {}]
       /\ vYes' = [vYes EXCEPT ![n] = {}]
       /\ vNo' = [vNo EXCEPT ![n] = {}]
       /\ peers' = [peers EXCEPT ![n] = [k \in Keys |-> Nil]]           \* 9434, 1035
       /\ pong' = [pong EXCEPT ![n] = [all |-> {}, fresh |-> {}]]
       /\ leaderAged' = [leaderAged EXCEPT ![n] = FALSE]
       /\ startupPhase' = [startupPhase EXCEPT ![n] = StartQuorumPhase(n)]
       /\ noop0' = [noop0 EXCEPT ![n] = [id |-> 0, done |-> FALSE]]
       /\ stallQ' = [stallQ EXCEPT ![n] = <<>>]
       /\ mcPending' = [mcPending EXCEPT ![n] =
              [EmptyMCTickets EXCEPT !.failed = mcPending[n].failed, !.noChange = mcPending[n].noChange,
                                 !.installed = mcPending[n].installed]]
       /\ config' = [config EXCEPT ![n] = IF n \in cfg.mem THEN [cfg EXCEPT !.det[n] = me] ELSE cfg]
       /\ knownPeers' = [knownPeers EXCEPT ![n] = [dKnownPeers[n] EXCEPT ![n] = me]]
       /\ dCfg' = IF zap THEN [dCfg EXCEPT ![n] = cfg] ELSE dCfg
       /\ dShadows' = IF zap THEN [dShadows EXCEPT ![n] = {}] ELSE dShadows
       /\ dCommit' = IF zap THEN [dCommit EXCEPT ![n] = ci] ELSE dCommit
       /\ dCommitTerm' = IF zap THEN [dCommitTerm EXCEPT ![n] = cit] ELSE dCommitTerm
       /\ shadows' = [shadows EXCEPT ![n] = sh]
       /\ commitIndex' = [commitIndex EXCEPT ![n] = ci]
       /\ commitIndexTerm' = [commitIndexTerm EXCEPT ![n] = cit]
       /\ snapSlot' = [snapSlot EXCEPT ![n] = <<>>]
       /\ log' = [log EXCEPT ![n] = lg]
       /\ idLen' = [idLen EXCEPT ![n] = Len(lg)]
       /\ torn' = [torn EXCEPT ![n] = tseq]
       /\ appendWitness' = [appendWitness EXCEPT ![n].current = appendWitness[n].saved]
       /\ par' = [par EXCEPT ![n] = IF unk THEN "unknown" ELSE "ok"]
       /\ cktall' = [cktall EXCEPT ![n] = [k \in Keys |-> NoCkt]]
       /\ byName' = [byName EXCEPT ![n] = [m \in Server |-> IF m \in want THEN Pending ELSE NoCkt]]
       /\ pendAddr' = [pendAddr EXCEPT ![n] = [m \in Server |-> dA(m)]]
       /\ wdReq' = [wdReq EXCEPT ![n] = [m \in Server |-> IF m \in want THEN dA(m) ELSE Nil]]
       /\ cktReplica' = [cktReplica EXCEPT ![n] = {}]
       /\ audit' = [audit EXCEPT ![n] = {}]
       /\ watchSilent' = [watchSilent EXCEPT ![n] = {}]
       /\ observer' = [observer EXCEPT ![n] = ClosedObserver]
       /\ blk' = [blk EXCEPT ![n] = NoBlk]
       /\ UNCHANGED <<stopped, link, conn, ckt, nextConn, nextCkt, lastApplied, lastAppliedTerm,
                      base, dKnownPeers, walGone, ghostVars>>

Restart(n, a) == RestartWithMode(n, a, FALSE)

\* SIGSTOP / wedged process: kernel keeps ACKing (zero window), nothing runs.
Stop(n) ==
    /\ up[n] /\ ~stopped[n]
    /\ stopped' = [stopped EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<up, inc, addr, link, netVars, viewVars, raftVars, logVars, durVars, ghostVars>>

Cont(n) ==
    /\ stopped[n]
    /\ stopped' = [stopped EXCEPT ![n] = FALSE]
    /\ UNCHANGED <<up, inc, addr, link, netVars, viewVars, raftVars, logVars, durVars, ghostVars>>

\* Directional link failure (asymmetric NIC failure / one-way partition).
LinkDown(x, y) ==
    /\ x /= y /\ link[x][y] = "ok"
    /\ link' = [link EXCEPT ![x][y] = "drop"]
    /\ UNCHANGED <<up, inc, addr, stopped, netVars, viewVars, raftVars, logVars, durVars, ghostVars>>

LinkUp(x, y) ==
    /\ link[x][y] = "drop"
    /\ link' = [link EXCEPT ![x][y] = "ok"]
    /\ UNCHANGED <<up, inc, addr, stopped, netVars, viewVars, raftVars, logVars, durVars, ghostVars>>

\* ========================================================================
\* ACTIONS: TRANSPORT (rpc25519 goroutines; not the Tube event loop)
\* ========================================================================

NewConn(n, m, a, q0) ==
    [d |-> n, di |-> inc[n], a |-> m, ai |-> inc[m], ad |-> a, dd |-> addr[n], dOpen |-> TRUE, aOpen |-> TRUE,
     q |-> [x \in Dirs |-> IF x = "da" THEN q0 ELSE <<>>], u |-> [x \in Dirs |-> 0],
     out |-> [x \in Dirs |-> <<>>], writer |-> [x \in Dirs |-> Nil],
     bg |-> [x \in Dirs |-> {}], read |-> [x \in Dirs |-> {}], closedBy |-> [x \in Dirs |-> Nil]]


\* An attempt can fail before START, complete both hand-offs, or leave a
\* START in flight when the dialer times out. This includes crossed healthy
\* dials cancelled by a competing registration. The origin closes a failed
\* local hand-off; if it already got an ACK, END addresses the known peer.
\* ckt2.go implRemotePeerAndGetCircuit/handOffNewCircuit, ckt.go newCircuit.
\* startWatchdog remembers its reconnect address. A failed pending entry
\* retries on its ticker; an established circuit is redialed after silence.
\* Each attempt abstracts its context deadline and the refresh backoff.

DialAttempt(n, m) ==
    /\ up[n] /\ ~stopped[n] /\ n /= m
    /\ ~\E c \in DOMAIN ckt : ckt[c].d = n /\ ckt[c].di = inc[n] /\ ckt[c].a = m /\ ckt[c].dSt = EpDialWait
    /\ LET c == byName[n][m] IN
       IF c = Pending THEN wdReq[n][m] /= Nil
       ELSE IF c > 0 THEN EpSt(c,n) = EpOpen /\ (wdReq[n][m] /= Nil \/ c \in watchSilent[n])
       ELSE FALSE
    /\ LET a == IF wdReq[n][m] /= Nil THEN wdReq[n][m] ELSE PeerAddrOf(byName[n][m],n)
           reach == /\ a \in Addrs /\ up[m] /\ ~stopped[m] /\ addr[m] = a
                    /\ link[n][m] = "ok" /\ link[m][n] = "ok"
           k0    == AddressRoute(conn,n,m,a)
           remoteOpen == IF k0 = 0 THEN TRUE
                         ELSE IF conn[k0].d = n THEN conn[k0].aOpen /\ conn[k0].ai = inc[m]
                         ELSE conn[k0].dOpen /\ conn[k0].di = inc[m]
           ok    == reach /\ remoteOpen
           kk    == IF k0 = 0 THEN nextConn ELSE k0
           startF == [cid |-> nextCkt, to |-> <<m, inc[m]>>, m |-> [type |-> "START", from |-> <<n, inc[n]>>]]
           endF(to) == [cid |-> nextCkt, to |-> to, m |-> [type |-> "END", from |-> <<n, inc[n]>>]]
           newC(ac, ainc, dState, aState) ==
               [d |-> n, di |-> inc[n], dd |-> addr[n], a |-> ac, ai |-> ainc,
                ad |-> a, k |-> kk, dSt |-> dState, aSt |-> aState, dSeen |-> Nil, aSeen |-> Nil]
           cn == IF k0 = 0 THEN conn @@ (kk :> NewConn(n, m, a, <<>>)) ELSE conn
           dir == SendDir(cn,kk,n)
           fast == ok /\ \A d \in Dirs :
                      cn[kk].out[d] = <<>> /\ cn[kk].writer[d] = Nil /\ WireRoom(cn,kk,d)
           otherInc == IF cn[kk].d = n THEN cn[kk].ai ELSE cn[kk].di
       IN \E result \in {"both", "pending", "timeout", "handoff", "failed"} :
          /\ CASE result = "both" ->
                   /\ fast
                   /\ conn' = cn
                   /\ ckt' = ckt @@ (nextCkt :> newC(m, inc[m], EpNew, EpNew))
               [] result = "handoff" ->
                   /\ fast
                   /\ conn' = QueueNotice(cn, kk, dir, endF(<<m, inc[m]>>))
                   /\ ckt' = ckt @@ (nextCkt :> newC(m, inc[m], EpGone, EpNew))
               [] result = "pending" ->
                   /\ k0 /= 0 \/ reach
                   /\ Len(cn[kk].out[dir]) < SendBufCap
                   /\ conn' = [cn EXCEPT ![kk].out[dir] = Append(@,startF)]
                   /\ ckt' = ckt @@ (nextCkt :> newC(m, otherInc, EpDialWait, "unstarted"))
               [] result = "timeout" ->
                   /\ k0 /= 0 \/ reach
                   /\ Len(cn[kk].out[dir]) < SendBufCap
                   /\ conn' = QueueNotice([cn EXCEPT ![kk].out[dir] = Append(@,startF)], kk, dir, endF(Nil))
                   /\ ckt' = ckt @@ (nextCkt :> newC(m, otherInc, EpGone, "unstarted"))
               [] result = "failed" -> UNCHANGED <<conn, ckt>>
          /\ wdReq' = [wdReq EXCEPT ![n][m] = IF result \notin {"both","pending"} /\ byName[n][m] = Pending THEN a ELSE Nil]
          /\ nextConn' = IF result /= "failed" /\ k0 = 0 THEN nextConn + 1 ELSE nextConn
          /\ nextCkt' = IF result /= "failed" THEN nextCkt + 1 ELSE nextCkt
    /\ UNCHANGED <<envVars, blk, knownPeers, cktall, byName, pendAddr, watchSilent, observer, cktReplica, audit,
                   raftVars, logVars, durVars, ghostVars>>

\* The acceptor's read loop handles a circuit-start fragment before any
\* ToPeerID check (srv.go:759-784 -> bootstrapCircuit): it creates its end
\* of the circuit and hands it to Tube's NewCircuitCh (HandleNewCircuit).
HandleStartFrame(k, dir, i) ==
    /\ k \in DOMAIN conn /\ dir \in Dirs
    /\ CanRead(k, dir, i)
    /\ FrameAt(k, dir, i).m.type = "START"
    /\ LET c == FrameAt(k, dir, i).cid
           r == Rcv(k, dir)
           rev == OtherDir(dir)
           cn == Consume(conn,k,dir,i)
           ack == [cid |-> c, to |-> FrameAt(k,dir,i).m.from,
                   m |-> [type |-> "STARTACK", from |-> <<r,inc[r]>>, timed |-> FALSE]]
       IN /\ EpSt(c, r) = "unstarted"
          /\ IF Len(cn[k].out[rev]) < SendBufCap
             THEN /\ ckt' = SetEp(ckt,c,r,EpNew)
                  /\ conn' = [cn EXCEPT ![k].out[rev] = Append(@,ack)]
             ELSE /\ ckt' = SetEp(ckt,c,r,EpGone)
                  /\ conn' = [cn EXCEPT ![k] = CloseSide(@,rev,"setup-ack-refused")]
    /\ UNCHANGED <<envVars, nextConn, nextCkt, blk, viewVars, raftVars, logVars, durVars, ghostVars>>

\* ckt.go newCircuit queues CallPeerCircuitEstablishedAck (-1) before
\* offering the acceptor circuit to Tube. An existing socket can carry START
\* through a one-way partition. Only receiving the ACK releases the dialer.
\* Its failed NewCircuitCh handoff closes the circuit with the now-known ID.
HandleSetupAckAs(k,dir,i,success) ==
    /\ CanRead(k,dir,i) /\ ~StaleTarget(k,dir,i) /\ FrameAt(k,dir,i).m.type = "STARTACK"
    /\ LET n == Rcv(k,dir)
           c == FrameAt(k,dir,i).cid
           pk == FrameAt(k,dir,i).m.from
           m == KName(pk)
           cn == Consume(conn,k,dir,i)
           msg == [type |-> "END", from |-> <<n,inc[n]>>]
       IN /\ EpSt(c,n) = EpDialWait /\ ckt[c].d = n /\ PeerKeyOf(c,n) = pk
          /\ ckt' = SetEp(ckt,c,n,IF success THEN EpNew ELSE EpGone)
          /\ conn' = IF success THEN cn ELSE BgSendOne(cn,n,c,msg)
          /\ wdReq' = IF success \/ byName[n][m] /= Pending THEN wdReq
                      ELSE [wdReq EXCEPT ![n][m] = ckt[c].ad]
    /\ UNCHANGED <<envVars, nextConn, nextCkt, blk, cktall, byName, pendAddr, knownPeers, watchSilent, observer, cktReplica, audit,
                   raftVars, logVars, durVars, ghostVars>>

HandleSetupAck(k,dir,i) == \E success \in BOOLEAN : HandleSetupAckAs(k,dir,i,success)

\* The five-second watchdog deadline or cancellation may beat the ACK.
\* At this point the origin has only an address; cleanup sends an empty
\* ToPeerID even if the acceptor has already registered this circuit.
CancelPendingDial(n,c) ==
    /\ up[n] /\ ~stopped[n] /\ ckt[c].d = n /\ EpSt(c,n) = EpDialWait
    /\ LET m == ckt[c].a
           k == RouteOf(conn,n,c)
           f == [cid |-> c, to |-> Nil, m |-> [type |-> "END", from |-> <<n,inc[n]>>]]
       IN /\ conn' = IF k = 0 THEN conn ELSE QueueNotice(conn,k,SendDir(conn,k,n),f)
          /\ ckt' = SetEp(ckt,c,n,EpGone)
          /\ wdReq' = IF byName[n][m] = Pending THEN [wdReq EXCEPT ![n][m] = ckt[c].ad] ELSE wdReq
    /\ UNCHANGED <<envVars, nextConn, nextCkt, blk, cktall, byName, pendAddr, knownPeers, watchSilent, observer, cktReplica, audit,
                   raftVars, logVars, durVars, ghostVars>>

\* TCP gives up on one side of a connection: FIN/RST from a dead peer,
\* keepalive (idle, ~150 s), or retransmission abort with data outstanding
\* (tcp_retries2, ~15 min).  Never enabled toward a stopped peer whose
\* kernel ACKs zero-window probes with both links up.  No TCP_USER_TIMEOUT,
\* app keepalives off (srv.go:337-349).  Previously completed outgoing frames may be read before the remote observes EOF.
ConnAbort(k, side) ==
    /\ k \in DOMAIN conn /\ side \in {"d", "a"}
    /\ LET me     == IF side = "d" THEN conn[k].d ELSE conn[k].a
           ot     == IF side = "d" THEN conn[k].a ELSE conn[k].d
           myOpen == IF side = "d" THEN conn[k].dOpen ELSE conn[k].aOpen
           otOpen == IF side = "d" THEN conn[k].aOpen ELSE conn[k].dOpen
       IN /\ myOpen
          /\ \/ ~otOpen
             \/ link[me][ot] = "drop"
             \/ link[ot][me] = "drop"
          /\ conn' = [conn EXCEPT ![k] = CloseSide(@, IF side = "d" THEN "da" ELSE "ad", "tcp")]
    /\ UNCHANGED <<envVars, ckt, nextConn, nextCkt, blk, viewVars, raftVars, logVars, durVars, ghostVars>>

\* live connections between n's current process and the process of key pk,
\* in either direction (the address fallbacks of destAddrToSendCh,
\* srv.go:2170-2230)
AnyConnTo(cn, n, pk) ==
    {k \in DOMAIN cn :
        \/ /\ cn[k].d = n /\ cn[k].di = inc[n] /\ cn[k].dOpen
           /\ cn[k].a = KName(pk) /\ cn[k].ai = KInc(pk)
        \/ /\ cn[k].a = n /\ cn[k].ai = inc[n] /\ cn[k].aOpen
           /\ cn[k].d = KName(pk) /\ cn[k].di = KInc(pk)}


\* rwPair/Client.runSendLoop halts the circuits registered on its socket.
\* The reader exits and cleanupCkt may notify over another existing route.

HaltOnDeadConn(n, c) ==
    /\ c \in DOMAIN ckt /\ up[n] /\ ~stopped[n]
    /\ EpSt(c, n) \in {EpNew, EpOpen}
    /\ LET k == ckt[c].k IN ~DirOpen(conn[k],SendDir(conn,k,n))
    /\ ckt' = SetEp(ckt, c, n, IF EpSt(c, n) = EpNew THEN "newdead" ELSE EpClosed)
    /\ LET pk  == PeerKeyOf(c, n)
           ks  == AnyConnTo(conn, n, pk)
           k2  == IF ks = {} THEN 0 ELSE MinSet(ks)
           d2  == IF k2 = 0 THEN "da" ELSE SendDir(conn, k2, n)
           f   == [cid |-> c, to |-> pk, m |-> [type |-> "END", from |-> <<n, inc[n]>>]]
       IN conn' = IF k2 = 0 THEN conn ELSE QueueNotice(conn, k2, d2, f)
    /\ UNCHANGED <<envVars, nextConn, nextCkt, blk, viewVars, raftVars, logVars, durVars, ghostVars>>

\* TCP ACKs for frames already read reach the sender once the reverse link works.
AckFlush(k, dir) ==
    /\ k \in DOMAIN conn /\ dir \in Dirs
    /\ conn[k].u[dir] > 0
    /\ RcvOpen(k, dir) /\ link[Rcv(k, dir)][Snd(k, dir)] = "ok"
    /\ conn' = [conn EXCEPT ![k].u[dir] = 0]
    /\ UNCHANGED <<envVars, ckt, nextConn, nextCkt, blk, viewVars, raftVars, logVars, durVars, ghostVars>>


\* One send loop drains its channel FIFO. A data frame accepted by the
\* buffered channel may wait in front of its writer or behind a stalled
\* frame. A frame only becomes visible to a recipient after a complete write.
LocalWriter(k, dir) ==
    /\ DirOpen(conn[k], dir)
    /\ up[Snd(k,dir)] /\ ~stopped[Snd(k,dir)]
    /\ IF dir = "da" THEN conn[k].di = inc[Snd(k,dir)] ELSE conn[k].ai = inc[Snd(k,dir)]

PipeOnly == UNCHANGED <<envVars, ckt, nextConn, nextCkt, blk, viewVars, raftVars, logVars, durVars, ghostVars>>

StartWrite(k, dir) ==
    /\ LocalWriter(k,dir) /\ conn[k].writer[dir] = Nil /\ conn[k].out[dir] /= <<>>
    /\ conn' = [conn EXCEPT ![k].writer[dir] = [f |-> Head(conn[k].out[dir]), partial |-> FALSE],
                            ![k].out[dir] = Tail(@)]
    /\ PipeOnly

\* The scheduler may expose a frame boundary inside sendMessage. A partial
\* frame is inaccessible to the receiver's decoder and invalidates the
\* stream if the write fails. A successful completion uses WriteQueuedFrame.
BeginPartialWrite(k, dir) ==
    /\ LocalWriter(k,dir) /\ conn[k].writer[dir] /= Nil
    /\ ~conn[k].writer[dir].partial
    /\ conn' = [conn EXCEPT ![k].writer[dir].partial = TRUE]
    /\ PipeOnly

WriteResult(k,dir,status) ==
    LET n == Snd(k,dir)
        f == conn[k].writer[dir].f
    IN IF blk[n].pend = <<>> THEN blk
       ELSE IF /\ blk[n].wait = [k |-> k, dir |-> dir, status |-> "write"]
               /\ Head(blk[n].pend).c = f.cid /\ Head(blk[n].pend).m = f.m
            THEN [blk EXCEPT ![n].wait.status = status] ELSE blk

WriterOthers == UNCHANGED <<envVars, ckt, nextConn, nextCkt, viewVars, raftVars, logVars, durVars, ghostVars>>

WriteQueuedFrame(k, dir) ==
    /\ LocalWriter(k,dir) /\ conn[k].writer[dir] /= Nil /\ WireRoom(conn,k,dir)
    /\ conn' = [conn EXCEPT ![k].q[dir] = Append(@, conn[k].writer[dir].f), ![k].writer[dir] = Nil]
    /\ blk' = WriteResult(k,dir,"sent") /\ WriterOthers

\* A syscall returning connection refused before writing is propagated to
\* the caller by DoneCh/LocalErr; wasConnRefused handles that specific error.
RefuseQueuedFrame(k,dir) ==
    /\ LocalWriter(k,dir) /\ conn[k].writer[dir] /= Nil /\ ~conn[k].writer[dir].partial
    /\ conn' = [conn EXCEPT ![k].writer[dir] = Nil]
    /\ blk' = WriteResult(k,dir,"refused") /\ WriterOthers

\* Discover expiry when the writer takes or attempts a deadline-tagged
\* frame (including the watchdog's START). END and STARTACK have no write
\* deadline. The plain writer fails a late frame without sending any bytes,
\* and closes its socket after a partial frame (common.go writeFrameByDeadline).
ExpireQueuedFrame(k, dir) ==
    /\ EnforceWriteDeadline /\ LocalWriter(k,dir) /\ conn[k].writer[dir] /= Nil
    /\ TimedMessage(conn[k].writer[dir].f.m)
    /\ conn' = IF conn[k].writer[dir].partial
               THEN [conn EXCEPT ![k] = CloseSide(@, dir, "partial-write")]
               ELSE [conn EXCEPT ![k].writer[dir] = Nil]
    /\ PipeOnly

\* A sender between messages can try a keepalive on its timer. The
\* keepalive itself is consumed by the remote read loop. Its only failure
\* that changes any circuit/connection state is a partial write.
PartialKeepAlive(k,dir) ==
    /\ dir \in KeepAliveDirections /\ LocalWriter(k,dir) /\ conn[k].writer[dir] = Nil
    /\ conn' = [conn EXCEPT ![k] = CloseSide(@, dir, "partial-keepalive")]
    /\ PipeOnly

\* A framing/decryption/non-timeout read error terminates the read loop.
\* This abstracts external stream failure (cli.go runReadLoop, srv.go
\* runReadLoop). A successfully decoded frame with a stale peer identity
\* follows DropUnroutable instead.
FramingReadError(k, dir) ==
    /\ RcvOpen(k,dir) /\ up[Rcv(k,dir)] /\ ~stopped[Rcv(k,dir)] /\ conn[k].q[dir] /= <<>>
    /\ conn' = [conn EXCEPT ![k] = CloseSide(@, OtherDir(dir), "read-error")]
    /\ PipeOnly

\* pump.go closeCktInBackgroundToAvoidDeadlock sends on the ORIGINAL
\* send channel when a slot opens, or abandons the notice after 5 seconds.
QueueCloseNotice(k,dir,f) ==
    /\ f \in conn[k].bg[dir] /\ LocalWriter(k,dir) /\ Len(conn[k].out[dir]) < SendBufCap
    /\ conn' = [conn EXCEPT ![k].out[dir] = Append(@,f), ![k].bg[dir] = @ \ {f}]
    /\ PipeOnly

ExpireCloseNotice(k,dir,f) ==
    /\ f \in conn[k].bg[dir] /\ LocalWriter(k,dir)
    /\ conn' = [conn EXCEPT ![k].bg[dir] = @ \ {f}]
    /\ PipeOnly

\* Buffered positive-duration sends can return through ckt.Context. A
\* zero-duration redirect still waits inside sendOneWayMessage for capacity
\* and then DoneCh, the pair halt, or the local peer context (srv.go).
AdmitWaiting(n) ==
    /\ up[n] /\ ~stopped[n] /\ blk[n].pend /= <<>> /\ blk[n].wait.status = "slot"
    /\ LET w == blk[n].wait
           item == Head(blk[n].pend)
           f == [cid |-> item.c, to |-> PeerKeyOf(item.c,n), m |-> item.m]
       IN /\ DirOpen(conn[w.k],w.dir) /\ Len(conn[w.k].out[w.dir]) < SendBufCap
          /\ conn' = [conn EXCEPT ![w.k].out[w.dir] = Append(@,f)]
          /\ blk' = [blk EXCEPT ![n].wait.status = "write"]
    /\ WriterOthers

WriteReturns(n) ==
    IF blk[n].pend = <<>> THEN {}
    ELSE LET w == blk[n].wait
             it == Head(blk[n].pend)
             p == conn[w.k]
             cancelled == TimedMessage(it.m) /\ EpSt(it.c,n) /= EpOpen
             pending == SeqElems(p.out[w.dir]) \cup
                        (IF p.writer[w.dir] = Nil THEN {} ELSE {p.writer[w.dir].f})
             unresolved == w.status = "slot" \/ \E f \in pending : f.cid = it.c /\ f.m = it.m
         IN IF w.status = "sent" THEN IF cancelled THEN BOOLEAN ELSE {TRUE}
            ELSE IF w.status = "refused" \/ cancelled \/ ~DirOpen(p,w.dir) \/ ~unresolved THEN {FALSE}
            ELSE {}

WaitingForWrite(n) == blk[n].pend /= <<>> /\ WriteReturns(n) = {}

\* The caller observes the write's actual result. wasConnRefused closes the
\* failing CID and adjusts replica membership only if it is still selected.
\* A new registration for the same peer survives (tube.go wasConnRefused).
UnblockSendAs(n,ok) ==
    /\ up[n] /\ ~stopped[n] /\ ok \in WriteReturns(n)
    /\ LET item == Head(blk[n].pend)
           c == item.c
           key == PeerKeyOf(c,n)
           refused == blk[n].wait.status = "refused"
           cancelledPV == ~ok /\ item.m.type = "RPV" /\ EpSt(c,n) /= EpOpen
           old == NS(n)
           cleanup == (refused \/ cancelledPV) /\ old.ca[key] = c
           updated == IF cleanup THEN AdjustCktReplica(DeleteFromCktAll(old,key,c),n) ELSE old
           cl == ApplyCloses(conn,ckt,n,IF refused THEN <<[c |-> c, why |-> "refused"]>> ELSE <<>>)
           done == IF ok \/ item.m.type /= "TKT" THEN updated
                   ELSE IF updated.book[item.m.tid].origin = updated.self
                        THEN RedirectFailed(updated,item.m.tid) ELSE updated
           resume == [done EXCEPT !.out = Tail(blk[n].pend), !.save2 = blk[n].save]
       IN WriteBack(n,resume,cl.cn,cl.ck)
UnblockSend(n) == \E ok \in BOOLEAN : UnblockSendAs(n,ok)

\* A full per-circuit Reads buffer and a further waiting fragment are
\* abstracted by more than SendBufCap undelivered data frames for that ckt.
\* The pump collects a never-read circuit after its fixed deadline. A slow
\* consumer that already took a frag retains its circuit. While the pump is
\* back-pressured, an upstream ToPeerID handoff can still time out and
\* discard a later incoming frame (srv.go handleReply_to_CallID_ToPeerID).
DataFrame(f) == f.m.type \notin {"END","START","STARTACK","PERR"}
ReceiverBacklogged(k,dir,c) ==
    Cardinality({j \in DOMAIN conn[k].q[dir] :
                 conn[k].q[dir][j].cid = c /\ DataFrame(conn[k].q[dir][j])}) > SendBufCap

ReceiverEverRead(n,c) ==
    \E k \in DOMAIN conn, dir \in Dirs : Rcv(k,dir) = n /\ c \in conn[k].read[dir]

StalledReceiverDrop(k, dir, i, closeIt) ==
    /\ CanRead(k,dir,i) /\ ~StaleTarget(k,dir,i) /\ DataFrame(FrameAt(k,dir,i))
    /\ LET c == FrameAt(k,dir,i).cid
           n == Rcv(k,dir)
       IN /\ EpSt(c,n) \in {EpNew,EpOpen} /\ ReceiverBacklogged(k,dir,c)
          /\ IF closeIt
             THEN /\ ~ReceiverEverRead(n,c)
                  /\ ckt' = SetEp(ckt,c,n,IF EpSt(c,n) = EpNew THEN "newdead" ELSE EpClosed)
                  /\ conn' = BgSendOne(Consume(conn,k,dir,i),n,c,[type |-> "END", from |-> <<n,inc[n]>>])
             ELSE /\ \E j \in (i+1)..Len(conn[k].q[dir]) :
                       /\ conn[k].q[dir][j].cid = c /\ DataFrame(conn[k].q[dir][j])
                       /\ conn' = Consume(conn,k,dir,j)
                  /\ UNCHANGED ckt
    /\ UNCHANGED <<envVars, nextConn, nextCkt, blk, viewVars, raftVars, logVars, durVars, ghostVars>>

\* srv.go/client runReadLoop and pump.go peerbackPump discard traffic for
\* a stale target or unknown circuit. The shared connection stays open.
DropUnroutable(k, dir, i) ==
    /\ CanRead(k,dir,i)
    /\ FrameAt(k,dir,i).m.type /= "START"
    /\ StaleTarget(k,dir,i) \/ EpSt(FrameAt(k,dir,i).cid, Rcv(k,dir)) \in {EpClosed,EpGone,"newdead"}
    /\ conn' = Consume(conn,k,dir,i)
    /\ PipeOnly

\* TubeNode.peerJoin rejects a decoded frag whose FromPeerID disagrees with
\* its actual circuit. Only that circuit can be removed from cktall.
RejectForeignFrame(k,dir,i) ==
    /\ CanRead(k,dir,i) /\ ~StaleTarget(k,dir,i) /\ DataFrame(FrameAt(k,dir,i))
    /\ LET n == Rcv(k,dir)
           c == FrameAt(k,dir,i).cid
           key == PeerKeyOf(c,n)
       IN /\ Running(n) /\ EpSt(c,n) = EpOpen /\ FrameAt(k,dir,i).m.from /= key
          /\ LET s == DeleteFromCktAll(NS(n),key,c) IN
             WriteBack(n,[s EXCEPT !.closes = <<[c |-> c, why |-> "unexpected peer"]>>],ConsumeTube(conn,k,dir,i),ckt)

\* CallPeerEndCircuit: the receiving pump closes its end (pump.go:229-234).
HandleEndCircuit(k, dir, i) ==
    /\ k \in DOMAIN conn /\ dir \in Dirs
    /\ CanRead(k, dir, i) /\ ~StaleTarget(k, dir, i)
    /\ FrameAt(k, dir, i).m.type = "END"
    /\ LET r == Rcv(k, dir)
           c == FrameAt(k, dir, i).cid
       IN /\ EpSt(c, r) \in {EpNew, EpOpen}
          /\ ckt' = SetEp(ckt, c, r, IF EpSt(c, r) = EpNew THEN "newdead" ELSE EpClosed)
    /\ conn' = Consume(conn, k, dir, i)
    /\ UNCHANGED <<envVars, nextConn, nextCkt, blk, viewVars, raftVars, logVars, durVars, ghostVars>>


\* An ordinary error fragment remains CallPeerError; Tube's cktHasError
\* branch skips it. Circuit.Close(reason) itself now sends END.

HandlePeerError(k, dir, i) ==
    /\ k \in DOMAIN conn /\ dir \in Dirs
    /\ CanRead(k, dir, i) /\ ~StaleTarget(k, dir, i)
    /\ FrameAt(k, dir, i).m.type = "PERR"
    /\ EpSt(FrameAt(k, dir, i).cid, Rcv(k, dir)) \in {EpNew, EpOpen}
    /\ conn' = Consume(conn, k, dir, i)
    /\ UNCHANGED <<envVars, ckt, nextConn, nextCkt, blk, viewVars, raftVars, logVars, durVars, ghostVars>>

\* ========================================================================
\* ACTIONS: TUBE CONNECTION MANAGEMENT (event loop)
\* ========================================================================

\* <-newCircuitCh -> handleNewCircuit (tube/tube.go:2006-2010, 2137-2389):
\* registers the circuit for the audit GC (cktAuditByCID, 2233) and calls
\* addToCktall.  A circuit halted before registration is still added, and
\* its reader exits at once.
HandleNewCircuit(n, c) ==
    /\ c \in DOMAIN ckt
    /\ Running(n)
    /\ EpSt(c, n) \in {EpNew, "newdead"}
    /\ LET s == NS(n)
           st == EpSt(c,n)
       IN IF NameInUse(n,c)
          THEN LET cn == IF st = EpNew THEN BgSendOne(conn,n,c,[type |-> "END",from |-> <<n,inc[n]>>])
                                      ELSE conn
               IN WriteBack(n,s,cn,SetEp(ckt,c,n,EpGone))
          ELSE WriteBackDispatch(n, AddToCktall([s EXCEPT !.aud = @ \cup {c}], n, c), conn,
                         SetEp(ckt,c,n,IF st = EpNew THEN EpOpen ELSE EpClosed))

\* A frame on an unselected still-open circuit can encounter a different
\* live owner when peerJoin tries to register the sender again.
RejectNamesakeFrame(k,dir,i) ==
    /\ CanRead(k,dir,i) /\ ~StaleTarget(k,dir,i)
    /\ LET n == Rcv(k,dir)
           c == FrameAt(k,dir,i).cid
       IN /\ Running(n) /\ EpSt(c,n) = EpOpen /\ PeerJoinRefused(n,c)
          /\ DataFrame(FrameAt(k,dir,i)) /\ FrameAt(k,dir,i).m.from = PeerKeyOf(c,n)
          /\ WriteBack(n,[NS(n) EXCEPT !.closes = <<[c |-> c,why |-> "name in use"]>>],
                       ConsumeTube(conn,k,dir,i),ckt)

\* <-s.peerLeftCh: cleanupGoneCktP + deleteFromCktAll (tube/tube.go:1262-1266,
\* 2392-2423, 2552-2593); entries are removed only if they still refer to it.
HandlePeerLeft(n, c) ==
    /\ c \in DOMAIN ckt
    /\ Running(n)
    /\ EpSt(c, n) = EpClosed
    /\ LET s  == NS(n)
           s1 == DeleteFromCktAll([s EXCEPT !.aud = @ \ {c}], PeerKeyOf(c, n), c)
       IN WriteBack(n, s1, conn, SetEp(ckt, c, n, EpGone))

\* Audit only closes unreferenced circuits with a dead local context.
\* All modeled peer-to-peer circuits have replica service at both ends.
\* Dead endpoints already reach HandlePeerLeft, while live duplicate
\* circuits may still be the peer's chosen return path (globalPruneCktCheck).
AuditGC(n) == Running(n) /\ UNCHANGED coreVars

ObserverCircuit(n,opened) ==
    /\ ModelObservers /\ Running(n) /\ observer[n].open /= opened
    /\ WriteBack(n, [NS(n) EXCEPT !.obs = [open |-> opened, notice |-> Nil]], conn, ckt)

\* ========================================================================
\* ACTIONS: RAFT (event loop)
\* ========================================================================

\* Timing assumptions (abstract time):
\*  * the read-lease window (MinElectionTimeout - 2*HeartbeatDur after the ping
\*    was SENT, tube/tube.go:12335-12370) ends before the follower's election
\*    timer (>= MinElectionDur after the AE, 6667-6693) or its sticky window
\*    (MinElectionDur + ClockDriftBound, 12534-12544) can expire: a node's
\*    timer / stickiness cannot expire while a leader holds lease credit from
\*    its current incarnation;
\*  * with TimelyHeartbeats, a running leader with an open circuit and working
\*    links delivers heartbeats (200 ms) before a follower's timer (>= 1.2 s).
\* (IF-THEN-ELSE, not \/: TLC evaluates both disjuncts of an action guard)
HasCredit(l, key) == IF peers[l][key] = Nil THEN FALSE ELSE peers[l][key].credit
\* A sent ping can grant credit after its follower has generated the ack
\* but before its leader consumes it. Preserve this ordering when an
\* election or restart-hold timer is allowed to expire.
CanStillCredit(l,key) ==
    IF peers[l][key] = Nil THEN FALSE
    ELSE \/ HasCredit(l,key)
         \/ /\ peers[l][key].pingFresh
            /\ \E m \in QueuedMessages :
                  /\ m.type = "AEA"
                  /\ m.from = key /\ m.leader = <<l,inc[l]>> /\ m.term = currentTerm[l]
                  /\ m.ping = peers[l][key].ping

\* Only a live process holds a lease: Crash leaves the dead leader's volatile
\* peers record in place (reset at Restart), and LeaseCreditExpire needs up[l].
NoCreditAt(n) == \A l \in Server : up[l] => ~CanStillCredit(l, <<n, inc[n]>>)

LeaderReachesMe(n) ==
    \E l \in Server :
        /\ leaderKey[n] = <<l, inc[l]>> /\ up[l] /\ role[l] = Leader /\ Running(l)
        /\ par[l] = "ok" /\ currentTerm[l] >= currentTerm[n]
        /\ link[l][n] = "ok" /\ link[n][l] = "ok"
        /\ LET c == cktall[l][<<n, inc[n]>>] IN
             /\ c /= NoCkt /\ EpSt(c, l) = EpOpen /\ EpSt(c, n) = EpOpen
             /\ RouteOf(conn, l, c) /= 0
             /\ conn[RouteOf(conn, l, c)].dOpen /\ conn[RouteOf(conn, l, c)].aOpen

\* The watchdog and live-namesake check use the same lastSeenTm threshold.
\* A fresh ack or timely direct heartbeat prevents expiry for that peer.
WatchdogSilence(n,m) ==
    /\ up[n] /\ ~stopped[n] /\ n /= m
    /\ LET c == byName[n][m] IN
       IF c <= 0 THEN FALSE
       ELSE /\ EpSt(c,n) = EpOpen /\ c \notin watchSilent[n]
            /\ ~RecentAck(NS(n),PeerKeyOf(c,n)) /\ ~HasCredit(n,PeerKeyOf(c,n))
            /\ ~(TimelyHeartbeats /\ leaderKey[n] = PeerKeyOf(c,n) /\ LeaderReachesMe(n))
            /\ watchSilent' = [watchSilent EXCEPT ![n] = @ \cup {c}]
    /\ UNCHANGED <<envVars, netVars, knownPeers, cktall, byName, pendAddr, wdReq, cktReplica, audit,
                   observer, raftVars, logVars, durVars, ghostVars>>

AckAge(n,k) ==
    /\ up[n] /\ RecentAck(NS(n),k) /\ k \notin pong[n].fresh /\ ~HasCredit(n,k)
    /\ peers' = [peers EXCEPT ![n][k].recentAck = FALSE]
    /\ UNCHANGED <<envVars, netVars, viewVars, campaign, mcPending, commitIndexTerm, role, currentTerm, votedFor, leaderKey, sticky,
                   pvTerm, pv2Term, pvYes, pvNo, vYes, vNo, pong, leaderAged, startupPhase, noop0,
                   stallQ, config, shadows, commitIndex, lastApplied, lastAppliedTerm, snapSlot,
                   logVars, durVars, ghostVars>>

ElectionTimerMayFire(n) ==
    \/ role[n] = Leader
    \/ /\ NoCreditAt(n)
       /\ (TimelyHeartbeats => ~LeaderReachesMe(n))

\* tube.go: electionTimeoutCh, pongpq.go: leaderPongQuorumTm. Following
\* the startup grace period, test the current membership even after self
\* removal. An empty queue supplies zero remote votes.
LeaderPongCheck(s0, n) ==
    LET s == OnLeaderIsCurrentMCcommitted(s0,n)
        need == RequiredVotes(s.cfg.mem, s.startup)
        voters == {KName(k) : k \in s.pong.fresh} \cap (s.cfg.mem \ {n})
        votes == Cardinality(voters) + (IF n \in s.cfg.mem THEN 1 ELSE 0)
    IN IF s.aged /\ s.startup /= "unanimous" /\ votes < need
       THEN BecomeFollower([s EXCEPT !.leader = Nil],n,s.term,Nil,TRUE)
       ELSE s

\* Local wall time can pass before a queued deferredCampaignCh event runs.
\* A deadline spans every lease based on a heartbeat this physical replica
\* acknowledged before restart. In-flight replies retain the ping send time.
RestartWindowTick(n) ==
    /\ Running(n) /\ campaign[n].remaining > 1
    /\ WriteBack(n, [NS(n) EXCEPT !.campaign.remaining = @ - 1], conn, ckt)

RestartWindowElapsed(n) ==
    /\ Running(n) /\ campaign[n].remaining = 1
    /\ \A l \in Server : up[l] =>
          \A i \in 0..(inc[n]-1) : ~CanStillCredit(l, <<n, i>>)
    /\ WriteBack(n, [NS(n) EXCEPT !.campaign.held = FALSE, !.campaign.remaining = 0], conn, ckt)

RunDeferredCampaign(n) ==
    /\ Running(n) /\ ~campaign[n].held /\ campaign[n].deferred /= "none"
    /\ LET s == [NS(n) EXCEPT !.campaign.deferred = "none"]
           r == IF campaign[n].deferred = "election" THEN BeginElection(s,n) ELSE BeginPreVote(s,n)
       IN WriteBack(n, r, conn, ckt)

\* tube.go: testBeginElection/testBeginPreVote are explicit test inputs.
\* A fixture can request either during the restart window even with the
\* election timer disabled. Normal elections enter via the timeout handlers.
ExplicitCampaignDuringRestart(n, kind) ==
    /\ Running(n) /\ campaign[n].held /\ kind \in {"election","preVote"}
    /\ WriteBack(n, IF kind = "election" THEN BeginElection(NS(n),n)
                     ELSE BeginPreVote(NS(n),n), conn, ckt)

\* tube.go: bootstrappedOrForcedMembership, unforced add of self. An
\* replica (including a shadow) that the watchdog reports up prevents bootstrap.
\* The administrative specification supplies the client request.
BootstrapSelf(n) ==
    /\ Running(n) /\ config[n].mem = {}
    /\ LET s == NS(n)
           visible == \E k \in Keys : KName(k) /= n /\ s.ca[k] > 0 /\ EpSt(s.ca[k], n) = EpOpen
                                   /\ s.ca[k] \notin watchSilent[n]
           me == [inc |-> inc[n], addr |-> addr[n]]
           added == [s EXCEPT !.cfg.mem = {n}, !.cfg.det[n] = me, !.known[n] = me, !.sh = @ \ {n}]
       IN /\ ~(RecentLeader(s) /\ s.role /= Leader) /\ ~visible
          /\ WriteBack(n, IF s.role = Leader THEN added
                          ELSE SaveRaftState([added EXCEPT !.term = @ + 1, !.vote = <<n,inc[n]>>,
                                                         !.campaign.resume = "bootstrap"]), conn, ckt)

ResumeCampaign(n) ==
    /\ up[n] /\ ~stopped[n] /\ campaign[n].resume /= "none"
    /\ LET s == [NS(n) EXCEPT !.campaign.resume = "none"]
           next == CASE campaign[n].resume = "requestVotes" -> SendRequestVotes(s,n)
                     [] campaign[n].resume = "bootstrap" -> BecomeLeader(s,n)
                     [] OTHER -> [TakeOverDataTickets(CommitLocalTail(s),1) EXCEPT !.save2 = TRUE]
       IN WriteBack(n, next, conn, ckt)

\* <-s.electionTimeoutCh (tube/tube.go:1384-1607)
ElectionTimeout(n) ==
    /\ Running(n)
    /\ ElectionTimerMayFire(n)
    /\ LET s0    == NS(n)
           lname == IF s0.leader = Nil THEN Nil ELSE KName(s0.leader)
           tear  == s0.role /= Leader /\ s0.leader /= Nil /\ s0.bn[lname] /= NoCkt   \* 1430-1433
           lc    == IF tear THEN s0.bn[lname] ELSE NoCkt
           lkey  == IF lc > 0 THEN PeerKeyOf(lc, n) ELSE s0.leader
           s1    == IF tear
                    THEN [DeleteFromCktAll(s0, lkey, lc) EXCEPT          \* 1434
                            !.closes = IF lc > 0 THEN Append(@, [c |-> lc, why |-> "nil"]) ELSE @,  \* 1435-1437
                            !.leader = Nil]                              \* 1438-1442
                    ELSE s0
           s2    == ResubmitStalledMC(ConnectToMC(s1, n), n)             \* 1451 connectToMC + deferred resubmit
           s3    == IF s2.role = Leader THEN LeaderPongCheck(s2, n)      \* 1452-1567
                    ELSE BeginPreVote(s2, n)                             \* 1601-1606
       IN WriteBack(n, s3, conn, ckt)

\* <-s.preVoteOkLeaderElecTimeoutCh (tube/tube.go:1368-1383): phase 2 over
PreVotePhase2Timeout(n) ==
    /\ Running(n)
    /\ pv2Term[n] > 0
    /\ LET s  == NS(n)
           go == s.role = Follower /\ s.pvTerm = 0 /\ s.pv2Term = s.term + 1
           s1 == IF go THEN BeginElection([s EXCEPT !.pv2Term = 0], n) ELSE [s EXCEPT !.pv2Term = 0]
       IN WriteBack(n, s1, conn, ckt)

\* leaderSendsHeartbeats may skip an empty heartbeat if a recent ack set
\* LastHeardAnything (a shorter window than followerDownDur). A stale
\* membership version or a catch-up AE always bypasses that optimization.

HBMcStale(s, n, k) ==
    LET sn == SeenOf(s, n, s.ca[k]) IN sn /= Nil /\ VersionGT(s.cfg, sn)
HBMaySkip(s, n, k) ==
    /\ s.peers[k] /= Nil /\ RecentAck(s,k)
    /\ PhysLastIdx(s.log, s.base) <= s.peers[k].lcri
    /\ ~HBMcStale(s, n, k)

RECURSIVE HBFold(_, _, _, _)
HBFold(s, n, K, skip) ==
    IF K = {} THEN s
    ELSE LET k  == CHOOSE k \in K : TRUE
             s1 == IF PhysLastIdx(s.log, s.base) > s.peers[k].lcri      \* 8760, 8870-8871 catch-up from LCRI+1
                   THEN SendAppendEntriesTo(s, n, k, s.peers[k].lcri + 1)
                   ELSE IF k \in skip THEN s                             \* 8879-8880 heard recently
                   ELSE SendHeartbeat(s, n, k)                           \* 8872-8886
         IN HBFold(s1, n, K \ {k}, skip)

\* leaderSendsHeartbeats(immediately) on leader node record s (tube/tube.go:8739-8901);
\* skip: keys whose empty heartbeat is time-skipped (must be HBMaySkip).
HBKeys(s) == {k \in Keys : s.ca[k] /= NoCkt}                             \* 8832 every cktall entry
HBPeers(s) == [s EXCEPT !.peers = [k \in Keys |->                        \* 8848-8863
                   IF k \in HBKeys(s) /\ @[k] = Nil THEN FreshPeer ELSE @[k]]]
HBSkippable(s, n) == {k \in HBKeys(s) : HBMaySkip(HBPeers(s), n, k)}
LeaderSendsHeartbeats(s, n, skip) ==
    LET s1  == HBPeers(s)
        s2  == HBFold(s1, n, HBKeys(s), skip)
        unc == (s.cfg.mem \cup s.sh) \ ({n} \cup {KName(k) : k \in HBKeys(s)})   \* 8763-8811
    IN CIBSet(s2, n, unc)                                                \* 8890-8899

\* <-s.leaderSendsHeartbeatsCh -> leaderSendsHeartbeats(false) (tube/tube.go:1210-1218)
LeaderHeartbeatWith(n, skip) ==
    /\ Running(n) /\ role[n] = Leader /\ par[n] = "ok"                  \* 8740-8748
    /\ skip \subseteq HBSkippable(NS(n), n)
    /\ WriteBack(n, LeaderSendsHeartbeats(NS(n), n, skip), conn, ckt)

LeaderHeartbeat(n) ==
    /\ Running(n) /\ role[n] = Leader /\ par[n] = "ok"
    /\ \E skip \in SUBSET HBSkippable(NS(n), n) : LeaderHeartbeatWith(n, skip)

\* peerJoin(frag, ckt) (tube/tube.go:9157-9248), run for EVERY frame the
\* event loop receives (1668), before its handler: a sender whose PeerID is
\* not in cktall gets the arrival circuit re-registered (addToCktall,
\* 9186-9201) -- e.g. a circuit a newer one displaced or whose cktall entry
\* an election-timeout teardown deleted, still open at this end; a replica
\* still unknown in s.peers makes a leader create a fresh RaftNodeInfo and
\* heartbeat at once (leaderSendsHeartbeats(true): nothing skipped, 9223-9246).
\* (The acceptor's circuit-start fragment, 1941-1945, runs peerJoin alone; its
\* heartbeat is a LeaderHeartbeat step with an empty skip set.)
PeerJoin(s, n, c) ==
    LET key == PeerKeyOf(c, n)
        s1  == IF s.ca[key] = NoCkt THEN AddToCktall(s, n, c) ELSE s
        s2  == [s1 EXCEPT !.peers[key] = FreshPeer]
    IN IF s1.peers[key] /= Nil \/ s1.role /= Leader THEN s1
       ELSE IF s2.par /= "ok" THEN s2                                \* 8740-8743 parBlocked
       ELSE LeaderSendsHeartbeats(s2, n, {})

\* node record of the receiver of frame i after peerJoin
NSJ(k, dir, i) == PeerJoin(NS(Rcv(k, dir)), Rcv(k, dir), FrameAt(k, dir, i).cid)

\* RequestPreVote (tube/tube.go:11591-11730): stateless responder
HandleRequestPreVote(k, dir, i) ==
    /\ k \in DOMAIN conn /\ dir \in Dirs
    /\ TubeFrame(k, dir, i) /\ FrameAt(k, dir, i).m.type = "RPV"
    /\ Running(Rcv(k, dir))
    /\ LET n   == Rcv(k, dir)
           req == FrameAt(k, dir, i).m
           s   == NSJ(k, dir, i)
           g   == IF HaveStickyLeader(s) THEN FALSE                      \* 11626-11634
                  ELSE IF req.cterm <= s.term THEN FALSE                 \* 11636-11641
                  ELSE LogUpToDateAsOurs(s, n, req)                      \* 11643-11651
           rep == [type |-> "PV", from |-> <<n, inc[n]>>, cand |-> req.from, cterm |-> req.cterm,
                   granted |-> g, cfg |-> s.cfg, vterm |-> s.term]
           c   == s.ca[req.from]                                         \* 11658-11663
       IN WriteBack(n, IF c = NoCkt THEN s ELSE AddOut(s, c, rep), ConsumeTube(conn, k, dir, i), ckt)

\* PreVote reply -> tallyPreVote
HandlePreVote(k, dir, i) ==
    /\ k \in DOMAIN conn /\ dir \in Dirs
    /\ TubeFrame(k, dir, i) /\ FrameAt(k, dir, i).m.type = "PV"
    /\ Running(Rcv(k, dir))
    /\ LET n == Rcv(k, dir) IN
       WriteBack(n, TallyPreVote(NSJ(k, dir, i), n, FrameAt(k, dir, i).m), ConsumeTube(conn, k, dir, i), ckt)

\* RequestVote (tube/tube.go:11336-11536): save BEFORE the reply (11426-11428)
HandleRequestVote(k, dir, i) ==
    /\ k \in DOMAIN conn /\ dir \in Dirs
    /\ TubeFrame(k, dir, i) /\ FrameAt(k, dir, i).m.type = "RV"
    /\ Running(Rcv(k, dir))
    /\ LET n      == Rcv(k, dir)
           req    == FrameAt(k, dir, i).m
           s      == NSJ(k, dir, i)
           denyLo == req.cterm < s.term                                  \* 11384-11389 no save
           stk    == ~denyLo /\ HaveStickyLeader(s)                      \* 11391-11397
           s1     == IF ~denyLo /\ ~stk /\ req.cterm > s.term            \* 11399-11402
                     THEN BecomeFollower(s, n, req.cterm, req.cfg, FALSE) ELSE s
           other  == ~denyLo /\ ~stk /\ req.cterm = s.term                \* 11404-11410
                     /\ s.vote /= Nil /\ s.vote /= req.from
           grant  == ~(denyLo \/ stk \/ other) /\ LogUpToDateAsOurs(s1, n, req)   \* 11413-11421
           s2     == IF grant THEN [s1 EXCEPT !.vote = req.from] ELSE s1
           s3     == IF ~denyLo /\ ~other THEN SaveRaftState(s2) ELSE s2   \* 11426-11428
           rep    == [type |-> "V", from |-> <<n, inc[n]>>, cand |-> req.from,
                      cterm |-> s3.term,                                 \* 11429 CandidatesTerm = our term
                      granted |-> grant, cfg |-> s.cfg,                  \* MC captured at the top (11353)
                      vterm |-> s.term]                                  \* FromPeerCurrentTerm before (11347)
           c      == s3.ca[req.from]                                     \* 11518-11523
       IN WriteBack(n, IF c = NoCkt THEN s3 ELSE AddOut(s3, c, rep), ConsumeTube(conn, k, dir, i), ckt)

\* Vote reply -> tallyVote (may becomeLeader)
HandleVote(k, dir, i) ==
    /\ k \in DOMAIN conn /\ dir \in Dirs
    /\ TubeFrame(k, dir, i) /\ FrameAt(k, dir, i).m.type = "V"
    /\ Running(Rcv(k, dir))
    /\ LET n == Rcv(k, dir) IN
       WriteBack(n, TallyVote(NSJ(k, dir, i), n, FrameAt(k, dir, i).m), ConsumeTube(conn, k, dir, i), ckt)

\* becomeLeader's saver.save after the noop0 broadcast (tube/tube.go:7473-7475)
BecomeLeaderSave(n) ==
    /\ up[n] /\ ~stopped[n] /\ blk[n].save
    /\ blk[n].pend = <<>>
    /\ WriteBack(n, SaveRaftState(NS(n)), conn, ckt)

\* AppendEntries (tube/tube.go:7854-8689) incl. the PAR-blocked path (7855-7858)
HandleAppendEntries(k, dir, i) ==
    /\ k \in DOMAIN conn /\ dir \in Dirs
    /\ TubeFrame(k, dir, i) /\ FrameAt(k, dir, i).m.type = "AE"
    /\ Running(Rcv(k, dir))
    /\ LET n  == Rcv(k, dir)
           ae == FrameAt(k, dir, i).m
           s  == NSJ(k, dir, i)
       IN IF s.par /= "ok"
          THEN WriteBack(n, ParObserveAE(s, n, ae), ConsumeTube(conn, k, dir, i), ckt)
          ELSE LET r   == AEStep(s, n, ae)
                   tgt == r.s.ca[ae.from]                                \* ackAE 8711-8716 cktall[ae.FromPeerID]
               IN WriteBackDispatch(n, IF tgt = NoCkt \/ r.ack = Nil THEN r.s ELSE AddOut(r.s, tgt, r.ack),
                            ConsumeTube(conn, k, dir, i), ckt)

\* AppendEntriesAck (tube/tube.go:9847-10134)
HandleAppendEntriesAck(k, dir, i) ==
    /\ k \in DOMAIN conn /\ dir \in Dirs
    /\ TubeFrame(k, dir, i) /\ FrameAt(k, dir, i).m.type = "AEA"
    /\ Running(Rcv(k, dir))
    /\ LET n == Rcv(k, dir) IN
       WriteBack(n, AckStep(NSJ(k, dir, i), n, FrameAt(k, dir, i).m, FrameAt(k, dir, i).cid),
                 ConsumeTube(conn, k, dir, i), ckt)

\* handleStateSnapshotEnclosed (tube/tube.go) keeps up to four assemblies.
\* Each starts on receipt of part 1. Frames carry a valid payload/checksum;
\* sid stands for its checksum. One part or two represents transfer length.
BeginAssembly(slots, key) ==
    LET room == IF Len(slots) >= 4 THEN Tail(slots) ELSE slots
    IN Append(SelectSeq(room, LAMBDA old : old /= key), key)

HandleStateSnapshot(k, dir, i) ==
    /\ k \in DOMAIN conn /\ dir \in Dirs
    /\ TubeFrame(k, dir, i) /\ FrameAt(k, dir, i).m.type = "SNAP"
    /\ Running(Rcv(k, dir))
    /\ LET n  == Rcv(k, dir)
           f  == FrameAt(k, dir, i)
           sn == f.m
           s  == NSJ(k, dir, i)
           key == <<f.cid, sn.sid>>
           apply(sx) == LET r == ApplySnapshot(sx, n, sn)
                            c == r.s.ca[sn.ae.from]
                        IN IF r.ack = Nil \/ c = NoCkt THEN r.s ELSE AddOut(r.s, c, r.ack)
           s1 == CASE sn.from /= s.leader -> s
                   [] sn.part = 1 -> [s EXCEPT !.slot = BeginAssembly(@, key)]
                   [] sn.part = -2 ->
                        IF \E j \in 1..Len(s.slot) : s.slot[j] = key
                        THEN apply([s EXCEPT !.slot = SelectSeq(@, LAMBDA old : old /= key)])
                        ELSE s
                   [] OTHER -> apply([s EXCEPT !.slot = <<>>])   \* single frame resets all assemblies
       IN WriteBack(n, s1, ConsumeTube(conn, k, dir, i), ckt)

\* RequestStateSnapshot (PAR, par.go:2640-2654) -> handleRequestStateSnapshot
\* on the circuit it arrived on (no role check).
HandleRequestStateSnapshotMsg(k, dir, i) ==
    /\ k \in DOMAIN conn /\ dir \in Dirs
    /\ TubeFrame(k, dir, i) /\ FrameAt(k, dir, i).m.type = "RSS"
    /\ Running(Rcv(k, dir))
    /\ LET n == Rcv(k, dir) IN
       WriteBack(n, HandleRequestStateSnapshot(NSJ(k, dir, i), n, FrameAt(k, dir, i).cid),
                 ConsumeTube(conn, k, dir, i), ckt)

\* Immediate one-entry batch after the initial noop. ClientSubmit and
\* FlushDataTicket also expose the waiting and batch scheduling boundaries.
ClientRequest(n) ==
    /\ Running(n) /\ role[n] = Leader /\ par[n] = "ok" /\ noop0[n].done
    /\ \A j \in DOMAIN clientTickets : clientTickets[j].stage[<<n, inc[n]>>] \notin {"unindexed", "batch"}
    /\ LET s == RegisterDataTicket(NS(n), FALSE)
           t == Len(s.book)
       IN WriteBack(n, ReplicateEntry(BindDataEntry(AdmitDataTicket(s, t), t), n, "data"), conn, ckt)

RedirectDataTicket(s, t) ==
    LET c == IF s.leader = Nil \/ KName(s.leader) = KName(s.self) THEN NoCkt ELSE s.ca[s.leader]
        m == [type |-> "TKT", from |-> s.self, tid |-> t, timed |-> s.book[t].timed]
    IN IF c = NoCkt \/ ~CanAttempt(conn,KName(s.self),c,m) THEN RedirectFailed(s,t)
       ELSE AddOut([s EXCEPT !.book[t].stage[s.self] = IF s.book[t].result = Nil THEN "follow" ELSE "none"],c,m)

\* Ordinary writes use waitForDur for transport and fail eagerly when no
\* leader can be reached. The membership API separately sets WaitLeaderDeadline.
RouteNewTicket(s,t) ==
    IF s.par /= "ok" THEN FinishDataTicket(s,t,"recovering",0,"local")
    ELSE IF s.role = Leader THEN AdmitDataTicket(s,t)
    ELSE RedirectDataTicket(s,t)

ClientSubmit(n, timed) ==
    /\ Running(n)
    /\ LET s == RegisterDataTicket(NS(n),FALSE)
           t == Len(s.book)
       IN WriteBack(n,RouteNewTicket([s EXCEPT !.book[t].timed = timed],t),conn,ckt)

\* SingleUpdateClusterMemberConfig permits both voting and shadow requests
\* to await a discoverable leader when the caller supplies a deadline. The
\* ordinary path here starts with a populated configuration. Bootstrap,
\* explicit leader URL dialing and forced membership have separate actions.
SubmitAdminTicket(n) ==
    /\ Running(n) /\ config[n].mem /= {}
    /\ \E op \in {"add","remove","shadowAdd","shadowRemove"}, x \in Server, wait \in BOOLEAN :
         \E d \in (IF op = "shadowAdd" THEN Det ELSE {Nil}),
            a \in (IF op = "add" THEN Addrs ELSE {InitAddr}) :
            LET s == RegisterDataTicket(NS(n),wait)
                t == Len(s.book)
                cmd == [kind |-> op, x |-> x, detail |-> d, addr |-> a]
            IN WriteBack(n,RouteNewTicket([s EXCEPT !.book[t].command = cmd],t),conn,ckt)

\* A forwarded proposal's 10s admission deadline is measured from origin
\* Ticket.T0. Time spent on discovery or on the wire counts toward it.
AgeAdminTicket(n,t) ==
    /\ Running(n) /\ clientTickets[t].origin = <<n,inc[n]>>
    /\ clientTickets[t].command.kind \in {"add","remove"} /\ ~clientTickets[t].old
    /\ \A k \in Keys : clientTickets[t].mcs[k] = Nil
    /\ WriteBack(n,[NS(n) EXCEPT !.book[t].old = TRUE],conn,ckt)

\* One-entry batches. The new leader's initial noop releases its waiting
\* writes, and batch timers submit in leader-stamped order.
FlushDataTicket(n, t) ==
    /\ Running(n) /\ role[n] = Leader /\ par[n] = "ok" /\ noop0[n].done
    /\ LET key == <<n, inc[n]>>
           ready == {j \in DOMAIN clientTickets : clientTickets[j].stage[key] \in {"unindexed", "batch"}}
       IN /\ t \in ready
          /\ \A j \in ready : clientTickets[t].stamp[key] <= clientTickets[j].stamp[key]
          /\ LET s == BindDataEntry(NS(n), t)
                 cmd == s.book[t].command
                 e == [kind |-> cmd.kind, term |-> s.term, id |-> s.nid]
                 entry == IF cmd.kind = "data" THEN e ELSE e @@ [x |-> cmd.x, detail |-> cmd.detail]
             IN WriteBack(n, ReplicateCommand(s,n,entry), conn, ckt)

DispatchStashedDataTicket(n, t) ==
    /\ CanRunEvent(n) /\ t \in DataDispatchDue(n)
    /\ t = MinSet(DataDispatchDue(n))
    /\ LET s == [NS(n) EXCEPT !.book[t].dispatch = FALSE, !.book[t].stashed = FALSE]
           s1 == IF s.role = Leader THEN AdmitDataTicket(s, t) ELSE RedirectDataTicket(s, t)
       IN WriteBack(n, s1, conn, ckt)

\* errorOutAwaitingLeaderTooLongTickets drops the stashed copy at its deadline.
\* expireAbandonedTickets, called by the 20s prune, removes canceled callers
\* from the waiting maps. The dispatch map can still hold the submission.
ExpireDataTicket(n, t) ==
    /\ Running(n) /\ clientTickets[t].origin = <<n, inc[n]>>
    /\ LET s == NS(n) IN
       \/ /\ s.book[t].stashed
          /\ WriteBack(n, FinishDataTicket([s EXCEPT !.book[t].stashed = FALSE],
                                           t, "expired", 0, "local"), conn, ckt)
       \/ /\ s.book[t].stage[s.self] \in {"follow", "unindexed", "indexed"}
          /\ s.book[t].contextDone /\ s.book[t].result = Nil
          /\ WriteBack(n, FinishDataTicket(s, t, "expired", 0, "local"), conn, ckt)

HandleTicketFrame(k, dir, i) ==
    /\ k \in DOMAIN conn /\ dir \in Dirs /\ TubeFrame(k, dir, i)
    /\ FrameAt(k, dir, i).m.type \in {"TKT", "TKTRES"} /\ Running(Rcv(k, dir))
    /\ LET n == Rcv(k, dir)
           s == NSJ(k, dir, i)
           m == FrameAt(k, dir, i).m
           t == m.tid
           s1 == IF m.type = "TKT"
                 THEN IF s.role /= Leader THEN TicketReply(s, t, "redirectError", 0)
                      ELSE IF s.par /= "ok" THEN TicketReply(s, t, "recovering", 0)
                      ELSE AdmitDataTicket(s, t)
                 ELSE IF s.book[t].stage[s.self] = "none" THEN s
                      ELSE FinishDataTicket(s, t, m.status, m.entry, "reply")
       IN WriteBack(n, s1, ConsumeTube(conn, k, dir, i), ckt)

\* Write waits on Done or ctx.Done. SingleUpdateClusterMemberConfig can also
\* return on errWriteDur. Either caller can stop waiting while a Tube send or
\* PAR save blocks the server goroutine. A timer return alone cannot trigger
\* the canceled-context prune. A later cancellation can make pruning eligible.
CallerReturn(q, reason) ==
    [q EXCEPT !.returned = IF @ = Nil THEN [status |-> reason, entry |-> 0, via |-> "caller"] ELSE @,
              !.contextDone = reason = "cancelled"]

CallerAbandonsTicket(n,t) ==
    /\ up[n] /\ ~stopped[n] /\ clientTickets[t].origin = <<n,inc[n]>>
    /\ LET q == clientTickets[t] IN
       /\ q.result = Nil /\ ~q.contextDone
       /\ \E reason \in (IF q.wait /\ q.returned = Nil THEN {"cancelled","timeout"} ELSE {"cancelled"}) :
            clientTickets' = [clientTickets EXCEPT ![t] = CallerReturn(@,reason)]
    /\ UNCHANGED <<envVars,netVars,viewVars,raftVars,logVars,durVars,raftHistoryVars>>

TicketWork(n) ==
    \E t \in DOMAIN clientTickets :
        FlushDataTicket(n, t) \/ DispatchStashedDataTicket(n, t) \/ ExpireDataTicket(n, t) \/ AgeAdminTicket(n,t)
        \/ CallerAbandonsTicket(n,t)

\* Ordinary logless membership requests. IDs distinguish concurrent client
\* requests with the same operation and target.
ClientChangeMembershipWithForce(n, op, x, force) ==
    /\ Running(n) /\ role[n] = Leader /\ par[n] = "ok"
    /\ op \in {"add","remove"} /\ x \in Server
    /\ \E address \in (IF op = "add" THEN Addrs ELSE {InitAddr}) :
          LET s == NS(n)
              id == <<n,inc[n],s.mc.issued + 1>>
              t == [op |-> op, x |-> x, id |-> id, force |-> force, addr |-> address]
          IN WriteBack(n,ChangeMembership([s EXCEPT !.mc.issued = @ + 1, !.mc.direct = @ \cup {id}],n,t),conn,ckt)

ClientChangeMembership(n,op,x) == ClientChangeMembershipWithForce(n,op,x,FALSE)

\* Locally created requests age in submission order. A forwarded request
\* carries its client's start time and can arrive already past its deadline.
\* Crossing a 10-second ticket deadline makes the next admission/commit
\* check eligible to return an error. A stalled ticket can still succeed
\* if quorum is ready when checked (tube.go: changeMembership).
MCDeadlineElapsed(n) ==
    /\ up[n]
    /\ LET s == NS(n)
           pending == TicketSet(s.stallQ) \cup s.mc.awaiting
                      \cup {s.mc.watch[i].t : i \in DOMAIN s.mc.watch}
           ids == {t.id : t \in pending}
       IN \E id \in ids \ s.mc.expired :
              LET due == IF id \in s.mc.direct
                         THEN {p \in ids \cap s.mc.direct : p[3] <= id[3]} ELSE {id}
              IN mcPending' = [mcPending EXCEPT ![n].expired = @ \cup due]
    /\ UNCHANGED <<envVars,netVars,viewVars,raftProtocolVars,logVars,durVars,ghostVars>>

\* An external admin ticket has reached this replica's event loop.
\* A live remote leader gets the forwarded command. The receiver rechecks
\* its own role and leader when it dequeues the frame (tube.go,
\* bootstrappedOrForcedMembership, redirectToLeader, forceChangeMC).
ForceLeaderVisible(s,n) ==
    IF s.role = Leader \/ s.leader = Nil \/ s.leader = <<n,inc[n]>> THEN FALSE
    ELSE LET c == s.ca[s.leader] IN
         s.sticky \/ (c > 0 /\ EpSt(c,n) = EpOpen /\ c \notin s.silent)

RecordAdmin(before, after, n, op, x, result) ==
    [after EXCEPT !.newAdmin = @ \cup
        {[node |-> n, op |-> op, x |-> x, result |-> result,
          eligibleLeader |-> ForceLeaderVisible(before,n),
          old |-> before.cfg, installed |-> after.cfg]}]

ForceReceived(s,n,op,x) ==
    IF ForceLeaderVisible(s,n)
    THEN LET c == s.ca[s.leader] IN
         IF s.par /= "ok" \/ c <= NoCkt
         THEN RecordAdmin(s,s,n,op,x,"noRoute")
         ELSE RecordAdmin(s,AddOut(s,c,[type |-> "FORCE", from |-> <<n,inc[n]>>,
                                          op |-> op, x |-> x]),n,op,x,"redirect")
    ELSE LET d == IF x = n THEN [inc |-> inc[n], addr |-> addr[n]]
                  ELSE IF x \in s.sh THEN s.cfg.det[x] ELSE s.known[x]
             allowed == IF op = "add" THEN x \notin s.cfg.mem /\ d /= Nil ELSE x \in s.cfg.mem
         IN IF ~allowed THEN RecordAdmin(s,s,n,op,x,"refused")
            ELSE LET cfg == [s.cfg EXCEPT !.cv = @ + 1, !.com = FALSE,
                              !.mem = IF op = "add" THEN @ \cup {x} ELSE @ \ {x},
                              !.det[x] = IF op = "add" THEN d ELSE IF x \in s.sh THEN s.known[x] ELSE Nil]
                     next == [s EXCEPT !.cfg = cfg,
                                !.sh = IF op = "add" THEN @ \ {x} ELSE @,
                                !.known[x] = IF x = n THEN d ELSE @,
                                !.newCfgHist = @ \cup {[ct |-> cfg.ct, cv |-> cfg.cv, mem |-> cfg.mem]},
                                !.save2 = TRUE]
                 \* The response precedes the deferred saver.save. A crash
                 \* can intervene. This path leaves role and progress intact.
                 IN RecordAdmin(s,next,n,op,x,"applied")

AdminForceContact(n,op,x) ==
    /\ Running(n) /\ op \in {"add","remove"} /\ x \in Server
    /\ WriteBack(n,ForceReceived(NS(n),n,op,x),conn,ckt)

HandleAdminForce(k,dir,i) ==
    /\ k \in DOMAIN conn /\ dir \in Dirs /\ TubeFrame(k,dir,i)
    /\ Running(Rcv(k,dir)) /\ FrameAt(k,dir,i).m.type = "FORCE"
    /\ LET n == Rcv(k,dir)
           m == FrameAt(k,dir,i).m
       IN WriteBack(n,ForceReceived(NSJ(k,dir,i),n,m.op,m.x),ConsumeTube(conn,k,dir,i),ckt)

\* tuberm -e reaches handleInstallEmptyMC. A named follower installs a
\* fresh configuration in its current term. Existing shadows survive.
\* A wrong target is ignored and a leader refuses the command.
AdminEmptyConfig(n) ==
    /\ Running(n) /\ role[n] /= Leader
    /\ LET s == NS(n)
           cfg == [EmptyCfg EXCEPT !.ct = s.term, !.cv = s.cfg.cv + 1, !.ci = s.ci, !.cit = s.cit,
                            !.det = [m \in Server |-> IF m \in s.sh THEN s.cfg.det[m] ELSE Nil]]
           next == SaveRaftState([s EXCEPT !.cfg = cfg,
                       !.newCfgHist = @ \cup {[ct |-> cfg.ct, cv |-> cfg.cv, mem |-> {}]}])
       IN WriteBack(n,RecordAdmin(s,next,n,"empty",n,"applied"),conn,ckt)

\* Leader admission for the shared replicated shadow command. In-flight
\* commands may apply after membership has changed. Preserve their input
\* address and incarnation all the way through WAL replication.
AdminShadow(n,op,x,d) ==
    /\ Running(n) /\ role[n] = Leader /\ par[n] = "ok" /\ noop0[n].done
    /\ x \in Server
    /\ IF op = "shadowAdd" THEN x \notin config[n].mem /\ d \in Det
       ELSE op = "shadowRemove" /\ x \in shadows[n] /\ d = Nil
    /\ LET s == NS(n)
           entry == [term |-> s.term, id |-> s.nid, kind |-> op, x |-> x, detail |-> d]
           ready == IF op = "shadowRemove" THEN ForgetShadowCircuit(s,x) ELSE s
       IN WriteBack(n,ReplicateCommand(ready,n,entry),conn,ckt)

AdminHasRedirect == \E e \in adminEvents : e.result = "redirect"
AdminHasOverride == \E e \in adminEvents : e.result = "applied" /\ e.op \in {"add","remove"}
AdminRoutingOK ==
    \A e \in adminEvents : e.op \in {"add","remove"} =>
        /\ (e.eligibleLeader => e.result \in {"redirect","noRoute"} /\ e.installed = e.old)
        /\ (e.result = "applied" =>
              /\ e.installed.ct = e.old.ct /\ e.installed.cv = e.old.cv + 1 /\ ~e.installed.com
              /\ e.installed.mem = IF e.op = "add" THEN e.old.mem \cup {e.x} ELSE e.old.mem \ {e.x})


\* WAL compaction (wal.Compact(keepIndex = LastApplied), tube/wal.go:1262-1498)
\* KEEPS entry LastApplied: CompactNewBeg(LastApplied) sets
\* BaseC = LastApplied - 1, CompactTerm = term(LastApplied - 1)
\* (rle.go CompactNewBeg), and does nothing unless LastApplied - 1 > BaseC
\* (1300-1302).  Called by maybeCompact after a WAL write once the log
\* exceeds the size threshold: the leader after appending a batch
\* (6273-6279, 6528-6531), a follower inside overwriteEntries (wal.go:
\* 995-1006) -- modeled as its own step at any point.  keepIndex beyond the
\* last entry panics (1290-1292).  Journaled.
Compact(n) ==
    /\ Running(n) /\ par[n] = "ok"
    /\ lastApplied[n] - 1 > base[n].idx
    /\ IF lastApplied[n] > Len(log[n])
       THEN /\ panics' = panics \cup {"CompactKeepBeyondLast"}
            /\ UNCHANGED <<envVars, netVars, viewVars, raftVars, logVars, durVars, nextId,
                           adminEvents, committed, applied, leaders, ackedCfg, cfgHist, ackHigh, nextSid, clientTickets>>
       ELSE /\ base' = [base EXCEPT ![n] = [idx |-> lastApplied[n] - 1,
                                            term |-> log[n][lastApplied[n] - 1].term]]
            /\ UNCHANGED <<envVars, netVars, viewVars, raftVars, log, idLen, torn, par,
                         dKnownPeers, dTerm, dVote, dCfg, dShadows, dCommit, dCommitTerm, walGone, ghostVars>>
            /\ appendWitness' = [appendWitness EXCEPT ![n].current = Len(log[n])]

\* ------------------------------------------------------------------------
\* Crash in the middle of a plain (unjournaled) WAL append: the entry bytes
\* are fsynced but the PAR sidecar identifiers are not published
\* (tube/wal.go saveRaftLogEntry and sync). Restart fences the node until repair.
\* Overwrites/compaction/snapshot installs are undo-journaled and do not tear.
\* ------------------------------------------------------------------------
CrashWith(n, lg, keepId, s) ==
    /\ up' = [up EXCEPT ![n] = FALSE]
    /\ stopped' = [stopped EXCEPT ![n] = FALSE]
    /\ conn' = CrashConn(n)
    /\ ckt' = CrashCkt(n)
    /\ blk' = [blk EXCEPT ![n] = NoBlk]
    /\ log' = [log EXCEPT ![n] = lg]
    /\ clientTickets' = s.book
    /\ idLen' = [idLen EXCEPT ![n] = keepId]
    /\ dTerm' = IF s.save THEN [dTerm EXCEPT ![n] = s.saved.term] ELSE dTerm
    /\ dVote' = IF s.save THEN [dVote EXCEPT ![n] = s.saved.vote] ELSE dVote
    /\ dCfg' = IF s.save THEN [dCfg EXCEPT ![n] = s.saved.cfg] ELSE dCfg
    /\ dKnownPeers' = IF s.save THEN [dKnownPeers EXCEPT ![n] = s.saved.known] ELSE dKnownPeers
    /\ dShadows' = IF s.save THEN [dShadows EXCEPT ![n] = s.saved.sh] ELSE dShadows
    /\ dCommit' = IF s.save THEN [dCommit EXCEPT ![n] = s.saved.ci] ELSE dCommit
    /\ appendWitness' = [appendWitness EXCEPT ![n].saved = s.savedHigh]
    /\ dCommitTerm' = IF s.save THEN [dCommitTerm EXCEPT ![n] = s.saved.cit] ELSE dCommitTerm
    /\ campaign' = [campaign EXCEPT ![n].persisted = s.campaign.persisted]
    /\ panics' = panics \cup s.newPanics
    /\ UNCHANGED <<inc, addr, link, nextConn, nextCkt, viewVars, raftWithoutCampaign, mcPending, base, torn, par, walGone,
                   adminEvents, committed, applied, leaders, ackedCfg, cfgHist, ackHigh, nextSid>>

\* ------------------------------------------------------------------------
\* Storage faults beyond fail-stop (PAR's fault model, brief S9).  An append
\* fsyncs the entry AND publishes its identifier before it is acknowledged
\* (wal.go:579-585: s.sync fsyncs the WAL, then parlog.sync), so these are
\* disk faults found at the next start, not crash windows:
\*   "lastId": the last cnt published PAR identifiers are lost (a lost or
\*             torn write at the sidecar tail; the entry bytes survive);
\*   "suffix": both files lose a matching tail. Startup sees an internally
\*             valid shorter log and checks the locally saved loss floor.
\*   "wal":    both files disappear. The snapshot anchor, state file and
\*             application checkpoint survive. The missing files fence.
StorageFaults == {"lastId", "suffix", "wal"}
CrashDiskFault(n, kind, cnt) ==
    /\ up[n] /\ kind \in StorageFaults /\ cnt \in Nat \ {0}
    /\ (kind = "lastId" => idLen[n] - cnt >= base[n].idx)
    /\ (kind = "suffix" => Len(PhysicalHistory(n)) - cnt >= base[n].idx)
    /\ up' = [up EXCEPT ![n] = FALSE]
    /\ stopped' = [stopped EXCEPT ![n] = FALSE]
    /\ conn' = CrashConn(n)
    /\ ckt' = CrashCkt(n)
    /\ blk' = [blk EXCEPT ![n] = NoBlk]
    /\ IF kind = "lastId"
       THEN /\ idLen' = [idLen EXCEPT ![n] = @ - cnt]
            /\ UNCHANGED <<log, torn, walGone>>
       ELSE LET disk == PhysicalHistory(n)
                keep == IF kind = "wal" THEN base[n].idx ELSE Len(disk) - cnt
            IN /\ log' = [log EXCEPT ![n] = SubSeq(disk, 1, keep)]
               /\ torn' = [torn EXCEPT ![n] = <<>>]
               /\ idLen' = [idLen EXCEPT ![n] = Min(@, keep)]
               /\ walGone' = [walGone EXCEPT ![n] = @ \/ kind = "wal"]
    /\ UNCHANGED <<inc, addr, link, nextConn, nextCkt, viewVars, raftVars, base, par,
                   dKnownPeers, dTerm, dVote, dCfg, dShadows, dCommit, dCommitTerm, appendWitness, ghostVars>>

\* follower: the plain-append branch of handleAppendEntries (8516-8544) tears
HandleAppendEntriesTornCrash(k, dir, i) ==
    /\ k \in DOMAIN conn /\ dir \in Dirs
    /\ TubeFrame(k, dir, i) /\ FrameAt(k, dir, i).m.type = "AE"
    /\ Running(Rcv(k, dir))
    /\ LET n  == Rcv(k, dir)
           ae == FrameAt(k, dir, i).m
           s  == NSJ(k, dir, i)
           r  == AEStep(s, n, ae)
       IN /\ s.par = "ok"
          /\ r.branch = "ExtendsAppend"
          \* r.pre: state just before the WAL write (prelude saves already done)
          /\ CrashWith(n, r.wlog, Len(s.log), r.pre)
          /\ UNCHANGED nextId

\* leader: replicateTicket's saveRaftLogEntry of a client entry tears (6262)
ClientRequestTornCrash(n) ==
    /\ Running(n) /\ role[n] = Leader /\ par[n] = "ok" /\ noop0[n].done
    /\ CrashWith(n, Append(log[n], [term |-> currentTerm[n], id |-> nextId, kind |-> "data"]),
                 Len(log[n]), BindDataEntry(RegisterDataTicket(NS(n), FALSE), Len(clientTickets) + 1))
    /\ nextId' = nextId + 1

\* ------------------------------------------------------------------------
\* PAR recovery (par.go:2295-2420), ticked every 100-200 ms on the event loop
\* ------------------------------------------------------------------------

\* parIdentifierCandidate checks the WAL frame, checksum, cluster and previous
\* entry chain. The modeled interruption and lost-ID faults retain these bytes.
CanIdentify(n) ==
    /\ Running(n) /\ par[n] = "unknown"
    /\ idLen[n] < Len(PhysicalHistory(n))
    /\ ~walGone[n]

\* Both repair paths install one identifier and rescan. The open log still
\* exposes the old prefix until parFinish opens the verified WAL.
InstallIdentifier(n) ==
    LET count == idLen[n] + 1 IN
    /\ idLen' = [idLen EXCEPT ![n] = count]
    /\ par' = [par EXCEPT ![n] = IF count = Len(PhysicalHistory(n)) /\ count >= ParProtected(NS(n))
                                  THEN "ready" ELSE "unknown"]
    /\ UNCHANGED <<envVars, netVars, viewVars, raftVars, log, base, torn, durVars, ghostVars>>

ParIdentifyInterrupted(n) ==
    /\ CanIdentify(n) /\ idLen[n] + 1 > ParProtected(NS(n))
    /\ InstallIdentifier(n)

\* The requester's PAR round and the donor both check membership and circuit
\* identity (tube/par.go parDonors, parHandle, parRound.add). A valid record
\* can come from a shadow. Only absence certificates require a voter quorum.
CanQueryDonor(n, m) ==
    /\ CanIdentify(n) /\ m /= n /\ Running(m)
    /\ idLen[n] + 1 <= ParProtected(NS(n))
    /\ m \in config[n].mem \cup shadows[n]
    /\ n \in config[m].mem \cup shadows[m]
    /\ LET c == byName[n][m] IN
       /\ c > 0 /\ EpSt(c, n) = EpOpen /\ EpSt(c, m) = EpOpen
       /\ PeerKeyOf(c, n) = <<m, inc[m]>> /\ PeerKeyOf(c, m) = <<n, inc[n]>>
    /\ link[n][m] = "ok" /\ link[m][n] = "ok"

\* A higher-term PAR query steps its receiver down even if that receiver has
\* no matching bytes. A donor in a higher term discards an older query.
ParObserveQuery(n, m) ==
    /\ CanQueryDonor(n, m) /\ currentTerm[m] < currentTerm[n]
    /\ WriteBack(m, BecomeFollower(NS(m), m, currentTerm[n], Nil, TRUE), conn, ckt)

ParRecoverFromDonor(n, m) ==
    /\ CanQueryDonor(n, m) /\ currentTerm[m] = currentTerm[n]
    /\ LET idx == idLen[n] + 1 IN
       /\ idx <= idLen[m] /\ idx > base[m].idx
       /\ PhysicalHistory(m)[idx] = PhysicalHistory(n)[idx]
    /\ InstallIdentifier(n)

\* parFinish reopens the verified physical log. An election can have been
\* won between the last identifier repair and this recovery tick.
ParFinish(n) ==
    /\ Running(n) /\ par[n] = "ready"
    /\ LET s == [NS(n) EXCEPT !.log = PhysicalHistory(n), !.par = "ok"]
           done == IF s.role = Leader THEN BecomeLeader([s EXCEPT !.role = Candidate], n)
                                      ELSE s
       IN WriteBack(n, done, conn, ckt)

\* parRequestSnapshot (par.go:2640-2654): ask the current leader for a snapshot
ParRequestSnapshot(n) ==
    /\ Running(n) /\ par[n] = "unknown"
    /\ role[n] = Follower /\ leaderKey[n] /= Nil
    /\ cktall[n][leaderKey[n]] /= NoCkt
    /\ WriteBack(n, AddOut(NS(n), cktall[n][leaderKey[n]],
                           [type |-> "RSS", from |-> <<n, inc[n]>>]), conn, ckt)

\* ========================================================================
\* ACTIONS: TIME PASSING (abstract time)
\* ========================================================================

\* lastLegitAppendEntries older than MinElection + ClockDriftBound (12534-12544)
StickyExpire(n) ==
    /\ up[n] /\ sticky[n]
    /\ NoCreditAt(n)
    /\ (TimelyHeartbeats => ~LeaderReachesMe(n))
    /\ sticky' = [sticky EXCEPT ![n] = FALSE]
    /\ UNCHANGED <<envVars, netVars, viewVars, campaign, role, currentTerm, votedFor, leaderKey,
                   pvTerm, pv2Term, pvYes, pvNo, vYes, vNo, peers, pong, leaderAged, startupPhase, noop0,
                   stallQ, mcPending, config, shadows, commitIndex, commitIndexTerm, lastApplied, lastAppliedTerm, snapSlot,
                   logVars, durVars, ghostVars>>

\* LastFullPing.Sent leaves the read-lease window (12353-12370)
LeaseCreditExpire(l, key) ==
    /\ up[l] /\ key \in Keys
    /\ peers[l][key] /= Nil
    /\ \/ /\ peers[l][key].credit
          /\ peers' = [peers EXCEPT ![l][key].credit = FALSE]
       \/ /\ peers[l][key].pingFresh
          /\ peers' = [peers EXCEPT ![l][key].pingFresh = FALSE, ![l][key].credit = FALSE]
    /\ UNCHANGED <<envVars, netVars, viewVars, campaign, role, currentTerm, votedFor, leaderKey, sticky,
                   pvTerm, pv2Term, pvYes, pvNo, vYes, vNo, pong, leaderAged, startupPhase, noop0,
                   stallQ, mcPending, config, shadows, commitIndex, commitIndexTerm, lastApplied, lastAppliedTerm, snapSlot,
                   logVars, durVars, ghostVars>>

\* a pong older than followerDownDur (5.3 s > the lease window)
PongExpire(l, key) ==
    /\ up[l] /\ key \in pong[l].fresh
    /\ ~HasCredit(l, key)
    /\ pong' = [pong EXCEPT ![l].fresh = @ \ {key}]
    /\ UNCHANGED <<envVars, netVars, viewVars, campaign, role, currentTerm, votedFor, leaderKey, sticky,
                   pvTerm, pv2Term, pvYes, pvNo, vYes, vNo, peers, leaderAged, startupPhase, noop0,
                   stallQ, mcPending, config, shadows, commitIndex, commitIndexTerm, lastApplied, lastAppliedTerm, snapSlot,
                   logVars, durVars, ghostVars>>

\* leader for at least followerDownDur (1497-1503)
SetQuorumClocks(n, aged, phase) ==
    /\ leaderAged' = [leaderAged EXCEPT ![n] = aged]
    /\ startupPhase' = [startupPhase EXCEPT ![n] = phase]
    /\ UNCHANGED <<envVars, netVars, viewVars, campaign, role, currentTerm, votedFor, leaderKey, sticky,
                   pvTerm, pv2Term, pvYes, pvNo, vYes, vNo, peers, pong, noop0,
                   stallQ, mcPending, config, shadows, commitIndex, commitIndexTerm, lastApplied, lastAppliedTerm, snapSlot,
                   logVars, durVars, ghostVars>>

LeaderAge(l) ==
    /\ up[l] /\ role[l] = Leader /\ ~leaderAged[l]
    /\ SetQuorumClocks(l, TRUE, startupPhase[l])

\* Wall-clock passage can be observed while a process is stopped. The
\* one-time commit scan is performed by an accepted AppendEntriesAck.
StartupClockTick(n) ==
    /\ up[n] /\ startupPhase[n] \in {"unanimous", "deadline"}
    /\ SetQuorumClocks(n, leaderAged[n], IF startupPhase[n] = "unanimous" THEN "deadline" ELSE "recheck")


\* ========================================================================
\* NEXT-STATE RELATION
\* ========================================================================

FrameIdx(k, dir) == 1..Len(conn[k].q[dir])

FrameNext ==
    \E k \in DOMAIN conn, dir \in Dirs :
      \E i \in FrameIdx(k, dir) :
        \/ HandleRequestPreVote(k, dir, i)
        \/ HandlePreVote(k, dir, i)
        \/ HandleRequestVote(k, dir, i)
        \/ HandleVote(k, dir, i)
        \/ HandleAppendEntries(k, dir, i)
        \/ HandleAppendEntriesAck(k, dir, i)
        \/ HandleTicketFrame(k, dir, i)
        \/ HandleStateSnapshot(k, dir, i)
        \/ HandleRequestStateSnapshotMsg(k, dir, i)
        \/ RejectNamesakeFrame(k,dir,i)
        \/ HandleEndCircuit(k, dir, i)
        \/ HandlePeerError(k, dir, i)
        \/ DropUnroutable(k, dir, i)
        \/ RejectForeignFrame(k, dir, i)
        \/ HandleStartFrame(k, dir, i)
        \/ HandleSetupAck(k, dir, i)
        \/ \E b \in BOOLEAN : StalledReceiverDrop(k, dir, i, b)
        \/ HandleAppendEntriesTornCrash(k, dir, i)
        \/ \E key \in Keys : AgeGossip(k,dir,i,key)

CoreNext ==
    \/ \E n \in Server : SubmitAdminTicket(n)
    \/ \E n \in Server :
        \/ Crash(n) \/ Stop(n) \/ Cont(n)
        \/ \E kind \in StorageFaults : CrashDiskFault(n, kind, 1)
        \/ \E a \in Addrs : Restart(n, a)
        \/ AdmitWaiting(n) \/ UnblockSend(n)
        \/ ElectionTimeout(n) \/ PreVotePhase2Timeout(n) \/ LeaderHeartbeat(n)
        \/ RestartWindowTick(n) \/ RestartWindowElapsed(n) \/ RunDeferredCampaign(n) \/ ResumeCampaign(n)
        \/ BecomeLeaderSave(n) \/ ClientRequest(n) \/ ClientRequestTornCrash(n)
        \/ (\E timed \in BOOLEAN : ClientSubmit(n, timed)) \/ TicketWork(n)
        \/ Compact(n) \/ AuditGC(n) \/ ParRequestSnapshot(n) \/ MCDeadlineElapsed(n)
        \/ StickyExpire(n) \/ LeaderAge(n) \/ StartupClockTick(n)
        \/ ParIdentifyInterrupted(n) \/ ParFinish(n)
        \/ \E m \in Server : ParObserveQuery(n, m)
        \/ \E m \in Server : DialAttempt(n, m) \/ WatchdogSilence(n,m) \/ ParRecoverFromDonor(n, m)
        \/ \E opened \in BOOLEAN : ObserverCircuit(n,opened)
        \/ \E op \in {"add", "remove"}, x \in Server : ClientChangeMembership(n, op, x)
        \/ \E key \in Keys : LeaseCreditExpire(n, key) \/ PongExpire(n, key) \/ AckAge(n,key)
        \/ \E c \in DOMAIN ckt : HandleNewCircuit(n, c) \/ HandlePeerLeft(n, c) \/ HaltOnDeadConn(n, c) \/ CancelPendingDial(n,c)
    \/ \E x, y \in Server : LinkDown(x, y) \/ LinkUp(x, y)
    \/ \E k \in DOMAIN conn : \/ \E side \in {"d", "a"} : ConnAbort(k, side)
                             \/ \E dir \in Dirs :
                                   \/ AckFlush(k,dir) \/ StartWrite(k,dir) \/ WriteQueuedFrame(k,dir)
                                   \/ BeginPartialWrite(k,dir) \/ ExpireQueuedFrame(k,dir) \/ RefuseQueuedFrame(k,dir) \/ FramingReadError(k,dir) \/ PartialKeepAlive(k,dir)
                                   \/ \E f \in conn[k].bg[dir] : QueueCloseNotice(k,dir,f) \/ ExpireCloseNotice(k,dir,f)
    \/ FrameNext


\* ========================================================================
\* INVARIANTS
\* ========================================================================

\* A successful caller must refer to the exact entry a leader committed.
\* Step-down/timeout errors can precede a later commit of the same request.
TicketSuccessCommitted ==
    \A t \in DOMAIN clientTickets :
        LET r == clientTickets[t].result IN
        IF r = Nil THEN TRUE ELSE
        r.status = "applied" =>
            /\ \E e \in clientTickets[t].entries : e.id = r.entry
            /\ \E e \in committed : e.id = r.entry

\* Finishing an applied copy finishes the waiting original. A later reply,
\* follower apply, expiration or step-down preserves the observed outcome.
TicketResultsStable ==
    [][\A t \in DOMAIN clientTickets :
        /\ (clientTickets[t].result /= Nil => clientTickets'[t].result = clientTickets[t].result)
        /\ (clientTickets[t].returned /= Nil => clientTickets'[t].returned = clientTickets[t].returned)]_coreVars

\* ---------------- type / structural ----------------
TypeOK ==
    /\ startupPhase \in [Server -> {"unanimous", "deadline", "recheck", "majority"}]
    /\ up \in [Server -> BOOLEAN] /\ stopped \in [Server -> BOOLEAN]
    /\ inc \in [Server -> Incs] /\ addr \in [Server -> Addrs]
    /\ link \in [Server -> [Server -> {"ok", "drop"}]]
    /\ role \in [Server -> {Follower, Candidate, Leader}]
    /\ campaign \in [Server -> [held : BOOLEAN, remaining : Nat, configured : Nat, promise : Nat,
                                 persisted : Nat, releaseOnAE : BOOLEAN, deferred : {"none","preVote","election"},
                                 resume : {"none","requestVotes","bootstrap","commitNoop"}]]
    /\ \A n \in Server : /\ campaign[n].held = (campaign[n].remaining > 0)
                         /\ campaign[n].remaining <= campaign[n].promise
                         /\ campaign[n].configured <= campaign[n].promise
                         /\ campaign[n].promise <= campaign[n].persisted
    /\ currentTerm \in [Server -> Nat] /\ dTerm \in [Server -> Nat]
    /\ votedFor \in [Server -> Keys \cup {Nil}] /\ dVote \in [Server -> Keys \cup {Nil}]
    /\ leaderKey \in [Server -> Keys \cup {Nil}]
    /\ par \in [Server -> {"ok", "unknown", "ready"}]
    /\ appendWitness \in [Server -> [current: Nat, saved: Nat]]
    /\ \A n \in Server : /\ Len(snapSlot[n]) <= 4
                          /\ Cardinality({snapSlot[n][j] : j \in 1..Len(snapSlot[n])}) = Len(snapSlot[n])
    /\ walGone \in [Server -> BOOLEAN]
    /\ \A n \in Server :
         /\ cktall[n] \in [Keys -> Nat]
         /\ byName[n] \in [Server -> Nat \cup {Pending}]
         /\ cktReplica[n] \subseteq Keys
         /\ \A k \in Keys : peers[n][k] = Nil \/ peers[n][k] \in PeerRec
         /\ config[n].mem \subseteq Server /\ shadows[n] \subseteq Server
         /\ base[n].idx \in Nat /\ idLen[n] \in Nat
         /\ commitIndex[n] \in Nat /\ commitIndexTerm[n] \in Nat /\ lastApplied[n] \in Nat
    /\ nextId \in Nat /\ nextConn \in Nat /\ nextCkt \in Nat

\* structural sanity (hold in every correct state)
LogBaseOK ==
    \A n \in Server :
        /\ base[n].idx <= Len(log[n])
        /\ (base[n].idx > 0 => log[n][base[n].idx].term = base[n].term)
        /\ base[n].idx <= idLen[n] /\ idLen[n] <= Len(PhysicalHistory(n))
        /\ (up[n] /\ par[n] = "ok" => torn[n] = <<>> /\ idLen[n] = Len(log[n]))
CommitIndexTermSound ==
    \A n \in Server :
        (up[n] /\ par[n] = "ok" /\ commitIndex[n] > 0) =>
            /\ commitIndex[n] <= Len(log[n])
            /\ commitIndexTerm[n] = log[n][commitIndex[n]].term

AppliedLenOK == \A n \in Server : Len(applied[n]) = lastApplied[n]
ViewsConsistent ==
    \A n \in Server : up[n] =>
        /\ \A k \in cktReplica[n] : cktall[n][k] /= NoCkt
        /\ \A k \in Keys : cktall[n][k] /= NoCkt =>
              /\ cktall[n][k] \in DOMAIN ckt
              /\ PeerKeyOf(cktall[n][k], n) = k
BlockedOnlyWhenUp == \A n \in Server : blk[n] /= NoBlk => up[n]

\* ---------------- the five classic Raft properties ----------------

\* At most one leader per term, compared by name (ElectionSafety) and by
\* incarnation (ElectionSafetyInc).
ElectionSafety ==
    \A a, b \in leaders : a.term = b.term => a.name = b.name
ElectionSafetyInc ==
    \A a, b \in leaders : a.term = b.term => a = b

\* Two logs with an entry of the same index and term agree on that entry and
\* the whole prefix (by entry identity).
LogMatching ==
    \A n, m \in Server :
        \A i \in 1..Min(Len(log[n]), Len(log[m])) :
            log[n][i].term = log[m][i].term =>
                \A j \in 1..i : log[n][j].id = log[m][j].id

\* Every entry a leader declared committed is in the log of every live
\* leader of a later term.
LeaderCompleteness ==
    \A e \in committed : \A l \in Server :
        (up[l] /\ role[l] = Leader /\ currentTerm[l] > e.term) =>
            (e.idx <= Len(log[l]) /\ log[l][e.idx].id = e.id)

\* No two replicas apply different entries at the same position.
StateMachineSafety ==
    \A n, m \in Server :
        \A i \in 1..Min(Len(applied[n]), Len(applied[m])) :
            applied[n][i].id = applied[m][i].id

\* A leader never truncates or overwrites its own log within its term (PAR
\* discard is not modeled).  Action property.
IsPrefix(a, b) == Len(a) <= Len(b) /\ \A i \in 1..Len(a) : a[i] = b[i]
LeaderAppendOnly ==
    [][\A l \in Server :
          (up[l] /\ up'[l] /\ inc'[l] = inc[l] /\ role[l] = Leader /\ role'[l] = Leader
           /\ currentTerm'[l] = currentTerm[l])
          => IsPrefix(log[l], log'[l])]_coreVars

\* ---------------- data loss / scenario invariants ----------------

\* A singleton can append/fsync its no-op before the final RaftState save.
\* Restart reconciles that window. Multi-voter candidates save before sending.
CandidateVoteDurable ==
    \A n \in Server : (up[n] /\ role[n] = Candidate) =>
        (dTerm[n] = currentTerm[n] /\ dVote[n] = <<n, inc[n]>>)

\* Stronger diagnostic including the singleton's intermediate fsync state.
DurableTermCoversLog == \A n \in Server : dTerm[n] >= LastTerm(n)

\* S2/S8 diagnostic: a leader's MatchIndex for a live follower incarnation
\* is evidence of a matching prefix (handleAppendEntriesAck).
MatchIndexSound ==
    \A l \in Server : (up[l] /\ role[l] = Leader) =>
        \A k \in Keys :
            LET f == KName(k) IN
            (peers[l][k] /= Nil /\ peers[l][k].mi > 0 /\ up[f] /\ inc[f] = KInc(k)) =>
                LET mi == peers[l][k].mi IN
                /\ mi <= Len(log[f]) /\ mi <= Len(log[l])
                /\ \A j \in 1..mi : log[f][j].id = log[l][j].id

\* S3 diagnostic: the LCRI that TermsRLE.Extends computes never exceeds the
\* true common prefix with the AE's sender (D-6).  Checked on acks in flight.
LcriSound ==
    \A m \in QueuedMessages : m.type = "AEA" => m.gx <= m.gt

\* S3: the production panic "terms should be monotone up" (D-91) is unreachable.
AppliedTermsMonotone ==
    /\ "TermsMonotoneUp" \notin panics
    /\ \A n \in Server : \A i \in 2..Len(applied[n]) :
           applied[n][i-1].term <= applied[n][i].term

\* S8: entries a follower acknowledged (accept) in term t stay in its log
\* while it remains in term t (D-5).  A storage fault (CrashDiskFault) may
\* destroy acknowledged entries; the code's promise then is the PAR fence
\* (par = "unknown": no votes, no campaigns, no AE processing beyond
\* parObserveAE) until recovery, so fenced nodes are exempt.  An UNFENCED node
\* missing an entry it acknowledged in its current term still violates it.
AckedStays ==
    \A n \in Server : (up[n] /\ par[n] = "ok" /\ ackHigh[n].term = currentTerm[n]) =>
        \A c \in ackHigh[n].claims : c.idx <= Len(log[n]) /\ log[n][c.idx].id = c.id

\* The commit and read paths consume one selected record per voting name.
\* A removed leader is allowed to continue replication until C' commits.
CommitCounted(l) == VotingPeers(NS(l),l) \cup
                    (IF l \in config[l].mem THEN {<<l,inc[l]>>} ELSE {})
LeaseCounted(l) == {k \in VotingPeers(NS(l),l) : peers[l][k].credit}
DistinctVoters(l,S) ==
    /\ {KName(k) : k \in S} \subseteq config[l].mem
    /\ Cardinality(S) = Cardinality({KName(k) : k \in S})
QuorumCountsDistinctVoters ==
    \A l \in Server : role[l] = Leader /\ up[l] =>
        DistinctVoters(l,CommitCounted(l)) /\ DistinctVoters(l,LeaseCounted(l))

\* tube.go: leaderCanServeReadsLocally. Its singleton fast path and the
\* multi-node quorum-minus-one threshold are applied here as written.
LeaseHolds(l) ==
    /\ up[l] /\ role[l] = Leader /\ Running(l) /\ par[l] = "ok"
    /\ \/ ClusterSize(l) <= 1
       \/ /\ noop0[l].done
          /\ Cardinality(LeaseCounted(l)) >= Quorum(l) - 1

\* S4/S10: a leader that can serve a local read under its lease has not been
\* overtaken: no entry of a higher term has been committed.
LeaseSafety ==
    \A l \in Server : LeaseHolds(l) => ~\E e \in committed : e.term > currentTerm[l]

\* Logless durability under ordinary reconfiguration. A later inverse
\* change is effective at installation even when its client times out.
\* Forced administrative reconfiguration may replace an isolated config.
AckedReconfigDurable ==
    \A a \in ackedCfg : \A n \in Server :
        (up[n] /\ VersionGT(config[n],a)
         /\ ~\E m \in Server : \E b \in mcPending[m].installed :
              b.x = a.x /\ b.op /= a.op /\ VersionGT(b,a) /\ VersionGTE(config[n],b))
        => IF a.op = "add" THEN a.x \in config[n].mem ELSE a.x \notin config[n].mem

\* A successful change was installed, and its ticket has a single outcome.
MembershipTicketOutcomes ==
    \A a \in ackedCfg :
        /\ a \in mcPending[a.id[1]].installed
        /\ ~\E f \in mcPending[a.id[1]].failed : f.id = a.id

\* S5: (ConfigTerm, ConfigVersion) determines one member set.
ConfigUnique ==
    \A a, b \in cfgHist : (a.ct = b.ct /\ a.cv = b.cv) => a.mem = b.mem

\* Only leaders retain pending membership work. This also checks the
\* step-down cleanup on equal-term AppendEntries from a rival leader.
NoFollowerLeaderOnlyPath ==
    \A n \in Server : (up[n] /\ role[n] /= Leader) =>
        (stallQ[n] = <<>> /\ mcPending[n].awaiting = {} /\ mcPending[n].watch = <<>>)

\* every modeled panic site
NoPanic == panics = {}

\* ---------------- connection-level diagnostics ----------------


\* A healthy live pair can close on a partial write or a terminal framing
\* error. Rejecting stale or unroutable traffic does neither. closedBy is
\* set by the action that actually closes the sending/receiving endpoint.
NoCollateralTeardown ==
    [][\A k \in DOMAIN conn, dir \in Dirs :
          LET p == conn[k]
              healthy == /\ up[p.a] /\ up[p.d] /\ up'[p.a] /\ up'[p.d]
                         /\ p.ai = inc[p.a] /\ p.di = inc[p.d] /\ p.dOpen /\ p.aOpen
                         /\ link[p.a][p.d] = "ok" /\ link[p.d][p.a] = "ok"
          IN (healthy /\ ~DirOpen(conn'[k], dir)) =>
             \/ conn'[k].closedBy[dir] = "read-error"
             \/ /\ conn'[k].closedBy[dir] = "setup-ack-refused"
                /\ Len(p.out[dir]) = SendBufCap
                /\ \E i \in DOMAIN p.q[OtherDir(dir)] : p.q[OtherDir(dir)][i].m.type = "START"
             \/ /\ conn'[k].closedBy[dir] = "partial-keepalive"
                /\ dir \in KeepAliveDirections /\ p.writer[dir] = Nil
             \/ /\ conn'[k].closedBy[dir] = "partial-write"
                /\ p.writer[dir] /= Nil /\ p.writer[dir].partial]_coreVars

SendQueueBounded ==
    \A k \in DOMAIN conn, dir \in Dirs : Len(conn[k].out[dir]) <= SendBufCap

\* ========================================================================
\* KV state machine, leases and sessions (S10)
\* ========================================================================
\* Raft supplies one committed order. Replicas can lag delivery and apply.
\* One representative key in "data" can move to the table "renamed".
\* Scalar time is the physical component of an HLC, rounded to ticks.
CONSTANTS KVReplicas, KVClients, KVMaxTime, KVLeaseDur, KVOff, KVDrift,
          KVSessDur, KVMaxLog, KVMaxSerial, KVNil

VARIABLES
    kvNow, kvLeader, kvMem, kvApplied,
    kvState,      \* tables, sessions, expiry index and LastAppliedLogHLC per replica
    kvSerial,     \* next client serial by client and session
    kvBelieves,   \* lease deadline last granted to each client
    kvMuts, kvDup, kvFirst, kvReply,
    kvLast,      \* most recent response delivered to each client
    kvIssued,    \* last request per client, including leader-local requests
    kvReceived   \* client/SessionID/serial replies available to acknowledge

kvVars == <<kvNow, kvLeader, kvMem, kvApplied, kvState, kvSerial,
            kvBelieves, kvMuts, kvDup, kvFirst, kvReply, kvLast, kvIssued, kvReceived>>
KVClock(n) == kvNow + KVOff[n]
KVTables == {"data", "renamed"}
KVAbsent == [exists |-> FALSE, leaf |-> KVNil]
KVTable(lf) == [exists |-> TRUE, leaf |-> lf]
KVSessionIds == 1..KVMaxLog
KVEmptyCache == [i \in {} |-> KVNil]
KVScans == {"scan", "bigScan", "largeEnvelopeScan"}
KVReads == {"read"} \cup KVScans
KVCAS == {"cas", "casVersion", "casValue"}
KVWrites == {"write", "lwrite", "deleteKey", "makeTable", "deleteTable", "renameTable"} \cup KVCAS
KVSessionOps == {"sessnew", "sessend"}
KVInitialState ==
    [tables |-> [t \in KVTables |-> KVAbsent], sessions |-> [c \in KVClients |-> KVNil],
     expiry |-> [c \in KVClients |-> 0], clock |-> 0]
kvStore == [n \in KVReplicas |-> kvState[n].tables["data"].leaf]
kvSess == [n \in KVReplicas |-> kvState[n].sessions]

\* prepOne (tube/tube.go) stamps wall time, LogHLC and ClockDriftBound
\* into a private log copy. logClockDriftBound uses the deployment default
\* of 500ms (one model tick) for an older unstamped ticket.
KVLoggedDrift(e) == IF e.drift = KVNil THEN 1 ELSE e.drift
KVHLCs == KVClock(kvLeader)..MaxSet({KVClock(n) : n \in KVReplicas})
KVEntry(op, c, sid, serial, oldE, ack, hlc) ==
    LET tm == KVClock(kvLeader)
    IN [op |-> op, cl |-> c, sid |-> sid, serial |-> serial, oldE |-> oldE, oldVal |-> c,
        tm |-> tm, hlc |-> hlc, drift |-> KVDrift[kvLeader], ack |-> ack,
        known |-> 0, until |-> IF op \in {"lwrite"} \cup KVCAS THEN tm + KVLeaseDur ELSE 0,
        leasor |-> IF op \in {"lwrite"} \cup KVCAS THEN c ELSE KVNil, autoDel |-> op \in {"lwrite"} \cup KVCAS]

KVKey(e) == <<e.cl, e.sid, e.serial>>
KVResponse(code, value) == [code |-> code, value |-> value]
\* A bigScan represents a remote range result larger than UserMaxPayload.
\* respondToClientTicketApplied drops its payload and sends an error. If
\* the reduced envelope still exceeds the limit there is no wire reply.
KVWire(op, res) ==
    IF op = "largeEnvelopeScan" THEN KVNil
    ELSE IF op = "bigScan" /\ res.code = "ok" THEN KVResponse("resultTooLarge", KVNil)
    ELSE res
KVDeliver(c, op, res) ==
    LET wire == KVWire(op,res)
    IN IF wire = KVNil THEN kvLast ELSE [kvLast EXCEPT ![c] = wire]
KVOutcome(st, code, value) == [state |-> st, res |-> KVResponse(code, value)]
KVNewLeaf(e, i, epoch, version) ==
    [val |-> e.cl, leasor |-> e.leasor, pid |-> e.cl, until |-> e.until,
     epoch |-> epoch, ver |-> version, autoDel |-> e.autoDel, widx |-> i]
KVVisible(lf, tm) == IF lf = KVNil THEN FALSE ELSE ~(lf.autoDel /\ lf.leasor /= KVNil /\ lf.until < tm)
KVBlocksRemoval(e, lf) ==
    IF lf = KVNil THEN FALSE ELSE lf.leasor /= KVNil /\ lf.leasor /= e.leasor
                                 /\ e.tm <= lf.until + KVLoggedDrift(e)

\* actions.go kvstoreWrite and writeFailedSetCurrentVal. A rejected write
\* returns the existing leaf with a drift-padded deadline on its reply copy.
KVRejected(st, lf, drift) ==
    KVOutcome(st, "rejected", [lf EXCEPT !.until = @ + drift])
KVStoreWrite(st, e, i, tm, drift) ==
    LET lf == st.tables["data"].leaf
        leased == IF lf = KVNil THEN FALSE ELSE lf.leasor /= KVNil
        same == IF ~leased THEN FALSE ELSE lf.leasor = e.leasor /\ lf.pid = e.cl
        blocked == IF lf = KVNil THEN FALSE ELSE i <= lf.widx \/
                    (leased /\ ((e.leasor /= KVNil /\ e.until <= lf.until) \/ (~same /\ tm <= lf.until + drift)))
    IN IF blocked THEN KVRejected(st, lf, drift)
       ELSE LET epoch == IF lf = KVNil THEN 1 ELSE IF same THEN lf.epoch ELSE lf.epoch + 1
                ver == IF lf = KVNil THEN 1 ELSE lf.ver + 1
                next == KVNewLeaf(e, i, epoch, ver)
            IN KVOutcome([st EXCEPT !.tables["data"] = KVTable(next)], "ok", next)

\* doCAS checks the first supplied comparison in order: version, lease
\* epoch, old value. The op selects that effective comparison. Each write
\* uses the client's name as its representative value. No supplied compare
\* falls through to ordinary write, including on an existing key.
KVDoCAS(st, e, i) ==
    LET lf == st.tables["data"].leaf
        drift == KVLoggedDrift(e)
    IN IF lf = KVNil
       THEN IF e.oldE > 0 \/ e.op = "casValue" THEN KVOutcome(st, "missing", KVNil)
            ELSE KVStoreWrite(st, e, i, e.tm, drift)
       ELSE IF KVBlocksRemoval(e, lf) \/
               CASE e.op = "casVersion" -> e.oldE > 0 /\ e.oldE /= lf.ver
                 [] e.op = "casValue" -> e.oldVal /= lf.val
                 [] OTHER -> e.oldE > 0 /\ e.oldE /= lf.epoch
            THEN KVRejected(st, lf, drift)
            ELSE KVStoreWrite(st, e, i, e.tm, drift)

\* GetLeaf/getLeafAsOf and scanTable (tube/kv.go) only hide expired leaves.
\* doDeleteKey uses the filtered lookup. Table operations inspect raw leaves,
\* including those hidden from reads, before changing the entire table.
KVExecute(st, e, i) ==
    LET tab == st.tables["data"]
        lf == tab.leaf
        seen == KVVisible(lf, e.tm)
    IN CASE e.op \in {"write","lwrite"} -> KVStoreWrite(st, e, i, e.tm, KVLoggedDrift(e))
         [] e.op \in KVCAS -> KVDoCAS(st, e, i)
         [] e.op = "read" -> IF seen THEN KVOutcome(st, "ok", lf) ELSE KVOutcome(st, "missing", KVNil)
         [] e.op = "legacyInvalidKey" -> KVOutcome(st, "keyTooLarge", KVNil)
         [] e.op \in KVScans -> IF tab.exists
                             THEN KVOutcome(st, "ok", IF seen THEN {lf} ELSE {})
                             ELSE KVOutcome(st, "missing", KVNil)
         [] e.op = "makeTable" -> KVOutcome([st EXCEPT !.tables["data"].exists = TRUE], "ok", KVNil)
         [] e.op = "deleteKey" ->
                IF ~seen THEN KVOutcome(st, "missing", KVNil)
                ELSE IF KVBlocksRemoval(e, lf) THEN KVOutcome(st, "leased", KVNil)
                ELSE KVOutcome([st EXCEPT !.tables["data"].leaf = KVNil], "ok", KVNil)
         [] e.op = "deleteTable" ->
                IF KVBlocksRemoval(e, lf) THEN KVOutcome(st, "leased", KVNil)
                ELSE KVOutcome([st EXCEPT !.tables["data"] = KVAbsent], "ok", KVNil)
         [] e.op = "renameTable" ->
                IF KVBlocksRemoval(e, lf) THEN KVOutcome(st, "leased", KVNil)
                ELSE IF ~tab.exists \/ st.tables["renamed"].exists THEN KVOutcome(st, "tableError", KVNil)
                ELSE KVOutcome([st EXCEPT !.tables["data"] = KVAbsent, !.tables["renamed"] = tab], "ok", KVNil)

KVSession(st, c, sid) ==
    IF sid = 0 THEN KVNil ELSE LET s == st.sessions[c] IN
        IF s = KVNil THEN KVNil ELSE IF s.id = sid THEN s ELSE KVNil
KVCachePut(cache, serial, res) ==
    [k \in DOMAIN cache \cup {serial} |-> IF k = serial THEN res ELSE cache[k]]
KVTrim(cache, minWaiting) == [k \in {v \in DOMAIN cache : v >= minWaiting} |-> cache[k]]
KVSetSession(st, c, s) ==
    [st EXCEPT !.sessions[c] = s, !.expiry[c] = IF s = KVNil THEN 0 ELSE s.endTm]
KVGarbageCollect(st) ==
    LET expired == {c \in KVClients : st.sessions[c] /= KVNil /\ st.expiry[c] <= st.clock}
    IN [st EXCEPT !.sessions = [c \in KVClients |-> IF c \in expired THEN KVNil ELSE st.sessions[c]],
                  !.expiry = [c \in KVClients |-> IF c \in expired THEN 0 ELSE st.expiry[c]]]

\* applyNewSess/updateSessTableByClientName keep the newest per client,
\* breaking ties on SessionID. Fresh IDs are chosen independently of time.
KVNewSession(st, e) ==
    LET prior == st.sessions[e.cl]
        s == [id |-> e.sid, t0 |-> e.tm, endTm |-> e.tm + KVSessDur,
              repEnd |-> e.tm + KVSessDur, maxApplied |-> 0, cache |-> KVEmptyCache]
    IN IF prior = KVNil THEN KVSetSession(st, e.cl, s)
       ELSE IF prior.t0 < e.tm \/ (prior.t0 = e.tm /\ prior.id < e.sid)
            THEN KVSetSession(st, e.cl, s) ELSE st

\* commitWhatWeCan (tube/tube.go) advances the replicated HLC before all
\* branches. Duplicate returns skip execution, refresh and GC. A missing
\* session skips execution but still runs GC. The cache stores failures too.
KVApplyStep(before, e, i) ==
    LET st == [before EXCEPT !.clock = Max(@, e.hlc)]
        s == KVSession(st, e.cl, e.sid)
        done(state, result, executed) == [state |-> state, res |-> result, ran |-> executed]
        control(state, code) == done(KVGarbageCollect(state), KVResponse(code, KVNil), FALSE)
    IN IF e.op = "noop" THEN control(st, "noop")
       ELSE IF e.op = "sessnew" THEN control(KVNewSession(st, e), "created")
       ELSE IF e.op = "sessend"
            THEN control(IF s = KVNil THEN st ELSE KVSetSession(st, e.cl, KVNil), "closed")
       ELSE IF e.sid /= 0 /\ s = KVNil THEN control(st, "unknownSession")
       ELSE IF s /= KVNil /\ e.serial \in DOMAIN s.cache THEN done(st, s.cache[e.serial].res, FALSE)
       ELSE IF s /= KVNil /\ e.op \notin KVReads /\ e.serial <= s.maxApplied
            THEN done(st, KVResponse("acknowledged", KVNil), FALSE)
       ELSE LET result == KVExecute(st, e, i)
                cache == IF s = KVNil THEN KVEmptyCache ELSE KVCachePut(s.cache, e.serial, [res |-> result.res, at |-> i])
                refreshed == IF s = KVNil THEN result.state
                             ELSE KVSetSession(result.state, e.cl,
                                  [s EXCEPT !.endTm = e.tm + KVSessDur, !.repEnd = e.tm + KVSessDur,
                                            !.cache = IF e.serial > s.maxApplied THEN KVTrim(cache, e.ack) ELSE cache,
                                            !.maxApplied = Max(@, e.serial)])
            IN done(KVGarbageCollect(refreshed), result.res, TRUE)

KVApply(n) ==
    /\ kvApplied[n] < Len(kvMem[n])
    /\ LET i == kvApplied[n] + 1
           e == kvMem[n][i]
           r == KVApplyStep(kvState[n], e, i)
           k == KVKey(e)
           mutation == r.ran /\ e.sid /= 0 /\ kvState[n].tables /= r.state.tables
           tracked == e.sid /= 0 /\ e.op \in KVWrites
           answer == [k |-> k, res |-> r.res]
           \* The logged result copy is cached before sending. The remote
           \* reply path may then strip that very copy's payload on the leader.
           cacheChanged == n = kvLeader /\ (e.op = "largeEnvelopeScan" \/ (e.op = "bigScan" /\ r.res.code = "ok")) /\ r.ran
                           /\ KVSession(r.state,e.cl,e.sid) /= KVNil
           after == IF cacheChanged THEN
                      [r.state EXCEPT !.sessions[e.cl].cache[e.serial].res = KVResponse("resultTooLarge",KVNil)]
                    ELSE r.state
       IN /\ kvState' = [kvState EXCEPT ![n] = after]
          /\ kvReceived' = IF n = kvLeader /\ e.sid /= 0 /\ e.op /= "sessend" /\ KVWire(e.op,r.res) /= KVNil
                           THEN kvReceived \cup {[key |-> k, at |-> i]} ELSE kvReceived
          /\ kvLast' = IF n = kvLeader /\ e.op /= "noop" THEN KVDeliver(e.cl, e.op, r.res) ELSE kvLast
          /\ kvApplied' = [kvApplied EXCEPT ![n] = i]
          /\ kvMuts' = IF mutation THEN [kvMuts EXCEPT ![n] = @ \cup {k}] ELSE kvMuts
          /\ kvDup' = kvDup \/ (mutation /\ k \in kvMuts[n])
          /\ kvFirst' = IF tracked /\ r.ran THEN kvFirst \cup {answer} ELSE kvFirst
          /\ kvReply' = IF n = kvLeader /\ tracked /\ r.res.code \notin {"unknownSession", "acknowledged"}
                        THEN kvReply \cup {answer} ELSE kvReply
          /\ kvBelieves' = IF n = kvLeader /\ e.op \in {"lwrite"} \cup KVCAS /\ r.res.code = "ok"
                           THEN [kvBelieves EXCEPT ![e.cl] = r.res.value.until] ELSE kvBelieves
    /\ UNCHANGED <<kvNow, kvLeader, kvMem, kvSerial, kvIssued>>

\* replicateTicket stalls submissions until the term's initial no-op has
\* committed and applied. A new leader's no-op also advances the GC clock.
KVLeaderReady ==
    ~\E i \in (kvApplied[kvLeader]+1)..Len(kvMem[kvLeader]) : kvMem[kvLeader][i].op = "noop"
KVAppend(e) ==
    /\ KVLeaderReady
    /\ Len(kvMem[kvLeader]) < KVMaxLog
    /\ kvMem' = [kvMem EXCEPT ![kvLeader] = Append(@, e)]

KVCreateSession(c) ==
    \E sid \in KVSessionIds, hlc \in KVHLCs :
        /\ ~\E i \in DOMAIN kvMem[kvLeader] :
               LET e == kvMem[kvLeader][i] IN e.op = "sessnew" /\ e.sid = sid /\ e.cl = c
        /\ KVAppend(KVEntry("sessnew", c, sid, 0, 0, 0, hlc))
        /\ UNCHANGED <<kvLast,kvIssued,kvReceived>>
        /\ UNCHANGED <<kvNow, kvLeader, kvApplied, kvState, kvSerial, kvBelieves,
                       kvMuts, kvDup, kvFirst, kvReply>>

KVEndSession(c) ==
    LET s == kvSess[kvLeader][c]
    IN /\ s /= KVNil
       /\ \E hlc \in KVHLCs : KVAppend(KVEntry("sessend", c, s.id, 0, 0, 0, hlc))
       /\ UNCHANGED <<kvLast,kvIssued,kvReceived>>
       /\ UNCHANGED <<kvNow, kvLeader, kvApplied, kvState, kvSerial, kvBelieves,
                      kvMuts, kvDup, kvFirst, kvReply>>

\* MinSessSerialWaiting belongs to the client. Older replies can stay
\* outstanding after the server sends them. Only received replies can be acked.
KVAckFloors(c, sid, serial) ==
    {f \in 0..serial : \A i \in 1..(f-1) : \E a \in kvReceived : a.key = <<c,sid,i>>}
KVKnownIndexes(c,sid) ==
    IF sid = 0 THEN {0}
    ELSE LET heard == {a \in kvReceived : a.key[1] = c /\ a.key[2] = sid}
             born == {i \in DOMAIN kvMem[kvLeader] :
                         LET e == kvMem[kvLeader][i] IN e.op = "sessnew" /\ e.cl = c /\ e.sid = sid}
         IN born \cup {a.at : a \in heard}
KVStamp(c, sid, serial, op, oldE) ==
    {[KVEntry(op, c, sid, serial, oldE, f, h) EXCEPT !.autoDel = a, !.known = k, !.oldVal = v] :
       f \in KVAckFloors(c,sid,serial), h \in KVHLCs, k \in KVKnownIndexes(c,sid),
       v \in IF op = "casValue" THEN KVClients ELSE {c},
       a \in IF op \in {"lwrite"} \cup KVCAS THEN BOOLEAN ELSE {FALSE}}

\* leaderDoneEarlyOnSessionStuff checks the local clock and returns an
\* error for expired sessions. Removal waits for apply. Admission and
\* local reads may evict acknowledged cache entries only on this leader.
KVSubmitEntry(e) ==
    LET l == kvLeader
        s == KVSession(kvState[l], e.cl, e.sid)
        behind == e.known > kvApplied[l]
        valid == IF s = KVNil THEN FALSE ELSE ~behind /\ KVClock(l) < s.endTm
        cached == IF valid THEN e.serial \in DOMAIN s.cache ELSE FALSE
        stale == IF valid THEN e.op \notin KVReads /\ e.serial <= s.maxApplied ELSE FALSE
    IN /\ IF e.sid = 0 \/ (valid /\ ~cached /\ ~stale) THEN KVAppend(e) ELSE UNCHANGED kvMem
       /\ kvState' = IF valid
                     THEN [kvState EXCEPT ![l].sessions[e.cl].cache = KVTrim(@, e.ack)]
                     ELSE kvState
       /\ kvLast' = IF e.sid = 0 THEN kvLast
                    ELSE IF s = KVNil THEN KVDeliver(e.cl,e.op,KVResponse("unknownSession",KVNil))
                    ELSE IF behind THEN KVDeliver(e.cl,e.op,KVResponse("behindClient",KVNil))
                    ELSE IF ~valid THEN KVDeliver(e.cl,e.op,KVResponse("sessionExpired",KVNil))
                    ELSE IF cached THEN KVDeliver(e.cl,e.op,s.cache[e.serial].res)
                    ELSE IF stale THEN KVDeliver(e.cl,e.op,KVResponse("acknowledged",KVNil))
                    ELSE kvLast
       /\ kvReceived' = IF e.sid /= 0 /\ (~valid \/ cached \/ stale) /\ e.op /= "largeEnvelopeScan"
                          THEN kvReceived \cup {[key |-> KVKey(e), at |-> IF cached THEN s.cache[e.serial].at ELSE 0]} ELSE kvReceived
       /\ kvReply' = IF cached /\ e.op \in KVWrites
                     THEN kvReply \cup {[k |-> KVKey(e), res |-> s.cache[e.serial].res]} ELSE kvReply

KVSubmit(c, op, oldE) ==
    LET s == kvSess[kvLeader][c]
        sids == IF s = KVNil THEN {0} ELSE {0, s.id}
    IN \E sid \in sids :
       LET serial == kvSerial[c][sid] IN
       /\ serial <= KVMaxSerial
       /\ \E e \in KVStamp(c,sid,serial,op,oldE) :
               KVSubmitEntry(e) /\ kvIssued' = [kvIssued EXCEPT ![c] = e]
       /\ kvSerial' = [kvSerial EXCEPT ![c][sid] = @ + 1]
       /\ UNCHANGED <<kvNow, kvLeader, kvApplied, kvBelieves, kvMuts, kvDup, kvFirst>>

\* Retries either race apply (a second immutable command enters the log)
\* or find its cached answer at admission. Only session commands dedup.
KVRetrySameSerial(c) ==
    LET logged == {kvMem[kvLeader][i] : i \in DOMAIN kvMem[kvLeader]}
        issued == IF kvIssued[c] = KVNil THEN {} ELSE {kvIssued[c]}
    IN \E e \in logged \cup issued :
        /\ e.cl = c /\ e.sid /= 0 /\ e.op \notin KVSessionOps
        /\ \E copy \in KVStamp(c,e.sid,e.serial,e.op,e.oldE) :
               copy.autoDel = e.autoDel /\ copy.oldVal = e.oldVal /\ KVSubmitEntry(copy)
        /\ UNCHANGED <<kvNow, kvLeader, kvApplied, kvSerial, kvBelieves, kvMuts, kvDup, kvFirst, kvIssued>>

\* CR-12 and CR-D-78. Invalid client-side and admission requests return
\* before append. Legacy stored tickets can still fail independently at apply.
KVRejectInput(c, reason) ==
    /\ UNCHANGED <<kvIssued,kvReceived>>
    /\ reason \in {"keyTooLarge", "contextCancelled", "sessionCancelled"}
    /\ kvLast' = [kvLast EXCEPT ![c] = KVResponse(reason,KVNil)]
    /\ UNCHANGED <<kvNow, kvLeader, kvMem, kvApplied, kvState, kvSerial, kvBelieves,
                   kvMuts, kvDup, kvFirst, kvReply>>

\* Load a pre-upgrade committed prefix before current-format entries.
\* No current writer emits these tickets. A valid older ticket has no
\* drift field. Older oversized keys reach PutLeaf and fail their ticket.
KVLoadLegacy(c, op) ==
    /\ UNCHANGED <<kvIssued,kvReceived>>
    /\ \A n \in KVReplicas : kvApplied[n] = 0
    /\ op \in {"lwrite", "cas", "legacyInvalidKey"}
    /\ \A i \in DOMAIN kvMem[kvLeader] : kvMem[kvLeader][i].drift = KVNil
    /\ \E hlc \in KVHLCs : KVAppend([KVEntry(op,c,0,0,0,0,hlc) EXCEPT !.drift = KVNil])
    /\ UNCHANGED <<kvNow, kvLeader, kvApplied, kvState, kvSerial, kvBelieves,
                   kvMuts, kvDup, kvFirst, kvReply, kvLast>>

KVShip(n) ==
    /\ n /= kvLeader /\ Len(kvMem[n]) < Len(kvMem[kvLeader])
    /\ kvMem' = [kvMem EXCEPT ![n] = Append(@, kvMem[kvLeader][Len(@) + 1])]
    /\ UNCHANGED <<kvLast,kvIssued,kvReceived>>
    /\ UNCHANGED <<kvNow, kvLeader, kvApplied, kvState, kvSerial, kvBelieves,
                   kvMuts, kvDup, kvFirst, kvReply>>

\* applyNewStateSnapshot replaces the session table and rebuilds the
\* expiry/name indexes. Restart rebuilds from the same restored table.
\* The client-keyed session function represents the name index directly.
KVRestore(st) ==
    LET sessions == [c \in KVClients |-> LET s == st.sessions[c] IN
                        IF s = KVNil THEN KVNil ELSE IF s.endTm = 0 THEN [s EXCEPT !.endTm = s.repEnd] ELSE s]
    IN [st EXCEPT !.sessions = sessions,
                  !.expiry = [c \in KVClients |-> IF sessions[c] = KVNil THEN 0 ELSE sessions[c].endTm]]
KVInstallSnapshot(n) ==
    /\ n /= kvLeader /\ kvApplied[n] < kvApplied[kvLeader]
    /\ kvApplied' = [kvApplied EXCEPT ![n] = kvApplied[kvLeader]]
    /\ kvMem' = [kvMem EXCEPT ![n] = SubSeq(kvMem[kvLeader], 1, Max(Len(@), kvApplied[kvLeader]))]
    /\ kvState' = [kvState EXCEPT ![n] = KVRestore(kvState[kvLeader])]
    /\ kvMuts' = [kvMuts EXCEPT ![n] = kvMuts[kvLeader]]
    /\ UNCHANGED <<kvLast,kvIssued,kvReceived>>
    /\ UNCHANGED <<kvNow, kvLeader, kvSerial, kvBelieves, kvDup, kvFirst, kvReply>>

KVFailover(n) ==
    /\ n /= kvLeader /\ Len(kvMem[n]) = Len(kvMem[kvLeader])
    /\ Len(kvMem[n]) < KVMaxLog
    /\ kvLeader' = n
    /\ \E h \in KVClock(n)..MaxSet({KVClock(r) : r \in KVReplicas}) :
          LET entry == [KVEntry("noop",KVNil,0,0,0,0,h) EXCEPT !.tm = KVClock(n), !.drift = KVDrift[n]]
          IN kvMem' = [kvMem EXCEPT ![n] = Append(@,entry)]
    /\ UNCHANGED <<kvLast,kvIssued,kvReceived>>
    /\ UNCHANGED <<kvNow, kvApplied, kvState, kvSerial, kvBelieves,
                   kvMuts, kvDup, kvFirst, kvReply>>

\* leaderServedLocalRead requires a valid Raft read lease (abstracted
\* by allowing a local or replicated request). Near session expiry it
\* falls back to the replicated path. The dry-write probe uses local now
\* and local drift; only a failed probe can respond without replication.
KVLocal(c, op) ==
    LET l == kvLeader
        st == kvState[l]
        s == st.sessions[c]
        sids == IF s = KVNil THEN {0} ELSE {0, s.id}
    IN \E sid \in sids :
       LET serial == kvSerial[c][sid]
           e == KVEntry(op,c,sid,serial,0,0,KVClock(l))
           probe == KVExecute(st,e,Len(kvMem[l])+1)
       IN /\ KVLeaderReady
          /\ serial <= KVMaxSerial
          /\ IF sid = 0 THEN TRUE ELSE KVClock(l) < s.repEnd - (KVSessDur \div 2)
          /\ op \in KVReads \/ probe.res.code = "rejected"
          /\ kvReceived' = IF sid /= 0 /\ KVWire(op,probe.res) /= KVNil
                           THEN kvReceived \cup {[key |-> KVKey(e), at |-> kvApplied[l]]} ELSE kvReceived
          /\ kvLast' = KVDeliver(c,op,probe.res)
          /\ kvIssued' = [kvIssued EXCEPT ![c] = e]
          /\ kvState' = IF sid = 0 THEN kvState
                        ELSE [kvState EXCEPT ![l].sessions[c].cache = KVCachePut(@,serial,[res |-> probe.res, at |-> kvApplied[l]])]
          /\ kvSerial' = [kvSerial EXCEPT ![c][sid] = @ + 1]
          /\ UNCHANGED <<kvNow, kvLeader, kvMem, kvApplied, kvBelieves, kvMuts, kvDup, kvFirst, kvReply>>
KVLeaderLocalRead(c) == \E op \in KVReads \cup {"lwrite"} : KVLocal(c,op)

KVTick ==
    /\ kvNow < KVMaxTime /\ kvNow' = kvNow + 1
    /\ UNCHANGED <<kvLast,kvIssued,kvReceived>>
    /\ UNCHANGED <<kvLeader, kvMem, kvApplied, kvState, kvSerial, kvBelieves, kvMuts, kvDup, kvFirst, kvReply>>

KV_Init ==
    /\ kvNow = 0 /\ kvLeader = CHOOSE n \in KVReplicas : TRUE
    /\ kvMem = [n \in KVReplicas |-> <<>>] /\ kvApplied = [n \in KVReplicas |-> 0]
    /\ kvState = [n \in KVReplicas |-> KVInitialState]
    /\ kvSerial = [c \in KVClients |-> [sid \in 0..KVMaxLog |-> 1]]
    /\ kvBelieves = [c \in KVClients |-> 0]
    /\ kvMuts = [n \in KVReplicas |-> {}] /\ kvDup = FALSE
    /\ kvFirst = {} /\ kvReply = {}
    /\ kvLast = [c \in KVClients |-> KVNil] /\ kvIssued = kvLast /\ kvReceived = {}

KVClientStep(c) ==
    \/ KVCreateSession(c) \/ KVEndSession(c)
    \/ \E reason \in {"keyTooLarge", "contextCancelled", "sessionCancelled"} : KVRejectInput(c,reason)
    \/ \E op \in {"lwrite","cas","legacyInvalidKey"} : KVLoadLegacy(c,op)
    \/ \E op \in KVReads \cup (KVWrites \ KVCAS) : KVSubmit(c, op, 0)
    \/ KVSubmit(c, "casValue", 0)
    \/ \E op \in {"cas","casVersion"}, oldE \in 0..2 : KVSubmit(c, op, oldE)
KV_Next ==
    \/ KVTick
    \/ \E n \in KVReplicas : KVApply(n) \/ KVShip(n) \/ KVInstallSnapshot(n) \/ KVFailover(n)
    \/ \E c \in KVClients : KVClientStep(c) \/ KVRetrySameSerial(c) \/ KVLeaderLocalRead(c)

KVLogAgreement ==
    \A n \in KVReplicas : IsPrefix(kvMem[n], kvMem[kvLeader])
KVLogImmutable ==
    [][\A n \in KVReplicas : IsPrefix(kvMem[n], kvMem'[n])]_kvVars
KVDeterminism ==
    \A n,m \in KVReplicas : kvApplied[n] = kvApplied[m] => kvState[n].tables = kvState[m].tables
\* Compare session identity, deadline and maximum applied serial at the same
\* applied index. The local-read retry profile exercises the cache-hit path.
KVSessionSummary(st) ==
    [c \in KVClients |-> LET s == st.sessions[c] IN
        IF s = KVNil THEN KVNil ELSE <<s.id,s.t0,s.endTm,s.repEnd,s.maxApplied>>]
KVSessDeterminism ==
    \A n,m \in KVReplicas : kvApplied[n] = kvApplied[m] =>
        KVSessionSummary(kvState[n]) = KVSessionSummary(kvState[m])
KVSessionIndexes ==
    \A n \in KVReplicas : kvState[n].expiry = KVRestore(kvState[n]).expiry
KVExactlyOnce == ~kvDup
KVReplyMatchesApply == \A a \in kvReply : \A b \in kvFirst : a.k = b.k => a.res = b.res
KVLeaseMutex ==
    ~\E c,d \in KVClients : c /= d /\ kvBelieves[c] > kvNow /\ kvBelieves[d] > kvNow
KVTypeOK ==
    /\ kvNow \in 0..KVMaxTime /\ kvLeader \in KVReplicas
    /\ \A n \in KVReplicas : kvApplied[n] \in 0..Len(kvMem[n])
    /\ \A n \in KVReplicas : DOMAIN kvState[n].tables = KVTables

\* ========================================================================
\* Czar client and reliable membership (tube/czar.go).
\* Tube is one linearizable CAS register. Member names are stable and a
\* restart changes the PeerID. Absolute deadlines are represented as ticks
\* remaining. The logical clock advances on CzTick, including during RPCs.
CONSTANTS CzMembers, CzLease, CzMinLeft, CzDrift, CzMaxEpoch, CzNil

CzFloor == -(CzDrift + 1)
CzClamp(x) == Max(x, CzFloor)
CzAge(x) == CzClamp(x - 1)
\* Default duration ratios follow NewCzar (5 s czar, 6 s member, 1 s beat).
\* Rounding establishes positive timer durations for small TLC configurations.
CzBeat == Max(1, CzLease \div 5)
CzMemberLease == Max(1, (6 * CzLease) \div 5)
CzPingTimeout == Max(1, Min(2*CzBeat, Min(CzMemberLease, CzLease) \div 2))
CzReportWindow == 2*CzMemberLease
CzEmpty == [n \in {} |-> 0]
CzTableTick(tbl) == [n \in DOMAIN tbl |-> [tbl[n] EXCEPT !.rem = CzAge(@)]]
CzTablePut(tbl,m,p,t) == [n \in DOMAIN tbl \cup {m} |->
                         IF n = m THEN [pid |-> p, rem |-> t] ELSE tbl[n]]
CzWithout(tbl, gone) == [n \in DOMAIN tbl \ gone |-> tbl[n]]

VARIABLES
    czReg,       \* durable czar-key leaf, including the last published member list
    czState,     \* Unknown | AmCzar | NotCzar
    czVers,      \* local version, current czar identity and membership deadlines
    czQ,         \* outstanding CAS tickets, including abandoned tickets
    czWait, czRes, czPid, czUp, czPanics,
    czCtl,       \* per-member timers, observed epoch, direct ping and upcalls
    czReports    \* latest best-effort czar-unreachable/<member> report in Tube

czVars == <<czReg, czState, czVers, czQ, czWait, czRes, czPid, czUp,
            czPanics, czCtl, czReports>>

CzEmptyVers == [E |-> 0, w |-> 0, rem |-> CzFloor, owner |-> CzNil,
                pid |-> 0, members |-> CzEmpty]
CzIdent(v) ==
    LET nodes == DOMAIN v.members \cup (IF v.owner = CzNil THEN {} ELSE {v.owner})
    IN [epoch |-> v.E, version |-> v.w,
        members |-> [n \in nodes |-> IF n = v.owner THEN v.pid ELSE v.members[n].pid]]
CzNoGrant == [rem |-> CzFloor, issuerRem |-> CzFloor, member |-> FALSE]
CzCtl0 == [learned |-> 0, delay |-> 0, phase |-> "idle", timer |-> 0,
           ping |-> CzNil, uptime |-> 0, heard |-> CzEmpty,
           published |-> CzIdent(CzEmptyVers), grant |-> CzNoGrant]
CzPublishedMembers(m) == czCtl[m].published.members
CzPublishedEpoch(m) == czCtl[m].published.epoch
CzPublishedVersion(m) == czCtl[m].published.version
CzOperatingGrant(m) == czCtl[m].grant.rem

\* setVersAt compares epoch and within-epoch version, using the instant already
\* checked by handlePing. Only identity/version changes produce an upcall.
CzAdmissible(old, v) == v.rem >= 0 /\ (v.E > old.E \/ (v.E = old.E /\ v.w >= old.w))
CzAdopt(ctl, old, v, notify) ==
    [ctl EXCEPT !.uptime = IF v.E /= old.E THEN 0 ELSE @,
                !.published = IF notify /\ CzIdent(v) /= CzIdent(old)
                              THEN CzIdent(v) ELSE @]
CzIdle(c, delay) == [c EXCEPT !.phase = "idle", !.timer = 0,
                              !.ping = CzNil, !.delay = Max(0,delay)]
CzVersionTick(v) == [v EXCEPT !.rem = CzAge(@), !.members = CzTableTick(@)]

\* doCAS and kvstoreWrite use the committed timestamp/drift for comparison.
\* OldLeaseEpochCAS=0 supplies no epoch precondition. A renewal requires the
\* same name and PeerID. A different incarnation can take over after expiry.
\* tube/tube.go doCAS and validLease, tube/actions.go kvstoreWrite.
\* Member time is the reference clock. A newly stamping Tube leader can
\* lead it by 0..CzDrift ticks, or lag the previous leader in that range.
CzApplyCAS(op, offset) ==
    LET leaf == czReg
        make(ep) == [leasor |-> op.m, pid |-> op.pid, epoch |-> ep,
                     rem |-> CzLease + offset, members |-> op.members]
        win(ep) == [ok |-> TRUE, err |-> "none", reg |-> make(ep)]
        reject == [ok |-> FALSE, err |-> "rejected", reg |-> leaf]
    IN IF leaf = CzNil
       THEN IF op.prevE = 0 THEN win(1)
            ELSE [ok |-> FALSE, err |-> "notfound", reg |-> CzNil]
       ELSE IF (leaf.rem + CzDrift >= offset /\ leaf.leasor /= op.m)
               \/ (op.prevE > 0 /\ op.prevE /= leaf.epoch) \/ CzLease + offset <= leaf.rem
            THEN reject
            ELSE IF leaf.leasor = op.m /\ leaf.pid = op.pid THEN win(leaf.epoch)
                 ELSE IF leaf.rem + CzDrift < offset THEN win(leaf.epoch+1) ELSE reject

CzFree(m) == czUp[m] /\ ~czWait[m] /\ czRes[m] = CzNil
CzFreshReport(m) == \E n \in CzMembers :
    czReports[n].owner = m /\ czReports[n].left > 0
\* tube/czar.go:1135-1231, 1483-1534. Unknown contention and renewal have distinct error
\* handling. Renewals snapshot the current membership in the CAS value.
CzSubmitWith(m, readReport) ==
    /\ CzFree(m) /\ czCtl[m].phase = "idle" /\ czCtl[m].delay = 0
    /\ czState[m] = "Unknown"
       \/ (czState[m] = "AmCzar" /\ czVers[m].rem >= 0
           /\ ~(readReport /\ CzFreshReport(m)))
    /\ LET ren == czState[m] = "AmCzar"
           v == czVers[m]
           op == [m |-> m, pid |-> czPid[m], renew |-> ren, orphan |-> FALSE,
                  prevE |-> IF ren THEN v.E ELSE Max(v.E,czCtl[m].learned),
                  members |-> CzWithout(v.members, {m,v.owner})]
       IN czQ' = Append(czQ,op)
    /\ czWait' = [czWait EXCEPT ![m] = TRUE]
    /\ UNCHANGED <<czReg,czState,czVers,czRes,czCtl,czReports,czPid,czUp,czPanics>>
CzSubmit(m) == CzSubmitWith(m,TRUE)

CzTubeApply(i, offset) ==
    /\ i \in 1..Len(czQ)
    /\ LET op == czQ[i]
           r == CzApplyCAS(op, offset)
           deliver == ~op.orphan /\ czUp[op.m] /\ czPid[op.m] = op.pid
           e == IF r.reg = CzNil THEN 0 ELSE r.reg.epoch
           rem == IF r.reg = CzNil THEN CzFloor ELSE
                  IF r.ok THEN r.reg.rem ELSE CzClamp(r.reg.rem+CzDrift)
           reply == [ok |-> r.ok, err |-> r.err, renew |-> op.renew, E |-> e,
                     rem |-> rem, owner |-> IF r.reg = CzNil THEN CzNil ELSE r.reg.leasor,
                     pid |-> IF r.reg = CzNil THEN 0 ELSE r.reg.pid,
                     members |-> IF r.reg = CzNil THEN CzEmpty ELSE r.reg.members]
       IN /\ czReg' = r.reg /\ czQ' = RemoveAt(czQ,i)
          /\ czRes' = IF deliver THEN [czRes EXCEPT ![op.m] = reply] ELSE czRes
          /\ czWait' = IF deliver THEN [czWait EXCEPT ![op.m] = FALSE] ELSE czWait
    /\ UNCHANGED <<czState,czVers,czCtl,czReports,czPid,czUp,czPanics>>
CzApplySome == \E i \in 1..Len(czQ), offset \in 0..CzDrift : CzTubeApply(i,offset)
CzOrphan(m) == [i \in 1..Len(czQ) |-> [czQ[i] EXCEPT !.orphan = @ \/ czQ[i].m = m]]
CzRespLost(m) ==
    /\ czUp[m] /\ (czWait[m] \/ czRes[m] /= CzNil)
    /\ czWait' = [czWait EXCEPT ![m] = FALSE] /\ czQ' = CzOrphan(m)
    /\ czRes' = [czRes EXCEPT ![m] = CzNil]
    /\ czState' = [czState EXCEPT ![m] = "Unknown"]
    /\ UNCHANGED <<czReg,czVers,czCtl,czReports,czPid,czUp,czPanics>>

\* RMember.start (czar.go:1236-1409,1534-1558): a rejected contention learns even an expired leaf's epoch;
\* a self-named rejection schedules a retry. A failed timer renewal steps
\* down to contend again. Renewal success separately publishes an operating lease.
CzHandleResult(m) ==
    /\ czUp[m] /\ czRes[m] /= CzNil
    /\ LET r == czRes[m]
           v == czVers[m]
           rejected == ~r.renew /\ r.err = "rejected"
           self == rejected /\ r.owner = m
           wanted == [E |-> r.E, w |-> IF r.E = v.E THEN v.w ELSE 0,
                      rem |-> r.rem, owner |-> r.owner, pid |-> r.pid, members |-> r.members]
           win == r.ok /\ ~r.renew /\ r.rem >= CzMinLeft /\ CzAdmissible(v,wanted)
           other == rejected /\ ~self
           adopt == win \/ (other /\ CzAdmissible(v,wanted))
           renewal == r.renew /\ r.ok
           \* A same-name different PeerID waits up to a second of the old lease;
           \* an own rejection retries after the short (one tick) backoff.
           delay == IF self THEN IF r.pid /= czPid[m] THEN Max(1,Min(CzBeat,r.rem)) ELSE 1
                    ELSE IF win \/ renewal THEN CzBeat ELSE 0
           c == CzIdle(czCtl[m],delay)
           c1 == [c EXCEPT !.learned = IF rejected THEN Max(@,r.E) ELSE @,
                            !.grant = IF renewal
                                      THEN [rem |-> r.rem,issuerRem |-> r.rem,member |-> FALSE]
                                      ELSE @]
       IN /\ czState' = [czState EXCEPT ![m] = IF win \/ renewal THEN "AmCzar"
                                               ELSE IF other THEN "NotCzar" ELSE "Unknown"]
          /\ czVers' = [czVers EXCEPT ![m] = IF adopt THEN wanted
                       ELSE IF renewal THEN [v EXCEPT !.E = r.E, !.rem = r.rem] ELSE v]
          /\ czCtl' = [czCtl EXCEPT ![m] = IF adopt THEN CzAdopt(c1,v,wanted,win) ELSE c1]
    /\ czRes' = [czRes EXCEPT ![m] = CzNil]
    /\ UNCHANGED <<czReg,czQ,czWait,czPid,czUp,czReports,czPanics>>

\* resolveLeader chooses the highest reported term (tube/helper.go:26-56).
\* During leader discovery, an unavailable leader or a conflicting inspection
\* result retries after a positive delay. Successful discovery is folded into
\* the next CAS. The abstract register already represents the selected leader.
CzLookupFailure(m,d) ==
    /\ CzFree(m) /\ czState[m] = "Unknown" /\ czCtl[m].delay = 0
    /\ czCtl' = [czCtl EXCEPT ![m] = CzIdle(@,d)]
    /\ UNCHANGED <<czReg,czVers,czState,czQ,czWait,czRes,czPid,czUp,czReports,czPanics>>

\* membersCannotReachUs and the renewal timer (czar.go:313-357,1483-1496).
CzDepose(m) ==
    /\ CzFree(m) /\ czState[m] = "AmCzar" /\ czCtl[m].delay = 0
    /\ czVers[m].rem >= 0 /\ CzFreshReport(m)
    /\ czCtl' = [czCtl EXCEPT ![m] = CzIdle(@,czVers[m].rem + CzDrift + CzBeat)]
    /\ czState' = [czState EXCEPT ![m] = "Unknown"]
    /\ UNCHANGED <<czReg,czVers,czQ,czWait,czRes,czPid,czUp,czReports,czPanics>>
CzExpireCheck(m) ==
    /\ CzFree(m) /\ czState[m] = "AmCzar" /\ czVers[m].rem < 0
    /\ czState' = [czState EXCEPT ![m] = "Unknown"]
    /\ czCtl' = [czCtl EXCEPT ![m] = CzIdle(@,0)]
    /\ UNCHANGED <<czReg,czVers,czQ,czWait,czRes,czPid,czUp,czReports,czPanics>>

\* setVersAt and expireSilentNodes (czar.go:365-416,454-510).
\* The per-name membership table omits the czar. Heard times and uptime
\* saturate one tick beyond the expiry test. An inherited entry gets the
\* new epoch's grace period, and a heard member gets its own grace period.
CzPrune(list,heard,uptime) ==
    LET gone == {n \in DOMAIN list :
                     /\ list[n].rem + CzDrift < 0
                     /\ (IF n \in DOMAIN heard THEN heard[n] ELSE uptime) > CzMemberLease}
    IN CzWithout(list,gone)
CzChangedList(v,t) ==
    [v EXCEPT !.members = t,
      !.w = IF CzIdent([v EXCEPT !.members = t]).members /= CzIdent(v).members THEN @+1 ELSE @]
CzExpireSilent(m) ==
    /\ CzFree(m) /\ czState[m] = "AmCzar" /\ czVers[m].rem >= 0
    /\ LET v == czVers[m]
           kept == CzPrune(v.members,czCtl[m].heard,czCtl[m].uptime)
           nv == CzChangedList(v,kept)
       IN /\ kept /= v.members /\ czVers' = [czVers EXCEPT ![m] = nv]
          /\ czCtl' = [czCtl EXCEPT ![m] =
                     [CzAdopt(@,v,nv,TRUE) EXCEPT !.heard = CzWithout(@,DOMAIN v.members \ DOMAIN kept)]]
    /\ UNCHANGED <<czReg,czState,czQ,czWait,czRes,czPid,czUp,czReports,czPanics>>

\* pingerConfig, handlePing and adoptPingReply (czar.go:257-305,631-758,
\* 1603-1669,1698-1797). First dial and first Ping have distinct deadlines. A heartbeat after a
\* successful first reply uses heartbeatPingTimeout. Time may elapse between
\* handlePing's acceptance and the member's adoption of its reply.
CzStartContact(m) ==
    /\ CzFree(m) /\ czState[m] = "NotCzar" /\ czCtl[m].delay = 0
    /\ czCtl[m].phase \in {"idle","connected"}
    /\ LET first == czCtl[m].phase = "idle"
           missing == first /\ czVers[m].owner = CzNil
           same == first /\ czVers[m].owner = m
       IN /\ czCtl' = [czCtl EXCEPT ![m] =
                     IF missing \/ same THEN CzIdle(@,0)
                     ELSE [@ EXCEPT !.phase = IF first THEN "dial" ELSE "heartbeat",
                                    !.timer = CzPingTimeout]]
          /\ czState' = IF same THEN [czState EXCEPT ![m] = "Unknown"] ELSE czState
          /\ czPanics' = IF missing THEN czPanics \cup {"MissingCzarDetail"} ELSE czPanics
          /\ czUp' = IF missing THEN [czUp EXCEPT ![m] = FALSE] ELSE czUp
    /\ UNCHANGED <<czReg,czVers,czQ,czWait,czRes,czPid,czReports>>

CzDialReady(m) ==
    /\ CzFree(m) /\ czState[m] = "NotCzar" /\ czCtl[m].phase = "dial"
    /\ czCtl[m].timer > 0 /\ czUp[czVers[m].owner]
    /\ czCtl' = [czCtl EXCEPT ![m].phase = "first", ![m].timer = CzLease]
    /\ UNCHANGED <<czReg,czState,czVers,czQ,czWait,czRes,czPid,czUp,czReports,czPanics>>

CzPingServe(m) ==
    /\ CzFree(m) /\ czState[m] = "NotCzar"
    /\ czCtl[m].phase \in {"first","heartbeat"} /\ czCtl[m].timer > 0 /\ czCtl[m].ping = CzNil
    /\ LET h == czVers[m].owner
           v == czVers[h]
           granted == Min(CzMemberLease,v.rem-CzDrift)
           list == CzTablePut(v.members,m,czPid[m],CzClamp(granted))
           heard == [n \in DOMAIN czCtl[h].heard \cup {m,h} |->
                     IF n \in {m,h} THEN 0 ELSE czCtl[h].heard[n]]
           kept == CzPrune(list,heard,czCtl[h].uptime)
           nv == CzChangedList(v,kept)
           c == CzAdopt(czCtl[h],v,nv,TRUE)
       IN /\ CzFree(h) /\ czState[h] = "AmCzar" /\ v.rem >= 0
          /\ czVers' = [czVers EXCEPT ![h] = nv]
          /\ czCtl' = [czCtl EXCEPT ![h] =
                        [c EXCEPT !.heard = CzWithout(heard,DOMAIN list \ DOMAIN kept)],
                        ![m].ping = nv]
    /\ UNCHANGED <<czReg,czState,czQ,czWait,czRes,czPid,czUp,czReports,czPanics>>

CzPingReceive(m) ==
    /\ CzFree(m) /\ czCtl[m].ping /= CzNil /\ czCtl[m].timer > 0
    /\ LET r == czCtl[m].ping
           v == czVers[m]
           beat == czCtl[m].phase = "heartbeat"
           usable == ~beat \/ (m \in DOMAIN r.members /\ r.rem > CzDrift)
           ok == usable /\ CzAdmissible(v,r)
           c == CzIdle(czCtl[m],IF ok THEN CzBeat ELSE 0)
           \* Heartbeat sends OperatingLeaseRenewCh before adoptPingReply.
           c1 == [c EXCEPT !.grant = IF beat /\ usable
                     THEN [rem |-> r.members[m].rem,issuerRem |-> r.rem,member |-> TRUE]
                     ELSE @]
       IN /\ czState' = [czState EXCEPT ![m] = IF ok THEN "NotCzar" ELSE "Unknown"]
          /\ czVers' = IF ok THEN [czVers EXCEPT ![m] = r] ELSE czVers
          /\ czCtl' = [czCtl EXCEPT ![m] = IF ok THEN
                 [CzAdopt(c1,v,r,TRUE) EXCEPT !.phase = "connected"] ELSE c1]
    /\ UNCHANGED <<czReg,czQ,czWait,czRes,czPid,czUp,czReports,czPanics>>

\* On dial failure wait out the known czar lease. First/heartbeat Ping
\* failure can publish a report if Tube is reachable; the write is best effort.
CzContactFailed(m,publish) ==
    /\ CzFree(m) /\ czState[m] = "NotCzar"
    /\ czCtl[m].phase \in {"dial","first","heartbeat","connected"}
    /\ (publish => czCtl[m].phase \in {"first","heartbeat"})
    /\ LET p == czCtl[m].phase
           delay == IF p = "dial" THEN Max(0,czVers[m].rem) ELSE IF p = "first" THEN 1 ELSE 0
       IN czCtl' = [czCtl EXCEPT ![m] = CzIdle(@,delay)]
    /\ czState' = [czState EXCEPT ![m] = "Unknown"]
    /\ czReports' = IF publish THEN [czReports EXCEPT
           ![m] = [owner |-> czVers[m].owner,left |-> CzReportWindow]] ELSE czReports
    /\ UNCHANGED <<czReg,czVers,czQ,czWait,czRes,czPid,czUp,czPanics>>

CzClientStep(m) ==
    \/ CzSubmit(m) \/ CzHandleResult(m) \/ CzDepose(m) \/ CzExpireCheck(m)
    \/ CzExpireSilent(m) \/ CzStartContact(m) \/ CzDialReady(m) \/ CzPingServe(m)
    \/ CzPingReceive(m)
    \/ czCtl[m].phase \in {"dial","first","heartbeat"} /\ czCtl[m].timer = 0
       /\ \E report \in BOOLEAN : CzContactFailed(m,report)
CzClientFault(m) ==
    \/ CzRespLost(m) \/ \E d \in 1..CzLease : CzLookupFailure(m,d)
    \/ \E report \in BOOLEAN : CzContactFailed(m,report)
    \* A failed ReadPrefixRange may miss otherwise fresh reports.
    \/ CzFreshReport(m) /\ czState[m] = "AmCzar" /\ CzSubmitWith(m,FALSE)

CzTick ==
    /\ czReg' = IF czReg = CzNil THEN CzNil ELSE
                    [czReg EXCEPT !.rem = CzAge(@), !.members = CzTableTick(@)]
    /\ czVers' = [m \in CzMembers |-> CzVersionTick(czVers[m])]
    /\ czRes' = [m \in CzMembers |-> IF czRes[m] = CzNil THEN CzNil ELSE
                        [czRes[m] EXCEPT !.rem = CzAge(@), !.members = CzTableTick(@)]]
    /\ czQ' = [i \in 1..Len(czQ) |-> [czQ[i] EXCEPT !.members = CzTableTick(@)]]
    /\ czReports' = [m \in CzMembers |-> [czReports[m] EXCEPT !.left = Max(0,@-1)]]
    /\ czCtl' = [m \in CzMembers |-> [czCtl[m] EXCEPT
           !.delay = Max(0,@-1), !.timer = Max(0,@-1),
           !.uptime = Min(CzMemberLease+1,@+1),
           !.heard = [n \in DOMAIN @ |-> Min(CzMemberLease+1,@[n]+1)],
           !.grant.rem = CzAge(@), !.grant.issuerRem = CzAge(@),
           !.ping = IF @ = CzNil THEN CzNil ELSE CzVersionTick(@)]]
    /\ UNCHANGED <<czState,czWait,czPid,czUp,czPanics>>

CzCrash(m) ==
    /\ czUp[m] /\ czUp' = [czUp EXCEPT ![m] = FALSE]
    /\ czWait' = [czWait EXCEPT ![m] = FALSE] /\ czRes' = [czRes EXCEPT ![m] = CzNil]
    /\ czQ' = CzOrphan(m)
    /\ UNCHANGED <<czReg,czState,czVers,czCtl,czReports,czPid,czPanics>>
CzRestart(m) ==
    /\ ~czUp[m] /\ czUp' = [czUp EXCEPT ![m] = TRUE]
    /\ czPid' = [czPid EXCEPT ![m] = @ + 1]
    /\ czState' = [czState EXCEPT ![m] = "Unknown"]
    /\ czVers' = [czVers EXCEPT ![m] = CzEmptyVers]
    /\ czCtl' = [czCtl EXCEPT ![m] = CzCtl0]
    /\ UNCHANGED <<czReg,czQ,czWait,czRes,czReports,czPanics>>

Cz_Init ==
    /\ czReg = CzNil /\ czQ = <<>>
    /\ czState = [m \in CzMembers |-> "Unknown"]
    /\ czVers = [m \in CzMembers |-> CzEmptyVers]
    /\ czCtl = [m \in CzMembers |-> CzCtl0]
    /\ czWait = [m \in CzMembers |-> FALSE] /\ czRes = [m \in CzMembers |-> CzNil]
    /\ czPid = [m \in CzMembers |-> 0] /\ czUp = [m \in CzMembers |-> TRUE]
    /\ czPanics = {} /\ czReports = [m \in CzMembers |-> [owner |-> CzNil,left |-> 0]]
Cz_Next ==
    \/ CzTick \/ CzApplySome
    \/ \E m \in CzMembers : CzClientStep(m) \/ CzClientFault(m) \/ CzCrash(m) \/ CzRestart(m)

CzAtMostOneCzarBelief ==
    Cardinality({m \in CzMembers : czUp[m] /\ czState[m] = "AmCzar" /\ czVers[m].rem >= 0}) <= 1
CzMemberLeaseBound ==
    \A m \in CzMembers :
        LET g == czCtl[m].grant IN g.member /\ g.rem > 0 => g.rem <= g.issuerRem - CzDrift
\* Pending replies carry the bound even while the member is waiting
\* to consume its direct RPC reply.
CzPingGrantBound ==
    \A m \in CzMembers : LET r == czCtl[m].ping IN r /= CzNil =>
        /\ m \in DOMAIN r.members
        /\ r.members[m].rem <= CzClamp(r.rem-CzDrift)
CzSubmittedEpoch ==
    \A i \in 1..Len(czQ) :
        LET o == czQ[i] IN ~o.orphan /\ ~o.renew => o.prevE >= czCtl[o.m].learned
CzNoPanic == czPanics = {}
CzSomeCzar == \E m \in CzMembers : czUp[m] /\ czState[m] = "AmCzar" /\ czVers[m].rem >= 0
CzTypeOK ==
    /\ czState \in [CzMembers -> {"Unknown","AmCzar","NotCzar"}]
    /\ czUp \in [CzMembers -> BOOLEAN] /\ czWait \in [CzMembers -> BOOLEAN]
    /\ \A m \in CzMembers :
        /\ czCtl[m].learned \in Nat
        /\ czCtl[m].phase \in {"idle","dial","first","connected","heartbeat"}
        /\ DOMAIN CzPublishedMembers(m) \subseteq CzMembers
        /\ czVers[m].owner \in CzMembers \cup {CzNil}
        /\ DOMAIN czVers[m].members \subseteq CzMembers
        /\ czVers[m].w \in Nat
        /\ (czWait[m] <=> \E i \in 1..Len(czQ) : czQ[i].m=m /\ ~czQ[i].orphan)

\* ========================================================================
\* Hermes reliable membership, operating lease and message-loss recovery
\* ========================================================================
\* One client update of an existing key, with a waiting read on each
\* interested replica. Each run chooses a WRITE or RMW. The value is a
\* version token (WRITE replaces 0 with 7, RMW adds 1). This projection
\* selects nonconflicting operations and the default, unicast-ACK mode.
\* After a restart the volatile store is empty. Reads of a missing key use
\* the caller's immediate not-found option. A subsequent INV installs it.
\* Replicas that have a current value can buffer a read pending validation.
\* Names identify processes and endpoint IDs distinguish the Tube service
\* from the Hermes service across incarnations. A service ID also stands
\* for its advertised URL. HermesAppMembers can be overridden to exercise
\* appIdentity's Tube fallback for members without an advertised app peer.
HermesAppMembers == CzMembers
\* RunFromDiskConfig sets the configured replication degree independently
\* of the number of members in the latest CZAR membership.
HermesReplicationDegree == 3
\* RunFromDiskConfig sets MessageLossTimeout to three seconds against the
\* five-second czar lease. Round to a positive number of model ticks.
HermesLossTicks == Max(1, (3*CzLease) \div 5)
HermesID(m, p, app) == <<m, p, IF app THEN "hermes" ELSE "tube">>
HermesSelf(m) == HermesID(m, czPid[m], TRUE)
HermesLive(details, apps) ==
    {HermesID(m, details[m], m \in apps) : m \in DOMAIN details}
HermesPair(x, y) == [src |-> x, dst |-> y]
HermesEnd(x, y, g) == [src |-> x, dst |-> y, cid |-> g]
HermesReverse(e) == HermesEnd(e.dst, e.src, e.cid)
HermesSlot(reg, x, y) == {e \in reg : e.src = x /\ e.dst = y}

VARIABLE hermes
HermesLocal0 == [phase |-> "valid", ver |-> 0, value |-> 0,
                  issued |-> "none", timer |-> -1, waiting |-> FALSE, read |-> -1, acks |-> {}]
HermesView0 == [epoch |-> 0, vers |-> 0, live |-> {}]
HermesInitial(established) ==
    LET ids == [m \in CzMembers |-> 0]
        live == HermesLive(ids,HermesAppMembers)
        mesh == {HermesEnd(HermesID(p[1], 0, TRUE), HermesID(p[2], 0, TRUE), 1) :
                 p \in {p \in CzMembers \X CzMembers : p[1] /= p[2]}}
    IN [key |-> [m \in CzMembers |-> HermesLocal0],
        view |-> [m \in CzMembers |-> IF established
                       THEN [epoch |-> 1, vers |-> 0, live |-> live] ELSE HermesView0],
        lease |-> [m \in CzMembers |-> IF established THEN CzLease ELSE 0],
        viewQ |-> [m \in CzMembers |-> <<>>], grantQ |-> [m \in CzMembers |-> <<>>],
        advertised |-> IF established THEN HermesAppMembers ELSE {},
        reg |-> IF established THEN mesh ELSE {}, opening |-> {}, exits |-> {}, dial |-> {},
        wire |-> {}, op |-> "none", committed |-> FALSE]

\* Membership upcalls and operating lease renewals are independently
\* selected in HermesNode.Start. An expiry is an absolute deadline:
\* copying delayed grant data must carry the remaining time, not renew it.
HermesRenew(m, remaining) ==
    /\ czUp[m] /\ remaining > 0 /\ hermes.lease[m] /= remaining
    /\ hermes' = [hermes EXCEPT !.lease[m] = remaining]

HermesConsumeGrant(m) ==
    /\ czUp[m] /\ hermes.grantQ[m] /= <<>>
    /\ hermes' = [hermes EXCEPT !.lease[m] = Max(0,Head(hermes.grantQ[m])), !.grantQ[m] = Tail(@)]

\* RMember starts first. Once the Hermes listener exists, SetAppPeer
\* makes its own PeerID and URL available for subsequent announcements.
HermesAdvertise(m) ==
    /\ czUp[m] /\ m \in HermesAppMembers \ hermes.advertised
    /\ hermes' = [hermes EXCEPT !.advertised = @ \cup {m}]

HermesMissing(reg, me, live) ==
    {HermesPair(me, x) : x \in {y \in live :
                     y[1] /= me[1] /\ HermesSlot(reg, me, y) = {}}}
HermesPeers(s, m) ==
    {e.dst : e \in {q \in s.reg : q.src = HermesSelf(m)}}
HermesRequired(s, m) ==
    IF s.view[m].live /= {} THEN s.view[m].live
    ELSE {HermesSelf(m)} \cup HermesPeers(s, m)

\* Broadcasts use installed circuits; acknowledgments target the sender,
\* which can now be a replaying reader. A set coalesces identical messages.
HermesMsg(s, m, dst, kind, ver) ==
    [src |-> HermesSelf(m), dst |-> dst, kind |-> kind, ver |-> ver,
     epoch |-> s.view[m].epoch, vers |-> s.view[m].vers,
     value |-> IF kind = "INV" THEN s.key[m].value ELSE 0,
     rmw |-> kind = "INV" /\ s.op = "rmw"]
HermesSend(s, m, dsts, kind, ver) ==
    [s EXCEPT !.wire = @ \cup {HermesMsg(s, m, d, kind, ver) :
                                d \in {peer \in dsts : HermesSlot(s.reg \ s.exits,HermesSelf(m),peer) /= {}}}]
HermesBroadcast(s, m, kind) == HermesSend(s, m, HermesPeers(s, m), kind, s.key[m].ver)

HermesComplete(s, m) ==
    [s EXCEPT !.key[m].phase = "valid",
                       !.key[m].read = IF s.key[m].waiting THEN s.key[m].value ELSE @,
                       !.key[m].waiting = FALSE, !.key[m].timer = -1, !.committed = TRUE]
HermesFinish(s, m) == HermesBroadcast(HermesComplete(s,m),m,"VAL")

\* applyMembershipChange replaces the view, dials missing peers, resets
\* in-flight ACK sets and reissues each current operation in the new view.
HermesAdopt(m) ==
    LET view == Head(hermes.viewQ[m])
        s == [hermes EXCEPT !.view[m] = view, !.viewQ[m] = Tail(@),
                    !.dial = @ \cup HermesMissing(hermes.reg, HermesSelf(m), view.live)]
        pending == s.key[m].phase \in {"write", "replay"}
        t == [s EXCEPT !.key[m].acks = {HermesSelf(m)}, !.key[m].phase = "replay"]
        replay == HermesBroadcast(t, m, "INV")
    IN /\ czUp[m] /\ hermes.viewQ[m] /= <<>>
       /\ hermes' = IF ~pending THEN s
                    ELSE IF HermesRequired(replay,m) \subseteq replay.key[m].acks
                         THEN HermesFinish(replay,m) ELSE replay

\* connectMembershipPeers issues one attempt. Both circuit ends are handed
\* to their own event loops. A failed attempt has no autonomous retry.
\* Two simultaneously distinguishable circuits suffice for a late exit to
\* race replacement. IDs may be reused only after every reference is gone.
HermesDial(q) ==
    /\ q \in hermes.dial
    /\ LET s == [hermes EXCEPT !.dial = @ \ {q}]
       IN \/ hermes' = s
          \/ /\ q.src = HermesSelf(q.src[1]) /\ q.dst = HermesSelf(q.dst[1])
             /\ czUp[q.src[1]] /\ czUp[q.dst[1]]
             /\ \E g \in 1..2 :
                   LET a == HermesEnd(q.src, q.dst, g)
                       ends == {a, HermesReverse(a)}
                   IN /\ ends \cap (s.reg \cup s.opening \cup s.exits) = {}
                      /\ hermes' = [s EXCEPT !.opening = @ \cup ends]
HermesRegister(c) ==
    /\ c \in hermes.opening
    /\ hermes' = [hermes EXCEPT !.opening = @ \ {c},
                     !.reg = (@ \ HermesSlot(@, c.src, c.dst)) \cup {c}]
HermesBreak(c) ==
    /\ c \in hermes.reg \ hermes.exits
    /\ LET ends == {c, HermesReverse(c)}
       IN hermes' = [hermes EXCEPT !.exits = @ \cup (ends \cap hermes.reg),
                                   !.opening = @ \ ends]
\* forgetCircuit: a late callback clears only its matching pointer. Redial
\* uses the most recently delivered membership, including successive adds.
HermesForget(c) ==
    /\ c \in hermes.exits
    /\ LET reg == hermes.reg \ {c}
           retry == IF c \in hermes.reg
                    THEN HermesMissing(reg, c.src, hermes.view[c.src[1]].live)
                    ELSE {}
       IN hermes' = [hermes EXCEPT !.exits = @ \ {c}, !.reg = reg, !.dial = @ \cup retry]
HermesNet ==
    \/ \E q \in hermes.dial : HermesDial(q)
    \/ \E c \in hermes.opening : HermesRegister(c)
    \/ \E c \in hermes.reg : HermesBreak(c)
    \/ \E c \in hermes.exits : HermesForget(c)

\* Write/Read/RMW read one atomic deadline before handing a ticket to the
\* loop. Time may advance before the event loop receives that ticket.
HermesAdmit(m, kind) ==
    /\ czUp[m] /\ hermes.lease[m] > 0 /\ hermes.key[m].issued = "none"
    /\ kind \in {"write", "rmw", "read"}
    /\ (kind = "rmw" => hermes.key[m].phase /= "absent")
    /\ IF kind = "read"
       THEN /\ ~hermes.key[m].waiting
            /\ hermes' = [hermes EXCEPT !.key[m].issued = kind]
       ELSE /\ hermes.op = "none"
            /\ hermes' = [hermes EXCEPT !.key[m].issued = kind, !.op = kind]
HermesClient(m) ==
    LET k == hermes.key[m]
        s == [hermes EXCEPT !.key[m].issued = "none"]
        version == IF k.issued = "rmw" THEN 1 ELSE 2
        issued == [s EXCEPT !.key[m].ver = version, !.key[m].value = IF k.issued = "rmw" THEN k.value + 1 ELSE 7,
                            !.key[m].timer = HermesLossTicks,
                            !.key[m].phase = "write", !.key[m].acks = {HermesSelf(m)}]
    IN /\ czUp[m] /\ k.issued /= "none"
       /\ CASE k.issued = "read" ->
                hermes' = IF k.phase = "valid" THEN [s EXCEPT !.key[m].read = k.value]
                          ELSE IF k.phase = "absent" THEN [s EXCEPT !.key[m].read = -2]
                          ELSE [s EXCEPT !.key[m].waiting = TRUE,
                                !.key[m].timer = IF @ < 0 THEN HermesLossTicks ELSE @]
            [] OTHER ->
                /\ k.phase = "valid" \/ (k.phase = "absent" /\ k.issued = "write")
                /\ hermes' = IF HermesReplicationDegree = 1 THEN HermesComplete(issued,m)
                             ELSE HermesBroadcast(issued,m,"INV")

\* checkCoordOrFollowerFailed handles the local message-loss timer even
\* after a lease expires. Its recipients check their own lease. Replay
\* preserves the original timestamp/value and RMW flag (known from op).
HermesTimeout(m) ==
    LET k == hermes.key[m]
        timed == [hermes EXCEPT !.key[m].timer = HermesLossTicks]
        s == [timed EXCEPT !.key[m].phase = "replay", !.key[m].acks = {HermesSelf(m)}]
        covered == HermesRequired(hermes,m) \subseteq k.acks
    IN /\ czUp[m] /\ k.timer = 0
       /\ CASE k.phase = "invalid" /\ k.waiting ->
                   hermes' = HermesBroadcast(s,m,"INV")
            [] k.phase \in {"write","replay"} ->
                   IF ~covered /\ hermes.view[m].live /= {}
                   THEN hermes' = HermesBroadcast(timed,m,"INV")
                   ELSE /\ k.phase = "replay" \/ hermes.op = "write"
                        /\ hermes' = HermesFinish(hermes,m)
            [] OTHER -> FALSE

\* HermesNode.Start checks the operating lease and the exact (czar epoch,
\* within-epoch version) before delivering INV, ACK or VALIDATE. An
\* equal-ts INV from a replay is ACKed without invalidating a valid key.
HermesReceive(msg) ==
    LET m == msg.dst[1]
        k == hermes.key[m]
        s == [hermes EXCEPT !.wire = @ \ {msg}]
        accepted == /\ HermesSlot(s.reg,msg.dst,msg.src) /= {}
                    /\ czUp[m] /\ msg.dst = HermesSelf(m) /\ s.lease[m] > 0
                    /\ msg.epoch = s.view[m].epoch /\ msg.vers = s.view[m].vers
        inv == IF msg.ver > k.ver
               THEN [s EXCEPT !.key[m].ver = msg.ver, !.key[m].value = msg.value,
                              !.key[m].phase = "invalid"] ELSE s
        ack == [s EXCEPT !.key[m].acks = @ \cup {msg.src}]
    IN /\ msg \in hermes.wire
       /\ hermes' =
            CASE ~accepted -> s
              [] msg.kind = "INV" -> HermesSend(inv,m,{msg.src},"ACK",msg.ver)
              [] msg.kind = "ACK" ->
                   IF msg.ver = k.ver /\ k.phase \in {"write","replay"}
                   THEN IF HermesRequired(ack,m) \subseteq ack.key[m].acks
                        THEN HermesFinish(ack,m) ELSE ack
                   ELSE s
              [] msg.kind = "VAL" ->
                   IF msg.ver = k.ver /\ k.phase /= "valid"
                   THEN [s EXCEPT !.key[m].phase = "valid", !.key[m].waiting = FALSE, !.key[m].timer = -1,
                         !.key[m].read = IF k.waiting THEN k.value ELSE @]
                   ELSE s
HermesDrop(msg) ==
    /\ msg \in hermes.wire
    /\ hermes' = [hermes EXCEPT !.wire = @ \ {msg}]
HermesProtocol ==
    \/ \E m \in CzMembers :
        \/ \E op \in {"write","rmw","read"} : HermesAdmit(m,op)
        \/ HermesClient(m) \/ HermesTimeout(m)
    \/ \E msg \in hermes.wire :
        \/ HermesReceive(msg)
        \/ HermesDrop(msg)

HermesTick ==
    hermes' = [hermes EXCEPT !.lease = [m \in CzMembers |-> Max(0, hermes.lease[m]-1)],
        !.key = [m \in CzMembers |-> [hermes.key[m] EXCEPT !.timer = IF @ > 0 THEN @-1 ELSE @]],
        !.grantQ = [m \in CzMembers |->
                      [i \in DOMAIN hermes.grantQ[m] |-> Max(0,hermes.grantQ[m][i]-1)]]]

HermesStop(m) ==
    LET id == HermesSelf(m)
        mine(q) == q.src = id
        involved(q) == q.src = id \/ q.dst = id
    IN hermes' = [hermes EXCEPT !.key[m] = [HermesLocal0 EXCEPT !.phase = "absent"], !.lease[m] = 0, !.view[m] = HermesView0,
                    !.viewQ[m] = <<>>, !.grantQ[m] = <<>>, !.advertised = @ \ {m},
                    !.reg = {c \in @ : ~mine(c)}, !.dial = {q \in @ : ~mine(q)},
                    !.opening = {c \in @ : ~involved(c)},
                    !.exits = {c \in @ \cup hermes.reg : c.dst = id}
                              \cup {c \in @ : ~involved(c)}]

\* NewRMember creates two independent FIFO channels, each of capacity ten.
\* Record publication in the same step as the producer. View payloads keep
\* their endpoint binding even when a later SetAppPeer updates the sender.
\* Equal consecutive deadlines on the same grant channel coalesce here.
HermesQueueUpcalls ==
    LET view(m) == [epoch |-> CzPublishedEpoch(m)', vers |-> CzPublishedVersion(m)',
                    live |-> HermesLive(CzPublishedMembers(m)',hermes.advertised)]
        emitView(m) == /\ czUp[m]
                       /\ <<CzPublishedEpoch(m),CzPublishedVersion(m),CzPublishedMembers(m)>>'
                          /= <<CzPublishedEpoch(m),CzPublishedVersion(m),CzPublishedMembers(m)>>
        emitGrant(m) == czUp[m] /\ CzOperatingGrant(m)' /= CzOperatingGrant(m)
        views == [m \in CzMembers |-> IF emitView(m) THEN Append(hermes.viewQ[m],view(m))
                                      ELSE hermes.viewQ[m]]
        grants == [m \in CzMembers |-> IF emitGrant(m)
                      THEN Append(hermes.grantQ[m], Max(0,CzOperatingGrant(m)'))
                      ELSE hermes.grantQ[m]]
    IN /\ \A m \in CzMembers : Len(views[m]) <= 10 /\ Len(grants[m]) <= 10
       /\ hermes' = [hermes EXCEPT !.viewQ = views, !.grantQ = grants]

HermesTypeOK ==
    /\ \A m \in CzMembers :
        /\ hermes.lease[m] \in Nat
        /\ Len(hermes.grantQ[m]) <= 10 /\ Len(hermes.viewQ[m]) <= 10
        /\ hermes.key[m].phase \in {"absent","valid","invalid","write","replay"}
        /\ hermes.key[m].issued \in {"none","write","rmw","read"}
        /\ hermes.key[m].timer \in (-1)..HermesLossTicks
        /\ hermes.key[m].ver \in 0..2
    /\ \A c \in hermes.reg : Cardinality(HermesSlot(hermes.reg,c.src,c.dst)) = 1
    /\ \A q \in hermes.dial : q.src[1] /= q.dst[1]
HermesPayloads ==
    \A msg \in hermes.wire : msg.kind = "INV" =>
        /\ msg.rmw = (hermes.op = "rmw")
        /\ msg.ver = (IF msg.rmw THEN 1 ELSE 2)
        /\ msg.value = (IF msg.rmw THEN 1 ELSE 7)
HermesRoundAgreement ==
    \A a,b \in CzMembers : hermes.key[a].ver = hermes.key[b].ver =>
                            hermes.key[a].value = hermes.key[b].value
\* Stable membership safety: any replica still returning a valid value
\* after the write/replay completes already holds the completed update.
HermesCompletedValue ==
    hermes.committed => \A m \in CzMembers :
        hermes.key[m].phase = "valid" => hermes.key[m].ver > 0

\* A delayed circuit-reader exit must preserve a replacement. Removing the
\* installed generation must issue a new attempt for every missing peer in
\* the latest view (HermesNode.forgetCircuit, connectMembershipPeers).
HermesCircuitCleanup ==
    [][\A c \in hermes.exits : HermesForget(c) =>
         IF c \in hermes.reg
         THEN HermesMissing(hermes'.reg,c.src,hermes.view[c.src[1]].live) \subseteq hermes'.dial
         ELSE hermes'.reg = hermes.reg]_hermes

\* ========================================================================
\* Model-checking bounds, entry points and fairness
\* ========================================================================
\* Each MC entry point selects the actions for its layer. MCSpecHermes
\* advances both czar and Hermes. Counter fc bounds injected faults and timers.
\* Resource guards on terms, queues, circuits and connections further
\* restrict exploration. The Live specifications specify their own fairness.

CONSTANTS
    \* --- base: fault and timer budgets ---
    MaxCrash,           \* process crashes (incl. disk-error panics in saves)
    MaxRestart,         \* process (re)starts
    MaxAddrChange,      \* restarts at a different listen address (D-36)
    MaxStop,            \* SIGSTOP / wedged processes
    MaxLinkDown,        \* directional link failures
    MaxElectionTimeout, \* election-timer firings (all nodes)
    MaxHeartbeat,       \* leader heartbeat-timer firings
    MaxClientReq,       \* client writes
    MaxReconfig,        \* membership changes submitted
    MaxCompact,         \* WAL compactions
    MaxAuditGC,         \* 20 s audit GC firings
    MaxTorn,            \* storage faults: crash inside a plain WAL append (PAR torn tail),
                        \* lost last PAR identifier, lost WAL (CrashDiskFault)
    MaxStallDrop,       \* injected transport delays, failed writes, dropped handoffs and read errors
    MaxParTick,         \* PAR snapshot requests
    \* --- base: resource bounds (state-space guards) ---
    MaxTerm,            \* largest term a candidate may start
    MaxCkt,             \* circuits created after Init
    MaxConn,            \* connections created after Init
    MaxQueue,           \* timers do not fire while a queue holds MaxQueue frames
    \* --- liveness premises ---
    LiveQ,              \* the healthy majority of EventuallyLeaderWithHealthyMajority
    LiveA, LiveB,       \* the pair of PeerConnectivityRestored / NoPersistentBlackHole
    \* --- KV / czar budgets ---
    MaxKVFailover, MaxKVRetry, MaxKVLocalRead,
    MaxCzLost, MaxCzCrash, MaxCzRestart

VARIABLE fc

FC0 == [crash |-> 0, restart |-> 0, addrChange |-> 0, stop |-> 0, linkDown |-> 0,
        eto |-> 0, hb |-> 0, req |-> 0, reconfig |-> 0, compact |-> 0, auditGC |-> 0,
        tornCrash |-> 0, stallDrop |-> 0, parTick |-> 0,
        kvFailover |-> 0, kvRetry |-> 0, kvLocalRead |-> 0,
        czLost |-> 0, czCrash |-> 0, czRestart |-> 0]

vars == <<coreVars, kvVars, czVars, hermes, fc>>
mcVars == vars

Bump(f) == fc' = [fc EXCEPT ![f] = @ + 1]
BaseOnly == UNCHANGED <<kvVars, czVars, hermes>>
NoFc == UNCHANGED fc
KVOnly == UNCHANGED <<coreVars, czVars, hermes>>
CzOnly == UNCHANGED <<coreVars, kvVars>>

\* All layers start in their own initial state. Each entry point advances
\* its selected actions. The reference relations leave the counters at FC0.
auxVars == <<kvVars, czVars, hermes, fc>>
AuxInit == KV_Init /\ Cz_Init /\ hermes = HermesInitial(FALSE) /\ fc = FC0
Init == CoreInit /\ AuxInit
Next == CoreNext /\ BaseOnly /\ NoFc
Spec == Init /\ [][Next]_vars

KV_Spec == Init /\ [][KV_Next /\ KVOnly /\ NoFc]_vars
Cz_Spec == Init /\ [][Cz_Next /\ CzOnly /\ NoFc /\ UNCHANGED hermes]_vars

CktBase == NN * NN + 1
CktRoom == nextCkt < CktBase + MaxCkt
ConnRoom == nextConn < CktBase + MaxConn
QueuesBelow == \A k \in DOMAIN conn : \A d \in Dirs : Len(conn[k].q[d]) + Len(conn[k].out[d]) < MaxQueue
\* the resource bounds a liveness counterexample may have exhausted
BoundHit == \/ \E n \in Server : currentTerm[n] >= MaxTerm
            \/ ~CktRoom \/ ~ConnRoom

\* ========================================================================
\* BOUNDED (fault-injection / time-driven) BASE ACTIONS
\* ========================================================================
MCCrash(n)      == fc.crash < MaxCrash /\ Crash(n) /\ Bump("crash") /\ BaseOnly
MCRestart(n, a) ==
    /\ fc.restart < MaxRestart
    /\ (a /= addr[n] => fc.addrChange < MaxAddrChange)
    /\ Restart(n, a)
    /\ fc' = [fc EXCEPT !.restart = @ + 1, !.addrChange = IF a /= addr[n] THEN @ + 1 ELSE @]
    /\ BaseOnly
MCStop(n)       == fc.stop < MaxStop /\ Stop(n) /\ Bump("stop") /\ BaseOnly
MCLinkDown(x, y) == fc.linkDown < MaxLinkDown /\ LinkDown(x, y) /\ Bump("linkDown") /\ BaseOnly
MCElectionTimeout(n) ==
    /\ fc.eto < MaxElectionTimeout /\ QueuesBelow
    /\ ElectionTimeout(n) /\ Bump("eto") /\ BaseOnly
MCLeaderHeartbeat(n) ==
    /\ fc.hb < MaxHeartbeat /\ QueuesBelow
    /\ LeaderHeartbeat(n) /\ Bump("hb") /\ BaseOnly
MCClientRequest(n) ==
    /\ fc.req < MaxClientReq
    /\ ClientRequest(n) \/ \E timed \in BOOLEAN : ClientSubmit(n, timed)
    /\ Bump("req") /\ BaseOnly
MCClientRequestTornCrash(n) ==
    /\ fc.tornCrash < MaxTorn /\ fc.req < MaxClientReq
    /\ ClientRequestTornCrash(n)
    /\ fc' = [fc EXCEPT !.tornCrash = @ + 1, !.req = @ + 1] /\ BaseOnly
MCClientChangeMembership(n, op, x) ==
    fc.reconfig < MaxReconfig /\ ClientChangeMembership(n, op, x) /\ Bump("reconfig") /\ BaseOnly
MCCompact(n)    == fc.compact < MaxCompact /\ Compact(n) /\ Bump("compact") /\ BaseOnly
MCAuditGC(n)    == fc.auditGC < MaxAuditGC /\ AuditGC(n) /\ Bump("auditGC") /\ BaseOnly
MCParRequestSnapshot(n) == fc.parTick < MaxParTick /\ ParRequestSnapshot(n) /\ Bump("parTick") /\ BaseOnly
\* storage faults found at restart share the storage-fault budget (MaxTorn)
MCCrashDiskFault(n, kind) ==
    fc.tornCrash < MaxTorn /\ CrashDiskFault(n, kind, 1) /\ Bump("tornCrash") /\ BaseOnly
MCHandleAppendEntriesTornCrash(k, dir, i) ==
    fc.tornCrash < MaxTorn /\ HandleAppendEntriesTornCrash(k, dir, i) /\ Bump("tornCrash") /\ BaseOnly
MCStalledReceiverDrop(k, dir, i, b) ==
    fc.stallDrop < MaxStallDrop /\ StalledReceiverDrop(k, dir, i, b) /\ Bump("stallDrop") /\ BaseOnly

\* ========================================================================
\* REACTIVE BASE ACTIONS (unbounded; resource guards only)
\* ========================================================================
MCCont(n)        == Cont(n) /\ NoFc /\ BaseOnly
MCLinkUp(x, y)   == LinkUp(x, y) /\ NoFc /\ BaseOnly
\* a candidate may not start a term beyond MaxTerm (resource bound)
MCPreVotePhase2Timeout(n) ==
    /\ currentTerm[n] + 1 <= MaxTerm
    /\ PreVotePhase2Timeout(n) /\ NoFc /\ BaseOnly
MCRestartWindowTick(n) == RestartWindowTick(n) /\ NoFc /\ BaseOnly
MCRestartWindowElapsed(n) == RestartWindowElapsed(n) /\ NoFc /\ BaseOnly
MCRunDeferredCampaign(n) ==
    /\ (campaign[n].deferred = "election" => currentTerm[n] + 1 <= MaxTerm)
    /\ RunDeferredCampaign(n) /\ NoFc /\ BaseOnly
MCResumeCampaign(n) == ResumeCampaign(n) /\ NoFc /\ BaseOnly
MCBecomeLeaderSave(n) == BecomeLeaderSave(n) /\ NoFc /\ BaseOnly
MCUnblockSend(n) == (AdmitWaiting(n) \/ UnblockSend(n)) /\ NoFc /\ BaseOnly
MCDialAttempt(n,m) ==
    DialAttempt(n,m) /\ nextConn' <= CktBase + MaxConn /\ nextCkt' <= CktBase + MaxCkt /\ NoFc /\ BaseOnly
MCWatchdogSilence(n,m) == WatchdogSilence(n,m) /\ NoFc /\ BaseOnly
MCAckAge(n,k) == AckAge(n,k) /\ NoFc /\ BaseOnly
MCObserverCircuit(n,opened) == ObserverCircuit(n,opened) /\ NoFc /\ BaseOnly
MCParIdentifyInterrupted(n) == ParIdentifyInterrupted(n) /\ NoFc /\ BaseOnly
MCParFinish(n) == ParFinish(n) /\ NoFc /\ BaseOnly
MCParObserveQuery(n, m) == ParObserveQuery(n, m) /\ NoFc /\ BaseOnly
MCParRecoverFromDonor(n, m) == ParRecoverFromDonor(n, m) /\ NoFc /\ BaseOnly
MCHandleNewCircuit(n, c) == HandleNewCircuit(n, c) /\ NoFc /\ BaseOnly
MCHandlePeerLeft(n, c)   == HandlePeerLeft(n, c) /\ NoFc /\ BaseOnly
MCCancelPendingDial(n,c) == CancelPendingDial(n,c) /\ NoFc /\ BaseOnly
MCHaltOnDeadConn(n, c)   == HaltOnDeadConn(n, c) /\ NoFc /\ BaseOnly
MCConnAbort(k, side)     == ConnAbort(k, side) /\ NoFc /\ BaseOnly
MCAckFlush(k, dir)       == AckFlush(k, dir) /\ NoFc /\ BaseOnly
MCDeadlineTick(n)        == MCDeadlineElapsed(n) /\ NoFc /\ BaseOnly
MCWriterStep(k,dir) ==
    /\ \/ StartWrite(k,dir)
       \/ WriteQueuedFrame(k,dir)
       \/ \E f \in conn[k].bg[dir] : QueueCloseNotice(k,dir,f)
    /\ NoFc /\ BaseOnly

\* Successful partial writes are observationally silent until completed.
\* Split a partial write only when this cfg explores transport failure, so
\* its later expiry can close the stream. Count injected expiry/errors once.
MCWriterDelay(k,dir) ==
    /\ fc.stallDrop < MaxStallDrop /\ BaseOnly
    /\ \/ BeginPartialWrite(k,dir) /\ NoFc
       \/ (ExpireQueuedFrame(k,dir) \/ RefuseQueuedFrame(k,dir) \/ FramingReadError(k,dir) \/ PartialKeepAlive(k,dir)
           \/ \E f \in conn[k].bg[dir] : ExpireCloseNotice(k,dir,f)) /\ Bump("stallDrop")

MCStickyExpire(n)        == StickyExpire(n) /\ NoFc /\ BaseOnly
MCStartupClockTick(n)    == StartupClockTick(n) /\ NoFc /\ BaseOnly
MCLeaderAge(n)           == LeaderAge(n) /\ NoFc /\ BaseOnly
MCLeaseCreditExpire(n, k) == LeaseCreditExpire(n, k) /\ NoFc /\ BaseOnly
MCPongExpire(n, k)       == PongExpire(n, k) /\ NoFc /\ BaseOnly

MCFrame(k, dir, i) ==
    /\ \/ HandleRequestPreVote(k, dir, i)
       \/ HandlePreVote(k, dir, i)
       \/ HandleRequestVote(k, dir, i)
       \/ HandleVote(k, dir, i)
       \/ HandleAppendEntries(k, dir, i)
       \/ HandleAppendEntriesAck(k, dir, i)
       \/ HandleTicketFrame(k, dir, i)
       \/ HandleStateSnapshot(k, dir, i)
       \/ HandleRequestStateSnapshotMsg(k, dir, i)
       \/ RejectNamesakeFrame(k,dir,i)
       \/ HandleEndCircuit(k, dir, i)
       \/ HandlePeerError(k, dir, i)
       \/ DropUnroutable(k, dir, i)
       \/ RejectForeignFrame(k, dir, i)
       \/ HandleStartFrame(k, dir, i)
       \/ HandleSetupAck(k, dir, i)
    /\ NoFc /\ BaseOnly

\* ========================================================================
\* BASE NEXT / SPEC
\* ========================================================================
MCNextNode(n) ==
    \/ fc.reconfig < MaxReconfig /\ SubmitAdminTicket(n) /\ Bump("reconfig") /\ BaseOnly
    \/ MCCrash(n) \/ MCStop(n) \/ MCCont(n)
    \/ \E kind \in StorageFaults : MCCrashDiskFault(n, kind)
    \/ \E a \in Addrs : MCRestart(n, a)
    \/ MCUnblockSend(n)
    \/ MCElectionTimeout(n) \/ MCPreVotePhase2Timeout(n) \/ MCLeaderHeartbeat(n)
    \/ MCRestartWindowTick(n) \/ MCRestartWindowElapsed(n) \/ MCRunDeferredCampaign(n) \/ MCResumeCampaign(n)
    \/ MCBecomeLeaderSave(n) \/ MCClientRequest(n) \/ MCClientRequestTornCrash(n)
    \/ TicketWork(n) /\ NoFc /\ BaseOnly
    \/ MCCompact(n) \/ MCAuditGC(n) \/ MCParRequestSnapshot(n)
    \/ MCStickyExpire(n) \/ MCLeaderAge(n) \/ MCStartupClockTick(n) \/ MCDeadlineTick(n)
    \/ MCParIdentifyInterrupted(n) \/ MCParFinish(n)
    \/ \E m \in Server : MCParObserveQuery(n, m)
    \/ \E m \in Server : MCDialAttempt(n, m) \/ MCWatchdogSilence(n,m) \/ MCParRecoverFromDonor(n, m)
    \/ \E opened \in BOOLEAN : MCObserverCircuit(n,opened)
    \/ \E op \in {"add", "remove"}, x \in Server : MCClientChangeMembership(n, op, x)
    \/ \E key \in Keys : MCLeaseCreditExpire(n, key) \/ MCPongExpire(n, key) \/ MCAckAge(n,key)
    \/ \E c \in DOMAIN ckt : MCHandleNewCircuit(n, c) \/ MCHandlePeerLeft(n, c) \/ MCHaltOnDeadConn(n, c) \/ MCCancelPendingDial(n,c)

MCNextFrames ==
    \E k \in DOMAIN conn, dir \in Dirs : \E i \in FrameIdx(k, dir) :
        \/ MCFrame(k, dir, i)
        \/ MCHandleAppendEntriesTornCrash(k, dir, i)
        \/ \E key \in Keys : AgeGossip(k,dir,i,key) /\ NoFc /\ BaseOnly
        \/ \E b \in BOOLEAN : MCStalledReceiverDrop(k, dir, i, b)

MCNext ==
    \/ \E n \in Server : MCNextNode(n)
    \/ \E x, y \in Server : MCLinkDown(x, y) \/ MCLinkUp(x, y)
    \/ \E k \in DOMAIN conn : \/ \E side \in {"d", "a"} : MCConnAbort(k, side)
                             \/ \E dir \in Dirs : MCAckFlush(k,dir) \/ MCWriterStep(k,dir) \/ MCWriterDelay(k,dir)
    \/ MCNextFrames

MCInit == Init

\* Quorum-based configurations can be deliberately superseded by an
\* administrator. Only these entry points enable overrides and bootstrap.
AdminInput(n) ==
    \/ BootstrapSelf(n)
    \/ AdminEmptyConfig(n)
    \/ \E op \in {"add","remove"}, x \in Server :
            AdminForceContact(n,op,x) \/ ClientChangeMembershipWithForce(n,op,x,TRUE)
    \/ \E x \in Server :
            AdminShadow(n,"shadowRemove",x,Nil) \/ \E d \in Det : AdminShadow(n,"shadowAdd",x,d)

AdminFrame ==
    \E k \in DOMAIN conn, dir \in Dirs : \E i \in FrameIdx(k,dir) : HandleAdminForce(k,dir,i)

AdminSpec ==
    Init /\ [][(CoreNext \/ AdminFrame \/ \E n \in Server :
                     AdminInput(n) \/ \E a \in Addrs : RestartWithMode(n,a,TRUE)) /\ BaseOnly /\ NoFc]_vars

MCAdminNext ==
    \/ MCNext
    \/ AdminFrame /\ NoFc /\ BaseOnly
    \/ \E n \in Server :
         \/ fc.reconfig < MaxReconfig /\ AdminInput(n) /\ currentTerm'[n] <= MaxTerm
            /\ Bump("reconfig") /\ BaseOnly
         \/ \E a \in Addrs :
              /\ fc.restart < MaxRestart /\ (a /= addr[n] => fc.addrChange < MaxAddrChange)
              /\ RestartWithMode(n,a,TRUE)
              /\ fc' = [fc EXCEPT !.restart = @ + 1, !.addrChange = IF a /= addr[n] THEN @ + 1 ELSE @]
              /\ BaseOnly

MCSpecAdmin == MCInit /\ [][MCAdminNext]_mcVars

MCSpec == MCInit /\ [][MCNext]_mcVars

\* ========================================================================
\* KV (S10) NEXT / SPEC
\* ========================================================================
MCKVNext ==
    \/ KVTick /\ NoFc /\ KVOnly
    \/ \E n \in KVReplicas :
         \/ KVApply(n) /\ NoFc /\ KVOnly
         \/ KVShip(n) /\ NoFc /\ KVOnly
         \/ KVInstallSnapshot(n) /\ NoFc /\ KVOnly
         \/ fc.kvFailover < MaxKVFailover /\ KVFailover(n) /\ Bump("kvFailover") /\ KVOnly
    \/ \E c \in KVClients :
         \/ KVClientStep(c) /\ NoFc /\ KVOnly
         \/ fc.kvRetry < MaxKVRetry /\ KVRetrySameSerial(c) /\ Bump("kvRetry") /\ KVOnly
         \/ fc.kvLocalRead < MaxKVLocalRead /\ KVLeaderLocalRead(c) /\ Bump("kvLocalRead") /\ KVOnly

MCSpecKV == MCInit /\ [][MCKVNext]_mcVars

\* ========================================================================
\* CZAR (S11) NEXT / SPEC
\* ========================================================================
\* Membership churn increases the version within one epoch. Bound only the
\* MC dispatcher, so the reference Cz_Next retains arbitrary version changes.
CzMaxVersion == 2*CzMaxEpoch + Cardinality(CzMembers)
MCCzClient(m) == CzClientStep(m) /\ (\A n \in CzMembers : czVers'[n].w <= CzMaxVersion)
                 /\ NoFc /\ CzOnly
MCCzApply == CzApplySome /\ (czReg' /= CzNil => czReg'.epoch <= CzMaxEpoch) /\ NoFc /\ CzOnly
MCCzEvents ==
    \/ MCCzApply
    \/ \E m \in CzMembers :
         \/ MCCzClient(m)
         \/ fc.czLost < MaxCzLost /\ CzClientFault(m) /\ Bump("czLost") /\ CzOnly
         \/ fc.czCrash < MaxCzCrash /\ CzCrash(m) /\ Bump("czCrash") /\ CzOnly
         \/ fc.czRestart < MaxCzRestart /\ CzRestart(m) /\ Bump("czRestart") /\ CzOnly

MCCzNext == ((CzTick /\ NoFc /\ CzOnly) \/ MCCzEvents) /\ UNCHANGED hermes

MCSpecCzar == MCInit /\ [][MCCzNext]_mcVars

\* The shared clock ages both grants once. The non-time czar steps include
\* grant publication and crash/restart. Hermes independently consumes each
\* upcall, so receipt of a lease may precede receipt of its membership.
HermesOnly == UNCHANGED <<coreVars, kvVars, czVars, fc>>
HermesCzarStep ==
    \/ CzTick /\ NoFc /\ CzOnly /\ HermesTick
    \/ /\ MCCzEvents
       /\ IF \E m \in CzMembers : czUp[m] /\ ~czUp'[m]
          THEN \E m \in CzMembers : czUp[m] /\ ~czUp'[m] /\ HermesStop(m)
          ELSE HermesQueueUpcalls
    \/ /\ HermesOnly
       /\ \/ HermesProtocol \/ HermesNet
          \/ \E m \in CzMembers :
                 HermesAdvertise(m) \/ HermesConsumeGrant(m) \/ HermesAdopt(m)
MCSpecHermes == Init /\ [][HermesCzarStep]_vars

\* Focused checks start after establishing the same group and key on all
\* replicas. In the round profile the czar keeps this view and can renew a
\* lease. Network faults manifest as rejected sends or lost messages.
HermesEstablishedInit == CoreInit /\ KV_Init /\ Cz_Init /\ fc = FC0 /\ hermes = HermesInitial(TRUE)
HermesRoundNext == HermesOnly /\
    (HermesProtocol \/ HermesTick \/ \E m \in CzMembers : HermesRenew(m, CzLease))
HermesRoundSpec == HermesEstablishedInit /\ [][HermesRoundNext]_vars
HermesCircuitSpec == HermesEstablishedInit /\ [][HermesOnly /\ HermesNet]_vars



\* ========================================================================
\* FAIRNESS (liveness pattern: weak fairness per process on its non-fault
\* steps; never on fault actions).
\*  - Tube's event loop is one goroutine selecting over ready channels
\*    (tube/tube.go:1061-2027): every ready timer / frame / circuit event is
\*    eventually taken.  Frames of each kind and each timer get their own WF.
\*  - Election timer: re-armed on every firing (resetElectionTimeout at
\*    tube/tube.go:1424); phase-2 timer armed by tallyPreVote (7029-7041);
\*    leader heartbeat re-armed by resetLeaderHeartbeat (8756-8758).
\*  - Transport goroutines (send loop, read loop, pump, watchdog attempt) run
\*    independently of the event loop; the kernel's TCP timers eventually
\*    abort a connection facing a dead peer or a dropped link (ConnAbort).
\* In the Live specs the election and heartbeat timers are NOT counter
\* bounded (progress depends on them); MaxTerm / MaxQueue still apply and
\* timer budgets do not supply evidence of protocol progress. Bounded
\* liveness counterexamples must be checked for exhausted resources.
\* ========================================================================
LiveElectionTimeout(n) == QueuesBelow /\ ElectionTimeout(n) /\ NoFc /\ BaseOnly
LiveLeaderHeartbeat(n) == QueuesBelow /\ LeaderHeartbeat(n) /\ NoFc /\ BaseOnly

\* A local writer that is scheduled cannot keep a plain framed data write
\* stuck forever after its deadline. This part of liveness is independent
\* of the finite fault-injection budget. Give a blocked END background
\* hand-off its timer as well.
LiveWriterTimeout(k,dir) ==
    /\ \/ (~WireRoom(conn,k,dir) /\ ExpireQueuedFrame(k,dir))
       \/ \E f \in conn[k].bg[dir] : ExpireCloseNotice(k,dir,f)
    /\ NoFc /\ BaseOnly

MCNextLive ==
    \/ MCNext
    \/ \E n \in Server : LiveElectionTimeout(n) \/ LiveLeaderHeartbeat(n)
    \/ \E k \in DOMAIN conn, dir \in Dirs : LiveWriterTimeout(k,dir)

FrameKind(n, ty) ==
    \E k \in DOMAIN conn, dir \in Dirs :
        /\ Rcv(k, dir) = n
        /\ \E i \in FrameIdx(k, dir) : FrameAt(k, dir, i).m.type = ty /\ MCFrame(k, dir, i)

TransportFor(n) ==
    \/ \E k \in DOMAIN conn, dir \in Dirs :
          /\ Rcv(k, dir) = n
          /\ \E i \in FrameIdx(k, dir) :
                /\ (FrameAt(k, dir, i).m.type \in {"END", "PERR"} \/ StaleTarget(k, dir, i)
                    \/ PeerJoinRefused(n,FrameAt(k,dir,i).cid)
                    \/ EpSt(FrameAt(k, dir, i).cid, n) /= EpOpen)
                /\ MCFrame(k, dir, i)
    \/ MCUnblockSend(n)
    \/ \E m \in Server : MCDialAttempt(n, m)
    \/ \E c \in DOMAIN ckt : MCHaltOnDeadConn(n, c) \/ MCCancelPendingDial(n,c)

Kernel == \E k \in DOMAIN conn : \/ \E side \in {"d", "a"} : MCConnAbort(k, side)
                                \/ \E dir \in Dirs : MCAckFlush(k,dir)

BaseFairness ==
    /\ \A n \in Server :
        /\ \A ty \in {"RPV", "PV", "RV", "V", "AE", "AEA", "SNAP", "RSS", "TKT", "TKTRES"} :
              WF_mcVars(FrameKind(n, ty))
        /\ WF_mcVars((\E t \in DOMAIN clientTickets : FlushDataTicket(n, t) \/ DispatchStashedDataTicket(n, t))
                     /\ NoFc /\ BaseOnly)
        /\ WF_mcVars(MCStartupClockTick(n))
        /\ WF_mcVars(LiveElectionTimeout(n))
        /\ WF_mcVars(LiveLeaderHeartbeat(n))
        /\ WF_mcVars(MCPreVotePhase2Timeout(n))
        /\ WF_mcVars(MCRestartWindowTick(n))
        /\ WF_mcVars(MCRestartWindowElapsed(n))
        /\ WF_mcVars(MCRunDeferredCampaign(n))
        /\ WF_mcVars(MCResumeCampaign(n))
        /\ WF_mcVars(MCBecomeLeaderSave(n))
        /\ WF_mcVars(\E c \in DOMAIN ckt : MCHandleNewCircuit(n, c) \/ MCHandlePeerLeft(n, c))
        /\ WF_mcVars(TransportFor(n))
        /\ WF_mcVars(MCParIdentifyInterrupted(n))
        /\ WF_mcVars(MCParFinish(n))
        /\ WF_mcVars(\E m \in Server : MCParObserveQuery(n, m))
        /\ \A m \in Server : WF_mcVars(MCWatchdogSilence(n,m)) /\ WF_mcVars(MCDialAttempt(n,m))
        /\ WF_mcVars(MCUnblockSend(n))
        /\ WF_mcVars(\E m \in Server : MCParRecoverFromDonor(n, m))
        /\ WF_mcVars(MCDeadlineTick(n))
        /\ WF_mcVars(MCStickyExpire(n) \/ MCLeaderAge(n)
                     \/ \E key \in Keys : MCLeaseCreditExpire(n, key) \/ MCPongExpire(n, key) \/ MCAckAge(n,key))
    /\ WF_mcVars(Kernel)
    /\ \A k \in 1..(CktBase + MaxConn), dir \in Dirs :
          WF_mcVars(IF k \in DOMAIN conn THEN MCWriterStep(k,dir) ELSE FALSE)
    /\ \A k \in 1..(CktBase + MaxConn), dir \in Dirs :
          WF_mcVars(IF k \in DOMAIN conn THEN LiveWriterTimeout(k,dir) ELSE FALSE)

LiveSpec == MCInit /\ [][MCNextLive]_mcVars /\ BaseFairness

\* Member event loops, committed CAS requests and time receive fair service.
czMcVars == <<czVars, fc>>
CzFairness ==
    /\ \A m \in CzMembers : WF_czMcVars(MCCzClient(m))
    /\ WF_czMcVars(MCCzApply)
    /\ WF_czMcVars(CzTick /\ NoFc /\ CzOnly)

LiveSpecCzar == MCInit /\ [][MCCzNext]_mcVars /\ CzFairness

\* ========================================================================
\* LIVENESS PROPERTIES (modeling brief section 5)
\* ========================================================================
AllHealthy(S) == \A n \in S : up[n] /\ ~stopped[n] /\ \A m \in S : link[n][m] = "ok"
LeaderCommitting(S) ==
    \E l \in Server :
        /\ up[l] /\ ~stopped[l] /\ role[l] = Leader /\ noop0[l].done
        /\ Cardinality({v \in S \cap config[l].mem :
              up[v] /\ ~stopped[v] /\ link[l][v] = "ok" /\ link[v][l] = "ok"})
           >= Quorum(l)

\* A majority may be served by a leader outside that subset. The leader's
\* initial no-op establishes that it has committed in its own term.
\* Requiring a committing leader on every stable suffix covers recovery
\* after a formerly healthy leader crashed or lost connectivity.
HealthyMajority(S) ==
    /\ S /= {} /\ AllHealthy(S)
    /\ \A n \in S : S \subseteq config[n].mem /\ Cardinality(S) >= Quorum(n)

EventuallyLeaderWithHealthyMajority ==
    (<>[]HealthyMajority(LiveQ)) => []<>LeaderCommitting(LiveQ)

\* S9: after crashes stop and everything is up and connected, some leader commits
EventuallyLeaderAfterCrashes ==
    (<>[]AllHealthy(Server)) => []<>LeaderCommitting(Server)

PairHealthy ==
    /\ LiveA /= LiveB /\ AllHealthy({LiveA, LiveB})
    /\ LiveB \in config[LiveA].mem /\ LiveA \in config[LiveB].mem
ReplicaConnected(x, y) ==
    LET key == <<y, inc[y]>>
        c   == cktall[x][key]
    IN /\ c /= NoCkt /\ key \in cktReplica[x]
       /\ EpSt(c, x) = EpOpen /\ EpSt(c, y) = EpOpen

\* S7: two healthy replicas eventually keep open replica circuits both ways
PeerConnectivityRestored ==
    (<>[]PairHealthy) =>
        <>[](ReplicaConnected(LiveA, LiveB) /\ ReplicaConnected(LiveB, LiveA))

BlackHole(n, m) ==
    LET c == cktall[n][<<m, inc[m]>>] IN
    /\ c /= NoCkt /\ EpSt(c, n) = EpOpen
    /\ EpSt(c, m) \in {EpClosed, EpGone, "newdead"}

\* S7: n's current circuit to m does not stay dead at m's end
NoPersistentBlackHole ==
    (<>[]PairHealthy) => <>[](~BlackHole(LiveA, LiveB) /\ ~BlackHole(LiveB, LiveA))

\* S6: an accepted synchronous send and its remaining burst eventually
\* finish, excluding a process that is stopped forever. This property also
\* checks effects of non-deadlined controls or the extra-encryption writer.
SenderEventuallyUnblocked ==
    \A n \in Server : (<>[](~stopped[n])) =>
        ((up[n] /\ blk[n].pend /= <<>>) ~> (~up[n] \/ blk[n].pend = <<>>))

\* leaderPongQuorumTm counts current voter names and permits a self pong
\* only while the leader votes in that config. A live shadow cannot stand
\* in for a missing voter. A sole voting leader needs no remote pong.
LeaderDeaf(l) ==
    Cardinality({v \in config[l].mem : up[v] /\ ~stopped[v] /\
                    link[l][v] = "ok" /\ link[v][l] = "ok"}) < Quorum(l)
StaleLeaderStepsDown ==
    \A l \in Server :
        (<>[](up[l] /\ ~stopped[l] /\ LeaderDeaf(l))) => []<>(role[l] /= Leader)

\* S3: with everyone healthy, the leader's log eventually reaches every member
LogsCaughtUp ==
    \E l \in Server : up[l] /\ role[l] = Leader /\
        \A f \in config[l].mem : Ids(log[f]) = Ids(log[l])
CatchUpEventually ==
    \A l, f \in Server :
        (l /= f /\ <>[](AllHealthy({l, f}) /\ role[l] = Leader /\ f \in config[l].mem)) =>
          []<>(Ids(log[f]) = Ids(log[l]))

\* S8: a follower behind the leader's compaction base eventually installs a snapshot
NeedsSnap(f) == \E l \in Server : up[l] /\ role[l] = Leader /\ up[f] /\ f /= l
                                  /\ f \in config[l].mem /\ Len(log[f]) < base[l].idx
SnapshotEventuallyInstalled ==
    (<>[]AllHealthy(Server)) => \A f \in Server : NeedsSnap(f) ~> ~NeedsSnap(f)

\* S11: some live member eventually becomes czar
EventuallyCzar ==
    (<>[](\E m \in CzMembers : czUp[m])) => []<>CzSomeCzar

\* S6 diagnostic: at least one live replica can get past its current send.
NoTotalStall ==
    (\E n \in Server : up[n]) =>
        ~\A n \in Server : up[n] => WaitingForWrite(n)

\* ---------------- committed-data durability (added during bug hunting) ----
\* Check the committed WAL against the initial voting set under fixed
\* membership and fail-stop crashes. Crashed voters' durable logs count.
\* Explicit disk loss and administrative override require different assumptions.
CommittedOnQuorum ==
    \A e \in committed :
        LET holders == {n \in InitMembers : e.idx <= Len(log[n]) /\ log[n][e.idx].id = e.id}
        IN 2 * Cardinality(holders) > Cardinality(InitMembers)


\* Historical config name. All decoded stale targets now obey the same rule.
NoCollateralTeardownExceptD38 == NoCollateralTeardown

\* ---------------- filtered S3 diagnostic (added during bug hunting) -------
\* LcriSound minus the harmless case seen in MC_hunt_S3_diag SIM: an ack whose
\* LCRI exceeds the stale AE's log end only up to the follower's own
\* compaction base (entries <= base are committed, so every current leader
\* holds them).  Still flags D-6 proper: an LCRI above both the true common
\* prefix and the follower's base (e.g. the leader's base inside the
\* follower's divergent tail).
LcriSoundExceptBase ==
    \A m \in QueuedMessages : m.type = "AEA" => m.gx <= Max(m.gt, base[m.from[1]].idx)

\* ---------------- filtered S4 diagnostic (added during bug hunting) -------
\* Narrower variants retained for the original targeted configurations.
QuorumCountsDistinctVotersExceptShadowLease ==
    \A l \in Server : up[l] /\ role[l] = Leader =>
        /\ DistinctVoters(l,CommitCounted(l))
        /\ Cardinality(LeaseCounted(l)) = Cardinality({KName(k) : k \in LeaseCounted(l)})
QuorumCountsNoDuplicateReplica ==
    \A l \in Server : up[l] /\ role[l] = Leader =>
        \A S \in {CommitCounted(l),LeaseCounted(l)} : Cardinality(S) = Cardinality({KName(k) : k \in S})

\* ---------------- premise checks for vacuity (use as INVARIANT) ---------
NeverHealthyMajorityLeader == ~(HealthyMajority(LiveQ) /\ LeaderCommitting(LiveQ))
NeverBlocked == \A n \in Server : blk[n].pend = <<>>
NeverNeedsSnap == \A f \in Server : ~NeedsSnap(f)
NeverCzar == ~CzSomeCzar
\* Checking one of these as an invariant asks TLC for a path to that
\* situation. Its reachability depends on the profile and its bounds.
NeverFencedWhileHealthy == ~(AllHealthy(Server) /\ \E n \in Server : up[n] /\ par[n] = "unknown")
NeverDeafLeader == ~\E l \in Server : up[l] /\ role[l] = Leader /\ LeaderDeaf(l)
NeverLaggingWhileHealthy ==
    ~(AllHealthy(Server) /\ (\E l \in Server : up[l] /\ role[l] = Leader) /\ ~LogsCaughtUp)
NeverPairDisconnected ==
    ~(PairHealthy /\ ~(ReplicaConnected(LiveA, LiveB) /\ ReplicaConnected(LiveB, LiveA)))
NeverBlackHole == ~(PairHealthy /\ (BlackHole(LiveA, LiveB) \/ BlackHole(LiveB, LiveA)))
NeverBlockedWithHealthyMajority == ~(HealthyMajority(LiveQ) /\ \E n \in LiveQ : blk[n].pend /= <<>>)

\* ========================================================================
\* cfg helpers for function-valued constants (cfg files cannot write them)
\* ========================================================================
KVR0 == CHOOSE r \in KVReplicas : TRUE
\* one replica's wall clock runs one tick ahead of the other (bounded skew)
MCKVOffSkew == [r \in KVReplicas |-> IF r = KVR0 THEN 0 ELSE 1]
MCKVOffZero == [r \in KVReplicas |-> 0]
\* every replica runs with the same ClockDriftBound ...
MCKVDriftSame == [r \in KVReplicas |-> 1]
\* ... or members are configured differently (D-53)
MCKVDriftMixed == [r \in KVReplicas |-> IF r = KVR0 THEN 1 ELSE 0]

\* ========================================================================
\* VIEW / constraints
\* ========================================================================
\* Historical projection used by some Specular safety configurations.
\* It merges states with different remaining budgets. Omit VIEW to explore
\* the full bounded state graph, as tube.cfg does.
MCView == <<coreVars, kvVars, czVars, hermes>>

=============================================================================
