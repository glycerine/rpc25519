-------------------------------- MODULE tube --------------------------------
\* Tube protocol model from Specular run 20261122-010209-d9f6.
\* Go source baseline: glycerine/rpc25519 at a66e9b9c. File and line
\* references, scenario numbers (S), and finding IDs (D) refer to that run.
\*
\* tube.cfg selects MCSpec for a bounded Raft/transport safety check.
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
    SendBufCap,       \* K: frames a connection direction holds before a sender blocks
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
    blk          \* blk[n]: event-loop blocking state, see BlkRec

\* --- Tube connection views (all volatile, per process) ---------------------
VARIABLES
    cktall,      \* cktall[n][key] \in circuit ids \cup {NoCkt}   (s.cktall map[PeerID])
    byName,      \* byName[n][m] \in circuit ids \cup {NoCkt, Pending} (s.cktAllByName)
    pendAddr,    \* pendAddr[n][m]: address stored in a pending cktPlus (PeerBaseServerAddr)
    wdReq,       \* wdReq[n][m]: watchdog pulse pending (requestReconnect set, one attempt)
    cktReplica,  \* cktReplica[n] \subseteq Keys (s.cktReplica map[PeerID])
    audit        \* audit[n]: circuits registered by handleNewCircuit (s.cktAuditByCID)

\* --- Raft volatile state ----------------------------------------------------
VARIABLES
    role,            \* Follower / Candidate / Leader
    currentTerm,     \* s.state.CurrentTerm (in memory)
    votedFor,        \* s.state.VotedFor (a key) or Nil
    leaderKey,       \* s.leaderID (key) or Nil
    sticky,          \* lastLegitAppendEntries within MinElection+ClockDriftBound (abstract time)
    pvTerm,          \* s.preVoteTerm (phase 1), 0 if none
    pv2Term,         \* s.preVoteOkLeaderElecTerm (phase-2 timer armed), 0 if none
    pvYes, pvNo,     \* pre-votes recorded by voter name (s.preVotes)
    vYes, vNo,       \* votes recorded by voter name (s.votes)
    peers,           \* peers[n][key]: RaftNodeInfo [mi, lcri, credit] or Nil (s.peers)
    pong,            \* pong[n] = [all, fresh]: leaderFullPongPQ entries by PeerID
    leaderAged,      \* leader for at least followerDownDur (abstract time)
    noop0,           \* noop0[n] = [id, done]: initialNoop0Tkt / initialNoop0HasCommitted
    stallQ,          \* stalledMembershipConfigChangeTkt (not cleared on step-down)
    config,          \* s.state.MC
    shadows,         \* s.state.ShadowReplicas names
    commitIndex,     \* s.state.CommitIndex
    lastApplied,     \* s.state.LastApplied (durable with the KV checkpoint)
    lastAppliedTerm, \* s.state.LastAppliedTerm
    snapSlot         \* multipart snapshot reassembly slot (one per node) or Nil

\* --- WAL and PAR -------------------------------------------------------------
VARIABLES
    log,         \* log[n]: Seq of [term, id, kind]; FULL history (entries <= base are compacted:
                 \* not visible to the code, kept for the invariants; after a snapshot install
                 \* the prefix is the leader's history)
    base,        \* base[n] = [idx, term]: CompactionDiscardedLast (wal BaseC/CompactTerm)
    idLen,       \* PAR sidecar: number of entries with published identifiers
    torn,        \* torn[n]: entry whose identifier was not published at a crash (PAR), or Nil
    par          \* par[n] \in {"ok","unknown"}

\* --- durable RaftState (persistor.go save: temp file + fsync + rename) ---------
VARIABLES
    dTerm, dVote, dCfg, dShadows, dCommit,
    walGone      \* WAL and .parlog missing while RaftState exists (openPARWAL,
                 \* par.go:2016-2034; scanParWAL 1390-1410): set by the "wal"
                 \* storage fault, cleared when a PAR snapshot install recreates
                 \* both files (parPrepareSnapshotWAL, tube.go:17792-17797)

\* --- ghosts --------------------------------------------------------------------
VARIABLES
    nextId,      \* fresh log entry ids
    committed,   \* entries a leader declared committed (coded commit rule): [idx, term, id]
    applied,     \* applied[n]: Seq of [id, term] applied to the state machine
    leaders,     \* history of becomeLeader: [term, name, inc]
    ackedCfg,    \* membership changes acknowledged to the client: [ct, cv, op, x]
    cfgHist,     \* every installed config: [ct, cv, mem]
    panics,      \* reachable panic sites (strings)
    ackHigh,     \* ackHigh[n] = [term, claims]: entries n acked (accept) in that term
    nextSid      \* fresh snapshot ids (whole-payload checksum in the code)

envVars   == <<up, inc, addr, stopped, link>>
netVars   == <<conn, ckt, nextConn, nextCkt, blk>>
viewVars  == <<cktall, byName, pendAddr, wdReq, cktReplica, audit>>
raftVars  == <<role, currentTerm, votedFor, leaderKey, sticky, pvTerm, pv2Term,
               pvYes, pvNo, vYes, vNo, peers, pong, leaderAged, noop0, stallQ,
               config, shadows, commitIndex, lastApplied, lastAppliedTerm, snapSlot>>
logVars   == <<log, base, idLen, torn, par>>
durVars   == <<dTerm, dVote, dCfg, dShadows, dCommit, walGone>>
ghostVars == <<nextId, committed, applied, leaders, ackedCfg, cfgHist, panics, ackHigh, nextSid>>

coreVars == <<envVars, netVars, viewVars, raftVars, logVars, durVars, ghostVars>>

\* ========================================================================
\* RECORD SHAPES (documentation; used by TypeOK)
\* ========================================================================

\* PeerDetail in MemberConfig.PeerNames: PeerID (incarnation, -1 = unknown /
\* "pending" URL) and Addr.
Det == [inc: Incs \cup {-1}, addr: Addrs]

\* MemberConfig (tube/tube.go:13651-13709): version (ConfigTerm, ConfigVersion),
\* members with details, IsCommitted, CommitIndex.
CfgRec == [mem: SUBSET Server, ct: Nat, cv: Nat, com: BOOLEAN, ci: Nat,
           det: [Server -> Det \cup {Nil}]]

\* RaftNodeInfo kept by a leader per PeerID (tube/tube.go:9952-9996)
PeerRec == [mi: Nat, lcri: Nat, credit: BOOLEAN]

\* ========================================================================
\* MEMBER CONFIG HELPERS
\* ========================================================================

\* tube/tube.go:13769-13792: versions compare (ConfigTerm, ConfigVersion) only.
VersionGT(a, b)  == a.ct > b.ct \/ (a.ct = b.ct /\ a.cv > b.cv)
VersionEq(a, b)  == a.ct = b.ct /\ a.cv = b.cv
VersionGTE(a, b) == VersionGT(a, b) \/ VersionEq(a, b)

\* MemberConfig.majority(): 0 for an empty config (tube.go, majority())
CfgMajority(c) == IF c.mem = {} THEN 0 ELSE Cardinality(c.mem) \div 2 + 1

\* s.clusterSize() / s.quorum() (tube/tube.go:6904-6931); NoFaultTolDur is
\* not modeled (brief section 3.2).
ClusterSize(n) == Cardinality(config[n].mem)
Quorum(n) == ClusterSize(n) \div 2 + 1

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

\* last PHYSICAL entry of the WAL (getRaftLogSummary, tube/tube.go:9648-9657):
\* index 0 / term 0 when no entry is left past the compaction base (e.g.
\* right after a snapshot install).  The code uses it for an ack's
\* PeerLogLastIndex/Term (8072-8075, 8642-8672) and for the leader's
\* catch-up checks (8760, 9998).
PhysLastIdx(lg, bs)  == IF Len(lg) > bs.idx THEN Len(lg) ELSE 0
PhysLastTerm(lg, bs) == IF Len(lg) > bs.idx THEN lg[Len(lg)].term ELSE 0

\* term at index i as the code can see it (i >= base.idx)
TermAtL(lg, bs, i) == IF i = 0 THEN 0 ELSE IF i = bs.idx THEN bs.term ELSE lg[i].term

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

\* TermsRLE.Extends (tube/rle.go:443-745), branch by branch.  Note 641:
\* beginOverlap = max(f, m)+1, i.e. it "assume[s] agreement up to any
\* compacted index" (609-614): the root of D-6.
Extends(L, F) ==
    IF F.b = 0 /\ Len(F.t) = 0 THEN ERes(TRUE, 0, FALSE)             \* 460-463
    ELSE IF Len(L.t) = 0 THEN                                        \* 472
        IF L.b = 0 THEN ERes(FALSE, 0, FALSE)                          \* 473-476
        ELSE IF F.b = L.b THEN
             (IF Len(F.t) = 0 THEN ERes(TRUE, L.b, FALSE)              \* 479-482
              ELSE ERes(FALSE, L.b, FALSE))                            \* 485
        ELSE IF F.b > L.b THEN ERes(FALSE, L.b, FALSE)                 \* 489-492
        ELSE ERes(FALSE, F.b, TRUE)                                    \* 502 NEED_LEADER_SNAPSHOT
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
\* logsAreMismatched (tube/tube.go:7557-7765) over a follower log (lg, bs).
\* The getTermIndexStartingAt iterator is modeled with its intended
\* semantics; its off-by-one (D-10, rle.go:1004-1036) is a code-review item.
\* ------------------------------------------------------------------------
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
      IF prevTerm = ae.prevTerm THEN
          \* 7717-7734: production-only fine-grained check of the follower's
          \* later entries against the AE: a later term conflict REJECTS
          \* instead of truncating (D-7).
          IF lli >= ae.prev + 1 THEN
              LET hi  == Min(lli, ae.prev + nes)
                  bad == {f \in (ae.prev + 1)..hi :
                            lg[f].term /= ae.ents[f - ae.prev].term}
              IN IF bad /= {} THEN
                    LET f == MinSet(bad) IN
                    LAMRes(TRUE, ae.ents[f - ae.prev].term, f)          \* 7727-7729
                 ELSE LAMRes(FALSE, -1, -1)                             \* 7731-7733
          ELSE LAMRes(FALSE, -1, -1)                                    \* 7736
      ELSE
          \* 7745-7764: conflict at PrevLogIndex; report first index of that term
          LET same == {i \in 1..ae.prev : i > baseC /\ lg[i].term = prevTerm /\
                          \A j \in i..ae.prev : j > baseC /\ lg[j].term = prevTerm}
          IN LAMRes(TRUE, prevTerm, IF same = {} THEN ae.prev ELSE MinSet(same))

\* ========================================================================
\* TRANSPORT (rpc25519) HELPERS
\* ========================================================================
\*
\* Connection record conn[k]: an auto-cli TCP connection from dialer d
\* (incarnation di) to acceptor a (incarnation ai) at address ad.  dOpen/aOpen:
\* that side's socket/read loop alive.  q[dir]: frames written by the sender
\* and not yet read by the receiving process; u[dir]: frames read by the
\* receiver whose TCP ACKs have not reached the sender (reverse direction
\* dropped).  A sender blocks when Len(q)+u reaches SendBufCap (kernel send
\* buffer + receive window): sends are hand-offs on an UNBUFFERED channel to
\* the connection's send loop, which writes with no deadline
\* (srv.go:2530-2547, srv.go:1975, cli.go:1914, srv.go:594, common.go:436-449).
\*
\* Circuit record ckt[c]: dialer d/di (its BaseServerAddr dd), acceptor a/ai
\* at address ad, original connection k, endpoint states dSt/aSt, and the
\* cktPlus "seen" report at each endpoint (dSeen/aSeen = [ct, cv, term] or Nil,
\* tube/watchdog.go:21-75).
\*
\* Frames are routed per message BY ADDRESS (srv.go:2047-2058, 2287-2366):
\* a dialer-side endpoint uses its process's current live auto-cli connection
\* to the acceptor address, which may be a newer connection than the one the
\* circuit was created on; an acceptor-side endpoint replies on the
\* connection keyed by the peer's ephemeral client address (its original
\* connection).  Per circuit, frames are FIFO; across circuits there is no
\* order (separate per-circuit reader goroutines, tube/tube.go:2275-2387).

Dirs == {"da", "ad"}

EpSt(c, n) == IF ckt[c].d = n /\ ckt[c].di = inc[n] THEN ckt[c].dSt
              ELSE IF ckt[c].a = n /\ ckt[c].ai = inc[n] THEN ckt[c].aSt
              ELSE EpGone

SetEp(cm, c, n, st) ==
    [cm EXCEPT ![c] = IF cm[c].d = n THEN [@ EXCEPT !.dSt = st]
                                     ELSE [@ EXCEPT !.aSt = st]]

\* PeerID (key) of the remote end of circuit c as seen from n
PeerKeyOf(c, n) == IF ckt[c].d = n THEN <<ckt[c].a, ckt[c].ai>>
                   ELSE <<ckt[c].d, ckt[c].di>>
PeerNameOf(c, n) == KName(PeerKeyOf(c, n))
\* cktP.PeerBaseServerAddr = ckt.RpbTo.BaseServerAddr (tube/tube.go:16742)
PeerAddrOf(c, n) == IF ckt[c].d = n THEN ckt[c].ad ELSE ckt[c].dd

\* Live auto-cli connections of n's current process to address a of m
DialerConns(cn, n, m, a) ==
    {k \in DOMAIN cn : cn[k].d = n /\ cn[k].di = inc[n] /\ cn[k].a = m
                       /\ cn[k].ad = a /\ cn[k].dOpen}

\* Connection a send from n on circuit c uses, or 0 when the transport
\* finds no connection and dials (srv.go:2287-2366).
RouteOf(cn, n, c) ==
    IF ckt[c].d = n
    THEN LET ks == DialerConns(cn, n, ckt[c].a, ckt[c].ad)
         IN IF ks = {} THEN 0 ELSE MinSet(ks)
    ELSE IF cn[ckt[c].k].aOpen THEN ckt[c].k ELSE 0

SendDir(cn, k, n) == IF cn[k].d = n /\ cn[k].di = inc[n] THEN "da" ELSE "ad"
Occ(cn, k, dir) == Len(cn[k].q[dir]) + cn[k].u[dir]

\* The receiving process absorbs frames as fast as they arrive: its socket
\* is open, it runs, both directions carry data/ACKs, and its event loop is
\* not itself stalled.  The per-connection read loop and the pump buffer
\* frames per circuit (ckt.Reads, capacity 100; pump.go:254-276) while the
\* Tube event loop catches up, so a sender to an absorbing peer does not
\* block.  Otherwise frames accumulate in the kernel buffers (SendBufCap)
\* and the sender's hand-off blocks (D-28), including toward a peer whose
\* own event loop is stalled (the stall cascade, D-32).
Absorbing(cn, k, dir) ==
    LET r  == IF dir = "da" THEN cn[k].a ELSE cn[k].d
        sd == IF dir = "da" THEN cn[k].d ELSE cn[k].a
    IN /\ IF dir = "da" THEN cn[k].aOpen ELSE cn[k].dOpen
       /\ up[r] /\ ~stopped[r] /\ link[sd][r] = "ok" /\ link[r][sd] = "ok"
       /\ blk[r].pend = <<>> /\ blk[r].dial = Nil

\* Blocking state of an event loop: frames stuck behind a full connection
\* (BlockedSend, D-28), a synchronous auto-cli dial in progress inside a
\* send (BlockedDial, D-29), and the becomeLeader save still to run.
NoBlk == [pend |-> <<>>, dial |-> Nil, save |-> FALSE]

\* One SendOneWay(ckt, frag, -1, ...) from n on circuit c (tube/tube.go:16610-16625
\* -> ckt.go:685-776 -> srv.go:2287-2547).  st = [cn, bl].
SendOne(st, n, c, msg) ==
    IF c = NoCkt \/ c = Pending THEN st
    \* ckt.go:685-694: a halted circuit returns ErrContextCancelledCkt at once
    ELSE IF EpSt(c, n) /= EpOpen THEN st
    ELSE
      LET k == RouteOf(st.cn, n, c)
          f == [cid |-> c, to |-> PeerKeyOf(c, n), m |-> msg]
      IN IF k = 0
         \* srv.go:2321-2366: no connection for the address -> auto-cli dial
         \* inline with ConnectTimeout 0 (D-29); the event loop blocks.
         THEN [st EXCEPT !.bl.dial = IF @ = Nil THEN [c |-> c, f |-> f] ELSE @]
         ELSE LET dir == SendDir(st.cn, k, n) IN
              IF \/ \E j \in 1..Len(st.bl.pend) : st.bl.pend[j].k = k
                 \/ (Occ(st.cn, k, dir) >= SendBufCap /\ ~Absorbing(st.cn, k, dir))
              \* srv.go:2539-2547: blocking select on the unbuffered sendCh;
              \* only the pair halt or the LocalPeer ctx escape (D-28).
              THEN [st EXCEPT !.bl.pend = Append(@, [k |-> k, dir |-> dir, f |-> f])]
              ELSE [st EXCEPT !.cn[k].q[dir] = Append(@, f)]

RECURSIVE SendSeq(_, _, _)
SendSeq(st, n, lst) ==
    IF lst = <<>> THEN st
    ELSE SendSeq(SendOne(st, n, Head(lst).c, Head(lst).m), n, Tail(lst))

\* Close-notification frames (pump.go:40-110 cleanupCkt with notifyPeer) are
\* sent in the background (errWriteDur -2); they never block the event loop
\* and are dropped when the route is gone or full.
BgSendOne(cn, n, c, msg) ==
    LET k == RouteOf(cn, n, c) IN
    IF k = 0 THEN cn
    ELSE LET dir == SendDir(cn, k, n) IN
         IF Occ(cn, k, dir) >= SendBufCap THEN cn
         ELSE [cn EXCEPT ![k].q[dir] = Append(@, [cid |-> c, to |-> PeerKeyOf(c, n), m |-> msg])]

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

\* ToPeerID of the frame is not the receiving process (stale incarnation
\* routed by address onto a connection to a newer process, D-38).
StaleTarget(k, dir, i) == FrameAt(k, dir, i).to /= <<Rcv(k, dir), inc[Rcv(k, dir)]>>

\* frame i is a Tube message for an endpoint Tube has registered
TubeFrame(k, dir, i) ==
    /\ CanRead(k, dir, i)
    /\ ~StaleTarget(k, dir, i)
    /\ EpSt(FrameAt(k, dir, i).cid, Rcv(k, dir)) = EpOpen

\* ========================================================================
\* NODE-STATE RECORD
\* Handlers are composed of several Go functions (becomeFollower, setMC,
\* connectToMC, commitWhatWeCan, ...).  They are modeled as operators on a
\* record holding one node's volatile + WAL state, written back at the end
\* of the action (WriteBack).  `out` collects frames to send (in order),
\* `closes` circuit closes, `save` whether saver.save ran.
\* ========================================================================

FreshPeer == [mi |-> 0, lcri |-> 0, credit |-> FALSE]

NS(n) ==
    [ role |-> role[n], term |-> currentTerm[n], vote |-> votedFor[n],
      leader |-> leaderKey[n], sticky |-> sticky[n],
      pvTerm |-> pvTerm[n], pv2Term |-> pv2Term[n], pvYes |-> pvYes[n], pvNo |-> pvNo[n],
      vYes |-> vYes[n], vNo |-> vNo[n], peers |-> peers[n], pong |-> pong[n],
      aged |-> leaderAged[n], noop0 |-> noop0[n], stallQ |-> stallQ[n],
      cfg |-> config[n], sh |-> shadows[n], ci |-> commitIndex[n],
      la |-> lastApplied[n], lat |-> lastAppliedTerm[n], slot |-> snapSlot[n],
      log |-> log[n], base |-> base[n], idLen |-> idLen[n],
      ca |-> cktall[n], bn |-> byName[n], pa |-> pendAddr[n], wd |-> wdReq[n],
      rep |-> cktReplica[n], aud |-> audit[n],
      appl |-> applied[n], ackH |-> ackHigh[n], par |-> par[n],
      seenOv |-> {},                   \* cktPlus.seen updates made in this step
      \* accumulators
      out |-> <<>>, closes |-> <<>>, save |-> FALSE, save2 |-> FALSE,
      newCommitted |-> {}, newPanics |-> {}, newCfgHist |-> {},
      newAcked |-> {}, newLeaders |-> {}, nid |-> nextId, sid |-> nextSid ]

AddOut(s, c, msg) == [s EXCEPT !.out = Append(@, [c |-> c, m |-> msg])]

RECURSIVE AddOutSet(_, _)
\* broadcast: one frame per element of S (S: set of [c, m]); deterministic order
AddOutSet(s, S) ==
    IF S = {} THEN s
    ELSE LET x == CHOOSE x \in S : TRUE IN AddOutSet(AddOut(s, x.c, x.m), S \ {x})

Panic(s, why) == [s EXCEPT !.newPanics = @ \cup {why}]

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
          !.bn    = IF s.bn[m] = c THEN [@ EXCEPT ![m] = NoCkt] ELSE @]

\* ------------------------------------------------------------------------
\* addToCktall(ckt) (tube/tube.go:16629-16836) for circuit c at n: newest
\* circuit per PeerID wins; the old one is removed from the maps
\* (deleteFromCktAll) but NOT closed (16666-16717); cktAllByName is
\* overwritten (16759); the new cktPlus has no reported MC yet
\* (newCktPlus + seen(nil, ...), 16748-16750); for MC members the MC
\* PeerDetail is updated without a version change (16795-16814), cktReplica
\* gets the PeerID (16816-16817) and s.peers a FRESH RaftNodeInfo
\* (MI = LCRI = 0, 16832; D-4).  Callers: handleNewCircuit and peerJoin.
\* ------------------------------------------------------------------------
AddToCktall(s, n, c) ==
    LET key == PeerKeyOf(c, n)
        m   == KName(key)
        old == s.ca[key]
        s1  == IF old /= NoCkt THEN DeleteFromCktAll(s, key, old) ELSE s
        s2  == [s1 EXCEPT !.ca[key] = c, !.bn[m] = c,
                          !.seenOv = {x \in @ : x.c /= c} \cup {[c |-> c, v |-> Nil]}]
    IN IF m \in s2.cfg.mem
       THEN [s2 EXCEPT !.cfg.det[m] = [inc |-> KInc(key), addr |-> PeerAddrOf(c, n)],
                       !.rep = @ \cup {key},
                       !.peers[key] = FreshPeer]
       ELSE s2

\* ------------------------------------------------------------------------
\* connectInBackgroundIfNoCircuitTo (tube/tube.go:15322-15400) +
\* backgroundConnectToPeer (15423-15450): create a pending cktPlus with a
\* watchdog if the name is absent from cktAllByName, then pulse that
\* cktPlus's watchdog once (one reconnect attempt per pulse,
\* tube/watchdog.go:225-246).  The dial address comes from the MC
\* PeerDetail at creation; an existing entry's stored address wins
\* (15365-15368).  Names outside the MC (e.g. shadows) get an empty address,
\* so their attempt fails ("skip reconnect with empty addr", watchdog.go:358).
\* ------------------------------------------------------------------------
CIB(s, n, m) ==
    IF m = n THEN s
    ELSE LET d == s.cfg.det[m]
             detAddr == IF m \in s.cfg.mem /\ d /= Nil THEN d.addr ELSE "none"
         IN IF s.bn[m] = NoCkt
            THEN [s EXCEPT !.bn[m] = Pending, !.pa[m] = detAddr, !.wd[m] = detAddr]
            ELSE LET a == IF s.bn[m] = Pending THEN s.pa[m] ELSE PeerAddrOf(s.bn[m], n)
                 IN [s EXCEPT !.wd[m] = a]

RECURSIVE CIBSet(_, _, _)
CIBSet(s, n, S) ==
    IF S = {} THEN s
    ELSE LET m == CHOOSE m \in S : TRUE IN CIBSet(CIB(s, n, m), n, S \ {m})

\* connectToMC (tube/tube.go:15226-15307): dial MC members and shadows that
\* are absent from cktAllByName (a pending entry is skipped forever, D-35).
\* Its deferred resubmitStalledTickets(true) (15228-15230) is applied by
\* the caller through ResubmitStalledMC.
ConnectToMC(s, n) ==
    LET want == {m \in (s.cfg.mem \cup s.sh) \ {n} : s.bn[m] = NoCkt}
    IN IF s.cfg.mem = {} THEN s ELSE CIBSet(s, n, want)

\* ------------------------------------------------------------------------
\* adjustCktReplicaForNewMembership (tube/tube.go:15526-15605).
\* Keeps cktReplica entries whose name is a member or shadow; then for each
\* member/shadow name not kept, adds cktall[det.PeerID] if present, else
\* calls connectInBackgroundIfNoCircuitTo.  Members with an unknown PeerID
\* are skipped (15582-15584).  Shadow PeerDetails are not tracked by the
\* model: a shadow counts as known through any cktall entry with its name.
\* ------------------------------------------------------------------------
AdjustCktReplica(s, n) ==
    LET grp   == (s.cfg.mem \cup s.sh) \ {n}
        keep  == {k \in s.rep : KName(k) \in grp}
        kept  == {KName(k) : k \in keep}
        todo  == grp \ kept
        memKey(m) == IF s.cfg.det[m] = Nil \/ s.cfg.det[m].inc = -1 THEN Nil
                     ELSE <<m, s.cfg.det[m].inc>>
        shKeys(m) == {k \in Keys : KName(k) = m /\ s.ca[k] /= NoCkt}
        addMem == {memKey(m) : m \in {x \in todo \cap s.cfg.mem :
                       memKey(x) /= Nil /\ s.ca[memKey(x)] /= NoCkt}}
        addSh  == UNION {shKeys(m) : m \in todo \ s.cfg.mem}
        dial   == {x \in todo \cap s.cfg.mem : memKey(x) /= Nil /\ s.ca[memKey(x)] = NoCkt}
                  \cup {x \in todo \ s.cfg.mem : shKeys(x) = {}}
    IN CIBSet([s EXCEPT !.rep = keep \cup addMem \cup addSh], n, dial)

\* putRemovedReplicasIntoShadows (tube/tube.go:14876-14896)
ShadowsAfter(sh, old, new) == (sh \cup (old.mem \ new.mem)) \ (new.mem \ old.mem)

\* setMC (tube/tube.go:14825-14871): ignore if not strictly newer; else
\* removed members become shadows, install, save (14865), adjust cktReplica.
SetMC(s, n, newCfg) ==
    IF VersionGTE(s.cfg, newCfg) THEN s
    ELSE LET s1 == [s EXCEPT !.sh = ShadowsAfter(@, s.cfg, newCfg),
                             !.cfg = newCfg,
                             !.save = TRUE,
                             !.newCfgHist = @ \cup {[ct |-> newCfg.ct, cv |-> newCfg.cv, mem |-> newCfg.mem]}]
         IN AdjustCktReplica(s1, n)

\* resetPreVoteState (tube/tube.go): clears phase 1 and phase 2 state.
ResetPreVote(s) == [s EXCEPT !.pvTerm = 0, !.pv2Term = 0, !.pvYes = {}, !.pvNo = {}]

\* becomeFollower(term, mc, save) (tube/tube.go:6572-6665)
BecomeFollower(s, n, t, mc, doSave) ==
    LET mcUpd == mc /= Nil /\ VersionGT(mc, s.cfg)                       \* 6599-6604
        s1 == [s EXCEPT !.role = Follower,                               \* 6583
                        !.noop0 = [id |-> 0, done |-> FALSE]]            \* 6595-6596
        s2 == IF mcUpd THEN SetMC(s1, n, mc) ELSE s1                     \* 6605-6607
        s3 == IF t > s.term \/ mcUpd                                     \* 6609-6619
              THEN [s2 EXCEPT !.term = t, !.vote = Nil, !.leader = Nil]
              ELSE s2
        s4 == ResetPreVote(s3)                                           \* 6621
    IN IF doSave THEN [s4 EXCEPT !.save = TRUE] ELSE s4                  \* 6625-6627

\* ========================================================================
\* APPLY AND COMMIT
\* ========================================================================

\* commitWhatWeCan apply loop (tube/tube.go:10408-10669), one entry at a time.
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
                   IN ApplyFrom(s2, onLeader)

\* commitWhatWeCan (tube/tube.go:10349-10690).  The deferred
\* resubmitStalledTickets(false) after noop0 (10536) is not modeled: stalled
\* membership tickets are retried by seen()/ack processing instead.
CommitWhatWeCan(s, onLeader) ==
    IF s.par = "unknown" THEN s                                          \* 10350-10352 parBlocked
    ELSE LET s1 == ApplyFrom(s, onLeader)
             \* 10672-10678: MC.CommitIndex follows CommitIndex
             s2 == IF s1.cfg.ci < s1.ci THEN [s1 EXCEPT !.cfg.ci = s1.ci] ELSE s1
         IN IF s2.la > s.la THEN [s2 EXCEPT !.save = TRUE] ELSE s2       \* 10679-10685

\* leaderAdvanceCommitIndex (tube/tube.go:10186-10343).
\* Counts the leader's own lastLogIndex UNCONDITIONALLY (10247) plus the
\* MatchIndex of every s.peers record (keyed by PeerID, no de-dup by name)
\* whose PeerName is in the MC (10250-10266); quorum of the MC (10269);
\* the entry must be from the current term (10322-10330).
CountGE(s, n, v) ==
    (IF Len(s.log) >= v THEN 1 ELSE 0) +
    Cardinality({k \in Keys : s.peers[k] /= Nil /\ KName(k) \in s.cfg.mem /\ s.peers[k].mi >= v})

LeaderAdvanceCommitIndex(s, n) ==
    LET quor  == Cardinality(s.cfg.mem) \div 2 + 1
        total == 1 + Cardinality({k \in Keys : s.peers[k] /= Nil /\ KName(k) \in s.cfg.mem})
        cands == {v \in 0..Len(s.log) : CountGE(s, n, v) >= quor}
        newCI == IF cands = {} THEN 0 ELSE MaxSet(cands)
    IN IF quor - 1 >= total THEN s                                       \* 10277-10280
       ELSE IF newCI = 0 THEN s                                          \* 10293-10296
       ELSE IF newCI <= s.ci THEN s                                      \* 10304-10314
       ELSE IF newCI > Len(s.log) THEN s                                 \* 10316-10319
       ELSE IF newCI <= s.base.idx \/ s.log[newCI].term /= s.term THEN s  \* 10322-10330
       ELSE LET s1 == [s EXCEPT !.ci = newCI,                            \* 10337-10338
                         !.newCommitted = @ \cup
                            {[idx |-> i, term |-> s.log[i].term, id |-> s.log[i].id] : i \in (s.ci + 1)..newCI}]
            IN CommitWhatWeCan(s1, TRUE)                                 \* 10341

\* ========================================================================
\* APPEND-ENTRIES CONSTRUCTION AND SENDING (leader side)
\* ========================================================================

EntsFrom(lg, b, e) ==
    [j \in 1..(e - b + 1) |-> [idx |-> b + j - 1, term |-> lg[b + j - 1].term,
                               id |-> lg[b + j - 1].id, kind |-> lg[b + j - 1].kind]]

\* newAE (tube/tube.go:8903-8948): term, LLI/LLT, commit, TermsRLE, MC,
\* ShadowReplicas.  (PeerID2LastHeard only feeds the dead watchdog check.)
AEMsg(s, n, prev, prevTerm, ents) ==
    [type |-> "AE", from |-> <<n, inc[n]>>, term |-> s.term, commit |-> s.ci,
     lli |-> Len(s.log), llt |-> LastTermL(s.log, s.base),
     prev |-> prev, prevTerm |-> prevTerm, ents |-> ents,
     rle |-> RLEOf(s.log, s.base), cfg |-> s.cfg, sh |-> s.sh,
     gids |-> Ids(s.log)]                     \* ghost: leader entry ids (for LcriSound)

\* skipAEBecauseNotReplica (tube/tube.go:9661-9690): only members and shadows
SkipNotReplica(s, m) == m \notin (s.cfg.mem \cup s.sh)

\* sendAppendEntriesTo(followerID, ..., beginIndex) (tube/tube.go:9696-9845)
SendAppendEntriesTo(s, n, key, beginIndex) ==
    IF s.par = "unknown" THEN s                                          \* 9697-9699
    ELSE IF SkipNotReplica(s, KName(key)) THEN s                         \* 9701-9704
    ELSE IF beginIndex < 1 THEN s                                        \* 9705-9708
    ELSE IF Len(s.log) = 0 \/ beginIndex > Len(s.log) THEN s             \* 9712-9715
    ELSE LET b    == Max(beginIndex, s.base.idx + 1)                     \* 9726-9732
             e    == Min(Len(s.log), b + AEBatchCap - 1)                 \* 9752-9768 cap
             ents == IF b > Len(s.log) THEN <<>> ELSE EntsFrom(s.log, b, e)
             prev == IF b > 1 /\ b <= Len(s.log) THEN b - 1 ELSE 0        \* 9770-9797
             prvT == IF prev = 0 THEN 0 ELSE TermAtL(s.log, s.base, prev)
             c    == s.ca[key]                                           \* 9826-9830
         IN IF c = NoCkt THEN s
            ELSE AddOut(s, c, AEMsg(s, n, prev, prvT, ents))             \* 9837

\* sendAppendEntriesEmptyHeartbeat (tube/tube.go:8974-9012); no MC filter
SendHeartbeat(s, n, key) ==
    IF s.par = "unknown" \/ KName(key) = n THEN s                        \* 8975-8981
    ELSE AddOut(s, s.ca[key], AEMsg(s, n, 0, 0, <<>>))                   \* 8983-9002

\* bcastAppendEntries (tube/tube.go:9046-9154): to every s.peers record
\* (except self) whose name is a member or shadow and that has a cktall entry.
BcastAppendEntries(s, n, ents, prev, prevTerm) ==
    IF Cardinality(s.cfg.mem) <= 1 THEN s                                \* 9048-9051
    ELSE LET tgt == {k \in Keys : s.peers[k] /= Nil /\ KName(k) /= n     \* 9087-9090
                       /\ ~SkipNotReplica(s, KName(k))                   \* 9110-9118
                       /\ s.ca[k] /= NoCkt}                              \* 9124-9128
             msg == AEMsg(s, n, prev, prevTerm, ents)
         IN AddOutSet(s, {[c |-> s.ca[k], m |-> msg] : k \in tgt})

\* ========================================================================
\* STATE SNAPSHOTS (leader side)
\* ========================================================================

\* getStateSnapshot (tube/tube.go:17908-17942): state with
\* CompactionDiscardedLast = (LastApplied, LastAppliedTerm) and an AE holding
\* the log past LastApplied.  `hist` and `appl` are ghosts (the history the
\* snapshot's KV embodies).
SnapState(s, n) ==
    [term |-> s.term, ci |-> s.ci, la |-> s.la, lat |-> s.lat,
     base |-> [idx |-> s.la, term |-> s.lat], cfg |-> s.cfg, sh |-> s.sh,
     hist |-> SubSeq(s.log, 1, s.la), appl |-> s.appl]

\* handleRequestStateSnapshot (tube/tube.go:17464-17551): sent on the circuit
\* the request / gap-ack arrived on (not necessarily cktall[peer]).
HandleRequestStateSnapshot(s, n, c) ==
    IF s.par = "unknown" THEN s                                          \* 17465-17467
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

\* parReconfigurationReady (par.go:2140-2176), disk mode (s.par != nil).
\* De-duplicates by name and requires the current PeerID: the developers'
\* own fix for the pattern D-3/D-17 exploit elsewhere.
ParReconfigReady(s, n, mc) ==
    IF s.par = "unknown" THEN FALSE                                      \* 2144-2146
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

\* onLeaderIsCurrentMCcommitted (tube/tube.go:17350-17413)
OnLeaderIsCurrentMCcommitted(s, n) ==
    IF s.cfg.com THEN s                                                  \* 17361-17363
    ELSE IF MongoCanReconfig(s, n, s.cfg, Nil)                           \* 17398-17404
         THEN [s EXCEPT !.cfg.com = TRUE,                                \* 17409
                        !.cfg.ci = IF s.ci > s.cfg.ci THEN s.ci ELSE @]  \* 17410 updateMCindex
         ELSE s
CurMCCommitted(s, n) == s.cfg.com \/ MongoCanReconfig(s, n, s.cfg, Nil)

\* changeMembership (tube/tube.go:14307-14769) at the leader, for a ticket
\* tkt = [op \in {"add","remove"}, x \in Server].  Force (-f) is not part
\* of the default model (operator override, brief section 3.2).
ChangeMembership(s, n, tkt0) ==
    \* 14350-14387: one change at a time; a new ticket queues behind stalled ones
    LET q   == s.stallQ
        s0  == IF q /= <<>> /\ tkt0 /= Head(q)
               THEN [s EXCEPT !.stallQ = Tail(Append(q, tkt0))]
               ELSE IF q /= <<>> THEN [s EXCEPT !.stallQ = Tail(q)] ELSE s
        tkt == IF q /= <<>> THEN Head(q) ELSE tkt0
        cur == s0.cfg
        new0 == [cur EXCEPT !.cv = @ + 1, !.com = FALSE]                 \* 14424-14426
        \* 14488-14538: members with a live circuit; a nil cktP.MC stalls (D-22)
        live == {m \in cur.mem \ {n} : LiveByName(s0, m)}
        needStall == \E m \in live : SeenOf(s0, n, s0.bn[m]) = Nil
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
                 [cfg |-> [new1 EXCEPT !.mem = @ \cup {x}, !.det[x] = [inc |-> -1, addr |-> addr[x]]],
                  s |-> [s0 EXCEPT !.bn[x] = Pending, !.pa[x] = addr[x], !.wd[x] = addr[x]]]
        newCfg == IF tkt.op = "add" THEN addCase.cfg
                  ELSE [new1 EXCEPT !.mem = @ \ {x}, !.det[x] = Nil]     \* 14661
        s1 == IF tkt.op = "add" THEN addCase.s ELSE s0
    IN
    IF needStall THEN [s0 EXCEPT !.stallQ = Append(@, tkt)]               \* 14517-14522
    ELSE IF tkt.op = "add" /\ x \in cur.mem THEN s0                      \* 14563-14571 error reply
    ELSE IF tkt.op = "remove" /\ x \notin cur.mem THEN s0                \* 14637-14643 nothing to do
    ELSE IF ~MongoCanReconfig(s1, n, cur, newCfg)                        \* 14705-14721
         THEN [s1 EXCEPT !.stallQ = Append(@, tkt)]                      \* 14715
    ELSE \* 14743-14759: install locally and acknowledge the client at once (D-19)
         LET s2 == SetMC(s1, n, newCfg) IN
         [s2 EXCEPT !.newAcked = @ \cup {[ct |-> newCfg.ct, cv |-> newCfg.cv,
                                          op |-> tkt.op, x |-> x]}]

\* one retry of the oldest stalled change (unstallChangeMC 10832-10843 /
\* seen()'s stalledOnSeenTkt pop, tube/watchdog.go:50-57)
ChangeMembershipRetry(s, n) ==
    IF s.role = Leader /\ s.stallQ /= <<>> THEN ChangeMembership(s, n, Head(s.stallQ)) ELSE s

\* connectToMC's deferred resubmitStalledTickets(true) (tube/tube.go:6031-6060,
\* 15228-15230): pops the oldest stalled change and calls
\* commandSpecificLocalActionsThenReplicateTicket, which panics "only for
\* leader" on a non-leader (17118-17122).  D-20.
ResubmitStalledMC(s, n) ==
    IF s.stallQ = <<>> THEN s
    ELSE IF s.role /= Leader
         THEN Panic([s EXCEPT !.stallQ = Tail(@)], "OnlyForLeaderResubmit")
         ELSE ChangeMembership(s, n, Head(s.stallQ))

\* cktPlus.seen(mc, lli, llt, term) (tube/watchdog.go:21-75) on circuit c.
SeenOn(s, n, c, cfgV, t) ==
    IF c = NoCkt \/ c = Pending THEN s
    ELSE LET s1 == [s EXCEPT !.seenOv = {x \in @ : x.c /= c} \cup
                                {[c |-> c, v |-> [ct |-> cfgV.ct, cv |-> cfgV.cv, term |-> t]]}]
         IN IF s1.role = Leader /\ s1.stallQ /= <<>> THEN ChangeMembershipRetry(s1, n)   \* 50-57
            ELSE IF s1.role = Leader /\ ~s1.cfg.com THEN OnLeaderIsCurrentMCcommitted(s1, n)  \* 59-62
            ELSE s1

SeenFrom(s, n, fromKey, cfgV, t) == SeenOn(s, n, s.ca[fromKey], cfgV, t)

\* ========================================================================
\* ELECTIONS
\* ========================================================================

\* haveStickyLeader (tube/tube.go:12513-12550); LeadershipTransferFrom is not modeled
HaveStickyLeader(s) ==
    IF s.role = Leader THEN TRUE
    ELSE IF s.leader = Nil THEN FALSE
    ELSE s.sticky

\* logUpToDateAsOurs (tube/tube.go:9275-9333)
LogUpToDateAsOurs(s, n, rv) ==
    IF s.par = "unknown" THEN FALSE                                      \* 9276-9278 PAR fence
    ELSE IF VersionGT(s.cfg, rv.cfg) THEN FALSE                          \* 9279-9285
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
    ELSE IF VersionGT(v.cfg, s.cfg) THEN s                               \* 7007-7009
    ELSE IF KName(v.from) \notin s.cfg.mem THEN s                        \* 7010-7013
    ELSE LET voter == KName(v.from)
             rec == /\ v.cand = <<n, inc[n]>>                            \* 7015
                    /\ voter \notin (s.pvYes \cup s.pvNo)                \* 7016-7017 de-dup by NAME
                    /\ (s.ca[v.from] /= NoCkt \/ v.from = <<n, inc[n]>>) \* 7018-7020 alive
             s1 == IF ~rec THEN s
                   ELSE IF v.granted THEN [s EXCEPT !.pvYes = @ \cup {voter}]
                   ELSE [s EXCEPT !.pvNo = @ \cup {voter}]
             q == Cardinality(s1.cfg.mem) \div 2 + 1
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
    IF s.par = "unknown" THEN s                                          \* 11168-11170
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
ReplicateEntry(s, n, kind) ==
    LET idx  == Len(s.log) + 1
        e    == [term |-> s.term, id |-> s.nid, kind |-> kind]
        prev == Len(s.log)
        prvT == LastTermL(s.log, s.base)
        s1   == [s EXCEPT !.log = Append(@, e), !.idLen = idx, !.nid = @ + 1,
                          !.noop0 = IF kind = "noop" THEN [id |-> s.nid, done |-> FALSE] ELSE @]
    IN IF Cardinality(s1.cfg.mem) > 1
       THEN BcastAppendEntries(s1, n, <<[idx |-> idx, term |-> e.term, id |-> e.id, kind |-> kind]>>, prev, prvT)
       ELSE CommitWhatWeCan([s1 EXCEPT !.ci = idx,
                                       !.newCommitted = @ \cup {[idx |-> idx, term |-> e.term, id |-> e.id]}], TRUE)

\* becomeLeader (tube/tube.go:7298-7479), up to and including the noop0
\* broadcast.  The state save (7473-7475) runs AFTER the broadcast: modeled
\* by blk[n].save and the separate action BecomeLeaderSave (D-1 window).
BecomeLeader(s, n) ==
    IF s.par = "unknown" THEN s                                          \* parBecomeLeader (par.go:2276-2282)
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
           IN [s2 EXCEPT !.save2 = TRUE]                                 \* 7473-7475 deferred save

\* beginElection (tube/tube.go:6738-6901): no saver.save at all (D-2).
BeginElection(s, n) ==
    IF s.par = "unknown" THEN s                                          \* 6739-6741
    ELSE IF n \notin s.cfg.mem THEN s                                    \* 6742-6745
    ELSE
      LET s0 == ResetPreVote(s) IN                                       \* 6746
      IF Cardinality(s0.cfg.mem) = 1 THEN BecomeLeader(s0, n)            \* 6748-6756 (no term++)
      ELSE
        LET s1 == ReinitPeers([s0 EXCEPT
                     !.term = IF s0.role = Follower THEN @ + 1 ELSE @,   \* 6757-6759
                     !.role = Candidate, !.vote = <<n, inc[n]>>,         \* 6760-6764
                     !.leader = Nil,                                     \* 6765-6768
                     !.vYes = {n}, !.vNo = {}])                          \* 6769-6777, 6778
            rv == [type |-> "RV", from |-> <<n, inc[n]>>, cterm |-> s1.term,
                   lli |-> Len(s1.log), llt |-> LastTermL(s1.log, s1.base), cfg |-> s1.cfg]
        IN AddOutSet(s1, {[c |-> s1.ca[k], m |-> rv] : k \in s1.rep})   \* 6879-6896 over cktReplica

\* tallyVote (tube/tube.go:7136-7271)
TallyVote(s0, n, v) ==
    LET s == SeenFrom(s0, n, v.from, v.cfg, v.vterm) IN                  \* 7138-7141
    IF v.cterm /= s.term THEN                                            \* 7146-7152
        IF v.cterm < s.term THEN s ELSE BecomeFollower(s, n, v.cterm, v.cfg, TRUE)
    ELSE IF s.role = Leader THEN s                                       \* 7153-7155
    ELSE IF s.role = Follower THEN s                                     \* 7156-7159
    ELSE IF KName(v.from) \notin s.cfg.mem THEN s                        \* 7205-7209
    ELSE IF VersionGT(v.cfg, s.cfg) /\ v.granted THEN s                  \* 7211-7216
    ELSE IF v.vterm >= s.term /\ v.granted THEN s                        \* 7218-7223 voter's term must be < ours
    ELSE LET voter == KName(v.from)
             rec == /\ v.cand = <<n, inc[n]>>                            \* 7228
                    /\ voter \notin (s.vYes \cup s.vNo)                  \* 7231-7233 de-dup by NAME
                    /\ (s.ca[v.from] /= NoCkt \/ v.from = <<n, inc[n]>>) \* 7235-7237
             s1 == IF ~rec THEN s
                   ELSE [s EXCEPT !.vYes = IF v.granted THEN @ \cup {voter} ELSE @,
                                  !.vNo  = IF v.granted THEN @ ELSE @ \cup {voter},
                                  !.peers[v.from] = IF @ = Nil THEN FreshPeer ELSE @]  \* 7246-7254
             q == Cardinality(s1.cfg.mem) \div 2 + 1
         IN IF Cardinality(s1.vYes) >= q THEN BecomeLeader(s1, n)        \* 7260-7263
            ELSE IF Cardinality(s1.vNo) >= q /\ s1.role /= Follower      \* 7264-7269
                 THEN BecomeFollower(s1, n, s1.term, Nil, FALSE)
            ELSE s1

\* ========================================================================
\* APPEND ENTRIES (follower side)
\* ========================================================================

\* aeMemberConfigHelper (tube/tube.go:7769-7835)
AEMemberConfigHelper(s, n, ae) ==
    IF VersionEq(s.cfg, ae.cfg)                                          \* 7774-7780
    THEN (IF ae.cfg.com /\ ~s.cfg.com THEN [s EXCEPT !.cfg.com = TRUE] ELSE s)
    ELSE IF VersionGTE(s.cfg, ae.cfg) THEN s                             \* 7782-7786
    ELSE LET s1 == [s EXCEPT !.sh = ae.sh]                               \* 7811-7813
             s2 == SetMC(s1, n, ae.cfg)                                  \* 7815 (MC adopted wholesale, incl. CommitIndex)
             s3 == IF Len(ae.ents) = 0 THEN [s2 EXCEPT !.save = TRUE] ELSE s2   \* 7823-7830
         IN ResubmitStalledMC(ConnectToMC(s3, n), n)                     \* 7831-7834 connectToMC("AE") + its defer

\* wal.overwriteEntries(keepIndex, es, ...) (tube/wal.go:793-1010): clamp
\* to the compaction barrier (824-860), truncate after keepIndex, append es.
\* Undo-journaled (897-993).  No guard against overwriting committed entries
\* in production (935-942 only with NoLogCompaction, D-11).
OverwriteEntries(lg, bs, keepIdx, es) ==
    LET barrier == bs.idx
        nes == Len(es)
        conv(x) == [term |-> x.term, id |-> x.id, kind |-> x.kind]
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
     cfg |-> cfg, leader |-> ae.from, gx |-> gx, gt |-> gt]

CommonPrefix(lg, ids) ==
    MaxSet({j \in 0..Min(Len(lg), Len(ids)) : \A i \in 1..j : lg[i].id = ids[i]})

\* AckedStays ghost: an accepting ack claims log[1..last] (MatchIndex := max(MI, last))
ClaimAck(s, last) ==
    LET cl == {[idx |-> i, id |-> s.log[i].id] : i \in 1..Min(last, Len(s.log))}
    IN [s EXCEPT !.ackH = IF @.term = s.term THEN [@ EXCEPT !.claims = @ \cup cl]
                          ELSE [term |-> s.term, claims |-> cl]]

\* handleAppendEntries (tube/tube.go:7854-8689) when not PAR-blocked.
\* Returns [s, ack, branch].  The branch order follows the code exactly.
AEStep(s0, n, ae) ==
    LET s1 == SeenFrom(s0, n, ae.from, ae.cfg, ae.term)                  \* 7958-7960 suppressWatchdogs
        F  == RLEOf(s1.log, s1.base)                                     \* 7969
        X  == Extends(ae.rle, F)                                         \* 7971
        prevci == s1.ci                                                  \* 7977
        maxCI  == Min(X.lcri, ae.commit)                                 \* 7981-7992
        adv    == maxCI > prevci                                         \* 7993
        \* 7998-8000: CommitIndex raised BEFORE the term check (75babbe9)
        s2 == IF adv THEN [s1 EXCEPT !.ci = maxCI, !.cfg.ci = IF maxCI > @ THEN maxCI ELSE @]
              ELSE s1
        s3 == IF ae.term > s2.term /\ ae.from /= s2.leader                \* 8017-8038
              THEN [s2 EXCEPT !.leader = ae.from] ELSE s2
        last0 == PhysLastIdx(s1.log, s1.base)                            \* 8072-8075 getRaftLogSummary
        lastT0 == PhysLastTerm(s1.log, s1.base)
        lcri0 == IF s1.base.idx > 0 THEN s1.base.idx ELSE X.lcri         \* 8093, 8123-8128
        gt0 == CommonPrefix(s1.log, ae.gids)
        fin(s) == IF adv THEN CommitWhatWeCan(s, FALSE) ELSE s            \* deferred 8004-8010
    IN
    IF ae.term < s3.term                                                 \* 8141-8148 term too low
    THEN [s |-> fin(s3), branch |-> "RejectTerm", pre |-> s3, wlog |-> Nil,
          ack |-> AckMsg(s3, n, ae, TRUE, FALSE, lcri0, last0, lastT0, s3.cfg, X.lcri, gt0)]
    ELSE
    LET s4 == [s3 EXCEPT !.sticky = TRUE]                                \* 8153 lastLegitAppendEntries
        s5 == AEMemberConfigHelper(s4, n, ae)                            \* 8161-8163
        s6 == IF ae.term > s5.term THEN BecomeFollower(s5, n, ae.term, ae.cfg, TRUE)  \* 8170-8175
              ELSE IF s5.role /= Follower THEN [s5 EXCEPT !.role = Follower]          \* 8180-8189 (not becomeFollower, D-13)
              ELSE s5
        s8 == [ResetPreVote(s6) EXCEPT !.leader = ae.from]               \* 8191, 8200-8211
        nes  == Len(ae.ents)
        eBeg == IF nes > 0 THEN ae.ents[1].idx ELSE 0                    \* 8066
        eEnd == IF nes > 0 THEN ae.ents[nes].idx ELSE 0                  \* 8067
        M    == LogsAreMismatched(s8.log, s8.base, ae)                   \* 8264
        rlen == Len(s1.log)                                              \* 8054 rlogicalLen
        from == rlen + 1 - eBeg                                          \* 8496
        ackE(rej, ns, lcri) == AckMsg(s8, n, ae, rej, ns, lcri, last0, lastT0, s8.cfg, X.lcri, gt0)
        early(b, a) == [s |-> fin(IF a.rej THEN s8 ELSE ClaimAck(s8, a.last)), branch |-> b, ack |-> a,
                        pre |-> s8, wlog |-> Nil]
        \* common tail after a WAL write (8549-8683)
        tail(b, lg2) ==
            LET sw == [s8 EXCEPT !.log = lg2, !.idLen = Len(lg2)]
                lli == Len(lg2)
                sc == IF ae.commit > sw.ci                               \* 8568-8604
                      THEN [sw EXCEPT !.ci = IF lli < ae.commit THEN lli ELSE ae.commit] ELSE sw
                sa == CommitWhatWeCan(sc, FALSE)                         \* 8638
                l1 == PhysLastIdx(sa.log, sa.base)                       \* 8642-8643 getRaftLogSummary
                t1 == PhysLastTerm(sa.log, sa.base)
                exact == l1 = EndiR(ae.rle) /\ t1 = LastTermR(ae.rle)    \* 8654-8655
                X2 == Extends(ae.rle, RLEOf(sa.log, sa.base))            \* 8659
                lc == IF exact THEN l1 ELSE IF X2.lcri = 0 THEN sa.base.idx ELSE X2.lcri   \* 8656-8663
                a == AckMsg(sa, n, ae, FALSE, IF exact THEN X.snap ELSE X2.snap, lc, l1, t1, s8.cfg,
                            IF exact THEN l1 ELSE X2.lcri, CommonPrefix(sa.log, ae.gids))
            IN [s |-> ClaimAck(sa, l1), branch |-> b, ack |-> a, pre |-> s8, wlog |-> lg2]
    IN
    CASE X.lcri = EndiR(ae.rle) ->                                       \* 8237-8248 upToDate
            early("UpToDate", ackE(FALSE, X.snap, X.lcri))
      [] M.rej ->                                                        \* 8273-8298 mismatch reject
            early("Mismatch", ackE(TRUE, X.snap \/ ae.prev > X.lcri, X.lcri))
      [] nes = 0 ->                                                      \* 8300-8307 heartbeat: update needed
            early("HeartbeatReject", ackE(TRUE, X.snap, X.lcri))
      [] ~X.ext /\ ae.prev > X.lcri ->                                   \* 8320-8326 gap
            early("GapReject", ackE(TRUE, X.snap, X.lcri))
      [] ~X.ext /\ eBeg > X.lcri + 1 ->                                  \* 8348-8354 cannot gap
            early("CannotGap", ackE(TRUE, X.snap, X.lcri))
      [] ~X.ext /\ eEnd <= X.lcri ->                                     \* 8377-8386 "nothing new": ACCEPT, last = whole log (D-3)
            early("NothingNew", ackE(FALSE, X.snap, X.lcri))
      [] ~X.ext ->                                                       \* 8400-8481 overwrite from LCRI+1
            tail("Overwrite",
                 OverwriteEntries(s8.log, s8.base, X.lcri,
                                  SubSeq(ae.ents, X.lcri + 2 - eBeg, nes)))
      [] from >= nes ->                                                  \* 8500-8508 no useful data, accept
            tail("ExtendsNoData", s8.log)
      [] from < 0 ->                                                     \* 8509-8515 "gap" but Rejected=false (set at 8484)
            early("ExtendsGap", ackE(FALSE, X.snap, X.lcri))
      [] OTHER ->                                                        \* 8516-8544 plain append (unjournaled)
            tail("ExtendsAppend",
                 OverwriteEntries(s8.log, s8.base, rlen, SubSeq(ae.ents, from + 1, nes)))

\* parObserveAE (par.go:2835-2851): the only AE processing of a PAR-blocked node
ParObserveAE(s, n, ae) ==
    IF ae.term < s.term THEN s
    ELSE LET conf == IF VersionGT(ae.cfg, s.cfg) THEN ae.cfg ELSE Nil
             s1 == IF ae.term > s.term \/ s.role /= Follower \/ conf /= Nil
                   THEN BecomeFollower(s, n, ae.term, conf, TRUE) ELSE s
         IN [s1 EXCEPT !.leader = ae.from, !.sticky = TRUE]

\* ========================================================================
\* APPEND ENTRIES ACK (leader side)
\* ========================================================================

\* handleAppendEntriesAck (tube/tube.go:9847-10134); c = arrival circuit.
\* The UnackedPing.AEID match (9963) is abstracted: a current-term ack
\* refreshes the pong and the lease credit (time is abstract; StickyExpire
\* and ElectionTimerMayFire encode the window ordering).
AckStep(s0, n, ack, c) ==
    IF s0.par = "unknown" THEN                                           \* 9848-9853
        (IF ack.term > s0.term THEN BecomeFollower(s0, n, ack.term, ack.cfg, TRUE) ELSE s0)
    ELSE
    LET s1 == SeenFrom(s0, n, ack.from, ack.cfg, ack.term) IN            \* 9875-9882
    IF ack.term > s1.term THEN BecomeFollower(s1, n, ack.term, ack.cfg, TRUE)   \* 9887-9893
    ELSE IF ack.term < s1.term THEN s1                                   \* 9894-9902
    ELSE IF s1.role /= Leader THEN s1                                    \* 9903-9905
    ELSE IF n \notin s1.cfg.mem /\ s1.cfg.mem /= {} /\ CurMCCommitted(s1, n)     \* 9912-9932 removed leader
         THEN BecomeFollower([OnLeaderIsCurrentMCcommitted(s1, n) EXCEPT !.leader = Nil],
                             n, s1.term, Nil, TRUE)
    ELSE IF ack.leader /= <<n, inc[n]>> THEN s1                          \* 9938-9942
    ELSE
      LET k  == ack.from
          f0 == IF s1.peers[k] = Nil THEN FreshPeer ELSE s1.peers[k]     \* 9952-9957
          s2 == [s1 EXCEPT !.peers[k] = [f0 EXCEPT !.lcri = ack.lcri, !.credit = TRUE],  \* 9963-9996
                           !.pong = [all |-> @.all \cup {k}, fresh |-> @.fresh \cup {k}]]  \* 9979
      IN IF ack.rej THEN
             IF ack.needSnap THEN HandleRequestStateSnapshot(s2, n, c)   \* 10010-10017 on the ACK's circuit
             ELSE LET s3 == IF ack.lcri > f0.mi                          \* 10019-10025 MI := LCRI
                            THEN LeaderAdvanceCommitIndex([s2 EXCEPT !.peers[k].mi = ack.lcri], n)
                            ELSE s2
                  IN SendAppendEntriesTo(s3, n, k, ack.lcri + 1)         \* 10031
         ELSE LET grow == ack.last > f0.mi                               \* 10108-10112 MI := max(MI, PeerLogLastIndex) (D-3)
                  s3 == IF grow THEN LeaderAdvanceCommitIndex([s2 EXCEPT !.peers[k].mi = ack.last], n)
                        ELSE s2                                          \* 10113-10117
              IN IF s3.role /= Leader THEN s3                            \* 10119-10121
                 ELSE IF s3.peers[k] /= Nil /\ PhysLastIdx(s3.log, s3.base) > s3.peers[k].lcri  \* 9998, 10126-10131
                      THEN SendAppendEntriesTo(s3, n, k, s3.peers[k].lcri + 1)
                      ELSE s3

\* ========================================================================
\* STATE SNAPSHOT INSTALL (follower side)
\* ========================================================================

\* parSnapshotAllowed (par.go:2656-2680)
ParSnapshotAllowed(s, n, sn) ==
    /\ ~VersionGT(s.cfg, sn.st.cfg)                                      \* 2660-2662
    /\ (s.par = "unknown" =>                                             \* 2663-2678
          /\ s.role = Follower /\ s.leader /= Nil /\ sn.from = s.leader
          /\ sn.st.term = s.term /\ sn.st.la >= ParProtected(s)
          /\ sn.st.ci >= sn.st.la
          /\ VersionEq(sn.st.cfg, s.cfg))

\* applyNewStateSnapshot (tube/tube.go:17695-17901).  The monotonicity
\* guards are `if false` (17755-17773): CommitIndex/LastApplied may go
\* backwards and the WAL is truncated to the snapshot base (D-5).
\* Returns [s, ack, branch].
ApplySnapshot(s, n, sn) ==
    IF ~ParSnapshotAllowed(s, n, sn) THEN [s |-> s, ack |-> Nil, branch |-> "Refused"]   \* 17696-17698
    ELSE IF sn.st.term /= s.term THEN [s |-> s, ack |-> Nil, branch |-> "TermMismatch"]   \* 17779-17782
    ELSE
      LET st == sn.st
          s1 == [s EXCEPT !.ci = st.ci, !.la = st.la, !.lat = st.lat,    \* 17800-17805
                          !.appl = st.appl,                              \* 17819-17832 KV replaced
                          !.cfg = st.cfg, !.sh = st.sh,                  \* 17834-17839
                          !.base = st.base,                              \* 17857
                          !.log = st.hist, !.idLen = Len(st.hist),       \* 17879 wal.installedSnapshot
                          !.par = "ok",                                  \* 17793-17797 PAR recovered
                          !.save = TRUE,                                 \* 17877
                          !.newCfgHist = @ \cup {[ct |-> st.cfg.ct, cv |-> st.cfg.cv, mem |-> st.cfg.mem]}]
          r  == AEStep(s1, n, sn.ae)                                     \* 17882-17887 handleAppendEntries(snap.AE, true)
      IN [s |-> r.s, ack |-> r.ack, branch |-> "Installed"]

\* ========================================================================
\* WRITE-BACK OF ONE EVENT-LOOP STEP
\* ========================================================================

RECURSIVE ApplyCloses(_, _, _, _)
\* Circuit.Close(reason) (ckt.go:1349-1369) + pump cleanupCkt notify
\* (pump.go:40-110): Close(nil) sends CallPeerEndCircuit; a non-nil reason
\* sets frag.Err, which ConvertFragmentToMessage turns into CallPeerError
\* (ckt.go:919-921), so the remote end is NOT closed (D-37).
ApplyCloses(cn, ck, n, cl) ==
    IF cl = <<>> THEN [cn |-> cn, ck |-> ck]
    ELSE LET x == Head(cl)
             openNow == EpSt(x.c, n) = EpOpen
             msg == IF x.why = "nil" THEN [type |-> "END", from |-> <<n, inc[n]>>]
                    ELSE [type |-> "PERR", from |-> <<n, inc[n]>>]
         IN ApplyCloses(IF openNow THEN BgSendOne(cn, n, x.c, msg) ELSE cn,
                        IF openNow THEN SetEp(ck, x.c, n, EpClosed) ELSE ck, n, Tail(cl))

ApplySeen(ck, n, ov) ==
    [c \in DOMAIN ck |->
        IF \E x \in ov : x.c = c
        THEN LET v == (CHOOSE x \in ov : x.c = c).v IN
             IF ck[c].d = n THEN [ck[c] EXCEPT !.dSeen = v] ELSE [ck[c] EXCEPT !.aSeen = v]
        ELSE ck[c]]

RECURSIVE SetToSeq(_)
SetToSeq(S) == IF S = {} THEN <<>>
               ELSE LET x == CHOOSE x \in S : TRUE IN <<x>> \o SetToSeq(S \ {x})

\* parSaveLocalMeta -> parLocalMeta.merge (persistor.go:245, par.go:893-914)
\* runs inside EVERY saver.save: the PAR term/vote copies restore a same-term
\* vote that was cleared in memory (and update s.state itself); a different
\* same-term vote is ErrPARConflict -> panicOn.
MergedTerm(n, s) == IF dTerm[n] > s.term THEN dTerm[n] ELSE s.term
MergedVote(n, s) ==
    IF dTerm[n] > s.term THEN dVote[n]
    ELSE IF dTerm[n] = s.term /\ dVote[n] /= Nil /\ s.vote = Nil THEN dVote[n]
    ELSE s.vote
VoteConflict(n, s) ==
    dTerm[n] = s.term /\ dVote[n] /= Nil /\ s.vote /= Nil /\ s.vote /= dVote[n]

\* Write node n's record back; cn0/ck0: conn/ckt after the step's own
\* frame consumption / endpoint changes.  s.save => saver.save at the end
\* (persistor.go:230-294: temp file + fsync + rename + dir fsync).
WriteBack(n, s, cn0, ck0) ==
    LET st == SendSeq([cn |-> cn0, bl |-> NoBlk], n, s.out)
        cl == ApplyCloses(st.cn, ck0, n, s.closes)
        tm == IF s.save THEN MergedTerm(n, s) ELSE s.term
        vt == IF s.save THEN MergedVote(n, s) ELSE s.vote
        pn == IF s.save /\ VoteConflict(n, s) THEN {"PARMetaVoteConflict"} ELSE {}
    IN
    /\ role'            = [role EXCEPT ![n] = s.role]
    /\ currentTerm'     = [currentTerm EXCEPT ![n] = tm]
    /\ votedFor'        = [votedFor EXCEPT ![n] = vt]
    /\ leaderKey'       = [leaderKey EXCEPT ![n] = s.leader]
    /\ sticky'          = [sticky EXCEPT ![n] = s.sticky]
    /\ pvTerm'          = [pvTerm EXCEPT ![n] = s.pvTerm]
    /\ pv2Term'         = [pv2Term EXCEPT ![n] = s.pv2Term]
    /\ pvYes'           = [pvYes EXCEPT ![n] = s.pvYes]
    /\ pvNo'            = [pvNo EXCEPT ![n] = s.pvNo]
    /\ vYes'            = [vYes EXCEPT ![n] = s.vYes]
    /\ vNo'             = [vNo EXCEPT ![n] = s.vNo]
    /\ peers'           = [peers EXCEPT ![n] = s.peers]
    /\ pong'            = [pong EXCEPT ![n] = s.pong]
    /\ leaderAged'      = [leaderAged EXCEPT ![n] = s.aged]
    /\ noop0'           = [noop0 EXCEPT ![n] = s.noop0]
    /\ stallQ'          = [stallQ EXCEPT ![n] = s.stallQ]
    /\ config'          = [config EXCEPT ![n] = s.cfg]
    /\ shadows'         = [shadows EXCEPT ![n] = s.sh]
    /\ commitIndex'     = [commitIndex EXCEPT ![n] = s.ci]
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
    /\ cktReplica'      = [cktReplica EXCEPT ![n] = s.rep]
    /\ audit'           = [audit EXCEPT ![n] = s.aud]
    /\ applied'         = [applied EXCEPT ![n] = s.appl]
    /\ ackHigh'         = [ackHigh EXCEPT ![n] = s.ackH]
    /\ conn'            = cl.cn
    /\ ckt'             = ApplySeen(cl.ck, n, s.seenOv)
    /\ blk'             = [blk EXCEPT ![n] = [st.bl EXCEPT !.save = s.save2]]
    /\ dTerm'           = IF s.save THEN [dTerm EXCEPT ![n] = tm] ELSE dTerm
    /\ dVote'           = IF s.save THEN [dVote EXCEPT ![n] = vt] ELSE dVote
    /\ dCfg'            = IF s.save THEN [dCfg EXCEPT ![n] = s.cfg] ELSE dCfg
    /\ dShadows'        = IF s.save THEN [dShadows EXCEPT ![n] = s.sh] ELSE dShadows
    /\ dCommit'         = IF s.save THEN [dCommit EXCEPT ![n] = s.ci] ELSE dCommit
    /\ walGone'         = [walGone EXCEPT ![n] = @ /\ s.par = "unknown"]   \* 17792-17797 files recreated
    /\ committed'       = committed \cup s.newCommitted
    /\ panics'          = panics \cup s.newPanics \cup pn
    /\ cfgHist'         = cfgHist \cup s.newCfgHist
    /\ ackedCfg'        = ackedCfg \cup s.newAcked
    /\ leaders'         = leaders \cup s.newLeaders
    /\ nextId'          = s.nid
    /\ nextSid'         = s.sid
    /\ UNCHANGED <<up, inc, addr, stopped, link, nextConn, nextCkt>>

\* The event loop can run a handler: alive, not stopped, not inside a
\* blocked send / inline dial, not between becomeLeader's broadcast and save.
Running(n) == up[n] /\ ~stopped[n] /\ blk[n].pend = <<>> /\ blk[n].dial = Nil /\ ~blk[n].save

\* ========================================================================
\* INITIAL STATE
\* ========================================================================

InitNodes == InitMembers \cup InitShadows

InitCfg == [mem |-> InitMembers, ct |-> 0, cv |-> 1, com |-> TRUE, ci |-> 0,
            det |-> [m \in Server |-> IF m \in InitMembers
                                      THEN [inc |-> 0, addr |-> InitAddr] ELSE Nil]]
\* NewMemberConfig for a node that was never part of the cluster
EmptyCfg == [mem |-> {}, ct |-> 0, cv |-> 0, com |-> FALSE, ci |-> 0,
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
ColdAddr(n, m) == IF m \in CfgOf0(n).mem /\ CfgOf0(n).det[m] /= Nil
                  THEN CfgOf0(n).det[m].addr ELSE "none"

CoreInit ==
    /\ up      = [n \in Server |-> n \in InitNodes]
    /\ inc     = [n \in Server |-> 0]
    /\ addr    = [n \in Server |-> InitAddr]
    /\ stopped = [n \in Server |-> FALSE]
    /\ link    = [x \in Server |-> [y \in Server |-> "ok"]]
    /\ conn = IF InitMeshed
              THEN [k \in MeshIds |-> [d |-> PairOf(k)[1], di |-> 0, a |-> PairOf(k)[2], ai |-> 0,
                                       ad |-> InitAddr, dOpen |-> TRUE, aOpen |-> TRUE,
                                       q |-> [x \in Dirs |-> <<>>], u |-> [x \in Dirs |-> 0]]]
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
    /\ cktReplica = [n \in Server |-> {<<m, 0>> : m \in {x \in Server : Meshed(n, x)}}]
    /\ audit = [n \in Server |-> {MeshCkt(n, m) : m \in {x \in Server : Meshed(n, x)}}]
    /\ role = [n \in Server |-> Follower]
    /\ currentTerm = [n \in Server |-> 0]
    /\ votedFor = [n \in Server |-> Nil]
    /\ leaderKey = [n \in Server |-> Nil]
    /\ sticky = [n \in Server |-> FALSE]
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
    /\ noop0 = [n \in Server |-> [id |-> 0, done |-> FALSE]]
    /\ stallQ = [n \in Server |-> <<>>]
    /\ config = [n \in Server |-> CfgOf0(n)]
    /\ shadows = [n \in Server |-> ShOf0(n)]
    /\ commitIndex = [n \in Server |-> 0]
    /\ lastApplied = [n \in Server |-> 0]
    /\ lastAppliedTerm = [n \in Server |-> 0]
    /\ snapSlot = [n \in Server |-> Nil]
    /\ log = [n \in Server |-> <<>>]
    /\ base = [n \in Server |-> [idx |-> 0, term |-> 0]]
    /\ idLen = [n \in Server |-> 0]
    /\ torn = [n \in Server |-> <<>>]
    /\ par = [n \in Server |-> "ok"]
    /\ dTerm = [n \in Server |-> 0]
    /\ dVote = [n \in Server |-> Nil]
    /\ dCfg = [n \in Server |-> CfgOf0(n)]
    /\ dShadows = [n \in Server |-> ShOf0(n)]
    /\ dCommit = [n \in Server |-> 0]
    /\ walGone = [n \in Server |-> FALSE]
    /\ nextId = 1
    /\ committed = {}
    /\ applied = [n \in Server |-> <<>>]
    /\ leaders = {}
    /\ ackedCfg = {}
    /\ cfgHist = {[ct |-> 0, cv |-> 1, mem |-> InitMembers]}
    /\ panics = {}
    /\ ackHigh = [n \in Server |-> [term |-> 0, claims |-> {}]]
    /\ nextSid = 1

\* ========================================================================
\* ACTIONS: PROCESS AND NETWORK FAULTS
\* ========================================================================

\* A process dies (kill -9, OOM, power loss, or a panic such as a persistor
\* I/O error, persistor.go:245-291).  Its sockets die; the peers' ends stay
\* open (half-open) until they detect it (ConnAbort).  Volatile state is
\* lost (reloaded by Restart).
CrashConn(n) == [k \in DOMAIN conn |-> [conn[k] EXCEPT
                    !.dOpen = IF conn[k].d = n /\ conn[k].di = inc[n] THEN FALSE ELSE @,
                    !.aOpen = IF conn[k].a = n /\ conn[k].ai = inc[n] THEN FALSE ELSE @]]
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

\* Process (re)start: onRestartRecoverPersistentRaftStateFromDisk
\* (tube/tube.go:9336-9479), parAfterRestore (par.go:2178-2211), new PeerID,
\* startup connectToMC (tube/tube.go:1036).  CurrentTerm is NOT reconciled
\* with the WAL's last term (9381-9383, D-1).  Listen address a may differ
\* from the previous one (D-36).
Restart(n, a) ==
    /\ ~up[n] /\ inc[n] < MaxInc /\ a \in Addrs
    /\ LET isTorn == idLen[n] < Len(log[n])
           lg    == SubSeq(log[n], 1, idLen[n])                          \* par.go:2105-2117 identified prefix
           tseq  == [j \in 1..(Len(log[n]) - idLen[n]) |->
                        [idx |-> idLen[n] + j, e |-> log[n][idLen[n] + j]]]
           cfg   == dCfg[n]
           ci    == Max(dCommit[n], lastApplied[n])
           floor == Max(Max(ci, lastApplied[n]), Max(base[n].idx, cfg.ci))  \* parProtected 2127-2133
           unk   == isTorn \/ floor > Len(lg)                            \* 2183-2185, 2200-2202 (D-49, D-50)
                    \/ walGone[n]                                       \* 2182-2185 + 2012: missing files -> inv.Err, no entries
           want  == IF cfg.mem = {} THEN {} ELSE (cfg.mem \cup dShadows[n]) \ {n}   \* 15233-15294
           dA(m) == IF m \in cfg.mem /\ cfg.det[m] /= Nil THEN cfg.det[m].addr ELSE "none"
       IN
       /\ up' = [up EXCEPT ![n] = TRUE]
       /\ inc' = [inc EXCEPT ![n] = @ + 1]
       /\ addr' = [addr EXCEPT ![n] = a]
       /\ role' = [role EXCEPT ![n] = Follower]
       /\ currentTerm' = [currentTerm EXCEPT ![n] = dTerm[n]]            \* 9378-9383
       /\ votedFor' = [votedFor EXCEPT ![n] = dVote[n]]
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
       /\ noop0' = [noop0 EXCEPT ![n] = [id |-> 0, done |-> FALSE]]
       /\ stallQ' = [stallQ EXCEPT ![n] = <<>>]
       /\ config' = [config EXCEPT ![n] = cfg]
       /\ shadows' = [shadows EXCEPT ![n] = dShadows[n]]
       /\ commitIndex' = [commitIndex EXCEPT ![n] = ci]
       /\ snapSlot' = [snapSlot EXCEPT ![n] = Nil]
       /\ log' = [log EXCEPT ![n] = lg]
       /\ idLen' = [idLen EXCEPT ![n] = Len(lg)]
       /\ torn' = [torn EXCEPT ![n] = tseq]
       /\ par' = [par EXCEPT ![n] = IF unk THEN "unknown" ELSE "ok"]
       /\ cktall' = [cktall EXCEPT ![n] = [k \in Keys |-> NoCkt]]
       /\ byName' = [byName EXCEPT ![n] = [m \in Server |-> IF m \in want THEN Pending ELSE NoCkt]]
       /\ pendAddr' = [pendAddr EXCEPT ![n] = [m \in Server |-> dA(m)]]
       /\ wdReq' = [wdReq EXCEPT ![n] = [m \in Server |-> IF m \in want THEN dA(m) ELSE Nil]]
       /\ cktReplica' = [cktReplica EXCEPT ![n] = {}]
       /\ audit' = [audit EXCEPT ![n] = {}]
       /\ blk' = [blk EXCEPT ![n] = NoBlk]
       /\ UNCHANGED <<stopped, link, conn, ckt, nextConn, nextCkt, lastApplied, lastAppliedTerm,
                      base, durVars, ghostVars>>

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
    [d |-> n, di |-> inc[n], a |-> m, ai |-> inc[m], ad |-> a, dOpen |-> TRUE, aOpen |-> TRUE,
     q |-> [x \in Dirs |-> IF x = "da" THEN q0 ELSE <<>>], u |-> [x \in Dirs |-> 0]]

\* One watchdog reconnect attempt (tube/watchdog.go:225-246, 340-432):
\* PreferExtantRemotePeerGetCircuit (ckt2.go:20-450) with a 5 s timeout,
\* reusing a live auto-cli connection to the address or dialing one
\* (srv.go:2287-2366).  On success both ends get a new circuit, delivered to
\* each Tube via NewCircuitCh.  On failure nothing is retried (the ticker of
\* a pending watchdog is stopped, 195-197; the periodic branch is dead,
\* 210/294-297 D-30).
\*
\* Failure over a live auto-cli connection (D-37b): the circuit-start
\* fragment (CallPeerStartCircuitAtMostOne, empty ToPeerID) is handed to the
\* connection BEFORE the ack wait (implRemotePeerAndGetCircuit,
\* ckt2.go:262-300); when no ack arrives in time the dialer closes its
\* half-made circuit WITH A REASON while its RemotePeerID is still empty
\* (ckt2.go:427-446): a CallPeerError with an empty ToPeerID follows the
\* start frame.  Tube never sees the dialer's end.  If the acceptor later
\* reads them (link healed, process resumed), it creates its end of the
\* circuit (HandleStartFrame) and then its read loop tears the whole
\* connection down on the unknown target (StaleTargetTeardown).  The start
\* frame is not written if the hand-off would block (full send buffer
\* toward a non-absorbing peer: the 5 s context expires first) or there is
\* no live connection (the inline dial fails).
DialAttempt(n, m) ==
    /\ up[n] /\ ~stopped[n] /\ n /= m
    /\ wdReq[n][m] /= Nil
    /\ byName[n][m] /= NoCkt          \* deleteFromCktAll stops the cktPlus watchdog (2553)
    /\ LET a     == wdReq[n][m]
           reach == /\ a \in Addrs /\ up[m] /\ ~stopped[m] /\ addr[m] = a
                    /\ link[n][m] = "ok" /\ link[m][n] = "ok"
           ks    == DialerConns(conn, n, m, a)
           k0    == IF ks = {} THEN 0 ELSE MinSet(ks)
           ok    == reach /\ (k0 = 0 \/ (conn[k0].aOpen /\ conn[k0].ai = inc[m]))
           kk    == IF k0 = 0 THEN nextConn ELSE k0
           strand == /\ ~ok /\ k0 /= 0
                     /\ (Occ(conn, k0, "da") + 2 <= SendBufCap \/ Absorbing(conn, k0, "da"))
       IN /\ wdReq' = [wdReq EXCEPT ![n][m] = Nil]
          /\ IF ok
             THEN /\ conn' = IF k0 = 0 THEN conn @@ (nextConn :> NewConn(n, m, a, <<>>)) ELSE conn
                  /\ nextConn' = IF k0 = 0 THEN nextConn + 1 ELSE nextConn
                  /\ ckt' = ckt @@ (nextCkt :> [d |-> n, di |-> inc[n], dd |-> addr[n],
                                                a |-> m, ai |-> inc[m], ad |-> a, k |-> kk,
                                                dSt |-> EpNew, aSt |-> EpNew,
                                                dSeen |-> Nil, aSeen |-> Nil])
                  /\ nextCkt' = nextCkt + 1
             ELSE IF strand
             THEN LET acc == <<conn[k0].a, conn[k0].ai>> IN
                  /\ conn' = [conn EXCEPT ![k0].q["da"] = @ \o
                                <<[cid |-> nextCkt, to |-> acc, m |-> [type |-> "START", from |-> <<n, inc[n]>>]],
                                  [cid |-> nextCkt, to |-> Nil, m |-> [type |-> "PERR", from |-> <<n, inc[n]>>]]>>]
                  /\ ckt' = ckt @@ (nextCkt :> [d |-> n, di |-> inc[n], dd |-> addr[n],
                                                a |-> conn[k0].a, ai |-> conn[k0].ai, ad |-> a, k |-> k0,
                                                dSt |-> EpGone, aSt |-> "unstarted",
                                                dSeen |-> Nil, aSeen |-> Nil])
                  /\ nextCkt' = nextCkt + 1
                  /\ UNCHANGED nextConn
             ELSE UNCHANGED <<conn, nextConn, ckt, nextCkt>>
    /\ UNCHANGED <<envVars, blk, cktall, byName, pendAddr, cktReplica, audit,
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
       IN /\ EpSt(c, r) = "unstarted"
          /\ ckt' = SetEp(ckt, c, r, EpNew)
    /\ conn' = Consume(conn, k, dir, i)
    /\ UNCHANGED <<envVars, nextConn, nextCkt, blk, viewVars, raftVars, logVars, durVars, ghostVars>>

\* TCP gives up on one side of a connection: FIN/RST from a dead peer,
\* keepalive (idle, ~150 s), or retransmission abort with data outstanding
\* (tcp_retries2, ~15 min).  Never enabled toward a stopped peer whose
\* kernel ACKs zero-window probes with both links up.  No TCP_USER_TIMEOUT,
\* app keepalives off (srv.go:337-349).  Queued frames are lost.
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
          /\ conn' = [conn EXCEPT ![k].dOpen = IF side = "d" THEN FALSE ELSE @,
                                  ![k].aOpen = IF side = "a" THEN FALSE ELSE @,
                                  ![k].q = [x \in Dirs |-> <<>>],
                                  ![k].u = [x \in Dirs |-> 0]]
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

\* A dead connection side halts the circuits it served with reason "conn
\* shut" (rwPair/cli send-loop defer: deletePair, then
\* ckt.Halt.ReqStop.CloseWithReason, srv.go:482-491, cli.go:585-589); a
\* halted circuit's Tube reader exits (cktHasDied is ignored,
\* tube/tube.go:1979-1991), calling ckt.Close(nil) in its defer (2278-2281),
\* whose pump cleanup is the circuit's FIRST and so notifies the peer with
\* the halt reason: a CallPeerError (pump.go:40-125).  The original pair is
\* already deleted, so the notification goes over another live connection
\* between the two processes if there is one (forbidAutoCli: never a new
\* dial), else it is dropped.  The reader then sends peerLeftCh.
HaltOnDeadConn(n, c) ==
    /\ c \in DOMAIN ckt /\ up[n] /\ ~stopped[n]
    /\ EpSt(c, n) \in {EpNew, EpOpen}
    /\ LET k == ckt[c].k IN IF ckt[c].d = n THEN ~conn[k].dOpen ELSE ~conn[k].aOpen
    /\ ckt' = SetEp(ckt, c, n, IF EpSt(c, n) = EpNew THEN "newdead" ELSE EpClosed)
    /\ LET pk  == PeerKeyOf(c, n)
           ks  == AnyConnTo(conn, n, pk)
           k2  == IF ks = {} THEN 0 ELSE MinSet(ks)
           d2  == IF k2 = 0 THEN "da" ELSE SendDir(conn, k2, n)
           f   == [cid |-> c, to |-> pk, m |-> [type |-> "PERR", from |-> <<n, inc[n]>>]]
       IN conn' = IF k2 = 0 \/ Occ(conn, k2, d2) >= SendBufCap THEN conn
                  ELSE [conn EXCEPT ![k2].q[d2] = Append(@, f)]
    /\ UNCHANGED <<envVars, nextConn, nextCkt, blk, viewVars, raftVars, logVars, durVars, ghostVars>>

\* TCP ACKs for frames already read reach the sender once the reverse link works.
AckFlush(k, dir) ==
    /\ k \in DOMAIN conn /\ dir \in Dirs
    /\ conn[k].u[dir] > 0
    /\ RcvOpen(k, dir) /\ link[Rcv(k, dir)][Snd(k, dir)] = "ok"
    /\ conn' = [conn EXCEPT ![k].u[dir] = 0]
    /\ UNCHANGED <<envVars, ckt, nextConn, nextCkt, blk, viewVars, raftVars, logVars, durVars, ghostVars>>

\* The blocked hand-off completes (send loop drained a frame) or returns
\* ErrShutdown because the pair halted (srv.go:2539-2547).
UnblockSend(n) ==
    /\ up[n] /\ ~stopped[n] /\ blk[n].pend /= <<>>
    /\ LET p  == Head(blk[n].pend)
           op == IF p.dir = "da" THEN conn[p.k].dOpen ELSE conn[p.k].aOpen
       IN \/ /\ ~op
             /\ blk' = [blk EXCEPT ![n].pend = Tail(@)]
             /\ UNCHANGED conn
          \/ /\ op /\ (Occ(conn, p.k, p.dir) < SendBufCap \/ Absorbing(conn, p.k, p.dir))
             /\ conn' = [conn EXCEPT ![p.k].q[p.dir] = Append(@, p.f)]
             /\ blk' = [blk EXCEPT ![n].pend = Tail(@)]
    /\ UNCHANGED <<envVars, ckt, nextConn, nextCkt, viewVars, raftVars, logVars, durVars, ghostVars>>

\* The inline auto-cli dial inside a send (D-29) ends: it fails (OS SYN
\* timeout ~127 s, refused, or the peer's ephemeral client port for an
\* accepted circuit) and the frame is lost, or, for a circuit this node
\* dialed, it reaches the acceptor address and the frame rides the new
\* connection (routed by address, possibly to a newer incarnation: D-38).
DialResolve(n) ==
    /\ up[n] /\ ~stopped[n] /\ blk[n].dial /= Nil
    /\ LET c == blk[n].dial.c
           f == blk[n].dial.f
           m == ckt[c].a
           a == ckt[c].ad
       IN \/ /\ blk' = [blk EXCEPT ![n].dial = Nil]
             /\ UNCHANGED <<conn, nextConn>>
          \/ /\ ckt[c].d = n /\ up[m] /\ ~stopped[m] /\ addr[m] = a
             /\ link[n][m] = "ok" /\ link[m][n] = "ok"
             /\ DialerConns(conn, n, m, a) = {}
             /\ conn' = conn @@ (nextConn :> NewConn(n, m, a, <<f>>))
             /\ nextConn' = nextConn + 1
             /\ blk' = [blk EXCEPT ![n].dial = Nil]
    /\ UNCHANGED <<envVars, ckt, nextCkt, viewVars, raftVars, logVars, durVars, ghostVars>>

\* While the receiver's event loop is stalled, its transport drops frames:
\* the read loop after 1 s + 4 s (srv.go:991-1015), or the pump after 2 s,
\* which also closes the circuit (pump.go:254-276).  D-32.
StalledReceiverDrop(k, dir, i, closeIt) ==
    /\ k \in DOMAIN conn /\ dir \in Dirs /\ closeIt \in BOOLEAN
    /\ CanRead(k, dir, i)
    /\ LET r == Rcv(k, dir)
           c == FrameAt(k, dir, i).cid
       IN /\ blk[r].pend /= <<>> \/ blk[r].dial /= Nil
          /\ ~StaleTarget(k, dir, i)
          /\ EpSt(c, r) = EpOpen
          /\ IF closeIt
             THEN /\ conn' = BgSendOne(Consume(conn, k, dir, i), r, c, [type |-> "END", from |-> <<r, inc[r]>>])
                  /\ ckt' = SetEp(ckt, c, r, EpClosed)
             ELSE /\ conn' = Consume(conn, k, dir, i)
                  /\ UNCHANGED ckt
    /\ UNCHANGED <<envVars, nextConn, nextCkt, blk, viewVars, raftVars, logVars, durVars, ghostVars>>

\* Frames for a circuit the receiving pump no longer knows are dropped
\* (pump.go:215-227): a black hole for the sender (D-37, D-39).
DropUnroutable(k, dir, i) ==
    /\ k \in DOMAIN conn /\ dir \in Dirs
    /\ CanRead(k, dir, i)
    /\ \/ /\ ~StaleTarget(k, dir, i)
          /\ EpSt(FrameAt(k, dir, i).cid, Rcv(k, dir)) \in {EpClosed, EpGone, "newdead"}
       \/ /\ StaleTarget(k, dir, i)
          /\ FrameAt(k, dir, i).m.type = "END"
    /\ conn' = Consume(conn, k, dir, i)
    /\ UNCHANGED <<envVars, ckt, nextConn, nextCkt, blk, viewVars, raftVars, logVars, durVars, ghostVars>>

\* A frame whose ToPeerID is not a local peer makes the server read loop
\* RETURN, tearing down the whole connection and every circuit on it
\* (srv.go:794-813; cli.go:526-538).  D-38.
StaleTargetTeardown(k, dir, i) ==
    /\ k \in DOMAIN conn /\ dir \in Dirs
    /\ CanRead(k, dir, i)
    /\ StaleTarget(k, dir, i)
    /\ FrameAt(k, dir, i).m.type /= "END"
    /\ conn' = [conn EXCEPT ![k].aOpen = IF dir = "da" THEN FALSE ELSE @,
                            ![k].dOpen = IF dir = "ad" THEN FALSE ELSE @,
                            ![k].q = [x \in Dirs |-> <<>>], ![k].u = [x \in Dirs |-> 0]]
    /\ UNCHANGED <<envVars, ckt, nextConn, nextCkt, blk, viewVars, raftVars, logVars, durVars, ghostVars>>

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

\* CallPeerError (a close with a reason): delivered to ckt.Errors, forwarded
\* to cktHasError, ignored by Tube (`if false`, tube/tube.go:1992-2004): the
\* circuit stays open on this side.  D-37.
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
    /\ LET s == [NS(n) EXCEPT !.aud = @ \cup {c}]
       IN WriteBack(n, AddToCktall(s, n, c), conn,
                    SetEp(ckt, c, n, IF EpSt(c, n) = EpNew THEN EpOpen ELSE EpClosed))

\* <-s.peerLeftCh: cleanupGoneCktP + deleteFromCktAll (tube/tube.go:1262-1266,
\* 2392-2423, 2552-2593); entries are removed only if they still refer to it.
HandlePeerLeft(n, c) ==
    /\ c \in DOMAIN ckt
    /\ Running(n)
    /\ EpSt(c, n) = EpClosed
    /\ LET s  == NS(n)
           s1 == DeleteFromCktAll([s EXCEPT !.aud = @ \ {c}], PeerKeyOf(c, n), c)
       IN WriteBack(n, s1, conn, SetEp(ckt, c, n, EpGone))

\* <-s.pruneDupClientCktCheckCh (every 20 s): globalPruneCktCheck
\* (tube/tube.go:2425-2492).  Every audited circuit not referenced from
\* cktall is closed with ErrPruned -- replica circuits included, no age
\* check -- which reaches the peer as CallPeerError (D-37, D-39).
AuditGC(n) ==
    /\ Running(n)
    /\ LET s     == NS(n)
           refd  == {s.ca[k] : k \in Keys} \ {NoCkt}
           prune == {c \in s.aud : c \notin refd /\ EpSt(c, n) = EpOpen}
       IN /\ prune /= {}
          /\ WriteBack(n, [s EXCEPT !.closes = [j \in 1..Cardinality(prune) |->
                                       [c |-> SetToSeq(prune)[j], why |-> "pruned"]]],
                       conn, ckt)

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
\* Only a live process holds a lease: Crash leaves the dead leader's volatile
\* peers record in place (reset at Restart), and LeaseCreditExpire needs up[l].
NoCreditAt(n) == \A l \in Server : up[l] => ~HasCredit(l, <<n, inc[n]>>)

LeaderReachesMe(n) ==
    \E l \in Server :
        /\ leaderKey[n] = <<l, inc[l]>> /\ up[l] /\ role[l] = Leader /\ Running(l)
        /\ par[l] = "ok" /\ currentTerm[l] >= currentTerm[n]
        /\ link[l][n] = "ok" /\ link[n][l] = "ok"
        /\ LET c == cktall[l][<<n, inc[n]>>] IN
             /\ c /= NoCkt /\ EpSt(c, l) = EpOpen /\ EpSt(c, n) = EpOpen
             /\ RouteOf(conn, l, c) /= 0
             /\ conn[RouteOf(conn, l, c)].dOpen /\ conn[RouteOf(conn, l, c)].aOpen

ElectionTimerMayFire(n) ==
    \/ role[n] = Leader
    \/ /\ NoCreditAt(n)
       /\ (TimelyHeartbeats => ~LeaderReachesMe(n))

\* leader branch of <-s.electionTimeoutCh (tube/tube.go:1452-1567): §6.2
\* step-down if the (quorum-1)-th newest pong (any acker, by PeerID,
\* tube/pongpq.go:21-46) is stale; skipped entirely when the leader is not in
\* its committed MC (1475-1478, D-16); an empty pong queue never steps down.
LeaderPongCheck(s0, n) ==
    LET s       == OnLeaderIsCurrentMCcommitted(s0, n)                   \* side effect of 17340-17347
        notInMC == s.cfg.com /\ n \notin s.cfg.mem
        sz      == Cardinality(s.cfg.mem)
        quor    == sz \div 2                                             \* s.quorum() - 1
        want    == /\ ~notInMC /\ sz > 1 /\ quor > 0
                   /\ s.aged                                             \* 1497-1503 leader for >= followerDownDur
                   /\ s.pong.all /= {}
                   /\ Cardinality(s.pong.fresh) < quor                   \* 1514-1517
    IN IF want THEN BecomeFollower([s EXCEPT !.leader = Nil], n, s.term, Nil, TRUE)   \* 1540-1553
       ELSE s

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

\* The empty heartbeat (8872-8886) is sent only if
\*   mcVersStale || immediately || time.Since(foll.LastHeardAnything) > leaderBeatDur.
\* LastHeardAnything is set by every current-term ack the leader accepts
\* (tube/tube.go:9959) -- exactly where AckStep grants lease credit -- and
\* the lease window (~0.8 s) is longer than leaderBeatDur (200 ms).  With
\* abstract time the leader may therefore skip the empty heartbeat to k only
\* while k holds credit, and never when k's last reported MC is stale
\* (mcVersStale, 8857-8860: cktP.MC != nil && MC.VersionGT(cktP.MC)) or the
\* call is immediate (peerJoin).  A catch-up AE (8870-8871) is never skipped.
HBMcStale(s, n, k) ==
    LET sn == SeenOf(s, n, s.ca[k]) IN sn /= Nil /\ VersionGT(s.cfg, sn)
HBMaySkip(s, n, k) ==
    /\ s.peers[k] /= Nil /\ s.peers[k].credit
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
       ELSE IF s2.par = "unknown" THEN s2                                \* 8740-8743 parBlocked
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
       IN WriteBack(n, IF c = NoCkt THEN s ELSE AddOut(s, c, rep), Consume(conn, k, dir, i), ckt)

\* PreVote reply -> tallyPreVote
HandlePreVote(k, dir, i) ==
    /\ k \in DOMAIN conn /\ dir \in Dirs
    /\ TubeFrame(k, dir, i) /\ FrameAt(k, dir, i).m.type = "PV"
    /\ Running(Rcv(k, dir))
    /\ LET n == Rcv(k, dir) IN
       WriteBack(n, TallyPreVote(NSJ(k, dir, i), n, FrameAt(k, dir, i).m), Consume(conn, k, dir, i), ckt)

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
           s3     == IF ~denyLo /\ ~other THEN [s2 EXCEPT !.save = TRUE] ELSE s2   \* 11426-11428
           rep    == [type |-> "V", from |-> <<n, inc[n]>>, cand |-> req.from,
                      cterm |-> s3.term,                                 \* 11429 CandidatesTerm = our term
                      granted |-> grant, cfg |-> s.cfg,                  \* MC captured at the top (11353)
                      vterm |-> s.term]                                  \* FromPeerCurrentTerm before (11347)
           c      == s3.ca[req.from]                                     \* 11518-11523
       IN WriteBack(n, IF c = NoCkt THEN s3 ELSE AddOut(s3, c, rep), Consume(conn, k, dir, i), ckt)

\* Vote reply -> tallyVote (may becomeLeader)
HandleVote(k, dir, i) ==
    /\ k \in DOMAIN conn /\ dir \in Dirs
    /\ TubeFrame(k, dir, i) /\ FrameAt(k, dir, i).m.type = "V"
    /\ Running(Rcv(k, dir))
    /\ LET n == Rcv(k, dir) IN
       WriteBack(n, TallyVote(NSJ(k, dir, i), n, FrameAt(k, dir, i).m), Consume(conn, k, dir, i), ckt)

\* becomeLeader's saver.save after the noop0 broadcast (tube/tube.go:7473-7475)
BecomeLeaderSave(n) ==
    /\ up[n] /\ ~stopped[n] /\ blk[n].save
    /\ blk[n].pend = <<>> /\ blk[n].dial = Nil
    /\ WriteBack(n, [NS(n) EXCEPT !.save = TRUE], conn, ckt)

\* AppendEntries (tube/tube.go:7854-8689) incl. the PAR-blocked path (7855-7858)
HandleAppendEntries(k, dir, i) ==
    /\ k \in DOMAIN conn /\ dir \in Dirs
    /\ TubeFrame(k, dir, i) /\ FrameAt(k, dir, i).m.type = "AE"
    /\ Running(Rcv(k, dir))
    /\ LET n  == Rcv(k, dir)
           ae == FrameAt(k, dir, i).m
           s  == NSJ(k, dir, i)
       IN IF s.par = "unknown"
          THEN WriteBack(n, ParObserveAE(s, n, ae), Consume(conn, k, dir, i), ckt)
          ELSE LET r   == AEStep(s, n, ae)
                   tgt == r.s.ca[ae.from]                                \* ackAE 8711-8716 cktall[ae.FromPeerID]
               IN WriteBack(n, IF tgt = NoCkt THEN r.s ELSE AddOut(r.s, tgt, r.ack),
                            Consume(conn, k, dir, i), ckt)

\* AppendEntriesAck (tube/tube.go:9847-10134)
HandleAppendEntriesAck(k, dir, i) ==
    /\ k \in DOMAIN conn /\ dir \in Dirs
    /\ TubeFrame(k, dir, i) /\ FrameAt(k, dir, i).m.type = "AEA"
    /\ Running(Rcv(k, dir))
    /\ LET n == Rcv(k, dir) IN
       WriteBack(n, AckStep(NSJ(k, dir, i), n, FrameAt(k, dir, i).m, FrameAt(k, dir, i).cid),
                 Consume(conn, k, dir, i), ckt)

\* StateSnapshotEnclosed (tube/tube.go:17560-17649): accepted only from the
\* current leaderID/leaderName; one global reassembly slot (D-47).
HandleStateSnapshot(k, dir, i) ==
    /\ k \in DOMAIN conn /\ dir \in Dirs
    /\ TubeFrame(k, dir, i) /\ FrameAt(k, dir, i).m.type = "SNAP"
    /\ Running(Rcv(k, dir))
    /\ LET n  == Rcv(k, dir)
           sn == FrameAt(k, dir, i).m
           s  == NSJ(k, dir, i)
           fromLeader == s.leader /= Nil /\ sn.from = s.leader           \* 17564-17569
           apply(sx) == LET r == ApplySnapshot([sx EXCEPT !.slot = Nil], n, sn)   \* 17633-17646 + reset
                            t == r.s.ca[sn.ae.from]
                        IN IF r.ack = Nil \/ t = NoCkt THEN r.s ELSE AddOut(r.s, t, r.ack)
           s1 == IF ~fromLeader THEN s
                 ELSE IF sn.part = 1 THEN [s EXCEPT !.slot = [sid |-> sn.sid, last |-> 1]]   \* 17581-17589
                 ELSE IF sn.part = -2                                    \* 17605-17637 last part
                      THEN (IF s.slot /= Nil /\ s.slot.sid = sn.sid /\ s.slot.last + 1 = 2
                            THEN apply(s) ELSE [s EXCEPT !.slot = Nil])
                 ELSE apply(s)                                           \* 17639-17647 single part
       IN WriteBack(n, s1, Consume(conn, k, dir, i), ckt)

\* RequestStateSnapshot (PAR, par.go:2640-2654) -> handleRequestStateSnapshot
\* on the circuit it arrived on (no role check).
HandleRequestStateSnapshotMsg(k, dir, i) ==
    /\ k \in DOMAIN conn /\ dir \in Dirs
    /\ TubeFrame(k, dir, i) /\ FrameAt(k, dir, i).m.type = "RSS"
    /\ Running(Rcv(k, dir))
    /\ LET n == Rcv(k, dir) IN
       WriteBack(n, HandleRequestStateSnapshot(NSJ(k, dir, i), n, FrameAt(k, dir, i).cid),
                 Consume(conn, k, dir, i), ckt)

\* A client write reaches the leader after noop0 commits; batched by
\* replicateTicket/replicateBatch (6147-6349); one entry per batch here.
ClientRequest(n) ==
    /\ Running(n) /\ role[n] = Leader /\ par[n] = "ok" /\ noop0[n].done
    /\ WriteBack(n, ReplicateEntry(NS(n), n, "data"), conn, ckt)

\* Membership change submitted at the leader (tuberm/tubeadd without -f):
\* changeMembership (tube/tube.go:14307-14769).
ClientChangeMembership(n, op, x) ==
    /\ Running(n) /\ role[n] = Leader /\ par[n] = "ok"
    /\ op \in {"add", "remove"} /\ x \in Server
    /\ (op = "add" => x \notin config[n].mem)
    /\ (op = "remove" => x \in config[n].mem)
    /\ WriteBack(n, ChangeMembership(NS(n), n, [op |-> op, x |-> x]), conn, ckt)

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
                           committed, applied, leaders, ackedCfg, cfgHist, ackHigh, nextSid>>
       ELSE /\ base' = [base EXCEPT ![n] = [idx |-> lastApplied[n] - 1,
                                            term |-> log[n][lastApplied[n] - 1].term]]
            /\ UNCHANGED <<envVars, netVars, viewVars, raftVars, log, idLen, torn, par, durVars, ghostVars>>

\* ------------------------------------------------------------------------
\* Crash in the middle of a plain (unjournaled) WAL append: the entry bytes
\* are fsynced but the PAR sidecar identifiers are not published
\* (tube/wal.go:556-584, 443-464).  Restart then fences the node (D-49).
\* Overwrites/compaction/snapshot installs are undo-journaled and do not tear.
\* ------------------------------------------------------------------------
CrashWith(n, lg, keepId, s) ==
    /\ up' = [up EXCEPT ![n] = FALSE]
    /\ stopped' = [stopped EXCEPT ![n] = FALSE]
    /\ conn' = CrashConn(n)
    /\ ckt' = CrashCkt(n)
    /\ blk' = [blk EXCEPT ![n] = NoBlk]
    /\ log' = [log EXCEPT ![n] = lg]
    /\ idLen' = [idLen EXCEPT ![n] = keepId]
    /\ dTerm' = IF s.save THEN [dTerm EXCEPT ![n] = MergedTerm(n, s)] ELSE dTerm
    /\ dVote' = IF s.save THEN [dVote EXCEPT ![n] = MergedVote(n, s)] ELSE dVote
    /\ dCfg' = IF s.save THEN [dCfg EXCEPT ![n] = s.cfg] ELSE dCfg
    /\ dShadows' = IF s.save THEN [dShadows EXCEPT ![n] = s.sh] ELSE dShadows
    /\ dCommit' = IF s.save THEN [dCommit EXCEPT ![n] = s.ci] ELSE dCommit
    /\ UNCHANGED <<inc, addr, link, nextConn, nextCkt, viewVars, raftVars, base, torn, par, walGone,
                   committed, applied, leaders, ackedCfg, cfgHist, panics, ackHigh, nextSid>>

\* ------------------------------------------------------------------------
\* Storage faults beyond fail-stop (PAR's fault model, brief S9).  An append
\* fsyncs the entry AND publishes its identifier before it is acknowledged
\* (wal.go:579-585: s.sync fsyncs the WAL, then parlog.sync), so these are
\* disk faults found at the next start, not crash windows:
\*   "lastId": the last cnt published PAR identifiers are lost (a lost or
\*             torn write at the sidecar tail; the entry bytes survive);
\*   "wal":    the WAL and its .parlog are lost (RaftState -- term, vote, MC,
\*             CommitIndex -- and the KV checkpoint survive).
\* Restart then fences the node (parAfterRestore, par.go:2178-2211):
\* rebuild from a donor (ParRecoverFromDonor) or a snapshot.
\* ------------------------------------------------------------------------
CrashDiskFault(n, kind, cnt) ==
    /\ up[n] /\ kind \in {"lastId", "wal"} /\ cnt \in Nat \ {0}
    /\ (kind = "lastId" => idLen[n] - cnt >= base[n].idx)
    /\ up' = [up EXCEPT ![n] = FALSE]
    /\ stopped' = [stopped EXCEPT ![n] = FALSE]
    /\ conn' = CrashConn(n)
    /\ ckt' = CrashCkt(n)
    /\ blk' = [blk EXCEPT ![n] = NoBlk]
    /\ IF kind = "lastId"
       THEN /\ idLen' = [idLen EXCEPT ![n] = @ - cnt]
            /\ UNCHANGED <<log, base, walGone>>
       ELSE /\ log' = [log EXCEPT ![n] = <<>>]
            /\ idLen' = [idLen EXCEPT ![n] = 0]
            /\ base' = [base EXCEPT ![n] = [idx |-> 0, term |-> 0]]
            /\ walGone' = [walGone EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<inc, addr, link, nextConn, nextCkt, viewVars, raftVars, torn, par,
                   dTerm, dVote, dCfg, dShadows, dCommit, ghostVars>>

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
                 Len(log[n]), NS(n))
    /\ nextId' = nextId + 1

\* ------------------------------------------------------------------------
\* PAR recovery (par.go:2295-2420), ticked every 100-200 ms on the event loop
\* ------------------------------------------------------------------------

\* rebuild the first unidentified entry from a donor holding identical
\* checked bytes (parIdentifierCandidate / joinReply; 2323-2393).  The query
\* round trip is abstracted to: live circuit, both links up, donor running.
ParRecoverFromDonor(n, m) ==
    /\ Running(n) /\ par[n] = "unknown" /\ torn[n] /= <<>> /\ m /= n
    /\ LET t == Head(torn[n])
           c == byName[n][m]
       IN /\ c > 0 /\ EpSt(c, n) = EpOpen /\ EpSt(c, m) = EpOpen
          /\ up[m] /\ ~stopped[m] /\ link[n][m] = "ok" /\ link[m][n] = "ok"
          /\ m \in config[n].mem \cup shadows[n]                         \* parDonors 2247-2266
          /\ t.idx <= idLen[m] /\ t.idx > base[m].idx
          /\ log[m][t.idx].id = t.e.id
          /\ LET lg   == Append(log[n], t.e)
                 rest == Tail(torn[n])
                 floor == Max(Max(commitIndex[n], lastApplied[n]), Max(base[n].idx, config[n].ci))
             IN /\ log' = [log EXCEPT ![n] = lg]
                /\ idLen' = [idLen EXCEPT ![n] = Len(lg)]
                /\ torn' = [torn EXCEPT ![n] = rest]
                /\ par' = [par EXCEPT ![n] = IF rest = <<>> /\ floor <= Len(lg) THEN "ok" ELSE "unknown"]  \* 2395-2409
    /\ UNCHANGED <<envVars, netVars, viewVars, raftVars, base, durVars, ghostVars>>

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
    /\ UNCHANGED <<envVars, netVars, viewVars, role, currentTerm, votedFor, leaderKey,
                   pvTerm, pv2Term, pvYes, pvNo, vYes, vNo, peers, pong, leaderAged, noop0,
                   stallQ, config, shadows, commitIndex, lastApplied, lastAppliedTerm, snapSlot,
                   logVars, durVars, ghostVars>>

\* LastFullPing.Sent leaves the read-lease window (12353-12370)
LeaseCreditExpire(l, key) ==
    /\ up[l] /\ key \in Keys
    /\ HasCredit(l, key)
    /\ peers' = [peers EXCEPT ![l][key].credit = FALSE]
    /\ UNCHANGED <<envVars, netVars, viewVars, role, currentTerm, votedFor, leaderKey, sticky,
                   pvTerm, pv2Term, pvYes, pvNo, vYes, vNo, pong, leaderAged, noop0,
                   stallQ, config, shadows, commitIndex, lastApplied, lastAppliedTerm, snapSlot,
                   logVars, durVars, ghostVars>>

\* a pong older than followerDownDur (5.3 s > the lease window)
PongExpire(l, key) ==
    /\ up[l] /\ key \in pong[l].fresh
    /\ ~HasCredit(l, key)
    /\ pong' = [pong EXCEPT ![l].fresh = @ \ {key}]
    /\ UNCHANGED <<envVars, netVars, viewVars, role, currentTerm, votedFor, leaderKey, sticky,
                   pvTerm, pv2Term, pvYes, pvNo, vYes, vNo, peers, leaderAged, noop0,
                   stallQ, config, shadows, commitIndex, lastApplied, lastAppliedTerm, snapSlot,
                   logVars, durVars, ghostVars>>

\* leader for at least followerDownDur (1497-1503)
LeaderAge(l) ==
    /\ up[l] /\ role[l] = Leader /\ ~leaderAged[l]
    /\ leaderAged' = [leaderAged EXCEPT ![l] = TRUE]
    /\ UNCHANGED <<envVars, netVars, viewVars, role, currentTerm, votedFor, leaderKey, sticky,
                   pvTerm, pv2Term, pvYes, pvNo, vYes, vNo, peers, pong, noop0,
                   stallQ, config, shadows, commitIndex, lastApplied, lastAppliedTerm, snapSlot,
                   logVars, durVars, ghostVars>>

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
        \/ HandleStateSnapshot(k, dir, i)
        \/ HandleRequestStateSnapshotMsg(k, dir, i)
        \/ HandleEndCircuit(k, dir, i)
        \/ HandlePeerError(k, dir, i)
        \/ DropUnroutable(k, dir, i)
        \/ StaleTargetTeardown(k, dir, i)
        \/ HandleStartFrame(k, dir, i)
        \/ \E b \in BOOLEAN : StalledReceiverDrop(k, dir, i, b)
        \/ HandleAppendEntriesTornCrash(k, dir, i)

CoreNext ==
    \/ \E n \in Server :
        \/ Crash(n) \/ Stop(n) \/ Cont(n)
        \/ \E kind \in {"lastId", "wal"} : CrashDiskFault(n, kind, 1)
        \/ \E a \in Addrs : Restart(n, a)
        \/ UnblockSend(n) \/ DialResolve(n)
        \/ ElectionTimeout(n) \/ PreVotePhase2Timeout(n) \/ LeaderHeartbeat(n)
        \/ BecomeLeaderSave(n) \/ ClientRequest(n) \/ ClientRequestTornCrash(n)
        \/ Compact(n) \/ AuditGC(n) \/ ParRequestSnapshot(n)
        \/ StickyExpire(n) \/ LeaderAge(n)
        \/ \E m \in Server : DialAttempt(n, m) \/ ParRecoverFromDonor(n, m)
        \/ \E op \in {"add", "remove"}, x \in Server : ClientChangeMembership(n, op, x)
        \/ \E key \in Keys : LeaseCreditExpire(n, key) \/ PongExpire(n, key)
        \/ \E c \in DOMAIN ckt : HandleNewCircuit(n, c) \/ HandlePeerLeft(n, c) \/ HaltOnDeadConn(n, c)
    \/ \E x, y \in Server : LinkDown(x, y) \/ LinkUp(x, y)
    \/ \E k \in DOMAIN conn : \/ \E side \in {"d", "a"} : ConnAbort(k, side)
                             \/ \E dir \in Dirs : AckFlush(k, dir)
    \/ FrameNext


\* ========================================================================
\* INVARIANTS
\* ========================================================================

\* ---------------- type / structural ----------------
TypeOK ==
    /\ up \in [Server -> BOOLEAN] /\ stopped \in [Server -> BOOLEAN]
    /\ inc \in [Server -> Incs] /\ addr \in [Server -> Addrs]
    /\ link \in [Server -> [Server -> {"ok", "drop"}]]
    /\ role \in [Server -> {Follower, Candidate, Leader}]
    /\ currentTerm \in [Server -> Nat] /\ dTerm \in [Server -> Nat]
    /\ votedFor \in [Server -> Keys \cup {Nil}] /\ dVote \in [Server -> Keys \cup {Nil}]
    /\ leaderKey \in [Server -> Keys \cup {Nil}]
    /\ par \in [Server -> {"ok", "unknown"}] /\ walGone \in [Server -> BOOLEAN]
    /\ \A n \in Server :
         /\ cktall[n] \in [Keys -> Nat]
         /\ byName[n] \in [Server -> Nat \cup {Pending}]
         /\ cktReplica[n] \subseteq Keys
         /\ \A k \in Keys : peers[n][k] = Nil \/ peers[n][k] \in PeerRec
         /\ config[n].mem \subseteq Server /\ shadows[n] \subseteq Server
         /\ base[n].idx \in Nat /\ idLen[n] \in Nat
         /\ commitIndex[n] \in Nat /\ lastApplied[n] \in Nat
    /\ nextId \in Nat /\ nextConn \in Nat /\ nextCkt \in Nat

\* structural sanity (hold in every correct state)
LogBaseOK ==
    \A n \in Server :
        /\ base[n].idx <= Len(log[n])
        /\ (base[n].idx > 0 => log[n][base[n].idx].term = base[n].term)
        /\ idLen[n] <= Len(log[n])
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

\* S1 diagnostic (precondition of D-1): durable term covers the WAL.  Expected to fail.
DurableTermCoversLog == \A n \in Server : dTerm[n] >= LastTerm(n)

\* S2/S8 diagnostic: a leader's MatchIndex for a live follower incarnation
\* is evidence of a matching prefix.  Expected to fail (D-3, D-5).
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
    \A k \in DOMAIN conn : \A d \in Dirs : \A i \in 1..Len(conn[k].q[d]) :
        LET m == conn[k].q[d][i].m IN
        m.type = "AEA" => m.gx <= m.gt

\* S3: the production panic "terms should be monotone up" (D-91) is unreachable.
AppliedTermsMonotone == "TermsMonotoneUp" \notin panics

\* S8: entries a follower acknowledged (accept) in term t stay in its log
\* while it remains in term t (D-5).  A storage fault (CrashDiskFault) may
\* destroy acknowledged entries; the code's promise then is the PAR fence
\* (par = "unknown": no votes, no campaigns, no AE processing beyond
\* parObserveAE) until recovery, so fenced nodes are exempt.  An UNFENCED node
\* missing an entry it acknowledged in its current term still violates it.
AckedStays ==
    \A n \in Server : (up[n] /\ par[n] = "ok" /\ ackHigh[n].term = currentTerm[n]) =>
        \A c \in ackHigh[n].claims : c.idx <= Len(log[n]) /\ log[n][c.idx].id = c.id

\* S4 diagnostic: every quorum a leader uses consists of distinct voting
\* members of its config (self only if a member).  Expected to fail (D-14..D-17).
CommitCounted(l) == {k \in Keys : peers[l][k] /= Nil /\ KName(k) \in config[l].mem}
LeaseCounted(l)  == {k \in Keys : peers[l][k] /= Nil /\ peers[l][k].credit}
QuorumCountsDistinctVoters ==
    \A l \in Server : (up[l] /\ role[l] = Leader) =>
        /\ l \in config[l].mem
        /\ \A a, b \in CommitCounted(l) : a /= b => KName(a) /= KName(b)
        /\ \A a \in LeaseCounted(l) : KName(a) \in config[l].mem
        /\ \A a, b \in LeaseCounted(l) : a /= b => KName(a) /= KName(b)

\* leaderCanServeReadsLocally (tube/tube.go:12286-12379): any TUBE_REPLICA
\* record of s.peers with a fresh LastFullPing counts (no MC filter, no
\* name de-dup); a single-node cluster always has the lease (12295-12304).
LeaseHolds(l) ==
    /\ up[l] /\ role[l] = Leader /\ Running(l)
    /\ \/ ClusterSize(l) <= 1
       \/ /\ noop0[l].done                                               \* 12306-12312
          /\ Cardinality({k \in Keys : peers[l][k] /= Nil}) >= Quorum(l) - 1   \* 12313-12317
          /\ Cardinality(LeaseCounted(l)) >= Quorum(l) - 1               \* 12341-12370

\* S4/S10: a leader that can serve a local read under its lease has not been
\* overtaken: no entry of a higher term has been committed.
LeaseSafety ==
    \A l \in Server : LeaseHolds(l) => ~\E e \in committed : e.term > currentTerm[l]

\* S5: an acknowledged membership change is never superseded by a config
\* that lacks it, unless a later acknowledged change touched the same node.
AckedReconfigDurable ==
    \A a \in ackedCfg : \A n \in Server :
        (up[n] /\ VersionGT(config[n], a)
         /\ ~\E b \in ackedCfg : b.x = a.x /\ VersionGT(b, a))
        => IF a.op = "add" THEN a.x \in config[n].mem ELSE a.x \notin config[n].mem

\* S5: (ConfigTerm, ConfigVersion) determines one member set.
ConfigUnique ==
    \A a, b \in cfgHist : (a.ct = b.ct /\ a.cv = b.cv) => a.mem = b.mem

\* S5: a follower never runs the leader-only resubmit path (D-20).
NoFollowerLeaderOnlyPath == "OnlyForLeaderResubmit" \notin panics

\* every modeled panic site
NoPanic == panics = {}

\* ---------------- connection-level diagnostics ----------------

\* S7: a connection side closes only because its peer process died, a link
\* dropped, or the other side closed first (no collateral teardown, D-38).
NoCollateralTeardown ==
    [][\A k \in DOMAIN conn :
          LET ad == conn[k].aOpen /\ k \in DOMAIN conn' /\ ~conn'[k].aOpen
              dd == conn[k].dOpen /\ k \in DOMAIN conn' /\ ~conn'[k].dOpen
              healthy == /\ up[conn[k].a] /\ up[conn[k].d] /\ up'[conn[k].a] /\ up'[conn[k].d]
                         /\ inc[conn[k].a] = conn[k].ai /\ inc[conn[k].d] = conn[k].di
                         /\ link[conn[k].a][conn[k].d] = "ok" /\ link[conn[k].d][conn[k].a] = "ok"
                         /\ conn[k].aOpen /\ conn[k].dOpen
          IN (healthy /\ (ad \/ dd)) => FALSE]_coreVars

\* ========================================================================
\* KV state machine, leases and sessions (S10)
\* ========================================================================
\* Replicas independently apply one committed log order. Catch-up copies the
\* leader's in-memory entries, including apply-time mutations. Each replica
\* uses its own clock offset and drift bound where the Go code does.

CONSTANTS
    KVReplicas,    \* replica names
    KVClients,     \* client names (one session per client; sid = client)
    KVMaxTime,     \* bound on the global time
    KVLeaseDur,    \* LeaseRequestDur of leased writes / CAS
    KVOff,         \* KVOff[n] \in Nat: wall-clock offset of replica n (skew)
    KVDrift,       \* KVDrift[n]: replica n's own cfg.ClockDriftBound (actions.go:576)
    KVSessDur,     \* session duration stamped at creation
    KVMaxLog,      \* bound on committed entries
    KVMaxSerial,   \* bound on client serials
    KVNil

VARIABLES
    kvNow,       \* global time
    kvLeader,    \* current leader replica
    kvMem,       \* kvMem[n]: n's in-memory log (Seq of entries), mutated by n's apply
    kvApplied,   \* kvApplied[n]: LastApplied
    kvStore,     \* kvStore[n]: leaf of the single key or KVNil (leased.Leaf)
    kvDead,      \* kvDead[n]: leaves moved to table "dead" (by epoch)
    kvSess,      \* kvSess[n][c]: session entry or KVNil
    kvSerial,    \* kvSerial[c]: client's next serial
    kvBelieves,  \* kvBelieves[c]: LeaseUntilTm the client was told it holds (0 = none)
    kvMuts,      \* ghost: kvMuts[n] = (client, serial) pairs that mutated n's KV
    kvDup,       \* ghost: some (client, serial) mutated a replica's KV twice
    kvFirst,     \* ghost: first applied result per (client, serial) at the leader
    kvReply,     \* ghost: last reply the client received per (client, serial)
    kvTid        \* fresh ticket ids

kvVars == <<kvNow, kvLeader, kvMem, kvApplied, kvStore, kvDead, kvSess, kvSerial,
            kvBelieves, kvMuts, kvDup, kvFirst, kvReply, kvTid>>

KVClock(n) == kvNow + KVOff[n]
KVMax(a, b) == IF a >= b THEN a ELSE b

\* ------------------------------------------------------------------------
\* Entries: a Ticket as appended by the leader (prepOne, tube/tube.go:6351-6412
\* stamps RaftLogEntryTm = leader wall clock and LeaseUntilTm = tm + dur).
\*   op \in {"sessnew","lwrite","cas","read"}; cl = client (= SessionID and
\*   Leasor name); pid = LeasorPeerID (FromID); oldE = OldLeaseEpochCAS.
\* ------------------------------------------------------------------------
KVEntry(op, c, serial, tid, oldE) ==
    LET tm == KVClock(kvLeader) IN
    [op |-> op, cl |-> c, serial |-> serial, tid |-> tid, tm |-> tm,
     until |-> tm + KVLeaseDur, leasor |-> c, pid |-> c, autoDel |-> TRUE,
     oldE |-> oldE, res |-> "none"]

\* leased.Leaf of the key
KVLeaf(e, epoch, ver) ==
    [val |-> e.cl, leasor |-> e.leasor, pid |-> e.pid, until |-> e.until,
     epoch |-> epoch, ver |-> ver, autoDel |-> e.autoDel, widx |-> 0]

\* writeFailedSetCurrentVal (tube/actions.go:766-789): on a rejected write the
\* APPLYING replica overwrites the logged ticket's lease fields with the
\* current leaf, adding its own ClockDriftBound to LeaseUntilTm (784).
KVMutate(n, e, lf) ==
    [e EXCEPT !.leasor = lf.leasor, !.pid = lf.pid,
              !.until = lf.until + KVDrift[n], !.autoDel = lf.autoDel]

\* kvstoreWrite wet path (tube/actions.go:562-764) for entry e at log index i
\* on replica n with current leaf lf.  Returns [ok, lf, e] (e possibly mutated).
KVStoreWrite(n, i, e, lf) ==
    IF lf = KVNil                                                        \* 599-622 new key
    THEN [ok |-> TRUE, lf |-> [KVLeaf(e, 1, 1) EXCEPT !.widx = i], e |-> e]
    ELSE IF i <= lf.widx                                                 \* 626-635
    THEN [ok |-> FALSE, lf |-> lf, e |-> KVMutate(n, e, lf)]
    ELSE IF lf.leasor = "" \/ lf.until = 0                               \* 637-660 no current leasor
    THEN [ok |-> TRUE, lf |-> [KVLeaf(e, lf.epoch + 1, lf.ver + 1) EXCEPT !.widx = i], e |-> e]
    ELSE IF e.until <= lf.until                                          \* 679-685 non-increasing until
    THEN [ok |-> FALSE, lf |-> lf, e |-> KVMutate(n, e, lf)]
    ELSE IF lf.leasor = e.leasor /\ lf.pid = e.pid                       \* 687-710 renewal: epoch kept
    THEN [ok |-> TRUE, lf |-> [lf EXCEPT !.val = e.cl, !.until = e.until, !.widx = i,
                                         !.ver = @ + 1, !.autoDel = e.autoDel], e |-> e]
    ELSE IF e.tm > lf.until + KVDrift[n]                                 \* 717-749 takeover (own drift, D-53)
    THEN [ok |-> TRUE, lf |-> [KVLeaf(e, lf.epoch + 1, lf.ver + 1) EXCEPT !.widx = i], e |-> e]
    ELSE [ok |-> FALSE, lf |-> lf, e |-> KVMutate(n, e, lf)]             \* 751-763

\* doCAS (tube/tube.go:12016-12141).  The lease check uses log time; the
\* OldLeaseEpochCAS check is enforced even on an EXPIRED lease (the bypass
\* is commented out at 12074-12091).
KVDoCAS(n, i, e, lf) ==
    IF lf = KVNil THEN
        (IF e.oldE > 0 THEN [ok |-> FALSE, lf |-> lf, e |-> e]           \* 12037-12041 ErrKeyNotFound
         ELSE KVStoreWrite(n, i, e, lf))                                 \* 12042-12047
    ELSE LET inForce == lf.leasor /= "" /\ lf.until /= 0                 \* 12059-12061, validLease 12143-12151
                        /\ ~(e.tm > lf.until + KVDrift[n])
         IN IF inForce /\ lf.leasor /= e.leasor                          \* 12063-12068 (by NAME)
            THEN [ok |-> FALSE, lf |-> lf, e |-> KVMutate(n, e, lf)]
            ELSE IF e.oldE /= lf.epoch                                   \* 12106-12113
            THEN [ok |-> FALSE, lf |-> lf, e |-> KVMutate(n, e, lf)]
            ELSE KVStoreWrite(n, i, e, lf)                               \* 12136-12137

\* GetLeaf(autoDelete=true) (tube/kv.go:483-512): an expired AutoDelete leaf
\* is moved to table "dead" and deleted using the APPLYING replica's
\* time.Now() (no drift margin).  Used by doReadKey via KVStoreReadLeaf
\* (tube/kv.go:815-820, tube/tube.go:12153-12178).
KVExpired(n, lf) == lf /= KVNil /\ lf.autoDel /\ lf.leasor /= "" /\ lf.until < KVClock(n)

\* ------------------------------------------------------------------------
\* Apply one entry (commitWhatWeCan, tube/tube.go:10408-10669): op first,
\* then the session (no duplicate filter, 10608-10655), then
\* garbageCollectOldSessions against LastAppliedLogHLC, which merges the
\* entry HLC with the local physical clock (10440, 10657; 16267-16296).
\* ------------------------------------------------------------------------
KVApply(n) ==
    /\ kvApplied[n] < Len(kvMem[n])
    /\ LET i  == kvApplied[n] + 1
           e  == kvMem[n][i]
           lf == kvStore[n]
           r  == CASE e.op = "lwrite" -> KVStoreWrite(n, i, e, lf)
                   [] e.op = "cas"    -> KVDoCAS(n, i, e, lf)
                   [] e.op = "read"   -> IF KVExpired(n, lf)
                                         THEN [ok |-> TRUE, lf |-> KVNil, e |-> e]
                                         ELSE [ok |-> lf /= KVNil, lf |-> lf, e |-> e]
                   [] OTHER           -> [ok |-> TRUE, lf |-> lf, e |-> e]
           mutated == (e.op \in {"lwrite", "cas"} /\ r.ok) \/ (e.op = "read" /\ KVExpired(n, lf))
           key == <<e.cl, e.serial>>
           res == IF r.ok THEN "ok" ELSE "rejected"
           s1  == IF e.op = "sessnew"                                    \* applyNewSess 16237-16265
                  THEN [kvSess[n] EXCEPT ![e.cl] = [maxApplied |-> 0, endTm |-> e.tm + KVSessDur]]
                  ELSE IF kvSess[n][e.cl] = KVNil THEN kvSess[n]          \* 10615-10627 unknown: op already applied (D-56)
                  ELSE [kvSess[n] EXCEPT ![e.cl].maxApplied = KVMax(@, e.serial)]
           lnow == KVMax(KVClock(n), e.tm)                               \* HLC merge with local clock (D-55)
           s2  == [c \in KVClients |-> IF s1[c] /= KVNil /\ s1[c].endTm <= lnow THEN KVNil ELSE s1[c]]
       IN
       /\ kvStore' = [kvStore EXCEPT ![n] = r.lf]
       /\ kvDead' = [kvDead EXCEPT ![n] = IF e.op = "read" /\ KVExpired(n, lf) THEN @ \cup {lf.epoch} ELSE @]
       /\ kvMem' = [kvMem EXCEPT ![n][i] = [r.e EXCEPT !.res = res]]
       /\ kvApplied' = [kvApplied EXCEPT ![n] = i]
       /\ kvSess' = [kvSess EXCEPT ![n] = s2]
       /\ kvMuts' = IF mutated /\ e.op /= "sessnew" THEN [kvMuts EXCEPT ![n] = @ \cup {key}] ELSE kvMuts
       /\ kvDup' = (kvDup \/ (mutated /\ e.op /= "sessnew" /\ key \in kvMuts[n]))
       \* the leader replies (respondToClientTicketApplied); later copies overwrite
       \* the cached reply (D-54)
       /\ kvFirst' = IF n = kvLeader /\ e.op /= "sessnew" /\ ~\E x \in kvFirst : x.k = key
                     THEN kvFirst \cup {[k |-> key, res |-> res]} ELSE kvFirst
       /\ kvReply' = IF n = kvLeader /\ e.op /= "sessnew"
                     THEN {x \in kvReply : x.k /= key} \cup {[k |-> key, res |-> res]} ELSE kvReply
       /\ kvBelieves' = IF n = kvLeader /\ r.ok /\ e.op \in {"lwrite", "cas"}
                        THEN [kvBelieves EXCEPT ![e.cl] = e.until] ELSE kvBelieves
    /\ UNCHANGED <<kvNow, kvLeader, kvSerial, kvTid>>

\* ------------------------------------------------------------------------
\* Client submission at the leader: leaderDoneEarlyOnSessionStuff
\* (tube/tube.go:16300-16510) admits against APPLIED session state only;
\* a timed-out session is killed locally with time.Now() (16364-16379, D-55).
\* ------------------------------------------------------------------------
KVAdmit(c, serial, isRead) ==
    LET l == kvLeader
        ste == kvSess[l][c]
    IN IF ste = KVNil THEN "unknown"                                     \* 16327-16336
       ELSE IF KVClock(l) >= ste.endTm THEN "killed"                     \* 16364-16379
       ELSE IF ~isRead /\ serial <= ste.maxApplied THEN "rejected"       \* 16485-16501
       ELSE "ok"

KVAppend(e) ==
    /\ Len(kvMem[kvLeader]) < KVMaxLog
    /\ kvMem' = [kvMem EXCEPT ![kvLeader] = Append(@, e)]

KVCreateSession(c) ==
    /\ kvSess[kvLeader][c] = KVNil
    /\ KVAppend(KVEntry("sessnew", c, 0, kvTid, 0))
    /\ kvTid' = kvTid + 1
    /\ UNCHANGED <<kvNow, kvLeader, kvApplied, kvStore, kvDead, kvSess, kvSerial,
                   kvBelieves, kvMuts, kvDup, kvFirst, kvReply>>

\* op \in {"lwrite","cas","read"}; CAS uses the client's believed epoch
KVSubmit(c, op, oldE) ==
    /\ kvSerial[c] <= KVMaxSerial
    /\ LET adm == KVAdmit(c, kvSerial[c], op = "read") IN
       /\ IF adm = "ok"
          THEN /\ KVAppend(KVEntry(op, c, kvSerial[c], kvTid, oldE))
               /\ UNCHANGED kvSess
          ELSE IF adm = "killed"
          THEN /\ kvSess' = [kvSess EXCEPT ![kvLeader][c] = KVNil]       \* local kill, outside the log
               /\ UNCHANGED kvMem
          ELSE UNCHANGED <<kvMem, kvSess>>
    /\ kvSerial' = [kvSerial EXCEPT ![c] = @ + 1]
    /\ kvTid' = kvTid + 1
    /\ UNCHANGED <<kvNow, kvLeader, kvApplied, kvStore, kvDead, kvBelieves, kvMuts,
                   kvDup, kvFirst, kvReply>>

\* The client resubmits the SAME (SessionID, SessionSerial) with a new
\* TicketID while the first copy is not yet applied (tube/fuzz_test.go:1665):
\* admission only consults applied state, so a second copy is logged (D-54).
KVRetrySameSerial(c) ==
    /\ kvSerial[c] > 1
    /\ LET serial == kvSerial[c] - 1
           prev == {i \in 1..Len(kvMem[kvLeader]) :
                      kvMem[kvLeader][i].cl = c /\ kvMem[kvLeader][i].serial = serial}
       IN /\ prev /= {}
          /\ LET e0 == kvMem[kvLeader][CHOOSE i \in prev : TRUE] IN
             /\ KVAdmit(c, serial, e0.op = "read") = "ok"
             /\ KVAppend([KVEntry(e0.op, c, serial, kvTid, e0.oldE) EXCEPT !.oldE = e0.oldE])
    /\ kvTid' = kvTid + 1
    /\ UNCHANGED <<kvNow, kvLeader, kvApplied, kvStore, kvDead, kvSess, kvSerial,
                   kvBelieves, kvMuts, kvDup, kvFirst, kvReply>>

\* A follower receives the next entry as the leader currently holds it in
\* memory: the original copy, or -- for a catch-up after the leader applied
\* and rejected it -- the MUTATED copy (wal raftLog slices, tube/wal.go:1194,
\* tube/tube.go:9733; D-52).
KVShip(n) ==
    /\ n /= kvLeader
    /\ Len(kvMem[n]) < Len(kvMem[kvLeader])
    /\ LET e == kvMem[kvLeader][Len(kvMem[n]) + 1] IN
       kvMem' = [kvMem EXCEPT ![n] = Append(@, [e EXCEPT !.res = "none"])]
    /\ UNCHANGED <<kvNow, kvLeader, kvApplied, kvStore, kvDead, kvSess, kvSerial,
                   kvBelieves, kvMuts, kvDup, kvFirst, kvReply, kvTid>>

\* Leader change to a caught-up replica (its in-memory copies may be mutated)
KVFailover(n) ==
    /\ n /= kvLeader
    /\ Len(kvMem[n]) = Len(kvMem[kvLeader])
    /\ kvLeader' = n
    /\ UNCHANGED <<kvNow, kvMem, kvApplied, kvStore, kvDead, kvSess, kvSerial,
                   kvBelieves, kvMuts, kvDup, kvFirst, kvReply, kvTid>>

\* leaderServedLocalRead (tube/tube.go:11802-12009): a lease-holding leader
\* reads with autoDelete OUTSIDE the log (parUpdate commits directly,
\* tube/par.go:503-507) and refreshes the session locally (11855, 11994).
KVLeaderLocalRead(c) ==
    /\ kvSess[kvLeader][c] /= KVNil
    /\ LET l == kvLeader lf == kvStore[l] IN
       /\ kvStore' = [kvStore EXCEPT ![l] = IF KVExpired(l, lf) THEN KVNil ELSE lf]
       /\ kvDead' = [kvDead EXCEPT ![l] = IF KVExpired(l, lf) THEN @ \cup {lf.epoch} ELSE @]
       /\ kvSess' = [kvSess EXCEPT ![l][c].endTm = KVClock(l) + KVSessDur]
    /\ UNCHANGED <<kvNow, kvLeader, kvMem, kvApplied, kvSerial, kvBelieves, kvMuts,
                   kvDup, kvFirst, kvReply, kvTid>>

KVTick ==
    /\ kvNow < KVMaxTime
    /\ kvNow' = kvNow + 1
    /\ UNCHANGED <<kvLeader, kvMem, kvApplied, kvStore, kvDead, kvSess, kvSerial,
                   kvBelieves, kvMuts, kvDup, kvFirst, kvReply, kvTid>>

KV_Init ==
    /\ kvNow = 0
    /\ kvLeader = CHOOSE n \in KVReplicas : TRUE
    /\ kvMem = [n \in KVReplicas |-> <<>>]
    /\ kvApplied = [n \in KVReplicas |-> 0]
    /\ kvStore = [n \in KVReplicas |-> KVNil]
    /\ kvDead = [n \in KVReplicas |-> {}]
    /\ kvSess = [n \in KVReplicas |-> [c \in KVClients |-> KVNil]]
    /\ kvSerial = [c \in KVClients |-> 1]
    /\ kvBelieves = [c \in KVClients |-> 0]
    /\ kvMuts = [n \in KVReplicas |-> {}]
    /\ kvDup = FALSE
    /\ kvFirst = {}
    /\ kvReply = {}
    /\ kvTid = 1

KV_Next ==
    \/ KVTick
    \/ \E n \in KVReplicas : KVApply(n) \/ KVShip(n) \/ KVFailover(n)
    \/ \E c \in KVClients :
         \/ KVCreateSession(c)
         \/ KVRetrySameSerial(c)
         \/ KVLeaderLocalRead(c)
         \/ \E op \in {"lwrite", "read"} : KVSubmit(c, op, 0)
         \/ \E e \in 0..2 : KVSubmit(c, "cas", e)


\* ---------------- invariants (brief section 5 / companion 3.4) -----------

\* replicas that applied the same prefix hold the same KV (State Machine Safety
\* at the KV level).  Expected to fail: D-51 (skew), D-52 (mutated catch-up),
\* D-53 (per-node drift).
KVDeterminism ==
    \A n, m \in KVReplicas :
        kvApplied[n] = kvApplied[m] => (kvStore[n] = kvStore[m] /\ kvDead[n] = kvDead[m])

\* same for sessions (D-55)
KVSessDeterminism ==
    \A n, m \in KVReplicas : kvApplied[n] = kvApplied[m] => kvSess[n] = kvSess[m]

\* each (client, serial) mutates a replica's KV at most once (D-54)
KVExactlyOnce == ~kvDup

\* the reply a client got reflects the first applied copy (D-54, D-56)
KVReplyMatchesApply ==
    \A x \in kvReply : \A y \in kvFirst : x.k = y.k => x.res = y.res

\* no two clients simultaneously believe they hold the key's lease (D-51)
KVLeaseMutex ==
    ~\E c1, c2 \in KVClients : c1 /= c2 /\ kvBelieves[c1] > kvNow /\ kvBelieves[c2] > kvNow

KVTypeOK ==
    /\ kvNow \in 0..KVMaxTime
    /\ kvLeader \in KVReplicas
    /\ \A n \in KVReplicas : kvApplied[n] <= Len(kvMem[n])

\* ========================================================================
\* Czar client protocol (S11)
\* ========================================================================
\* Tube serves a linearizable CAS register on the czar key. A submitted CAS
\* may take effect after the caller stops waiting. Replies may be lost or
\* late. Lease deadlines are represented as remaining ticks, clamped below.

CONSTANTS
    CzMembers,    \* member process names
    CzLease,      \* leaseDurCzar (5 s, czar.go:149) in ticks
    CzMinLeft,    \* "left < 2*time.Second" threshold (czar.go:1062) in ticks
    CzDrift,      \* the Tube leader's ClockDriftBound in ticks
    CzMaxEpoch,   \* bound on LeaseEpoch (state-space bound)
    CzNil

\* remaining-time domain: below -(CzDrift+1) nothing changes any more
CzFloor == -(CzDrift + 1)
CzClamp(x) == IF x < CzFloor THEN CzFloor ELSE x

VARIABLES
    czReg,       \* the czar key's leaf: CzNil or [leasor, pid, epoch, rem]
    czState,     \* cState: "Unknown" | "AmCzar" | "NotCzar"
    czVers,      \* czar.vers: [E (CzarLeaseEpoch), rem (CzarLeaseUntilTm - now)]
    czQ,         \* CAS tickets submitted to Tube, not yet applied: [m, pid, prevE, orphan]
    czWait,      \* czWait[m]: m waits for the reply to its non-orphan CAS
    czRes,       \* czRes[m]: reply delivered to m (CzNil if none)
    czPid,       \* czPid[m]: PeerID incarnation of member m
    czUp,        \* czUp[m]: process alive
    czPanics     \* ghost: panic sites reached

czVars == <<czReg, czState, czVers, czQ, czWait, czRes, czPid, czUp, czPanics>>

\* ------------------------------------------------------------------------
\* The CAS a member issues (czar.go:1013-1039, 1295-1301): sessionless CAS
\* on the czar key, OldLeaseEpochCAS = vers.CzarLeaseEpoch, lease CzLease,
\* AutoDelete false.  Applied with Tube's doCAS (tube/tube.go:12016-12141)
\* and kvstoreWrite (tube/actions.go:562-764) at the leader's time (now).
\* Result: [ok, err, reg, epoch, rem, czarName] where for a rejection the
\* ticket carries the CURRENT leaf via writeFailedSetCurrentVal
\* (actions.go:766-789): LeaseEpoch, LeaseUntilTm + ClockDriftBound, and
\* CurValOnFailedWrite whose CzarName is the current leasor.
\* ------------------------------------------------------------------------
CzRej(lf) == [ok |-> FALSE, err |-> "rejected", reg |-> lf, epoch |-> lf.epoch,
              rem |-> CzClamp(lf.rem + CzDrift), czarName |-> lf.leasor]
CzWin(lf2) == [ok |-> TRUE, err |-> "none", reg |-> lf2, epoch |-> lf2.epoch,
               rem |-> lf2.rem, czarName |-> lf2.leasor]

CzApplyCAS(op) ==
    LET lf == czReg
        new(ep) == [leasor |-> op.m, pid |-> op.pid, epoch |-> ep, rem |-> CzLease]
    IN
    IF lf = CzNil THEN
        (IF op.prevE > 0
         THEN [ok |-> FALSE, err |-> "notfound", reg |-> lf, epoch |-> 0, rem |-> CzFloor, czarName |-> CzNil]  \* 12037-12041
         ELSE CzWin(new(1)))                                             \* 12042-12047 -> 599-622
    ELSE
      LET inForce == lf.rem + CzDrift >= 0                               \* validLease 12143-12151
      IN IF inForce /\ lf.leasor /= op.m THEN CzRej(lf)                  \* 12063-12068
         ELSE IF op.prevE /= lf.epoch THEN CzRej(lf)                     \* 12106-12113 (even if expired)
         ELSE IF CzLease <= lf.rem THEN CzRej(lf)                        \* 679-685 non-increasing until
         ELSE IF lf.leasor = op.m /\ lf.pid = op.pid                     \* 687-710 renewal, same epoch
         THEN CzWin([lf EXCEPT !.rem = CzLease])
         ELSE IF lf.rem + CzDrift < 0                                    \* 717-749 takeover: epoch++
         THEN CzWin(new(lf.epoch + 1))
         ELSE CzRej(lf)                                                  \* 751-763

\* contend / renew: submit a CAS and wait for it (5 s context)
CzSubmit(m) ==
    /\ czUp[m] /\ ~czWait[m] /\ czRes[m] = CzNil
    /\ czQ' = czQ \cup {[m |-> m, pid |-> czPid[m], prevE |-> czVers[m].E, orphan |-> FALSE]}
    /\ czWait' = [czWait EXCEPT ![m] = TRUE]
    /\ UNCHANGED <<czReg, czState, czVers, czRes, czPid, czUp, czPanics>>

\* Tube commits one submitted CAS; the reply reaches the member only if it
\* still waits for that ticket.
CzTubeApply(op) ==
    /\ op \in czQ
    /\ LET r == CzApplyCAS(op) IN
       /\ (r.ok => r.reg.epoch <= CzMaxEpoch)
       /\ czReg' = r.reg
       /\ czQ' = czQ \ {op}
       /\ czRes' = IF ~op.orphan /\ czUp[op.m] /\ czPid[op.m] = op.pid
                   THEN [czRes EXCEPT ![op.m] = r] ELSE czRes
       /\ czWait' = IF ~op.orphan /\ czUp[op.m] /\ czPid[op.m] = op.pid
                    THEN [czWait EXCEPT ![op.m] = FALSE] ELSE czWait
    /\ UNCHANGED <<czState, czVers, czPid, czUp, czPanics>>

\* The reply is lost or arrives after the 5 s timeout: czarTkt == nil ->
\* "continue fullRestart" (czar.go:1041-1044).  The CAS may still commit.
CzRespLost(m) ==
    /\ czUp[m] /\ czWait[m]
    /\ czWait' = [czWait EXCEPT ![m] = FALSE]
    /\ czQ' = {[o EXCEPT !.orphan = o.orphan \/ o.m = m] : o \in czQ}
    /\ UNCHANGED <<czReg, czState, czVers, czRes, czPid, czUp, czPanics>>

\* setVers (czar.go:221-260): refuses an expired version and a decreasing one
CzSetVers(m, E, rem) ==
    IF rem < 0 THEN "expired"                                            \* 223-225 now.After(until)
    ELSE IF E < czVers[m].E THEN "decreasing"                            \* 226-233
    ELSE "ok"

\* handle the CAS reply (czar.go:1046-1210)
CzHandleResult(m) ==
    /\ czUp[m] /\ czRes[m] /= CzNil
    /\ LET r == czRes[m] IN
       IF r.ok THEN                                                      \* 1049-1129
           IF r.rem < CzMinLeft                                          \* 1060-1068: too little left
           THEN /\ czState' = [czState EXCEPT ![m] = "Unknown"]
                /\ UNCHANGED <<czVers, czUp, czPanics>>
           ELSE IF CzSetVers(m, r.epoch, r.rem) = "ok"                   \* 1101-1119
           THEN /\ czState' = [czState EXCEPT ![m] = "AmCzar"]
                /\ czVers' = [czVers EXCEPT ![m] = [E |-> r.epoch, rem |-> r.rem]]
                /\ UNCHANGED <<czUp, czPanics>>
           ELSE /\ czState' = [czState EXCEPT ![m] = "Unknown"]          \* continue fullRestart
                /\ UNCHANGED <<czVers, czUp, czPanics>>
       ELSE IF r.err /= "rejected" THEN                                  \* 1145-1150 not rejectedWritePrefix
           /\ czState' = [czState EXCEPT ![m] = "Unknown"]
           /\ UNCHANGED <<czVers, czUp, czPanics>>
       ELSE IF r.czarName = m /\ r.rem > CzFloor                         \* 1161-1167 PANIC (D-63)
       THEN /\ czPanics' = czPanics \cup {"WhyAmICzarOnFailedWrite"}
            /\ czUp' = [czUp EXCEPT ![m] = FALSE]                        \* no recover: the member dies
            /\ UNCHANGED <<czState, czVers>>
       ELSE IF CzSetVers(m, r.epoch, r.rem) = "ok"                       \* 1170-1203 NotCzar
       THEN /\ czState' = [czState EXCEPT ![m] = "NotCzar"]
            /\ czVers' = [czVers EXCEPT ![m] = [E |-> r.epoch, rem |-> r.rem]]
            /\ UNCHANGED <<czUp, czPanics>>
       ELSE /\ czState' = [czState EXCEPT ![m] = "NotCzar"]              \* setVers refused: vers unchanged (D-62)
            /\ UNCHANGED <<czVers, czUp, czPanics>>
    /\ czRes' = [czRes EXCEPT ![m] = CzNil]
    /\ UNCHANGED <<czReg, czQ, czWait, czPid>>

\* expireCheckCh (czar.go:1070-1074): the czar notices its lease ran out
CzExpireCheck(m) ==
    /\ czUp[m] /\ czState[m] = "AmCzar" /\ czVers[m].rem < 0
    /\ czState' = [czState EXCEPT ![m] = "Unknown"]
    /\ UNCHANGED <<czReg, czVers, czQ, czWait, czRes, czPid, czUp, czPanics>>

\* one tick of real time: every remaining lease shrinks
CzTick ==
    /\ czReg' = IF czReg = CzNil THEN CzNil ELSE [czReg EXCEPT !.rem = CzClamp(@ - 1)]
    /\ czVers' = [m \in CzMembers |-> [czVers[m] EXCEPT !.rem = CzClamp(@ - 1)]]
    /\ czRes' = [m \in CzMembers |-> IF czRes[m] = CzNil THEN CzNil
                                     ELSE [czRes[m] EXCEPT !.rem = CzClamp(@ - 1)]]
    \* a tick changes something (avoid pure stuttering)
    /\ czReg' /= czReg \/ czVers' /= czVers \/ czRes' /= czRes
    /\ UNCHANGED <<czState, czQ, czWait, czPid, czUp, czPanics>>

CzCrash(m) ==
    /\ czUp[m]
    /\ czUp' = [czUp EXCEPT ![m] = FALSE]
    /\ czWait' = [czWait EXCEPT ![m] = FALSE]
    /\ czRes' = [czRes EXCEPT ![m] = CzNil]
    /\ czQ' = {[o EXCEPT !.orphan = o.orphan \/ o.m = m] : o \in czQ}
    /\ UNCHANGED <<czReg, czState, czVers, czPid, czPanics>>

\* restart: new PeerID, fresh vers (shipped binaries also pick a new name;
\* the model keeps the name and changes the PeerID)
CzRestart(m) ==
    /\ ~czUp[m]
    /\ czUp' = [czUp EXCEPT ![m] = TRUE]
    /\ czPid' = [czPid EXCEPT ![m] = @ + 1]
    /\ czState' = [czState EXCEPT ![m] = "Unknown"]
    /\ czVers' = [czVers EXCEPT ![m] = [E |-> 0, rem |-> CzFloor]]
    /\ UNCHANGED <<czReg, czQ, czWait, czRes, czPanics>>

Cz_Init ==
    /\ czReg = CzNil
    /\ czState = [m \in CzMembers |-> "Unknown"]
    /\ czVers = [m \in CzMembers |-> [E |-> 0, rem |-> CzFloor]]
    /\ czQ = {}
    /\ czWait = [m \in CzMembers |-> FALSE]
    /\ czRes = [m \in CzMembers |-> CzNil]
    /\ czPid = [m \in CzMembers |-> 0]
    /\ czUp = [m \in CzMembers |-> TRUE]
    /\ czPanics = {}

Cz_Next ==
    \/ CzTick
    \/ \E op \in czQ : CzTubeApply(op)
    \/ \E m \in CzMembers :
        \/ CzSubmit(m) \/ CzRespLost(m) \/ CzHandleResult(m) \/ CzExpireCheck(m)
        \/ CzCrash(m) \/ CzRestart(m)


\* ---------------- properties (companion section 4.3) ---------------------

\* at most one live member believes it is czar with an unexpired version
CzAtMostOneCzarBelief ==
    ~\E m1, m2 \in CzMembers :
        /\ m1 /= m2 /\ czUp[m1] /\ czUp[m2]
        /\ czState[m1] = "AmCzar" /\ czVers[m1].rem >= 0
        /\ czState[m2] = "AmCzar" /\ czVers[m2].rem >= 0

\* no member reaches a panic (D-63)
CzNoPanic == czPanics = {}

\* liveness: some live member eventually becomes czar (D-62)
CzSomeCzar == \E m \in CzMembers : czUp[m] /\ czState[m] = "AmCzar"

CzTypeOK ==
    /\ czState \in [CzMembers -> {"Unknown", "AmCzar", "NotCzar"}]
    /\ czWait \in [CzMembers -> BOOLEAN]

\* ========================================================================
\* Model-checking bounds, entry points and fairness
\* ========================================================================
\* Each MC specification advances one abstraction and holds the other two
\* in their initial states. Counter fc bounds injected faults and timers.
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
    MaxStallDrop,       \* transport drops toward a stalled receiver
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

vars == <<coreVars, kvVars, czVars, fc>>
mcVars == vars

Bump(f) == fc' = [fc EXCEPT ![f] = @ + 1]
BaseOnly == UNCHANGED <<kvVars, czVars>>
NoFc == UNCHANGED fc
KVOnly == UNCHANGED <<coreVars, czVars>>
CzOnly == UNCHANGED <<coreVars, kvVars>>

\* All three abstractions start in their own initial state. A specification
\* advances one of them. The unbounded relations leave the counters at FC0.
Init == CoreInit /\ KV_Init /\ Cz_Init /\ fc = FC0
Next == CoreNext /\ BaseOnly /\ NoFc
Spec == Init /\ [][Next]_vars

KV_Spec == Init /\ [][KV_Next /\ KVOnly /\ NoFc]_vars
Cz_Spec == Init /\ [][Cz_Next /\ CzOnly /\ NoFc]_vars

CktBase == NN * NN + 1
CktRoom == nextCkt < CktBase + MaxCkt
ConnRoom == nextConn < CktBase + MaxConn
QueuesBelow == \A k \in DOMAIN conn : \A d \in Dirs : Len(conn[k].q[d]) < MaxQueue
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
MCClientRequest(n) == fc.req < MaxClientReq /\ ClientRequest(n) /\ Bump("req") /\ BaseOnly
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
MCBecomeLeaderSave(n) == BecomeLeaderSave(n) /\ NoFc /\ BaseOnly
MCUnblockSend(n) == UnblockSend(n) /\ NoFc /\ BaseOnly
MCDialResolve(n) == DialResolve(n) /\ nextConn' <= CktBase + MaxConn /\ NoFc /\ BaseOnly
MCDialAttempt(n, m) == CktRoom /\ ConnRoom /\ DialAttempt(n, m) /\ NoFc /\ BaseOnly
MCParRecoverFromDonor(n, m) == ParRecoverFromDonor(n, m) /\ NoFc /\ BaseOnly
MCHandleNewCircuit(n, c) == HandleNewCircuit(n, c) /\ NoFc /\ BaseOnly
MCHandlePeerLeft(n, c)   == HandlePeerLeft(n, c) /\ NoFc /\ BaseOnly
MCHaltOnDeadConn(n, c)   == HaltOnDeadConn(n, c) /\ NoFc /\ BaseOnly
MCConnAbort(k, side)     == ConnAbort(k, side) /\ NoFc /\ BaseOnly
MCAckFlush(k, dir)       == AckFlush(k, dir) /\ NoFc /\ BaseOnly
MCStickyExpire(n)        == StickyExpire(n) /\ NoFc /\ BaseOnly
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
       \/ HandleStateSnapshot(k, dir, i)
       \/ HandleRequestStateSnapshotMsg(k, dir, i)
       \/ HandleEndCircuit(k, dir, i)
       \/ HandlePeerError(k, dir, i)
       \/ DropUnroutable(k, dir, i)
       \/ StaleTargetTeardown(k, dir, i)
       \/ HandleStartFrame(k, dir, i)
    /\ NoFc /\ BaseOnly

\* ========================================================================
\* BASE NEXT / SPEC
\* ========================================================================
MCNextNode(n) ==
    \/ MCCrash(n) \/ MCStop(n) \/ MCCont(n)
    \/ \E kind \in {"lastId", "wal"} : MCCrashDiskFault(n, kind)
    \/ \E a \in Addrs : MCRestart(n, a)
    \/ MCUnblockSend(n) \/ MCDialResolve(n)
    \/ MCElectionTimeout(n) \/ MCPreVotePhase2Timeout(n) \/ MCLeaderHeartbeat(n)
    \/ MCBecomeLeaderSave(n) \/ MCClientRequest(n) \/ MCClientRequestTornCrash(n)
    \/ MCCompact(n) \/ MCAuditGC(n) \/ MCParRequestSnapshot(n)
    \/ MCStickyExpire(n) \/ MCLeaderAge(n)
    \/ \E m \in Server : MCDialAttempt(n, m) \/ MCParRecoverFromDonor(n, m)
    \/ \E op \in {"add", "remove"}, x \in Server : MCClientChangeMembership(n, op, x)
    \/ \E key \in Keys : MCLeaseCreditExpire(n, key) \/ MCPongExpire(n, key)
    \/ \E c \in DOMAIN ckt : MCHandleNewCircuit(n, c) \/ MCHandlePeerLeft(n, c) \/ MCHaltOnDeadConn(n, c)

MCNextFrames ==
    \E k \in DOMAIN conn, dir \in Dirs : \E i \in FrameIdx(k, dir) :
        \/ MCFrame(k, dir, i)
        \/ MCHandleAppendEntriesTornCrash(k, dir, i)
        \/ \E b \in BOOLEAN : MCStalledReceiverDrop(k, dir, i, b)

MCNext ==
    \/ \E n \in Server : MCNextNode(n)
    \/ \E x, y \in Server : MCLinkDown(x, y) \/ MCLinkUp(x, y)
    \/ \E k \in DOMAIN conn : \/ \E side \in {"d", "a"} : MCConnAbort(k, side)
                             \/ \E dir \in Dirs : MCAckFlush(k, dir)
    \/ MCNextFrames

MCInit == Init

MCSpec == MCInit /\ [][MCNext]_mcVars

\* ========================================================================
\* KV (S10) NEXT / SPEC
\* ========================================================================
MCKVNext ==
    \/ KVTick /\ NoFc /\ KVOnly
    \/ \E n \in KVReplicas :
         \/ KVApply(n) /\ NoFc /\ KVOnly
         \/ KVShip(n) /\ NoFc /\ KVOnly
         \/ fc.kvFailover < MaxKVFailover /\ KVFailover(n) /\ Bump("kvFailover") /\ KVOnly
    \/ \E c \in KVClients :
         \/ KVCreateSession(c) /\ NoFc /\ KVOnly
         \/ fc.kvRetry < MaxKVRetry /\ KVRetrySameSerial(c) /\ Bump("kvRetry") /\ KVOnly
         \/ fc.kvLocalRead < MaxKVLocalRead /\ KVLeaderLocalRead(c) /\ Bump("kvLocalRead") /\ KVOnly
         \/ \E op \in {"lwrite", "read"} : KVSubmit(c, op, 0) /\ NoFc /\ KVOnly
         \/ \E e \in 0..2 : KVSubmit(c, "cas", e) /\ NoFc /\ KVOnly

MCSpecKV == MCInit /\ [][MCKVNext]_mcVars

\* ========================================================================
\* CZAR (S11) NEXT / SPEC
\* ========================================================================
MCCzNext ==
    \/ CzTick /\ NoFc /\ CzOnly
    \/ \E op \in czQ : CzTubeApply(op) /\ NoFc /\ CzOnly
    \/ \E m \in CzMembers :
         \/ CzSubmit(m) /\ NoFc /\ CzOnly
         \/ CzHandleResult(m) /\ NoFc /\ CzOnly
         \/ CzExpireCheck(m) /\ NoFc /\ CzOnly
         \/ fc.czLost < MaxCzLost /\ CzRespLost(m) /\ Bump("czLost") /\ CzOnly
         \/ fc.czCrash < MaxCzCrash /\ CzCrash(m) /\ Bump("czCrash") /\ CzOnly
         \/ fc.czRestart < MaxCzRestart /\ CzRestart(m) /\ Bump("czRestart") /\ CzOnly

MCSpecCzar == MCInit /\ [][MCCzNext]_mcVars

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
\* the properties carry the BoundHit escape.
\* ========================================================================
LiveElectionTimeout(n) == QueuesBelow /\ ElectionTimeout(n) /\ NoFc /\ BaseOnly
LiveLeaderHeartbeat(n) == QueuesBelow /\ LeaderHeartbeat(n) /\ NoFc /\ BaseOnly

MCNextLive ==
    \/ MCNext
    \/ \E n \in Server : LiveElectionTimeout(n) \/ LiveLeaderHeartbeat(n)

FrameKind(n, ty) ==
    \E k \in DOMAIN conn, dir \in Dirs :
        /\ Rcv(k, dir) = n
        /\ \E i \in FrameIdx(k, dir) : FrameAt(k, dir, i).m.type = ty /\ MCFrame(k, dir, i)

TransportFor(n) ==
    \/ \E k \in DOMAIN conn, dir \in Dirs :
          /\ Rcv(k, dir) = n
          /\ \E i \in FrameIdx(k, dir) :
                /\ (FrameAt(k, dir, i).m.type \in {"END", "PERR"} \/ StaleTarget(k, dir, i)
                    \/ EpSt(FrameAt(k, dir, i).cid, n) /= EpOpen)
                /\ MCFrame(k, dir, i)
    \/ MCUnblockSend(n) \/ MCDialResolve(n)
    \/ \E m \in Server : MCDialAttempt(n, m)
    \/ \E c \in DOMAIN ckt : MCHaltOnDeadConn(n, c)

Kernel == \E k \in DOMAIN conn : \/ \E side \in {"d", "a"} : MCConnAbort(k, side)
                                \/ \E dir \in Dirs : MCAckFlush(k, dir)

BaseFairness ==
    /\ \A n \in Server :
        /\ \A ty \in {"RPV", "PV", "RV", "V", "AE", "AEA", "SNAP", "RSS"} :
              WF_mcVars(FrameKind(n, ty))
        /\ WF_mcVars(LiveElectionTimeout(n))
        /\ WF_mcVars(LiveLeaderHeartbeat(n))
        /\ WF_mcVars(MCPreVotePhase2Timeout(n))
        /\ WF_mcVars(MCBecomeLeaderSave(n))
        /\ WF_mcVars(\E c \in DOMAIN ckt : MCHandleNewCircuit(n, c) \/ MCHandlePeerLeft(n, c))
        /\ WF_mcVars(TransportFor(n))
        /\ WF_mcVars(\E m \in Server : MCParRecoverFromDonor(n, m))
        /\ WF_mcVars(MCStickyExpire(n) \/ MCLeaderAge(n)
                     \/ \E key \in Keys : MCLeaseCreditExpire(n, key) \/ MCPongExpire(n, key))
    /\ WF_mcVars(Kernel)

LiveSpec == MCInit /\ [][MCNextLive]_mcVars /\ BaseFairness

\* czar: the contend/renew loop and Tube's apply run until done (czar.go:1013-1210)
CzFairness ==
    /\ \A m \in CzMembers : WF_mcVars(CzSubmit(m) /\ NoFc /\ CzOnly)
                          /\ WF_mcVars(CzHandleResult(m) /\ NoFc /\ CzOnly)
                          /\ WF_mcVars(CzExpireCheck(m) /\ NoFc /\ CzOnly)
    /\ WF_mcVars(\E op \in czQ : CzTubeApply(op) /\ NoFc /\ CzOnly)
    /\ WF_mcVars(CzTick /\ NoFc /\ CzOnly)

LiveSpecCzar == MCInit /\ [][MCCzNext]_mcVars /\ CzFairness

\* ========================================================================
\* LIVENESS PROPERTIES (modeling brief section 5)
\* ========================================================================
AllHealthy(S) == \A n \in S : up[n] /\ ~stopped[n] /\ \A m \in S : link[n][m] = "ok"
LeaderCommitting(S) == \E l \in S : up[l] /\ role[l] = Leader /\ noop0[l].done

\* S6/S7/S9/S4: a healthy majority eventually has a committing leader
EventuallyLeaderWithHealthyMajority ==
    (<>[]AllHealthy(LiveQ)) => <>(LeaderCommitting(LiveQ) \/ BoundHit)

\* S9: after crashes stop and everything is up and connected, some leader commits
EventuallyLeaderAfterCrashes ==
    (<>[]AllHealthy(Server)) => <>(LeaderCommitting(Server) \/ BoundHit)

PairHealthy == AllHealthy({LiveA, LiveB})
ReplicaConnected(x, y) ==
    LET key == <<y, inc[y]>>
        c   == cktall[x][key]
    IN /\ c /= NoCkt /\ key \in cktReplica[x]
       /\ EpSt(c, x) = EpOpen /\ EpSt(c, y) = EpOpen

\* S7: two healthy replicas eventually keep open replica circuits both ways
PeerConnectivityRestored ==
    (<>[]PairHealthy) =>
        (<>[](ReplicaConnected(LiveA, LiveB) /\ ReplicaConnected(LiveB, LiveA)) \/ <>BoundHit)

BlackHole(n, m) ==
    LET c == cktall[n][<<m, inc[m]>>] IN
    /\ c /= NoCkt /\ EpSt(c, n) = EpOpen
    /\ EpSt(c, m) \in {EpClosed, EpGone, "newdead"}

\* S7: n's current circuit to m does not stay dead at m's end
NoPersistentBlackHole ==
    (<>[]PairHealthy) => (<>[](~BlackHole(LiveA, LiveB) /\ ~BlackHole(LiveB, LiveA)) \/ <>BoundHit)

\* S6: a blocked event loop is eventually released (fails by design toward
\* a stopped-but-ACKing peer; see brief section 5)
SenderEventuallyUnblocked ==
    \A n \in Server : (up[n] /\ blk[n].pend /= <<>>) ~> (~up[n] \/ blk[n].pend = <<>>)

\* S4: a leader that hears from no voter eventually steps down
LeaderDeaf(l) == \A v \in Server \ {l} : link[v][l] = "drop"
StaleLeaderStepsDown ==
    \A l \in Server :
        (<>[]LeaderDeaf(l)) => [](role[l] = Leader => <>(role[l] /= Leader \/ ~up[l]))

\* S3: with everyone healthy, the leader's log eventually reaches every member
LogsCaughtUp ==
    \E l \in Server : up[l] /\ role[l] = Leader /\
        \A f \in config[l].mem : Ids(log[f]) = Ids(log[l])
CatchUpEventually ==
    (<>[]AllHealthy(Server)) => <>(LogsCaughtUp \/ BoundHit)

\* S8: a follower behind the leader's compaction base eventually installs a snapshot
NeedsSnap(f) == \E l \in Server : up[l] /\ role[l] = Leader /\ up[f] /\ f /= l
                                  /\ f \in config[l].mem /\ Len(log[f]) < base[l].idx
SnapshotEventuallyInstalled ==
    (<>[]AllHealthy(Server)) => \A f \in Server : [](NeedsSnap(f) => <>(~NeedsSnap(f) \/ BoundHit))

\* S11: some live member eventually becomes czar
EventuallyCzar ==
    (<>[](\E m \in CzMembers : czUp[m])) => <>CzSomeCzar

\* S6 diagnostic: not every live replica's event loop is stuck in a send or
\* an inline dial at the same time (the D-28 / D-32 stall cascade).
NoTotalStall ==
    (\E n \in Server : up[n]) =>
        ~\A n \in Server : up[n] => (blk[n].pend /= <<>> \/ blk[n].dial /= Nil)

\* ---------------- committed-data durability (added during bug hunting) ----
\* Raft's durability core: an entry a leader declared committed is durably held
\* (in the WAL; crashed nodes count) by a majority of the initial members, so
\* no single crash can lose it.  For cfgs without reconfiguration
\* (MaxReconfig = 0).  Analysis aid for the AckedStays finding.
CommittedOnQuorum ==
    \A e \in committed :
        LET holders == {n \in InitMembers : e.idx <= Len(log[n]) /\ log[n][e.idx].id = e.id}
        IN 2 * Cardinality(holders) > Cardinality(InitMembers)

\* ---------------- filtered S7 diagnostic (added during bug hunting) -------
\* NoCollateralTeardown minus the known D-38 cause: a teardown of a healthy
\* connection side that reads a data frame addressed to a stale incarnation
\* of the reader (StaleTargetTeardown with to /= Nil).  Still flags D-37
\* (CallPeerError with an empty ToPeerID, to = Nil) and any other cause.
D38Cause(k, dir) ==
    \E i \in FrameIdx(k, dir) :
        /\ CanRead(k, dir, i) /\ StaleTarget(k, dir, i)
        /\ FrameAt(k, dir, i).to /= Nil /\ FrameAt(k, dir, i).m.type /= "END"
NoCollateralTeardownExceptD38 ==
    [][\A k \in DOMAIN conn :
          LET ad == conn[k].aOpen /\ k \in DOMAIN conn' /\ ~conn'[k].aOpen
              dd == conn[k].dOpen /\ k \in DOMAIN conn' /\ ~conn'[k].dOpen
              healthy == /\ up[conn[k].a] /\ up[conn[k].d] /\ up'[conn[k].a] /\ up'[conn[k].d]
                         /\ inc[conn[k].a] = conn[k].ai /\ inc[conn[k].d] = conn[k].di
                         /\ link[conn[k].a][conn[k].d] = "ok" /\ link[conn[k].d][conn[k].a] = "ok"
                         /\ conn[k].aOpen /\ conn[k].dOpen
          IN (healthy /\ ((ad /\ ~D38Cause(k, "da")) \/ (dd /\ ~D38Cause(k, "ad")))) => FALSE]_coreVars

\* ---------------- filtered S3 diagnostic (added during bug hunting) -------
\* LcriSound minus the harmless case seen in MC_hunt_S3_diag SIM: an ack whose
\* LCRI exceeds the stale AE's log end only up to the follower's own
\* compaction base (entries <= base are committed, so every current leader
\* holds them).  Still flags D-6 proper: an LCRI above both the true common
\* prefix and the follower's base (e.g. the leader's base inside the
\* follower's divergent tail).
LcriSoundExceptBase ==
    \A k \in DOMAIN conn : \A d \in Dirs : \A i \in 1..Len(conn[k].q[d]) :
        LET m == conn[k].q[d][i].m IN
        m.type = "AEA" => m.gx <= Max(m.gt, base[m.from[1]].idx)

\* ---------------- filtered S4 diagnostic (added during bug hunting) -------
\* QuorumCountsDistinctVoters minus the D-15 conjunct found in MC_hunt_S4_diag
\* SIM (a shadow's ack counted for the read lease).  Still flags D-14 (a
\* removed leader counting itself) and D-17 (one replica counted twice).
QuorumCountsDistinctVotersExceptShadowLease ==
    \A l \in Server : (up[l] /\ role[l] = Leader) =>
        /\ l \in config[l].mem
        /\ \A a, b \in CommitCounted(l) : a /= b => KName(a) /= KName(b)
        /\ \A a, b \in LeaseCounted(l) : a /= b => KName(a) /= KName(b)

\* QuorumCountsDistinctVoters minus D-15 (shadow lease) and D-14 (a removed
\* leader keeps leading and counts itself, found in MC_hunt_S4_diag_noshadow
\* SIM): only D-17, one replica counted twice, remains.
QuorumCountsNoDuplicateReplica ==
    \A l \in Server : (up[l] /\ role[l] = Leader) =>
        /\ \A a, b \in CommitCounted(l) : a /= b => KName(a) /= KName(b)
        /\ \A a, b \in LeaseCounted(l) : a /= b => KName(a) /= KName(b)

\* ---------------- premise checks for vacuity (use as INVARIANT) ---------
NeverHealthyMajorityLeader == ~(AllHealthy(LiveQ) /\ LeaderCommitting(LiveQ))
NeverBlocked == \A n \in Server : blk[n].pend = <<>>
NeverNeedsSnap == \A f \in Server : ~NeedsSnap(f)
NeverCzar == ~CzSomeCzar
\* premise checks added during bug hunting (the triggering situation of each
\* liveness property is reachable; each must be reported VIOLATED)
NeverFencedWhileHealthy == ~(AllHealthy(Server) /\ \E n \in Server : up[n] /\ par[n] = "unknown")
NeverDeafLeader == ~\E l \in Server : up[l] /\ role[l] = Leader /\ LeaderDeaf(l)
NeverLaggingWhileHealthy ==
    ~(AllHealthy(Server) /\ (\E l \in Server : up[l] /\ role[l] = Leader) /\ ~LogsCaughtUp)
NeverPairDisconnected ==
    ~(PairHealthy /\ ~(ReplicaConnected(LiveA, LiveB) /\ ReplicaConnected(LiveB, LiveA)))
NeverBlackHole == ~(PairHealthy /\ (BlackHole(LiveA, LiveB) \/ BlackHole(LiveB, LiveA)))
NeverBlockedWithHealthyMajority == ~(AllHealthy(LiveQ) /\ \E n \in LiveQ : blk[n].pend /= <<>>)

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
MCView == <<coreVars, kvVars, czVars>>

=============================================================================
