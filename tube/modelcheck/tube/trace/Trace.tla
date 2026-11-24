------------------------------- MODULE Trace -------------------------------
(***************************************************************************)
(* Trace validation for tube.tla (Tube Raft + connection management).      *)
(* Category A: one NDJSON trace, cursor l over events in file order.        *)
(*                                                                          *)
(* Event envelope in the NDJSON file:                                       *)
(*   {"tag":"trace", "event":{                                              *)
(*      "name": <spec action name>,  "nid": <PeerName>, "inc": <int>,       *)
(*      "state": <post-action node snapshot>,                               *)
(*      "msg":   <message fields, frame events only>,                       *)
(*      "peer":  {"name","inc"} for circuit events, "dialer": bool,         *)
(*      "branch": <code path, HandleAppendEntries / handleAEAck>, ... }}   *)
(* The harness observes Raft and its connections. The KV, czar and Hermes  *)
(* variables stay at their initial values throughout replay.                *)
(***************************************************************************)
EXTENDS tube, Json, IOUtils, Sequences, TLC

\* ========================================================================
\* TRACE LOADING
\* ========================================================================
JsonFile ==
    IF "JSON" \in DOMAIN IOEnv THEN IOEnv.JSON
    ELSE "../traces/trace.ndjson"

TraceRecords == TLCEval(ndJsonDeserialize(JsonFile))
TraceLog == TLCEval(
    LET all == TraceRecords
    IN SelectSeq(all, LAMBDA x : /\ "tag" \in DOMAIN x /\ x.tag = "trace"
                                 /\ "event" \in DOMAIN x))

ASSUME Len(TraceLog) > 0

SeqToSet(sq) == {sq[j] : j \in DOMAIN sq}

\* Server and initial membership come from the trace (Trace.cfg overrides)
TraceNids == TLCEval(IF TraceRecords[1].tag = "config" THEN SeqToSet(TraceRecords[1].config.allNodes)
                      ELSE {TraceLog[j].event.nid : j \in 1..Len(TraceLog)})
TraceInitMembers == TLCEval(
    IF "members" \in DOMAIN TraceLog[1].event THEN SeqToSet(TraceLog[1].event.members)
    ELSE TraceNids)
TraceServer == TLCEval(TraceNids \cup TraceInitMembers)
TraceEndpoint == CHOOSE n \in TraceServer : TRUE
TraceMaxInc == TLCEval(MaxSet({0} \cup {r.event.inc : r \in SeqToSet(TraceLog)}))
TraceAddresses == TLCEval({InitAddr} \cup {r.event.addr : r \in {r \in SeqToSet(TraceLog) :
                                                       r.event.name = "Restart"}})

\* ========================================================================
\* CURSOR
\* ========================================================================
VARIABLES l, cidmap
traceVars == <<l,cidmap>>
ObservedCircuitRows == SelectSeq(TraceLog, LAMBDA r :
    /\ "cid" \in DOMAIN r.event /\ r.event.name /= "DialAttempt")
ObservedCircuitIDs == TLCEval({r.event.cid : r \in SeqToSet(ObservedCircuitRows)})
KnownEndIDs == TLCEval({r.event.cid : r \in {r \in SeqToSet(ObservedCircuitRows) :
                         /\ r.event.name = "HandleEndCircuit" /\ "to" \in DOMAIN r.event}})

logline == TraceLog[l]
ev == logline.event

IsEvent(name) == l <= Len(TraceLog) /\ ev.name = name
IsNodeEvent(name, n) == IsEvent(name) /\ ev.nid = n
StepTrace == l' = l + 1

Key(rec) == <<rec.name, rec.inc>>
\* absent PeerIDs are encoded as {"name": "", "inc": -1}
KeyOrNil(rec) == IF rec.name = "" THEN Nil ELSE Key(rec)

RoleMap(r) == CASE r = "FOLLOWER" -> Follower
                [] r = "CANDIDATE" -> Candidate
                [] r = "LEADER" -> Leader
                [] OTHER -> r

\* ========================================================================
\* POST-STATE VALIDATION
\* ========================================================================
\* Raft core of node n after the action
ValidateRaft(n) ==
    LET st == ev.state IN
    /\ role'[n] = RoleMap(st.role)
    /\ currentTerm'[n] = st.term
    /\ votedFor'[n] = KeyOrNil(st.votedFor)
    /\ leaderKey'[n] = KeyOrNil(st.leader)
    /\ commitIndex'[n] = st.commitIndex
    /\ commitIndexTerm'[n] = st.commitTerm
    /\ campaign'[n].held = st.restartHoldoff
    /\ (campaign'[n].deferred /= "none") = st.deferred
    /\ (campaign'[n].deferred = "election") = st.deferredElection
    /\ lastApplied'[n] = st.lastApplied
    /\ Len(log'[n]) = st.lli
    /\ LastTermL(log'[n], base'[n]) = st.llt
    /\ base'[n].idx = st.compactIndex
    /\ config'[n].ct = st.cfgTerm /\ config'[n].cv = st.cfgVer
    /\ config'[n].mem = SeqToSet(st.members)
    /\ par'[n] = (IF st.parUnknown THEN "unknown" ELSE IF ev.info.parActive THEN "ready" ELSE "ok")

\* connection views of node n after the action
ValidateViews(n) ==
    LET st == ev.state IN
    /\ {k \in Keys : cktall'[n][k] /= NoCkt} = {Key(r) : r \in SeqToSet(st.cktall)}
    /\ {m \in Server : byName'[n][m] = Pending} = SeqToSet(st.pendingNames)
    /\ {m \in Server : byName'[n][m] > 0} = SeqToSet(st.circuitNames)
    /\ cktReplica'[n] = {Key(r) : r \in SeqToSet(st.cktReplica)}

\* durable state (only for events that report it)
ValidateDurable(n) ==
    "dTerm" \in DOMAIN ev.state =>
        /\ dTerm'[n] = ev.state.dTerm
        /\ dVote'[n] = KeyOrNil(ev.state.dVote)

ValidatePostState(n) ==
    /\ ValidateRaft(n) /\ ValidateViews(n) /\ ValidateDurable(n)
    \* The caller of each logged synchronous Tube send has returned here.
    /\ blk'[n].pend = <<>>
    /\ config'[n].com = ev.info.cfgCommitted
    /\ config'[n].ci = ev.info.cfgCI
    /\ shadows'[n] = SeqToSet(ev.info.shadows)

\* leader progress record after handleAppendEntriesAck
ValidateAckPeer(n) ==
    "peerRec" \in DOMAIN ev =>
        LET k == Key(ev.peerRec) IN
        /\ peers'[n][k] /= Nil
        /\ peers'[n][k].mi = ev.peerRec.mi
        /\ peers'[n][k].lcri = ev.peerRec.lcri

\* ========================================================================
\* FRAME MATCHING
\* ========================================================================
\* a deliverable Tube frame toward n of type ty from the trace's sender,
\* agreeing with the message fields the trace captured
FrameFits(k, dir, i, n, ty) ==
    /\ Rcv(k, dir) = n
    /\ ("cid" \in DOMAIN ev.msg =>
        IF ev.msg.cid \in DOMAIN cidmap THEN cidmap[ev.msg.cid] = FrameAt(k,dir,i).cid ELSE FALSE)
    /\ TubeFrame(k, dir, i)
    /\ LET m == FrameAt(k, dir, i).m IN
       /\ m.type = ty
       /\ m.from = Key(ev.msg.from)
       /\ ("term" \in DOMAIN ev.msg =>
             (IF ty \in {"RPV", "RV", "PV", "V"} THEN m.cterm = ev.msg.term ELSE m.term = ev.msg.term))
       /\ ("prev" \in DOMAIN ev.msg => m.prev = ev.msg.prev)
       /\ ("nents" \in DOMAIN ev.msg => Len(m.ents) = ev.msg.nents)
       /\ ("commit" \in DOMAIN ev.msg => m.commit = ev.msg.commit)
       /\ ("granted" \in DOMAIN ev.msg => m.granted = ev.msg.granted)
       /\ ("rejected" \in DOMAIN ev.msg => m.rej = ev.msg.rejected)
       /\ ("lcri" \in DOMAIN ev.msg => m.lcri = ev.msg.lcri)
       /\ ("last" \in DOMAIN ev.msg => m.last = ev.msg.last)
       /\ ("part" \in DOMAIN ev.msg => m.part = ev.msg.part)

FrameEvent(name, ty, Act(_, _, _)) ==
    \E n \in Server :
        /\ IsNodeEvent(name, n)
        /\ \E k \in DOMAIN conn, dir \in Dirs : \E i \in FrameIdx(k, dir) :
              /\ FrameFits(k, dir, i, n, ty)
              /\ Act(k, dir, i)
              /\ ValidatePostState(n)
              /\ StepTrace

\* ========================================================================
\* ACTION WRAPPERS (one trace event type per base action)
\* ========================================================================
HandleRequestPreVoteIfLogged == FrameEvent("HandleRequestPreVote", "RPV", HandleRequestPreVote)
HandlePreVoteIfLogged        == FrameEvent("HandlePreVote", "PV", HandlePreVote)
HandleRequestVoteIfLogged    == FrameEvent("HandleRequestVote", "RV", HandleRequestVote)
HandleVoteIfLogged           == FrameEvent("HandleVote", "V", HandleVote)
HandleStateSnapshotIfLogged  == FrameEvent("HandleStateSnapshot", "SNAP", HandleStateSnapshot)
HandleRequestStateSnapshotIfLogged ==
    FrameEvent("HandleRequestStateSnapshot", "RSS", HandleRequestStateSnapshotMsg)

\* the branch name the code took is checked against AEStep's branch
HandleAppendEntriesIfLogged ==
    \E n \in Server :
        /\ IsNodeEvent("HandleAppendEntries", n)
        /\ \E k \in DOMAIN conn, dir \in Dirs : \E i \in FrameIdx(k, dir) :
              /\ FrameFits(k, dir, i, n, "AE")
              /\ ("branch" \in DOMAIN ev =>
                    IF par[n] /= "ok" THEN ev.branch = "ParObserve"
                    ELSE AEStep(NS(n), n, FrameAt(k, dir, i).m).branch = ev.branch)
              /\ HandleAppendEntries(k, dir, i)
              /\ ValidatePostState(n)
              /\ StepTrace

HandleAppendEntriesAckIfLogged ==
    \E n \in Server :
        /\ IsNodeEvent("HandleAppendEntriesAck", n)
        /\ \E k \in DOMAIN conn, dir \in Dirs : \E i \in FrameIdx(k, dir) :
              /\ FrameFits(k, dir, i, n, "AEA")
              /\ HandleAppendEntriesAck(k, dir, i)
              /\ ValidatePostState(n)
              /\ ValidateAckPeer(n)
              /\ StepTrace

NodeEvent(name, Act(_)) ==
    \E n \in Server : IsNodeEvent(name, n) /\ Act(n) /\ ValidatePostState(n) /\ StepTrace

ElectionTimeoutIfLogged      == NodeEvent("ElectionTimeout", ElectionTimeout)
PreVotePhase2TimeoutIfLogged == NodeEvent("PreVotePhase2Timeout", PreVotePhase2Timeout)

\* PeerIDs this step handed a frame to (the event sent list)
SentKeys == IF "sent" \in DOMAIN ev THEN {Key(x.to) : x \in SeqToSet(ev.sent)} ELSE {}

\* the time-gated empty heartbeats the code skipped are exactly the cktall
\* keys it sent nothing to; LeaderHeartbeatWith checks they were skippable.
\* via=peerJoin: the circuit-start fragment's peerJoin heartbeat
\* (leaderSendsHeartbeats(true), tube/tube.go:9246) never skips.
LeaderHeartbeatIfLogged ==
    \E n \in Server :
        /\ IsNodeEvent("LeaderHeartbeat", n)
        /\ LeaderHeartbeatWith(n, IF "via" \in DOMAIN ev THEN {} ELSE HBKeys(NS(n)) \ SentKeys)
        /\ ValidatePostState(n)
        /\ StepTrace
BecomeLeaderSaveIfLogged     == NodeEvent("BecomeLeaderSave", BecomeLeaderSave)
ClientRequestIfLogged        == NodeEvent("ClientRequest", ClientRequest)
\* Compact is captured MID-STEP: inside replicateBatch
\* after the leader's WAL append, or inside the follower's overwriteEntries
\* after the WAL write but before the commit/apply of that AE.  The spec
\* compacts as its own step just before the enclosing ClientRequest /
\* HandleAppendEntries, so lli/llt (new entries) and a follower's
\* commitIndex (raised early, 7998) are validated by that next event; here
\* the compaction base, the LastApplied it is computed from, and the role
\* and term are checked.
CompactIfLogged ==
    \E n \in Server :
        /\ IsNodeEvent("Compact", n)
        /\ Compact(n)
        /\ base'[n].idx = ev.state.compactIndex
        /\ lastApplied'[n] = ev.state.lastApplied
        /\ role'[n] = RoleMap(ev.state.role) /\ currentTerm'[n] = ev.state.term
        /\ StepTrace
AuditGCIfLogged              == NodeEvent("AuditGC", AuditGC)
ParRequestSnapshotIfLogged   == NodeEvent("ParRequestSnapshot", ParRequestSnapshot)

ClientChangeMembershipIfLogged ==
    \E n \in Server :
        /\ IsNodeEvent("ClientChangeMembership", n)
        /\ ClientChangeMembership(n, ev.op, ev.target)
        \* Every node in this fixture advertises the configured test address.
        /\ (ev.op = "add" /\ byName'[n][ev.target] = Pending =>
              pendAddr'[n][ev.target] = InitAddr)
        /\ ValidatePostState(n)
        /\ StepTrace

\* circuit events: the circuit is identified by its two endpoints and the side
CktIDMatches(c) == IF "cid" \in DOMAIN ev THEN IF ev.cid \in DOMAIN cidmap THEN cidmap[ev.cid] = c ELSE TRUE ELSE TRUE

CktOf(n) ==
    {c \in DOMAIN ckt : /\ CktIDMatches(c) /\ PeerKeyOf(c, n) = Key(ev.peer)
                        /\ (ckt[c].d = n) = ev.dialer}

HandleNewCircuitIfLogged ==
    \E n \in Server :
        /\ IsNodeEvent("HandleNewCircuit", n)
        /\ \E c \in CktOf(n) : HandleNewCircuit(n, c)
        /\ ValidatePostState(n)
        /\ StepTrace

HandlePeerLeftIfLogged ==
    \E n \in Server :
        /\ IsNodeEvent("HandlePeerLeft", n)
        /\ \E c \in CktOf(n) : HandlePeerLeft(n, c)
        /\ ValidatePostState(n)
        /\ StepTrace

\* watchdog reconnect attempt result ("ok": bool): success creates a circuit
\* whose dialer end Tube will register; a failure may strand a start frame
\* (dialer end already gone)
FastDial(n,m) ==
    LET a == IF wdReq[n][m] /= Nil THEN wdReq[n][m] ELSE PeerAddrOf(byName[n][m],n)
        k == AddressRoute(conn,n,m,a)
    IN /\ up[m] /\ ~stopped[m] /\ a = addr[m] /\ link[n][m] = "ok" /\ link[m][n] = "ok"
       /\ IF k = 0 THEN TRUE ELSE /\ conn[k].dOpen /\ conn[k].aOpen
          /\ (IF conn[k].d = n THEN conn[k].ai ELSE conn[k].di) = inc[m]
          /\ \A d \in Dirs : conn[k].out[d] = <<>> /\ conn[k].writer[d] = Nil /\ WireRoom(conn,k,d)
DialAttemptIfLogged ==
    \E n \in Server :
        /\ IsNodeEvent("DialAttempt", n)
        /\ DialAttempt(n, ev.target)
        /\ IF ev.ok THEN /\ nextCkt' > nextCkt
                         /\ ckt'[nextCkt].dSt = IF FastDial(n,ev.target) THEN EpNew ELSE EpDialWait
           ELSE nextCkt' > nextCkt => ckt'[nextCkt].dSt = EpGone
        /\ (ev.serviceUnavailable => nextCkt' = nextCkt)
        /\ IF ev.ok \/ "cid" \notin DOMAIN ev THEN TRUE
           ELSE IF ev.cid \notin ObservedCircuitIDs THEN nextCkt' = nextCkt
           ELSE /\ nextCkt' > nextCkt
                /\ ckt'[nextCkt].aSt = IF ev.cid \in KnownEndIDs THEN EpNew ELSE "unstarted"
        /\ StepTrace

ParRecoverFromDonorIfLogged ==
    \E n \in Server :
        /\ IsNodeEvent("ParRecoverFromDonor", n)
        /\ ParRecoverFromDonor(n, ev.donor)
        /\ par'[n] = (IF ev.state.parUnknown THEN "unknown" ELSE IF ev.info.parActive THEN "ready" ELSE "ok")
        /\ StepTrace

\* environment events (simnet fault injection / process lifecycle)
\* harness disk faults applied to the crashed node's files
CrashIfLogged ==
    \E n \in Server :
        /\ IsNodeEvent("Crash", n)
        /\ IF "diskFault" \notin DOMAIN ev THEN Crash(n)
           ELSE IF ev.diskFault = "lastIdentifierTorn"
           THEN CrashDiskFault(n, "lastId", IF "tornIdentifiers" \in DOMAIN ev THEN ev.tornIdentifiers ELSE 1)
           ELSE IF ev.diskFault = "walLost" THEN CrashDiskFault(n, "wal", 1)
           ELSE FALSE
        /\ StepTrace

RestartIfLogged ==
    \E n \in Server :
        /\ IsNodeEvent("Restart", n)
        /\ Restart(n, ev.addr)
        /\ inc'[n] = ev.inc
        /\ ValidatePostState(n)
        /\ StepTrace

LinkDownIfLogged ==
    IsEvent("LinkDown") /\ LinkDown(ev.from, ev.to) /\ StepTrace
LinkUpIfLogged ==
    IsEvent("LinkUp") /\ LinkUp(ev.from, ev.to) /\ StepTrace
StopIfLogged == \E n \in Server : IsNodeEvent("Stop", n) /\ Stop(n) /\ StepTrace
ContIfLogged == \E n \in Server : IsNodeEvent("Cont", n) /\ Cont(n) /\ StepTrace

\* transport (rpc25519) events: the frame is identified by its receiver,
\* sender, wire type and ToPeerID (empty for a stranded dial's error frame)
WireType(m) == CASE m.type = "STARTACK" -> "CallPeerCircuitEstablishedAck"
                 [] m.type = "END"   -> "CallPeerEndCircuit"
                 [] m.type = "PERR"  -> "CallPeerError"
                 [] m.type = "START" -> "CallPeerStartCircuitAtMostOne"
                 [] OTHER            -> "CallPeerTraffic"
TransportFits(k, dir, i, n) ==
    /\ CktIDMatches(FrameAt(k,dir,i).cid)
    /\ Rcv(k, dir) = n
    /\ FrameAt(k, dir, i).m.from = Key(ev.msg.from)
    /\ ("type" \in DOMAIN ev.msg => WireType(FrameAt(k, dir, i).m) = ev.msg.type)
    /\ ("to" \in DOMAIN ev => FrameAt(k, dir, i).to = KeyOrNil(ev.to))
TransportFrameEvent(name, Act(_, _, _)) ==
    \E n \in Server :
        /\ IsNodeEvent(name, n)
        /\ \E k \in DOMAIN conn, dir \in Dirs : \E i \in FrameIdx(k, dir) :
              /\ TransportFits(k, dir, i, n)
              /\ Act(k, dir, i)
              /\ StepTrace

HandleEndCircuitIfLogged    == TransportFrameEvent("HandleEndCircuit", HandleEndCircuit)
HandlePeerErrorIfLogged     == TransportFrameEvent("HandlePeerError", HandlePeerError)
DropUnroutableIfLogged      == TransportFrameEvent("DropUnroutable", DropUnroutable)
DropStaleTargetIfLogged == TransportFrameEvent("DropStaleTarget", DropUnroutable)

ConnAbortIfLogged ==
    \E n \in Server :
        /\ IsNodeEvent("ConnAbort", n)
        /\ \E k \in DOMAIN conn :
              /\ (conn[k].d = n /\ conn[k].di = inc[n] /\ conn[k].a = ev.peer.name)
                 \/ (conn[k].a = n /\ conn[k].ai = inc[n] /\ conn[k].d = ev.peer.name)
              /\ ConnAbort(k, IF conn[k].d = n THEN "d" ELSE "a")
        /\ StepTrace

\* ========================================================================
\* SILENT ACTIONS (no trace event; tightly constrained to the next event)
\* ========================================================================
NextIsNode(n) == l <= Len(TraceLog) /\ ev.nid = n

\* Time passing the code does not log.  Applied lazily and deterministically:
\* only the abstract-time effects the NEXT event's recorded outcome requires,
\* one at a time in a fixed order, so no branch can dead-end on a time step
\* the real run did not need (TraceMatched requires every branch to finish).
SentOps(op) == IF "sent" \in DOMAIN ev THEN {x \in SeqToSet(ev.sent) : x.op = op} ELSE {}
SentGranted(op) == \E x \in SentOps(op) : "granted" \in DOMAIN x /\ x.granted

\* the leader's election timeout stepped down on stale pongs (tube/tube.go:1540-1553)
PongStepDown(n) == ev.name = "ElectionTimeout" /\ "exit" \in DOMAIN ev
                   /\ ev.exit = "leaderPongStepDown"
\* the event needs n to have no sticky leader: a granted (pre-)vote, or a
\* RequestVote that moved n's term (haveStickyLeader denies before both)
NeedStickyOff(n) ==
    /\ sticky[n]
    /\ \/ ev.name = "HandleRequestPreVote" /\ SentGranted("PV")
       \/ ev.name = "HandleRequestVote" /\ (SentGranted("V") \/ ev.state.term > currentTerm[n])
\* lease credits that must have run out: every live leader's credit for n
\* before n's own election timer fires or its stickiness ends
\* (ElectionTimerMayFire / StickyExpire), or all of a stepping-down leader's
\* credits (PongExpire needs ~HasCredit)
NeedCreditOff(n) ==
    LET me == <<n, inc[n]>> IN
    IF \/ ev.name = "ElectionTimeout" /\ role[n] /= Leader
       \/ NeedStickyOff(n)
    THEN {p \in Server \X {me} : up[p[1]] /\ CanStillCredit(p[1], p[2])}
    ELSE IF PongStepDown(n) THEN {p \in {n} \X Keys : HasCredit(p[1], p[2])}
    ELSE {}
NeedPongOff(n) == IF PongStepDown(n) THEN pong[n].fresh ELSE {}
NeedAged(n) == PongStepDown(n) /\ ~leaderAged[n]

\* an event may fire only once the time effects it needs are all in place
\* (otherwise it could run with, e.g., stickiness still on and leave a
\* dead branch)
NeedWindowElapsed(n) ==
    /\ "state" \in DOMAIN ev /\ ~ev.state.restartHoldoff
    /\ campaign[n].held

NeedTime(n) == NeedWindowElapsed(n) \/ NeedCreditOff(n) /= {} \/ NeedPongOff(n) /= {} \/ NeedAged(n) \/ NeedStickyOff(n)
\* In the simnet profile a new process can receive a packet addressed to the
\* old PeerID over a fresh reverse socket. A send-side close can precede any
\* corresponding read-loop error observed by the harness.
StaleWireRows == SelectSeq(TraceLog,LAMBDA z : z.event.name = "DropStaleTarget")
StaleWireIDs == TLCEval({z.event.cid : z \in SeqToSet(StaleWireRows)})
NeedOldSocketClose ==
    IF l > Len(TraceLog) THEN {} ELSE
    LET n == ev.nid
        send == IF "sent" \in DOMAIN ev THEN SeqToSet(ev.sent) ELSE {}
    IN {k \in DOMAIN conn :
       /\ conn[k].d = n /\ conn[k].di = inc[n] /\ conn[k].dOpen /\ ~conn[k].aOpen
       /\ conn[k].ai /= inc[conn[k].a]
       /\ \/ ev.name = "DialAttempt" /\ ev.ok /\ ev.target = conn[k].a
          \/ /\ \E r \in DOMAIN conn : conn[r].a = n /\ conn[r].ai = inc[n] /\ conn[r].aOpen
                             /\ conn[r].d = conn[k].a /\ conn[r].di = inc[conn[k].a]
             /\ \E x \in send : /\ x.cid \in StaleWireIDs /\ x.to.name = conn[k].a /\ x.to.inc = conn[k].ai}
TimeDone == NeedOldSocketClose = {} /\ \A n \in Server : NextIsNode(n) => ~NeedTime(n)

SilentTime ==
    \/ NeedOldSocketClose /= {} /\ ConnAbort(CHOOSE k \in NeedOldSocketClose : TRUE,"d")
    \/ \E n \in Server :
        /\ NextIsNode(n)
        /\ CASE NeedWindowElapsed(n) -> IF campaign[n].remaining > 1 THEN RestartWindowTick(n) ELSE RestartWindowElapsed(n)
             [] NeedCreditOff(n) /= {} ->
                  LET p == CHOOSE p \in NeedCreditOff(n) : TRUE IN LeaseCreditExpire(p[1], p[2])
             [] NeedPongOff(n) /= {} ->
                  PongExpire(n, CHOOSE k \in NeedPongOff(n) : TRUE)
             [] NeedAged(n) -> LeaderAge(n)
             [] NeedStickyOff(n) -> StickyExpire(n)
             [] OTHER -> FALSE

\* readable frames at n of wire type ty from sender key pk
FramesAt(n, pk, ty) ==
    UNION {{<<k, dir, i>> : i \in {j \in FrameIdx(k, dir) :
                                   /\ Rcv(k, dir) = n /\ CanRead(k, dir, j)
                                   /\ FrameAt(k, dir, j).m.type = ty
                                   /\ FrameAt(k, dir, j).m.from = pk}} : <<k, dir>> \in (DOMAIN conn) \X Dirs}
OldestOf(F) == CHOOSE f \in F : \A g \in F : FrameAt(f[1], f[2], f[3]).cid <= FrameAt(g[1], g[2], g[3]).cid

\* The acceptor's transport reads a stranded circuit-start frame (no trace
\* event; the harness logs the resulting HandleNewCircuit).  Only right
\* before a HandleNewCircuit (acceptor side) that has no circuit to
\* register yet; the oldest such frame first.
SilentStart ==
    \E n \in Server :
        /\ NextIsNode(n) /\ ev.name = "HandleNewCircuit" /\ ~ev.dialer
        /\ ~\E c \in CktOf(n) : EpSt(c, n) \in {EpNew, "newdead"}
        /\ LET F == FramesAt(n, Key(ev.peer), "START") IN
           F /= {} /\ HandleStartFrame(OldestOf(F)[1], OldestOf(F)[2], OldestOf(F)[3])

\* kernel / transport steps the harness does not log (TCP ACKs, send-loop
\* drains, registered circuits halted after their connection died)
\* handleNewCircuit returns early after addToCktall refuses a recent live
\* namesake. This branch has no registration log, but its END is observed at
\* the opposite endpoint. Materialize precisely that pending refusal.
SilentRegistrationRefusal ==
    /\ IsEvent("HandleEndCircuit")
    /\ LET n == ev.msg.from.name IN
       \E c \in DOMAIN ckt :
          /\ CktIDMatches(c) /\ EpSt(c,n) = EpNew /\ PeerKeyOf(c,n) = <<ev.nid,ev.inc>>
          /\ NameInUse(n,c) /\ HandleNewCircuit(n,c)

\* A real watchdog starts its next dial only after the recorded owner has
\* been quiet long enough. Age the associated key's shorter lease and pong
\* windows before the owner can cross that threshold.
SilentDialClock(n,m) ==
    LET c == byName[n][m] IN
    IF c <= 0 THEN FALSE
    ELSE LET key == PeerKeyOf(c,n) IN
      /\ c \notin watchSilent[n] /\ wdReq[n][m] = Nil
      /\ CASE HasCredit(n,key) -> LeaseCreditExpire(n,key)
            [] key \in pong[n].fresh -> PongExpire(n,key)
            [] RecentAck(NS(n),key) -> AckAge(n,key)
            [] OTHER -> WatchdogSilence(n,m)

\* Only drain the FIFO if the next observed receive requires a frame in it
\* or the next recorded send needs the writer to have released this channel.
NeedsWriter(k,dir) ==
    LET inflight == SeqToSet(conn[k].out[dir]) \cup conn[k].bg[dir]
                    \cup (IF conn[k].writer[dir] = Nil THEN {} ELSE {conn[k].writer[dir].f})
        wanted(f) == IF ev.name = "HandleNewCircuit" THEN
                         f.m.type = IF ev.dialer THEN "STARTACK" ELSE "START"
                     ELSE IF "msg" \in DOMAIN ev THEN
                       IF "type" \in DOMAIN ev.msg THEN WireType(f.m) = ev.msg.type ELSE FALSE
                     ELSE FALSE
        fids == {f.cid : f \in {f \in inflight : wanted(f)}}
        seen == IF "cid" \in DOMAIN ev THEN
                    IF ev.cid \in DOMAIN cidmap THEN {cidmap[ev.cid]} ELSE {}
                ELSE {}
        sends == IF "sent" \in DOMAIN ev THEN SeqToSet(ev.sent) ELSE {}
    IN \/ Rcv(k,dir) = ev.nid /\ seen \cap fids /= {}
       \/ Snd(k,dir) = ev.nid /\ \E x \in sends :
              /\ x.cid \in DOMAIN cidmap /\ RouteOf(conn,ev.nid,cidmap[x.cid]) = k
       \/ \E n \in Server : blk[n].wait /= Nil /\ blk[n].wait.k = k /\ blk[n].wait.dir = dir

SilentRegistrationClock ==
    /\ IsEvent("HandleNewCircuit")
    /\ Key(ev.peer) \in {Key(r) : r \in SeqToSet(ev.state.cktall)}
    /\ \E c \in CktOf(ev.nid) : EpSt(c,ev.nid) \in {EpNew,"newdead"}
          /\ NameInUse(ev.nid,c) /\ SilentDialClock(ev.nid,ev.peer.name)

SilentTransport ==
    /\ l <= Len(TraceLog)
    /\ \/ SilentRegistrationClock
       \/ SilentRegistrationRefusal
       \* The trace hooks record Raft handlers. The ordinary ticket result at
       \* the head of the same circuit is consumed by dispatchFrag first.
       \/ \E k \in DOMAIN conn, dir \in Dirs : \E i \in FrameIdx(k,dir) :
            /\ NextIsNode(Rcv(k,dir)) /\ FrameAt(k,dir,i).m.type = "TKTRES"
            /\ HandleTicketFrame(k,dir,i)
       \/ \E n \in Server : UnblockSend(n)
       \/ \E k \in DOMAIN conn, dir \in Dirs :
             \/ \E i \in FrameIdx(k,dir) :
                    /\ IsNodeEvent("HandleNewCircuit",Rcv(k,dir)) /\ ev.dialer
                    /\ CktIDMatches(FrameAt(k,dir,i).cid) /\ HandleSetupAckAs(k,dir,i,TRUE)
             \* The socket consumes a late circuit-setup acknowledgement
             \* before the pump reports its next unroutable data frame.
             \/ \E i \in FrameIdx(k,dir) :
                   /\ IsNodeEvent("DropUnroutable", Rcv(k,dir))
                   /\ "cid" \in DOMAIN ev /\ ev.cid \in DOMAIN cidmap
                   /\ FrameAt(k,dir,i).m.type = "STARTACK" /\ CktIDMatches(FrameAt(k,dir,i).cid)
                   /\ DropUnroutable(k,dir,i)
             \/ AckFlush(k, dir)
             \/ /\ NeedsWriter(k,dir)
                /\ \/ StartWrite(k, dir)
                   \/ WriteQueuedFrame(k, dir)
                   \/ \E f \in conn[k].bg[dir] : QueueCloseNotice(k, dir, f)
       \/ \E n \in Server : IsNodeEvent("DialAttempt", n) /\ SilentDialClock(n, ev.target)
       \/ \E n \in Server : IsNodeEvent("HandlePeerLeft", n) /\ \E c \in CktOf(n) : HaltOnDeadConn(n, c)

\* ========================================================================
\* INIT / NEXT
\* ========================================================================
TraceInit ==
    /\ CoreInitWithRestored(InitMembers) /\ AuxInit
    /\ l = 1 /\ cidmap = [c \in {} |-> 0]

TraceEvent ==
    \/ HandleRequestPreVoteIfLogged
    \/ HandlePreVoteIfLogged
    \/ HandleRequestVoteIfLogged
    \/ HandleVoteIfLogged
    \/ HandleAppendEntriesIfLogged
    \/ HandleAppendEntriesAckIfLogged
    \/ HandleStateSnapshotIfLogged
    \/ HandleRequestStateSnapshotIfLogged
    \/ ElectionTimeoutIfLogged
    \/ PreVotePhase2TimeoutIfLogged
    \/ LeaderHeartbeatIfLogged
    \/ BecomeLeaderSaveIfLogged
    \/ ClientRequestIfLogged
    \/ ClientChangeMembershipIfLogged
    \/ CompactIfLogged
    \/ AuditGCIfLogged
    \/ ParRequestSnapshotIfLogged
    \/ NodeEvent("ResumeCampaign", ResumeCampaign)
    \/ NodeEvent("RunDeferredCampaign", RunDeferredCampaign)
    \/ NodeEvent("ParFinish", ParFinish)
    \/ NodeEvent("ParIdentifyInterrupted", ParIdentifyInterrupted)
    \/ ParRecoverFromDonorIfLogged
    \/ HandleNewCircuitIfLogged
    \/ HandlePeerLeftIfLogged
    \/ DialAttemptIfLogged
    \/ CrashIfLogged
    \/ RestartIfLogged
    \/ LinkDownIfLogged
    \/ LinkUpIfLogged
    \/ StopIfLogged
    \/ ContIfLogged
    \/ HandleEndCircuitIfLogged
    \/ HandlePeerErrorIfLogged
    \/ DropUnroutableIfLogged
    \/ DropStaleTargetIfLogged
    \/ ConnAbortIfLogged

TraceCoreNext ==
    \/ TimeDone /\ TraceEvent
    \/ (SilentTime /\ UNCHANGED traceVars)
    \/ (SilentTransport /\ UNCHANGED traceVars)
    \/ (SilentStart /\ UNCHANGED traceVars)


TraceNext ==
    /\ TraceCoreNext /\ UNCHANGED auxVars
    /\ IF l' > l /\ IsEvent("DialAttempt") THEN
          IF "cid" \in DOMAIN ev /\ nextCkt' > nextCkt
          THEN cidmap' = cidmap @@ (ev.cid :> nextCkt) ELSE UNCHANGED cidmap
       ELSE UNCHANGED cidmap

\* WF on TraceNext: a run that can still consume events keeps doing so, so
\* TraceMatched fails only when the trace cannot be matched.
TraceSpec == TraceInit /\ [][TraceNext]_<<vars, traceVars>> /\ WF_<<vars, traceVars>>(TraceNext)

\* the whole trace was consumed
TraceMatched == <>(l > Len(TraceLog))

TraceView == <<vars, traceVars>>

TraceAlias ==
    [ l |-> l, event |-> IF l <= Len(TraceLog) THEN ev ELSE "done",
      role |-> role, currentTerm |-> currentTerm, votedFor |-> votedFor,
      leaderKey |-> leaderKey, commitIndex |-> commitIndex, log |-> log,
      cktall |-> cktall, byName |-> byName, blk |-> blk ]

=============================================================================
