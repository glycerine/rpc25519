----------------------------- MODULE IdentityChecks -----------------------------
EXTENDS tube

\* The incumbent has a nonzero Raft match index so its progress can be
\* tracked across a same-incarnation replacement and a delayed peer-left
\* notification. A replacement with a different PeerID begins fresh.
VARIABLE phase, trial

Old == MeshCkt(LiveA,LiveB)
Source == PairOf(Old)[1]
Sink == PairOf(Old)[2]
New == NN * NN + 1
OldKey == <<Source, 0>>
OwnerKey == <<Source, IF trial = "same" THEN 0 ELSE 1>>

PrepareCandidate ==
    LET peer == [ckt[Old] EXCEPT !.di = OwnerKey[2], !.aSt = EpNew]
        state == [NS(Sink) EXCEPT !.peers[OldKey] = [FreshPeer EXCEPT !.mi = 7],
                      !.silent = IF trial = "silent" THEN {Old} ELSE {}]
    IN WriteBack(Sink, state, conn, ckt @@ (New :> peer))

Move(p,act) ==
    /\ phase = p /\ phase' = p + 1 /\ UNCHANGED trial
    /\ act /\ BaseOnly /\ NoFc

IdentityNext ==
    \/ Move(0,PrepareCandidate)
    \/ Move(1,HandleNewCircuit(Sink,New))
    \/ trial = "same" /\ Move(2,WriteBack(Sink,NS(Sink),conn,SetEp(ckt,Old,Sink,EpClosed)))
    \/ trial = "same" /\ Move(3,HandlePeerLeft(Sink,Old))

IdentitySpec ==
    /\ MCInit /\ phase = 0 /\ trial \in {"same","silent","recent"}
    /\ [][IdentityNext]_<<vars,phase,trial>>
    /\ WF_<<vars,phase,trial>>(IdentityNext)

IdentityProgress == <>(phase = IF trial = "same" THEN 4 ELSE 2)

IdentityResult ==
    /\ (phase = 1 => (NameInUse(Sink,New) = (trial = "recent")))
    /\ (phase >= 2 =>
          CASE trial = "recent" ->
                 /\ EpSt(New,Sink) = EpGone /\ EpSt(Old,Sink) = EpOpen
                 /\ cktall[Sink][OldKey] = Old /\ peers[Sink][OldKey].mi = 7
                 /\ byName[Sink][Source] = Old /\ cktall[Sink][OwnerKey] = NoCkt
            [] trial = "silent" ->
                 /\ byName[Sink][Source] = New /\ cktall[Sink][OwnerKey] = New
                 /\ EpSt(New,Sink) = EpOpen /\ cktall[Sink][OldKey] = NoCkt
                 /\ peers[Sink][OldKey] = Nil /\ peers[Sink][OwnerKey] = FreshPeer
            [] OTHER ->
                 /\ EpSt(New,Sink) = EpOpen /\ byName[Sink][Source] = New
                 /\ cktall[Sink][OldKey] = New /\ peers[Sink][OldKey].mi = 7
                 /\ (phase = 2 => EpSt(Old,Sink) = EpOpen)
                 /\ (phase = 4 => EpSt(Old,Sink) = EpGone))
=============================================================================
