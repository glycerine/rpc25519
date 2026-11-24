----------------------------- MODULE NetworkChecks -----------------------------
EXTENDS tube

\* The fixtures use a connected pair and a one-frame send queue. RSS is an
\* ordinary Tube request for a state snapshot. Its transport payload needs
\* only the sender identity. The EXCEPT updates in FramesWithIdentity
\* supply two different identities that the recipient must distinguish.
VARIABLE phase, trial
Pair == MeshCkt(LiveA,LiveB)
Sender == PairOf(Pair)[1]
Receiver == PairOf(Pair)[2]
Sibling == NN * NN + 1
VoterKey == <<Sender,0>>
Recipient == <<Receiver,0>>
Request == [type |-> "RSS", from |-> VoterKey]
DataFrameFor(c,msg,to) == [cid |-> c, m |-> msg, to |-> to]
RequestFrame == DataFrameFor(Pair,Request,Recipient)
EndNotice == [RequestFrame EXCEPT !.m.type = "END"]
EndPhase == CASE trial = "partial" -> 5 [] trial = "expiry" -> 4 [] trial = "waiting" -> 4
             [] trial \in {"full","routing"} -> 1 [] OTHER -> 2

Change(p,a) ==
    /\ phase = p /\ phase' = p + 1 /\ UNCHANGED trial
    /\ a /\ BaseOnly /\ NoFc

SendRequest ==
    LET net == IF trial = "full" THEN [conn EXCEPT ![Pair].out["da"] = <<EndNotice>>] ELSE conn
        ns == [NS(Sender) EXCEPT !.out = <<[c |-> Pair, m |-> Request]>>]
    IN /\ WriteBack(Sender,ns,net,ckt)
       /\ trial = "full" \/ blk'[Sender].pend /= <<>>

OtherCircuit == ckt @@ (Sibling :> ckt[Pair])
FramesWithIdentity ==
    LET first == IF trial = "stale" THEN [RequestFrame EXCEPT !.to = <<Receiver,1>>]
                 ELSE DataFrameFor(Sibling,[Request EXCEPT !.from = <<Sender,1>>],Recipient)
    IN WriteBack(Receiver,NS(Receiver),[conn EXCEPT ![Pair].q["da"] = <<first, RequestFrame>>],OtherCircuit)

\* Installing the new direct pair overwrites the destination's map entry.
\* Removing its predecessor has no routing effect. Removing the current
\* entry leaves only a registered reverse auto-client available on synthetic
\* addresses (srv.go deletePair, destAddrToSendCh and findAutoCliPair).
NewDirect == NN * NN + 1
Reverse == NewDirect + 1
PrepareRoutes ==
    LET reversed == [conn[Pair] EXCEPT !.a = Sender, !.d = Receiver]
        pairMap == conn @@ (NewDirect :> conn[Pair]) @@ (Reverse :> reversed)
    IN WriteBack(Sender,NS(Sender),pairMap,ckt)
RoutesMatch ==
    LET withoutOld == [conn EXCEPT ![Pair].dOpen = FALSE]
        withoutNew == [conn EXCEPT ![NewDirect].dOpen = FALSE]
        withoutReverse == [withoutNew EXCEPT ![Reverse].aOpen = FALSE]
    IN /\ RouteOf(conn,Sender,Pair) = NewDirect
       /\ RouteOf(withoutOld,Sender,Pair) = NewDirect
       /\ RouteOf(withoutNew,Sender,Pair) = (IF SyntheticAutoClientAddrs THEN Reverse ELSE 0)
       /\ SendDir(conn,Reverse,Sender) = "ad"
       /\ RouteOf(withoutReverse,Sender,Pair) = 0

FinishTimedWrite(k,d) == IF EnforceWriteDeadline THEN ExpireQueuedFrame(k,d) ELSE WriteQueuedFrame(k,d)
NetworkNext ==
    \/ trial = "routing" /\ Change(0,PrepareRoutes)
    \/ trial \in {"stale","foreign"} /\
        (\/ Change(0,FramesWithIdentity)
         \/ Change(1,IF trial = "stale" THEN DropUnroutable(Pair,"da",1) ELSE RejectForeignFrame(Pair,"da",1)))
    \/ trial \in {"full","waiting","expiry","partial"} /\ Change(0,SendRequest)
    \/ trial \in {"waiting","expiry","partial"} /\ Change(1,StartWrite(Pair,"da"))
    \/ trial = "waiting" /\ Change(2,WriteQueuedFrame(Pair,"da"))
    \/ trial = "expiry" /\ Change(2,FinishTimedWrite(Pair,"da"))
    \/ trial = "partial" /\ Change(2,BeginPartialWrite(Pair,"da"))
    \/ trial = "partial" /\ Change(3,FinishTimedWrite(Pair,"da"))
    \/ trial \in {"waiting","expiry","partial"} /\ Change(EndPhase - 1,UnblockSend(Sender))

NetworkSpec ==
    /\ MCInit /\ phase = 0 /\ trial \in {"stale","foreign","waiting","expiry","partial","full","routing"}
    /\ [][NetworkNext]_<<vars,phase,trial>>
    /\ WF_<<vars,phase,trial>>(NetworkNext)
NetworkProgress == <>(phase = EndPhase)

NetworkResult ==
   LET p == conn[Pair] IN
   CASE phase = 0 -> TRUE
     [] trial = "routing" -> RoutesMatch
     [] trial = "full" ->
             /\ ~CanEnqueue(conn,Sender,Pair) /\ ~WaitingForWrite(Sender)
             /\ blk[Sender] = NoBlk /\ p.out["da"] = <<EndNotice>> /\ p.q["da"] = <<>>
     [] trial \in {"stale","foreign"} ->
            phase = 1 \/
               /\ p.q["da"] = <<RequestFrame>> /\ p.aOpen /\ p.dOpen /\ EpSt(Pair,Receiver) = EpOpen
               /\ byName[Receiver][Sender] = Pair /\ cktall[Receiver][VoterKey] = Pair
               /\ EpSt(Sibling,Receiver) = IF trial = "foreign" THEN EpClosed ELSE EpOpen
     [] phase = 1 ->
             /\ WaitingForWrite(Sender) /\ ~Running(Sender) /\ p.out["da"] = <<RequestFrame>>
             /\ p.writer["da"] = Nil /\ p.q["da"] = <<>>
     [] phase = 2 \/ (trial = "partial" /\ phase = 3) ->
             /\ WaitingForWrite(Sender) /\ ~Running(Sender) /\ p.q["da"] = <<>>
             /\ p.writer["da"].f = RequestFrame /\ p.writer["da"].partial = (phase = 3)
             /\ ((ENABLED ExpireQueuedFrame(Pair,"da")) = EnforceWriteDeadline)
     [] OTHER ->
             /\ ~WaitingForWrite(Sender) /\ p.writer["da"] = Nil /\ p.aOpen
             /\ p.q["da"] = (IF trial = "waiting" \/ ~EnforceWriteDeadline THEN <<RequestFrame>> ELSE <<>>)
             /\ p.dOpen = (~EnforceWriteDeadline \/ trial /= "partial")
             /\ (phase = EndPhase => Running(Sender) /\ blk[Sender] = NoBlk)
=============================================================================
