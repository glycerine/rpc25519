---------------------------- MODULE RedirectChecks ----------------------------
EXTENDS tube
VARIABLE phase, trial

\* Two connected voters. The origin redirects to its last known leader.
Pair == MeshCkt(LiveA,LiveB)
Origin == PairOf(Pair)[1]
Target == PairOf(Pair)[2]
OriginID == <<Origin,0>>
TargetID == <<Target,0>>
EndNotice == [cid |-> Pair, m |-> [type |-> "END", from |-> OriginID], to |-> TargetID]
Trials == {"positiveFull","zeroFull","ordinaryRefused","adminRefused"}
LastPhase == CASE trial = "positiveFull" -> 2 [] trial = "zeroFull" -> 8 [] OTHER -> 5

Advance(p,action) ==
    /\ phase = p /\ phase' = p + 1 /\ UNCHANGED trial
    /\ action /\ BaseOnly /\ NoFc

PrepareRedirect ==
    LET net == IF trial \in {"zeroFull","positiveFull"}
               THEN [conn EXCEPT ![Pair].out["da"] = <<EndNotice>>] ELSE conn
    IN WriteBack(Origin,[NS(Origin) EXCEPT !.leader = TargetID],net,ckt)

Submit ==
    IF trial = "adminRefused"
    THEN /\ SubmitAdminTicket(Origin)
         /\ clientTickets'[1].wait /\ clientTickets'[1].command.kind = "remove"
         /\ clientTickets'[1].command.x = Target
         /\ blk'[Origin].pend /= <<>>
    ELSE /\ ClientSubmit(Origin,trial /= "zeroFull")
         /\ (trial = "ordinaryRefused" => blk'[Origin].pend /= <<>>)

RedirectNext ==
    \/ Advance(0,PrepareRedirect)
    \/ Advance(1,Submit)
    \/ trial /= "positiveFull" /\ Advance(2,StartWrite(Pair,"da"))
    \/ trial \in {"ordinaryRefused","adminRefused"} /\
          (\/ Advance(3,RefuseQueuedFrame(Pair,"da"))
           \/ Advance(4,UnblockSendAs(Origin,FALSE)))
    \/ trial = "zeroFull" /\
          (\/ Advance(3,AdmitWaiting(Origin))
           \/ Advance(4,WriteQueuedFrame(Pair,"da"))
           \/ Advance(5,StartWrite(Pair,"da"))
           \/ Advance(6,WriteQueuedFrame(Pair,"da"))
           \/ Advance(7,UnblockSendAs(Origin,TRUE)))
RedirectSpec ==
    /\ MCInit /\ phase = 0 /\ trial \in Trials
    /\ [][RedirectNext]_<<vars,phase,trial>>
    /\ WF_<<vars,phase,trial>>(RedirectNext)
RedirectProgress == <>(phase = LastPhase)
RedirectResults ==
    IF phase < 2 THEN TRUE
    ELSE LET t == clientTickets[1]
             p == conn[Pair]
             w == blk[Origin]
         IN /\ CASE trial = "positiveFull" ->
                       /\ t.result.status = "redirectError" /\ ~t.stashed
                       /\ w = NoBlk /\ p.out["da"] = <<EndNotice>> /\ EpSt(Pair,Origin) = EpOpen
                  [] trial = "zeroFull" ->
                       /\ t.result = Nil /\ t.stage[OriginID] = "follow" /\ ~t.stashed
                       /\ IF phase = 8 THEN w = NoBlk
                          ELSE w.wait.status = CASE phase \in {2,3} -> "slot"
                                                [] phase = 7 -> "sent" [] OTHER -> "write"
                       /\ (phase = 4 => p.writer["da"].f = EndNotice /\ Head(p.out["da"]).m.type = "TKT")
                       /\ (phase = 6 => ~ENABLED ExpireQueuedFrame(Pair,"da"))
                       /\ (phase = 8 => Len(p.q["da"]) = 2 /\ p.q["da"][1] = EndNotice
                                       /\ p.q["da"][2].m.type = "TKT" /\ ~p.q["da"][2].m.timed)
                  [] phase < 5 -> t.result = Nil /\ ~t.stashed /\
                                  (phase = 4 => w.wait.status = "refused" /\ WriteReturns(Origin) = {FALSE})
                  [] OTHER ->
                       /\ w = NoBlk /\ EpSt(Pair,Origin) = EpClosed /\ p.dOpen /\ p.aOpen
                       /\ cktall[Origin][TargetID] = NoCkt
                       /\ IF trial = "adminRefused" THEN t.result = Nil /\ t.stashed
                          ELSE t.result.status = "redirectError" /\ ~t.stashed
=============================================================================
