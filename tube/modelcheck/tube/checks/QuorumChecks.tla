--------------------------- MODULE QuorumChecks ---------------------------
EXTENDS tube
VARIABLE stage
\* Add a voter to a committed singleton and contrast its matching vs stale
\* config report. Two incarnations of that voter share a single quorum slot.
AdmissionAndCounting ==
   LET n == LiveA
       m == LiveB
       k == <<m,inc[m]>>
       alt == <<m,inc[m]+1>>
       a == [NS(n) EXCEPT !.cfg.mem = {n}, !.term = 1, !.role = Leader,
                !.cfg.ct = 1, !.noop0.done = TRUE, !.mc.issued = 1]
       t == [op |-> "add", x |-> m, id |-> <<n,inc[n],1>>,force |-> FALSE, addr |-> InitAddr]
       pending == ChangeMembership(a,n,t)
       pe == pending.mc.awaiting
       v == pending.cfg
       heard == SeenOn(pending,n,a.bn[m],v,1)
       clash == SeenOn(pending,n,a.bn[m],[v EXCEPT !.mem = {m}],1)
       late == OnLeaderIsCurrentMCcommitted([pending EXCEPT !.mc.expired = {t.id}],n)
       lost == BecomeFollower(pending,n,2,Nil,TRUE)
       b == [a EXCEPT !.cfg.mem = {m}, !.peers[k] = [FreshPeer EXCEPT !.mi = 2],
                                       !.peers[alt] = [FreshPeer EXCEPT !.mi = 4]]
       detached == [b EXCEPT !.bn[m] = NoCkt]
       newlyHeard == MarkPeerHeard(detached,alt)
       pong1 == [all |-> {k},fresh |-> {k}]
   IN /\ Cardinality(pe) = 1 /\ pending.newAcked = {} /\ ~v.com
      /\ heard.newAcked = pe /\ heard.mc.awaiting = {} /\ heard.cfg.com
      /\ clash.newAcked = {} /\ clash.mc.awaiting = pe
      /\ late.mc.awaiting = {} /\ late.mc.failed = {MCError(t,"unconfirmedTimeout")}
      /\ lost.mc.awaiting = {} /\ lost.mc.failed = {MCError(t,"unconfirmed")}
      /\ CountGE(b,n,3) = 0
      /\ CountGE(newlyHeard,n,3) = 1
      /\ RecordPong(pong1,alt) = [all |-> {alt},fresh |-> {alt}]

Ticket(i) == [op |-> "add", x |-> LiveB, id |-> <<LiveA,inc[LiveA],i>>, force |-> FALSE, addr |-> InitAddr]
\* The first and third tickets use the local default timeout. The second
\* was forwarded with its own deadline and can expire independently.
Prepare ==
    LET s == NS(LiveA)
        setup == [s EXCEPT !.mc.issued = 3, !.mc.direct = {Ticket(1).id,Ticket(3).id},
                           !.stallQ = <<Ticket(1),Ticket(2),Ticket(3)>>]
    IN WriteBack(LiveA,setup,conn,ckt) /\ NoFc /\ BaseOnly
DeadlineStep ==
    \/ stage = 0 /\ Prepare /\ stage' = 1
    \/ stage = 1 /\ MCDeadlineTick(LiveA) /\ stage' = 2
CheckVars == <<mcVars,stage>>
QuorumSpec == (Init /\ stage = 0) /\ [][DeadlineStep]_CheckVars /\ WF_CheckVars(DeadlineStep)
ExpiryOK ==
    LET ids == mcPending[LiveA].expired IN
    /\ (stage = 0 => AdmissionAndCounting)
    /\ (stage = 2 => ids \in {{Ticket(1).id},{Ticket(2).id},{Ticket(1).id,Ticket(3).id}})
    /\ (stage = 1 => ids = {})
DeadlineObserved == <>(stage = 2)
===========================================================================
