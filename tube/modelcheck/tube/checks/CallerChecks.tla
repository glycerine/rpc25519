--------------------------- MODULE CallerChecks ---------------------------
EXTENDS tube
VARIABLE trial, phase
Trials == {"normal", "cancelled", "timeout"}
Ready == Running(LiveA) /\ role[LiveA] = Leader /\ noop0[LiveA].done
Elect ==
    \/ MCElectionTimeout(LiveA)
    \/ MCPreVotePhase2Timeout(LiveA)
    \/ MCBecomeLeaderSave(LiveA)
    \/ MCResumeCampaign(LiveA)

Submit ==
    IF trial /= "timeout" THEN ClientSubmit(LiveA,FALSE)
    ELSE /\ SubmitAdminTicket(LiveA) /\ clientTickets'[1].wait
         /\ clientTickets'[1].command.kind = "shadowAdd" /\ clientTickets'[1].command.x = LiveB
         /\ clientTickets'[1].command.detail = [inc |-> -1, addr |-> InitAddr]

Step ==
    /\ UNCHANGED trial
    /\ \/ /\ phase = 0 /\ ~Ready /\ Elect /\ UNCHANGED phase
       \/ /\ phase = 0 /\ Ready /\ Submit /\ BaseOnly /\ NoFc /\ phase' = 1
       \/ /\ phase = 1 /\ trial /= "normal" /\ CallerAbandonsTicket(LiveA,1)
          /\ clientTickets'[1].returned.status = trial /\ BaseOnly /\ NoFc /\ phase' = 2
       \/ /\ phase = (IF trial = "normal" THEN 1 ELSE 2)
          /\ FlushDataTicket(LiveA,1) /\ BaseOnly /\ NoFc /\ phase' = 3

CallerSpec ==
    /\ MCInit /\ trial \in Trials /\ phase = 0
    /\ [][Step]_<<vars,trial,phase>>
    /\ WF_<<vars,trial,phase>>(Step)

CallerResults ==
    IF phase = 0 THEN TRUE
    ELSE LET q == clientTickets[1] IN
         /\ IF phase = 1 THEN q.returned = Nil ELSE
            IF trial = "normal" THEN q.returned = q.result
            ELSE /\ q.returned = [status |-> trial, entry |-> 0, via |-> "caller"]
                 /\ q.contextDone = (trial = "cancelled")
         /\ IF phase < 3 THEN q.result = Nil ELSE
            /\ q.result.status = "applied" /\ q.result.via = "leader"
            /\ \E e \in committed : e.id = q.result.entry
            /\ \E i \in DOMAIN applied[LiveA] : applied[LiveA][i].id = q.result.entry
            /\ (trial = "timeout" => LiveB \in shadows[LiveA])

CallerProgress == <>(phase = 3)
===========================================================================
