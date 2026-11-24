--------------------------- MODULE DonorChecks ---------------------------
EXTENDS UpdateCases

SetDonor(term) ==
    LET entries == ExampleLog(4)
        s == [NS(SenderNode) EXCEPT !.role = Leader, !.term = term, !.leader = <<SenderNode, inc[SenderNode]>>,
               !.cfg.ct = term, !.log = entries, !.idLen = 4, !.high = 4]
    IN WriteBack(SenderNode, SaveRaftState(s), conn, ckt)

PrepareRequester ==
    LET s == [NS(DiskNode) EXCEPT !.par = "unknown", !.term = 2, !.cfg.ci = 4, !.cfg.ct = 2,
               !.idLen = 2, !.log = ExampleLog(2), !.ci = 2, !.cit = 1, !.la = 2, !.lat = 1, !.appl = ExampleApplied(2)]
    IN WriteBack(DiskNode, SaveRaftState(s), conn, ckt)
AddCheckedBytes ==
    /\ torn' = [torn EXCEPT ![DiskNode] = [j \in 1..2 |-> [idx |-> j+2, e |-> ExampleLog(4)[j+2]]]]
    /\ UNCHANGED <<envVars, netVars, viewVars, raftVars, log, base, idLen, par, durVars, ghostVars>>
DonorInitial == Init /\ phase = 0 /\ trial \in {"newer", "older"}
DonorNext == \/ Advance(0, SetDonor(IF trial = "older" THEN 1 ELSE 3))
             \/ Advance(1, PrepareRequester)
             \/ Advance(2, AddCheckedBytes)
             \/ trial = "older" /\ (\/ Advance(3, ParObserveQuery(DiskNode, SenderNode))
                                    \/ Advance(4, ParRecoverFromDonor(DiskNode, SenderNode))
                                    \/ Advance(5, ParRecoverFromDonor(DiskNode, SenderNode))
                                    \/ Advance(6, ParFinish(DiskNode)))
DonorSpec == DonorInitial /\ [][DonorNext]_fixtureVars /\ WF_fixtureVars(DonorNext)
DonorResults == /\ (phase = 3 => ~ENABLED ParIdentifyInterrupted(DiskNode))
                /\ (trial = "newer" /\ phase = 3 => ~ENABLED ParRecoverFromDonor(DiskNode,SenderNode))
                /\ (phase \in 4..7 => role[SenderNode] = Follower /\ currentTerm[SenderNode] = 2)
                /\ (phase = 5 => par[DiskNode] = "unknown" /\ idLen[DiskNode] = 3)
                /\ (phase = 6 => par[DiskNode] = "ready" /\ idLen[DiskNode] = 4)
                /\ (phase = 7 => par[DiskNode] = "ok" /\ log[DiskNode] = ExampleLog(4))
DonorTrialsFinish == <>(phase = IF trial = "older" THEN 7 ELSE 3)
=========================================================================
