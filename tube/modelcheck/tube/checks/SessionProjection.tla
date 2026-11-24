-------------------------- MODULE SessionProjection --------------------------
EXTENDS KVRegression
CheckVars == <<testVars>>
ProjectNext ==
    /\ step < 13
    /\ CASE step = 0 -> New(A,1)
         [] step = 1 -> KVApply(L)
         [] step = 2 -> KVShip(F)
         [] step = 3 -> KVApply(F)
         [] step = 4 -> TryLocal(A,1,"read")
         [] step = 5 -> KVFailover(F)
         [] step = 6 -> KVApply(F)
         [] step = 7 -> KVRetrySameSerial(A) /\ Len(kvMem'[F]) = 3
         [] step = 8 -> KVApply(F)
         [] step \in {9,10} -> KVShip(L)
         [] step \in {11,12} -> KVApply(L)
    /\ KVOnly /\ NoFc /\ UNCHANGED scenario /\ step'=step+1
ProjectSpec == Init /\ scenario="cacheRetention" /\ step=0 /\ [][ProjectNext]_CheckVars /\ WF_CheckVars(ProjectNext)
Completed == <>(step=13)
BeforeRenewal == step < 13
RenewalRecorded ==
    step = 13 => \A n \in {L,F} :
        LET e == kvMem[n][kvApplied[n]]
            s == kvState[n].sessions[A]
        IN /\ s /= KVNil /\ s.maxApplied = e.serial
           /\ s.repEnd = e.tm + KVSessDur /\ s.endTm = s.repEnd
           /\ e.serial \in DOMAIN s.cache /\ s.cache[e.serial].fromLog
=============================================================================
