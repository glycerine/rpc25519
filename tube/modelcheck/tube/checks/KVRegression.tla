---------------------------- MODULE KVRegression ----------------------------
EXTENDS tube

VARIABLE scenario, step
testVars == <<vars, scenario, step>>
Fixtures == {"retry", "clock", "lease", "sizes", "legacy", "snapshot", "drift", "legacyDrift"}
A == CHOOSE c \in KVClients : TRUE
B == CHOOSE c \in KVClients \ {A} : TRUE
L == KVR0
F == CHOOSE f \in KVReplicas \ {L} : TRUE
Leaf(n) == kvState[n].tables["data"].leaf
New(c,sid) ==
    /\ KVCreateSession(c)
    /\ kvMem'[kvLeader][Len(kvMem'[kvLeader])].sid = sid
    /\ kvMem'[kvLeader][Len(kvMem'[kvLeader])].hlc = KVClock(kvLeader)
Put(c,sid,op,oldE) ==
    /\ KVSubmit(c,op,oldE)
    /\ Len(kvMem'[kvLeader]) = Len(kvMem[kvLeader]) + 1
    /\ LET e == kvMem'[kvLeader][Len(kvMem'[kvLeader])] IN
         /\ e.sid = sid /\ e.hlc = KVClock(kvLeader) /\ e.ack = 0
         /\ e.autoDel = (op \in {"lwrite"} \cup KVCAS)
TryLocal(c,sid,op) == KVLocal(c,op) /\ kvIssued'[c].sid = sid

\* One command reaches the log twice before apply. A later command is
\* admitted behind SESS_END and must receive an error at its own log index.
Retry ==
    CASE step = 0 -> New(A,1)
      [] step = 1 -> KVApply(L)
      [] step = 2 -> Put(A,1,"lwrite",0)
      [] step = 3 -> KVRetrySameSerial(A) /\ Len(kvMem'[L]) = 3
      [] step \in {4,5} -> KVApply(L)
      [] step = 6 -> KVEndSession(A)
      [] step = 7 -> Put(A,1,"write",0)
      [] step \in {8,9} -> KVApply(L)
      [] step = 10 -> KVInstallSnapshot(F)
Length == CASE scenario = "retry" -> 11 [] scenario = "clock" -> 13
            [] scenario = "lease" -> 21 [] scenario = "sizes" -> 10
            [] scenario = "legacy" -> 4 [] scenario = "snapshot" -> 12
            [] scenario = "drift" -> 14 [] scenario = "legacyDrift" -> 11

\* A local read leaves the original end time alone. A logged read renews
\* the session on all replicas. A later log clock drives expiry at equality.
Clock ==
    CASE step = 0 -> New(A,1)
      [] step = 1 -> KVApply(L)
      [] step = 2 -> TryLocal(A,1,"read")
      [] step \in {3,4} -> KVTick
      [] step = 5 -> Put(A,1,"read",0)
      [] step = 6 -> KVApply(L)
      [] step = 7 -> KVInstallSnapshot(F)
      [] step \in {8,9,10} -> KVTick
      [] step = 11 -> New(B,1)
      [] step = 12 -> KVApply(L)

\* A raw table scan protects a hidden-but-still-leased key during the
\* drift margin. The same contender can take over one tick later.
Lease ==
    CASE step = 0 -> Put(A,0,"lwrite",0)
      [] step = 1 -> KVApply(L)
      [] step = 2 -> TryLocal(B,0,"lwrite")
      [] step \in {3,4,5} -> KVTick
      [] step = 6 -> Put(B,0,"read",0)
      [] step = 7 -> KVApply(L)
      [] step = 8 -> Put(B,0,"renameTable",0)
      [] step = 9 -> KVApply(L)
      [] step = 10 -> Put(B,0,"deleteTable",0)
      [] step = 11 -> KVApply(L)
      [] step = 12 -> KVTick
      [] step = 13 -> Put(B,0,"lwrite",0)
      [] step = 14 -> KVApply(L)
      [] step = 15 -> KVInstallSnapshot(F)
      [] step = 16 -> KVFailover(F)
      [] step = 17 -> KVApply(F)
      [] step = 18 -> Put(A,0,"casVersion",2)
      [] step = 19 -> KVApply(F)
      [] step = 20 -> KVInstallSnapshot(L)

Sizes ==
    CASE step = 0 -> KVRejectInput(A,"contextCancelled")
      [] step = 1 -> KVRejectInput(A,"sessionCancelled")
      [] step = 2 -> KVRejectInput(A,"keyTooLarge")
      [] step = 3 -> Put(A,0,"lwrite",0)
      [] step = 4 -> KVApply(L)
      [] step = 5 -> Put(A,0,"bigScan",0)
      [] step = 6 -> KVApply(L)
      [] step = 7 -> KVRejectInput(A,"keyTooLarge")
      [] step = 8 -> Put(A,0,"largeEnvelopeScan",0)
      [] step = 9 -> KVApply(L)

Legacy ==
    CASE step = 0 -> KVLoadLegacy(A,"legacyInvalidKey")
      [] step = 1 -> KVApply(L)
      [] step = 2 -> KVShip(F)
      [] step = 3 -> KVApply(F)

Snapshot ==
    CASE step = 0 -> New(A,1)
      [] step = 1 -> KVApply(L)
      [] step = 2 -> KVInstallSnapshot(F)
      [] step = 3 -> New(A,2)
      [] step = 4 -> KVApply(L)
      [] step = 5 -> KVInstallSnapshot(F)
      [] step \in {6,7,8} -> KVTick
      [] step = 9 -> New(B,1)
      [] step = 10 -> KVApply(L)
      [] step = 11 -> KVInstallSnapshot(F)

Drift ==
    CASE step = 0 -> Put(A,0,"lwrite",0)
      [] step = 1 -> KVApply(L)
      [] step = 2 -> KVShip(F)
      [] step = 3 -> KVApply(F)
      [] step = 4 -> KVFailover(F)
      [] step = 5 -> KVApply(F)
      [] step \in {6,7} -> KVTick
      [] step = 8 -> Put(B,0,"lwrite",0)
      [] step = 9 -> KVApply(F)
      [] step \in {10,11} -> KVShip(L)
      [] step \in {12,13} -> KVApply(L)

LegacyDrift ==
    CASE step = 0 -> KVLoadLegacy(A,"lwrite")
      [] step \in {1,2,3} -> KVTick
      [] step = 4 -> KVLoadLegacy(B,"lwrite")
      [] step \in {5,6} -> KVShip(F)
      [] step \in {7,8} -> KVApply(L)
      [] step \in {9,10} -> KVApply(F)

TestNext ==
    /\ step < Length
    /\ CASE scenario = "retry" -> Retry [] scenario = "clock" -> Clock
         [] scenario = "lease" -> Lease [] scenario = "sizes" -> Sizes
         [] scenario = "legacy" -> Legacy [] scenario = "snapshot" -> Snapshot
         [] scenario = "drift" -> Drift [] scenario = "legacyDrift" -> LegacyDrift
    /\ step' = step + 1 /\ UNCHANGED scenario /\ KVOnly /\ NoFc
KVRegressionSpec ==
    Init /\ scenario \in Fixtures /\ step = 0 /\ [][TestNext]_testVars /\ WF_testVars(TestNext)
FinishAll == <>(step = Length)

KVCheckpoints ==
  CASE scenario = "retry" ->
       /\ (step \in {5,6}) => (kvLast[A].code = "ok" /\ kvLast[A].value.widx = 2 /\ Leaf(L).ver = 1)
       /\ (step \in {10,11}) => (kvLast[A].code = "unknownSession" /\ kvSess[L][A] = KVNil /\ Leaf(L).ver = 1)
    [] scenario = "clock" ->
       /\ (step = 3) => kvSess[L][A].endTm = 3
       /\ (step = 8) => (kvSess[L][A].endTm = 5 /\ kvSess[F][A].endTm = 5)
       /\ (step = 13) => (kvSess[L][A] = KVNil /\ kvSess[L][B] /= KVNil)
    [] scenario = "lease" ->
       /\ (step = 3) => (kvLast[B].code = "rejected" /\ kvLast[B].value.until = 3)
       /\ (step = 8) => (kvLast[B].code = "missing" /\ Leaf(L).leasor = A)
       /\ (step \in {10,12}) => (kvLast[B].code = "leased" /\ Leaf(L).leasor = A)
       /\ (step = 15) => (kvLast[B].code = "ok" /\ Leaf(L).leasor = B /\ Leaf(L).epoch = 2)
       /\ (step = 21) => (kvLast[A].code = "rejected" /\ kvLast[A].value.until = 6)
    [] scenario = "sizes" ->
       /\ (step = 1) => kvLast[A].code = "contextCancelled"
       /\ (step = 2) => kvLast[A].code = "sessionCancelled"
       /\ (step = 3) => (Len(kvMem[L]) = 0 /\ kvLast[A].code = "keyTooLarge")
       /\ (step = 7) => (kvLast[A].code = "resultTooLarge" /\ kvLast[A].value = KVNil)
       /\ (step = 10) => (kvLast[A].code = "keyTooLarge" /\ Leaf(L).ver = 1)
    [] scenario = "legacy" ->
       (step = 4) => (kvLast[A].code = "keyTooLarge" /\ kvApplied[F] = 1 /\ Leaf(F) = KVNil)
    [] scenario = "drift" ->
       (step = 14) => (Leaf(L) = Leaf(F) /\ Leaf(F).leasor = B /\ Leaf(F).epoch = 2)
    [] scenario = "legacyDrift" ->
       (step = 11) => (Leaf(L) = Leaf(F) /\ Leaf(F).leasor = A /\ kvLast[B].code = "rejected")
    [] scenario = "snapshot" ->
       /\ (step = 6) => (kvSess[F][A].id = 2 /\ kvState[F].expiry[A] = 3)
       /\ (step = 12) => (kvSess[F][A] = KVNil /\ kvSess[F][B].endTm = 6 /\ kvState[F].expiry[A] = 0)
=============================================================================
