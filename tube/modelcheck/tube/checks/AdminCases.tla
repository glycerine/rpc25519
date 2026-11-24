----------------------------- MODULE AdminCases -----------------------------
EXTENDS tube
VARIABLE trial, stage

A == LiveA
B == LiveB
X == CHOOSE n \in Server \ {A,B} : TRUE
D == [inc |-> 0, addr |-> InitAddr]

ScenarioInit == Init /\ stage = 0 /\ trial \in {"force", "shadow", "empty", "zap", "bootstrap", "observed", "known", "missing", "fenced"}
Turn(p,action) == stage = p /\ stage' = p+1 /\ UNCHANGED trial /\ action /\ BaseOnly /\ NoFc
History(length) == [i \in 1..length |-> [term |-> 1,id |-> i,kind |-> IF i = 1 THEN "noop" ELSE "data"]]
Prepare(n,leader,length,ci,configCI) ==
    LET lg == History(length)
        s == [NS(n) EXCEPT !.role = IF leader THEN Leader ELSE Follower,
                       !.term = 1, !.leader = <<A,0>>, !.sticky = trial /= "observed",
                       !.par = IF trial = "fenced" THEN "unknown" ELSE @,
                       !.cfg.ct = 1, !.cfg.ci = configCI, !.cfg.cit = 1,
                       !.log = lg, !.idLen = length, !.high = length,
                       !.ci = ci, !.cit = 1, !.la = ci, !.lat = 1,
                       !.noop0 = [id |-> 1,done |-> TRUE], !.nid = length+1,
                       !.appl = [i \in 1..ci |-> [id |-> i,term |-> 1]]]
    IN WriteBack(n, SaveRaftState(IF leader THEN ReinitPeers(s) ELSE s), conn, ckt)
Receive(n,fn(_,_,_)) ==
    \E k \in DOMAIN conn, dir \in Dirs : Rcv(k,dir) = n /\ \E i \in FrameIdx(k,dir) : fn(k,dir,i)

ForceNext ==
    \/ Turn(0,Prepare(A,TRUE,1,1,1))
    \/ Turn(1,Prepare(B,FALSE,1,1,1))
    \/ Turn(2,AdminForceContact(B,"remove",B))
    \/ Turn(3,Receive(A,HandleAdminForce))
    \/ Turn(4,BecomeLeaderSave(A))
ShadowNext ==
    \/ Turn(0,Prepare(A,TRUE,1,1,1))
    \/ Turn(1,Prepare(B,FALSE,1,1,1))
    \/ Turn(2,AdminShadow(A,"shadowAdd",X,D))
    \/ Turn(3,Receive(B,HandleAppendEntries))
    \/ Turn(4,Receive(A,HandleAppendEntriesAck))
    \/ Turn(5,LeaderHeartbeatWith(A,{}))
    \/ Turn(6,Receive(B,HandleAppendEntries))
    \/ Turn(7,Receive(A,HandleAppendEntriesAck))
    \/ Turn(8,AdminShadow(A,"shadowRemove",X,Nil))
    \/ Turn(9,Receive(B,HandleAppendEntries))
    \/ Turn(10,Receive(A,HandleAppendEntriesAck))
    \/ Turn(11,LeaderHeartbeatWith(A,{}))
    \/ Turn(12,Receive(B,HandleAppendEntries))
EmptyNext == Turn(0,Prepare(B,FALSE,2,2,2)) \/ Turn(1,AdminEmptyConfig(B))
ZapNext ==
    \/ Turn(0,Prepare(B,FALSE,4,2,4))
    \/ Turn(1,CrashDiskFault(B,"suffix",2))
    \/ Turn(2,RestartWithMode(B,InitAddr,TRUE))
BootstrapNext ==
    \/ Turn(0,Crash(B))
    \/ Turn(1,RestartWithMode(B,InitAddr,TRUE))
    \/ Turn(2,BootstrapSelf(B))
    \/ Turn(3,ResumeCampaign(B))
    \/ Turn(4,ResumeCampaign(B))
KnownNext ==
    \/ Turn(0,AdminForceContact(A,"remove",B))
    \/ Turn(1,BecomeLeaderSave(A))
    \/ Turn(2,AdminForceContact(A,"add",B))
    \/ Turn(3,BecomeLeaderSave(A))
FencedNext ==
    \/ Turn(0,Prepare(B,FALSE,1,1,1))
    \/ Turn(1,AdminForceContact(B,"remove",B))

Done == CASE trial \in {"force","observed"} -> stage = 5
          [] trial = "known" -> stage = 4 [] trial = "missing" -> stage = 1 [] trial = "fenced" -> stage = 2 [] trial = "shadow" -> stage = 13
          [] trial = "empty" -> stage = 2 [] trial = "zap" -> stage = 3 [] OTHER -> stage = 5
TransportStep ==
    /\ UNCHANGED <<stage,trial>>
    /\ \/ \E k \in DOMAIN conn, dir \in Dirs : MCWriterStep(k,dir)
       \/ \E n \in Server : MCUnblockSend(n)

ScenarioNext ==
    \/ TransportStep
    \/ Done /\ UNCHANGED <<vars,trial,stage>>
    \/ CASE trial \in {"force","observed"} -> ForceNext
         [] trial = "known" -> KnownNext [] trial = "fenced" -> FencedNext
         [] trial = "missing" -> Turn(0,AdminForceContact(A,"add",X)) [] trial = "shadow" -> ShadowNext
         [] trial = "empty" -> EmptyNext [] trial = "zap" -> ZapNext [] OTHER -> BootstrapNext
ScenarioSpec == ScenarioInit /\ [][ScenarioNext]_<<vars,trial,stage>>
ExpectAdmin ==
    /\ AdminRoutingOK
    /\ (trial \in {"force","observed"} /\ stage >= 3 => AdminHasRedirect /\ config[B].mem={A,B})
    /\ (trial \in {"force","observed"} /\ stage >= 4 => AdminHasOverride /\ config[A].mem={A})
    /\ (trial \in {"force","observed"} /\ stage = 4 => dCfg[A].mem={A,B} /\ blk[A].save)
    /\ (trial \in {"force","observed"} /\ stage = 5 => dCfg[A].mem={A} /\ ~blk[A].save)
    /\ (trial = "shadow" /\ stage >= 4 => log[B][2].kind = "shadowAdd" /\ log[B][2].detail=D)
    /\ (trial = "shadow" /\ stage \in 5..10 => X \in shadows[A])
    /\ (trial = "shadow" /\ stage \in 7..12 => X \in shadows[B] /\ knownPeers[B][X]=D)
    /\ (trial = "shadow" /\ Done => X \notin shadows[A] \cup shadows[B] /\ knownPeers[B][X]=D)
    /\ (trial = "known" /\ stage \in {1,2} => config[A].mem={A} /\ knownPeers[A][B]=D)
    /\ (trial = "known" /\ Done => dCfg[A].mem={A,B} /\ dCfg[A].det[B]=D /\ dCfg[A].cv=InitCfg.cv+2)
    /\ (trial = "missing" /\ Done => config[A]=InitCfg /\ \E e \in adminEvents : e.result="refused" /\ e.x=X)
    /\ (trial = "fenced" /\ Done => config[B].mem={A,B} /\ \E e \in adminEvents : e.result="noRoute")
    /\ (trial = "empty" /\ Done => dCfg[B].ct=1 /\ dCfg[B].cv=InitCfg.cv+1 /\ dCfg[B].mem={}
                        /\ dCfg[B].ci=2 /\ dCfg[B].cit=1 /\ appendWitness[B].saved=2)
    /\ (trial = "zap" /\ Done => par[B]="unknown" /\ dCfg[B].mem={} /\ dCfg[B].ct=0 /\ dCfg[B].cv=0
                        /\ dCfg[B].ci=2 /\ dCfg[B].cit=1 /\ dCommit[B]=2 /\ dCommitTerm[B]=1
                        /\ shadows[B]={} /\ dShadows[B]={} /\ knownPeers[B][A]=D)
    /\ (trial = "bootstrap" /\ stage=3 => dVote[B] = <<B,1>> /\ dTerm[B]=1 /\ dCfg[B].mem={B})
    /\ (trial = "bootstrap" /\ Done => role[B]=Leader /\ noop0[B].done)
=============================================================================
