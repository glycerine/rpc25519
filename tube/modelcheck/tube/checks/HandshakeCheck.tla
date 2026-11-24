-------------------------- MODULE HandshakeCheck --------------------------
EXTENDS tube
VARIABLE step, trial

CheckVars == <<mcVars,step,trial>>
Pair == CHOOSE k \in MeshIds : TRUE
Dialer == PairOf(Pair)[1]
Acceptor == PairOf(Pair)[2]
FreshCircuit == CktBase

\* Reconnect on an existing pair as the watchdog requests. The two paths
\* differ only in reverse-link availability or the STARTACK queue outcome.
RequestDial ==
    /\ wdReq' = [wdReq EXCEPT ![Dialer][Acceptor] = addr[Acceptor]]
    /\ UNCHANGED <<envVars,netVars,cktall,byName,pendAddr,knownPeers,watchSilent,
                   observer,cktReplica,audit,raftVars,logVars,durVars,ghostVars>>
Dial ==
    /\ DialAttempt(Dialer,Acceptor) /\ nextCkt' = FreshCircuit+1
    /\ ckt'[FreshCircuit].dSt = EpDialWait
FillReverseQueue ==
    LET endFrame == [cid |-> Pair, to |-> <<Dialer,inc[Dialer]>>,
                     m |-> [type |-> "END", from |-> <<Acceptor,inc[Acceptor]>>]]
    IN /\ conn' = QueueNotice(conn,Pair,"ad",endFrame) /\ PipeOnly

ReopenReverseLink ==
    CASE step=0 -> RequestDial
      [] step=1 -> LinkDown(Acceptor,Dialer)
      [] step=2 -> Dial
      [] step=3 -> StartWrite(Pair,"da")
      [] step=4 -> WriteQueuedFrame(Pair,"da")
      [] step=5 -> HandleStartFrame(Pair,"da",1)
      [] step=6 -> HandleNewCircuit(Acceptor,FreshCircuit)
      [] step=7 -> StartWrite(Pair,"ad")
      [] step=8 -> WriteQueuedFrame(Pair,"ad")
      [] step=9 -> LinkUp(Acceptor,Dialer)
      [] step=10 -> HandleSetupAckAs(Pair,"ad",1,TRUE)
      [] step=11 -> HandleNewCircuit(Dialer,FreshCircuit)
      [] OTHER -> FALSE
RefuseSetupReply ==
    CASE step=0 -> RequestDial
      [] step=1 -> Dial
      [] step=2 -> StartWrite(Pair,"da")
      [] step=3 -> WriteQueuedFrame(Pair,"da")
      [] step=4 -> FillReverseQueue
      [] step=5 -> HandleStartFrame(Pair,"da",1)
      [] step=6 -> CancelPendingDial(Dialer,FreshCircuit)
      [] OTHER -> FALSE
TestNext ==
    /\ IF trial = "partition" THEN ReopenReverseLink ELSE RefuseSetupReply
    /\ NoFc /\ BaseOnly /\ step' = step+1 /\ UNCHANGED trial
HandshakeSpec ==
    /\ MCInit /\ step = 0 /\ trial \in {"partition","refusal"}
    /\ [][TestNext]_CheckVars /\ WF_CheckVars(TestNext)
DialerWaits ==
    (trial = "partition" /\ step = 9) =>
        /\ ckt[FreshCircuit].dSt = EpDialWait /\ ckt[FreshCircuit].aSt = EpOpen
        /\ ~ENABLED HandleSetupAckAs(Pair,"ad",1,TRUE)
HandshakeFinishes ==
    <>(IF trial = "partition" THEN
           step=12 /\ ckt[FreshCircuit].dSt=EpOpen /\ ckt[FreshCircuit].aSt=EpOpen
       ELSE
           step=7 /\ ckt[FreshCircuit].dSt=EpGone /\ ckt[FreshCircuit].aSt=EpGone
           /\ ~conn[Pair].aOpen /\ conn[Pair].closedBy["ad"]="setup-ack-refused")
=============================================================================
