---- MODULE DurableChecks ----
EXTENDS tube

N == CHOOSE n \in Server : TRUE
Other == CHOOSE n \in Server : n /= N
Key == <<N,inc[N]>>

InstallAndPromote ==
    LET s == [NS(N) EXCEPT !.term = 1, !.vote = Key, !.ci = 1, !.cit = 1, !.high = 5]
        update == [s.cfg EXCEPT !.cv = @ + 1, !.com = FALSE]
        installed == SetMC(s,N,update)
    IN [installed EXCEPT !.cfg.com = TRUE, !.term = 2, !.vote = Nil,
                         !.ci = 2, !.cit = 2, !.high = 6,
                         !.known[N] = Nil, !.sh = {N}]

DurableAction ==
    /\ fc.crash = 0
    /\ LET staged == InstallAndPromote IN
        \/ WriteBack(N,staged,conn,ckt)
        \/ CrashWith(N,log[N],idLen[N],staged) /\ UNCHANGED nextId
    /\ Bump("crash") /\ BaseOnly

DurableSpec == MCInit /\ [][DurableAction]_mcVars

ExpectedSave ==
    fc.crash = 1 => /\ dTerm[N] = 1 /\ dVote[N] = Key
                    /\ ~dCfg[N].com /\ dCfg[N].cv = 1 + (NS(Other).cfg.cv)
                    /\ dShadows[N] = {} /\ dKnownPeers[N][N] /= Nil
                    /\ dCommit[N] = 1 /\ dCommitTerm[N] = 1
                    /\ appendWitness[N].saved = 5
                    /\ up[N] => /\ config[N].com /\ currentTerm[N] = 2 /\ votedFor[N] = Nil
                                /\ knownPeers[N][N] = Nil /\ shadows[N] = {N}
                                /\ commitIndex[N] = 2 /\ commitIndexTerm[N] = 2 /\ appendWitness[N].current = 6

RepeatedSave ==
    LET saved == SaveRaftState([NS(N) EXCEPT !.term = 3, !.vote = Key])
        restored == SaveRaftState([saved EXCEPT !.vote = Nil, !.ci = 2])
        rejected == SaveRaftState([restored EXCEPT !.vote = <<Other,inc[Other]>>, !.ci = 3])
    IN /\ restored.vote = Key /\ restored.saved.vote = Key /\ restored.saved.ci = 2
       /\ "PARMetaVoteConflict" \in rejected.newPanics /\ rejected.saved = restored.saved
BeforeSave == fc.crash = 0
====
