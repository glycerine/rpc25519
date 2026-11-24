---------------------------- MODULE StartupCheck ----------------------------
EXTENDS tube
LeaderN == LiveA
Fast == LiveB
Slow == CHOOSE n \in Server \ {LeaderN,Fast} : TRUE

\* Deliver election messages from both voters, but replicate the no-op only
\* to the fast voter. Drive the leader past its configured startup interval
\* only once its first positive acknowledgement has been recorded.
FastReplication ==
    \E k \in DOMAIN conn, d \in Dirs : \E i \in FrameIdx(k,d) :
        /\ (Rcv(k,d) /= Slow \/ FrameAt(k,d,i).m.type \notin {"AE", "SNAP"})
        /\ MCFrame(k,d,i)

ReachedFastAck == IF peers[LeaderN][<<Fast,0>>] = Nil THEN FALSE
                  ELSE peers[LeaderN][<<Fast,0>>].mi > 0

TickAfterAck == ReachedFastAck /\ MCStartupClockTick(LeaderN)
NextCheck ==
    \/ MCElectionTimeout(LeaderN)
    \/ MCPreVotePhase2Timeout(LeaderN)
    \/ MCResumeCampaign(LeaderN)
    \/ MCBecomeLeaderSave(LeaderN)
    \/ FastReplication
    \/ TickAfterAck
    \/ startupPhase[LeaderN] = "recheck" /\ MCLeaderHeartbeat(LeaderN)
\* This schedule takes Tube's ready-write path on every connection.
\* Leave enough buffer room for the unread no-op and heartbeat on Slow.
ReadyWrites == \A n \in Server : blk[n].wait = Nil
CheckSpec == MCInit /\ [][NextCheck /\ ReadyWrites']_mcVars

ClockCorrect ==
    /\ Quorum(LeaderN) = (IF startupPhase[LeaderN] = "unanimous" THEN 3 ELSE 2)
    /\ Quorum(Fast) = 2
PendingWrite ==
    startupPhase[LeaderN] \in {"unanimous","deadline","recheck"} => commitIndex[LeaderN] = 0
\* The non-growing accepted ack that consumes the recheck must commit.
CommitAfterRecheck ==
    startupPhase[LeaderN] = "majority" => commitIndex[LeaderN] = 1
BeforeCommit == commitIndex[LeaderN] = 0
=============================================================================
