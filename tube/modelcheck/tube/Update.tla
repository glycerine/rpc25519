------------------------------- MODULE Update -------------------------------
EXTENDS tube

\* Properties for the multi-voter, ordinary (quorum-admitted) Raft mode.
\* When a RequestVote first enters the outgoing pipeline, its sender must
\* already have saved that term and self-vote.
\* tube/tube.go: persistSelfVote, beginElection, onRestartRecoverPersistentRaftStateFromDisk.
RequestVotePersistedBeforeSend ==
    [][\A m \in QueuedMessages' \ QueuedMessages :
        IF m.type = "RV" THEN
            LET n == KName(m.from) IN dTerm'[n] = m.cterm /\ dVote'[n] = m.from
        ELSE TRUE]_mcVars

RestartReconcilesLogTerm ==
    [][\A n \in Server :
        (~up[n] /\ up'[n]) => currentTerm'[n] >= LastTermL(log'[n], base'[n])]_mcVars

\* tube/wal.go: overwriteEntries; tube/tube.go: advanceFollowerCommit,
\* applyNewStateSnapshot. Inspect live protocol transitions between storage
\* faults. Identified, applied entries must survive suffix repair and install.
AppliedPrefixRetained ==
    [][\A n \in Server :
        (up[n] /\ up'[n] /\ inc[n] = inc'[n] /\ par[n] = "ok" /\ par'[n] = "ok") =>
             /\ lastApplied'[n] >= lastApplied[n]
             /\ IsPrefix(applied[n], applied'[n])
             /\ Len(log'[n]) >= lastApplied[n]
             /\ \A i \in 1..lastApplied[n] : log'[n][i].id = applied[n][i].id]_mcVars

\* Membership observations use actual waiter IDs and the config that the
\* leader acknowledged (changeMembership, resolveMCAwaitingCommit, becomeFollower).
NoPendingOnStepDown ==
    [][\A n \in Server :
        (up[n] /\ up'[n] /\ role[n] = Leader /\ role'[n] = Follower) =>
            (stallQ'[n] = <<>> /\ mcPending'[n].awaiting = {})]_mcVars

\* The bounded baseline remains the reference for interactions. The two
\* focused schedules below restrict when a timer/fault/request can occur;
\* each selected action is still taken through tube's MC wrappers.
FullUpdateSpec == MCSpec

ElectionIO ==
    \/ MCNextFrames
    \/ \E n \in Server : MCBecomeLeaderSave(n) \/ MCUnblockSend(n) \/ MCResumeCampaign(n)
    \/ \E k \in DOMAIN conn, d \in Dirs : MCAckFlush(k,d) \/ MCWriterStep(k,d)

ElectionAt(n) == MCElectionTimeout(n) \/ MCPreVotePhase2Timeout(n)

\* Crash the designated campaigner after RequestVote and before becoming
\* leader. Its durable vote must survive a change of process incarnation.
CrashCampaignNext ==
    \/ ElectionIO
    \/ inc[LiveA] = 0 /\ ElectionAt(LiveA)
    \/ role[LiveA] = Candidate
       /\ (\E m \in QueuedMessages : m.type = "RV" /\ m.from = <<LiveA,inc[LiveA]>>)
       /\ MCCrash(LiveA)
    \/ MCRestart(LiveA, InitAddr)

CrashCampaignSpec == MCInit /\ [][CrashCampaignNext]_mcVars

\* Reachability canaries. Adding one as an INVARIANT asks TLC for an
\* ordinary execution reaching the stated milestone.
BeforeReplicatedWrite ==
    ~(\A n \in InitMembers : \E i \in 1..lastApplied[n] : log[n][i].kind = "data")

BeforeRedirectedForceApplied ==
    ~(\E redirect, appliedEvent \in adminEvents :
        /\ redirect.result = "redirect" /\ appliedEvent.result = "applied"
        /\ appliedEvent.op \in {"add","remove"} /\ redirect.op = appliedEvent.op
        /\ redirect.x = appliedEvent.x /\ redirect.node /= appliedEvent.node)

BeforeCampaignerRestarts ==
    ~(up[LiveA] /\ inc[LiveA] = 1 /\ dTerm[LiveA] > 0 /\
      dVote[LiveA] = <<LiveA,0>> /\ currentTerm[LiveA] = dTerm[LiveA])

\* First elect LiveA, then choose a reconfiguration attempt and allow its
\* acknowledgements/timeout/step-down to race. Network handlers retain the
\* complete ordinary reference behavior. The profile bounds unsolicited
\* failures and the number of writes and changes.
MembershipNext ==
    \/ ElectionIO
    \/ ElectionAt(LiveA)
    \/ MCLeaderHeartbeat(LiveA)
    \/ MCClientChangeMembership(LiveA, "remove", LiveB)
    \/ MCLeaderAge(LiveA)
    \/ MCDeadlineTick(LiveA)
    \/ \E k \in Keys : MCLeaseCreditExpire(LiveA,k) \/ MCPongExpire(LiveA,k)

MembershipSpec == MCInit /\ [][MembershipNext]_mcVars

BeforeMembershipAck == ackedCfg = {}
BeforePendingChangeStepsDown ==
    [][~(\E n \in Server :
        /\ role[n] = Leader /\ role'[n] = Follower /\ up[n] /\ up'[n]
        /\ (stallQ[n] /= <<>> \/ mcPending[n].awaiting /= {}))]_mcVars

\* Single-voter liveness under a stable, unfaulted environment. No bound
\* escape is present. Only the eligible election steps and post-broadcast
\* save must be scheduled. A successful run reaches an actual applied noop.
SoloElectionNext == ElectionAt(LiveA) \/ MCBecomeLeaderSave(LiveA) \/ MCResumeCampaign(LiveA)
SoloElectionSpec ==
    /\ MCInit
    /\ [][SoloElectionNext]_mcVars
    /\ WF_mcVars(MCElectionTimeout(LiveA))
    /\ WF_mcVars(MCPreVotePhase2Timeout(LiveA))
    /\ WF_mcVars(MCBecomeLeaderSave(LiveA))
    /\ WF_mcVars(MCResumeCampaign(LiveA))

SoloEventuallyCommits == <>(role[LiveA] = Leader /\ noop0[LiveA].done /\ lastApplied[LiveA] > 0)

\* Commit once, lose the process, recover the same disk, then elect and apply
\* a fresh no-op. Requiring both terms distinguishes this from the first election.
CrashCommittedSingleton ==
    /\ fc.crash = 0 /\ role[LiveA] = Leader /\ noop0[LiveA].done
    /\ MCCrash(LiveA)
RestartSingleton == MCRestart(LiveA,InitAddr)
TimeoutOncePerBoot == fc.eto = inc[LiveA] /\ MCElectionTimeout(LiveA)
RecoverElectionNext ==
    \/ TimeoutOncePerBoot \/ MCPreVotePhase2Timeout(LiveA) \/ MCBecomeLeaderSave(LiveA)
    \/ MCResumeCampaign(LiveA) \/ MCRunDeferredCampaign(LiveA)
    \/ CrashCommittedSingleton \/ RestartSingleton
    \/ MCRestartWindowTick(LiveA) \/ MCRestartWindowElapsed(LiveA)
RecoverElectionSpec ==
    /\ MCInit /\ [][RecoverElectionNext]_mcVars
    /\ WF_mcVars(CrashCommittedSingleton) /\ WF_mcVars(RestartSingleton)
    /\ WF_mcVars(MCRestartWindowTick(LiveA)) /\ WF_mcVars(MCRestartWindowElapsed(LiveA))
    /\ WF_mcVars(TimeoutOncePerBoot) /\ WF_mcVars(MCPreVotePhase2Timeout(LiveA))
    /\ WF_mcVars(MCRunDeferredCampaign(LiveA))
    /\ WF_mcVars(MCBecomeLeaderSave(LiveA)) /\ WF_mcVars(MCResumeCampaign(LiveA))
PostRestartCommit ==
    <>(inc[LiveA] = 1 /\ role[LiveA] = Leader /\ noop0[LiveA].done
       /\ currentTerm[LiveA] = 2 /\ commitIndexTerm[LiveA] = 2 /\ lastApplied[LiveA] >= 2)
=============================================================================
