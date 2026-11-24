---------------------------- MODULE ElectionChecks ----------------------------
EXTENDS tube

VARIABLE stage
v == <<mcVars, stage>>
N == LiveA

\* Recover a voter, request a pre-vote then an election then another pre-vote,
\* and let the deferred request run. Crash when the self-vote has been sent.
RInit == MCInit /\ stage = 0
Explicit(kind) == ExplicitCampaignDuringRestart(N,kind) /\ NoFc /\ BaseOnly
RStep ==
    /\ CASE stage = 0 -> MCCrash(N)
         [] stage = 1 -> MCRestart(N,InitAddr)
         [] stage = 2 -> Explicit("preVote")
         [] stage = 3 -> Explicit("election")
         [] stage = 4 -> Explicit("preVote")
         [] stage = 5 -> MCRestartWindowElapsed(N)
         [] stage = 6 -> MCRunDeferredCampaign(N)
         [] stage = 7 -> MCResumeCampaign(N)
         [] stage = 8 -> MCCrash(N)
         [] stage = 9 -> MCRestart(N,InitAddr)
         [] OTHER -> FALSE
    /\ stage' = stage + 1
R == RInit /\ [][RStep]_v /\ WF_v(RStep)
RDone == <>(stage = 10)
RProof ==
    /\ stage \in 2..5 => currentTerm[N] = 0 /\ role[N] = Follower /\ campaign[N].held
    /\ stage = 3 => campaign[N].deferred = "preVote"
    /\ stage \in 4..6 => campaign[N].deferred = "election"
    /\ stage = 6 => ~campaign[N].held
    /\ stage = 7 => campaign[N].resume = "requestVotes" /\ role[N] = Candidate
                      /\ dVote[N] = <<N,1>> /\ dTerm[N] = 1
    /\ stage = 8 => campaign[N].resume = "none" /\ vYes[N] = {N}
    /\ stage = 10 => inc[N] = 2 /\ role[N] = Follower /\ campaign[N].held
                       /\ dVote[N] = <<N,1>> /\ votedFor[N] = dVote[N] /\ currentTerm[N] = 1

\* First start of a previously unconfigured replica, then self-add.
BStep ==
    /\ CASE stage = 0 -> MCRestart(N,InitAddr)
         [] stage = 1 -> BootstrapSelf(N) /\ NoFc /\ BaseOnly
         [] stage \in {2,3} -> MCResumeCampaign(N)
         [] OTHER -> FALSE
    /\ stage' = stage + 1
B == RInit /\ [][BStep]_v /\ WF_v(BStep)
BDone == <>(stage = 4)
BProof ==
    /\ stage = 1 => config[N].mem = {} /\ ~campaign[N].held
    /\ stage = 2 => campaign[N].resume = "bootstrap" /\ role[N] = Follower
                       /\ dTerm[N] = 1 /\ dVote[N] = <<N,1>> /\ dCfg[N].mem = {N}
    /\ stage = 3 => role[N] = Leader /\ currentTerm[N] = 1 /\ config[N].mem = {N}
    /\ stage = 4 => lastApplied[N] = 1 /\ campaign[N].resume = "none"

\* An image from before persistSelfVote, with RaftState in term zero and
\* one fsynced, identified term-three entry. Only the input image is seeded.
\* The two restarts and the resulting vote are the normal startup relation.
OldImage ==
    /\ CrashWith(N,<<[term |-> 3, id |-> nextId, kind |-> "noop"]>>,1,NS(N))
    /\ nextId' = nextId+1 /\ NoFc /\ BaseOnly

\* Focus on the restart-delay projection. The local AE kernel may discard a
\* stale heartbeat after handling the timer. The saver below is the same
\* kernel called when handleRequestVote records a newer term and its vote.
LateHeartbeat ==
    LET ae == [AEMsg(NS(LiveB),LiveB,0,0,<<>>) EXCEPT !.term = 1]
        r == AEStep(NS(N),N,ae)
    IN WriteBack(N,r.s,conn,ckt) /\ NoFc /\ BaseOnly
OrdinarySave == WriteBack(N, SaveRaftState(NS(N)),conn,ckt) /\ NoFc /\ BaseOnly
RebootSmall == MCRestart(N,InitAddr) /\ campaign'[N].configured = 1
DStep ==
    /\ CASE stage = 0 -> OldImage
         [] stage = 1 -> RebootSmall
         [] stage = 2 -> MCRestartWindowTick(N)
         [] stage = 3 -> MCRestartWindowElapsed(N)
         [] stage = 4 -> MCCrash(N)
         [] stage = 5 -> RebootSmall
         [] stage = 6 -> MCRestartWindowTick(N)
         [] stage = 7 -> MCRestartWindowElapsed(N)
         [] stage = 8 -> LateHeartbeat
         [] stage = 9 -> OrdinarySave
         [] stage = 10 -> MCCrash(N)
         [] stage = 11 -> RebootSmall
         [] OTHER -> FALSE
    /\ stage' = stage + 1
D == RInit /\ [][DStep]_v /\ WF_v(DStep)
DDone == <>(stage=12)
DProof ==
    /\ stage \in {2,6} => campaign[N].remaining = 2 /\ campaign[N].persisted = 2
    /\ stage \in {3,7} => campaign[N].remaining = 1 /\ campaign[N].held
    /\ stage \in {4,8} => ~campaign[N].held /\ campaign[N].promise = 2
    /\ stage = 9 => campaign[N].promise = 1 /\ campaign[N].persisted = 2 /\ ~campaign[N].releaseOnAE
    /\ stage = 10 => campaign[N].persisted = 1
    /\ stage = 12 => campaign[N].held /\ campaign[N].remaining = 1

WStep ==
    /\ CASE stage = 0 -> OldImage
         [] stage = 1 -> MCRestart(N,InitAddr)
         [] stage = 2 -> MCCrash(N)
         [] stage = 3 -> MCRestart(N,InitAddr)
         [] OTHER -> FALSE
    /\ stage' = stage + 1
W == RInit /\ [][WStep]_v /\ WF_v(WStep)
WDone == <>(stage=4)
WProof ==
    /\ stage = 1 => LastTerm(N)=3 /\ dTerm[N]=0 /\ ~up[N]
    /\ stage \in {2,4} => up[N] /\ currentTerm[N]=3 /\ dTerm[N]=3
                             /\ votedFor[N] = <<N,1>> /\ dVote[N] = <<N,1>>
ExistingFollowerInit == CoreInitWithRestored({N})
Receive(kind) ==
    \E k \in DOMAIN conn, dir \in Dirs : \E i \in FrameIdx(k,dir) :
       /\ Rcv(k,dir) = N
       /\ IF kind="RPV" THEN HandleRequestPreVote(k,dir,i) ELSE HandleRequestVote(k,dir,i)
       /\ NoFc /\ BaseOnly
FromVoter(kind,granted) ==
    \E m \in QueuedMessages :
        /\ m.type=kind
        /\ m.from= <<N,inc[N]>> /\ m.cand = <<LiveB,inc[LiveB]>> /\ m.granted=granted
HStep ==
    /\ CASE stage=0 -> MCElectionTimeout(LiveB)
         [] stage=1 -> Receive("RPV")
         [] stage=2 -> WriteBack(LiveB,BeginElection(NS(LiveB),LiveB),conn,ckt) /\ NoFc /\ BaseOnly
         [] stage=3 -> MCResumeCampaign(LiveB)
         [] stage=4 -> Receive("RV")
         [] stage=5 -> MCRestartWindowElapsed(N)
         [] stage=6 -> WriteBack(LiveB,SendRequestVotes(NS(LiveB),LiveB),conn,ckt) /\ NoFc /\ BaseOnly
         [] stage=7 -> Receive("RV")
         [] OTHER -> FALSE
    /\ stage'=stage+1
HPump ==
    /\ stage \in 1..7
    /\ \/ \E k \in DOMAIN conn, d \in Dirs : MCWriterStep(k,d)
       \/ \E n \in Server : MCUnblockSend(n)
    /\ UNCHANGED stage
H == RInit /\ [][HStep \/ HPump]_v /\ WF_v(HStep) /\ WF_v(HPump)
HDone == <>(stage=8)
RVFromCandidate == \E m \in QueuedMessages : m.type="RV" /\ m.from = <<LiveB,inc[LiveB]>>
HProof ==
    /\ stage=3 => ~RVFromCandidate /\ campaign[LiveB].resume="requestVotes"
    /\ stage=4 => RVFromCandidate /\ campaign[LiveB].resume="none"
    /\ stage \in {3,4} => dTerm[LiveB]=1 /\ dVote[LiveB] = <<LiveB,inc[LiveB]>>
    /\ stage=2 => FromVoter("PV",FALSE) /\ campaign[N].held
    /\ stage=5 => FromVoter("V",FALSE) /\ votedFor[N]=Nil /\ currentTerm[N]=0
    /\ stage=8 => FromVoter("V",TRUE) /\ dVote[N] = <<LiveB,inc[LiveB]>> /\ currentTerm[N]=1

\* In a singleton, the campaign does not save a vote before the noop. An
\* ordinary crash can retain that fsynced entry ahead of the durable term.
SStep ==
    /\ CASE stage=0 -> MCElectionTimeout(N)
         [] stage=1 -> MCPreVotePhase2Timeout(N)
         [] stage=2 -> MCCrash(N)
         [] stage=3 -> MCRestart(N,InitAddr)
         [] OTHER -> FALSE
    /\ stage'=stage+1
S == RInit /\ [][SStep]_v /\ WF_v(SStep)
SDone == <>(stage=4)
SProof ==
    /\ stage=2 => role[N]=Leader /\ campaign[N].resume="commitNoop"
                 /\ dTerm[N]=0 /\ currentTerm[N]=1 /\ LastTerm(N)=1 /\ lastApplied[N]=0
    /\ stage=3 => ~up[N] /\ dTerm[N]=0 /\ LastTerm(N)=1
    /\ stage=4 => role[N]=Follower /\ dTerm[N]=1 /\ currentTerm[N]=1
                 /\ dVote[N] = <<N,1>> /\ votedFor[N]=dVote[N]
================================================================================
