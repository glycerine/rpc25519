--------------------------- MODULE UpdateCases ---------------------------
EXTENDS tube
VARIABLE phase, trial

\* Compact projections of TestLaggingFollowerRestartIsNotFenced,
\* TestInterruptedAppendTailIsIdentifiedLocally, TestInterleavedMultipartSnapshotsBothAssemble
\* and Test1002_stale_same_term_snapshot_does_not_roll_back_follower.
StorageTrials == {"lag", "loss", "protected", "interrupted", "snap"}
PacketTrials == {"repair", "shortBatch", "oldTerm", "badPrev", "protectedWrite", "commitTerm", "compacted"}
DiskNode == LiveA
SenderNode == LiveB
ExampleLog(length) == [i \in 1..length |-> [term |-> 1, id |-> i, kind |-> "data"]]
ExampleApplied(length) == [i \in 1..length |-> [term |-> 1, id |-> i]]

FixtureInit == Init /\ phase = 0 /\ trial \in (StorageTrials \cup PacketTrials)
UseCore(action) == action /\ BaseOnly /\ NoFc /\ UNCHANGED trial
Advance(p, action) == phase = p /\ phase' = p + 1 /\ UseCore(action)

PrepareDisk ==
    LET len == CASE trial = "lag" -> 2 [] trial = "loss" -> 5 [] OTHER -> 4
        ci  == IF trial = "protected" THEN 4 ELSE 2
        cfgCI == IF trial \in {"lag", "loss"} THEN 5 ELSE ci
        s == [NS(DiskNode) EXCEPT !.term = 1, !.log = ExampleLog(len), !.idLen = len,
                                  !.high = len, !.ci = ci, !.cit = 1, !.cfg.ci = cfgCI, !.cfg.cit = 1,
                                  !.la = 2, !.lat = 1, !.appl = ExampleApplied(2)]
    IN WriteBack(DiskNode, SaveRaftState(s), conn, ckt)
DiskNext ==
    \/ Advance(0, PrepareDisk)
    \/ Advance(1, CASE trial = "lag" -> Crash(DiskNode)
                    [] trial = "loss" -> CrashDiskFault(DiskNode, "suffix", 3)
                    [] OTHER -> CrashDiskFault(DiskNode, "lastId", 2))
    \/ Advance(2, Restart(DiskNode, InitAddr))
    \/ trial = "interrupted" /\
          (\/ Advance(3, ParIdentifyInterrupted(DiskNode))
           \/ Advance(4, ParIdentifyInterrupted(DiskNode))
           \/ Advance(5, ParFinish(DiskNode)))

\* Two transfers overlap on the same circuit but use different checksums.
\* Both snapshots come from the same leader in term 1. The follower's
\* three-entry log already contains the first snapshot and still needs the fourth entry.
EnqueueSnapshots ==
    LET leader == <<SenderNode, inc[SenderNode]>>
        c == cktall[SenderNode][<<DiskNode, inc[DiskNode]>>]
        k == ckt[c].k
        dir == SendDir(conn,k,SenderNode)
        follower == [NS(DiskNode) EXCEPT !.term = 1, !.leader = leader, !.log = ExampleLog(3),
                                         !.idLen = 3, !.la = 1, !.ci = 1, !.cit = 1, !.appl = ExampleApplied(1),
                                         !.high = 3, !.lat = 1]
        state(length) == [NS(SenderNode) EXCEPT !.term = 1, !.role = Leader, !.leader = leader,
                             !.la = 1, !.ci = 1, !.cit = 1, !.lat = 1, !.appl = ExampleApplied(1),
                             !.log = ExampleLog(length), !.idLen = length, !.high = length]
        snapshot(length, id, part) ==
            LET s == state(length)
            IN [type |-> "SNAP", from |-> leader, sid |-> id, part |-> part,
                st |-> IF part > 0 THEN Nil ELSE SnapState(s,SenderNode),
                ae |-> IF part > 0 THEN Nil ELSE AEMsg(s, SenderNode, 1, 1, EntsFrom(s.log, 2, length))]
        frames == <<snapshot(2, 1, 1), snapshot(4, 2, 1), snapshot(2, 1, -2), snapshot(4, 2, -2)>>
        q == [j \in 1..Len(frames) |-> [cid |-> c, to |-> <<DiskNode, inc[DiskNode]>>, m |-> frames[j]]]
    IN WriteBack(DiskNode, follower, [conn EXCEPT ![k].q[dir] = q], ckt)
SnapshotNext ==
    \/ Advance(0, EnqueueSnapshots)
    \/ phase \in 1..4 /\ UNCHANGED <<phase,trial>>
       /\ (\E k \in DOMAIN conn, d \in Dirs : MCWriterStep(k,d)
           \/ \E n \in Server : MCUnblockSend(n))
    \/ phase \in 1..4 /\ phase' = phase + 1 /\
           UseCore(\E k \in DOMAIN conn, dir \in Dirs : Rcv(k,dir) = DiskNode /\ HandleStateSnapshot(k, dir, 1))

StorageNext == IF trial = "snap" THEN SnapshotNext ELSE DiskNext
StorageResults ==
    CASE trial = "snap" ->
             /\ (phase = 3 => Len(snapSlot[DiskNode]) = 2)
             /\ (phase = 4 => Len(snapSlot[DiskNode]) = 1 /\ Len(log[DiskNode]) = 3)
             /\ (phase = 5 => snapSlot[DiskNode] = <<>> /\ log[DiskNode] = ExampleLog(4))
      [] phase < 3 -> TRUE
      [] trial = "lag" -> par[DiskNode] = "ok" /\ Len(log[DiskNode]) = 2
      [] trial = "loss" -> par[DiskNode] = "unknown" /\ torn[DiskNode] = <<>>
      [] trial = "protected" -> par[DiskNode] = "unknown" /\ ~ENABLED ParIdentifyInterrupted(DiskNode)
      [] OTHER -> CASE phase \in {3,4} -> par[DiskNode] = "unknown" /\ Len(log[DiskNode]) = 2
                    [] phase = 5 -> par[DiskNode] = "ready" /\ idLen[DiskNode] = 4 /\ Len(log[DiskNode]) = 2
                    [] OTHER -> par[DiskNode] = "ok" /\ log[DiskNode] = ExampleLog(4) /\ torn[DiskNode] = <<>>

\* Concrete packet fixtures from Tube's regression tests. These set up the
\* sender and receiver's WAL snapshots first, then run the reference frame
\* handlers and transport writer. The protected overwrite case deliberately
\* supplies contradictory histories to check the defensive WAL contract.
E(termValue, identity) == [term |-> termValue, id |-> identity, kind |-> "data"]
Common == E(1,1)
PacketLeaderLog ==
    CASE trial = "badPrev" -> <<E(2,5), E(3,2)>>
      [] trial = "compacted" -> <<Common, E(3,2), E(3,3)>>
      [] OTHER -> <<Common, E(3,2)>>
PacketFollowerLog == <<Common, E(1,7), E(1,8)>>
PacketRoute ==
    LET c == cktall[SenderNode][<<DiskNode, inc[DiskNode]>>]
    IN [c |-> c, k |-> ckt[c].k, d |-> SendDir(conn,ckt[c].k,SenderNode)]

\* TestDiskFollowerRepairsSuffixAfterMatchingPreviousEntry,
\* TestFollowerCommitTermAtVerifiedPrefix, TestRejectedHeartbeatCannotAdvanceCommit,
\* Test1001_commit_does_not_count_follower_divergent_suffix.
PacketImage(n, lg, termValue, ci) ==
    [NS(n) EXCEPT !.term = termValue, !.leader = <<SenderNode,inc[SenderNode]>>,
         !.role = IF n = SenderNode THEN Leader ELSE Follower,
         !.log = lg, !.idLen = Len(lg), !.high = Len(lg),
         !.ci = ci, !.cit = IF ci = 0 THEN 0 ELSE lg[ci].term,
         !.la = ci, !.lat = IF ci = 0 THEN 0 ELSE lg[ci].term,
         !.appl = [i \in 1..ci |-> [id |-> lg[i].id, term |-> lg[i].term]],
         !.cfg.ci = ci, !.cfg.cit = IF ci = 0 THEN 0 ELSE lg[ci].term]

PrepareSender ==
    LET ci == IF trial \in {"shortBatch","repair","protectedWrite"} THEN 1 ELSE 2
        s == PacketImage(SenderNode,PacketLeaderLog,3,ci)
        s1 == IF trial = "compacted" THEN [s EXCEPT !.base = [idx |-> 2, term |-> 3]] ELSE s
    IN WriteBack(SenderNode,SaveRaftState(s1),conn,ckt)

PrepareReceiver ==
    LET ci == CASE trial = "protectedWrite" -> 2
                [] trial \in {"shortBatch","repair"} -> 1
                [] OTHER -> 0
        s == PacketImage(DiskNode,PacketFollowerLog,IF trial="oldTerm" THEN 4 ELSE 3,ci)
        l == NS(SenderNode)
        prev == IF trial \in {"badPrev","repair","protectedWrite"} THEN 1 ELSE 0
        ents == CASE trial="shortBatch" -> EntsFrom(l.log,1,1)
                   [] trial \in {"repair","protectedWrite"} -> EntsFrom(l.log,2,2)
                   [] OTHER -> <<>>
        ae == AEMsg(l,SenderNode,prev,IF prev = 0 THEN 0 ELSE l.log[prev].term,ents)
        r == PacketRoute
        f == [cid |-> r.c, to |-> <<DiskNode,inc[DiskNode]>>, m |-> ae]
    IN WriteBack(DiskNode,SaveRaftState(s),[conn EXCEPT ![r.k].q[r.d] = <<f>>],ckt)

PacketNext ==
    \/ Advance(0,PrepareSender)
    \/ Advance(1,PrepareReceiver)
    \/ Advance(2, HandleAppendEntries(PacketRoute.k,PacketRoute.d,1))
    \/ Advance(3, \E dir \in Dirs :
          Rcv(PacketRoute.k,dir) = SenderNode /\ HandleAppendEntriesAck(PacketRoute.k,dir,1))
    \/ phase = 3 /\ UNCHANGED <<phase,trial>>
       /\ (\E dir \in Dirs : MCWriterStep(PacketRoute.k,dir)
          \/ \E n \in Server : MCUnblockSend(n))

PacketResults ==
    IF phase < 3 THEN TRUE
    ELSE LET l == NS(SenderNode)
             f == NS(DiskNode)
             k == <<DiskNode, inc[DiskNode]>>
             acks == {m \in QueuedMessages : m.type = "AEA" /\ m.from = k}
         IN /\ (phase = 3 => acks /= {})
            /\ CASE trial = "repair" ->
                    /\ f.log = PacketLeaderLog
                    /\ f.ci = 1 /\ f.cit = 1
                    /\ \A a \in acks : ~a.rej /\ a.lcri = 2
                    /\ (phase = 4 => l.peers[k].mi = 2)
                 [] trial = "shortBatch" ->
                    /\ f.log = PacketFollowerLog /\ f.ci = 1
                    /\ \A a \in acks : a.last > a.lcri /\ a.lcri = 1
                    /\ (phase = 4 => l.peers[k].mi = 1 /\ l.ci = 1)
                 [] trial \in {"oldTerm","badPrev"} ->
                    /\ f.log = PacketFollowerLog /\ f.ci = 0
                    /\ \A a \in acks : a.rej
                 [] trial = "protectedWrite" ->
                    /\ f.log = PacketFollowerLog /\ f.la = 2 /\ f.ci = 2
                    /\ \A a \in acks : a.rej /\ a.needSnap
                 [] trial = "commitTerm" ->
                    /\ f.ci = 1 /\ f.cit = 1 /\ f.cfg.ci = 1 /\ f.cfg.cit = 1
                 [] trial = "compacted" ->
                    /\ f.ci = 0
                    /\ \A a \in acks : a.rej /\ a.needSnap /\ a.lcri = 0

FixtureNext == IF trial \in StorageTrials THEN StorageNext ELSE PacketNext
fixtureVars == <<mcVars,phase,trial>>
FixtureSpec == FixtureInit /\ [][FixtureNext]_fixtureVars /\ WF_fixtureVars(FixtureNext)
FixtureResults == IF trial \in StorageTrials THEN StorageResults ELSE PacketResults
EveryFixtureFinishes ==
    <>(phase = CASE trial = "interrupted" -> 6 [] trial = "snap" -> 5
                  [] trial \in StorageTrials -> 3 [] OTHER -> 4)
=============================================================================
