------------------------ MODULE TicketGuardFixture ------------------------
EXTENDS tube
\* A replica can receive the entry before it learns that the leader applied it.
\* The maker's live waiter models the unserialized pointer of prepOne.
TwoApplyPaths ==
    \A n,m \in Server : n /= m =>
        LET s == RegisterDataTicket(NS(n), FALSE)
            r == [id |-> 42, idx |-> 1, maker |-> <<m,inc[m]>>]
            local == [r EXCEPT !.maker = s.self]
            followerCopy == [s EXCEPT !.book[1].entries = {r}, !.book[1].stage[s.self] = "follow"]
            formerLeader == [s EXCEPT !.book[1].entries = {local}, !.book[1].stage[s.self] = "indexed"]
            entry == [id |-> 42, kind |-> "data", term |-> 1]
            f == CompleteAppliedDataTicket(followerCopy, entry, FALSE)
            l == CompleteAppliedDataTicket(formerLeader, entry, FALSE)
            late == FinishDataTicket(f, 1, "stepdown", 0, "local")
        IN /\ f.book[1].result = [status |-> "applied", entry |-> 42, via |-> "follower"]
           /\ l.book[1].result = Nil
           /\ late.book[1].result = f.book[1].result
FixtureNext == UNCHANGED vars
FixtureSpec == Init /\ [][FixtureNext]_vars
=============================================================================
