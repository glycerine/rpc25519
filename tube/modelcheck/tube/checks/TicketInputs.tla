-------------------- MODULE TicketInputs --------------------
EXTENDS tube
\* A deadline on a transport write does not request leader discovery.
CheckTicketInputs ==
    \A n \in InitMembers :
        LET plain == RegisterDataTicket(NS(n), FALSE)
            p == RouteNewTicket(plain,1)
            admin == RegisterDataTicket(NS(n), TRUE)
            cmd == [kind |-> "remove", x |-> n, detail |-> Nil, addr |-> InitAddr]
            a == RouteNewTicket([admin EXCEPT !.book[1].command=cmd],1)
        IN /\ p.book[1].result.status = "redirectError" /\ ~p.book[1].stashed
           /\ a.book[1].result = Nil /\ a.book[1].stashed

\* A forwarded removal of an absent voter is successful without a log write.
\* A delayed add must still satisfy P1 or its deadline. Shadow changes instead
\* go into the replicated-entry pipeline, and reject an already-voting target.
CheckAdminRouting ==
    \A l,o \in InitMembers, x \in Server \ InitMembers : l /= o =>
        LET client == RegisterDataTicket(NS(o),TRUE)
            leader == [NS(l) EXCEPT !.role = Leader, !.book = client.book]
            run(kind, target, old) ==
               LET cmd == [kind |-> kind, x |-> target, detail |-> [inc |-> -1,addr |-> InitAddr],addr |-> InitAddr]
                   s == [leader EXCEPT !.book[1].command = cmd, !.book[1].old = old]
               IN DispatchAdminRequests(AdmitDataTicket(s,1),1)
            n == run("remove",x,FALSE)
            late == run("add",x,TRUE)
            shadow == run("shadowAdd",x,FALSE)
            invalid == run("shadowAdd",o,FALSE)
        IN /\ n.book[1].mcs[n.self] \in n.mc.noChange
           /\ \E i \in DOMAIN n.out : n.out[i].m.type="TKTRES" /\ n.out[i].m.status="unchanged"
           /\ \E i \in DOMAIN late.out : late.out[i].m.type="TKTRES" /\ late.out[i].m.status="admissionTimeout"
           /\ shadow.book[1].stage[shadow.self] = "unindexed"
           /\ \E i \in DOMAIN invalid.out : invalid.out[i].m.type="TKTRES" /\ invalid.out[i].m.status="invalidShadow"

FixtureNext == UNCHANGED vars
FixtureSpec == Init /\ [][FixtureNext]_vars
=====================================================================
