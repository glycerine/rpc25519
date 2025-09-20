tube: a small RAFT in Golang
====

Author: Jason E. Aten, Ph.D.

Initial release: 2025 September 18


The rpc25519/tube/ subdirectory contains Tube, 
a tested and working implementation of Diego Ongaro's Raft algorithm
for distributed consensus (https://raft.github.io/). 
Raft is a better organized and more concretely 
specified version of Multi-Paxos.

There is a getting started with Tube guide here:
https://github.com/glycerine/rpc25519/blob/master/tube/example/local/README.md

From the Tube introduction in [tube.go](https://github.com/glycerine/rpc25519/blob/master/tube/tube.go#L5), the central implementation file, the
name Tube is midwestern shorthand for "innertube",
and is referring the the sport of "tubing", which
involves rafting down rivers.

> A tube is a small raft, delightfully
> used for floating down rivers on a sunny
> summer's day in some parts of the world.

Tube implements the Pre-voting and 
Sticky-leader optimizations, and includes
core Raft (chapter 3 of Ongaro's 2014 Raft dissertation),
membership changes (chapter 4), log compaction (chapter 5),
and client-side interaction sessions for 
linearizability (chapter 6). We use the
Mongo-Raft-Reconfig algorithm for single-server-at-a-time
membership changes ( https://arxiv.org/abs/2102.11960 ),
as it provides a clean separation of membership
and Raft log data; and its safety has been formally proven.

Tube provides a basic sorted key/value store as its
baseline/sample/example state machine (which may suffice
for many use cases). User program defined replicated
operations -- custom state machine actions for
a user defined machine -- are
planned but still TODO at the moment. The built in
key/value store allows arbitrary []byte or string keys to
refer to arbitrary []byte slice values. Multiple
key-value namespaces, each called a "table", are
available. See the tube/cmd/tup utility for a 
sample client/client code.

Testing uses the Go testing/synctest package
(https://pkg.go.dev/testing/synctest) 
and our own gosimnet network simulator
(https://github.com/glycerine/gosimnet) whose
implementation is embedded here in simnet.go.

On Go 1.24.3 or Go 1.25, the synctest tests
run with `GOEXPERIMENT=synctest go test -v`;
or `GOEXPERIMENT=synctest go test -v -race`.

more in depth discussion/details from tube.go
--------------------

A tube is a small raft, delightfully
used for floating down rivers on a sunny
summer's day in some parts of the world.

Tube, this package, gives all of the core
Raft algorithm in a deliberately small,
compact form. All the core Raft logic
is in this file, along with important
and common optimizations like pre-voting
and sticky-leader.

Some externally oriented client facing
APIs are in admin.go. The Raft write ahead log
is in wal.go. The nodes save their persistent
state using persistor.go. A simple key-value
store finite-state-machine is implemented
whose actions are defined in actions.go.

We communicate log summaries within the cluster using
a run-length-encoding of the term history
called a TermsRLE in the rle.go file.
There are just a handful of files, and
nearly all of the implementation's behavior
and optimizations can be readily
discerned from this central tube.go file.

This makes Tube great for understanding,
using, and even extending the Raft algorithm.

Log compaction and snapshots are implemented (chapter 5).

The client session system from chapter 6 is
implemented to preserve linearizability
(aka linz for short).

Clients can use CreateNewSession to establish
a server side record of their activity, and thus obtain
linearizability (exactly once semantics) within
that session. The client must increment a
SessionSerial number with each request. If the
server detects a missing serial number, it informs
the client by erroring out the Session. If
the server sees a repeated (duplicate) serial number,
it returns the cached reply rather than
re-replicate the operation. See Chapter 6 of
the Raft dissertation for details.

Membership changes (chapter 4) are
single-server-at-a-time (SSAAT)
and utilize the Mongo-Raft-Reconfig algorithm
from https://arxiv.org/abs/2102.11960
See also [1][2][3].

We prefer the Mongo-Raft-Reconfig algorithm over the
dissertation SSAAT algorithm because, in
addition to being model checked and used
successfully in for several years in MongoDB operations:

a) it has been formally proven. Neither of the
dissertation membership change algorithms has been
formally proven, and thus naturally bugs have
been found subsequent to their publication[4].
Despite this known concern, they have still not been
formally proven.

b) it keeps the MemberConfig (MC) separate from
the raft log. This greatly simplifies membership
management, especially when the raft log has to be truncated
due to leadership change. As a "logless" algorithm
the membership is kept instead in the persistent
(state-machine) state which is separate
from the Raft log.

[1] https://conf.tlapl.us/2024/SiyuanZhou-HowWeDesignedAndModelCheckedMongoDBReconfigurationProtocol.pdf

[2] https://will62794.github.io/distributed-systems/consensus/2025/08/25/logless-raft.html

[3] https://github.com/will62794/logless-reconfig

[4] https://groups.google.com/d/msg/raft-dev/t4xj6dJTP6E/d2D9LrWRza8J
"bug in single-server membership changes" post by Diego Ongaro
2025 Jul 10


On Pre-Voting
-------------

Pre-voting gives a more stable cluster.
The idea is that a singleton node (or any
minority count of nodes) is never
able to elect itself leader, by design,
to prevent the zombie apocolypse.

Just kidding? I am not. Zombie nodes
are in a minority, cut-off from the rest of the
cluster by partition (network partially
goes down/network card fails).
The have lived in a half-dead state "on the other side of
the fence" for a while, and so
in all likelihood have started elections
trying to find a new leader. With lots
of failed elections, each one incrementing
their current term, when zombies
"come back from the dead" (the parition
heals/network comes back up), they
disrupt the majority cluster with their
local high "current" term number,
which in core Raft forces delays and
possible leader churn resulting
in cluster instability.

While the zombies won't win the election,
since by Raft rules their logs are too
far out of date, fighting them off
takes resources and time away from serving
client requests (all reads/writes are
blocked in the meantime, since who should
be leader is in doubt).

The Pre-Vote mechanism is highly desirable,
because it prevents all this.

(Apparently Cloudflare endured six hour
control plane outage in 2020 due to
a network switch failure because they
did not run Etcd with the pre-vote
turned on).
https://blog.cloudflare.com/a-byzantine-failure-in-the-real-world/

So: the pre-vote mechanism is always
used here in Tube.

In addition to preventing leader churn,
PreVote creates a leader read-lease, so
that the leader can serve reads locally from
memory without violating linearizability.

This is much faster than waiting to
hear from a quorum across the network.
You really want PreVoting for this
optimization alone. Tube will automatically
check if all the conditions for serving
reads locally are met (there are a bunch
that I'm not going into here, but they
are not that onerous to meet in normal
operation), and will do
so if it can maintain safety. It
does this automatically, but this optimizatoin
requires PreVoting to be used on
all cluster nodes. If the leader has
heard from a quorum recently, it knows
it cannot have be deposed before
another leader election timeout, since any
other node will lose a pre-vote. The
trade-off is enduring the same window of
non-availability on leader crash:
usually very much worth it.

Protocol Aware Recovery (PAR) is half implemented;
the wal.go and par.go files have some
mechanism, but recovery and online
checks have not been fully completed.
See too the notes at the end of this (tube.go) file.

------
Copyright (C) 2025 Jason E. Aten, Ph.D. All rights reserved.
