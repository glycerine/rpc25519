package main

/*
https://hermesp.quora.com/

Q & A by
Antonis Katsarakis
Hermes' Author

Q: Why does Hermes treat RMWs different from a Write, is it necessary?

RMWs are semantically stronger than
linearizable writes. RMWs need consensus
while linearizable writes need not.
Therefore, Hermes’ implementation of
RMWs could seamlessly support writes.
However, this raises the following question.

Can we do a more efficient implementation
of writes without paying the overhead for consensus?

Hermes treats writes differently than RMWs
for efficiency. This allows writes in Hermes
to be non-conflicting in contrast to RMWs.
In particular, Hermes’ writes need never
abort even if there are multiple concurrent
writes to the same object without violating
linearizability.



Q: What's the disadvantage of Hermes compare with Paxos?

A: Hermes' local reads and fast concurrent writes
(1 round-trip) from all replicas are much more
efficient than state machine replication through
Paxos in most scenarios. However, Hermes' updates
have to collect acknowledgments from all replicas
before committing.

This is a disadvantage on write latency when some
replica response is slow (e.g., in WAN). In this
case, a majority-based protocol (e.g., Paxos),
even if it requires more round-trips, it might
be able to provide updates with lower latency
since it need not wait for the slowest replica.

------

Q: How does the Hermes Protocol behave under coordinator failure?

A: A key feature of Hermes is that every
replica can equally serve reads and writes.
The replica that initiates a write,
is called the "coordinator" of
that particular operation (i.e., the write).

The coordinator completes the write
only after receiving the acknowledgments
from all followers (i.e., the rest of
the replicas of the object targeted
by the write).

So there are two cases where the coordinator may fail.
[A] After it invalidates all followers.
[B] Before it invalidates all of the followers.


The first case is easier to grasp
and it goes as follows.

1. The coordinator fails and gets
reliably removed from the membership

2. Since all replicas are invalidated
and because Hermes allows for safe
write replays the replica (any alive)
that receives the first operation for
the blocked object will simply replay
the write to unblock itself and
the rest of the followers. Thus,
if such an operation is a read
then it is guaranteed to see the
latest value.

The second case is less intuitive since there could be 2 sub-cases:

a. No followers got invalidated
b. Some of the followers got invalidated

A key concept to grasp here is that the
write has not been completed yet (i.e., it
is not visible to the external client)

If a. occurs then there is no need to "unblock"
anything since the incomplete write has
not left any trace to the datastore.

If b. occurs read may happen on nodes
which have not seen yet the invalidation
and they may return the last
successfully written value (i.e., the one
before the incomplete write). If a read
is received in one of the invalidated
nodes then this node would replay
the write to unblock (as in [B]).

Once the replay completes successfully
it will be able to return its local
value which from now on is the
most recent one.

------

Q: Can you provide some insight into
the choice of starting KVS to implement
Hermes on top of? Could Memcached
or Redis be used instead?

The choice of the replication protocol is
orthogonal to the choice of a datastore,
and indeed, Hermes can be used with any
datastore. We choose ccKVS since its
minimalist design allows us to focus
on the impact of the replication
protocol itself without regarding
for optimizations or overheads of
any commercial-grade datastore.
No feature in ccKVS favors any of
the protocols, hence facilitating
a fair apples-to-apples comparison.

------

Q: How does Hermes compare with Leader-based approaches and per-key leases?

Fast writes

Hermes writes are leaderless. This
means that writes can be executed
equally fast, without any additional
hops (i.e., 1RTT commit in the
absence of faults), and in a
load-balanced manner from any
replica. Hermes' writes are also
fully inter-key concurrent.

Local reads

Hermes allows for local reads from
all of its replicas through a membership
lease*. Most leader-based approaches
that use leases allow for local reads
solely from the leader (e.g.,
Multi-Paxos with Leader Leases).

Other leader-based replication protocols (e.g.,
An Algorithm for Replicated Objects with Efficient Reads
https://dl.acm.org/doi/pdf/10.1145/2933057.2933111)
apply object leases. This approach becomes
costly to match Hermes' inter-key concurrency
as we explain next.

Cost of Membership Lease vs. Per-key (object) leases
Hermes single membership lease per-shard
(which can have Millions of keys) suffices
to serve local reads for any key in
the shard, without having to sacrifice
inter-key concurrency.

Any other linearizable protocol can be
deployed per-key to match the inter-key
concurrency of Hermes. However, this means
that protocols which apply object leases
for local reads would also have one lease per-key.

The renewal of per-key leases is expensive.
Its cost is at least linear increase in
messages with the number of keys in a
shard — even in the absence of reads
and write operations to those keys.
(see related work section in Hermes paper)

*An almost equally efficient and safe
Hermes' variant w/o leases (or loosely
synchronized clocks) is described
in the discussion section of the paper.

------

Q: How does Hermes use loosely-synchronized clocks?
Are they necessary?

Each replica in Hermes holds a single membership lease
to allow for efficient local reads.

However, there is a Hermes variant that works
even without leases (i.e., asynchronously)
with small overhead over its initial protocol.
This variant of Hermes is described in the
discussion section of the Hermes’ paper.

--------

Q: Parallel writes to different keys

Enjoyed reading your paper! I have a question
regarding writes to different keys.
If these writes are allowed to
happen in parallel, how is global
order of writes achieved?
1) Is global order of writes necessary
for distributed systems?
2) If so, then do you need some
form of synchronization barriers?

A: Great Question!

Global order matters because it gives
intuitive guarantees to the programmer
and allows for easy porting of single-node
applications to the distributed setting.

Hermes's key point is to advocate for
a more concurrent and dynamic way to
achieve this global order and not to
relax the consistency guarantees.
Hermes leverages the fact that
linearizability is compositional* (i.e., a
local property) and thus offering per-key
linearizability** composes to (across-key)
linearizability (i.e., a total order of
all writes). This allows Hermes to
implicitly achieve a global order of
writes rather than imposing an explicit
global order of all writes (e.g., through
a single log or serialization point),
which comes at a high contention/cost.

*Note that this compositionality does
not hold for more relaxed consistency
models. For example, combining the order
of two keys, each with per-key sequential
consistency, would not necessarily
result in an overall sequentially
consistent order of writes across the keys.

** Also note that per-key linearizability
does not relax any of the session orders
either. For instance, an operation
belonging to the same session (e.g.,
a single client) is executed only
after the client's last operation is completed.

All that said, systems that do explicitly
order all of their operations (including
writes to different keys) can provide
(across-key) transactions more easily
than Hermes but that comes at the cost of concurrency.

Let me know if that answers your question.

follow-up:

A more intuitive answer might be the following.

Invalidating all object replicas
before completing an update prohibits
reads from returning the previous value
of the object which provides a more
dynamic approach to achieve a global order of updates.

In short, updates are linearized (and are
globally ordered) at the point when the
last replica is invalidated forming a
global order of updates dynamically.
Because of Invalidations as in cache coherence
no subsequent reads may return a stale
value which respects this global order.
Similarly any subsequent update is
guaranteed to be successfully linearized
later in the global order through the
monotonically increasing timestamps.

Invalidations can be thought as short
of a barrier here since a read that
happens in the middle of the update
(to the same object) must wait for
the write to be globally serialized
(i.e., get all of its acks) before
being able to return the new value.
This similarly applies to RMW updates*.

Notice that concurrent reads or updates
to different keys which is the common
case can proceed in parallel.

*Hermes, can globally serialize all concurrent
(blind) writes initiated by different
nodes which might be a less intuitive
optimization. However, this still respects
the following, a write B invoked after
the completion of an earlier write A is
still guaranteed to be linearized
after the linearization point of A.

*/
