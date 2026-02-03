package tube

// docs for fencing tokens/external consistency
//
// Note: our LeaseEpoch is a fencing token (returned by Write/CAS with lease).
// See for example its use in czar.go reliable membership implementation.
/*
Q:
How are fencing tokens used in a chubby or etcd like strongly consistent paxos cluster to provide external consistency to other services not in direct contact with the chubby/etcd?

A:

(https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html)

In distributed systems like Chubby, Etcd, or Zookeeper, fencing tokens (or epoch numbers) are a critical mechanism used to handle the "split-brain" or "zombie leader" problem when interacting with external resources.

They ensure that when a lock is transferred from one client to another, the old client cannot accidentally corrupt data in an external system (like a storage service or database) that isn't part of the consensus cluster itself.

### The Problem: The Zombie Leader

In a Paxos/Raft cluster, obtaining a lock is strongly consistent. However, the *application* using that lock talks to external systems (like an Object Store) that don't know about the lock service.

1. Client A acquires the lock (Leader).
2. Client A pauses (GC pause, network partition) but thinks it still holds the lock.
3. The Lock Service expires A's session and grants the lock to Client B.
4. Client B writes new data to the Storage Service.
5. Client A wakes up and writes *stale* data to the Storage Service, overwriting B's work.

### The Solution: Fencing Tokens

Fencing tokens solve this by adding a monotonically increasing number (sequencer) to every lock acquisition. This number is passed to the external resource to validate requests.

#### How It Works Step-by-Step

1. Lock Acquisition:
When a client acquires a lock from the consensus service (Chubby/Etcd), the service returns a `Fencing Token`. This is simply a number that increments every time the lock is granted (e.g., ).
2. Request with Token:
When the client attempts to write to an external resource (e.g., a database or object store), it *must* include this token in the write request.
3. Validation (The Fence):
The external resource maintains the "highest token seen so far" for that specific data/resource.
* If Request Token < Max Token Seen: The resource rejects the request. This "fences off" the old leader.
* If Request Token >= Max Token Seen: The resource accepts the write and updates its stored "Max Token."


### Example Scenario

Let's trace the "Zombie Leader" failure with fencing tokens applied:

| Step | Actor | Action | State Change |
| --- | --- | --- | --- |
| 1 | Client A | Acquires lock from Chubby. | Receives `Token = 33`. |
| 2 | Client A | Pauses (GC/Network delay). | Lock expires at Chubby. |
| 3 | Client B | Acquires lock from Chubby. | Receives `Token = 34`. |
| 4 | Client B | Writes to Storage Service with `Token = 34`. | Storage sets `MaxToken = 34`. Write Accepted. |
| 5 | Client A | Wakes up. Writes to Storage with `Token = 33`. | Storage sees . |
| 6 | Storage | REJECTS Client A's request. | Client A realizes it lost leadership. |

### Requirements for External Services

For this mechanism to work, the external service (the one *not* in the Paxos cluster) doesn't need to implement Paxos or complex consensus. It only needs two simple capabilities:

1. Persist a single number: Store the highest token number seen associated with the resource.
2. Conditional Update: Check the incoming token against the stored token before allowing operations (similar to Compare-and-Swap).

### Implementation in Real Systems

* Chubby: Uses `sequencers`. A client passes the sequencer to the file server. The file server validates the sequencer against Chubby to ensure the client is still the lock holder.
* Zookeeper: Uses `zxid` (ZooKeeper Transaction Id) which can be used as a fencing token.
* HDFS (Hadoop): When the NameNode fails over, the new NameNode uses an epoch number to fence off the old NameNode from the JournalNodes/DataNodes.

---
*/
