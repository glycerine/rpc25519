package tube

import (
	"fmt"
	"time"

	//rpc "github.com/glycerine/rpc25519"
	"testing"
)

/* outline of safety tests for Tube's Raft with pre-vote
   and leader stickiness always in place.

**I. Safety Tests (Ensuring "nothing bad happens")**

1.  **Term Consistency and Progression:**
    *   **Split Vote Resolution:**
        *   Create a scenario (e.g., 3 nodes where one votes for itself, and the other two also vote for themselves, or a 4-node with a 2-2 split if votes are cast simultaneously). Verify that no leader is elected in that term, a new term starts, and eventually, a leader is elected in a subsequent term.
        *   *Key check:* Terms must strictly increase.
    *   **Stale Candidate Rejection (Log Up-to-Date Check):**
        *   Have a follower with a more up-to-date log (higher term in last log entry, or same term but longer log).
        *   Ensure a candidate with a less up-to-date log cannot win the election if the more up-to-date follower is reachable and participates.
        *   *Key check:* `handleRequestVote` correctly applies the "log is at least as up-to-date" rule.
    *   **Old Leader Rejoining Cluster:**
        *   Elect a leader (L1).
        *   Partition L1 away.
        *   Elect a new leader (L2) in the majority partition in a higher term.
        *   Heal the partition. L1 attempts to assert leadership.
        *   *Key check:* L1 should detect L2's higher term (or messages from L2) and step down to become a follower, adopting L2's term. L1 should not disrupt L2.
    *   **Candidate Steps Down on Higher Term Discovery:**
        *   Node A starts an election for term `T`.
        *   Before it wins, it receives a `RequestVote` or `AppendEntries` from Node B for term `T+1`.
        *   *Key check:* Node A should abandon its candidacy, become a follower, and update its term to `T+1`.
    *   **Candidate Steps Down on Legitimate Leader Discovery (Same Term):**
        *   Node A starts an election for term `T`.
        *   Before it wins, it receives an `AppendEntries` (heartbeat) from an already established leader L for the same term `T`.
        *   *Key check:* Node A should abandon its candidacy and become a follower.

2.  **Pre-Vote Specific Safety:**
    *   **Partitioned Node Rejoining (Pre-Vote Active):**
        *   Partition a node A away. Let it attempt pre-votes (which should fail due to lack of quorum).
        *   Meanwhile, the main cluster continues, possibly electing a new leader or advancing terms.
        *   Heal the partition. Node A tries to initiate a pre-vote.
        *   *Key check:* Node A's pre-vote requests should either be for a stale (lower or same) term compared to the main cluster's current term (and thus potentially rejected by up-to-date nodes), or if its `currentTerm` is low, its `preVoteTerm` (`currentTerm+1`) should be correctly evaluated by others. Crucially, it should not be able to start a *real* election that disrupts a stable leader if its logs are behind or it can't get pre-vote quorum from nodes that see the current leader.
    *   **No Term Increment on Pre-Vote Failure:**
        *   Induce a pre-vote scenario where the node cannot get a pre-vote quorum.
        *   *Key check:* The node should *not* increment its `CurrentTerm`. It should reset its election timer and remain a follower.

3.  **Sticky Leader Specific Safety (if implemented and distinct from general pre-vote):**
    *   **Follower Rejects Votes/Pre-Votes with Recent Leader Contact:**
        *   Establish a stable leader L.
        *   Ensure followers are receiving heartbeats from L.
        *   Have another node C become a candidate and request votes/pre-votes from these followers.
        *   *Key check:* Followers that have recently heard from L should deny votes/pre-votes to C, assuming C is not significantly ahead in log/term or part of a leadership transfer.

**II. Liveness Tests (Ensuring "something good eventually happens")**

1.  **Leader Re-election After Failure/Partition:**
    *   **Clean Leader Shutdown:** Elect a leader, then gracefully stop it.
        *   *Key check:* A new leader is elected from the remaining nodes in a timely manner.
    *   **Leader Crash/Network Disconnection:** Elect a leader, then simulate a crash or disconnect its network from the majority.
        *   *Key check:* A new leader is elected.
    *   **Multiple Successive Leader Failures:**
        *   In a 5-node cluster, elect L1. Kill L1. L2 is elected. Kill L2.
        *   *Key check:* L3 should be elected.
    *   **Follower Failure and Recovery:**
        *   Elect a leader. Stop a follower. The cluster should continue. Restart the follower.
        *   *Key check:* The follower should rejoin and catch up. No new election should be triggered if the leader remains stable.

2.  **Network Partition Scenarios & Healing:**
    *   **Minority Partition Reintegration:**
        *   Partition a minority of nodes. The majority elects/maintains a leader.
        *   Heal the partition.
        *   *Key check:* Minority nodes should rejoin, become followers of the majority's leader, and catch up without triggering unnecessary elections.
    *   **Temporary Full Network Split (then heal):**
        *   Split a 3-node cluster into (A), (B), (C) or (A,B), (C). No progress or new stable leader.
        *   Heal the network.
        *   *Key check:* An election should occur, and one leader should emerge.

3.  **Election Under Stress:**
    *   **High Message Latency/Loss (but not total partition):** Simulate an unreliable network where some vote requests/responses or heartbeats are delayed or lost.
        *   *Key check:* Elections should still eventually succeed. Randomized timeouts are key here. This tests the resilience provided by retries and timeout mechanisms.
    *   **Concurrent Candidates (Beyond `Test022`):**
        *   In a 5 or 7 node cluster, try to trigger 3+ nodes to become candidates simultaneously (if possible with your test harness by manipulating election timers or injecting `MsgHup` equivalents).
        *   *Key check:* The election process should resolve to a single leader.

*/

// This test focuses on split-vote handling.
// We construct clusters of increasing size
// for each test, and then force conditions
// that favor a split pre-vote.
//
// Each node in the cluster, at the same instant,
// is told to start the election process and thus
// broadcast simultaneous pre-vote requests.
//
// In Tube, pre-vote itself is a two-phase afair, and
// the pre-vote phases always preceed an election.
//
// Tube's pre-vote is constructed as an independent
// layer on top of core Raft, and is stateless
// on the responder's side. It answers the
// question honestly: if I held an election
// for this term with my logs, would you
// vote for me? The replies incorporate
// the sticky-leader information, but ignore
// any other concurrent pre-vote activity,
// such as will be present in the 050 test.
// It is "as if", any given pre-vote request is
// the only one seen. Are the logs up to
// date enough to vote for the requestor?
// Do we have a stable sticky-leader that
// would cause us to decline?
//
// This test lead us to this design, as its
// initial runs suffered from minutes of
// livelock, and frequent non-termination.
//
// The node who holds a successful pre-vote
// enters phase two of the pre-vote.
//
// It pretends that its Raft election timer
// has just been reset, and waits that
// random amount of time before
// advancing its term and becoming a
// candidate. During this time it can
// be deposed by new leader, thus preventing
// many livelocks.
//
// As is, this second randomized wait phase
// the occurs after the initial pre-vote
// on election timeout was started, would double
// the expected time to elect a leader.
// We simply cut our election duration
// in half compared to reference, to
// restore expectations.
//
// You can think of this as simply holding
// the pre-vote half-way into the normal
// Raft leader election timeout, on average,
// rather than at the end of the leader
// timeout as is usually done.
//
// We also found it essential to spread out
// the default election timeout range for
// larger clusters. [T, 2*T] is the standard
// Raft election timeout, for a minimum
// election timeout duration T. This is indeed
// what we use for a three node or smaller
// cluster. For larger clusters, we draw from
// [T, K*T], where K = cluster size - 2.
// For instance [T, 3*T] for 4 node clusters,
// and [T, 4*T] for clusters with 5 nodes.
//
// # Tested behavior
//
// We verify that a leader is elected, and
// confirm that term changes are strictly monotonically
// increasing on each node as they transition
// through terms.
//
// sample timings, realtime run.
// clusterSize = 2; node w=1 (node_1) won election in 2.36s
// clusterSize = 3; node w=0 (node_0) won election in 1.61s
// clusterSize = 4; node w=1 (node_1) won election in 1.90s
// clusterSize = 5; node w=0 (node_0) won election in 1.87s
// clusterSize = 6; node w=4 (node_4) won election in 2.67s
// clusterSize = 7; node w=0 (node_0) won election in 2.41s
// clusterSize = 8; node w=2 (node_2) won election in 2.74s
// clusterSize = 9; node w=7 (node_7) won election in 1.86s
func Test050_split_vote_resolution(t *testing.T) {

	// fun variation might be a distributed decentralized
	// sort, kind of like the bully algorithm.
	// https://gemini.google.com/app/d1d3782cf4fdc096

	bubbleOrNot(t, func(t *testing.T) {

		// latest time to elect in all these, after
		// adding s.clusterSize() based spreading to
		// s.electionTimeoutDur() timeout draws.
		//
		// realtime 2-9, test time 45s
		// faketime 2-9, test time 5s
		// good test, but too slow for regular realtime testing:
		// for numNodes := 2; numNodes < 10; numNodes++ {
		for numNodes := 2; numNodes < 6; numNodes++ {
			cfg := NewTubeConfigTest(numNodes, t.Name(), faketime)

			//cfg.MinElectionDur =
			c := NewCluster(t.Name(), cfg)

			vv("split vote test: asking all %v nodes to become candidate at the ~ same time", numNodes)
			// don't race with the initial election timeout,
			// we are forcing beginPreVote immediately and
			// that will set timeouts.
			c.NoInitialLeaderTimeout = true

			giveAllNodesInitialMemberConfig(c, false)
			c.Start() // needs to run at least part of main loop to build grid.

			g0 := time.Now()
			_ = g0
			c.waitForConnectedGrid() // this is maybe the slowest part
			t0 := time.Now()
			//vv("grid established in %v, %v nodes, at %v", time.Since(g0), numNodes, t0)

			// perfectly split vote, all at once.
			for i := range numNodes {
				panicAtCap(c.Nodes[i].testBeginPreVote) <- true
			}

			// verify terms are strictly monotonically increasing, per node
			node2term := make(map[string]*testTermChange)

			choose2 := numNodes * (numNodes - 1) / 2
			allowed := time.Duration(choose2) * cfg.MinElectionDur * 10 // time allowed to elect a leader
			timeout := time.After(allowed)
			var maxterm int64
		elected:
			for {
				select {
				case u := <-c.termChanges:
					//vv("%v cluster sees member term change: '%#v'", numNodes, u)
					maxterm = max(maxterm, u.newterm)

					// self consistent
					if u.oldterm >= u.newterm {
						panic(fmt.Sprintf("safety violation, term did not increase on node '%v': old='%v'; new='%v'", u.peerID, u.oldterm, u.newterm))
					}
					// and change to change consistent
					old, ok := node2term[u.peerID]
					if ok {
						if old.newterm >= u.newterm {
							panic(fmt.Sprintf("safety violation, term did not increase on node '%v': old='%v'; new='%v'", u.peerID, old.newterm, u.newterm))
						}
					} else {
						// first one for this peer
						node2term[u.peerID] = u
					}

				case leader := <-c.LeaderElectedCh:
					// give them time to depose other candidates with
					// their first round of heartbeats.
					time.Sleep(cfg.HeartbeatDur * 3)

					w, ok := c.Name2num[leader]
					if !ok {
						panic(fmt.Sprintf("no node number for leader '%v'", leader))
					}
					//vv("cluster c.LeaderElectedCh fired. leader=%v; node number=%v'", leader, w)
					elap := time.Since(t0)
					alwaysPrintf("good: clusterSize = %v; node w=%v (%v) won election in %v", numNodes, w, leader, elap.Truncate(time.Millisecond))
					term := int64(-1)
					for i := range c.Nodes {
						look := c.Nodes[i].Inspect()
						roleExpect := FOLLOWER
						if i == w {
							roleExpect = LEADER
						}
						if look.Role != roleExpect {
							panic(fmt.Sprintf("error: expected node %v to be %v (but is %v) in term %v", i, roleExpect, look.Role, look.State.CurrentTerm)) // CANDIDATE seen... size 8 is rough!
						}
						if i == 0 {
							term = look.State.CurrentTerm
						} else {
							if look.State.CurrentTerm != term {
								panic(fmt.Sprintf("error: inconsistent terms. expected node %v to also be at term %v, but is at %v", i, term, look.State.CurrentTerm))
							}
						}
					}
					break elected
				case <-timeout:
					elap := time.Since(t0)
					panic(fmt.Sprintf("bad: no leader elected, in %v node cluster, after %v", numNodes, elap)) // bad: no leader elected, in 8 node cluster, after 1m20.003295173s
				}
			}
			vv("maxterm = %v when numnodes = %v", maxterm, numNodes)
			c.Close()
			//time.Sleep(3 * cfg.MinElectionDur)
			time.Sleep(time.Second)

		} // for numNodes loop
	})
}
