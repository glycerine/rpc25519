package tube

import (
	"fmt"
	"time"

	//rpc "github.com/glycerine/rpc25519"
	"testing"
)

// partition the network, then
//
// a) make sure minority
// cannot elect a leader or serve reads/writes;
//
// b) make sure majority do elect a leader
// and do serve reads/writes.
//
// c) heal the parition and check:
// the minority do not disrupt the sticky-leader.
// Reads and writes are linearizable across
// the rejoin point.
//
// for flexible paxos style different sizes of quorums?
// do that later/deferred. Start simple.
func Test051_partition_and_rejoin(t *testing.T) {

	// INVAR: Even when partitioned, especially then,
	// each node has got to be continually running
	// timers to wake up and try again! How
	// can we test this without putting assert/panics
	// into the prod code... keep the history of
	// timer resets + notes about role, and where in
	// code, in a history. assert over that history.
	// a history is like a channel but inf growth
	// and can view as well as add to it; but should
	// not be deleted from? or could be like ring
	// buffer. If a chan indicates readiness to
	// provide a value, then well it can change its
	// mind and upon running a func backed channel,
	// it can return a pointer to a ticket with error...
	// how are channels implemented? in go, to model
	// our histories after them... its just a time series!
	// hint cn/dfs!?! the safety/liveness assertions
	// should be signal FSM on top of a set of
	// output time series. the tree os signals
	// can compute up to the top, and output
	// a correct or invariant violation guarantee
	// at each timestep! If at any step we
	// see violation, we have the full history
	// of the time series written to disk, and
	// also we can stop it immediately and reply,
	// right?

	// we can only manipulate the simnet, not
	// the real network sockets.
	onlyBubbled(t, func(t *testing.T) {

		minClusterSz := 3
		maxClusterSz := 4
		for numNodes := minClusterSz; numNodes < maxClusterSz; numNodes++ {

			forceLeader := 0
			c, leader, leadi, maxterm := setupTestCluster(t, numNodes, forceLeader, 51)
			_, _, _ = leader, leadi, maxterm

			vv("confirming test setup ok...")
			for j := range numNodes {
				look := c.Nodes[j].Inspect()
				if j == 0 {
					if look.Role != LEADER {
						panic(fmt.Sprintf("error: expected node 0 to be leader (but is %v), leader is: '%v', term=%v", look.Role, look.CurrentLeaderName, look.State.CurrentTerm))
					}
					vv("good: node %v is leader, at term %v", j, look.State.CurrentTerm)
				} else {
					if look.Role != FOLLOWER {
						panic(fmt.Sprintf("error: expected node %v to be follower (but is %v), leader is: '%v', term=%v", j, look.Role, look.CurrentLeaderName, look.State.CurrentTerm))
					}
					vv("good: node %v is follower, at term %v", j, look.State.CurrentTerm)
				}
				if look.State.CurrentTerm != maxterm {
					panic(fmt.Sprintf("wanted maxterm(%v) to still hold, but now %v, at node %v:\n%v", maxterm, look.State.CurrentTerm, j, look))
				}
			}

			vv("partition %v nodes into majority vs minority, and keep leader on the majority side, so no new election.", numNodes)

			majority := []int{leadi, (leadi + 1) % numNodes}
			minority := []int{(leadi + 2) % numNodes}
			vv("majority = '%#v'", majority)
			vv("minority = '%#v'", minority)
			if minority[0] != 2 {
				panic("assumption that 2 is the minority partition violated")
			}

			if true {
				c.IsolateNode(2)
			} else {
				deafProb := map[int]float64{2: 1.0}
				dropProb := map[int]float64{2: 1.0}
				c.DeafDrop(deafProb, dropProb)
			}
			//vv("simnet is now: %v", c.SimnetSnapshot())
			// 33 sec??

			// tests:
			// TODO: need to do reads/writes and linz before finishing this out.
			// for now, test leader election or not stuff.

			// a) make sure non-quorum side does not
			// elect a leader or serve reads/writes;

			vv("what does the minority do? during this 10 minutes? with 2 isolated and 0 the leader") // nothing would be good as far as term increments.
			time.Sleep(time.Minute * 10)
			vv("done with 10 minutes of sleep")

			// maxterm should not have changed on either side.

			for j := range numNodes {
				look := c.Nodes[j].Inspect()
				if j == 0 {
					if look.Role != LEADER {
						panic(fmt.Sprintf("error: expected node 0 to be leader (but is %v), leader is: '%v', term=%v", look.Role, look.CurrentLeaderName, look.State.CurrentTerm))
					}
				} else {
					if look.Role != FOLLOWER {
						panic(fmt.Sprintf("error: expected node %v to be follower (but is %v), leader is: '%v', term=%v", j, look.Role, look.CurrentLeaderName, look.State.CurrentTerm))
					}
				}
				if look.State.CurrentTerm != maxterm {
					panic(fmt.Sprintf("wanted maxterm(%v) to still hold, but now %v, at node %v:\n%v", maxterm, look.State.CurrentTerm, j, look))
				}
			}

			// b) make sure quorum side does elect a leader
			// and it does serve reads/writes, even if the original
			// leader was on the other side of the partition.

			vv("healed partition! then another 10 seconds of sleep")
			// c) heal the parition and check:
			// the minority do not disrupt the sticky-leader.
			const deliverDroppedSends = true
			c.AllHealthyAndPowerOn(deliverDroppedSends)

			time.Sleep(time.Second * 10)

			for j := range numNodes {
				look := c.Nodes[j].Inspect()
				if j == 0 {
					if look.Role != LEADER {
						panic(fmt.Sprintf("error: expected node 0 to be leader (but is %v), leader is: '%v', term=%v", look.Role, look.CurrentLeaderName, look.State.CurrentTerm))
					}
				} else {
					if look.Role != FOLLOWER {
						panic(fmt.Sprintf("error: expected node %v to be follower (but is %v), leader is: '%v', term=%v", j, look.Role, look.CurrentLeaderName, look.State.CurrentTerm))
					}
				}
				if look.State.CurrentTerm != maxterm {
					panic(fmt.Sprintf("wanted maxterm(%v) to still hold, but now %v, at node %v:\n%v", maxterm, look.State.CurrentTerm, j, look))
				}
			}

			vv("cluster stayed stable! another 10 minutes of sleep")

			time.Sleep(time.Minute * 10)
			vv("done with 2nd 10 minutes of sleep")

			for j := range numNodes {
				look := c.Nodes[j].Inspect()
				if j == 0 {
					if look.Role != LEADER {
						panic(fmt.Sprintf("error: expected node 0 to be leader (but is %v), leader is: '%v', term=%v", look.Role, look.CurrentLeaderName, look.State.CurrentTerm))
					}
				} else {
					if look.Role != FOLLOWER {
						panic(fmt.Sprintf("error: expected node %v to be follower (but is %v), leader is: '%v', term=%v", j, look.Role, look.CurrentLeaderName, look.State.CurrentTerm))
					}
				}
				if look.State.CurrentTerm != maxterm {
					panic(fmt.Sprintf("wanted maxterm(%v) to still hold, but now %v, at node %v:\n%v", maxterm, look.State.CurrentTerm, j, look))
				}
			}

			// d) Reads and writes should remain linearizable across
			// the rejoin point.

			// cleanup
			c.Close()
			//if numNodes < maxClusterSz-1 {
			//	time.Sleep(time.Second)
			//}

		} // for numNodes loop
	})
}

func (c *TubeCluster) waitForLeader(t0 time.Time) (leader string, leadi int, maxterm int64) {

	// verify terms are strictly monotonically increasing, per node
	node2term := make(map[string]*testTermChange)

	cfg := c.Cfg
	numNodes := cfg.ClusterSize
	choose2 := numNodes * (numNodes - 1) / 2
	if choose2 == 0 {
		choose2 = 1 // handle single node case
	}
	allowed := time.Duration(choose2) * cfg.MinElectionDur * 10 // time allowed to elect a leader
	timeout := time.After(allowed)

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

		case leader = <-c.LeaderElectedCh:
			w, ok := c.Name2num[leader]
			if !ok {
				panic(fmt.Sprintf("no node number for leader '%v'", leader))
			}
			leadi = w
			//vv("cluster c.LeaderElectedCh fired. leader=%v; node number=%v'", leader, w)

			elap := time.Since(t0)
			_ = elap
			//alwaysPrintf("good: clusterSize = %v; node w=%v (%v) won election in %v", numNodes, w, leader, elap.Truncate(time.Millisecond))

			// give them time to depose other candidates with
			// their first round of heartbeats.
			time.Sleep(cfg.HeartbeatDur * 3)

			term := int64(-1)
			for i := range c.Nodes {
				look := c.Nodes[i].Inspect()
				roleExpect := FOLLOWER
				if i == w {
					roleExpect = LEADER
				}
				if look.Role != roleExpect {
					panic(fmt.Sprintf("error: expected node %v to be %v (but is %v) in term %v", i, roleExpect, look.Role, look.State.CurrentTerm)) // CANDIDATE seen... size 8 is rough! 015_tube_non_parallel_linz (tube_test.go) red under realtime without synctest (might be sporadic): error: expected node 0 to be LEADER (but is FOLLOWER) in term 2
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
	//c.Close()
	//time.Sleep(3 * cfg.MinElectionDur)
	//time.Sleep(time.Second)
	return
}

// get first no-op committed by leader
func (c *TubeCluster) waitForLeaderNoop(t0 time.Time) {

	cfg := c.Cfg
	allowedNoop := cfg.MinElectionDur * 5 // time allowed to get no-op committed
	select {
	case noop0leader := <-c.LeaderNoop0committedCh:
		_ = noop0leader
		elap := time.Since(t0)
		_ = elap
		//vv("good: leader first noop0 was committed after %v by %v", elap, noop0leader)
		//NO! racey! vv("noop0 ticket = %v", noop0tkt)
	case <-time.After(allowedNoop):
		elap := time.Since(t0)
		vv("bad: NO leader no-op was committed after %v", elap)
		panic("leader did not commit first noop0")
	}
}

// IF the minority had the leader, then verify
// that the paritioned off minority leader steps
// down without a pong qorum, and does not serve
// any stale reads, and cannot renew its read lease.
// Verify that the rest of the cluster
// waits for the leader's read lease to expire before
// electing a new leader and serving reads/writes.
func Test052_partition_leader_away_and_rejoin(t *testing.T) {
	//return // red while we work on bootstrapping...

	// we can only manipulate the simnet, not
	// the real network sockets.
	onlyBubbled(t, func(t *testing.T) {

		minClusterSz := 3
		maxClusterSz := 4
		for numNodes := minClusterSz; numNodes < maxClusterSz; numNodes++ {

			forceLeader := 0
			c, leader, leadi, maxterm0 := setupTestCluster(t, numNodes, forceLeader, 52)
			_, _, _ = leader, leadi, maxterm0

			vv("confirming test setup ok... leadi = %v; leader='%v'; maxterm0=%v", leadi, leader, maxterm0)
			for j := range numNodes {
				look := c.Nodes[j].Inspect()
				if j == 0 {
					if look.Role != LEADER {
						panic(fmt.Sprintf("error: expected node 0 to be leader (but is %v), leader is: '%v', term=%v", look.Role, look.CurrentLeaderName, look.State.CurrentTerm))
					}
					vv("good: node %v is leader, at term %v", j, look.State.CurrentTerm)
				} else {
					if look.Role != FOLLOWER {
						panic(fmt.Sprintf("error: expected node %v to be follower (but is %v), leader is: '%v', term=%v", j, look.Role, look.CurrentLeaderName, look.State.CurrentTerm))
					}
					vv("good: node %v is follower, at term %v", j, look.State.CurrentTerm)
				}
				if look.State.CurrentTerm != maxterm0 {
					panic(fmt.Sprintf("wanted maxterm0(%v) to still hold, but now %v, at node %v:\n%v", maxterm0, look.State.CurrentTerm, j, look))
				}
			}

			vv("partition %v nodes into majority vs minority, with leader on the minority side, so should be new election on majority side; and leader should step down when partitioned...", numNodes)

			minority := []int{leadi}
			vv("minority = '%#v'", minority)

			majority := []int{(leadi + 1) % numNodes, (leadi + 2) % numNodes}
			vv("majority = '%#v'", majority)

			if minority[0] != 0 {
				panic("assumption that 0 is the minority partition violated")
			}

			// problem: now that we try to make NEW connections
			// after detecting node failures, those newly made
			// connections do not have the deaf/drop applied!
			// so ISOLATE host instead! Update: fixed now.
			// See also fuzz_test.go:272.
			if false { // true { // 4.1 sec vs 4.7-5.1 sec,
				// so it is a bit faster to isolate,
				// but then that does not test that our DropDeaf is working.
				c.IsolateNode(0)
			} else {
				deafProb := map[int]float64{0: 1.0}
				dropProb := map[int]float64{0: 1.0}
				c.DeafDrop(deafProb, dropProb)
			}
			//vv("after IsolateNode(0), simnet is now: %v", c.SimnetSnapshot())

			// tests:
			// TODO: need to do reads/writes and linz before finishing this out.
			// for now, test leader election or not stuff.

			// a) make sure non-quorum side does not
			// elect a leader or serve reads/writes;

			vv("what does the minority do? during this 10 minutes? with {1,2} isolated from {0} and 0 was the leader") // nothing would be good as far as term increments.
			time.Sleep(time.Minute * 10)
			vv("done with 10 minutes of sleep")

			// arg. we see
			//simnet.go:1133 2000-01-01 00:00:05.235000000 +0000 UTC registering new client 'auto-cli-from-srv_node_1-to-srv_node_0___YoPR6t-ckJ91C7mcXJO9'
			// simnet.go:1151 2000-01-01 00:00:05.901000000 +0000 UTC cli is auto-cli of basesrv='srv_node_1'
			//simnet.go:1133 2000-01-01 00:00:05.722000000 +0000 UTC registering new client 'auto-cli-from-srv_node_2-to-srv_node_0___fMKt1BfLmaBbUmG3cLz_'
			//simnet.go:1151 2000-01-01 00:00:06.221000000 +0000 UTC cli is auto-cli of basesrv='srv_node_2'
			// but no applying allNewCircuitsInjectFault to new auto-cli
			// because the fault is on the other end of the connection.

			//vv("after 10 minutes of sleep, simnet is now: %v", c.SimnetSnapshot(true))

			// leader should have stepped down <- no longer true??

			// new term should be > maxterm0

			leaderSeenMajority := false
			followerSeenMajority := false
			newLeaderMajority := -1
			newTerm := int64(-1)
			for j := range numNodes {
				look := c.Nodes[j].Inspect()
				if j == 0 {
					if look.Role != FOLLOWER {
						panic(fmt.Sprintf("error: expected node 0 to be follower (but is %v), leader is: '%v', term=%v", look.Role, look.CurrentLeaderName, look.State.CurrentTerm))
					} else {
						vv("good: node_0 became follower; term = %v ; maxterm0 = %v", look.State.CurrentTerm, maxterm0)
					}
					if look.LastLeaderActiveStepDown.IsZero() {
						panic(fmt.Sprintf("error: expected minority node 0 to have actively stepped down from leadership, but LastLeaderActiveStepDown is zero time"))
					}
				} else {
					switch look.Role {
					case LEADER:
						leaderSeenMajority = true
						newLeaderMajority = j
						newTerm = look.State.CurrentTerm
					case FOLLOWER:
						followerSeenMajority = true
					case CANDIDATE:
						panic(fmt.Sprintf("error: expected node %v be leader or follower, not candidate (is %v) leader is: '%v', term=%v", j, look.Role, look.CurrentLeaderName, look.State.CurrentTerm))
					}
				}
				switch j {
				case 0:
					if look.State.CurrentTerm != maxterm0 {
						panic(fmt.Sprintf("wanted 1st maxterm0(%v) to still hold on paritioned former leader, but now %v, at node %v:\n%v", maxterm0, look.State.CurrentTerm, j, look))
					}
				default:
					if look.State.CurrentTerm <= maxterm0 {
						panic(fmt.Sprintf("wanted 1st maxterm0(%v) to no longer hold, but now %v, at node %v:\n%v", maxterm0, look.State.CurrentTerm, j, look))
					}
				}
			}

			// b) make sure quorum side elected a leader
			if !leaderSeenMajority {
				panic("majority did not elect leader after the original was partitioned away")
			}
			if !followerSeenMajority {
				panic("majority did not have one follower")
			}
			if newTerm <= maxterm0 {
				panic(fmt.Sprintf("newTerm(%v) <= maxterm0(%v)", newTerm, maxterm0))
			}

			vv("about to heal partition! then another 10 seconds of sleep")
			// c) heal the parition and check:
			// the minority do not disrupt the (already established) sticky-leader.
			//const deliverDroppedSends = true // works
			const deliverDroppedSends = false // works
			c.AllHealthyAndPowerOn(deliverDroppedSends)

			vv("c.AllHealthyAndPowerOn(deliverDroppedSends=%v), simnet is now: %v", deliverDroppedSends, c.SimnetSnapshot(true).HealthSummary())

			time.Sleep(time.Second * 10)

			for j := range numNodes {
				look := c.Nodes[j].Inspect()
				switch j {
				case newLeaderMajority:
					if look.Role != LEADER {
						panic(fmt.Sprintf("error: expected node %v to CONTINUE to be leader after parition healed (but is %v), leader is: '%v', term=%v", newLeaderMajority, look.Role, look.CurrentLeaderName, look.State.CurrentTerm))
					}
				default:
					if look.Role != FOLLOWER {
						panic(fmt.Sprintf("error: expected node %v to be follower (but is %v), leader is: '%v', term=%v", j, look.Role, look.CurrentLeaderName, look.State.CurrentTerm))
					}
				}
				if look.State.CurrentTerm != newTerm {
					panic(fmt.Sprintf("wanted newTerm(%v) to still hold, but now %v, at node %v:\n%v", newTerm, look.State.CurrentTerm, j, look))
				}
			}

			vv("cluster stayed stable! another 10 minutes of sleep")

			time.Sleep(time.Minute * 10)
			vv("done with 2nd 10 minutes of sleep")

			for j := range numNodes {
				look := c.Nodes[j].Inspect()
				switch j {
				case newLeaderMajority:
					if look.Role != LEADER {
						panic(fmt.Sprintf("error: expected new leader(%v) to still be leader (but is %v), leader is: '%v', term=%v", newLeaderMajority, look.Role, look.CurrentLeaderName, look.State.CurrentTerm))
					}
				default:
					if look.Role != FOLLOWER {
						panic(fmt.Sprintf("error: expected node %v to be follower (but is %v), leader is: '%v', term=%v", j, look.Role, look.CurrentLeaderName, look.State.CurrentTerm))
					}
				}
				if look.State.CurrentTerm != newTerm {
					panic(fmt.Sprintf("wanted newTerm(%v) to still hold, but now %v, at node %v:\n%v", newTerm, look.State.CurrentTerm, j, look))
				}
			}

			// d) Reads and writes should remain linearizable across
			// the rejoin point.

			// cleanup
			c.Close()
			//if numNodes < maxClusterSz-1 {
			//	time.Sleep(time.Second)
			//}

		} // for numNodes loop
	})
}
