package tube

import (
	"fmt"
	//"strings"
	//"sort"
	"context"
	"time"

	//rpc "github.com/glycerine/rpc25519"
	"testing"
)

var _ = fmt.Sprintf

// after compaction in a three node cluster (node_0,1,2),
// then we add node_4 and expect it to get wal
// entries replicated to it. keep0 was not doing this.
// based on 055 reboot_test as starting point.
func Test059_new_node_joins_after_compaction(t *testing.T) {
	//return
	onlyBubbled(t, func(t *testing.T) {

		ctx := context.Background()
		minClusterSz := 3
		maxClusterSz := 3
		for numNodes := minClusterSz; numNodes <= maxClusterSz; numNodes++ {

			forceLeader := 0
			c, leader, leadi, maxterm := setupTestCluster(t, numNodes, forceLeader, 55)
			_, _, _ = leader, leadi, maxterm

			time.Sleep(time.Second * 5)

			for j := range numNodes {
				look := c.Nodes[j].Inspect()
				vv("after startup: node %v has CktReplicaByName: '%v'", j, look.CktReplicaByName)
			}

			_, err := c.Nodes[1].Write(ctx, "", "key", Val("value"), 0, nil, "", 0, leaseAutoDelFalse)
			panicOn(err)

			vv("good: done with the Write")

			// allow time for replication
			time.Sleep(time.Second * 5)

			// verify they have all caught up (and
			// thus also compacted logs).
			var lli, llt int64
			var leaderInsp *Inspection
			for j := range numNodes {
				insp := c.Nodes[j].Inspect()
				if j == leadi {
					leaderInsp = insp
				}
				if j == 0 {
					lli = insp.LastLogIndex
					llt = insp.LastLogTerm
				} else {
					if insp.LastLogIndex != lli ||
						insp.LastLogTerm != llt {
						panic(fmt.Sprintf("node %v does not have up to date wal: insp.LastLogIndex(%v) != lli(%v) || insp.LastLogTerm(%v) != llt(%v)", j, insp.LastLogIndex, lli, insp.LastLogTerm, llt))
					}
				}
			}

			vv("add node_4 to cluster. c.Cfg='%v'", c.Cfg)
			node4 := NewTubeNode("node_4", c.Cfg)

			// attempt to make connections not need to
			// stall for leader. not sure this will work.
			// drat. URL == "boot.blank" prevents this from working!
			//node4.state.MC = c.BootMC
			//mc := node4.NewMemberConfig("compact_test")
			//for name, url := range leaderInsp {
			//	mc.PeerNames.set(name, &PeerDetail{Name: name, URL: url})
			//}
			node4.state.MC = leaderInsp.MC
			// Arg. alone it did not; we need to wait more.
			// Try: <-node4.verifyPeerReplicaOrNot below

			c.Halt.AddChild(node4.Halt)
			err = node4.InitAndStart()
			panicOn(err)
			c.Nodes = append(c.Nodes, node4)

			// wait until we have a connection to leader.
			// When stashForLeader was true, we did not
			// need this since we would stash our call
			// in s.ticketsAwaitingLeader and wait while
			// for round-trip to the leader and back
			// eventually completed and then automatically
			// we would resume the ticket and complete
			// the AddPeerIDToCluster call. But that made
			// for a crappy command line experience
			// of hangs when no leader could/will ever be
			// found. So we prefer NOT to stashForLeader
			// and instead to eager return errors to the
			// to that effect to the command line. But that means
			// we here have to pre-establish a circuit to
			// the leader before we attempt to AddPeerIDToCluster.
			for {
				cktP := <-node4.verifyPeerReplicaOrNot
				vv("got cktP = %v", cktP)
				// could be not leader... keep waiting if not.
				if cktP.PeerName == c.Nodes[leadi].name {
					break
				}
			}
			// same call that tube/tubecli.go does to join cluster.
			baseServerHostPort := node4.BaseServerHostPort()
			errWriteDur := time.Second * 10
			actualLeaderURL := c.Nodes[0].URL

			const forceChange = false
			const nonVoting = false
			ctx5sec, canc5 := context.WithTimeout(ctx, 5*time.Second)
			memlistAfterAdd, stateSnapshot, err := node4.AddPeerIDToCluster(ctx5sec, forceChange, nonVoting, node4.name, node4.PeerID, node4.PeerServiceName, baseServerHostPort, actualLeaderURL, errWriteDur)
			canc5()
			_ = memlistAfterAdd
			_ = stateSnapshot

			// actually the problem is that we were
			// not "catching-up" the new node. since
			// it did not have sufficient state, it could
			// not accept the incremental new log
			// entries it was being sent. We need state transfer!

			// allow time for replication
			time.Sleep(time.Second * 5)

			// does node0 (leader) see new node4?
			inspLead := c.Nodes[leadi].Inspect()
			if _, ok := inspLead.CktReplicaByName[node4.name]; !ok {
				panic(fmt.Sprintf("contacted (%v) -> leader (%v) does not have node4 in its CktReplicaByName: %v", c.Nodes[leadi].name, inspLead.CurrentLeaderName, inspLead.CktReplicaByName))
			}
			vv("good: leader sees new node4 as a replica. it should be replicating logs to it...")

			// verify node4 and others have all caught up.
			// refer to same lli, llt we set above.

			insp := node4.Inspect()
			vv("after startup: node4 has CktReplicaByName: '%v'", insp.CktReplicaByName)
			if insp.LastLogIndex != lli ||
				insp.LastLogTerm != llt {
				panic(fmt.Sprintf("node4 does not have up to date wal: insp.LastLogIndex(%v) != lli(%v) || insp.LastLogTerm(%v) != llt(%v)", insp.LastLogIndex, lli, insp.LastLogTerm, llt))
			}

			for j := range len(c.Nodes) {
				insp := c.Nodes[j].Inspect()
				if insp.LastLogIndex != lli ||
					insp.LastLogTerm != llt {
					panic(fmt.Sprintf("node '%v' does not have up to date wal: insp.LastLogIndex(%v) != lli(%v) || insp.LastLogTerm(%v) != llt(%v)", c.Nodes[j].name, insp.LastLogIndex, lli, insp.LastLogTerm, llt))
				}
			}

			if false {
				//snap0 := c.SimnetSnapshot()
				//vv("%v numNode cluster after 5 seconds, snap = '%v'", numNodes, snap0.LongString())

				//select {}
				//time.Sleep(time.Minute)
				vv("power off leader, then sleep 10 sec")

				c.Nodes[leadi].SimCrash()

				time.Sleep(time.Second * 10)

				//snap := c.SimnetSnapshot()
				//vv("5 seconds after crashing the leader") // , snap = '%v'", snap.LongString())

				vv("done with 10 sec of sleep, power ON node_0, then sleep 20 sec")

				c.SimBoot(leadi)
				time.Sleep(time.Second * 20)

				vv("done with 20 sec of sleep after node_0 back on.")

				for j := range numNodes {
					look := c.Nodes[j].Inspect()
					if j == 0 {
						if look.Role == LEADER {
							panic("error: expected node_0 to NOT be leader")
						}
					}
				}

				//time.Sleep(time.Second * 10)

				//snap := c.SimnetSnapshot()
				//vv("snap = '%v'", snap.LongString())

				haveLeader := false
				for j := numNodes - 1; j >= 0; j-- {
					look := c.Nodes[j].Inspect()
					if look.Role == LEADER {
						haveLeader = true
					}
					n := len(look.CktReplicaByName)
					vv("node %v has CktReplicaByName: '%v'", j, look.CktReplicaByName)
					// node 2 has 3 replica to: '[node_0 node_1 node_2]'
					// node 1 has 3 replica to: '[node_0 node_1 node_2]'
					// node 0 has 1 replica to: '[node_0]'
					vv("node %v has %v ckt to: '%v'", j, n, keys(look.CktReplicaByName))
					if n != numNodes {
						// good: red test here initially
						// in that leader coming back with
						// a different PeerID may not
						// be contacted correctly by the rest
						// of the cluster, since they have
						// an old, stale PeerID they are
						// attempting to talk on. Thus we need
						// to request contact via the extact
						// PeerServiceName at the given
						// address, rather than the exact
						// PeerID.
						vv("bad! will panic! node j=%v had %v CktReplicaByName, not numNodes=%v", j, n, numNodes)

						snap := c.SimnetSnapshot(false)
						vv("pre-panic snap = '%v'", snap)
						panic("fix the above lack of CktReplicaByName entries!")
					}
				}
				if !haveLeader {
					panic("should have elected leader")
				}
			} // end if false

			vv("all good 059: finish up test by closing down cluster")
			// cleanup
			c.Close()
		} // for numNodes loop
	})
}

// like local with NoLogCompaction=true, we need to test
// handleAE when we get a snapshot still! so we
//
// a) do not apply the noop0 again since it is in the compacted state already;
//
// b) not ask for from the leader again, since
// we already have it as a part of the compacted state.
func Test061_handleAppendEntries_is_snapshot_aware(t *testing.T) {
	//return
	onlyBubbled(t, func(t *testing.T) {

		//ctx := context.Background()
		minClusterSz := 3
		maxClusterSz := 3
		for numNodes := minClusterSz; numNodes <= maxClusterSz; numNodes++ {

			forceLeader := 0
			cfg := NewTubeConfigTest(numNodes, t.Name(), faketime)
			cfg.NoLogCompaction = true // does get applied to memwal now.

			c, leader, leadi, maxterm := setupTestClusterWithCustomConfig(cfg, t, numNodes, forceLeader, 55)
			_, _, _ = leader, leadi, maxterm

			time.Sleep(time.Second * 5)

			if leadi != 0 {
				panic("expected node 0 to be leader -- why did forceLeader=0 not work?")
			}

			// do like tubecli and send in leader's snapshot
			insp0 := c.Nodes[0].Inspect()

			stateSnapshot := insp0.State
			c.Nodes[1].ApplyNewStateSnapshotCh <- stateSnapshot
			vv("sent c.Nodes[1].ApplyNewStateSnapshotCh <- stateSnapshot")

			time.Sleep(time.Second * 5)

			vv("all good 060: finish up test by closing down cluster")
			// cleanup
			c.Close()
		} // for numNodes loop
	})
}
