package tube

import (
	//"context"
	"fmt"
	"testing"
	"time"
	//rpc "github.com/glycerine/rpc25519"
)

/* TODO (membership change not implemented yet).

**III. Cluster Membership Changes (More advanced, but impacts election):**

*   While not strictly *just* leader election, if you plan to support dynamic membership changes, you'll need tests for how elections behave during and after:
    *   Adding a new node to the cluster.
    *   Removing a node from the cluster.
    *   Replacing a node.
    *   *Key checks here involve leader stability during the transition and correct quorum calculations.*

**Test Harness and Methodology Tips:**

*   **Deterministic Time (if not already):** Using a simulated clock (`faketime` seems to indicate you might be) is invaluable for reproducible tests of timeout-dependent logic.
*   **Control over Network:** A testing framework that allows you to selectively drop messages, introduce latency, and create/heal partitions between specific nodes is extremely powerful.
*   **Node Control:** Ability to stop, start, and restart nodes at specific points in the test.
*   **State Inspection:** Your `Inspect()` method is good. Ensure you can easily check `role`, `currentTerm`, `votedFor`, `leaderId`, and relevant log indices at any point.
*   **Event Injection:** Your `testBeginElection` channel is a good example. Consider similar channels or hooks to trigger specific behaviors or check states at precise moments.
*   **Clear Pass/Fail Criteria:** Define what constitutes success for each test (e.g., "leader X elected within Y time", "node Z has term T and role Follower").
*   **Vary Cluster Sizes:** Test with different cluster sizes (e.g., 1, 2, 3, 5 nodes) as behavior can differ, especially around quorum. Your `Test024_one_node_cannot_elect_leader` (assuming it's for a cluster size > 1 where one node is isolated) is a good example of this.
*/

// start with clusterup_test.go, and do +1/-1 config
// change tests.
func Test400_cluster_add_remove_nodes(t *testing.T) {
	//return
	bubbleOrNot(func() {

		n := 3
		cfg := NewTubeConfigTest(n, t.Name(), faketime)
		c := NewCluster(t.Name(), cfg)
		c.Start()
		defer c.Close()

		c.waitForConnectedGrid()
	})
}

// how should we treat the PeerID? ephemeral? yes.
func Test401_add_node(t *testing.T) {

	// we can only manipulate the simnet, not
	// the real network sockets.
	onlyBubbled(t, func() {

		minClusterSz := 3
		maxClusterSz := 4
		for numNodes := minClusterSz; numNodes < maxClusterSz; numNodes++ {

			forceLeader := 0
			c, leader, leadi, maxterm0 := setupTestCluster(t, numNodes, forceLeader, 401)
			_, _, _ = leader, leadi, maxterm0

			vv("confirming test setup ok...")
			for j := range numNodes {
				look := c.Nodes[j].Inspect()
				if j == 0 {
					if look.Role != LEADER {
						t.Fatalf("error: expected node 0 to be leader (but is %v), leader is: '%v', term=%v", look.Role, look.CurrentLeaderName, look.State.CurrentTerm)
					}
					vv("good: node %v is leader, at term %v", j, look.State.CurrentTerm)
				} else {
					if look.Role != FOLLOWER {
						t.Fatalf("error: expected node %v to be follower (but is %v), leader is: '%v', term=%v", j, look.Role, look.CurrentLeaderName, look.State.CurrentTerm)
					}
					vv("good: node %v is follower, at term %v", j, look.State.CurrentTerm)
				}
				if look.State.CurrentTerm != maxterm0 {
					panic(fmt.Sprintf("wanted maxterm0(%v) to still hold, but now %v, at node %v:\n%v", maxterm0, look.State.CurrentTerm, j, look))
				}
			}

			leaderNode := c.Nodes[0]
			leaderURL := leaderNode.URL

			name4 := fmt.Sprintf("node_%v", numNodes+1)
			cliCfg := *c.Cfg
			cliCfg.PeerServiceName = TUBE_CLIENT
			node4 := NewTubeNode(name4, &cliCfg)
			err := node4.InitAndStart()
			panicOn(err)
			// cluster close won't shut us down, since
			// were not a part of the original setup...
			defer node4.Close()

			vv("ask leader (or other, who should tell us leader) for cluster membership list, before adding the node4") // seen
			list0, _, _, _, _, err := node4.GetPeerListFrom(bkg, leaderURL, leaderNode.name)
			panicOn(err)
			vv("back from node4.GetPeerListFrom, list0 printed out:") // seen
			for name, det := range list0.PeerNames.All() {
				fmt.Printf("    %v -> %v\n", name, det.URL)
			}

			// do we auto-join when we contact the server...hmm.
			// that's maybe not desired! might need to have an
			// extra "official join" step so that we also change
			// the quorum at that point simultaneously.
			lenList0 := list0.PeerNames.Len()
			if lenList0 != numNodes {
				panic(fmt.Sprintf("expected %v, got %v nodes", numNodes, lenList0))
			}

			// we want clients and/or potential new
			// members to be able to find/get a list
			// of the whole rest of the cluster.
			//
			// a) clients so they can fallback to another
			// leader if the curent one dies.
			//
			// b) new members so they can vote and
			// backup the current state.

			// okay, so paradox here is that the
			// node4 is a follower, waiting to hear
			// from a leader. but it doesn't know
			//
			vv("add node4, going from cluster size %v -> %v. this should now fail because node4 is a TUBE_CLIENT and not a TUBE_REPLICA !", numNodes, numNodes+1) // seen
			memlistAfterAdd, _, err := node4.AddPeerIDToCluster(bkg, false, node4.name, node4.PeerID, node4.PeerServiceName, node4.BaseServerHostPort(), leaderURL, 0)
			if err == nil {
				panic("wanted error! got nil")
			}

			if false {
				_, present := memlistAfterAdd.CktReplicaByName[node4.name]
				if present {
					panic(fmt.Sprintf("expected node4 to NOT be in membership after SingleUpdateClusterMemberConfig; memlistAfterAdd = '%v'; node4.PeerID = '%v'", memlistAfterAdd, node4.PeerID))
				}

				vv("had node4 ask leader to join cluster; memlistAfterAdd = '%v'", memlistAfterAdd)
				if len(memlistAfterAdd.CktReplicaByName) != 3 {
					panic(fmt.Sprintf("expected to have only 3 nodes after failing to add node4 membership, but we have '%v'", len(memlistAfterAdd.CktReplicaByName)))
				}
				time.Sleep(time.Second * 10)
				vv("done with 10 seconds of sleep")
			}

			vv("for list1, ask leader for cluster membership list again, node4 should NOT be on it")
			list1, _, _, _, _, err := node4.GetPeerListFrom(bkg, leaderURL, leaderNode.name)
			panicOn(err)
			vv("node4 got list1 back from leader, list= '%#v'", list1)

			lenList0 = list0.PeerNames.Len()
			lenList1 := list1.PeerNames.Len()
			if lenList1 != lenList0 {
				panic(fmt.Sprintf("expected list1 to have same number. len list1=%v but len list0 = %v", lenList1, lenList0))
			}

			// now do a (should be) successful addition, of node5

			name5 := fmt.Sprintf("node_%v", numNodes+2)
			cliCfg5 := *c.Cfg
			cliCfg5.PeerServiceName = TUBE_REPLICA
			node5 := NewTubeNode(name5, &cliCfg5)
			err = node5.InitAndStart()
			panicOn(err)
			// cluster close won't shut us down, since
			// were not a part of the original setup...
			defer node5.Close()

			vv("ask leader (or other, who should tell us leader) for cluster membership list, before adding the node5") // seen
			list5pre, _, _, _, _, err := node5.GetPeerListFrom(bkg, leaderURL, leaderNode.name)
			panicOn(err)
			vv("back from node5.GetPeerListFrom, list5pre printed out:") // seen
			for name, det := range list5pre.PeerNames.All() {
				fmt.Printf("    %v -> %v\n", name, det.URL)
			}

			l5 := list5pre.PeerNames.Len()
			if l5 != numNodes {
				panic(fmt.Sprintf("expected %v, got %v nodes", numNodes, l5))
			}

			vv("add node5, going from cluster size %v -> %v. this should work; node5 is a TUBE_REPLICA", numNodes, numNodes+1) // seen
			memlistAfterAdd5, _, err := node5.AddPeerIDToCluster(bkg, false, node5.name, node5.PeerID, node5.PeerServiceName, node5.BaseServerHostPort(), leaderURL, 0)
			panicOn(err)

			_, present := memlistAfterAdd5.CktReplicaByName[node5.name]
			if !present {
				panic(fmt.Sprintf("expected node5 to be in membership after SingleUpdateClusterMemberConfig; memlistAfterAdd5 = '%v'; node5.PeerID = '%v'", memlistAfterAdd5, node5.PeerID))
			}

			vv("had node5 ask leader to join cluster; memlistAfterAdd5 = '%v'", memlistAfterAdd5)
			if len(memlistAfterAdd5.CktReplicaByName) != 4 {
				msg := fmt.Sprintf("expected to have 4 nodes after adding node5 to membership, but we have '%v'", len(memlistAfterAdd5.CktReplicaByName))
				vv("about to panic: '%v'", msg)
				panic(msg)
			}
			time.Sleep(time.Second * 10)
			vv("done with 10 seconds of sleep")

			vv("for list5, ask leader for cluster membership list again, node5 should be on it")
			list5, _, _, _, _, err := node5.GetPeerListFrom(bkg, leaderURL, leaderNode.name)
			panicOn(err)

			lenList5 := list5.PeerNames.Len()
			lenList0 = list0.PeerNames.Len()
			if lenList5 != lenList0+1 {
				panic(fmt.Sprintf("expected list1 to have one more than list0. len list5=%v but len list0 = %v", lenList5, lenList0))
			}

			// dump log to confirm what we have
			vv("node0 log:")
			err = c.Nodes[0].DumpRaftWAL()
			panicOn(err)

			look0 := c.Nodes[0].Inspect()
			vv("look0 = '%v'", look0)

			// other stuff

			xmem := NewExternalCluster(map[string]string{"x1": "", "x2": "", "x3": ""})
			_ = xmem

			// node4 never got added? right, but
			// it is connected -- it got the peer list back.
			//tkt, err := node5.Write("xmem", xmem.SerzBytes(), 0) // fine.

			// hung because it doesn't know how to contact a leader!
			// we stopped sending AE to clients. but maybe we
			// do need to heartbeat to them (no data) so they
			// know who the leader is(!)
			_, err = node4.UseLeaderURL(bkg, leaderURL)
			panicOn(err)
			tkt, err := node4.Write(bkg, "", "xmem", xmem.SerzBytes(), 0, nil)
			panicOn(err)
			vv("tkt = '%v'", tkt)

			vv("node5 log:")
			err = node5.DumpRaftWAL()
			panicOn(err)

			vv("node0 log:")
			err = c.Nodes[0].DumpRaftWAL()
			panicOn(err)

			//vv("stopping after add node to verify that much works.")
			//select {}
			//return

			vv("node5 should have links to rest of cluster grid now... right; yes so it can hold elections if need be. Or we could be lazy and wait until needed to avoid spinning up O(n*n) network links.")

			c.showClusterGrid("srv_node_5", 3)

			vv("about to remove node_5 with leaderNode.RemovePeerIDFromCluster()")

			//removeMe := node5.PeerID
			const nonVoting bool = false
			memlistAfterRemove, _, err := leaderNode.RemovePeerIDFromCluster(bkg, nonVoting, node5.name, node5.PeerID, node5.PeerServiceName, node5.BaseServerHostPort(), leaderURL, 0)
			panicOn(err)
			//vv("back from RemovePeerIDFromCluster(node5); memlistAfterRemove = '%v'", memlistAfterRemove) // seen

			_, present = memlistAfterRemove.CktReplicaByName[node5.name]
			if present {
				panic(fmt.Sprintf("expected node5 to NOT be in membership after SingleUpdateClusterMemberConfig; memlistAfterRemove = '%v'; node5.PeerID = '%v'", memlistAfterRemove, node5.PeerID))
			}
			//vv("for list2, about to call c.Nodes[1] GetPeerListFrom(leaderURL='%v')", leaderURL) // seen

			list2, _, _, _, _, err := c.Nodes[1].GetPeerListFrom(bkg, leaderURL, leaderNode.name) // got to here
			panicOn(err)

			lenList2 := list2.PeerNames.Len()
			lenList0 = list0.PeerNames.Len()
			if lenList2 != lenList0 ||
				lenList2 != len(memlistAfterRemove.CktReplicaByName) {
				panic("expected list2 to have same len as list0")
			}
			//vv("good, after RemovePeerIDFromCluster(node5) list2 was same size as list0(%v); list2 = '%v'", len(list0), list2)

			// cleanup
			c.Close()
			//if numNodes < maxClusterSz-1 {
			//	time.Sleep(time.Second)
			//}

			vv("401 back from cluster Close")
		} // for numNodes loop
	})
}

// The s.name or PeerName is a kind of abstract,
// long-term Peer identifier. It is
// more stable than PeerID. PeerID will change on
// reboot. We do want to go through
// the whole re-enrollment process after a reboot, so we
// can catch-them up; in case they lost alot of
// log/state changes. But we need a stable identifier: the PeerName.
// The PeerName or s.name is just the name of the node.
// Short. Sweet. Simple.

func Test402_build_up_a_cluster_from_one_node(t *testing.T) {
	//return
	onlyBubbled(t, func() {

		baseNodeCount := 1

		forceLeader := 0
		// try a very high cluster size, to avoid
		// single nodes trying to go it alone.

		// :) nice! seems to have worked.
		artificiallyHighClusterSize := 19
		c, leader, leadi, maxterm0 := setupTestCluster(t, baseNodeCount, forceLeader, 402)
		_, _, _ = leader, leadi, maxterm0

		//vv("confirming test setup ok...")
		look := c.Nodes[0].Inspect()
		if look.Role != LEADER {
			t.Fatalf("error: expected node 0 to be leader (but is %v), leader is: '%v', term=%v", look.Role, look.CurrentLeaderName, look.State.CurrentTerm)
		}
		//vv("good: node 0 is leader, at term %v", look.State.CurrentTerm)

		if look.State.CurrentTerm != 1 {
			panic(fmt.Sprintf("wanted to be at term 1, but now %v, at node 0:\n%v", look.State.CurrentTerm, look))
		}

		leaderNode := c.Nodes[0]
		leaderURL := leaderNode.URL
		//vv("leaderURL = '%v'", leaderURL)

		totNodes := 9

		if totNodes > artificiallyHighClusterSize {
			panic("better keep totNodes <= artificiallyHighClusterSize")
		}
		// i is the node number to add, one at a time.
		for i := baseNodeCount; i < totNodes; i++ {

			name1 := fmt.Sprintf("node_%v", i)
			cliCfg := *c.Cfg
			cliCfg.ClusterSize = artificiallyHighClusterSize
			cliCfg.PeerServiceName = TUBE_REPLICA
			node1 := NewTubeNode(name1, &cliCfg)
			err := node1.InitAndStart()
			panicOn(err)
			// cluster close won't shut us down, since
			// were not a part of the original setup...
			defer node1.Close()

			newExpectedClusterSz := i + baseNodeCount
			//vv("add node '%v', going -> cluster size %v", name1, newExpectedClusterSz)
			// we automatically get a member list afterwards
			memlistAfterAdd, _, err := node1.AddPeerIDToCluster(bkg, false, name1, node1.PeerID, node1.PeerServiceName, node1.BaseServerHostPort(), leaderURL, 0)
			panicOn(err)

			_, present := memlistAfterAdd.CktReplicaByName[node1.name]
			if !present {
				panic(fmt.Sprintf("expected '%v' to be in membership after SingleUpdateClusterMemberConfig; memlistAfterAdd = '%v'; node1.PeerID = '%v'", name1, memlistAfterAdd, node1.PeerID))
			}

			//vv("we had node1 ask leader to join cluster; memlistAfterAdd = '%v'", memlistAfterAdd)
			if len(memlistAfterAdd.CktReplicaByName) != newExpectedClusterSz {
				panic(fmt.Sprintf("expected to have %v nodes after adding node1 membership, but we have '%v'", newExpectedClusterSz, len(memlistAfterAdd.CktReplicaByName)))
			}

		} // end for i < totNodes

		// let all nodes finish connecting in the background.
		// Under synctest this is always long enough.
		time.Sleep(time.Second)
		if c.Cfg.UseSimNet {
			if c.Snap == nil {
				c.Snap = c.Cfg.RpcCfg.GetSimnetSnapshotter()
				if c.Snap == nil {
					panic("grid is connected, why no snapshotter?")
				}
			}
			snp := c.Snap.GetSimnetSnapshot()
			//vv("at end, simnet = '%v'", snp.LongString())
			//vv("at end, simnet.Peer = '%v'", snp.Peer)
			//vv("at end, simnet.DNS = '%#v'", snp.DNS)
			matrix := snp.PeerMatrix()
			gotUR := matrix.UpRightTriCount
			want := totNodes * (totNodes - 1) / 2
			vv("expected %v, got gotUR=%v", want, gotUR)
			vv("matrix='%v'", matrix)
			//if gotUR != want {
			if gotUR < want {
				// panic: wrong number of connections at end (got 64, want 36) on 402. yeah we need prunning??
				panic(fmt.Sprintf("wrong number of connections at end (got %v, want %v)", gotUR, want))
			}
		}

		// cleanup
		c.Close()
		vv("402 back from cluster Close; totNodes = %v", totNodes)
	})

}

// remove nodes, reduce cluster size
func Test403_reduce_a_cluster_down_to_one_node(t *testing.T) {
	//return // other 40 are green
	// first build up, just like 402. But then also reduce down
	// the cluster, shrinking it down to one node.
	//onlyBubbled(t, func() {
	bubbleOrNot(func() {

		baseNodeCount := 1

		forceLeader := 0
		// try a very high cluster size, to avoid
		// single nodes trying to go it alone.

		// :) nice! seems to have worked.
		artificiallyHighClusterSize := 19 // TODO: make this a default?
		c, leader, leadi, maxterm0 := setupTestCluster(t, baseNodeCount, forceLeader, 403)
		_, _, _ = leader, leadi, maxterm0

		//vv("confirming test setup ok...")
		look := c.Nodes[0].Inspect()
		if look.Role != LEADER {
			t.Fatalf("error: expected node 0 to be leader (but is %v), leader is: '%v', term=%v", look.Role, look.CurrentLeaderName, look.State.CurrentTerm)
		}
		//vv("good: node 0 is leader, at term %v", look.State.CurrentTerm)

		if look.State.CurrentTerm != 1 {
			panic(fmt.Sprintf("wanted to be at term 1, but now %v, at node 0:\n%v", look.State.CurrentTerm, look))
		}

		leaderNode := c.Nodes[0]
		leaderURL := leaderNode.URL
		//vv("leaderURL = '%v'", leaderURL)

		totNodes := 3
		nodes := make([]*TubeNode, 0, totNodes)
		nodes = append(nodes, c.Nodes[0])

		if totNodes > artificiallyHighClusterSize {
			panic("better keep totNodes <= artificiallyHighClusterSize")
		}
		// i is the node number to add, one at a time.
		for i := baseNodeCount; i < totNodes; i++ {

			name1 := fmt.Sprintf("node_%v", i)
			cliCfg := *c.Cfg
			cliCfg.ClusterSize = artificiallyHighClusterSize
			cliCfg.PeerServiceName = TUBE_REPLICA
			node1 := NewTubeNode(name1, &cliCfg)
			err := node1.InitAndStart()
			panicOn(err)
			nodes = append(nodes, node1)
			// cluster close won't shut us down, since
			// were not a part of the original setup...
			defer node1.Close()

			if false { // name1 == "node_1" {
				// it looked like node_1 was getting redudant AE.
				// let's assert they are minimal.
				go func(node1 *TubeNode) {
					vv("test that node1 is getting non-redundant AE")
					var gotBegIndex1, gotEndIndex1 int64
					var gotBegIndex2, gotEndIndex2 int64
					for i := 0; i < 6; i++ {
						select {
						case ae := <-node1.testGotAeFromLeader:
							//vv("good: node_1 got AE on i=%v '%v'", i, ae) // data racy

							switch i {
							case 0:
								// ignore the first one; we have an
								// empty log and it starts at 2, we
								// it will get rejected
							case 1:
								n := len(ae.Entries)
								if n > 0 {
									gotBegIndex1 = ae.Entries[0].Index
									gotEndIndex1 = ae.Entries[n-1].Index
									vv("gotBegIndex1=%v; gotEndIndex1=%v; i=%v", gotBegIndex1, gotEndIndex1, i)
								}
							default:
								n := len(ae.Entries)
								if n > 0 {
									gotBegIndex2 = ae.Entries[0].Index
									gotEndIndex2 = ae.Entries[n-1].Index

									if gotBegIndex2 <= gotEndIndex1 {
										panic(fmt.Sprintf("inefficiency detected: why are we (i=%v) re-sending [%v:%v] when we have [%v:%v] ???", i, gotBegIndex2, gotEndIndex2, gotBegIndex1, gotEndIndex1))
									}
									gotBegIndex1 = gotBegIndex2
									gotEndIndex2 = gotEndIndex2
									vv("gotBegIndex2=%v; gotEndIndex2=%v; on i=%v", gotBegIndex2, gotEndIndex2, i)
								}

							}
						case aeAck := <-node1.testGotAeAckFromFollower:
							vv("got aeAck at i = %v: '%v'", i, aeAck)
						}
					}
				}(node1)
			}

			newExpectedClusterSz := i + baseNodeCount
			vv("add node '%v', going to cluster size %v", name1, newExpectedClusterSz)
			// we automatically get an Inspection and thus .CktReplicaByName for member list afterwards
			inspAfterAdd, _, err := node1.AddPeerIDToCluster(bkg, false, name1, node1.PeerID, node1.PeerServiceName, node1.BaseServerHostPort(), leaderURL, 0)
			panicOn(err)

			// on addition, we should get back a committed
			// membership change, right?

			//time.Sleep(time.Second * 5)

			// for sure it should be in Newest... sanity check
			_, presentNewest := inspAfterAdd.MC.PeerNames.get2(node1.name)
			if !presentNewest {
				panic(fmt.Sprintf("expected '%v' to be in (newest) membership after SingleUpdateClusterMemberConfig; inspAfterAdd = '%v'", name1, inspAfterAdd.MC.Short()))
			} else {
				vv("good, found our addition in newest: node1.name='%v'", node1.name)
			}
			// okay: so we have it Newest, but the problem is
			// that it is not in Committed, because... we need
			// to update Committed on followers too.

			//_, present := inspAfterAdd.CktReplicaByName[node1.name]
			_, present := inspAfterAdd.MC.PeerNames.get2(node1.name)
			if !present {
				panic(fmt.Sprintf("expected '%v' to be in (committed) membership after SingleUpdateClusterMemberConfig; inspAfterAdd = '%v'", name1, inspAfterAdd.MC.Short())) // panic here
			}

			vv("we had node1 ask leader to join cluster; inspAfterAdd = '%v'", inspAfterAdd.MC.Short()) // not seen??
			if len(inspAfterAdd.CktReplicaByName) != newExpectedClusterSz {
				panic(fmt.Sprintf("expected to have %v nodes after adding node1 membership, but we have '%v'", newExpectedClusterSz, len(inspAfterAdd.CktReplicaByName)))
			}
			vv("good: we see node1.name = '%v' a part of the membership change in inspAfterAdd.MC: '%v'", node1.name, inspAfterAdd.MC)
		} // end for i < totNodes

		// let all nodes finish connecting in the background.
		// Under synctest this is always long enough.
		time.Sleep(time.Second)
		if c.Cfg.UseSimNet {
			if c.Snap == nil {
				c.Snap = c.Cfg.RpcCfg.GetSimnetSnapshotter()
				if c.Snap == nil {
					panic("grid is connected, why no snapshotter?")
				}
			}
			snp := c.Snap.GetSimnetSnapshot()
			//vv("at end, simnet = '%v'", snp.LongString())
			//vv("at end, simnet.Peer = '%v'", snp.Peer)
			//vv("at end, simnet.DNS = '%#v'", snp.DNS)
			matrix := snp.PeerMatrix()
			gotUR := matrix.UpRightTriCount
			want := totNodes * (totNodes - 1) / 2
			vv("expected %v, got gotUR=%v", want, gotUR)
			vv("matrix='%v'", matrix)
			if gotUR < want { // used to be gotUR==want but we pessimistically connect more than the upper right triangle to get reboot_test working
				panic(fmt.Sprintf("wrong number of connections at end (got %v, want %v)", gotUR, want))
			}
		}

		// verify we have no tickets waiting at any node.
		if len(nodes) != totNodes {
			panic("what? why don't we have all our nodes?")
		}
		var lli0 int64
		var lastTerm0 int64
		for i := 0; i < totNodes; i++ {
			preRemoveLook := nodes[i].Inspect()
			if i == 0 {
				lli0 = preRemoveLook.LastLogIndex
				lastTerm0 = preRemoveLook.LastLogTerm
			} else {
				if preRemoveLook.LastLogIndex != lli0 {
					panic(fmt.Sprintf("at node i = %v, preRemoveLook.LastLogIndex=%v; but lli0=%v", i, preRemoveLook.LastLogIndex, lli0))
				}
				if preRemoveLook.LastLogTerm != lastTerm0 {
					panic(fmt.Sprintf("at node i = %v, preRemoveLook.LastLogTerm=%v; but lastTerm0=%v", i, preRemoveLook.LastLogTerm, lastTerm0))
				}
			}
			if len(preRemoveLook.WaitingAtLeader) > 0 {
				panic(fmt.Sprintf("should be empty... WaitingAtLeader = '%#v'", preRemoveLook.WaitingAtLeader))
			}
			if len(preRemoveLook.WaitingAtFollow) > 0 {
				panic(fmt.Sprintf("should be empty... WaitingAtFollow = '%#v'", preRemoveLook.WaitingAtFollow))
			}
		}
		vv("good: no tickets waiting, and all logs are ending at LastLogIndex:%v / LastLogTerm=%v", lli0, lastTerm0)

		vv("........ it's decrement time! time to reduce cluster size!")
		fmt.Printf(`
       =============  BEGIN REMOVING NODES ============
`)

		node0 := nodes[0]

		// i is the node number to subtract, one at a time.
		for i := totNodes - 1; i > 0; i-- {

			namei := fmt.Sprintf("node_%v", i)

			newExpectedClusterSz := i
			vv("remove node '%v', going to cluster size %v", namei, newExpectedClusterSz)
			// we automatically get a member list afterwards
			const nonVoting bool = false
			memlistAfterRemove, _, err := node0.RemovePeerIDFromCluster(bkg, nonVoting, namei, nodes[i].PeerID, nodes[i].PeerServiceName, nodes[i].BaseServerHostPort(), leaderURL, 0)
			panicOn(err) // panic: Q1 unmet, mongoLeaderCanReconfig cannot reconfig on 'node_0': inCurrentConfigCount(1) < curMC.majority(2); curMC='[term:1; vers=5; idx:2_]{node_0,node_1}

			_, present := memlistAfterRemove.CktReplicaByName[nodes[i].name]
			if present {
				panic(fmt.Sprintf("expected '%v' to be gone! membership after SingleUpdateClusterMemberConfig; memlistAfterRemove = '%v'; nodes[%v].PeerID = '%v'", namei, memlistAfterRemove, i, nodes[i].PeerID)) // red! hit.
			}

			//vv("we had nodes[i] ask leader to remote it from cluster; memlistAfterRemove = '%v'", memlistAfterRemove)
			if len(memlistAfterRemove.CktReplicaByName) != newExpectedClusterSz {
				panic(fmt.Sprintf("expected to have %v nodes after removing nodes[i] membership, but we have '%v'", newExpectedClusterSz, len(memlistAfterRemove.CktReplicaByName)))
			}

			// allow simnet to remove the node too.
			nodes[i].Close()

		} // end for i from totNodes ... 1

		vv("all done with cluster reduce. should be down to just one node, node0")

		// the inspections above verified the removes
		// after each one, but since the nodes are
		// still technically "up", and have not been
		// shutdown, simnet still knows about them.
		// We have to actually close each node that
		// goes down to have the simnet see the close
		// and stop reporting on them.

		// let all nodes finish connecting in the background.
		// Under synctest this is always long enough.
		time.Sleep(time.Second)
		if c.Cfg.UseSimNet {
			if c.Snap == nil {
				c.Snap = c.Cfg.RpcCfg.GetSimnetSnapshotter()
				if c.Snap == nil {
					panic("grid is connected, why no snapshotter?")
				}
			}
			snp := c.Snap.GetSimnetSnapshot()
			//vv("at end, simnet = '%v'", snp.LongString())
			//vv("at end, simnet.Peer = '%v'", snp.Peer)
			//vv("at end, simnet.DNS = '%#v'", snp.DNS)
			matrix := snp.PeerMatrix()

			// Note: TCP by default wastes no bandwidth on
			// keep alives, and so the remote can crash or
			// suffer network outage and the local socket
			// will remain open. Hence we can expect our
			// node_0 to (correctly) have an open read to
			// the remote node_1, even though that node is
			// down. This is "operating as intended".
			//
			// But in our case of removing a node from the
			// cluster, we do want to have our node_0
			// deliberately close all connections to
			// replica nodes that have been removed.
			// So we want to do this at the tube layer,
			// not deeper in the simnet.
			gotUR := matrix.UpRightTriCount
			want := 0
			vv("expected %v, got gotUR=%v; matrix.NumPeer = %v", want, gotUR, matrix.NumPeer)
			vv("matrix='%v'", matrix)
			if matrix.NumPeer != 1 {
				if gotUR != want {
					panic(fmt.Sprintf("wrong number of connections at end (got %v, want %v)", gotUR, want))
				}
			}
		}

		lastLook := nodes[0].Inspect()
		_, hasUs := lastLook.MC.PeerNames.get2("node_0") // leaderNode.name
		if !hasUs || lastLook.MC.PeerNames.Len() != 1 {
			panic(fmt.Sprintf("want cluster size 1 (just us) now, not: lastLook.MC='%v'", lastLook.MC.ShortProv()))
		}

		// cleanup
		c.Close()
		vv("403 back from cluster Close; totNodes = %v", totNodes)
	})

}
