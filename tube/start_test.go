package tube

import (
	"fmt"
	//"strings"
	//"sort"
	"time"

	//rpc "github.com/glycerine/rpc25519"
	"testing"
)

var _ = fmt.Sprintf

// start a 1 node cluster. have it elect itself.
// based on 057 restart_test.
func Test063_start_one_node_cluster(t *testing.T) {

	onlyBubbled(t, func(t *testing.T) {

		minClusterSz := 1
		maxClusterSz := 1
		for numNodes := minClusterSz; numNodes <= maxClusterSz; numNodes++ {

			forceLeader := 0
			c, leader, leadi, maxterm := setupTestCluster(t, numNodes, forceLeader, 63)
			_, _, _ = leader, leadi, maxterm
			vv("back from setupTestCluster -- with one node")

			vv("verify the one node thinks it is leader")
			if leadi != 0 {
				panicf("got leadi=%v but expected 0", leadi)
			}
			look := c.Nodes[leadi].Inspect()
			if look.CurrentLeaderName != c.Nodes[leadi].name {
				panicf("wanted node_0 to know itself as leader, but it thinks '%v'  is leader", look.CurrentLeaderName)
			}
			vv("good in 1 node cluster of '%v', look.CurrentLeaderName='%v'", c.Nodes[leadi].name, look.CurrentLeaderName)

			if false {
				//time.Sleep(time.Second * 50)

				for j := range numNodes {
					look := c.Nodes[j].Inspect()
					//vv("after startup: node %v look: '%v'", j, look)
					vv("after startup: node %v has CktReplicaByName: '%v'", j, look.CktReplicaByName)
				}
				vv("done with inspections")
				//c.Close()
				//return

				//snap0 := c.SimnetSnapshot()
				//vv("%v numNode cluster after 5 seconds, snap = '%v'", numNodes, snap0.LongString())

				//select {}
				//time.Sleep(time.Minute)
				vv("power off leader")

				c.Nodes[leadi].SimCrash()

				time.Sleep(time.Second * 15)

				//panic("15 seconds after poweroff of leader 0, what happened?")

				//snap := c.SimnetSnapshot()
				//vv("5 seconds after crashing the leader") // , snap = '%v'", snap.LongString())

				// notice alot of client failing to find srv_node_0
				vv("done with 15 sec of sleep, power ON node_0")

				c.SimBoot(leadi)
				time.Sleep(time.Second * 2)
				// reconnect timeouts should have all been done now
				tm2secAfterReboot := time.Now()
				time.Sleep(time.Second * 20)
				vv("done with 20 sec of sleep after node_0 back on.")

				for j := range numNodes {
					look := c.Nodes[j].Inspect()
					if j == 0 {
						if look.Role == LEADER {
							panic("error: expected node_0 to NOT be leader")
						}
					}
					//vv("node_0 back on 20 sec, look[%v] = %v", j, look)
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
						vv("bad! will panic! xnode j=%v had %v CktReplicaByName, not numNodes=%v", j, n, numNodes)

						//snap := c.SimnetSnapshot()
						//vv("pre-panic snap = '%v'", snap.LongString())
						panic("fix the above lack of CktReplicaByName entries!")
					}
				}
				if !haveLeader {
					panic("should have elected leader")
				}

				// assert there there we zero watchdog timeouts
				// after the successful reconnect of node 0.
				for j := range numNodes {
					var maxTO time.Time
				innerFor:
					for {
						select {
						case to := <-c.Nodes[j].testWatchdogTimeoutReconnectCh:
							if to.After(maxTO) {
								maxTO = to
							}
						default:
							break innerFor
						}
					}
					if maxTO.After(tm2secAfterReboot) {
						panic(fmt.Sprintf("node %v had a watchdog reconnect timeout('%v') after tm2secAfterReboot='%v'", j, nice(maxTO), nice(tm2secAfterReboot)))
					}
				}
			}
			vv("all good 063: finish up test by closing down cluster")
			// cleanup
			c.Close()
		} // for numNodes loop
	})
}

func Test064_one_node_in_two_node_cluster(t *testing.T) {

	onlyBubbled(t, func(t *testing.T) {

		minClusterSz := 2
		maxClusterSz := 2
		for numNodes := minClusterSz; numNodes <= maxClusterSz; numNodes++ {

			forceLeader := -1 // nobody forced.
			c, leader, leadi, maxterm := setupTestCluster(t, numNodes, forceLeader, 63)
			_, _, _ = leader, leadi, maxterm
			vv("back from setupTestCluster -- numNodes=%v", numNodes)

			vv("verify only one of the nodes thinks it is leader")

			var look *Inspection
			leaderCount := 0
			inspLead := ""
			for j := range numNodes {
				look = c.Nodes[j].Inspect()
				if j == 0 {
					inspLead = look.CurrentLeaderName
				} else {
					if look.CurrentLeaderName != inspLead {
						panicf("node %v has different leader '%v' vs node_0 with '%v' leader", j, look.CurrentLeaderName, inspLead)
					}
				}
				if look.CurrentLeaderName == c.Nodes[j].name {
					leaderCount++
				}
			}
			if leaderCount != 1 {
				panicf("wanted one leader, got %v in cluster size %v", leaderCount, numNodes)
			}
			vv("good in %v node cluster, have single leader look.CurrentLeaderName='%v'", numNodes, look.CurrentLeaderName)

			if false {
				//time.Sleep(time.Second * 50)

				for j := range numNodes {
					look := c.Nodes[j].Inspect()
					//vv("after startup: node %v look: '%v'", j, look)
					vv("after startup: node %v has CktReplicaByName: '%v'", j, look.CktReplicaByName)
				}
				vv("done with inspections")
				//c.Close()
				//return

				//snap0 := c.SimnetSnapshot()
				//vv("%v numNode cluster after 5 seconds, snap = '%v'", numNodes, snap0.LongString())

				//select {}
				//time.Sleep(time.Minute)
				vv("power off leader")

				c.Nodes[leadi].SimCrash()

				time.Sleep(time.Second * 15)

				//panic("15 seconds after poweroff of leader 0, what happened?")

				//snap := c.SimnetSnapshot()
				//vv("5 seconds after crashing the leader") // , snap = '%v'", snap.LongString())

				// notice alot of client failing to find srv_node_0
				vv("done with 15 sec of sleep, power ON node_0")

				c.SimBoot(leadi)
				time.Sleep(time.Second * 2)
				// reconnect timeouts should have all been done now
				tm2secAfterReboot := time.Now()
				time.Sleep(time.Second * 20)
				vv("done with 20 sec of sleep after node_0 back on.")

				for j := range numNodes {
					look := c.Nodes[j].Inspect()
					if j == 0 {
						if look.Role == LEADER {
							panic("error: expected node_0 to NOT be leader")
						}
					}
					//vv("node_0 back on 20 sec, look[%v] = %v", j, look)
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
						vv("bad! will panic! xnode j=%v had %v CktReplicaByName, not numNodes=%v", j, n, numNodes)

						//snap := c.SimnetSnapshot()
						//vv("pre-panic snap = '%v'", snap.LongString())
						panic("fix the above lack of CktReplicaByName entries!")
					}
				}
				if !haveLeader {
					panic("should have elected leader")
				}

				// assert there there we zero watchdog timeouts
				// after the successful reconnect of node 0.
				for j := range numNodes {
					var maxTO time.Time
				innerFor:
					for {
						select {
						case to := <-c.Nodes[j].testWatchdogTimeoutReconnectCh:
							if to.After(maxTO) {
								maxTO = to
							}
						default:
							break innerFor
						}
					}
					if maxTO.After(tm2secAfterReboot) {
						panic(fmt.Sprintf("node %v had a watchdog reconnect timeout('%v') after tm2secAfterReboot='%v'", j, nice(maxTO), nice(tm2secAfterReboot)))
					}
				}
			}
			vv("all good 063: finish up test by closing down cluster")
			// cleanup
			c.Close()
		} // for numNodes loop
	})
}
