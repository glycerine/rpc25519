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

// kill, pause, and reboot the leader, can it rejoin the
// cluster as a follower? Just like manual
// etc/local prod testing, but now in an actual test.
func Test055_kill_pause_reboot_node_0(t *testing.T) {

	onlyBubbled(t, func(t *testing.T) {

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

					snap := c.SimnetSnapshot()
					vv("pre-panic snap = '%v'", snap)
					panic("fix the above lack of CktReplicaByName entries!")
				}
			}
			if !haveLeader {
				panic("should have elected leader")
			}

			vv("all good 055: finish up test by closing down cluster")
			// cleanup
			c.Close()
		} // for numNodes loop
	})
}
