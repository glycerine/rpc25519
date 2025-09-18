package tube

import (
	"fmt"
	"time"

	//rpc "github.com/glycerine/rpc25519"
	"testing"
)

// work on test for watchdog cktPlus re-start ckt
// mechanism.
// We want to reconnect to all partners in the grid
// at once rather than waiting for each in turn to
// fail in order to find one that works--to chop
// off the tail latency on waiting for a tubels/tup startup.
func Test058_watchdog(t *testing.T) {

	t.Skip("still a work in progress") // TODO finish up this test.

	onlyBubbled(t, func() {

		minClusterSz := 2
		maxClusterSz := 2
		for numNodes := minClusterSz; numNodes <= maxClusterSz; numNodes++ {

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

			majority := []int{leadi}
			minority := []int{(leadi + 1) % numNodes}
			vv("majority = '%#v'", majority)
			vv("minority = '%#v'", minority)
			if minority[0] != 1 {
				panic("assumption that {1} is the minority partition violated")
			}

			c.IsolateNode(1)

			//vv("simnet is now: %v", c.SimnetSnapshot())
			time.Sleep(time.Minute * 1)
			vv("done with 1 minute of sleep")

			// maxterm should not have changed on either side.

			for j := range numNodes {
				look := c.Nodes[j].Inspect()
				if j == 0 {
					if look.Role != FOLLOWER {
						panic(fmt.Sprintf("error: expected node have stepped-down (is %v), leader is: '%v', term=%v", look.Role, look.CurrentLeaderName, look.State.CurrentTerm))
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

			// cleanup
			vv("end of test 058 for now")
			c.Close()

			return

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

			//if numNodes < maxClusterSz-1 {
			//	time.Sleep(time.Second)
			//}
			// cleanup
			c.Close()

		} // for numNodes loop
	})
}
