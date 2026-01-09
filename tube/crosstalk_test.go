package tube

import (
	"fmt"
	//"strings"
	"time"

	rpc "github.com/glycerine/rpc25519"
	"testing"
)

var _ = fmt.Sprintf

// verify that matches and drop/deaf do not
// cross talk into the wrong simnet queues.
func Test056_no_crosstalk_dropdeaf_to_other_queues(t *testing.T) {

	onlyBubbled(t, func(t *testing.T) {

		minClusterSz := 3
		maxClusterSz := 3
		for numNodes := minClusterSz; numNodes <= maxClusterSz; numNodes++ {

			forceLeader := 0
			c, leader, leadi, maxterm := setupTestCluster(t, numNodes, forceLeader, 55)
			_, _, _ = leader, leadi, maxterm

			time.Sleep(time.Second * 50)

			//vv("%v numNode cluster after 5 seconds, snap = '%v'", numNodes, c.SimnetSnapshot().LongString())

			//vv("power off leader")

			c.Nodes[leadi].SimCrash()

			time.Sleep(time.Second * 50)

			snap := c.SimnetSnapshot(false)
			//vv("50 seconds after crashing the leader") // , snap = '%v'", snap.LongString())

			for i, peer := range snap.Peer {
				_ = i
				for k, sum := range peer.ConnmapOrigin {
					if sum.DroppedSendQ.Len() > 0 {
						for it := sum.DroppedSendQ.Tree.Min(); !it.Limit(); it = it.Next() {
							send, orig, targ := rpc.ItemToSendOrigTarg(it)
							//vv("orig = '%v'; targ='%v'", orig, targ)
							if orig == "srv_node_0" || targ == "srv_node_0" {
								continue
							}
							alwaysPrintf("k='%v' orig='%v'; targ='%v'; send = '%v'", k, orig, targ, send)
							panic("should not have any drops where no node_0")
						}
					}

					if sum.DeafReadQ.Len() > 0 {
						for it := sum.DeafReadQ.Tree.Min(); !it.Limit(); it = it.Next() {
							send, orig, targ := rpc.ItemToSendOrigTarg(it)
							//read, orig, targ := rpc.ItemToReadOrigTarg(it)
							//vv("orig = '%v'; targ='%v' from='%v'", orig, targ, rpc.ItemToMopString(it))
							if orig == "srv_node_0" || targ == "srv_node_0" {
								continue
							}
							alwaysPrintf("k='%v' orig='%v'; targ='%v'; send = '%v'", k, orig, targ, send)
							panic("should not have any deaf read sends where no node_0")
						}
					}

				}
			}
			// well hang/deadlock waiting for this node
			// to close when it is already down in c.Close()
			// if we don't turn it back on first.
			c.SimBoot(leadi)

			// cleanup
			c.Close()
		} // for numNodes loop
	})
}
