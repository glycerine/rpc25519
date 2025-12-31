package tube

import (
	//"bytes"
	//"fmt"
	"time"

	"context"
	"testing"
)

func Test079_sublease_from_leader(t *testing.T) {

	// subleases must never be longer than the 0.5 election timeout,
	// so that leader can keep them in memory.
	bubbleOrNot(t, func(t *testing.T) {

		numNodes := 3
		//n := 3

		c, _, leadi, _ := setupTestCluster(t, numNodes, 0, 79)
		//cfg := NewTubeConfigTest(n, t.Name(), globalUseSimnet)
		//c := NewCluster(t.Name(), cfg)
		//c.Start()
		defer c.Close()
		if leadi != 0 {
			panicf("leader should be 0, not %v", leadi)
		}

		nodes := c.Nodes
		leader := nodes[0]
		ctx := context.Background()
		//sess, err := nodes[0].CreateNewSession(ctx, leader.URL)
		sess, err := nodes[1].CreateNewSession(ctx, leader.URL)
		panicOn(err)
		defer sess.Close()
		vv("good: got session to leader (maybe from leader to leader, but meh, we cannot always know who is leader...). sess=%p", sess)

		leaseKey := Key("leaseKeyA")
		leaseTkt, err := sess.RamOnlySublease(ctx, leaseKey, 0)
		panicOn(err)
		now := time.Now()
		until := leaseTkt.SubleaseGrantedUntilTm
		left := until.Sub(now)
		if left <= 0 {
			panicf("left(%v) <= 0; until='%v'; now='%v'", left, until, now)
		}
		vv("lease has left %v", left)

		// avoid bubble complaint about still live goro by sleeping 1 sec after.
		// since halter can take 500 msec to shutdown.
		c.Close()
		time.Sleep(time.Second)
	})
}
