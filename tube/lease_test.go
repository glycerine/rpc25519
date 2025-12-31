package tube

import (
	//"bytes"
	//"fmt"
	"time"

	"context"
	"testing"
)

func Test079_lease_from_leader(t *testing.T) {

	// leases allow a "Czar and master of partition" model:
	// subleases must never be longer than the Czar's lease,
	// so that master can keep them in memory.

	// How heirarchical leases work to create RAM-only
	// fast updates of some state;
	// from Butler Lampson's 1996 paper, "How to Build
	// a Highly Available System Using Consensus":
	//
	// "Run consensus once to elect a czar C and give C
	// a lease on a large part of the state.
	// Now C gives out sub-leases on x and y to masters.
	// Each master controls its own resources. The masters
	// renew their sub-leases with the czar. This is cheap since it
	// doesn’t require any coordination. The czar renews
	// its lease by consensus. This costs more, but there's
	// only one czar lease. Also, the czar can be simple
	// and less likely to fail, so a longer lease may be
	// acceptable. Hierarchical leases are commonly used in
	// replicated file systems and in clusters.
	// By combining the ideas of consensus, leases, and
	// hierierarchy, it’s possible to build highly
	// available systems that are also highly efficient."

	// here we test that our write of a key can be leased
	// and not re-leased until that lease has expired.
	bubbleOrNot(t, func(t *testing.T) {

		numNodes := 3
		//n := 3

		cfg := NewTubeConfigTest(numNodes, t.Name(), faketime)
		cfg.MinElectionDur = 10 * time.Second
		cfg.HeartbeatDur = time.Second
		c, _, leadi, _ := setupTestClusterWithCustomConfig(cfg, t, numNodes, 0, 79)

		defer c.Close()
		//vv("c.Cfg = '%#v'", c.Cfg)

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

		leaseTable := Key("leaseTableA")
		leaseKey := Key("leaseKeyA")
		leaseVal := Val("leaseValA")
		leaseVtype := "lease"
		leaseRequestDur := time.Minute
		leaseTkt, err := sess.Write(ctx, leaseTable, leaseKey, leaseVal, 0, leaseVtype, leaseRequestDur)
		panicOn(err)
		now := time.Now()
		until := leaseTkt.LeaseUntilTm
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
