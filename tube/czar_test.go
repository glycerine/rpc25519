package tube

import (
	//"bytes"
	//"context"
	//"fmt"
	"time"

	"testing"
)

func Test808_czar_only_one_at_a_time(t *testing.T) {

	bubbleOrNot(t, func(t *testing.T) {

		numNodes := 3
		forceLeader := 0
		c, leader, leadi, _ := setupTestCluster(t, numNodes, forceLeader, 808)
		defer c.Close()
		_, _ = leader, leadi

		// nodes := c.Nodes
		_ = leader
		leaderNode := c.Nodes[leadi]
		leaderURL := leaderNode.URL

		cliName := "member_0_test808"

		cliCfg := *c.Cfg

		// setupTestCluster should have:
		//cliCfg.RpcCfg.QuietTestMode = false
		//cliCfg.UseSimNet = true
		//cliCfg.isTest = true

		cliCfg.MyName = cliName
		cliCfg.PeerServiceName = TUBE_CLIENT
		cliCfg.ClockDriftBound = 500 * time.Millisecond
		for _, node := range c.Nodes {
			cliCfg.Node2Addr[node.name] = node.URL
		}
		tableSpace := "czar_test_808"

		vv("leader is '%v' at url = '%v'; cfg.Node2Addr = '%#v'", leader, leaderURL, c.Cfg.Node2Addr)

		mem := NewRMember(tableSpace, &cliCfg)
		mem.Start()
		<-mem.Ready.Chan
		vv("mem.Ready.Chan has closed") // seen.

		amCz := <-mem.debugAmCzarCh
		if !amCz {
			panicf("only 1 member, it should be czar")
		}
		vv("good: saw amCz = %v", amCz)
		mem.czar.Halt.RequestStop()
	})
}
