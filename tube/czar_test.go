package tube

import (
	//"bytes"
	//"context"
	"fmt"
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
		vv("leader is '%v' at url = '%v'", leader, leaderURL)

		startOneMember := func(num int, cfg *TubeConfig) *RMember {

			cliName := fmt.Sprintf("member_test808_%v", num)

			cliCfg := *cfg

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

			vv("at num=%v, cliName = '%v'; cliCfg.Node2Addr = '%#v'", num, cliName, cliCfg.Node2Addr)

			mem := NewRMember(tableSpace, &cliCfg)
			mem.Start()
			<-mem.Ready.Chan
			vv("mem.Ready.Chan has closed num = %v", num) // seen 0 only.
			return mem
		}

		var mems []*RMember
		for i := range 5 {
			vv("top i = %v mem loop", i)
			mem := startOneMember(i, c.Cfg)
			mems = append(mems, mem)

			vv("about to wait for mem.debugAmCzarCh on i = %v", i) // i = 0 only seen.

			amCz := <-mem.debugAmCzarCh
			if i == 0 {
				if !amCz {
					panicf("0th (first) member should be czar")
				}
			} else {
				if amCz {
					panicf("only 0-th (first) member should be czar, not i=%v too", i)
				}
			}
			vv("good: at i=%v, saw amCz = %v", i, amCz)
		}
		for _, mem := range mems {
			mem.czar.Halt.RequestStop()
			<-mem.czar.Halt.Done.Chan
		}
	})
}
