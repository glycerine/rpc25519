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

		var mems []*RMember
		for i := range 5 {
			vv("top i = %v mem loop", i)
			mem := testStartOneMember(t, i, c.Cfg)
			mems = append(mems, mem)

			vv("about to wait for mem.debugAmCzarCh on i = %v", i) // i = 0 only seen.

			amCz := <-mem.testingAmCzarCh
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

func testStartOneMember(t *testing.T, num int, cfg *TubeConfig) *RMember {

	cliName := fmt.Sprintf("%v_%v", t.Name(), num)

	cliCfg := *cfg

	// setupTestCluster should have:
	//cliCfg.RpcCfg.QuietTestMode = false
	//cliCfg.UseSimNet = true
	//cliCfg.isTest = true

	cliCfg.MyName = cliName
	cliCfg.PeerServiceName = TUBE_CLIENT
	cliCfg.ClockDriftBound = 500 * time.Millisecond
	tableSpace := t.Name()

	vv("at num=%v, cliName = '%v'; cliCfg.Node2Addr = '%#v'", num, cliName, cliCfg.Node2Addr)

	mem := NewRMember(tableSpace, &cliCfg)
	mem.Start()
	<-mem.Ready.Chan
	vv("mem.Ready.Chan has closed for = %v", cliName)
	return mem
}

func Test809_lease_epoch_monotone_after_leader_change(t *testing.T) {

	bubbleOrNot(t, func(t *testing.T) {

		numNodes := 3
		forceLeader := 0
		c, leader, leadi, _ := setupTestCluster(t, numNodes, forceLeader, 809)
		defer c.Close()
		_, _ = leader, leadi

		// nodes := c.Nodes
		_ = leader
		leaderNode := c.Nodes[leadi]
		leaderURL := leaderNode.URL
		vv("leader is '%v' at url = '%v'", leader, leaderURL)

		var mems []*RMember
		for i := range 5 {
			vv("top i = %v mem loop", i)
			mem := testStartOneMember(t, i, c.Cfg)
			mems = append(mems, mem)

			vv("about to wait for mem.debugAmCzarCh on i = %v", i) // i = 0 only seen.

			amCz := <-mem.testingAmCzarCh
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
		// assert cts that lease epoch advances monotically.

		// change czar a couple of times to advance the lease epoch

		// change leader
		c.Nodes[leadi].Halt.ReqStop.Close()
		<-c.Nodes[leadi].Halt.Done.Chan

		for _, mem := range mems {
			mem.czar.Halt.RequestStop()
			<-mem.czar.Halt.Done.Chan
		}
	})
}
