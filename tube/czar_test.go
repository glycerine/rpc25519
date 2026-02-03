package tube

import (
	//"bytes"
	"context"
	"fmt"
	"time"

	"testing"
)

var _ = context.Background

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

		N := 5
		mems := make([]*RMember, N)
		pings := make([]*PingReply, N)
		_ = pings
		for i := range N {
			vv("top i = %v mem loop", i)
			mem := testStartOneMember(t, i, c.Cfg)
			mems[i] = mem

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

		// stop 3
		for i, mem := range mems {
			if i <= 2 {
				mem.czar.Halt.RequestStop()
				<-mem.czar.Halt.Done.Chan
			}
		}

		if true { // without this, the czar change over works.
			// change leader
			c.Nodes[leadi].Halt.ReqStop.Close()
			<-c.Nodes[leadi].Halt.Done.Chan
			vv("====================== we asked to halt leadi= %v ; its url='%v'", leadi, c.Nodes[leadi].URL)
		}

		vv("sleep 120 sec after leader crash")
		time.Sleep(time.Second * 120)
		vv("has been 120 sec after leader crash")

		// confirm a leader has been elected; this is a pre-requisite.
		leadi2, haveLeader, leadURL := testClusterGetCurrentLeader(c)
		if !haveLeader {
			panic("must have elected a new leader by now.")
		}
		vv("good: new leader is '%v' at '%v'", leadi2, leadURL)

		// but the problem is that the squady members do not know how
		// to find the new leader.

		// confirm LeaseEpoch has advanced: inspect
		//var err error
		//pings[3], err = mems[3].czar.inspect(context.Background())
		//panicOn(err)
		//pings[4], err = mems[4].czar.inspect(context.Background())
		//panicOn(err)

		numCzar := 0
		cur := czarState(mems[3].czar.cState.Load())
		vv("cur[3] = %v", cur)
		if cur == amCzar {
			numCzar++
		}
		cur = czarState(mems[4].czar.cState.Load())
		vv("cur[4] = %v", cur)
		if cur == amCzar {
			numCzar++
		}
		if numCzar != 1 {
			panicf("wanted 1 czar, got %v", numCzar)
		}

		vv("begin shutdown / cleanup: shut down other 2")
		for i, mem := range mems {
			if i > 2 {
				mem.czar.Halt.RequestStop()
				<-mem.czar.Halt.Done.Chan
			}
		}
	})
}
