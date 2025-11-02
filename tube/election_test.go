package tube

import (
	"fmt"
	"time"

	rpc "github.com/glycerine/rpc25519"
	"testing"
)

// Tests that are here:
//
// Test020_election_on_sim_net:  Basic leader election in a 2-node cluster.
//
// Test040_election_then_first_noop:  Election followed by the
// leader committing its initial no-op entry.
//
// Test021_election_first_reqVote_becomes_leader:
// Forces a specific node (node 0) to start an election and become
// leader, then verifies heartbeats and the no-op commit on followers.
//
// Test022_election_timeouts_very_close_together: two nodes
// time out very close together, checking that a leader is
// still eventually elected.
//
// Test023_two_nodes_can_elect_leader: a 2-node cluster
// successfully elects leader.
//
// Test024_one_node_cannot_elect_leader: > 1 configured cluster size
// where a single node tries to elect itself and should fail if
// others are expected.

func Test020_election_on_sim_net(t *testing.T) {

	bubbleOrNot(t, func(t *testing.T) {
		const newStyle = true
		numNodes := 2
		if newStyle {
			/*
				// As in membership_test.go 401_add_node, the forced leader
				// gets a boot-strap config written as their
				// first log entry, by using setupTestCluster.
				// See implementation of setupTestCluster in
				// partition_test.go.

				forceLeader := -1
				cluster, leaderName, leadi, maxterm0 := setupTestCluster(t, numNodes, forceLeader)
				_, _, _, _ = cluster, leaderName, leadi, maxterm0
				defer cluster.Close()

				vv("confirming test setup ok... leaderName = '%v'", leaderName)
			*/

			cfg := NewTubeConfigTest(numNodes, t.Name(), faketime)

			cluster := NewCluster(t.Name(), cfg)
			defer cluster.Close()

			for _, node := range cluster.Nodes {
				// without this, the lack of memCfg in first
				// log entry means the nodes won't hold an
				// election at all, thinking some other
				// node has a config in their log and
				// thus is supposed to be first leader.
				node.resetElectionTimeout("election_test.go:65")
			}

			giveAllNodesInitialMemberConfig(cluster, true)

			cluster.Start() // needs to run at least part of main loop to build grid.

			g0 := time.Now()
			t0 := g0
			cluster.waitForConnectedGrid() // this is maybe the slowest part
			vv("%v grid established in %v, %v nodes, after %v", t.Name(), time.Since(g0), numNodes, time.Since(t0))

			if cluster.Cfg.UseSimNet {
				if cluster.Snap == nil {
					cluster.Snap = cfg.RpcCfg.GetSimnetSnapshotter()
					if cluster.Snap == nil {
						panic("grid is connected, why no snapshotter?")
					}
				}
				str := cluster.Snap.GetSimnetSnapshot()
				vv("simnet = '%v'", str)
			}

			// let the first noop get committed so we know the cluster is "up".
			leader, leadi, maxterm := cluster.waitForLeader(t0)
			vv("waitForLeader saw maxterm = %v when numnodes = %v", maxterm, numNodes)

			cluster.waitForLeaderNoop(t0)
			vv("good: noop committed, cluster size %v is up. leader='%v'; leaderi=%v; maxterm=%v", numNodes, leader, leadi, maxterm)

		} else { // old style, not working without a manual entry in the first node's log to bootstrap stuff...

			numNodes := 2
			cfg := NewTubeConfigTest(numNodes, t.Name(), faketime)
			c := NewCluster(t.Name(), cfg)

			t0 := time.Now()
			c.Start()
			defer c.Close()

			// time allowed to elect a leader, 10 full attempts should suffice.
			allowed := cfg.MinElectionDur * 20
			select {
			case leader := <-c.LeaderElectedCh:
				elap := time.Since(t0)
				vv("good: leader(%v) was elected after %v", leader, elap)
			case <-time.After(allowed):
				elap := time.Since(t0)
				vv("bad: NO leader was elected after %v", elap)
				panic("did not elect a leader")
			}
		}
	})
}

func Test040_election_then_first_noop(t *testing.T) {

	bubbleOrNot(t, func(t *testing.T) {
		numNodes := 2
		t0 := time.Now()
		// old style works no more? we need to designate leader with an initial config committed in their log. all other peers will hang with no config in their logs.
		// but this test is testing whether or not
		// a leader election can happen. So we don't
		// want to hang everyone. Give everyone an
		// initial config...
		cfg := NewTubeConfigTest(numNodes, t.Name(), faketime)
		c := NewCluster(t.Name(), cfg)

		// this is now incorporated into setupTestCluster
		// for when no forceLeader is requested.
		boot := c.Nodes[0].NewFirstRaftLogEntryBootstrap() // has a boot.Done chan this is closed once applied.
		boot.NewConfig.BootCount = numNodes
		boot.DoElection = true
		for _, node := range c.Nodes {
			boot.NewConfig.PeerNames.set(node.name, &PeerDetail{Name: node.name, URL: "boot.blank"})
		}

		for _, node := range c.Nodes {
			boot1 := boot.Clone()
			node.testBootstrapLogCh <- boot1
			//<-boot.Done // hung here b/c nodes not started
		}

		c.Start()

		defer c.Close()

		allowedElect := cfg.MinElectionDur * 20 // time allowed to elect a leader
		select {
		case leaderName := <-c.LeaderElectedCh:
			elap := time.Since(t0)
			vv("good: leader(%v) was elected after %v", leaderName, elap)
		case <-time.After(allowedElect):
			elap := time.Since(t0)
			panic(fmt.Sprintf("bad: NO leader was elected after %v", elap))
		}

		// get first no-op committed by leader

		allowedNoop := cfg.MinElectionDur * 5 // time allowed to get no-op committed
		select {
		case noop0leader := <-c.LeaderNoop0committedCh:
			elap := time.Since(t0)
			vv("good: leader first noop0 was committed after %v by %v", elap, noop0leader)
			//NO! racey! vv("noop0 ticket = %v", noop0tkt)
		case <-time.After(allowedNoop):
			elap := time.Since(t0)
			vv("bad: NO leader no-op was committed after %v", elap)
			panic("leader did not commit first noop0")
		}

	})
}

func Test021_election_first_reqVote_becomes_leader(t *testing.T) {

	// on the first election, the first candidate to
	// get their requestVotes to a quorum should become leader.
	bubbleOrNot(t, func(t *testing.T) {
		numNodes := 3
		cfg := NewTubeConfigTest(numNodes, t.Name(), faketime)
		c := NewCluster(t.Name(), cfg)

		// custom config here
		vv("asking node 0 to become candidate")
		c.NoInitialLeaderTimeout = true

		giveAllNodesInitialMemberConfig(c, true)

		c.Start() // needs to run at least part of main loop to build grid.
		defer c.Close()

		c.waitForConnectedGrid()
		t0 := time.Now()
		vv("grid established at %v", t0)

		wantLeader := c.Nodes[0].name
		panicAtCap(c.Nodes[0].testBeginElection) <- true

		allowed := cfg.MinElectionDur * 20 // time allowed to elect a leader
		select {
		case leader := <-c.LeaderElectedCh:
			vv("cluster c.LeaderElectedCh fired. leader=%v", leader)
			elap := time.Since(t0)
			look0 := c.Nodes[0].Inspect()
			if look0.Role != LEADER {
				t.Fatalf("error: expected node 0 to be leader (but is %v), leader is: '%v', term=%v", look0.Role, look0.CurrentLeaderName, look0.State.CurrentTerm)
			}
			vv("good: node 0 became leader; was elected after %v", elap)
		case <-time.After(allowed):
			elap := time.Since(t0)
			vv("bad: NO leader was elected after %v", elap)
			panic("did not elect a leader")
		}

		// confirm that other nodes see node 0 as leader;
		// that they get an AppendEntries (AE) from them.
		t1 := time.Now()

		for i := 1; i <= 2; i++ {
			select {
			case ae := <-c.Nodes[i].testGotAeFromLeader:
				obsLeader := ae.LeaderName
				if obsLeader != wantLeader {
					panic(fmt.Sprintf("obsLeader(%v) != wantLeader(%v)", obsLeader, wantLeader))
				}
				vv("good: node %v got AE from expected leader '%v' after %v", i, obsLeader, time.Since(t1))
			case <-time.After(2 * cfg.HeartbeatDur):
				elap := time.Since(t1)
				vv("bad: NO leader heartbeat in %v", elap)
				panic("node did not see leader hb")
			}
		}

		for i := 1; i <= 2; i++ {
			select {
			case lli := <-c.Nodes[i].testGotGoodAE_lastLogIndexNow:
				vv("good: node %v got leader's AE (new last log index:%v) after %v", i, lli, time.Since(t1))
				if lli != 1 {
					//panic(fmt.Sprintf("expected lli=1 from noop0, but got %v on node i=%v", lli, i))
				}
			case <-time.After(2 * cfg.HeartbeatDur):
				elap := time.Since(t1)
				vv("bad: NO leader heartbeat in %v", elap)
				panic("node did not see leader hb")
			}
		}

		// does the leader back aeAck from both followers?

		for i := 1; i <= 2; i++ {
			select {
			case ack := <-c.Nodes[0].testGotAeAckFromFollower:
				lli := ack.PeerLogLastIndex
				from := rpc.AliasDecode(ack.FromPeerID)
				vv("good: leader got ack from '%v' (new last log index:%v) after %v", from, lli, time.Since(t1))
				if lli != 1 {
					// we started sending heartbeats super early to
					// get the latest cluster membership distributed,
					// so lli == 0 occurs now. Hence we comment out
					// this long standing test:
					//	panic(fmt.Sprintf("expected lli==1 from noop0, got lli %v on node %v", lli, from))
				}
			case <-time.After(2 * cfg.HeartbeatDur):
				elap := time.Since(t1)
				vv("bad: NO leader heartbeat in %v", elap)
				panic("node did not see leader hb")
			}
		}

		// do we see the ticket noop0 commit?
		allowedNoop := cfg.HeartbeatDur * 3 // time allowed to get no-op committed
		select {
		case noop0leader := <-c.LeaderNoop0committedCh:
			elap := time.Since(t0)
			vv("good: leader first noop0 was committed after %v by %v", elap, noop0leader)
			//NO! data-racey! vv("noop0 ticket = %v", noop0tkt)
		case <-time.After(allowedNoop):
			elap := time.Since(t0)
			vv("bad: NO leader no-op was committed after %v", elap)
			panic("leader did not commit first noop0")
		}

	})
}

func Test022_election_timeouts_very_close_together(t *testing.T) {

	// if the initial election timeouts are very close on
	// 2 of 3 nodes then we don't want candidates voting
	// for each other, for sure. The next randomized
	// election timeout should resolve and elect a leader
	// in the next term.
	bubbleOrNot(t, func(t *testing.T) {
		numNodes := 3
		cfg := NewTubeConfigTest(numNodes, t.Name(), faketime)
		c := NewCluster(t.Name(), cfg)

		// custom config here
		vv("asking node 0 to become candidate -- but it might not always win!")
		c.NoInitialLeaderTimeout = true
		giveAllNodesInitialMemberConfig(c, false)

		c.Start() // needs to run at least part of main loop to build grid.
		defer c.Close()

		c.waitForConnectedGrid()
		t0 := time.Now()
		vv("grid established at %v", t0)

		panicAtCap(c.Nodes[0].testBeginElection) <- true

		// the hard case! other node times out too, about the same time.
		time.Sleep(time.Microsecond)
		panicAtCap(c.Nodes[1].testBeginElection) <- true

		// actually, with the new two-phase pre-vote,
		// it is not deterministic that the first node[0]
		// will win the election; don't assert that below.
		// Instead check the c.Name2num[leader] node.

		allowed := cfg.MinElectionDur * 20 // time allowed to elect a leader
		var leader string
		var leaderI int
		var ok bool
		select {
		case leader = <-c.LeaderElectedCh:
			vv("cluster c.LeaderElectedCh fired. leader=%v", leader)
			elap := time.Since(t0)
			leaderI, ok = c.Name2num[leader]
			if !ok {
				panic(fmt.Sprintf("unknown leader '%v'", leader))
			}
			lookL := c.Nodes[leaderI].Inspect()
			if lookL.Role != LEADER {
				t.Fatalf("error: expected node 0 to be leader (but is %v), leader is: '%v', term=%v", lookL.Role, lookL.CurrentLeaderName, lookL.State.CurrentTerm)
			}
			vv("good: node 0 became leader; was elected after %v", elap)
		case <-time.After(allowed):
			elap := time.Since(t0)
			vv("bad: NO leader was elected after %v", elap)
			panic("did not elect a leader")
		}

		// confirm that other nodes see node 0 as leader;
		// that they get an AppendEntries (AE) from them.
		t1 := time.Now()

		for i := 0; i < numNodes; i++ {
			if i == leaderI {
				continue
			}
			select {
			case ae := <-c.Nodes[i].testGotAeFromLeader:
				obsLeader := ae.LeaderName
				if obsLeader != leader {
					panic(fmt.Sprintf("obsLeader(%v) != leader(%v)", obsLeader, leader))
				}
				vv("good: node %v got AE from expected leader '%v' after %v", i, obsLeader, time.Since(t1))
			case <-time.After(2 * cfg.HeartbeatDur):
				elap := time.Since(t1)
				vv("bad: NO leader heartbeat in %v", elap)
				panic("node did not see leader hb")
			}
		}

		for i := 0; i < numNodes; i++ {
			if i == leaderI {
				continue
			}
			select {
			case lli := <-c.Nodes[i].testGotGoodAE_lastLogIndexNow:
				vv("good: node %v got leader's AE (new last log index:%v) after %v", i, lli, time.Since(t1))
				if lli != 1 {
					// we started sending heartbeats super early to
					// get the latest cluster membership distributed,
					// so lli == 0 occurs now. Hence we comment out
					// this long standing test:
					//panic(fmt.Sprintf("expected lli=1 from noop0, but got %v on node i=%v", lli, i))
				}
			case <-time.After(2 * cfg.HeartbeatDur):
				elap := time.Since(t1)
				vv("bad: NO leader heartbeat in %v", elap)
				panic("node did not see leader hb")
			}
		}

		// does the leader back aeAck from both followers?

		for i := 0; i < numNodes; i++ {
			if i != leaderI {
				continue
			}
			select {
			case ack := <-c.Nodes[leaderI].testGotAeAckFromFollower:
				lli := ack.PeerLogLastIndex
				from := rpc.AliasDecode(ack.FromPeerID)
				vv("good: leader got ack from '%v' (new last log index:%v) after %v", from, lli, time.Since(t1))
				if lli != 1 {
					//panic(fmt.Sprintf("expected lli==1 from noop0, got lli %v on node %v", lli, from))
				}
			case <-time.After(2 * cfg.HeartbeatDur):
				elap := time.Since(t1)
				vv("bad: NO leader heartbeat in %v", elap)
				panic("node did not see leader hb")
			}
		}

		// do we see the ticket noop0 commit?
		allowedNoop := cfg.HeartbeatDur * 3 // time allowed to get no-op committed
		select {
		case noop0leader := <-c.LeaderNoop0committedCh:
			elap := time.Since(t0)
			vv("good: leader first noop0 was committed after %v by %v", elap, noop0leader)
			//NO! data-racey! vv("noop0 ticket = %v", noop0tkt)
		case <-time.After(allowedNoop):
			elap := time.Since(t0)
			vv("bad: NO leader no-op was committed after %v", elap)
			panic("leader did not commit first noop0")
		}

	})
}

func Test023_two_nodes_can_elect_leader(t *testing.T) {

	bubbleOrNot(t, func(t *testing.T) {

		n := 2

		cfg := NewTubeConfigTest(n, t.Name(), globalUseSimnet)

		c := NewCluster(t.Name(), cfg)

		giveAllNodesInitialMemberConfig(c, true)
		c.Start()
		defer c.Close()

		nodes := c.Nodes

		t0 := time.Now()
		leader := -1
		haveLeader := false
		var leaderTerm int64
		limit := 100
		if !faketime {
			limit = 3
		}
	leaderElected:
		for i := range limit {

			// check both nodes have term 0 in follower state.
			look0 := nodes[0].Inspect()
			look1 := nodes[1].Inspect()
			switch {
			case look0.Role == LEADER && look1.Role == FOLLOWER:
				leader = 0
				haveLeader = true
				leaderTerm = look0.State.CurrentTerm
			case look1.Role == LEADER && look0.Role == FOLLOWER:
				leader = 1
				haveLeader = true
				leaderTerm = look1.State.CurrentTerm
			}
			if haveLeader {
				if look0.State.CurrentTerm == 0 {
					panic(fmt.Sprintf("i=%v; node 0, got zero CurrentTerm", i))
				}
				if look1.State.CurrentTerm == 0 {
					panic(fmt.Sprintf("i=%v; node 1, got zero CurrentTerm", i))
				}
				break leaderElected
			}
			//vv("i=%v, sleep %v", i, c.Cfg.MinElectionDur*2)
			time.Sleep(c.Cfg.MinElectionDur * 2)
		}
		vv("good: leader = %v after %v elections, two nodes got a leader, elap = %v", leader, leaderTerm, time.Since(t0))
	})
}

func Test024_one_node_cannot_elect_leader(t *testing.T) {

	//if !faketime {
	//	t.Skip("takes 12 seconds without faketime, even at N=3")
	//}

	bubbleOrNot(t, func(t *testing.T) {

		// it turns out that n = 2 but only one node
		// started is a strong test
		// of the raft leader election logic. We
		// should never get to elect a leader under
		// these circumstances, even if they run for
		// a long time. And, the only node's current terms should always
		// stay at 0, since pre-vote should always fail.
		n := 2
		// but then only ever bring up node 1

		cfg := NewTubeConfigTest(n, t.Name(), globalUseSimnet)
		cfg.deadOnStartCount = 1
		c := NewCluster(t.Name(), cfg)
		giveAllNodesInitialMemberConfig(c, true)
		c.Start()
		defer c.Close()

		nodes := c.Nodes

		var elections int
		electN := 100 // 2.6 seconds under faketime
		if !faketime {
			// otherwise 100 elections takes 400 sec ~ 6.6 minutes.
			electN = 3 // 3 * 4 = 12 seconds.
		}
		for i := range electN {

			// check the node 1 has term 0 in follower state.
			look := nodes[1].Inspect()
			if got, want := look.Role, FOLLOWER; got != want {
				panic(fmt.Sprintf("i=%v; node 1, expected FOLLOWER, we see '%v'", i, got))
			}
			if look.State.CurrentTerm != 0 {
				panic(fmt.Sprintf("i=%v; node 1, expected CurrentTerm of 0; we see '%v'", i, look.State.CurrentTerm))
			}
			elections = look.ElectionCount
			if elections > electN {
				break
			}
			dur := c.Cfg.MinElectionDur * 2
			//vv("sleep for one election cycle")
			time.Sleep(dur)
			//vv("check again")
		}
		vv("good: %v elections later, one node alone was not able to elect a leader", elections)
	})
}

// and send the boot config on <-testBootstrapLogCh to each.
func giveAllNodesInitialMemberConfig(cluster *TubeCluster, doElection bool) {
	numNodes := len(cluster.Nodes)
	boot := cluster.Nodes[0].NewFirstRaftLogEntryBootstrap() // has a boot.Done chan this is closed once applied.
	boot.NewConfig.BootCount = numNodes
	boot.DoElection = doElection
	for _, node := range cluster.Nodes {
		boot.NewConfig.PeerNames.set(node.name, &PeerDetail{Name: node.name, URL: "boot.blank"})
	}
	//cluster.Nodes[forceLeader].testBootstrapLogCh <- boot
	for _, node := range cluster.Nodes {
		boot1 := boot.Clone()
		node.testBootstrapLogCh <- boot1
	}
}
