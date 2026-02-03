package tube

import (
	"fmt"
	"time"

	"testing"
)

// forceLeader < 0 will not force a leader.
// If we force a leader, we now use the bootstrap
// first log entry to do dynamic cluster init; to
// allow the chapter 4 membership reconfig to work/
// be tested.
func setupTestCluster(t *testing.T, numNodes, forceLeader, testNum int) (c *TubeCluster, leader string, leadi int, maxterm int64) {
	return setupTestClusterWithCustomConfig(nil, t, numNodes, forceLeader, testNum)
}

func setupTestClusterWithCustomConfig(cfg *TubeConfig, t *testing.T, numNodes, forceLeader, testNum int) (c *TubeCluster, leader string, leadi int, maxterm int64) {

	if cfg == nil {
		cfg = NewTubeConfigTest(numNodes, t.Name(), faketime)
	}
	cfg.testNum = testNum

	defer func() {

		for _, node := range c.Nodes {
			cfg.Node2Addr[node.name] = node.URL
		}

		vv("end of setupTestCluster")
	}()

	if forceLeader >= 0 {
		cfg.InitialLeaderName = fmt.Sprintf("node_%v", forceLeader)
	}
	// singleDesignatedLeader := false
	// if numNodes == 1 && forceLeader == 0 {
	// 	singleDesignatedLeader = true
	// 	// this should cause node_0 to become leader immediately.
	// 	cfg.InitialLeaderName = "node_0"
	// }
	c = NewCluster(t.Name(), cfg)

	if forceLeader >= 0 {
		vv("setupTestCluster('%v'): asking node %v to become candidate", t.Name(), forceLeader)
		c.NoInitialLeaderTimeout = true

		//if !singleDesignatedLeader {
		// so the problem here is that if we start the nodes,
		// they won't have their MemberConfig listing their
		// PeerIDs. But if we don't start the nodes, the
		// PeerIDs won't even exist. Starting the node
		// creates the peerID, and the client could
		// be listening on any port.
		boot := c.Nodes[forceLeader].NewFirstRaftLogEntryBootstrap()
		// but its really the size that counts, right?
		// so we give the leader an initial log entry
		// with the short names as keys and boot.blank values
		// so they can later be replaced. That should allow
		// the leader to win any election it participates
		// in initially, plus we start its election first below;
		// and the others have no election timeout b/c of
		// c.NoInitialLeaderTimeout = true above.
		boot.NewConfig.BootCount = numNodes
		for _, node := range c.Nodes {
			boot.NewConfig.PeerNames.Set(node.name, &PeerDetail{Name: node.name, URL: "boot.blank"})
		}
		//c.Nodes[forceLeader].testBootstrapLogCh <- boot
		c.BootMC = boot.NewConfig

		// test from Start() rather than needing loop going:
		// inject an actual log. Sets MC from boot.NewConfig.Clone()
		c.Nodes[forceLeader].testSetupFirstRaftLogEntryBootstrapLog(boot)
		vv("back from testSetupFirstRaftLogEntryBootstrapLog(boot); c.Nodes[forceLeader].state.CurrentTerm = %v", c.Nodes[forceLeader].state.CurrentTerm)

		// for the rest, make an empty membership config (MC)
		// and get it into their state and log so we
		// don't crash when they send us nothing in the
		// first 401 pre-vote.
		for i, node := range c.Nodes {
			if i == forceLeader {
				continue
			}
			emptyMC := &FirstRaftLogEntryBootstrap{
				NewConfig: node.NewMemberConfig("setupTestCluster"),
				Done:      make(chan struct{}),
			}
			node.testSetupFirstRaftLogEntryBootstrapLog(emptyMC)
		}

		// so the first WAL log entry is now setup for sure.
		panicAtCap(c.Nodes[forceLeader].testBeginElection) <- true
		//}
	} else {
		// give *all* nodes an initial config, so we
		// can test the election process on startup.

		// Note that FirstRaftLogEntryBootstrap
		// has a boot.Done chan that is closed once applied.
		// But boot will not be processed/applied until
		// after Start() is called on the node, which c.Start()
		// does below.
		boot := c.Nodes[0].NewFirstRaftLogEntryBootstrap()
		boot.NewConfig.BootCount = numNodes
		boot.DoElection = true
		for _, node := range c.Nodes {
			boot.NewConfig.PeerNames.Set(node.name, &PeerDetail{Name: node.name, URL: "boot.blank"})

		}
		for _, node := range c.Nodes {
			boot1 := boot.Clone()
			// bufferred 1 so should not hang.
			node.testBootstrapLogCh <- boot1
			// Don't do this; as it
			// will hang here b/c c.Start() not called yet.
			//<-boot.Done
		}
		c.BootMC = boot.NewConfig
	}

	c.Start() // needs to run at least part of main loop to build grid.

	g0 := time.Now()
	t0 := g0
	if numNodes > 1 {
		c.waitForConnectedGrid() // this is maybe the slowest part
	}
	vv("%v grid established in %v, %v nodes, after %v", t.Name(), time.Since(g0), numNodes, time.Since(t0))

	if false {
		if c.Cfg.UseSimNet {
			if c.Snap == nil {
				c.Snap = cfg.RpcCfg.GetSimnetSnapshotter()
				if c.Snap == nil {
					panic("grid is connected, why no snapshotter?")
				}
			}
			str := c.Snap.GetSimnetSnapshot(false)
			vv("simnet = '%v'", str)
		}
	}

	// let the first noop get committed so we know the cluster is "up".
	leader, leadi, maxterm = c.waitForLeader(t0)
	vv("waitForLeader saw maxterm = %v when numnodes = %v; leadi='%v'; leader='%v'", maxterm, numNodes, leadi, leader)

	if forceLeader >= 0 && leadi != forceLeader {
		panic(fmt.Sprintf("arg. asked for leader %v, but got %v", forceLeader, leadi))
	} else {
		vv("good: forceLeader=%v and leadi=%v", forceLeader, leadi)
	}

	c.waitForLeaderNoop(t0)
	vv("good: noop committed, cluster size %v is up", numNodes)

	// assert that each node actually has cktReplica and cktAllByName
	// filled in correctly.
	nodes := c.Nodes
	nNode := len(nodes)

	//vv("top waitForConnectedGrid(); nNode = %v", nNode)
	//defer vv("end waitForConnectedGrid()")

	for _, node := range c.Nodes {
		insp := node.Inspect()
		n := len(insp.CktReplicaByName)
		if n != nNode {
			//vv("insp on node '%v' = %v'", node.name, insp)
			panic(fmt.Sprintf("node '%v' has %v CktReplicaByName, not %v as required: %v", node.name, n, nNode, insp.CktReplicaByName))
		} else {
			vv("setupTestCluster good: node '%v' has expected CktReplicaByName count of %v: %v", node.name, n, insp.CktReplicaByName)
		}
		if insp.Role == LEADER {
			if len(insp.Peers) != nNode {
				panic(fmt.Sprintf("why does leader not have full Peers info (only %v out of %v)? '%v'", len(insp.Peers), nNode, insp.Peers))
			}
			for peerID, info := range insp.Peers {
				_ = peerID
				if info.PeerName == node.name {
					// skip ourselves as leader we don't track our own liveness
					continue
				}
				if info.LastHeardAnything.IsZero() {
					panic(fmt.Sprintf("info.LastHeardAnything should be filled in for peer name '%v': '%#v'", info.PeerName, info))
				}
				lag := time.Since(info.LastHeardAnything)
				lim := 2 * node.maxElectionTimeoutDur()
				if lag > lim {
					panic(fmt.Sprintf("on leader node '%v': peer '%v' had lag(%v) > 2*node.maxElectionTimeoutDur()=%v; should never happen in a healthy cluster; info='%v'", node.name, info.PeerName, lag, lim, info))
				} else {
					vv("good: leader sees s.peer '%v' with lag='%v'", info.PeerName, lag)
				}
			}
		}
	}
	return
}

func testClusterGetCurrentLeader(c *TubeCluster) (leadi int, haveLeader bool, leadURL string) {

	for i, node := range c.Nodes {
		insp := node.Inspect()
		if insp.Role == LEADER {
			haveLeader = true
			leadi = i
			leadURL = insp.CurrentLeaderURL
			return
		}
	}
	return
}
