package tube

import (
	"fmt"
	"reflect"
	"time"

	"testing"
)

// forceLeader < 0 will not force a leader.
// If we force a leader, we now use the bootstrap
// first log entry to do dynamic cluster init; to
// allow the chapter 4 membership reconfig to work/
// be tested.
func SetupTestCluster(t *testing.T, numNodes, forceLeader, testNum int) (c *TubeCluster, leader string, leadi int, maxterm int64) {
	return SetupTestClusterWithCustomConfig(nil, t, numNodes, forceLeader, testNum)
}

// SetupTestClusterWithCustomConfig is exported so hermes/ testing can use it too.
func SetupTestClusterWithCustomConfig(cfg *TubeConfig, t *testing.T, numNodes, forceLeader, testNum int) (c *TubeCluster, leader string, leadi int, maxterm int64) {

	if cfg == nil {
		cfg = NewTubeConfigTest(numNodes, t.Name(), faketime)
	}
	cfg.testNum = testNum

	defer func() {

		for _, node := range c.Nodes {
			cfg.Node2Addr[node.name] = node.URL
		}

		//vv("end of setupTestCluster")
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
		//vv("setupTestCluster('%v'): asking node %v to become candidate", t.Name(), forceLeader)
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
		//vv("back from testSetupFirstRaftLogEntryBootstrapLog(boot); c.Nodes[forceLeader].state.CurrentTerm = %v", c.Nodes[forceLeader].state.CurrentTerm)

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
		c.WaitForConnectedGrid() // this is maybe the slowest part
	}
	//vv("%v grid established in %v, %v nodes, after %v", t.Name(), time.Since(g0), numNodes, time.Since(t0))

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
	leader, leadi, maxterm = c.WaitForLeader(t0)
	//vv("waitForLeader saw maxterm = %v when numnodes = %v; leadi='%v'; leader='%v'", maxterm, numNodes, leadi, leader)

	if forceLeader >= 0 && leadi != forceLeader {
		panic(fmt.Sprintf("arg. asked for leader %v, but got %v", forceLeader, leadi))
	} else {
		//vv("good: forceLeader=%v and leadi=%v", forceLeader, leadi)
	}

	c.WaitForLeaderNoop(t0)
	//vv("good: noop committed, cluster size %v is up", numNodes)

	// assert that each node actually has cktReplica and cktAllByName
	// filled in correctly.
	nodes := c.Nodes
	nNode := len(nodes)

	//vv("top WaitForConnectedGrid(); nNode = %v", nNode)
	//defer vv("end WaitForConnectedGrid()")

	for _, node := range c.Nodes {
		insp := node.Inspect()
		n := len(insp.CktReplicaByName)
		if n != nNode {
			//vv("insp on node '%v' = %v'", node.name, insp)
			panic(fmt.Sprintf("node '%v' has %v CktReplicaByName, not %v as required: %v", node.name, n, nNode, insp.CktReplicaByName))
		} else {
			//vv("setupTestCluster good: node '%v' has expected CktReplicaByName count of %v: %v", node.name, n, insp.CktReplicaByName)
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
					//vv("good: leader sees s.peer '%v' with lag='%v'", info.PeerName, lag)
				}
			}
		}
	}
	return
}

func InTestClusterGetCurrentLeader(c *TubeCluster) (leadi int, haveLeader bool, leadURL string) {

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

// WaitForConnectedGrid waits until all n*(n-1)
// circuit endpoints have been reported before returning.
// We verify that the first ckts established
// are to replicas, not clients.
func (c *TubeCluster) WaitForConnectedGrid() (replicaCktCount int) {
	nodes := c.Nodes
	nNode := len(nodes)

	//vv("top WaitForConnectedGrid(); nNode = %v", nNode)
	//defer vv("end WaitForConnectedGrid()")

	for i, g := range nodes {
		_ = i
		select { // 031 hung intermit here
		case <-g.verifyPeersNeededSeen.Chan:
			//vv("i=%v all peer connections need have been seen(%v) by node '%v': '%#v'", i, g.verifyPeersNeeded, g.name, g.verifyPeersSeen.GetKeySlice()) // data race read vs prev write at tube.go:7267

			// failing test will just hang above.
			// we cannot really do case <-time.After(time.Minute) with faketime.
		case cktP := <-g.verifyPeerReplicaOrNot:
			replicaCktCount++
			//vv("grid connection seen from (%v)=='%v': cktP.isReplica = %v for ckt='%#v'", rpc.AliasDecode(cktP.ckt.RemotePeerID), cktP.ckt.RemotePeerID, cktP.isReplica, cktP.ckt)
			if !cktP.isReplica() {
				panic(fmt.Sprintf("all circuits during cluster setup should be replicas; this was not: '%#v'; cktP.ckt.CircuitID = '%v'", cktP, cktP.ckt.CircuitID))
			}
		}
	}
	// get them all if we did not above.
	var cases []reflect.SelectCase
	for k, g := range nodes {
		_ = k
		//vv("adding select case %v: '%v' (%v)", k, g.name, g.PeerID)
		cases = append(cases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(g.verifyPeerReplicaOrNot),
		})
	}
	for replicaCktCount < nNode*(nNode-1) {
		//vv("top of for, replicaCktCount = %v", replicaCktCount)
		chosenCase, recv, recvOK := reflect.Select(cases)
		_ = chosenCase
		if recvOK {
			cktP := recv.Interface().(*cktPlus)
			replicaCktCount++
			//vv("node=chosenCase=%v; cktP.isReplica = %v for ckt='%#v'", chosenCase, cktP.isReplica, cktP.ckt)
			if !cktP.isReplica() {
				panic(fmt.Sprintf("all circuits during cluster setup should be replicas; this was not: '%#v'; cktP.ckt.CircuitID = '%v'", cktP, cktP.ckt.CircuitID))
			}
		}
	}
	return
}

func (c *TubeCluster) WaitForLeader(t0 time.Time) (leader string, leadi int, maxterm int64) {

	// verify terms are strictly monotonically increasing, per node
	node2term := make(map[string]*testTermChange)

	cfg := c.Cfg
	numNodes := cfg.ClusterSize
	choose2 := numNodes * (numNodes - 1) / 2
	if choose2 == 0 {
		choose2 = 1 // handle single node case
	}
	allowed := time.Duration(choose2) * cfg.MinElectionDur * 10 // time allowed to elect a leader
	timeout := time.After(allowed)

elected:
	for {
		select {
		case u := <-c.termChanges:
			//vv("%v cluster sees member term change: '%#v'", numNodes, u)
			maxterm = max(maxterm, u.newterm)

			// self consistent
			if u.oldterm >= u.newterm {
				panic(fmt.Sprintf("safety violation, term did not increase on node '%v': old='%v'; new='%v'", u.peerID, u.oldterm, u.newterm))
			}
			// and change to change consistent
			old, ok := node2term[u.peerID]
			if ok {
				if old.newterm >= u.newterm {
					panic(fmt.Sprintf("safety violation, term did not increase on node '%v': old='%v'; new='%v'", u.peerID, old.newterm, u.newterm))
				}
			} else {
				// first one for this peer
				node2term[u.peerID] = u
			}

		case leader = <-c.LeaderElectedCh:
			w, ok := c.Name2num[leader]
			if !ok {
				panic(fmt.Sprintf("no node number for leader '%v'", leader))
			}
			leadi = w
			//vv("cluster c.LeaderElectedCh fired. leader=%v; node number=%v'", leader, w)

			elap := time.Since(t0)
			_ = elap
			//alwaysPrintf("good: clusterSize = %v; node w=%v (%v) won election in %v", numNodes, w, leader, elap.Truncate(time.Millisecond))

			// give them time to depose other candidates with
			// their first round of heartbeats.
			time.Sleep(cfg.HeartbeatDur * 3)

			term := int64(-1)
			for i := range c.Nodes {
				look := c.Nodes[i].Inspect()
				roleExpect := FOLLOWER
				if i == w {
					roleExpect = LEADER
				}
				if look.Role != roleExpect {
					panic(fmt.Sprintf("error: expected node %v to be %v (but is %v) in term %v", i, roleExpect, look.Role, look.State.CurrentTerm)) // CANDIDATE seen... size 8 is rough! 015_tube_non_parallel_linz (tube_test.go) red under realtime without synctest (might be sporadic): error: expected node 0 to be LEADER (but is FOLLOWER) in term 2
				}
				if i == 0 {
					term = look.State.CurrentTerm
				} else {
					if look.State.CurrentTerm != term {
						panic(fmt.Sprintf("error: inconsistent terms. expected node %v to also be at term %v, but is at %v", i, term, look.State.CurrentTerm))
					}
				}
			}
			break elected
		case <-timeout:
			elap := time.Since(t0)
			panic(fmt.Sprintf("bad: no leader elected, in %v node cluster, after %v", numNodes, elap)) // bad: no leader elected, in 8 node cluster, after 1m20.003295173s
		}
	}
	//c.Close()
	//time.Sleep(3 * cfg.MinElectionDur)
	//time.Sleep(time.Second)
	return
}

// get first no-op committed by leader
func (c *TubeCluster) WaitForLeaderNoop(t0 time.Time) {

	cfg := c.Cfg
	allowedNoop := cfg.MinElectionDur * 5 // time allowed to get no-op committed
	select {
	case noop0leader := <-c.LeaderNoop0committedCh:
		_ = noop0leader
		elap := time.Since(t0)
		_ = elap
		//vv("good: leader first noop0 was committed after %v by %v", elap, noop0leader)
		//NO! racey! vv("noop0 ticket = %v", noop0tkt)
	case <-time.After(allowedNoop):
		elap := time.Since(t0)
		vv("bad: NO leader no-op was committed after %v", elap)
		panic("leader did not commit first noop0")
	}
}
