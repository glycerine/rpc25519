package tube

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	//_ "net/http/pprof" // for web based profiling while running
	"testing"
)

func init() {
	return
	addr := "127.0.0.1:9999"
	fmt.Printf("webprofile starting at '%v'...\n", addr)
	go func() {
		http.ListenAndServe(addr, nil)
	}()
}

var bkg = context.Background()

func Test000_election_timeout_dur(t *testing.T) {

	T := 100 * time.Millisecond
	s := &TubeNode{}
	s.cfg = TubeConfig{
		isTest:          true,
		PeerServiceName: TUBE_REPLICA,
		MinElectionDur:  T,
		HeartbeatDur:    T / 10,
	}
	var min time.Duration = time.Hour
	var max time.Duration
	n := 1_000_000
	for range n {
		x := s.electionTimeoutDur()
		if x < T || x > 2*T {
			panic(fmt.Sprintf("electionTimeoutDur %v is out of bounds of [T, 2T] with T = %v", x, T))
		}
		if x > max {
			max = x
		}
		if x < min {
			min = x
		}
	}
	if min > time.Duration(float64(T)*1.1) || max < time.Duration(float64(T)*1.9) {
		panic(fmt.Sprintf("something is off: min=%v, max=%v, T=%v", min, max, T))
	}
	//vv("min = %v, max = %v", min, max)
}

/*

func Test005_tube_second_write_to_different_node(t *testing.T) {
	bubbleOrNot(func() {

	n := 2 // number of nodes
	cfg := NewTubeConfig(n, t.Name(), globalUseSynctest)

	var nodes []*TubeNode
	for i := range n {
		name := fmt.Sprintf("tube_node_%v", i)
		nodes = append(nodes, NewTubeNode(name, name, cfg))
	}
	c := NewTubeCluster(cfg, nodes)
	c.Start()
	defer c.Close()

	//time.Sleep(time.Second)

	// write to node 0
	v := []byte("123")
	err := nodes[0].Write("a", v, 0)
	panicOn(err)

	//time.Sleep(time.Second)

	// write to node 1 a new, updated value
	v2 := []byte("45")
	err = nodes[1].Write("a", v2, 0)
	panicOn(err)

	//time.Sleep(time.Second)

	// but read back from both nodes, node1
	v3, err := nodes[1].Read("a", 0)
	panicOn(err)
	//vv("v3 = '%v'", string(v3))

	if !bytes.Equal(v3, v2) {
		t.Fatalf("2nd write a:'%v' to node0, read back from node1 '%v'", string(v2), string(v3))
	}

	// but read back from both nodes, node0
	v4, err := nodes[0].Read("a", 0)
	panicOn(err)
	vv("v4 = '%v'", string(v4))

	if !bytes.Equal(v4, v2) {
		t.Fatalf("2nd write a:'%v' to node0, read back from node0 '%v'", string(v2), string(v4))
	}
})
}

func Test006_tube_second_write_to_different_node_3_nodes(t *testing.T) {
	bubbleOrNot(func() {

	n := 3 // number of nodes
		cfg := NewTubeConfig(n, t.Name(), globalUseSynctest)

	var nodes []*TubeNode
	for i := range n {
		name := fmt.Sprintf("tube_node_%v", i)
		nodes = append(nodes, NewTubeNode(name, name, cfg))
	}
	c := NewTubeCluster(cfg, nodes)
	c.Start()
	defer c.Close()

	//time.Sleep(time.Second)

	// write to node 0
	v := []byte("123")
	err := nodes[0].Write("a", v, 0)
	panicOn(err)

	//time.Sleep(time.Second)

	// write to node 1 a new, updated value
	v2 := []byte("45")
	err = nodes[1].Write("a", v2, 0)
	panicOn(err)

	//time.Sleep(time.Second)

	// read back from all 3 nodes
	for i := range n {
		v3, err := nodes[i].Read("a", 0)
		panicOn(err)
		//vv("v3 = '%v'", string(v3))

		if !bytes.Equal(v3, v2) {
			t.Fatalf("error 2nd write a:'%v' but read back from node %v '%v'", string(v2), i, string(v3))
		}
	}
})
}

// if our replication has not finished, the read should
// pause and wait to return until it has a valid value.
func Test007_reads_should_wait_for_valid_value(t *testing.T) {
	bubbleOrNot(func() {

	n := 2
		cfg := NewTubeConfig(n, t.Name(), globalUseSynctest)

	var nodes []*TubeNode
	for i := range n {
		name := fmt.Sprintf("tube_node_%v", i)
		nodes = append(nodes, NewTubeNode(name, name, cfg))
	}
	c := NewTubeCluster(cfg, nodes)
	c.Start()
	defer c.Close()

	//time.Sleep(time.Second)

	// write to node 0
	v := []byte("123")
	err := nodes[0].Write("a", v, 0)
	panicOn(err)

	// but read back from node 1
	v2, err := nodes[1].Read("a", 0)
	panicOn(err)

	if !bytes.Equal(v, v2) {
		t.Fatalf("write a:'%v' to node0, read back from node1 '%v'", string(v), string(v2))
	}
})
}

// start testing failure scenarios: if the coordinator
// fails can the follower recover on its own by
// re-playing the INV delivered value after a timeout.
func Test008_coord_fails_before_VALIDATE_then_replay(t *testing.T) {
	bubbleOrNot(func() {

	n := 2
		cfg := NewTubeConfig(n, t.Name(), globalUseSynctest)

	var nodes []*TubeNode
	for i := range n {
		name := fmt.Sprintf("tube_node_%v", i)
		nodes = append(nodes, NewTubeNode(name, name, cfg))
	}
	c := NewTubeCluster(cfg, nodes)
	c.Start()
	defer c.Close()

	// write to node 0
	v := []byte("123")
	err := nodes[0].Write("a", v, 0)
	panicOn(err)

	// but read back from node 1; which should
	// timeout after 2 seconds and replay the write itself.
	t0 := time.Now()
	v2, err := nodes[1].Read("a", 0)
	panicOn(err)
	vv("recovery: node1 read finished after %v", time.Since(t0))

	if !bytes.Equal(v, v2) {
		t.Fatalf("write a:'%v' to node0, read back from node1 '%v'", string(v), string(v2))
	}
})
}

func Test009_follower_fails_does_not_ACK(t *testing.T) {
	bubbleOrNot(func() {

	n := 2
	cfg := NewTubeConfig(n, t.Name(), globalUseSynctest)

	var nodes []*TubeNode
	for i := range n {
		name := fmt.Sprintf("tube_node_%v", i)
		nodes = append(nodes, NewTubeNode(name, name, cfg))
	}
	c := NewTubeCluster(cfg, nodes)
	c.Start()
	defer c.Close()

	// write to node 0
	v := []byte("123")
	err := nodes[0].Write("a", v, 0)
	panicOn(err)

	// read back from node 0 which will not have had
	// its INV replied to with an ACK; it should
	// timeout after 2 seconds and unblock itself, finishing the read.
	t0 := time.Now()
	v2, err := nodes[0].Read("a", 0)
	panicOn(err)
	vv("recovery: node0 read finished after %v", time.Since(t0))

	if !bytes.Equal(v, v2) {
		t.Fatalf("write a:'%v' to node0, read back from node0 '%v'", string(v), string(v2))
	}
})
}
*/

// tests basd on history!
// Section 8.1, page 113
//
// "It is often necessary in the proof to refer to variables from prior states in the execution. To make this precise, the specification is augmented with history variables; these variables carry information about past events forward to states that follow. For example, one history variable called elections maintains a record of every successful election in the execution, including the complete log of each server at the time it cast its vote and the complete log of the new leader. The history variables are never read in the specification and would not exist at all in a real implementation; they are only “accessed” in the proof.

// We think a machine-checked proof for Raft would be feasible with more capable tools (e.g, Coq [7]), and one has recently been created for Multi-Paxos [101]. p115
// SCHIPER, N., RAHLI, V., VAN RENESSE, R., BICKFORD, M., AND CONSTABLE, R. L. Developing correctly replicated databases using formal tools. In Proc. DSN’14, IEEE/IFIP International Conference on Dependable Systems and Networks (2014). 115, 168, 169

// Howard describes a nice design for building ocaml-raft correctly [37, 36]. It collects all the Raft state transitions in one module, while all code for determining when transitions should occur is elsewhere. Each transition checks its pre-conditions using assertions and has no system-level code intermixed, so the code resembles the core of the Raft specification. Because all of the code that manipulates the state variables is collected in one place, it is easier to verify that state variables transition in restricted ways. A separate module invokes the transitions at the appropriate times. Moreover, ocaml-raft can simulate an entire cluster in a single process, which allows it to assert Raft’s invariants across virtual servers during execution. For example, it can check that there is at most one leader per term at runtime.

// For end-to-end testing, Jepsen and Knossos are useful tools that have already found bugs in two Raft implementations (in read-only request handling) [45].

// p115 - p116
// Some of the most difficult to find bugs are those that only occur in unlikely circumstances such
// as during leadership changes or partial network outages. Thus, testing should aim to increase the likelihood of such events. There are three ways to do this.
// First, Raft servers can be configured to encourage rare events for testing. For example, setting the election timeout very low and the heartbeat interval very high will result in more leader changes. Also, having servers take snapshots very frequently will result in more servers falling behind and needing to receive a snapshot over the network.
//	Second, the environment can be manipulated to encourage rare events for testing. For example, servers can be randomly restarted and cluster membership changes can be invoked frequently (or continuously) to exercise those code paths. Starting other processes to contend for servers’ resources may expose timing-related bugs, and the network can be manipulated in various ways to create events that occur only rarely in production, such as:
// • Randomlydroppingmessages(andvaryingthefrequencyofdropsbetweenserversandlinks); • Adding random message delays;
// • Randomly disabling and restoring network links; and
//• Randomly partitioning the network.
//	Third, running the tests for a longer period of time will increase the chance of discovering a rare problem. A larger number of machines can run tests in parallel. Moreover, entire clusters can run as separate processes on a single server to reduce network latency, and disk overheads can be reduced by persisting to RAM only (for example, with a RAM-based file system such as tmpfs [64]). While not entirely realistic, these techniques can exercise the implementation aggressively in a much shorter period of time.

func Test600_local_read_criteria(t *testing.T) {

	// we should be allowing some leader local reads through,
	// when the proper safety conditions are met:
	// 1) pre-vote is on; requires 2 servers minimum;
	// 2) the leader has committed noop0 at the
	// start of its term (which always happens in our implementation);
	// 3)
	//
	// Pre-vote on a majority of the cluster alone
	// gives the leader a strong guarantee that there
	// can be no other leader for the next T minimum
	// election timeout period.
	// A newer leader would have had to obtain
	// pre-votes from a majority of the cluster. Because
	// the pre-vote is denied if each node has heard
	// from the leader within T,
	// they will never pre-vote yes to any other
	// possible leader while in regular contact with
	// the existing leader.
	//
	// 3) within a minimum election timeout period T,
	// the leader has obtained a quorum on round-trip
	// AE/Ack from followers that confirm
	//   a) the leader's current term: this means they
	//      have the current term, and their election
	//      timeout has been reset at a point > send of AE.
	//      The most we can assume is the send time
	//      of an acknowledged AE (AppendEntries).
	//      Hence we should never allow a local read
	//      if AE send time + T has elapsed, or simply
	//      Acked AE send time + min election timer > now.
	//   b)
}

func Test601_client_session_serial_contiguous(t *testing.T) {
	// all serial numbers in a client session must
	// be contiguous. If there is a gap, the leader
	// must kill the session and force the client
	// to start a new session, so that they realize
	// the gap (lost message from client before it
	// arrived at the leader), and so the client re-tries.
	//
	// Test here that the client gets an ErrSerialGapInSession
	// back if it skips a Serial number in its tickets
	// for the same session.

}

const globalUseSimnet = true

func Test001_no_replicas_write_new_value(t *testing.T) {

	bubbleOrNot(t, func(t *testing.T) {

		n := 1
		cfg := NewTubeConfigTest(n, t.Name(), globalUseSimnet)
		cfg.testNum = 1

		c := NewCluster(t.Name(), cfg)
		nodes := c.Nodes

		c.Start()
		defer c.Close()

		// TODO add back in once writes are working.
		//if false {
		if true {
			// read of an empty log should give back nothing
			tkt, err := nodes[0].Read(bkg, "", "a", 0, nil)
			v1 := tkt.Val
			if err != ErrKeyNotFound {
				t.Fatalf("expected ErrKeyNotFound, got '%v'", err)
			}
			if v1 != nil {
				t.Fatalf("expected nil value from empty store, got '%v'", string(v1))
			}
		}

		var v []byte
		for i := range 10 {
			//i := 1
			//v := []byte("123")
			v = []byte(fmt.Sprintf("%v", i))
			//vv("about to write '%v'", string(v))
			tkt, err := nodes[0].Write(bkg, "", "a", v, 0, nil, "", 0, leaseAutoDelFalse)
			panicOn(err)
			_ = tkt
		}
		// there should be no waiting tickets left now.
		pect := nodes[0].Inspect()
		nWait := len(pect.WaitingAtLeader)
		if got, want := nWait, 0; got != want {
			t.Fatalf("should be nothing waiting; we see '%#v'", pect.WaitingAtLeader)
		}
		vv("good: nothing Waiting; nWait = %v", nWait)

		//}
		// TODO add back in once writes are working.
		// if false {
		tkt, err := nodes[0].Read(bkg, "", "a", 0, nil)
		panicOn(err)
		v2 := tkt.Val

		if !bytes.Equal(v, v2) {
			t.Fatalf("write a:'%v' to node0, read back from node0 '%v'", string(v), string(v2))
		}

		// v2, err = nodes[0].Read("a", 0)
		// panicOn(err)

		// if !bytes.Equal(v, v2) {
		// 	t.Fatalf("write a:'%v' to node0, read back from node0 '%v'", string(v), string(v2))
		// }
		// }
	})
}

func Benchmark_101_no_replicas_write_new_value(b *testing.B) {

	// used to be bubbleOrNot(func(), and benchmarks don't have T,
	// only B; so we cannot just do:
	//bubbleOrNot(t, func(t *testing.T) {

	n := 1
	cfg := NewTubeConfigTest(n, b.Name(), globalUseSimnet)

	c := NewCluster(b.Name(), cfg)
	nodes := c.Nodes

	c.Start()
	defer c.Close()

	// TODO add back in once writes are working.
	if true {
		// read empty should give back nothing
		tkt, err := nodes[0].Read(bkg, "", "a", 0, nil)
		v1 := tkt.Val
		if err != ErrKeyNotFound {
			b.Fatalf("expected ErrKeyNotFound, got '%v'", err)
		}
		if v1 != nil {
			b.Fatalf("expected nil value from empty store, got '%v'", string(v1))
		}
	}

	var v []byte
	i := 0
	for b.Loop() {
		i++
		//i := 1
		//v := []byte("123")
		v = []byte(fmt.Sprintf("%v", i))
		//vv("about to write '%v'", string(v))
		tkt, err := nodes[0].Write(bkg, "", "a", v, 0, nil, "", 0, leaseAutoDelFalse)
		_ = tkt
		panicOn(err)
	}
	vv("end of b.Loop i = %v", i)
	// there should be no waiting tickets left now.
	pect := nodes[0].Inspect()
	nWait := len(pect.WaitingAtLeader)
	if got, want := nWait, 0; got != want {
		b.Fatalf("should be nothing waiting; we see '%#v'", pect.WaitingAtLeader)
	}
	vv("good: nothing WaitingAtLeader; nWait = %v", nWait)

	//}
	// TODO add back in once writes are working.
	// if false {
	tkt, err := nodes[0].Read(bkg, "", "a", 0, nil)
	v2 := tkt.Val
	panicOn(err)

	if !bytes.Equal(v, v2) {
		b.Fatalf("write a:'%v' to node0, read back from node0 '%v'", string(v), string(v2))
	}
	//})
}

// moved to write_test.go for the moment
/*
func Test002_tube_write_new_value(t *testing.T) {

	bubbleOrNot(t, func(t *testing.T) {

		n := 2
		//n := 3
		cfg := NewTubeConfigTest(n, t.Name(), globalUseSimnet)

		var nodes []*TubeNode
		for i := range n {
			name := fmt.Sprintf("tube_node_%v", i)
			nodes = append(nodes, NewTubeNode(name, name, cfg))
		}
		// note: must share the same rpcCfg to get the same simnet!
		c := NewTubeCluster(cfg, nodes)
		c.Start()
		defer c.Close()

		var v []byte
		i := 0
		N := 1
		for range N {
			i++

			// Write
			v = []byte(fmt.Sprintf("%v", i))
			//vv("about to write '%v'", string(v))
			err := nodes[0].Write("a", v, 0)
			panicOn(err)

			// Read all nodes
			for j := range n {
				v2, err := nodes[j].Read("a", 0)
				panicOn(err)

				if !bytes.Equal(v, v2) {
					t.Fatalf("write a:'%v' to node0, read back from node0 '%v'", string(v), string(v2))
				}
			}
		}

		vv("end of t.Loop i = %v", i)

		// there should be no waiting tickets left now.
		pect := nodes[0].Inspect()
		nWait := len(pect.WaitingAtLeader)
		if got, want := nWait, 0; got != want {
			t.Fatalf("should be nothing waiting; we see '%#v'", pect.WaitingAtLeader)
		}
		vv("good: nothing WaitingAtLeader; nWait = %v", nWait)

			// // write to node 0
			// v := []byte("123")
			// err := nodes[0].Write("a", v, 0)
			// panicOn(err)

			// // but read back from node 1
			// v2, err := nodes[1].Read("a", 0)
			// panicOn(err)
			// vv("Read returned v2 = '%v'", string(v2))
			// if !bytes.Equal(v, v2) {
			// 	t.Fatalf("write a:'%v' to node0, read back from node1 '%v'", string(v), string(v2))
			// }
	})
}
*/

func Test003_tube_3_node_write_then_read(t *testing.T) {

	bubbleOrNot(t, func(t *testing.T) {

		defer func() {
			vv("test 003 wrapping up.")
		}()
		numNodes := 3

		c, _, _, _ := setupTestCluster(t, numNodes, -1, 3)
		//cfg := NewTubeConfigTest(n, t.Name(), globalUseSimnet)
		//c := NewCluster(t.Name(), cfg)
		//c.Start()
		defer c.Close()

		nodes := c.Nodes

		var v []byte
		i := 0
		N := 10
		for range N {
			i++

			// Write
			v = []byte(fmt.Sprintf("%v", i))
			//vv("about to write '%v'", string(v))
			tktW, err := nodes[0].Write(bkg, "", "a", v, 0, nil, "", 0, leaseAutoDelFalse)
			panicOn(err)
			_ = tktW

			// Read all nodes
			for j := range numNodes {
				tkt, err := nodes[j].Read(bkg, "", "a", 0, nil)
				panicOn(err)
				v2 := tkt.Val

				if !bytes.Equal(v, v2) {
					t.Fatalf("write a:'%v' to node0, read back from node0 '%v'", string(v), string(v2))
				}
				//vv("003test done with read j = %v", j)
			}
		}

		// verify that the initial-nop got committed, and
		// that the logs have the expected term
		// sequence 1=noop, i=write a=i up to N

		vv("003 end of t.Loop i = %v", i)

		// there should be no waiting tickets left now.
		pect := nodes[0].Inspect()
		nWait := len(pect.WaitingAtLeader)
		if got, want := nWait, 0; got != want {
			t.Fatalf("should be nothing waiting; we see '%#v'", pect.WaitingAtLeader)
		}
		vv("003 good: nothing WaitingAtLeader; nWait = %v", nWait)

		/*
			// write to node 0
			v := []byte("123")
			err := nodes[0].Write("a", v, 0)
			panicOn(err)

			// but read back from node 1
			v2, err := nodes[1].Read("a", 0)
			panicOn(err)
			vv("Read returned v2 = '%v'", string(v2))
			if !bytes.Equal(v, v2) {
				t.Fatalf("write a:'%v' to node0, read back from node1 '%v'", string(v), string(v2))
			}
		*/
	})
}

func Test010_tube_write_new_value_two_replicas(t *testing.T) {
	bubbleOrNot(t, func(t *testing.T) {

		numNodes := 3 // number of nodes (primary + 2 replicas)

		c, _, _, _ := setupTestCluster(t, numNodes, -1, 10)
		//cfg := NewTubeConfigTest(n, t.Name(), globalUseSimnet)
		//c := NewCluster(t.Name(), cfg)
		//c.Start()
		defer c.Close()

		nodes := c.Nodes

		//time.Sleep(time.Second)

		// write to node 0
		v := []byte("123")
		tkt0, err := nodes[0].Write(bkg, "", "a", v, 0, nil, "", 0, leaseAutoDelFalse)
		panicOn(err)
		_ = tkt0

		//time.Sleep(time.Second)

		// but read back from node 1
		tkt1, err := nodes[1].Read(bkg, "", "a", 0, nil)
		panicOn(err)
		v1 := tkt1.Val

		if !bytes.Equal(v, v1) {
			t.Fatalf("write a:'%v' to node0, read back from node1 '%v'", string(v), string(v1))
		}

		// read back from node 2 too
		tkt2, err := nodes[2].Read(bkg, "", "a", 0, nil)
		panicOn(err)
		v2 := tkt2.Val

		if !bytes.Equal(v, v2) {
			t.Fatalf("write a:'%v' to node0, read back from node2 '%v'", string(v), string(v2))
		}
	})
}

// non-parallel version, see 016 in porc_test.go for parallelized.
func Test015_tube_non_parallel_linz(t *testing.T) {

	// Annoying: non-bubbled can get long
	// delays between heartbeats and thus elect a new
	// leader and thus violate the assertions herein
	// that the leader has not changed.
	// So keep to bubble-only for now.
	//bubbleOrNot(t, func(t *testing.T) {
	onlyBubbled(t, func(t *testing.T) {

		defer func() {
			vv("test 015 wrapping up.")
		}()
		numNodes := 5

		c, _, _, _ := setupTestCluster(t, numNodes, -1, 15)
		//cfg := NewTubeConfigTest(n, t.Name(), globalUseSimnet)
		//c := NewCluster(t.Name(), cfg)
		//c.Start()
		defer c.Close()

		nodes := c.Nodes

		var v []byte
		i := 0
		N := 100
		for range N {
			i++

			// Write
			v = []byte(fmt.Sprintf("%v", i))
			//vv("about to write '%v'", string(v))
			tktW, err := nodes[0].Write(bkg, "", "a", v, 0, nil, "", 0, leaseAutoDelFalse)
			panicOn(err)
			_ = tktW

			// Read all nodes in parallel
			for jnode := range numNodes {
				//go func(i, jnode int) {
				tkt, err := nodes[jnode].Read(bkg, "", "a", 0, nil)
				panicOn(err)
				v2 := tkt.Val
				i2, err := strconv.Atoi(string(v2))
				panicOn(err)
				if i2 != i {
					t.Fatalf("write a:'%v' to node 0, read back from node %v a different value: '%v'", i, jnode, i2)
				}
				//vv("015test done with read j = %v", j)
				//}(i, jnode)
			}
		}

		// verify that the initial-nop got committed, and
		// that the logs have the expected term
		// sequence 1=noop, i=write a=i up to N

		vv("015 end of t.Loop i = %v", i)

		// there should be no waiting tickets left now.
		pect := nodes[0].Inspect()
		nWait := len(pect.WaitingAtLeader)
		if got, want := nWait, 0; got != want {
			t.Fatalf("should be nothing waiting; we see '%#v'", pect.WaitingAtLeader)
		}

		vv("015 good: nothing WaitingAtLeader; nWait = %v", nWait)
	})
}

// simple write benchmark, real network on
// single machine (darwin), not faketime:
// N=1000 in 380.562263ms => 2627.690912170133 writes/sec cluster size n=3
// N=1000 in 419.581581ms => 2383.3267361657613 writes/sec cluster size n=4
// N=1000 in 520.596616ms => 1920.8730315680732 writes/sec cluster size n=5
// N=1000 in 574.153127ms => 1741.6956435038278 writes/sec cluster size n=7
// N=1000 in 771.408714ms => 1296.3296652622464 writes/sec cluster size n=8
// N=1000 in 866.312148ms => 1154.3183393060303 writes/sec cluster size n=9
func Test017_write_throughput(t *testing.T) {
	return
	bubbleOrNot(t, func(t *testing.T) {

		defer func() {
			vv("test 017 wrapping up.")
		}()

		minClusterSz := 3
		maxClusterSz := 4
		for numNodes := minClusterSz; numNodes < maxClusterSz; numNodes++ {

			forceLeader := -1 // -1 => no forced leader
			c, leader, leadi, maxterm := setupTestCluster(t, numNodes, forceLeader, 17)
			_, _, _ = leader, leadi, maxterm

			// n := 3
			// cfg := NewTubeConfigTest(n, t.Name(), globalUseSimnet)

			// c := NewCluster(t.Name(), cfg)
			// c.Start()
			//defer c.Close()

			nodes := c.Nodes
			n := numNodes

			N := 1000
			vv("begin %v writes...", N)
			t0 := time.Now()
			var v []byte
			i := 0
			for range N {
				i++

				// Write
				v = []byte(fmt.Sprintf("%v", i))
				//vv("about to write '%v'", string(v))
				tktW, err := nodes[0].Write(bkg, "", "a", v, 0, nil, "", 0, leaseAutoDelFalse)
				panicOn(err)
				_ = tktW

				if false {
					// Read all nodes in parallel
					for jnode := range n {
						//go func(i, jnode int) {
						tkt, err := nodes[jnode].Read(bkg, "", "a", 0, nil)
						panicOn(err)
						v2 := tkt.Val
						i2, err := strconv.Atoi(string(v2))
						panicOn(err)
						if i2 != i {
							t.Fatalf("write a:'%v' to node 0, read back from node %v a different value: '%v'", i, jnode, i2)
						}
						//vv("017test done with read j = %v", j)
						//}(i, jnode)
					}
				}
			}
			elap := time.Since(t0)
			vv("N=%v in %v => %v writes/sec cluster size n=%v", N, elap, float64(N)/(float64(elap)/1e9), n)

			// verify that the initial-nop got committed, and
			// that the logs have the expected term
			// sequence 1=noop, i=write a=i up to N

			vv("017 end of t.Loop i = %v", i)

			// there should be no waiting tickets left now.
			pect := nodes[0].Inspect()
			nWait := len(pect.WaitingAtLeader)
			if got, want := nWait, 0; got != want {
				t.Fatalf("should be nothing waiting; we see '%#v'", pect.WaitingAtLeader)
			}

			vv("017 good: nothing WaitingAtLeader; nWait = %v", nWait)

			c.Close()
		}
	})
}
