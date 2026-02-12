package hermes

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"testing"

	"github.com/glycerine/rpc25519/tube"
)

type HermesCluster struct {
	Cfg   *HermesConfig
	Nodes []*HermesNode
}

func NewHermesCluster(cfg *HermesConfig, nodes []*HermesNode) *HermesCluster {
	return &HermesCluster{Cfg: cfg, Nodes: nodes}
}

func (s *HermesCluster) Start() {
	for i, n := range s.Nodes {
		_ = i
		err := n.Init()
		panicOn(err)
	}
	time.Sleep(time.Second)
	// now that they are all started, form a complete mesh
	// by connecting each to all the others
	sz := len(s.Nodes)
	for i, n0 := range s.Nodes {
		for j, n1 := range s.Nodes {
			if j <= i || i == sz-1 {
				continue
			}
			// tell a fresh client to connect to server and then pass the
			// conn to the existing server.

			firstFrag := n0.newFrag()
			// firstFrag.FragOp = 0 // leave 0
			firstFrag.ToPeerName = n1.name
			firstFrag.SetUserArg("fromPeerName", n0.name)
			firstFrag.SetUserArg("toPeerName", n1.name)
			remotePeerName := n1.name
			var userString string
			var errWriteDur time.Duration

			//vv("about to connect i=%v to j=%v", i, j)
			ckt, _, _, err := n0.MyPeer.NewCircuitToPeerURL("hermes-ckt", n1.URL, firstFrag, errWriteDur, userString, remotePeerName)
			panicOn(err)
			// must manually tell the service goro about the new ckt in this case.
			n0.MyPeer.NewCircuitCh <- ckt
			vv("created ckt between n0 '%v' and n1 '%v'.", n0.name, n1.name) //, ckt.String())
		}
	}
}

func (s *HermesCluster) Close() {
	for _, n := range s.Nodes {
		n.Close()
	}
}

func Test010_TS_Compare_is_fair(t *testing.T) {
	//return // green but the fairness version is off atm.

	// check that our blake3 hashing gives fair version comparison
	N := int64(100_000)

	awin := 0
	var a, b TS
	record0 := make([]byte, N)
	record1 := make([]byte, N)
	a.CoordID = "blah1"
	b.CoordID = "blah2"
	for i := range N {
		a.Version = i
		b.Version = i
		if a.Compare(&b) > 0 {
			awin++
			record0[i] = 1
		}
	}
	// confirm repeatable/deterministic.
	for i := range N {
		a.Version = i
		b.Version = i
		if a.Compare(&b) > 0 {
			record1[i] = 1
		}
	}
	pct := 100 * float64(awin) / float64(N)
	vv("Coordinator A won %0.1f %% of the time. N = %v", pct, N)
	if pct > 55 || pct < 45 {
		t.Fatalf("exptected pct %v to be closer to 50", pct)
	}
	if bytes.Compare(record0, record1) != 0 {
		t.Fatalf("error: TS.Compare() must be deterministic.")
	}
}

func newHermesTestCluster(cfg *HermesConfig) *HermesCluster {
	n := cfg.ReplicationDegree

	//cfg.MemOnly = true

	dataDir, err := tube.GetServerDataDir()
	panicOn(err)
	os.RemoveAll(filepath.Join(dataDir, "hermes_pebble_test"))

	var nodes []*HermesNode
	for i := range n {
		name := fmt.Sprintf("hermes_node_%v", i)
		node := NewHermesNode(name, cfg)

		// fake operating lease for now, since tests were developed
		// before and we don't want to bring up a full tube raft cluster
		// to test them at the moment.
		node.operLeaseUntilTm = time.Now().Add(10 * time.Minute)

		nodes = append(nodes, node)
	}
	return NewHermesCluster(cfg, nodes)
}

const memOnlyTests = true

func Test001_no_replicas_write_new_value(t *testing.T) {
	n := 1
	cfg := &HermesConfig{
		MemOnly:            memOnlyTests,
		ReplicationDegree:  n,
		MessageLossTimeout: time.Second * 5,
		TCPonly_no_TLS:     true,
		testName:           t.Name(),
	}
	c := newHermesTestCluster(cfg)
	nodes := c.Nodes
	c.Start()
	defer c.Close()

	// read empty should give back nothing
	v1, err := nodes[0].Read("a", 0)
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got '%v'", err)
	}
	if v1 != nil {
		t.Fatalf("expected nil value from empty store, got '%v'", string(v1))
	}

	v := []byte("123")
	err = nodes[0].Write("a", v, 0)
	panicOn(err)

	v2, err := nodes[0].Read("a", 0)
	panicOn(err)

	if !bytes.Equal(v, v2) {
		t.Fatalf("write a:'%v' to node0, read back from node0 '%v'", string(v), string(v2))
	}
}

func Test002_hermes_write_new_value(t *testing.T) {
	n := 2
	cfg := &HermesConfig{
		MemOnly:            memOnlyTests,
		ReplicationDegree:  n,
		MessageLossTimeout: time.Second * 5,
		TCPonly_no_TLS:     true,
		testName:           t.Name(),
	}
	c := newHermesTestCluster(cfg)
	nodes := c.Nodes
	c.Start()
	defer c.Close()

	// write to node 0
	v := []byte("123")
	err := nodes[0].Write("a", v, 0)
	panicOn(err)
	vv("good 1: back from nodes[0].Write a -> 123")

	// but read back from node 1
	v2, err := nodes[1].Read("a", 0)
	panicOn(err)

	if !bytes.Equal(v, v2) {
		t.Fatalf("write a:'%v' to node0, read back from node0 '%v'", string(v), string(v2))
	}
	vv("good 2: back from nodes[1].Read a -> 123")

}

func Test003_hermes_write_new_value_two_replicas(t *testing.T) {

	n := 3 // number of nodes (primary + 2 replicas)
	cfg := &HermesConfig{
		MemOnly:            memOnlyTests,
		ReplicationDegree:  n,
		MessageLossTimeout: time.Second * 5,
		TCPonly_no_TLS:     true,
		testName:           t.Name(),
	}
	c := newHermesTestCluster(cfg)
	nodes := c.Nodes
	c.Start()
	defer c.Close()

	//time.Sleep(time.Second)

	// write to node 0
	v := []byte("123")
	err := nodes[0].Write("a", v, 0)
	panicOn(err)
	vv("good 1: back from nodes[0].Write a -> 123") // not seen

	//time.Sleep(time.Second)

	// but read back from node 1
	v2, err := nodes[1].Read("a", 0)
	panicOn(err)

	if !bytes.Equal(v, v2) {
		t.Fatalf("write a:'%v' to node0, read back from node1 '%v'", string(v), string(v2))
	}
	vv("good 2: back from nodes[1].Read a -> 123")

	// read back from node 2 too
	v3, err := nodes[2].Read("a", 0)
	panicOn(err)

	if !bytes.Equal(v, v3) {
		t.Fatalf("write a:'%v' to node0, read back from node2 '%v'", string(v), string(v3))
	}
	vv("good 3: back from nodes[2].Read a -> 123")

}

func Test004_hermes_write_twice(t *testing.T) {
	n := 2 // number of nodes
	cfg := &HermesConfig{
		MemOnly:            memOnlyTests,
		ReplicationDegree:  n,
		MessageLossTimeout: time.Second * 5,
		TCPonly_no_TLS:     true,
		testName:           t.Name(),
	}
	c := newHermesTestCluster(cfg)
	nodes := c.Nodes
	c.Start()
	defer c.Close()

	// have to let the cluster come up, or else we
	// won't wait for the second node? Not needed now, because
	// the nodes know their replication degree, and
	// will wait until the INV have been sent.
	//time.Sleep(time.Second)

	// write to node 0
	v := []byte("123")
	err := nodes[0].Write("a", v, 0)
	panicOn(err)

	// write to node 0 a new, updated value
	v2 := []byte("45")
	err = nodes[0].Write("a", v2, 0)
	panicOn(err)

	// there is no guarantee that the INV have
	// reached nodes[1] by this point, so wait a moment
	// before trying to read.
	//time.Sleep(time.Second)

	// but read back from node 1
	v3, err := nodes[1].Read("a", 0)
	panicOn(err)
	//vv("v3 = '%v'", string(v3))

	if !bytes.Equal(v3, v2) {
		t.Fatalf("error: 2nd write a:'%v' to node0, read back from node1 '%v'", string(v2), string(v3))
	}

	vv("past first read from 1. about to read from 0.") // not seen with o3 and no nextWake.

	// confirm read from node0 too.
	v4, err := nodes[0].Read("a", 0)
	panicOn(err)
	vv("v4 = '%v'", string(v4))

	if !bytes.Equal(v4, v2) {
		t.Fatalf("error: 2nd write a:'%v' to node0, read back from node0 '%v'", string(v2), string(v4))
	}

}

func Test005_hermes_second_write_to_different_node(t *testing.T) {
	n := 2 // number of nodes
	cfg := &HermesConfig{
		MemOnly:            memOnlyTests,
		ReplicationDegree:  n,
		MessageLossTimeout: time.Second * 5,
		TCPonly_no_TLS:     true,
		testName:           t.Name(),
	}
	c := newHermesTestCluster(cfg)
	nodes := c.Nodes
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

}

func Test006_hermes_second_write_to_different_node_3_nodes(t *testing.T) {

	n := 3 // number of nodes
	cfg := &HermesConfig{
		MemOnly:            memOnlyTests,
		ReplicationDegree:  n,
		MessageLossTimeout: time.Second * 5,
		TCPonly_no_TLS:     true,
		testName:           t.Name(),
	}
	c := newHermesTestCluster(cfg)
	nodes := c.Nodes
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
		vv("v3 = '%v'", string(v3))

		if !bytes.Equal(v3, v2) {
			t.Fatalf("error 2nd write a:'%v' but read back from node %v '%v'", string(v2), i, string(v3))
		}
	}

}

// if our replication has not finished, the read should
// pause and wait to return until it has a valid value.
func Test007_reads_should_wait_for_valid_value(t *testing.T) {
	n := 2
	cfg := &HermesConfig{
		MemOnly:            memOnlyTests,
		ReplicationDegree:  n,
		MessageLossTimeout: time.Second * 5,
		TCPonly_no_TLS:     true,
		testName:           t.Name(),
	}
	c := newHermesTestCluster(cfg)
	nodes := c.Nodes
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
}

// start testing failure scenarios: if the coordinator
// fails can the follower recover on its own by
// re-playing the INV delivered value after a timeout.
func Test008_coord_fails_before_VALIDATE_then_replay(t *testing.T) {
	n := 2

	orig := useBcastAckOptimization
	useBcastAckOptimization = false // otherwise VAL omitted altogether! (and our test tests nothing).
	defer func() {
		useBcastAckOptimization = orig
	}()
	cfg := &HermesConfig{
		MemOnly:            memOnlyTests,
		ReplicationDegree:  n,
		MessageLossTimeout: time.Second * 2,
		TCPonly_no_TLS:     true,

		testScenario: map[string]bool{"ignore VALIDATE": true},
		testName:     t.Name(),
	}
	c := newHermesTestCluster(cfg)
	nodes := c.Nodes
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
}

func Test009_follower_fails_does_not_ACK(t *testing.T) {

	n := 2
	cfg := &HermesConfig{
		MemOnly:            memOnlyTests,
		ReplicationDegree:  n,
		MessageLossTimeout: time.Second * 2,
		TCPonly_no_TLS:     true,

		testScenario: map[string]bool{"ignore ACK": true},
		testName:     t.Name(),
	}
	c := newHermesTestCluster(cfg)
	nodes := c.Nodes
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
}
