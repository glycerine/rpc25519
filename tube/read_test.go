package tube

/*
import (
	//"bytes"
	"fmt"
	"math"
	"time"

	"testing"
)

func Test000_no_replicas_write_new_value(t *testing.T) {
	N := 1
	c := newCluster(N, nil)
	defer c.stop()

	// read empty should give back nothing
	v1, err := c.node[0].Read("a", 0)
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got '%v'", err)
	}
	if v1 != "" { // nil {
		t.Fatalf("expected nil value from empty store, got '%v'", string(v1))
	}

	//v := Val([]byte("123"))
	v := Val("123")
	err = c.node[0].Write("a", v, 0)
	panicOn(err)

	v2, err := c.node[0].Read("a", 0)
	panicOn(err) // key not found.

	//if !bytes.Equal(v, v2) {
	if v != v2 {
		t.Fatalf("write a:'%v' to node0, read back from node0 '%v'", string(v), string(v2))
	}
}

func Test001_arb_write_then_read_same_node_two_node_cluster(t *testing.T) {

	// even simpler, two nodes write to 0, read from 0.

	N := 2
	c := newCluster(N, nil)
	defer c.stop()

	// write to node 0
	v := Val([]byte("123"))
	err := c.node[0].Write("a", v, 0)
	panicOn(err)

	//vv("about to read from node 0")

	// read back from node 0
	v2, err := c.node[0].Read("a", 0)
	panicOn(err)

	//if !bytes.Equal(v, v2) {
	if v != v2 {
		t.Fatalf("write a:'%v' to node0, read back from node0 '%v'", string(v), string(v2))
	}
}

func Test002_arb_write_new_value(t *testing.T) {
	//N := 3 // works
	N := 2
	c := newCluster(N, nil)
	defer c.stop()

	// write to node 0
	v := Val([]byte("123"))
	err := c.node[0].Write("a", v, 0)
	panicOn(err)

	//vv("about to read from node 1")

	// but read back from node 1
	v2, err := c.node[1].Read("a", 0)
	panicOn(err)

	//if !bytes.Equal(v, v2) {
	if v != v2 {
		t.Fatalf("write a:'%v' to node0, read back from node1 '%v'", string(v), string(v2))
	}
}

func Test003_arb_write_new_value_two_replicas(t *testing.T) {
	N := 3 // number of nodes (primary + 2 replicas)
	c := newCluster(N, nil)
	defer c.stop()

	// write to node 0
	v := Val([]byte("123"))
	err := c.node[0].Write("a", v, 0)
	panicOn(err)

	//time.Sleep(time.Second)

	// but read back from node 1
	v2, err := c.node[1].Read("a", 0)
	panicOn(err)

	//if !bytes.Equal(v, v2) {
	if v != v2 {
		t.Fatalf("write a:'%v' to node0, read back from node1 '%v'", string(v), string(v2))
	}

	// read back from node 2 too
	v3, err := c.node[2].Read("a", 0)
	panicOn(err)

	//if !bytes.Equal(v, v3) {
	if v != v3 {
		t.Fatalf("write a:'%v' to node0, read back from node2 '%v'", string(v), string(v3))
	}
}

func Test004_arb_write_twice(t *testing.T) {
	N := 2 // number of nodes
	c := newCluster(N, nil)
	defer c.stop()

	// have to let the cluster come up, or else we
	// won't wait for the second node? Not now, because
	// the nodes know there replication degree, and
	// will wait until the INV have been sent.
	//time.Sleep(time.Second)

	// write to node 0
	v := Val([]byte("123"))
	err := c.node[0].Write("a", v, 0)
	panicOn(err)

	//time.Sleep(time.Second)

	// write to node 0 a new, updated value
	v2 := Val([]byte("45"))
	err = c.node[0].Write("a", v2, 0)
	panicOn(err)

	// there is no guarantee that the INV have
	// reached c.node[1] by this point, so wait a moment
	// before trying to read.
	//time.Sleep(time.Second)

	// but read back from node 1
	v3, err := c.node[1].Read("a", 0)
	panicOn(err)
	//vv("v3 = '%v'", string(v3))

	//if !bytes.Equal(v3, v2) {
	if v3 != v2 {
		t.Fatalf("error: 2nd write a:'%v' to node0, read back from node1 '%v'", string(v2), string(v3))
	}

	//vv("past first read from 1. about to read from 0.") // not seen with o3 and no nextWake.

	// confirm read from node0 too.
	v4, err := c.node[0].Read("a", 0)
	panicOn(err)
	//vv("v4 = '%v'", string(v4))

	//if !bytes.Equal(v4, v2) {
	if v4 != v2 {
		t.Fatalf("error: 2nd write a:'%v' to node0, read back from node0 '%v'", string(v2), string(v4))
	}

}

func Test005_arb_second_write_to_different_node(t *testing.T) {
	n := 2 // number of nodes
	c := newCluster(n, nil)
	defer c.stop()

	//time.Sleep(time.Second)

	// write to node 0
	v := Val([]byte("123"))
	err := c.node[0].Write("a", v, 0)
	panicOn(err)

	//time.Sleep(time.Second)

	// write to node 1 a new, updated value
	v2 := Val([]byte("45"))
	err = c.node[1].Write("a", v2, 0)
	panicOn(err)

	//time.Sleep(time.Second)

	// but read back from both nodes, node1
	v3, err := c.node[1].Read("a", 0)
	panicOn(err)
	//vv("v3 = '%v'", string(v3))

	//if !bytes.Equal(v3, v2) {
	if v3 != v2 {
		t.Fatalf("2nd write a:'%v' to node0, read back from node1 '%v'", string(v2), string(v3))
	}

	// but read back from both nodes, node0
	v4, err := c.node[0].Read("a", 0)
	panicOn(err)
	//vv("v4 = '%v'", string(v4))

	// if !bytes.Equal(v4, v2) {
	if v4 != v2 {
		t.Fatalf("2nd write a:'%v' to node0, read back from node0 '%v'", string(v2), string(v4))
	}

}

func Test006_arb_second_write_to_different_node_3_nodes(t *testing.T) {
	n := 3 // number of nodes
	c := newCluster(n, nil)
	defer c.stop()

	//time.Sleep(time.Second)

	// write to node 0
	v := Val([]byte("123"))
	err := c.node[0].Write("a", v, 0)
	panicOn(err)

	//time.Sleep(time.Second)

	// write to node 1 a new, updated value
	v2 := Val([]byte("45"))
	err = c.node[1].Write("a", v2, 0)
	panicOn(err)

	//time.Sleep(time.Second)

	// read back from all 3 nodes
	for i := range n {
		v3, err := c.node[i].Read("a", 0)
		panicOn(err)
		//vv("v3 = '%v'", string(v3))

		//if !bytes.Equal(v3, v2) {
		if v3 != v2 {
			t.Fatalf("error 2nd write a:'%v' but read back from node %v '%v'", string(v2), i, string(v3))
		}
	}

}

// if our replication has not finished, the read should
// pause and wait to return until it has a valid value.
func Test007_reads_should_wait_for_valid_value(t *testing.T) {
	n := 2
	c := newCluster(n, nil)
	defer c.stop()

	//time.Sleep(time.Second)

	// write to node 0
	v := Val([]byte("123"))
	err := c.node[0].Write("a", v, 0)
	panicOn(err)

	// but read back from node 1
	v2, err := c.node[1].Read("a", 0)
	panicOn(err)

	//if !bytes.Equal(v, v2) {
	if v != v2 {
		t.Fatalf("write a:'%v' to node0, read back from node1 '%v'", string(v), string(v2))
	}
}

// start testing failure scenarios: if the coordinator
// fails can the follower recover on its own by
// re-playing the INV delivered value after a timeout.
func Test008_coord_fails_before_VALIDATE_then_replay(t *testing.T) {
	n := 2
	c := newCluster(n, nil)
	defer c.stop()

	// write to node 0
	v := Val([]byte("123"))
	err := c.node[0].Write("a", v, 0)
	panicOn(err)

	// but read back from node 1; which should
	// timeout after 2 seconds and replay the write itself.
	t0 := time.Now()
	_ = t0
	v2, err := c.node[1].Read("a", 0)
	panicOn(err)
	//vv("recovery: node1 read finished after %v", time.Since(t0))

	//if !bytes.Equal(v, v2) {
	if v != v2 {
		t.Fatalf("write a:'%v' to node0, read back from node1 '%v'", string(v), string(v2))
	}
}

func Test009_follower_fails_does_not_ACK(t *testing.T) {
	n := 2
	c := newCluster(n, nil)
	defer c.stop()

	// write to node 0
	v := Val([]byte("123"))
	err := c.node[0].Write("a", v, 0)
	panicOn(err)

	// read back from node 0 which will not have had
	// its INV replied to with an ACK; it should
	// timeout after 2 seconds and unblock itself, finishing the read.
	t0 := time.Now()
	_ = t0
	v2, err := c.node[0].Read("a", 0)
	panicOn(err)
	//vv("recovery: node0 read finished after %v", time.Since(t0))

	//if !bytes.Equal(v, v2) {
	if v != v2 {
		t.Fatalf("write a:'%v' to node0, read back from node0 '%v'", string(v), string(v2))
	}
}

func assertEqf(a, b float64, t *testing.T) {
	if math.Abs(a-b) > 1e-8 {
		panic(fmt.Sprintf("a:%v != b:%v", a, b))
	}
}

func assert(b bool, t *testing.T) {
	if !b {
		panic(fmt.Sprintf("error assert not true"))
	}
}

// verify math operations to turn TS timestamps into float64
func Test012_TS_to_float64(t *testing.T) {

	N := 5
	c := newCluster(N, nil)
	defer c.stop()

	beg := make([]TS, N)
	f := make([]float64, N)
	delta := 1.0 / float64(N)
	for i := range N {
		beg[i] = TS{CoordID: fmt.Sprintf("node_%v", i)}
		f[i] = c.num(beg[i])
		assertEqf(f[i], float64(i)/float64(N), t)
		assertEqf(c.floor(beg[i]), 0, t)
		if i == 0 {
			assertEqf(c.ceil(beg[i]), 0, t)
		} else {
			if i == 1 {
				assertEqf(c.add(beg[i], beg[i-1]), delta, t)
			}
			assertEqf(c.ceil(beg[i]), 1, t)
			assertEqf(c.diff(beg[i], beg[i-1]), delta, t)
			assertEqf(c.diff(beg[i-1], beg[i]), -delta, t)
			assertEqf(c.dist(beg[i-1], beg[i]), delta, t)
		}
	}
	// increment Version
	for j := range 5 {
		jf := float64(j)
		for i := range N {
			beg[i].Version = int64(j)
			f[i] = c.num(beg[i])
			assertEqf(f[i], jf+(float64(i)/float64(N)), t)
			assertEqf(c.floor(beg[i]), jf, t)
			if i == 0 {
				assertEqf(c.ceil(beg[i]), jf, t)
			} else {
				assertEqf(c.ceil(beg[i]), jf+1, t)
			}
		}
	}
	// increment CommandNum
	for j := range 5 {
		jf := float64(j) * 1000
		for i := range N {
			beg[i].CommandNum = int64(j)
			f[i] = c.num(beg[i])
			assertEqf(f[i], jf+4+(float64(i)/float64(N)), t)
			assertEqf(c.floor(beg[i]), jf+4, t)
			if i == 0 {
				assertEqf(c.ceil(beg[i]), jf+4, t)
			} else {
				assertEqf(c.ceil(beg[i]), jf+5, t)
			}
		}
	}
}

// check that our highest promised is not going higher than necessary
// and thus causing extra conflicts.
// Because maybe the "highest seen" is really meant to be
// highest "promised", not highest seen.
func Test014_promised_is_minimal_required(t *testing.T) {

	N := 2
	c := newCluster(N, nil)
	defer c.stop()

	prom := make([]TS, N)
	seen := make([]TS, N)
	f := make([]float64, N)
	dist := make([]float64, N)
	delta := 1.0 / float64(N)

	// assert initial state of TS is sane, and in [0,1)
	for i := range N {
		prom[i], seen[i] = c.node[i].getHighestPromisedAndSeen()
		dist[i] = c.dist(prom[i], seen[i])
		f[i] = c.num(prom[i])
		// we start 0 apart, no gap
		assertEqf(dist[i], 0, t)
		// and we start between [0, 1)
		assertEqf(c.floor(prom[i]), 0, t)
		// strictly less than 1 to start with.
		assert(f[i] < 1, t)
		if i > 0 {
			assertEqf(c.dist(prom[i], prom[i-1]), delta, t)
			assertEqf(c.ceil(prom[i]), 1, t)
		} else {
			// i == 0
			assertEqf(c.ceil(prom[i]), 0, t)
		}
		vv("initially i=%v:\n prom[%v]='%v' -> %v\n seen[%v]='%v' -> %v\n", i, i, prom[i], f[i], i, seen[i], c.num(seen[i]))
	}

	// write to node 0
	v := Val([]byte("123"))
	err := c.node[0].Write("a", v, 0)
	panicOn(err)

	// can we say they should have advanced by at most 2? do we count
	// the number of nodeIDs (tie breaker names) in there?
	vv("wrote a:123 to node 0, about to read from node 1")

	for i := range N {
		prom[i], seen[i] = c.node[i].getHighestPromisedAndSeen()
		dist[i] = c.dist(prom[i], seen[i])
		f[i] = c.num(prom[i])
		vv("\n prom[%v]='%v' -> %v\n seen[%v]='%v' -> %v\n", i, prom[i], f[i], i, seen[i], c.num(seen[i]))
	}

	return
	// but read back from node 1
	v2, err := c.node[1].Read("a", 0)
	panicOn(err)

	//if !bytes.Equal(v, v2) {

	if v != v2 {
		t.Fatalf("write a:'%v' to node0, read back from node1 '%v'", string(v), string(v2))
	}
}
*/
