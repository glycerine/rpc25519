package tube

/*
import (
	//"bytes"
	"fmt"
	//"time"

	"testing"
)


// some larger cluster checks
func Test010_larger_cluster(t *testing.T) {

	cfg := &RaftConfig{
		AutoRetry: 10,
		NoDisk:    true,
	}

	N := 4 // > 33 we hang.
	for n := 3; n < N; n++ {
		vv("n= %v", n)
		c := newCluster(n, cfg)

		//time.Sleep(time.Second)

		// write to node 0
		v := Val([]byte("123"))
		err := c.node[0].Write("a", v, 0)
		panicOn(err)

		vv("done with write to node 0 of a:123")

		// but read back from all nodes
		var got []string
		var report string
		for i := range n {
			v2, err := c.node[i].Read("a", 0)
			panicOn(err)
			got = append(got, string(v2))
			if v != v2 {
				//t.Fatalf("write a:'%v' to node0, read back from node(i=%v) '%v'; nodes gave so far: '%#v'", string(v), i, string(v2), got)
				report += fmt.Sprintf("write a:'%v' to node0, read back from node(i=%v) '%v'; nodes gave so far: '%#v'; kv='%#v'; N=%v\n", string(v), i, string(v2), got, c.node[i].state.HashMap, N)

			}
		}
		if report != "" {
			vv("%v", report)
			t.Fatalf("%v", report)
		}
		c.stop()
	}
}

func Test011_accepted_versus_chosen(t *testing.T) {
	return
	cfg := &RaftConfig{
		deaf2a: map[string]bool{"node_1": true, "node_2": true},
		deaf2b: map[string]bool{"node_1": true, "node_2": true},
		NoDisk: false, // allow recovery checking
	}
	n := 3
	//vv("n= %v", n)
	c := newCluster(n, cfg)

	//time.Sleep(time.Second)

	// write to node 0
	v := Val([]byte("123"))
	err := c.node[0].Write("a", v, 0)
	panicOn(err)

	vv("done with write to node 0 of a:123")
	select {}
	// but read back from all nodes
	for i := range n {
		v2, err := c.node[i].Read("a", 0)
		panicOn(err)

		if v != v2 {
			t.Fatalf("write a:'%v' to node0, read back from node(i=%v) '%v'", string(v), i, string(v2))
		}
	}
	c.stop()

}
*/
