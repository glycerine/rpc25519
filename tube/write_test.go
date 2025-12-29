package tube

import (
	"bytes"
	"fmt"
	//"time"

	"testing"
)

func Test002_tube_write_new_value(t *testing.T) {

	bubbleOrNot(t, func(t *testing.T) {

		numNodes := 2 // must still be able to elect a leader.
		//n := 3

		c, _, _, _ := setupTestCluster(t, numNodes, -1, 2)
		//cfg := NewTubeConfigTest(n, t.Name(), globalUseSimnet)
		//c := NewCluster(t.Name(), cfg)
		//c.Start()
		defer c.Close()

		nodes := c.Nodes

		var v []byte
		i := 0
		N := 1
		for range N {
			i++

			// Write
			v = []byte(fmt.Sprintf("%v", i))
			//vv("about to write '%v'", string(v))
			txtW, err := nodes[0].Write(bkg, "", "a", v, 0, nil, "")
			panicOn(err)
			_ = txtW

			// Read all nodes
			for j := range numNodes {
				tktj, err := nodes[j].Read(bkg, "", "a", 0, nil)
				panicOn(err)
				vj := tktj.Val

				if !bytes.Equal(v, vj) {
					t.Fatalf("write a:'%v' to node0, read back from node j=%v: '%v'", string(v), j, string(vj))
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
