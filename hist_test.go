package rpc25519

import (
	"fmt"
	"testing"
)

// helpers for simgrid test, a test.

func Test708_gridHistory_helper(t *testing.T) {
	n := 3
	gridCfg := &simGridConfig{
		ReplicationDegree: n,
	}
	var nodes []*simGridNode
	for i := range n {
		name := fmt.Sprintf("node%v", i)
		nodes = append(nodes, newSimGridNode(name, gridCfg))
	}

	h := newGridHistory(nodes)
	//vv("empty hist: \n%v", h)

	m := &Fragment{}
	h.addSend(nodes[0].name, nodes[1].name, m)
	h.addRead(nodes[1].name, nodes[0].name, m)

	//vv("after a send from 0 -> 1: \n%v", h)

	h.reset()
	for range 10 {
		for i := range n {
			for j := range n {
				if i == j {
					continue
				}
				h.addSend(nodes[i].name, nodes[j].name, m)
				h.addRead(nodes[j].name, nodes[i].name, m)
			}
		}
	}
	//vv("after 10 sends/reads from each to each: \n%v", h)
}
