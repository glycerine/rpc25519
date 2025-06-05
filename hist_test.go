package rpc25519

import (
	"fmt"
	"testing"
)

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

	hist := newGridHistory(nodes)
	vv("empty hist: \n%v", hist)
}
