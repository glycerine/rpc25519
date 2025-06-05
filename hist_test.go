package rpc25519

import (
	"fmt"
	"sync"
	"testing"
)

// helpers for simgrid test

// simple message history, our own copies.
// in observed order.
type msghistory struct {
	h []*Fragment
}

func newMsghistory() *msghistory {
	return &msghistory{}
}

func (s *msghistory) reset() {
	s.h = nil
}

// a matrix of histories
type matrix struct {
	nrow int
	ncol int
	m    *omap[string, *omap[string, *msghistory]]
}

func newMatrix(nodes []*simGridNode) *matrix {
	fromI := newOmap[string, *omap[string, *msghistory]]()
	n := len(nodes)
	for i := range nodes {
		toJ := newOmap[string, *msghistory]()
		for j := range nodes {
			toJ.set(nodes[j].name, newMsghistory())
		}
		fromI.set(nodes[i].name, toJ)
	}
	return &matrix{
		nrow: n,
		ncol: n,
		m:    fromI,
	}
}

func (s *matrix) String() (r string) {
	r = fmt.Sprintf("matrix[%v x %v]\n", s.nrow, s.ncol)
	const header = false
	if header {
		r += "       "
		for col := range s.m.all() {
			r += fmt.Sprintf("%6s  ", col)
		}
		r += "\n"
	}
	i := 0
	for row, cols := range s.m.all() {
		r += fmt.Sprintf("%v: ", row)
		j := 0
		for _, hist := range cols.all() {
			if i == j {
				r += "     _  "
			} else {
				r += fmt.Sprintf("%6v  ", len(hist.h))
			}
			j++
		}
		r += "\n"
		i++
	}
	return
}

func (s *matrix) reset() {
	for _, cols := range s.m.all() {
		for _, hist := range cols.all() {
			hist.reset()
		}
	}
}

// a pair of history matrixes, one for sends,
// one for reads.
type gridhistory struct {
	mut   sync.Mutex
	reads *matrix
	sends *matrix
}

func newGridHistory(nodes []*simGridNode) *gridhistory {
	return &gridhistory{
		reads: newMatrix(nodes),
		sends: newMatrix(nodes),
	}
}

func (h *gridhistory) String() (r string) {
	h.mut.Lock()
	defer h.mut.Unlock()
	return fmt.Sprintf(`gridhistory{
 reads: %v
 sends: %v
}`, h.reads, h.sends)
}

func (g *gridhistory) addSend(a, b string, frag *Fragment) {
	g.mut.Lock()
	defer g.mut.Unlock()

	ah, ok := g.sends.m.get2(a)
	if !ok {
		panic(fmt.Sprintf("no a key '%v'", a))
	}
	h, ok := ah.get2(b)
	if !ok {
		panic(fmt.Sprintf("no b key '%v'", b))
	}
	h.h = append(h.h, frag)
}

func (g *gridhistory) addRead(a, b string, frag *Fragment) {
	g.mut.Lock()
	defer g.mut.Unlock()

	ah, ok := g.reads.m.get2(a)
	if !ok {
		panic(fmt.Sprintf("no a key '%v'", a))
	}
	h, ok := ah.get2(b)
	if !ok {
		panic(fmt.Sprintf("no b key '%v'", b))
	}
	h.h = append(h.h, frag)
}

func (g *gridhistory) reset() {
	g.mut.Lock()
	defer g.mut.Unlock()

	g.reads.reset()
	g.sends.reset()
}

func (g *gridhistory) sentBy(name string) (sends int) {
	g.mut.Lock()
	defer g.mut.Unlock()

	ah, ok := g.sends.m.get2(name)
	if !ok {
		panic(fmt.Sprintf("no sends key '%v'", name))
	}
	for _, hist := range ah.all() {
		sends += len(hist.h)
	}
	return
}
func (g *gridhistory) readBy(name string) (reads int) {
	g.mut.Lock()
	defer g.mut.Unlock()

	ah, ok := g.reads.m.get2(name)
	if !ok {
		panic(fmt.Sprintf("no reads key '%v'", name))
	}
	for _, hist := range ah.all() {
		reads += len(hist.h)
	}
	return
}

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
