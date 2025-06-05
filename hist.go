package rpc25519

import (
	"fmt"
	"sync"
)

// helpers for simgrid test

// simple message history, our own copies.
// in observed order.
type msghistory struct {
	h []*Message
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
	r = fmt.Sprintf("matrix[%v x %v]\n       ", s.nrow, s.ncol)
	for col := range s.m.all() {
		r += fmt.Sprintf("%6s  ", col)
	}
	r += "\n"
	for row, cols := range s.m.all() {
		r += fmt.Sprintf("%v: ", row)
		for _, hist := range cols.all() {
			r += fmt.Sprintf("%6v  ", len(hist.h))
		}
		r += "\n"
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

func (g *gridhistory) addSend(a, b *simGridNode, m *Message) {
	g.mut.Lock()
	defer g.mut.Unlock()

	h := g.sends.m.get(a.name).get(b.name)
	h.h = append(h.h, m)
}

func (g *gridhistory) addRead(a, b *simGridNode, m *Message) {
	g.mut.Lock()
	defer g.mut.Unlock()

	h := g.reads.m.get(a.name).get(b.name)
	h.h = append(h.h, m)
}

func (g *gridhistory) reset() {
	g.mut.Lock()
	defer g.mut.Unlock()

	g.reads.reset()
	g.sends.reset()
}
