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

// a matrix of histories
type smatrix struct {
	nrow int
	ncol int
	m    *omap[string, *omap[string, *msghistory]]
}

func newSmatrix(nodes []*simGridNode) *smatrix {
	fromI := newOmap[string, *omap[string, *msghistory]]()
	n := len(nodes)
	for i := range nodes {
		toJ := newOmap[string, *msghistory]()
		for j := range nodes {
			toJ.set(nodes[j].name, newMsghistory())
		}
		fromI.set(nodes[i].name, toJ)
	}
	return &smatrix{
		nrow: n,
		ncol: n,
		m:    fromI,
	}
}

func (s *smatrix) String() (r string) {
	r = fmt.Sprintf("smatrix[%v x %v]\n       ", s.nrow, s.ncol)
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

// a pair of history matrixes, one for sends,
// one for reads.
type gridhistory struct {
	mut   sync.Mutex
	reads *smatrix
	sends *smatrix
}

func newGridHistory(nodes []*simGridNode) *gridhistory {
	return &gridhistory{
		reads: newSmatrix(nodes),
		sends: newSmatrix(nodes),
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
