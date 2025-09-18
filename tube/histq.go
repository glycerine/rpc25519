package tube

import (
	"fmt"
	"time"

	//"github.com/glycerine/idem"
	rb "github.com/glycerine/rbtree"
)

// tkthistQ orders Tickets by T0
type tkthistQ struct {
	tree *rb.Tree
}

func (s *tkthistQ) Len() int {
	return s.tree.Len()
}

func (s *tkthistQ) pop() *Ticket {
	if s.tree.Len() == 0 {
		return nil
	}
	it := s.tree.Min()
	top := it.Item().(*Ticket)
	s.tree.DeleteWithIterator(it)
	return top
}

func (s *tkthistQ) peek() *Ticket {
	if s.tree.Len() == 0 {
		return nil
	}
	it := s.tree.Min()
	top := it.Item().(*Ticket)
	return top
}

// add an ticket to the tree.
// If the item is already in the tree,
// do nothing and return false.
func (s *tkthistQ) add(tkt *Ticket) (added bool) {
	return s.tree.Insert(tkt)
}

func (s *tkthistQ) String() (r string) {
	i := 0
	r = fmt.Sprintf("tkthistQ[len %v]:\n", s.tree.Len())
	for it := s.tree.Min(); !it.Limit(); it = it.Next() {
		tkt := it.Item().(*Ticket)
		r += fmt.Sprintf("[%02d] Ticket{T0:%v, TicketID:%v}\n", i, tkt.T0.Format(rfc3339NanoNumericTZ0pad), tkt.TicketID)
		i++
	}
	return
}

// linear scan to delete by ticketID
func (s *tkthistQ) delTicketID(ticketID string) {
	it := s.tree.Min()
	if it.Limit() {
		return
	}
	for !it.Limit() {
		tkt := it.Item().(*Ticket)
		if tkt.TicketID == ticketID {
			delit := it
			it = it.Next()
			s.tree.DeleteWithIterator(delit)
		} else {
			it = it.Next()
		}
	}
}

// delete all tickets at or before deadTm.
func (s *tkthistQ) deleteLTE(deadTm time.Time) {
	it := s.tree.Min()
	if it.Limit() {
		return
	}
	for !it.Limit() {
		tkt := it.Item().(*Ticket)
		if lte(tkt.T0, deadTm) {
			delit := it
			it = it.Next()
			s.tree.DeleteWithIterator(delit)
		} else {
			it = it.Next()
		}
	}
}

func (s *tkthistQ) deleteAll() {
	s.tree.DeleteAll()
	return
}

// order by Ticket.T0
func newTkthistQ() *tkthistQ {
	return &tkthistQ{
		tree: rb.NewTree(func(a, b rb.Item) int {
			av := a.(*Ticket)
			bv := b.(*Ticket)

			if av == bv {
				return 0 // points to same memory (or both nil)
			}
			if av == nil {
				// just a is nil; b is not. sort nils to the front
				// so they get popped and GC-ed sooner (and
				// don't become temporary memory leaks by sitting at the
				// back of the queue.x
				panic("no nils")
				return -1
			}
			if bv == nil {
				panic("no nils")
				return 1
			}
			// INVAR: neither av nor bv is nil
			if av == bv {
				return 0 // pointer equality is immediate
			}

			// sort largest (most recent) time first.
			if av.T0.After(bv.T0) {
				return -1
			}
			if av.T0.Before(bv.T0) {
				return 1
			}
			// INVAR T0 equal, break ties
			if av.TicketID < bv.TicketID {
				return -1
			}
			if av.TicketID > bv.TicketID {
				return 1
			}
			return 0
		}),
	}
}
