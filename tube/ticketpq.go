package tube

import (
	//"time"

	//"github.com/glycerine/idem"
	rb "github.com/glycerine/rbtree"
)

// ticketPQ is used to dedup requests.
type ticketPQ struct {
	owner   string
	orderby string
	tree    *rb.Tree
}

func (s *ticketPQ) peek() *Ticket {
	if s.tree.Len() == 0 {
		return nil
	}
	it := s.tree.Min()
	return it.Item().(*Ticket)
}

func (s *ticketPQ) pop() *Ticket {
	if s.tree.Len() == 0 {
		return nil
	}
	it := s.tree.Min()
	top := it.Item().(*Ticket)
	s.tree.DeleteWithIterator(it)
	return top
}

func (s *ticketPQ) add(op *Ticket) (added bool, it rb.Iterator) {
	if op == nil {
		panic("do not put nil into ticketPQ!")
	}
	added, it = s.tree.InsertGetIt(op)
	return
}

func (s *ticketPQ) del(op *Ticket) (found bool) {
	if op == nil {
		panic("cannot delete nil ticket!")
	}
	var it rb.Iterator
	it, found = s.tree.FindGE_isEqual(op)
	if !found {
		return
	}
	s.tree.DeleteWithIterator(it)
	return
}

func (s *ticketPQ) deleteAll() {
	s.tree.DeleteAll()
	return
}

// order by Ticket.T0 time of first observation.
func newTicketPQ(owner string) *ticketPQ {
	return &ticketPQ{
		owner:   owner,
		orderby: "ticketT0",
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
				return -1
			}
			if bv == nil {
				return 1
			}
			// INVAR: neither av nor bv is nil
			if av == bv {
				return 0 // pointer equality is immediate
			}

			if av.T0.Before(bv.T0) {
				return -1
			}
			if av.T0.After(bv.T0) {
				return 1
			}
			// INVAR arrivalTm equal, break ties
			if av.TicketID < bv.TicketID {
				return -1
			}
			if av.TicketID > bv.TicketID {
				return 1
			}
			// same ticket but different session?
			if av.SessionID < bv.SessionID {
				return -1
			}
			if av.SessionID > bv.SessionID {
				return 1
			}
			return 0
		}),
	}
}
