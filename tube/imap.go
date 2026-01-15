package tube

import (
	"fmt"
	"iter"
	//rb "github.com/glycerine/rbtree"
)

// imap is an indexed-map for storing and
// retreiving *Ticket efficiently by either TicketID or LogIndex.
//
// Inside: a regular Go map m has TicketID as a key;
// and an ordered omap providing ordered
// access by RaftLogEntry.Index int64; Ticket.LogIndex.
type imap struct {
	ordered *Omap[int64, *Ticket]
	m       map[string]*Ticket

	// if we deferred adding to ordered b/c they lacked LogIndex
	awaitIndex *Omap[string, *Ticket]
}

func newImap() *imap {
	return &imap{
		ordered:    NewOmap[int64, *Ticket](),
		m:          make(map[string]*Ticket),
		awaitIndex: NewOmap[string, *Ticket](),
	}
}

// Len returns the number of keys stored in the imap.
func (s *imap) Len() int {
	return len(s.m)
}

func (s *imap) String() (r string) {
	r = fmt.Sprintf("imap{\n")
	for logIndex, tkt := range s.ordered.All() {
		r += fmt.Sprintf("  [LogIndex: %v] Desc:'%v'; TicketID:'%v'\n", logIndex, tkt.Desc, tkt.TicketID)
	}
	r += "}\n"
	return
}

func (s *imap) del(tkt *Ticket) { // (found bool, next rb.Iterator) {
	_, present := s.m[tkt.TicketID]
	if !present {
		return
	}
	s.ordered.Delkey(tkt.LogIndex)
	s.awaitIndex.Delkey(tkt.TicketID)
	delete(s.m, tkt.TicketID)
	return
}

// deleteAll clears the tree in O(1) time.
func (s *imap) deleteAll() {
	s.ordered.DeleteAll()
	s.awaitIndex.DeleteAll()
	clear(s.m)
}

// set is an upsert. It does an insert if the key is
// not already present returning newlyAdded true;
// otherwise it updates the current key's value in place.
func (s *imap) set(tktID string, tkt *Ticket) (newlyAdded bool) {

	// the size of m map and ordered tree might disagree
	// because we add tkt.LogIndex=0 tickets into the map but not
	// the tree since they will get assigned a LogIndex
	// by replicateTicket shortly and then our tree
	// would be wrong and not in sorted order anymore.

	_, ok := s.m[tktID]
	newlyAdded = !ok

	if tkt.LogIndex > 0 {
		s.ordered.Set(tkt.LogIndex, tkt)
	} else {
		s.awaitIndex.Set(tkt.TicketID, tkt)
	}
	s.m[tktID] = tkt

	return
}

// get2 returns the val corresponding to key in O(1).
func (s *imap) get2(tktID string) (val *Ticket, found bool) {
	val, found = s.m[tktID]
	return
}

// get does get2 but without the found flag.
func (s *imap) get(tktID string) (val *Ticket) {
	return s.m[tktID]
}

func (s *imap) repair() {
	// first repair earlier set() additions that could
	// not be added to the tree because they
	// lacked LogIndex at the time.
	for tktID, tkt := range s.awaitIndex.All() {
		_, ok := s.m[tktID]
		if ok {
			if tkt.LogIndex > 0 {
				s.ordered.Set(tkt.LogIndex, tkt)
				s.awaitIndex.Delkey(tktID)
			}
		} else {
			s.awaitIndex.Delkey(tktID)
		}
	}
}

func (s *imap) all() iter.Seq2[int64, *Ticket] {
	s.repair()
	return s.ordered.All()
}

func (s *imap) allByTicketID() iter.Seq2[int64, *Ticket] {
	s.repair()
	return s.ordered.All()
}
