package tube

import (
	"fmt"
	"time"

	//"github.com/glycerine/idem"
	rb "github.com/glycerine/rbtree"
)

// pongPQ used by leaders to know they have
// a majority of followers or not. Per section
// 6.2 leaders must step down, and tell
// any clients, if they do not seem a quroum
// of followers within a leader election timeout.
// ordered by Pong.RecvTm
type pongPQ struct {
	name string
	tree *rb.Tree
}

func (s *pongPQ) Len() int {
	return s.tree.Len()
}

// quorumPongTm acts like a 1-based index into
// a sorted array of timestamps.
// quorumPongTm returns the newest time
// at which we had a quorum of roundtrip pongs,
// to allow leaders to step down (per section 6.2
// of the raft thsis) if either ok is false or tm > max
// leader election time, (assuming that we have
// been leader for at least min election dur;
// so this is checked on the leader election timeout).
// quor = size of quorum to check.
// If we have < quor in tree, false is returned for ok,
// and tm will be the zero time and oldest will be nil.
func (s *pongPQ) quorumPongTm(quor int) (tm time.Time, ok bool, newest *Pong) {
	n := s.tree.Len()
	if n == 0 || quor < 1 {
		// single node cluster
		// Test001_no_replicas_write_new_value
		ok = true
		tm = time.Now()
		return
	}
	// PRE: n > 0
	goal := n - 1
	// so: goal >= 0

	// PRE: quor >= 1
	if quor > n {
		ok = false
		// still give back the best timestamp we can? naw. no point.
		return
	} else {
		// 1 <= quor <= n
		ok = true
		// quorum of 1 wants the [0] smallest (first)
		goal = quor - 1
		// 0 <= goal <= n-1
	}

	i := 0
	// tree is sorted by highest time first (most recent first),
	// so as we look further into the tree, times get older.
	for it := s.tree.Min(); !it.Limit(); it = it.Next() {
		if i == goal {
			newest = it.Item().(*Pong)
			tm = newest.RecvTm
			if tm.IsZero() {
				panicf("%v: should never have 0 time in newest.RecvTm!", s.name)
			}
			return
		}
		i++
	}
	panic(fmt.Sprintf("should be unreachable since Len() was sufficient. i=%v, quor=%v, n=%v; ok=%v; tm=%v; goal=%v", i, quor, n, ok, tm, goal))
}

func (s *pongPQ) pop() *Pong {
	if s.tree.Len() == 0 {
		return nil
	}
	it := s.tree.Min()
	top := it.Item().(*Pong)
	s.tree.DeleteWithIterator(it)
	return top
}

// add adds pong to s. Any earlier pre-existing
// same pong.PeerID in the tree will be
// deleted from the tree first,
// if one is present. Thus the tree only
// contains at most one pong from a peer
// at any given point.
func (s *pongPQ) add(pong Pong) (it rb.Iterator) {
	if pong.RecvTm.IsZero() {
		panicf("%v: should never have 0 time in pong.RecvTm!", s.name)
	}
	s.delPeerID(pong.PeerID)
	var added bool
	added, it = s.tree.InsertGetIt(&pong)
	if !added {
		panic(fmt.Sprintf("must be added! pong = '%v', tree = '%v'", &pong, s))
	}
	return
}

func (s *pongPQ) String() (r string) {
	i := 0
	r = fmt.Sprintf("pongPQ[len %v]:\n", s.tree.Len())
	for it := s.tree.Min(); !it.Limit(); it = it.Next() {
		pong := it.Item().(*Pong)
		r += fmt.Sprintf("[%02d] Pong{RecvTm:%v, PeerID:%v PeerName:%v}\n", i, pong.RecvTm.Format(rfc3339NanoNumericTZ0pad), pong.PeerID, pong.PeerName)
		i++
	}
	return
}

func (s *pongPQ) delPeerID(peerID string) {
	it := s.tree.Min()
	if it.Limit() {
		return
	}
	for !it.Limit() {
		pong := it.Item().(*Pong)
		if pong.PeerID == peerID {
			delit := it
			it = it.Next()
			s.tree.DeleteWithIterator(delit)
		} else {
			it = it.Next()
		}
	}
}

func (s *pongPQ) deleteAll() {
	s.tree.DeleteAll()
	return
}

// order by Pong.RecvTm
func newPongPQ(name string) *pongPQ {
	return &pongPQ{
		name: name,
		tree: rb.NewTree(func(a, b rb.Item) int {
			av := a.(*Pong)
			bv := b.(*Pong)

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
			// INVAR: neither av nor bv is nil; AND av != bv

			// sort largest (most recent) time first.
			if av.RecvTm.After(bv.RecvTm) {
				return -1
			}
			if av.RecvTm.Before(bv.RecvTm) {
				return 1
			}
			// INVAR RecvTm equal, break ties
			if av.PeerID < bv.PeerID {
				return -1
			}
			if av.PeerID > bv.PeerID {
				return 1
			}
			return 0
		}),
	}
}
