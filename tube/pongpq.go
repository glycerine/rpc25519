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

// The leader counts itself only while it is a voter. Evaluate other pongs
// against today's membership even if the sender was a voter when it replied.
func (s *TubeNode) leaderPongQuorumTm() (time.Time, bool, *Pong) {
	need := s.quorum()
	if s.isVotingMemberOfCurrentMC(s.name) {
		need--
	}
	return s.leaderFullPongPQ.quorumPongTm(need, s.isVotingMemberOfCurrentMC)
}

// Return the time of the oldest reply needed to make a quorum.
func (s *pongPQ) quorumPongTm(need int, isVoter func(string) bool) (time.Time, bool, *Pong) {
	if need == 0 {
		return time.Now(), true, nil
	}
	for it := s.tree.Min(); !it.Limit(); it = it.Next() {
		pong := it.Item().(*Pong)
		if !isVoter(pong.PeerName) {
			continue
		}
		need--
		if need == 0 {
			return pong.RecvTm, true, pong
		}
	}
	return time.Time{}, false, nil
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
	// Specular Bug 15: one pong per replica name, so the
	// old and new incarnation of a restarted replica
	// (different PeerIDs) are not both counted.
	if pong.PeerName != "" {
		s.delPeerName(pong.PeerName)
	}
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

func (s *pongPQ) delPeerName(peerName string) {
	for it := s.tree.Min(); !it.Limit(); {
		pong := it.Item().(*Pong)
		if pong.PeerName == peerName {
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
