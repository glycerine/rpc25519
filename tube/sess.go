package tube

import (
	rb "github.com/glycerine/rbtree"
)

type sessTableByExpiry struct {
	tree *rb.Tree
}

func (s *sessTableByExpiry) Len() int {
	return s.tree.Len()
}

func (s *sessTableByExpiry) Clear() {
	s.tree.DeleteAll()
}

func (s *sessTableByExpiry) Delete(ste *SessionTableEntry) {

	// try to prevent
	// panic: DeleteWithIterator called with iterator not from this tree.
	byTree := ste.bySeenIter.Tree()
	if byTree != s.tree {
		alwaysPrintf("yuck! ste.bySeenIter from wrong rbtree %p vs s.tree=%p", byTree, s.tree)
		return
	}
	s.tree.DeleteWithIterator(ste.bySeenIter)
}

func newSessTableByExpiry() *sessTableByExpiry {
	return &sessTableByExpiry{
		tree: rb.NewTree(func(a, b rb.Item) int {
			av := a.(*SessionTableEntry)
			bv := b.(*SessionTableEntry)

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

			// sort smallest (oldest) time first.
			if av.SessionEndxTm.Before(bv.SessionEndxTm) {
				return -1
			}
			if av.SessionEndxTm.After(bv.SessionEndxTm) {
				return 1
			}
			// INVAR SessionEndxTm equal, break ties
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
