package rpc25519

import (
	"container/heap"
	"fmt"
	"sync"
	"time"
)

// An pqTimeItem is something we manage in a priority queue.
type pqTimeItem struct {
	value    *fop
	priority time.Time // The priority of the item in the queue.
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

// pqTime is member hea in PQ to implement a PriorityQueue;
// it implements heap.Interface and holds pqTimeItems.
type pqTime []*pqTimeItem

// pq is a Priority Queue. the hea member inside
// implements heap.Interface and holds pqTimeItems,
// behind a sync.Mutex for goroutine safety
// if needed. If not, see methods below.
type pq struct {
	mut sync.Mutex
	hea pqTime // []*pqTimeItem below
}

// "public" goroutine safe interface, mutex protected:

func (p *pq) size() (sz int) {
	p.mut.Lock()
	defer p.mut.Unlock()
	sz = len(p.hea)
	return
}

func (p *pq) pop() *pqTimeItem {
	p.mut.Lock()
	defer p.mut.Unlock()
	return p.hea.Pop().(*pqTimeItem)
}

func (p *pq) peek() (op *fop) { // , timeout time.Time) {
	p.mut.Lock()
	defer p.mut.Unlock()
	return p.hea.peek()
}

// add a new item to the queue.
func (p *pq) add(op *fop) *pqTimeItem {
	p.mut.Lock()
	defer p.mut.Unlock()
	return p.hea.add(op)
}

func (p *pq) delOneItem(item *pqTimeItem) {
	p.mut.Lock()
	defer p.mut.Unlock()
	p.hea.delOneItem(item)
}

// update modifies the priority and value of an pqTimeItem in the queue.
func (p *pq) update(item *pqTimeItem, value *fop) { // , priority time.Time) {
	p.mut.Lock()
	defer p.mut.Unlock()
	p.hea.update(item, value) // , value.when) //priority)
}

// private, inside (unlocked) impl:

func (pq pqTime) Len() int { return len(pq) }

func (pq pqTime) Less(i, j int) bool {
	// lowest time at end of queue, where pop() or next() will read it.
	return pq[i].priority.After(pq[j].priority)
}

func (pq pqTime) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *pqTime) Push(x any) {
	n := len(*pq)
	item := x.(*pqTimeItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *pqTime) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // don't stop the GC from reclaiming the item eventually
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// match the pq method set, so we can
// switch easily between locked or not.
func (pq *pqTime) pop() *pqTimeItem {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // don't stop the GC from reclaiming the item eventually
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

func (pq *pqTime) size() int {
	return len(*pq)
}

func (pq *pqTime) peek() (op *fop) { // , timeout time.Time) {
	n := len(*pq)
	if n == 0 {
		return
	}
	return (*pq)[n-1].value //, (*pq)[n-1].priority
}

// add a new item to the queue.
func (pq *pqTime) add(op *fop) *pqTimeItem {
	n := len(*pq)
	item := &pqTimeItem{
		priority: op.when,
		value:    op,
		index:    n,
	}
	*pq = append(*pq, item)
	heap.Fix(pq, n)
	return item
}

func (pq *pqTime) delOneItem(item *pqTimeItem) {
	old := *pq
	n := len(old)
	if n == 0 {
		panic("cannot delete from empty pq")
	}

	i := item.index
	if i < 0 || i >= n {
		panic(fmt.Sprintf("bad index %v on item to delete: '%v'", item.index, item.value))
	}
	if i < n-1 {
		// swap it to the end to delete it.
		old.Swap(i, n-1)
	}
	// INVAR: pu is at old[n-1]
	// similar to Pop() now...
	item.index = -1 // "for safety" as the example says.
	old[n-1] = nil
	*pq = old[0 : n-1]
	if i < n-1 {
		// have to repair now that we stuck
		// i in the middle instead of the end.
		heap.Fix(pq, i)
	}
}

// update modifies the priority and value of an pqTimeItem in the queue.
func (pq *pqTime) update(item *pqTimeItem, value *fop) {
	item.value = value
	item.priority = value.when
	heap.Fix(pq, item.index)
}
