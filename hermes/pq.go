package hermes

/* is garbage, this container/heap BS. Use rbtree instead! see pqtree.go

import (
	"container/heap"
	"fmt"
	"sync"
	"time"
)

// An pqTimeItem is something we manage in a priority queue.
type pqTimeItem struct {
	value    *Ticket
	priority time.Time // The priority of the item in the queue.
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

// pq is a Priority Queue. the hea member inside
// implements heap.Interface and holds pqTimeItems,
// behind a sync.Mutex for goroutine safety.
type pq struct {
	mut sync.Mutex
	hea pqTime
}

// "public" goroutine safe interface:

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

func (p *pq) peek() (tkt *Ticket, timeout time.Time) {
	p.mut.Lock()
	defer p.mut.Unlock()
	return p.hea.peek()
}

// add a new item to the queue.
func (p *pq) add(timeout time.Time, tkt *Ticket) *pqTimeItem {
	p.mut.Lock()
	defer p.mut.Unlock()
	return p.hea.add(timeout, tkt)
}

// get does a linear scan to find key in pq,
// returning its index, or -1 if not found.
func (p *pq) get(key Key) (items []*pqTimeItem) {
	p.mut.Lock()
	defer p.mut.Unlock()
	return p.hea.get(key)
}

// del does a linear scan to delete items with key from pq. It is
// a no-op if key is not present, and found will be false.
// del deletes multiple instances of key, if found.
func (p *pq) del(key Key) (found bool) {
	p.mut.Lock()
	defer p.mut.Unlock()
	return p.hea.del(key)
}

func (p *pq) delOneItem(item *pqTimeItem) {
	p.mut.Lock()
	defer p.mut.Unlock()
	p.hea.delOneItem(item)
}

// update modifies the priority and value of an pqTimeItem in the queue.
func (p *pq) update(item *pqTimeItem, value *Ticket, priority time.Time) {
	p.mut.Lock()
	defer p.mut.Unlock()
	p.hea.update(item, value, priority)
}

// inside (unlocked) impl:

// pqTime is member hea in PQ to implement a PriorityQueue;
// it implements heap.Interface and holds pqTimeItems.
type pqTime []*pqTimeItem

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

func (pq *pqTime) peek() (tkt *Ticket, timeout time.Time) {
	n := len(*pq)
	if n == 0 {
		return
	}
	return (*pq)[n-1].value, (*pq)[n-1].priority
}

// add a new item to the queue.
func (pq *pqTime) add(timeout time.Time, pu *Ticket) *pqTimeItem {
	n := len(*pq)
	item := &pqTimeItem{
		priority: timeout,
		value:    pu,
		index:    n,
	}
	*pq = append(*pq, item)
	heap.Fix(pq, n)
	return item
}

// get does a linear scan to find key in pq,
// returning its index, or -1 if not found.
func (pq *pqTime) get(key Key) (items []*pqTimeItem) {
	for _, item := range *pq {
		if item.value.keym.key == key {
			items = append(items, item)
		}
	}
	return
}

// del does a linear scan to delete items with key from pq. It is
// a no-op if key is not present, and found will be false.
// del deletes multiple instances of key, if found.
func (pq *pqTime) del(key Key) (found bool) {
	items := pq.get(key)
	if len(items) == 0 {
		return false
	}
	for _, item := range items {
		pq.delOneItem(item)
	}
	return true
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
	item.index = -1 // "for safety"
	old[n-1] = nil
	*pq = old[0 : n-1]
	if i < n-1 {
		// have to repair now that we stuck
		// i in the middle instead of the end.
		heap.Fix(pq, i)
	}
}

// update modifies the priority and value of an pqTimeItem in the queue.
func (pq *pqTime) update(item *pqTimeItem, value *Ticket, priority time.Time) {
	item.value = value
	item.priority = priority
	heap.Fix(pq, item.index)
}

// for sorting by highest version timestamp
// Warning: don't sort the PQ itself, only apply
// this to other independent slices that share
// no values (copies of pointers is okay), or you
// might mess up the priority queue order.
// We don't adjust the indexes in Swap below, but still,
// tread cautiously: make sure you create your
// own slide of pointers to *pqTiemItem and
// populate with copies of pointers, rather than
// any subslice of the full priority queue.
type highestTSVersionFirst []*pqTimeItem

func (pq highestTSVersionFirst) Len() int { return len(pq) }

func (pq highestTSVersionFirst) Less(i, j int) bool {
	return pq[i].value.TS.Compare(&pq[j].value.TS) > 0
}

func (pq highestTSVersionFirst) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}
*/
