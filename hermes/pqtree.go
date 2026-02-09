package hermes

import (
	"fmt"
	"sync"
	"time"

	rb "github.com/glycerine/rbtree"
)

// replacement for that container/heap garbage that
// occasionally gives the wrong order...

// pq is a Priority Queue.
// holds pqTimeItems.
// behind a sync.Mutex for goroutine safety.
type pq struct {
	mut    sync.Mutex
	pqtree *pqTime
}

type pqTime struct {
	tree *rb.Tree
}

// pqTimeItem are the elements in the pqTime
type pqTimeItem struct {
	tkt      *HermesTicket
	priority time.Time // The priority of the item in the queue.
}

func (item *pqTimeItem) String() (r string) {
	r = fmt.Sprintf(`&pqTimeItem{
   priority: %v
        tkt: %v
}`, nice(item.priority), item.tkt)
	return
}

func (p *pq) String() (r string) {
	p.mut.Lock()
	defer p.mut.Unlock()

	return p.pqtree.String()
}

func (p *pqTime) String() (r string) {
	r = fmt.Sprintf(`&pqTime[len %v]{
`, p.size())
	i := 0
	for it := p.tree.Min(); !it.Limit(); it = it.Next() {
		item := it.Item().(*pqTimeItem)
		r += fmt.Sprintf("[%02d] %v\n", i, item)
		i++
	}
	r += "}\n"
	return
}

// order by time, key, TicketID (so we don't
// delete duplicate time tickets!)
func newPqTime() *pqTime {
	return &pqTime{
		tree: rb.NewTree(func(a, b rb.Item) int {
			av := a.(*pqTimeItem)
			bv := b.(*pqTimeItem)

			if av == bv {
				return 0 // identical
			}

			if av.priority.Equal(bv.priority) {
				atkt := av.tkt
				btkt := bv.tkt
				if atkt.Key == btkt.Key {
					if atkt.TicketID == btkt.TicketID {
						return 0
					}
					if atkt.TicketID < atkt.TicketID {
						return -1
					}
					return 1
				}
				if atkt.Key < btkt.Key {
					return -1
				}
				return 1
			}
			if av.priority.Before(bv.priority) {
				return -1
			}
			return 1
		}),
	}
}

/////////////////

// "public" goroutine safe interface:

func (p *pq) size() (sz int) {
	p.mut.Lock()
	defer p.mut.Unlock()
	sz = p.pqtree.tree.Len()
	return
}

func (p *pq) pop() *pqTimeItem {
	p.mut.Lock()
	defer p.mut.Unlock()
	return p.pqtree.pop()
}

func (p *pq) peek() (tkt *HermesTicket, timeout time.Time) {
	p.mut.Lock()
	defer p.mut.Unlock()
	item := p.pqtree.peekItem()
	return item.tkt, item.priority
}

// add a new item to the queue.
func (p *pq) add(timeout time.Time, tkt *HermesTicket) *pqTimeItem {
	p.mut.Lock()
	defer p.mut.Unlock()

	item := &pqTimeItem{
		priority: timeout,
		tkt:      tkt,
	}
	added := p.pqtree.tree.Insert(item)
	_ = added
	return item
}

// get does a linear scan to find key in pq,
// returning its index, or -1 if not found.
func (p *pq) get(key Key) (items []*pqTimeItem) {
	p.mut.Lock()
	defer p.mut.Unlock()
	return p.pqtree.get(key)
}

// del does a linear scan to delete items with key from pq. It is
// a no-op if key is not present, and found will be false.
// del deletes multiple instances of key, if found.
func (p *pq) del(key Key) (found bool) {
	p.mut.Lock()
	defer p.mut.Unlock()
	return p.pqtree.del(key)
}

func (p *pq) delOneItem(item *pqTimeItem) {
	p.mut.Lock()
	defer p.mut.Unlock()
	p.pqtree.delOneItem(item)
}

// update modifies the priority and value of an pqTimeItem in the queue.
func (p *pq) update(item *pqTimeItem, value *HermesTicket, priority time.Time) {
	p.mut.Lock()
	defer p.mut.Unlock()
	p.pqtree.update(item, value, priority)
}

// inside (unlocked) impl:

// match the pq method set, so we can
// switch easily between locked or not.

func (s *pqTime) peekItem() *pqTimeItem {
	if s.tree.Len() == 0 {
		return nil
	}
	it := s.tree.Min()
	return it.Item().(*pqTimeItem)
}

func (pq *pqTime) peek() (tkt *HermesTicket, timeout time.Time) {
	if pq.tree.Len() == 0 {
		return
	}
	it := pq.tree.Min()
	pqi := it.Item().(*pqTimeItem)
	return pqi.tkt, pqi.priority
}

func (s *pqTime) pop() *pqTimeItem {
	if s.tree.Len() == 0 {
		return nil
	}
	it := s.tree.Min()
	top := it.Item().(*pqTimeItem)
	s.tree.DeleteWithIterator(it)
	return top
}

/*
func (s *pqTime) add(item *pqTimeItem) (added bool, it rb.Iterator) {
	added, it = s.tree.InsertGetIt(item)
	return
}
*/

// add a new item to the queue.
func (pq *pqTime) add(timeout time.Time, tkt *HermesTicket) *pqTimeItem {
	item := &pqTimeItem{
		priority: timeout,
		tkt:      tkt,
	}
	added := pq.tree.Insert(item)
	_ = added
	return item
}

func (pq *pqTime) size() int {
	return pq.tree.Len()
}

// get does a linear scan to find key in pq,
// returning its index, or -1 if not found.
func (pq *pqTime) get(key Key) (items []*pqTimeItem) {

	for it := pq.tree.Min(); !it.Limit(); it = it.Next() {
		item := it.Item().(*pqTimeItem)
		if item.tkt.keym.key == key {
			items = append(items, item)
		}
	}
	return
}

// del does a linear scan to delete items with key from pq. It is
// a no-op if key is not present, and found will be false.
// del deletes multiple instances of key, if found.
func (pq *pqTime) del(key Key) (found bool) {

	if pq.tree.Len() == 0 {
		return false
	}
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

	n := pq.size()
	if n == 0 {
		panic("cannot delete from empty pq")
	}
	found := pq.tree.DeleteWithKey(item)
	_ = found
}

// update modifies the priority and value of an pqTimeItem in the queue.
func (pq *pqTime) update(item *pqTimeItem, tkt *HermesTicket, priority time.Time) error {

	it, exact := pq.tree.FindGE_isEqual(item)
	//if it == pq.tree.Limit() {
	if !exact {
		return fmt.Errorf("error on pqTime.update(): item not found in tree of len '%v'! item='%v'\n\n tree='%v'", pq.size(), item, pq)
	}
	// delete and re-add to keep the proper tree ordering.
	item2 := it.Item().(*pqTimeItem)
	if item2 == item {
		found := pq.tree.DeleteWithKey(item2)
		if !found {
			panic("what? should have been able to pq.tree.DeleteWithKey")
		}

		item.tkt = tkt
		item.priority = priority
		added := pq.tree.Insert(item)
		if !added {
			panic("what? should have been able to pq.tree.Insert()")
		}
	}
	return nil
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
	return pq[i].tkt.TS.Compare(&pq[j].tkt.TS) > 0
}

func (pq highestTSVersionFirst) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}
