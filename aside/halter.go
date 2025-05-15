package idem

import (
	"fmt"
	"time"

	mut "github.com/glycerine/one_tree_rwmut"
)

// To avoid deadlock issues, any goroutine
// using this package synchronizes
// with other goroutines using
// a process-wide single mutex.
// This lock is used for all idem.Halter
// and idem.IdemCloseChan operations,
// even for goroutines from different packages.
// This is a common idiom used by the
// Go standard library.
//
// Because the same mutex is used by
// all goroutines, there cannot be any
// two-lock ordering variation, which
// would create deadlocks. In earlier designs,
// we observed such deadlocks when packages would
// add sub-package Halters as children
// of their own Halter to form a supervision tree.
//
// Since shutdown is not a performance
// sensitive event, and even end-of-operation
// coordination should be a minor part of
// an overall task, we expect minimal performance
// impact from this design, while gaining
// deadlock freedom and a simple, easy to reason about
// implementation.

// IdemCloseChan can have Close() called on it
// multiple times, and it will only close
// Chan once.
type IdemCloseChan struct {
	Chan   chan struct{}
	closed bool

	children []*IdemCloseChan

	// not exported b/c it would lead to data races.
	// Use Reason() to safely extract.
	whyClosed error // CloseWithReason() sets this.

	taskCount int
}

// ErrGiveUp is returned by IdemCloseChan.WaitTilDone
// if the giveup channel close was the reason that
// WaitTilDone returned.
var ErrGiveUp = fmt.Errorf("giveup channel was closed.")

// ErrNotClosed is returned by FirstTreeReason if
// it is called on a tree that is not completely closed.
var ErrNotClosed = fmt.Errorf("tree is not closed")

// ErrTasksAllDone is the reason when the task count reached 0.
var ErrTasksAllDone = fmt.Errorf("tasks all done")

// NewIdemCloseChan makes a new IdemCloseChan.
func NewIdemCloseChan() *IdemCloseChan {
	return &IdemCloseChan{
		Chan: make(chan struct{}),
	}
}

var ErrAlreadyClosed = fmt.Errorf("Chan already closed")

// Close returns ErrAlreadyClosed if it has been
// called before. It never closes IdemClose.Chan more
// than once, so it is safe to ignore the returned
// error value. Close() is safe for concurrent access by multiple
// goroutines. Close returns nil on the initial call, and
// ErrAlreadyClosed on any subsequent call.
//
// When we actually close the underlying c.Chan,
// we also call Close on any children, while holding
// our mutex. This does not happen if the underlying
// channel is already closed, so the recursion stops at
// the depth of the first already-closed node in the tree.
func (c *IdemCloseChan) Close() error {
	mut.Lock()
	defer mut.Unlock()
	return c.nolockingClose()
}

var ErrBailed = fmt.Errorf("bail chan closed before getting the lock")

func (c *IdemCloseChan) nolockingClose() error {
	if !c.closed {
		close(c.Chan)
		c.closed = true
		for _, child := range c.children {
			child.nolockingClose()
		}
		return nil
	}
	return ErrAlreadyClosed
}

// CloseWithReason is a no-op if c is already closed.
// Otherwise, we record the why and close c.
// Use Reason() to get the why back. Like
// Close(), we recursive call CloseWithReason()
// on any children. Naturally decendents
// will only set why if they were still open
// when the recursive CloseWithReason gets to them,
// and if any child is already closed the
// recursion stops.
func (c *IdemCloseChan) CloseWithReason(why error) error {
	mut.Lock()
	defer mut.Unlock()
	return c.nolockingCloseWithReason(why)
}

func (c *IdemCloseChan) nolockingCloseWithReason(why error) error {
	if !c.closed {
		c.whyClosed = why
		close(c.Chan)
		c.closed = true
		for _, child := range c.children {
			child.nolockingCloseWithReason(why)
		}
		return nil
	}
	return ErrAlreadyClosed
}

// Reason gets the why set with CloseWithReason().
// It also returns the closed state of c.
// If c is still open, why will be nil,
// as why is only set during the initial CloseWithReason
// if c was still open.
// The returned why will also be nil
// if c was closed without a reason, by Close()
// for example.
//
// Callers should be prepared to handle a nil why
// no matter what the state of isClosed.
func (c *IdemCloseChan) Reason() (why error, isClosed bool) {
	mut.Lock()
	defer mut.Unlock()
	return c.nolockingReason()
}

func (c *IdemCloseChan) nolockingReason() (why error, isClosed bool) {
	why = c.whyClosed
	isClosed = c.closed
	return
}

// Reason1 is the same as Reason but without the isClosed.
// This is easier to use in logging.
func (c *IdemCloseChan) Reason1() (why error) {
	mut.Lock()
	defer mut.Unlock()
	return c.nolockingReason1()
}

func (c *IdemCloseChan) nolockingReason1() (why error) {
	why = c.whyClosed
	return
}

// IsClosed tells you if Chan is already closed or not.
func (c *IdemCloseChan) IsClosed() bool {
	mut.Lock()
	defer mut.Unlock()
	return c.closed
}

func (c *IdemCloseChan) nolockingIsClosed() bool {
	return c.closed
}

// Halter helps shutdown a goroutine
type Halter struct {
	// The owning goutine should call Done.Close() as its last
	// action once it has received the ReqStop() signal.
	Done *IdemCloseChan

	// Other goroutines call ReqStop.Close() in order
	// to request that the owning goroutine stop immediately.
	// The owning goroutine should select on ReqStop.Chan
	// in order to recognize shutdown requests.
	ReqStop *IdemCloseChan

	children []*Halter
}

func NewHalter() (h *Halter) {
	h = &Halter{
		Done:    NewIdemCloseChan(),
		ReqStop: NewIdemCloseChan(),
	}
	//vv("NewHalter created h=%p at %v", h, stack())
	return
}

// RequestStop closes the h.ReqStop channel
// if it has not already done so. Safe for
// multiple goroutine access.
func (h *Halter) RequestStop() {
	h.ReqStop.Close()
}

// MarkDone closes the h.Done channel
// if it has not already done so. Safe for
// multiple goroutine access.
func (h *Halter) MarkDone() {
	h.Done.Close()
}

// IsStopRequested returns true iff h.ReqStop has been Closed().
func (h *Halter) IsStopRequested() bool {
	return h.ReqStop.IsClosed()
}

// IsDone returns true iff h.Done has been Closed().
func (h *Halter) IsDone() bool {
	return h.Done.IsClosed()
}

// AddChild adds a child IdemCloseChan that will
// be closed when its parent is Close()-ed. If
// c is already closed, we close child immediately.
func (c *IdemCloseChan) AddChild(child *IdemCloseChan) {
	if child == c {
		panic("cannot add ourselves as a child of ourselves; would deadlock")
	}
	mut.Lock()
	defer mut.Unlock()
	c.nolockingAddChild(child)
}

func (c *IdemCloseChan) RemoveChild(child *IdemCloseChan) {
	mut.Lock()
	defer mut.Unlock()
	c.nolockingRemoveChild(child)
}
func (c *IdemCloseChan) nolockingRemoveChild(child *IdemCloseChan) {
	for i, ch := range c.children {
		if ch == child {
			c.children = append(c.children[:i], c.children[i+1:]...)
			return
		}
	}
}

// AddChild adds child to h's children; and
// adds child.ReqStop to the children of
// h.ReqStop.
//
// Note that if h.ReqStop is already closed,
// then child.ReqStop will also be
// ClosedWithReason with the same reason
// as h, if any is available.
func (h *Halter) AddChild(child *Halter) {
	vv("Halter(%p).AddChild(%p)  h.ReqStop=%p  child.ReqStop=%p; stack=\n%v", h, child, h.ReqStop, child.ReqStop, stack())
	if child == h {
		panic("cannot add ourselves as a child of ourselves; would deadlock")
	}
	mut.Lock()
	defer mut.Unlock()

	for _, c := range h.children {
		if child == c {
			panic(fmt.Sprintf("arg. alrady added! duplicate halter child: %p", child))
		}
	}
	h.children = append(h.children, child)     // halter children
	h.ReqStop.nolockingAddChild(child.ReqStop) // ReqStop children
}

func (c *IdemCloseChan) nolockingAddChild(child *IdemCloseChan) {
	vv("IdemCloseChan(%p).nolockingAddChild(%p)", c, child)
	if child == c {
		panic("cannot add ourselves as a child of ourselves; would deadlock")
	}
	if c.closed {
		child.nolockingCloseWithReason(c.whyClosed)
	}
	// check for duplicates
	pre := len(c.children)
	ptrs := []string{fmt.Sprintf("%p", child)}
	for _, e := range c.children {
		if e == child {
			panic(fmt.Sprintf("already have child %p, don't make dups/cycles!", child))
		} else {
			ptrs = append(ptrs, fmt.Sprintf(", %p", e))
		}
	}
	c.children = append(c.children, child)
	vv("IdemCloseChan(%p).AddChild() grew, now has %v -> %v children {%v}", c, pre, len(c.children), ptrs)
}

// RemoveChild removes child from the set
// of h.children. It does not undo any
// close operation that AddChild did on addition.
func (h *Halter) RemoveChild(child *Halter) {
	mut.Lock()
	defer mut.Unlock()
	h.nolockingRemoveChild(child)
}
func (h *Halter) nolockingRemoveChild(child *Halter) {
	for i, ch := range h.children {
		if ch == child {
			h.children = append(h.children[:i], h.children[i+1:]...)
			return
		}
	}
	h.ReqStop.nolockingRemoveChild(child.ReqStop) // write race vs :364 read in Test408_multiple_circuits_open_and_close
}

// StopTreeAndWaitTilDone first calls ReqStop.CloseWithReason(why)
// on h (which will recursively call ReqStop.CloseWithReason(why)
// on all non-closed children in the ReqStop parallel,
// tree, efficiently stopping at the first already-closed node).
// Then it waits up to atMost time
// for all tree members to Close their
// Done.Chan. It may return much more quickly
// than atMost, but will never wait longer.
// An atMost duration <= 0 will wait indefinitely.
func (h *Halter) StopTreeAndWaitTilDone(atMost time.Duration, giveup <-chan struct{}, why error) {
	// since ReqStop keeps a parallel tree, we only
	// need do this on the top level;
	// to efficiently stop traversal at first already
	// closed tree level.
	h.ReqStop.CloseWithReason(why)

	// We used to have to close our own Done channel.
	// Otherwise, waitTilDoneOrAtMost would block, waiting for it.
	// But/update: we added a flag to visit to allow skipping the
	// root. That way we can wait to close our own Done
	// until the very end, allowing a user to recursively
	// wait on a halter tree.
	isCycle := map[*Halter]bool{h: true}
	h.waitTilDoneOrAtMost(atMost, giveup, false, isCycle)

	h.Done.CloseWithReason(why)
}

// WaitTilDoneOrAtMost waits to return until
// all descendents have closed their Done.Chan or atMost
// time has elapsed. If atMost is <= 0, then we
// wait indefinitely for all the Done.Chan.
func (h *Halter) waitTilDoneOrAtMost(atMost time.Duration, giveup <-chan struct{}, visitSelf bool, isCycle map[*Halter]bool) {
	//defer vv("returning from waitTilDoneOrAtMost, h = %p", h)

	// a nil timeout channel will never fire in a select.
	var to <-chan time.Time
	// but we need a idem closed channel to handle
	// all possible reads of the timer by our children.
	timerGone := NewIdemCloseChan()
	if atMost > 0 {
		to = time.After(atMost)
	}
	vv("in waitTilDoneOrAtMost, atMost = %v, visitSelf=%v in h Halter=%p", atMost, visitSelf, h)
	h.visit(visitSelf, func(y *Halter) {
		if isCycle[y] {
			vv("cycle detected, not waiting on y=%p again", y)
			return
		}
		if isCycle[h] {
			vv("cycle detected, not waiting on h=%p again", h)
			return
		}
		isCycle[y] = true
		vv("timeout in waitTilDoneOrAtMost t=%p, atMost = %v, visitSelf=%v in h Halter=%p, y = %p", to, atMost, visitSelf, h, y)
		select {
		case <-y.Done.Chan:
		case <-to:
			timerGone.Close() // since timers are only good once:
		case <-timerGone.Chan:
		case <-giveup:
		}
	})
}

// visit calls f on each member of h's Halter tree in
// a pre-order traversal. f(h) is called,
// and then f(d) is called recusively on
// all descendants d of h.
func (h *Halter) visit(visitSelf bool, f func(y *Halter)) {
	vv("visit(visitSelf=%v) h=%p", visitSelf, h)
	if visitSelf {
		f(h)
	}

	mut.Lock()
	bairn := append([]*Halter{}, h.children...)
	mut.Unlock()

	for _, child := range bairn {
		child.visit(true, f)
	}
}

// WaitTilClosed does not return until either
// a) we and all our children have closed our
// channels; or b) the supplied giveup channel
// has been closed.
//
// We recursively call WaitTilClosed on all of
// our children, and then wait for our own close.
//
// Note we cannot use a time.After channel for giveup,
// since that only fires once--we have possibly
// many children to wait for.
// Hence giveup must be _closed_, not just sent on,
// to have them all stop waiting.
//
// If the giveup channel was closed, the returned
// error will be ErrGiveUp. Otherwise it will
// be the first Reason() supplied by any
// CloseWithReason() calls in the tree. The
// first non-nil 'why' error we encounter
// is sticky. If err is returned nil, you
// know that either CloseWithReason was
// not used, or that there were only nil
// reason closes.
func (c *IdemCloseChan) WaitTilClosed(giveup <-chan struct{}) (err error) {

	// true here means close ourselves before returning.
	err = c.helperWaitTilClosed(giveup, true)
	if err != nil {
		return
	}

	mut.Lock()
	bairn := append([]*IdemCloseChan{}, c.children...)
	mut.Unlock()

	for _, child := range bairn {
		err = child.FirstTreeReason()
		if err != nil {
			return err
		}
	}
	return
}

// PRE: the lock is not held. We never hold
// hold it for long either.
func (c *IdemCloseChan) helperWaitTilClosed(giveup <-chan struct{}, closeSelf bool) (err error) {
	why, isClosed := c.Reason()
	if isClosed {
		err = why
		return
	}
	// a little bit of protection against simultaneous mutation
	// since we don't/can't hold the lock during recursion.
	mut.Lock()
	bairn := append([]*IdemCloseChan{}, c.children...)
	mut.Unlock()

	for _, child := range bairn {
		err1 := child.helperWaitTilClosed(giveup, true)
		// first error is sticky
		if err == nil {
			err = err1
		}
	}
	if closeSelf {
		// wait for our own close.
		select {
		case <-c.Chan:
			// first error is still sticky, don't overwrite with c.Reason().
			if err != nil {
				return
			}
			err = c.FirstTreeReason()
			return
		case <-giveup:
			return ErrGiveUp
		}
	}
	return
}

// WaitTilChildrenClosed is just like WaitTilClosed, but
// does not require (or check) if we ourselves, the root
// of our tree, is closed.
func (c *IdemCloseChan) WaitTilChildrenClosed(giveup <-chan struct{}) (err error) {

	err = c.helperWaitTilClosed(giveup, false)
	if err != nil {
		return
	}

	mut.Lock()
	bairn := append([]*IdemCloseChan{}, c.children...)
	mut.Unlock()

	for _, child := range bairn {
		err = child.FirstTreeReason()
		if err != nil {
			return err
		}
	}
	return
}

// FirstTreeReason scans the whole tree root at c
// for the first non-nil close reason, and returns it.
// If the tree is not completely closed at any node,
// it returns ErrNotClosed.
func (c *IdemCloseChan) FirstTreeReason() (err error) {
	mut.Lock()
	defer mut.Unlock()
	return c.nolockingFirstTreeReason()
}

func (c *IdemCloseChan) nolockingFirstTreeReason() (err error) {

	why, isClosed := c.nolockingReason()
	if !isClosed {
		return ErrNotClosed
	}
	if why != nil {
		return why
	}

	for _, child := range c.children {
		err = child.nolockingFirstTreeReason()
		if err != nil {
			return err
		}
	}
	return
}

// TaskDone subtracts one from the task count.
func (c *IdemCloseChan) TaskDone() int {
	return c.TaskAdd(-1)
}

// TaskAdd adds delta to the taskCount. When
// the taskCount reaches zero (or below),
// this channel will be closed.
//
// TaskAdd, TaskDone, TaskWait are like
// sync.WaitGroup but with sane integration
// with other channels; use the giveup channel
// in TaskWait, or just integreate c.Chan
// into your own select loop.
func (c *IdemCloseChan) TaskAdd(delta int) (newval int) {
	mut.Lock()
	defer mut.Unlock()
	return c.nolockingTaskAdd(delta)
}

func (c *IdemCloseChan) nolockingTaskAdd(delta int) (newval int) {
	c.taskCount += delta
	newval = c.taskCount
	if c.taskCount <= 0 {
		//vv("taskCount is now %v, closing ourselves/all children.", c.taskCount)
		// inlined CloseWithReason, since we hold the mut.
		if !c.closed {
			c.whyClosed = ErrTasksAllDone
			close(c.Chan)
			c.closed = true
			for _, child := range c.children {
				child.nolockingCloseWithReason(c.whyClosed)
			}
		}
	} else {
		//vv("taskCount is now %v", c.taskCount)
	}
	return
}

// TaskWait waits to return until either c or giveup
// is closed, whever happens first.
//
// TaskAdd, TaskDone, TaskWait are like
// sync.WaitGroup but with sane integration
// with other channels.
//
// You can readily integrate c.Chan into your
// own select statement.
func (c *IdemCloseChan) TaskWait(giveup <-chan struct{}) (err error) {
	select {
	case <-giveup:
		return ErrGiveUp
	case <-c.Chan:
		err, _ = c.Reason()
	}
	return
}
