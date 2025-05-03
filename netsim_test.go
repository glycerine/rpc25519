package rpc25519

import (
	//"context"
	"fmt"
	//"os"
	//"strings"
	mathrand2 "math/rand/v2"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	//cv "github.com/glycerine/goconvey/convey"
	//"github.com/glycerine/idem"
	rb "github.com/glycerine/rbtree"
)

var _ = fmt.Sprintf
var _ = time.Sleep

// waitQ is a priority queue giving priority to
// the the smallest timestamp (first/next) fop
// (Fragment operation). It is backed by a red-black tree.
// waitQ.peek() returns the fop
// such that it can be waited on
// with a time.After() channel that will be bubbled
// by the testing/synctest mechanism.
type waitQ struct {
	tree *rb.Tree
}

func (s *waitQ) peek() *fop {
	if s.tree.Len() == 0 {
		return nil
	}
	it := s.tree.Min()
	return it.Item().(*fop)
}

func (s *waitQ) pop() *fop {
	if s.tree.Len() == 0 {
		return nil
	}
	it := s.tree.Min()
	top := it.Item().(*fop)
	s.tree.DeleteWithIterator(it)
	return top
}

func (s *waitQ) add(op *fop) (added bool, it rb.Iterator) {
	added, it = s.tree.InsertGetIt(op)
	return
}

// order by when, then Frag(circuitID, fromID, sn...); try
// hard not to delete tickets with the same when,
// and even then we may have reason to keep
// the exact same ticket for a task at the same time;
// so use fop.sn too.
func newWaitQ() *waitQ {
	return &waitQ{
		tree: rb.NewTree(func(a, b rb.Item) int {
			av := a.(*fop)
			bv := b.(*fop)

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
			if av.when.Before(bv.when) {
				return -1
			}
			if av.when.After(bv.when) {
				return 1
			}
			// Hopefully not, but just in case...
			// av.frag could be nil (so could bv.frag)
			if av.frag == nil && bv.frag == nil {
				return 0
			}
			if av.frag == nil {
				return -1
			}
			if bv.frag == nil {
				return 1
			}
			// INVAR: a.when == b.when
			return av.frag.Compare(bv.frag)
		}),
	}
}

var netsimLastSn int64

func netsimNextSn() int64 {
	return atomic.AddInt64(&netsimLastSn, 1)
}

type netsim struct {
	//ckts map[string]*Circuit
	seed [32]byte
	rng  *mathrand2.ChaCha8

	msgSendCh chan *fop
}

// fop is a Fragment operation, either a
// time or half of a network operation
// such as a read or a send of a Fragment
// on a circuit. The netsim will match
// sends and reads, possibly with fault
// injection like not matching (dropping it),
// or delaying a message.
type fop struct {
	sn int64

	senderLC int64
	readerLC int64

	dur  time.Duration // timer duration
	when time.Time     // when read for READS, when sent for SENDS?

	sorter uint64

	kind mopkind
	frag *Fragment
	ckt  *Circuit

	sendfop *fop // for reads, which send did we get?
	readfop *fop // for sends, which read did we go to?

	pqit rb.Iterator

	FromPeerID string
	ToPeerID   string
	note       string

	// clients of scheduler wait on proceed.
	// timer fires, send delivered, read accepted by kernel
	proceed chan struct{}
}

func (op *fop) String() string {
	var spouse *fop
	var spousenote string
	switch op.kind {
	case READ:
		spouse = op.sendfop
		if spouse != nil {
			spousenote = spouse.note
		}
	case SEND:
		spouse = op.readfop
		if spouse != nil {
			spousenote = spouse.note
		}
	}
	return fmt.Sprintf("fop{kind:%v, from:%v, to:%v, sn:%v, when:%v, note:'%v'; spouse.note='%v'}", op.kind, op.FromPeerID, op.ToPeerID, op.sn, op.when, op.note, spousenote)
}

func newSend(ckt *Circuit, frag *Fragment, senderPeerID, note string) (op *fop) {
	op = &fop{
		note:       note,
		ckt:        ckt,
		frag:       frag,
		sn:         netsimNextSn(),
		kind:       SEND,
		proceed:    make(chan struct{}),
		FromPeerID: senderPeerID,
	}
	switch senderPeerID {
	case ckt.LocalPeerID:
		op.ToPeerID = ckt.RemotePeerID
	case ckt.RemotePeerID:
		op.ToPeerID = ckt.LocalPeerID
	default:
		panic("bad senderPeerID, not on ckt")
	}
	return
}

func newRead(ckt *Circuit, readerPeerID, note string) (op *fop) {
	op = &fop{
		note:     note,
		when:     time.Now(),
		ckt:      ckt,
		sn:       netsimNextSn(),
		kind:     READ,
		proceed:  make(chan struct{}),
		ToPeerID: readerPeerID,
	}
	switch readerPeerID {
	case ckt.LocalPeerID:
		op.FromPeerID = ckt.RemotePeerID
	case ckt.RemotePeerID:
		op.FromPeerID = ckt.LocalPeerID
	default:
		panic("bad readerPeerID, not on ckt")
	}
	return
}
func newTimer(dur time.Duration) *fop {
	return &fop{
		sn:      netsimNextSn(),
		kind:    TIMER,
		dur:     dur,
		when:    time.Now().Add(dur),
		proceed: make(chan struct{}),
	}
}

func newNetsim(seed [32]byte) (s *netsim) {
	s = &netsim{
		//ckts:     make(map[string]*Circuit),
		seed: seed,
		rng:  mathrand2.NewChaCha8(seed),
	}
	return
}

func (s *netsim) AddPeer(peerID string, ckt *Circuit) (err error) {
	//s.ckts[peerID] = ckt
	return nil
}

func Test500_synctest_basic(t *testing.T) {
	synctest.Run(func() {

		//var seed [32]byte
		//netsim := newNetsim(seed)
		//netsim.start()

		dur := time.Second * 10
		timer := newTimer(dur)

		addTimer := make(chan *fop)
		schedDone := make(chan struct{})
		defer close(schedDone)

		ckts := make(map[string]*Circuit)
		newCktCh := make(chan *Circuit)

		hop := time.Second * 2 // duration of network single hop/delay
		tick := time.Second

		// use ckt now
		cktReadCh := make(chan *fop)
		cktSendCh := make(chan *fop)

		readOn := func(ckt *Circuit, readerPeerID, note string) *fop {
			read := newRead(ckt, readerPeerID, note)
			cktReadCh <- read
			return read
		}
		sendOn := func(ckt *Circuit, frag *Fragment, fromPeerID, note string) *fop {
			if frag.FromPeerID != fromPeerID {
				panic("bad caller: sendOn with frag.FromPeerID != fromPeerID")
			}
			send := newSend(ckt, frag, frag.FromPeerID, note)
			cktSendCh <- send
			return send
		}

		// play the "scheduler" part
		go func() {
			pq := newWaitQ()
			var nextPQ <-chan time.Time

			showQ := func() {
				i := 0
				tsPrintfMut.Lock()
				fmt.Printf("\n ------- PQ --------\n")
				for it := pq.tree.Min(); it != pq.tree.Limit(); it = it.Next() {
					op := it.Item().(*fop)
					fmt.Printf("pq[%2d] = %v\n", i, op)
					i++
				}
				if i == 0 {
					fmt.Printf("empty PQ\n")
				}
				tsPrintfMut.Unlock()
			}

			handleSend := func(send *fop, first bool) {
				vv("top of handleSend, here is the Q prior to send: '%v'\n", send)
				showQ()

				if first {
					send.when = time.Now().Add(hop)
				}

				ckt, ok := ckts[send.frag.CircuitID]
				if !ok {
					panic(fmt.Sprintf("bad SEND, unregistered ckt op.frag.CircuitID='%v'", send.frag.CircuitID))
				}
				// buffer on ckt.sentFromLocal or ckt.sentFromRemote
				switch send.ToPeerID {
				case ckt.LocalPeerID:
					ckt.sentFromRemote = append(ckt.sentFromRemote, send)
				case ckt.RemotePeerID:
					ckt.sentFromLocal = append(ckt.sentFromLocal, send)
				default:
					panic(fmt.Sprintf("bad SEND op on ckt, not for local or remote. send.ToPeerID = '%v'; ckt.LocalPeerID='%v'; ckt.RemotePeerID='%v'", send.ToPeerID, ckt.LocalPeerID, ckt.RemotePeerID))
				}
			}

			handleRead := func(read *fop, first bool) {
				vv("top of handleRead, here is the Q prior to read='%v'\n", read)
				showQ()
				// read from ckt.sentFromLocal or ckt.sentFromRemote
				ckt, ok := ckts[read.ckt.CircuitID]
				if !ok {
					panic("bad READ op.frag.CircuitID; not in ckts")
				}
				vv("READ resources: len(ckt.sentFromLocal)=%v; and len(ckt.sentFromRemote)='%v'; op='%v'; reader is local=%v; reader is remote=%v", len(ckt.sentFromLocal), len(ckt.sentFromRemote), read, ckt.LocalPeerID == ckt.LocalPeerID, ckt.RemotePeerID == ckt.LocalPeerID)
				switch read.ToPeerID {
				case ckt.LocalPeerID:
					if len(ckt.sentFromRemote) > 0 {
						// can service the read
						send := ckt.sentFromRemote[0]
						read.frag = send.frag // TODO clone()?
						// matchmaking
						vv("[1]matchmaking send '%v' -> read '%v'", send, read)
						read.sendfop = send
						send.readfop = read

						ckt.sentFromRemote = ckt.sentFromRemote[1:]

						// why the asymmetry? this is ok, but not below?
						// think b/c we are randomly deleting?
						//pq.delOneItem(read.pqit)

						close(read.proceed)
						close(send.proceed)
					} else {
						//panic("stall the read?")
						vv("1st no sends for reader, stalling the read: len(ckt.sentFromLocal)=%v; and len(ckt.sentFromRemote)='%v'; read='%v'", len(ckt.sentFromLocal), len(ckt.sentFromRemote), read)
						read.when = time.Now().Add(tick)
						_, read.pqit = pq.add(read)

						//below: queueNext()

					}
				case ckt.RemotePeerID:
					if len(ckt.sentFromLocal) > 0 {
						// can service the read op
						send := ckt.sentFromLocal[0]
						read.frag = send.frag // TODO clone()?
						// matchmaking
						vv("[2]matchmaking send '%v' -> read '%v'", send, read)
						read.sendfop = send
						send.readfop = read

						ckt.sentFromLocal = ckt.sentFromLocal[1:]
						//pq.delOneItem(read.pqit)
						close(read.proceed)
						close(send.proceed)
					} else {
						//panic("stall the read?")
						vv("2nd no sends for reader, stalling the read: len(ckt.sentFromLocal)=%v; and len(ckt.sentFromRemote)='%v'; read='%v'", len(ckt.sentFromLocal), len(ckt.sentFromRemote), read)
						read.when = time.Now().Add(tick)
						_, read.pqit = pq.add(read)
						//below: queueNext()

					}
				default:
					panic("bad READ op on ckt, not for local or remote")
				}
			}

			queueNext := func() {
				vv("top of queueNext")
				showQ()
				next := pq.peek()
				if next != nil {
					wait := next.when.Sub(time.Now())
					nextPQ = time.After(wait)
				} else {
					vv("queueNext: empty PQ")
				}
			}
			for i := 0; ; i++ {
				if i > 0 {
					time.Sleep(tick)
				}
				select {
				case ckt := <-newCktCh:
					ckts[ckt.CircuitID] = ckt
					vv("registered ckt '%v'", ckt.CircuitID)

				case <-nextPQ:
					op := pq.peek()
					if op != nil {
						pq.pop()
						vv("got from <-nextPQ: op = %v. PQ is now:", op)
						showQ()
						switch op.kind {
						case READ:
							vv("have READ from <-nextPQ: op='%v'", op)
							handleRead(op, false)
						case SEND:
							vv("have SEND from <-nextPQ: op=%v", op)
							handleSend(op, false)
						case TIMER:
							vv("have TIMER firing from <-nextPQ")
							close(op.proceed)
						}
					} // end if op != nil
					vv("done with if op != nil, going to queueNext()")
					queueNext()

				case timer := <-addTimer:
					_, timer.pqit = pq.add(timer)
					queueNext()

				case send := <-cktSendCh:
					vv("send := <-cktSendCh")
					handleSend(send, true)
					// old:
					//  send.when = time.Now().Add(hop)
					//  _, send.pqit = pq.add(send)
					//  queueNext()
					//  close(send.proceed)

				case read := <-cktReadCh:
					vv("read := <-cktReadCh")
					handleRead(read, true)
					// old:
					//read := newRead()
					//_, read.pqit = pq.add(read)
					//queueNext()

				case <-schedDone:
					return
				}
			}
		}()

		cktID := "ckt1"
		reader1 := "reader1"
		sender1 := "sender1"
		ckt1 := &Circuit{
			LocalPeerID:  sender1,
			RemotePeerID: reader1,
			CircuitID:    cktID,
		}
		newCktCh <- ckt1

		// sender 1
		go func() {
			frag := NewFragment()
			frag.FromPeerID = sender1
			frag.ToPeerID = reader1
			frag.CircuitID = cktID
			frag.Serial = 1
			note := "from sender1 to reader1 on ckt1"
			send := sendOn(ckt1, frag, sender1, note)
			<-send.proceed
			vv("sender1 sent '%v'", note)

		}()

		/*
			// sender 2
			go func() {
				sendCh <- &Fragment{
					FromPeerID:  "sender2",
					ToPeerID:    "reader1",
					CircuitID:   "ckt1",
					FragSubject: "from sender2 to reader1 on ckt2"}
				vv("sender2 sent")
			}()
		*/

		// reader 1
		go func() {
			read := readOn(ckt1, reader1, "read1, reader1") // get a read op dest "reader1"
			<-read.proceed
			vv("reader 1 got 1st frag from %v; Serial=%v; op='%v'", read.FromPeerID, read.frag.Serial, read)

			read2 := readOn(ckt1, reader1, "read2, reader1") // get a read op dest "reader1"

			<-read2.proceed // blocked here so read2/send lost?
			vv("reader 1 got 2nd frag from %v; Serial=%v; op='%v'", read2.FromPeerID, read2.frag.Serial, read2)

		}()

		vv("dur=%v, about to wait on timer at %v", dur, time.Now())
		//netsim.addTimer <- timer
		addTimer <- timer
		<-timer.proceed
		vv("timer fired at %v", time.Now())

		// sender 2 sends on same ckt
		go func() {
			vv("sender2 is running")
			frag := NewFragment()
			frag.FromPeerID = sender1
			frag.ToPeerID = reader1
			frag.CircuitID = cktID
			frag.Serial = 2
			note := "Serial 2 message from sender2 to reader1 on ckt1"
			send := sendOn(ckt1, frag, sender1, note)
			<-send.proceed // blocked here, so send/read lost???
			vv("sender2 sent Serial 2")

		}()

		timer2 := newTimer(dur)
		vv("dur=%v, about to wait on 2nd timer at %v", dur, time.Now())
		//netsim.addTimer <- timer
		addTimer <- timer2
		<-timer2.proceed
		vv("timer2 fired at %v", time.Now())

		/*
			// Create a context.Context which is canceled after a timeout.
			const timeout = 5 * time.Second
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			// Wait just less than the timeout.
			time.Sleep(timeout - time.Nanosecond)
			synctest.Wait()
			fmt.Printf("before timeout: ctx.Err() = %v\n", ctx.Err())

			// Wait the rest of the way until the timeout.
			time.Sleep(time.Nanosecond)
			synctest.Wait()
			fmt.Printf("after timeout:  ctx.Err() = %v\n", ctx.Err())
		*/
	})
}
