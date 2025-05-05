package rpc25519

import (
	//"context"
	//"crypto/ed25519"
	//"crypto/tls"
	"fmt"
	//"io"
	//"log"
	//"net"
	//"strings"
	mathrand2 "math/rand/v2"
	"sync/atomic"
	"testing/synctest"
	"time"

	"github.com/glycerine/idem"
	rb "github.com/glycerine/rbtree"
)

type SimNetConfig struct{}

type simnet struct {
	scenario *scenario

	cfg       *Config
	simNetCfg *SimNetConfig
	netAddr   *SimNetAddr // satisfy uConn

	srv *Server
	cli *Client

	clinode *simnode
	srvnode *simnode

	// for now just clinode and srvnode in nodes;
	// plan is to add full network.
	nodes []*simnode

	cliReady chan *Client
	halt     *idem.Halter // just srv.halt for now.

	msgSendCh     chan *mop
	msgReadCh     chan *mop
	addTimer      chan *mop
	newScenarioCh chan *scenario
	nextTimer     *time.Timer
	lastArmTm     time.Time
}

type simnode struct {
	name    string
	LC      int64
	readQ   *pq
	preArrQ *pq
	timerQ  *pq
	net     *simnet
}

func (s *simnet) newSimnode(name string) *simnode {
	return &simnode{
		name:    name,
		readQ:   newPQinitTm(name + " readQ "),
		preArrQ: s.newPQarrivalTm(name + " preArrQ "),
		timerQ:  newPQcompleteTm(name + " timerQ "),
		net:     s,
	}
}

func (cfg *Config) newSimNetOnServer(simNetConfig *SimNetConfig, srv *Server) *simnet {

	scen := newScenario(time.Second, time.Second, time.Second, [32]byte{})

	// server creates simnet; must start server first.
	s := &simnet{

		cfg:       cfg,
		srv:       srv,
		halt:      srv.halt,
		cliReady:  make(chan *Client),
		simNetCfg: simNetConfig,
		msgSendCh: make(chan *mop),
		msgReadCh: make(chan *mop),
		addTimer:  make(chan *mop),

		newScenarioCh: make(chan *scenario),
		scenario:      scen,
		// high duration b/c no need to fire spuriously
		// and force the Go runtime to do extra work when
		// we are about to s.nextTimer.Stop() just below.
		nextTimer: time.NewTimer(time.Hour * 10_000),
	}
	s.nextTimer.Stop()
	s.clinode = s.newSimnode("CLIENT")
	s.srvnode = s.newSimnode("SERVER")

	s.nodes = []*simnode{s.clinode, s.srvnode}

	// let client find the shared simnet in their cfg.
	cfg.simnetRendezvous.mut.Lock()
	cfg.simnetRendezvous.simnet = s
	cfg.simnetRendezvous.mut.Unlock()

	s.Start()
	return s
}

var simnetLastMopSn int64

func simnetNextMopSn() int64 {
	return atomic.AddInt64(&simnetLastMopSn, 1)
}

type mopkind int

const (
	TIMER mopkind = 1
	SEND  mopkind = 2
	READ  mopkind = 3
)

func (k mopkind) String() string {
	switch k {
	case TIMER:
		return "TIMER"
	case SEND:
		return "SEND"
	case READ:
		return "READ"
	default:
		return fmt.Sprintf("unknown mopkind %v", int(k))
	}
}

// leave the cli/srv setup in place to avoid the
// startup overhead for every time, and test
// at the peer/ckt/frag level a particular test scenario.
type scenario struct {
	seed [32]byte
	rng  *mathrand2.ChaCha8

	tick   time.Duration
	minHop time.Duration
	maxHop time.Duration
}

func newScenario(tick, minHop, maxHop time.Duration, seed [32]byte) *scenario {
	return &scenario{
		seed:   seed,
		rng:    mathrand2.NewChaCha8(seed),
		tick:   tick,
		minHop: minHop,
		maxHop: maxHop,
	}
}
func (s *scenario) rngHop() (hop time.Duration) {
	if s.maxHop <= s.minHop {
		return s.minHop
	}
	vary := s.maxHop - s.minHop
	r := s.rng.Uint64()
	r = r % uint64(vary)
	if r < 0 {
		r = -r
	}
	r += uint64(s.minHop)
	hop = time.Duration(r)
	return
}

func (s *simnet) rngTieBreaker() int {
	return s.scenario.rngTieBreaker()
}
func (s *scenario) rngTieBreaker() int {
	for {
		a := s.rng.Uint64()
		b := s.rng.Uint64()
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		// loop and try again on ties.
	}
}

func (pq *pq) String() (r string) {
	i := 0
	r = fmt.Sprintf("\n ------- %v %v PQ --------\n", pq.owner, pq.orderby)
	for it := pq.tree.Min(); it != pq.tree.Limit(); it = it.Next() {

		item := it.Item() // interface{}
		if IsNil(item) {
			panic("do not put nil into the pq")
		}
		op := item.(*mop)
		r += fmt.Sprintf("pq[%2d] = %v\n", i, op)
		i++
	}
	if i == 0 {
		r += fmt.Sprintf("empty PQ\n")
	}
	return
}

// Message operation
type mop struct {
	sn int64

	// number of times handleSend() has seen this mop.
	seen int

	originCli bool

	senderLC int64
	readerLC int64
	originLC int64

	timerC   chan time.Time
	timerDur time.Duration
	//timerStarted time.Time // replace with the more generate initTm

	// when was the operation initiated?
	initTm time.Time

	// when did the send message arrive?
	arrivalTm time.Time

	// when: when the operation completes and
	// control returns to user code.
	// READS: when the read returns to user who called readMessage()
	// SENDS: when the send returns to user who called sendMessage()
	// TIMERS: when user code <-timerC gets sent the current time.
	completeTm time.Time

	kind mopkind
	msg  *Message

	sendmop *mop // for reads, which send did we get?
	readmop *mop // for sends, which read did we go to?

	pqit rb.Iterator

	// clients of scheduler wait on proceed.
	// timer fires, send delivered, read accepted by kernel
	proceed chan struct{}
}

func (op *mop) String() string {
	var msgSerial int64
	if op.msg != nil {
		msgSerial = op.msg.HDR.Serial
	}
	who := "SERVER"
	if op.originCli {
		who = "CLIENT"
	}
	// var verb string
	// switch op.kind {
	// case SEND:
	// 	verb = fmt.Sprintf("init at %v", op.initTm)
	// case READ:
	// 	verb = "initiated"
	// case TIMER:
	// 	verb = fmt.Sprintf("%v set for %v ", op.timerDur, op.completeTm)
	// }
	now := time.Now()
	var ini, arr, complete string
	if op.initTm.IsZero() {
		ini = "unk"
	} else {
		ini = fmt.Sprintf("%v", op.initTm.Sub(now))
	}
	if op.arrivalTm.IsZero() {
		arr = "unk"
	} else {
		arr = fmt.Sprintf("%v", op.arrivalTm.Sub(now))
	}
	if op.completeTm.IsZero() {
		complete = "unk"
	} else {
		complete = fmt.Sprintf("%v", op.completeTm.Sub(now))
	}
	return fmt.Sprintf("mop{%v %v init:%v, arr:%v, complete:%v op.sn:%v, msg.sn:%v}", who, op.kind, ini, arr, complete, op.sn, msgSerial)
}

func who(isCli bool) string {
	if isCli {
		return "CLIENT"
	} else {
		return "SERVER"
	}
}

type pq struct {
	owner   string
	orderby string
	tree    *rb.Tree
}

func (s *pq) peek() *mop {
	if s.tree.Len() == 0 {
		return nil
	}
	it := s.tree.Min()
	return it.Item().(*mop)
}

func (s *pq) pop() *mop {
	if s.tree.Len() == 0 {
		return nil
	}
	it := s.tree.Min()
	top := it.Item().(*mop)
	s.tree.DeleteWithIterator(it)
	return top
}

func (s *pq) add(op *mop) (added bool, it rb.Iterator) {
	if op == nil {
		panic("do not put nil into pq!")
	}
	added, it = s.tree.InsertGetIt(op)
	return
}

// order by arrivalTm; for the pre-arrival preArrQ.
func (s *simnet) newPQarrivalTm(owner string) *pq {
	return &pq{
		owner:   owner,
		orderby: "arrivalTm",
		tree: rb.NewTree(func(a, b rb.Item) int {
			av := a.(*mop)
			bv := b.(*mop)

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

			if av.arrivalTm.Before(bv.arrivalTm) {
				return -1
			}
			if av.arrivalTm.After(bv.arrivalTm) {
				return 1
			}
			// INVAR arrivalTm equal, break ties
			// with the permutor
			return s.rngTieBreaker()
		}),
	}
}

// order by mop.initTm, then mop.sn;
// for reads (readQ).
func newPQinitTm(owner string) *pq {
	return &pq{
		owner:   owner,
		orderby: "initTm",
		tree: rb.NewTree(func(a, b rb.Item) int {
			av := a.(*mop)
			bv := b.(*mop)

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

			if av.initTm.Before(bv.initTm) {
				return -1
			}
			if av.initTm.After(bv.initTm) {
				return 1
			}
			// INVAR initTm equal, but delivery order should not matter...
			// we can check that with chaos tests... to break ties here
			// could just use mop.sn ? try, b/c want determinism/repeatability...
			// but this is not really deterministic, is it?!!! different
			// goro can create their sn first...
			if av.sn < bv.sn {
				return -1
			}
			if av.sn > bv.sn {
				return 1
			}
			// must be the same if same sn.
			return 0

			// // if av.completeTm.Before(bv.completeTm) {
			// // 	return -1
			// // }
			// // if av.completeTm.After(bv.completeTm) {
			// // 	return 1
			// // }
			//
			// // Hopefully not, but just in case...
			// // av.msg could be nil (so could bv.msg)
			// if av.msg == nil && bv.msg == nil {
			// 	return 0
			// }
			// if av.msg == nil {
			// 	return -1
			// }
			// if bv.msg == nil {
			// 	return 1
			// }
			//
			// if av.msg.HDR.CallID == bv.msg.HDR.CallID {
			// 	if av.msg.HDR.Serial == bv.msg.HDR.Serial {
			// 		return 0
			// 	}
			// 	if av.msg.HDR.Serial < bv.msg.HDR.Serial {
			// 		return -1
			// 	}
			// 	return 1
			// }
			// if av.msg.HDR.CallID < bv.msg.HDR.CallID {
			// 	return -1
			// }
			// return 1
		}),
	}
}

// order by mop.completeTm then mop.sn; for timers
func newPQcompleteTm(owner string) *pq {
	return &pq{
		owner:   owner,
		orderby: "completeTm",
		tree: rb.NewTree(func(a, b rb.Item) int {
			av := a.(*mop)
			bv := b.(*mop)

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

			if av.completeTm.Before(bv.completeTm) {
				return -1
			}
			if av.completeTm.After(bv.completeTm) {
				return 1
			}
			// INVAR when equal, delivery order should not matter?
			// could just use mop.sn ? yes, b/c want determinism/repeatability.
			// but this is not really deterministic, is it?!!!
			if av.sn < bv.sn {
				return -1
			}
			if av.sn > bv.sn {
				return 1
			}
			// must be the same if same sn.
			return 0

			// // if av.initTm.Before(bv.initTm) {
			// // 	return -1
			// // }
			// // if av.initTm.After(bv.initTm) {
			// // 	return 1
			// // }
			//
			// // Hopefully not, but just in case...
			// // av.msg could be nil (so could bv.msg)
			// if av.msg == nil && bv.msg == nil {
			// 	return 0
			// }
			// if av.msg == nil {
			// 	return -1
			// }
			// if bv.msg == nil {
			// 	return 1
			// }
			// // INVAR: a.initTm == b.initTm if we uncomment above.
			//
			// if av.msg.HDR.CallID == bv.msg.HDR.CallID {
			// 	if av.msg.HDR.Serial == bv.msg.HDR.Serial {
			// 		return 0
			// 	}
			// 	if av.msg.HDR.Serial < bv.msg.HDR.Serial {
			// 		return -1
			// 	}
			// 	return 1
			// }
			// if av.msg.HDR.CallID < bv.msg.HDR.CallID {
			// 	return -1
			// }
			// return 1
		}),
	}
}

func (s *simnet) handleSend(send *mop) {
	////zz("top of handleSend(send = '%v')", send)

	if send.seen == 0 {
		if send.originCli {
			send.senderLC = s.clinode.LC
			send.originLC = s.clinode.LC
		} else {
			send.senderLC = s.srvnode.LC
			send.originLC = s.srvnode.LC
		}
		send.arrivalTm = send.initTm.Add(s.scenario.rngHop())
	}
	send.seen++
	if send.seen != 1 {
		panic(fmt.Sprintf("should see each send only once now, not %v", send.seen))
	}

	if send.originCli {
		s.srvnode.preArrQ.add(send)
		//vv("cli.LC:%v  SEND TO SERVER %v", s.clinode.LC, send)
		////zz("cli.LC:%v  SEND TO SERVER %v    srvPreArrQ: '%v'", s.clinode.LC, send, s.srvnode.preArrQ)
	} else {
		s.clinode.preArrQ.add(send)
		//vv("srv.LC:%v  SEND TO CLIENT %v", s.srvnode.LC, send)
		////zz("srv.LC:%v  SEND TO CLIENT %v    cliPreArrQ: '%v'", s.srvnode.LC, send, s.clinode.preArrQ)
	}
	// rpc25519 peer/ckt/frag does async sends, so let
	// the sender keep going.
	// We could optionally (chaos?) add some
	// delay, but then we'd need another "send finished" PQ,
	// which is just extra we probably don't need. At
	// least for now.
	send.completeTm = time.Now() // on the sender side.
	close(send.proceed)
}

func (s *simnet) handleRead(read *mop) {
	////zz("top of handleRead(read = '%v')", read)

	if read.seen == 0 {
		if read.originCli {
			//read.senderLC = s.clinode.LC
			read.originLC = s.clinode.LC
		} else {
			//read.senderLC = s.srvnode.LC
			read.originLC = s.srvnode.LC
		}
		// read.completeTm?
		// unknown yet, dispatcher will assign
		// when the send arrives.
	}
	read.seen++
	if read.seen != 1 {
		panic(fmt.Sprintf("should see each send only once now, not %v", read.seen))
	}

	if read.originCli {
		s.clinode.readQ.add(read)
		//vv("cliLC:%v  READ at CLIENT: %v", s.clinode.LC, read)
		////zz("cliLC:%v  READ %v at CLIENT, now cliReadQ: '%v'", s.clinode.LC, read, s.clinode.readQ)
		s.clinode.dispatch()
	} else {
		s.srvnode.readQ.add(read)
		//vv("srvLC:%v  READ at SERVER: %v", s.srvnode.LC, read)
		////zz("srvLC:%v  READ %v at SERVER, now srvReadQ: '%v'", s.srvnode.LC, read, s.srvnode.readQ)
		s.srvnode.dispatch()
	}
}

// dispatch delivers sends to reads, and fires timers.
// calls node.net.armTimer() at the end.
func (node *simnode) dispatch() (bump time.Duration) {

	// to be deleted at the end, so
	// we don't dirupt the iteration order
	// and miss something.
	var preDel []*mop
	var readDel []*mop
	var timerDel []*mop

	// We had an early bug from returning early and
	// forgetting about preDel, readDel, timerDel
	// that were waiting for deletion at
	// the end. Doing the deletes here in
	// a defer allows us to return safely at any
	// point, and still do the required cleanup.
	defer func() {
		// take care of any deferred-to-keep-sane iteration deletes
		for _, op := range preDel {
			////zz("delete '%v'", op)
			node.preArrQ.tree.DeleteWithKey(op)
		}
		for _, op := range readDel {
			////zz("delete '%v'", op)
			node.readQ.tree.DeleteWithKey(op)
		}
		for _, op := range timerDel {
			////zz("delete '%v'", op)
			node.timerQ.tree.DeleteWithKey(op)
		}
		node.net.armTimer()
		//vv("=== end of dispatch %v", node.name)
		narr := node.preArrQ.tree.Len()
		nread := node.readQ.tree.Len()
		// it is normal to have preArrQ if no reads...
		if narr > 0 && nread > 0 {
			alwaysPrintf("ummm... why did these not get dispatched? narr = %v, nread = %v; summary node summary:\n%v", narr, nread, node.String())
			panic("should have been dispatchable, no?")
		}
	}()

	nT := node.timerQ.tree.Len()  // number of timers
	nR := node.readQ.tree.Len()   // number of reads
	nS := node.preArrQ.tree.Len() // number of sends

	if nT == 0 && nR == 0 && nS == 0 {
		return
	}

	readIt := node.readQ.tree.Min()
	preIt := node.preArrQ.tree.Min()
	timerIt := node.timerQ.tree.Min()

	now := time.Now()

	// do timers first, so we can exit
	// immediately if no sends and reads match.
	for ; timerIt != node.timerQ.tree.Limit(); timerIt = timerIt.Next() {

		timer := timerIt.Item().(*mop)
		//vv("check TIMER: %v", timer)

		if !now.Before(timer.completeTm) {
			// timer.completeTm <= now
			//vv("have TIMER firing")
			timerDel = append(timerDel, timer)
			select { // hung here! how long should we wait.. why cannot deliver? what does the timer do? default: nothing case.
			case timer.timerC <- now:
				//vv("sent on timerC")
			default:
				// this is what the Go runtime does. otherwise our clock gets 0.01 increments each time...
				vv("timerC default: giving up on timer immediately!")
				//case <-time.After(time.Millisecond * 10):
				//vv("giving up on timer after 10 msec: '%v'", timer)
				//panic("giving up on timer?!? why blocked? or must we ignore?")
			case <-node.net.halt.ReqStop.Chan:
				return
			}
		} else {
			// smallest timer > now
			break // check send->read next, don't return yet.
		}
	}

	for {
		if readIt == node.readQ.tree.Limit() {
			return
		}
		if preIt == node.preArrQ.tree.Limit() {
			return
		}

		read := readIt.Item().(*mop)
		send := preIt.Item().(*mop)

		// the event of receiving the msg is after
		// any LC advance

		// *** here is where we could re-order
		// messages in a chaos test.

		// reads can start before sends,
		// the read blocks until they match.
		//
		// reads can start after sends,
		// the kernel buffers the sends
		// until the read attempts (our pre-arrival queue).

		// Causality also demands that
		// a read can complete (now) only after it was initiated;
		// and so can be matched (now) only to a send already initiated.

		// To keep from violating causality,
		// during our chaos testing, we want
		// to make sure that the read completion (now)
		// cannot happen before the send initiation.
		// Also forbid any reads that have not happened
		// "yet" (now), should they get rearranged by chaos.
		if now.Before(read.initTm) { // now < read.initTm
			// are we done? since readQ is ordered
			// by initTm, all subsequent reads in it
			// will have >= initTm.
			vv("rejecting delivery to read that has not happened: '%v'", read)
			panic("how possible?")
			return
		}
		// INVAR: this read.initTm <= now

		if now.Before(send.arrivalTm) { // now < send.arrivalTm
			// are we done? since preArrQ is ordered
			// by arrivalTm, all subsequent pre-arrivals (sends)
			// will have even >= arrivalTm.
			vv("rejecting deliver of send that has not happened: '%v'", send)
			// TODO: I think we might need to set a timer on its delivery then!
			return
		}
		// INVAR: this send.arrivalTm <= now

		// Since both have happened, they can be matched.

		// Service this read with this send.

		read.msg = send.msg.CopyForSimNetSend()
		// advance our logical clock
		node.LC = max(node.LC, send.originLC) + 1
		////zz("servicing cli read: started LC %v -> serviced %v (waited: %v) read.sn=%v", read.originLC, node.LC, node.LC-read.originLC, read.sn)

		// track clocks on either end for this send and read.
		read.readerLC = node.LC
		read.senderLC = send.senderLC
		send.readerLC = node.LC
		read.completeTm = now
		read.arrivalTm = send.arrivalTm // easier diagnostics

		// matchmaking
		//vv("[1]matchmaking: \nsend '%v' -> \nread '%v'", send, read)
		read.sendmop = send
		send.readmop = read

		preDel = append(preDel, send)
		readDel = append(readDel, read)

		close(read.proceed)
		// used to have this. but in reality
		// our sends are asynchronous not RPCs,
		// so we don't block. This should happen
		// as soon as we have the send assigned
		// to the pre-arrival queue.
		//close(send.proceed)

		readIt = readIt.Next()
		preIt = preIt.Next()
		//} else {
		// INVAR: smallest read.originLC <= smallest send.originLC
		//}
	}
}

func (s *simnet) qReport() (r string) {
	for _, node := range s.nodes {
		r += node.String()
	}
	return
}

func (node *simnode) String() (r string) {
	r += node.name + " Q summary:\n"
	r += node.readQ.String()
	r += node.preArrQ.String()
	r += node.timerQ.String()
	return
}

func (s *simnet) Start() {
	////zz("simnet.Start() top")
	go s.scheduler()
}

func (s *simnet) schedulerReport() string {
	now := time.Now()
	return fmt.Sprintf("lastArmTm.After(now) = %v [%v out] %v; qReport = '%v'", s.lastArmTm.After(now), s.lastArmTm.Sub(now), s.lastArmTm, s.qReport())
}

// makes it clear on a stack trace which goro this is.
func (s *simnet) scheduler() {

	defer func() {
		r := recover()
		if r != nil {
			vv("scheduler panic-ing: %v", s.schedulerReport())
			panic(r)
		}
	}()

	// init phase

	// get a client before anything else.
	select {
	case s.cli = <-s.cliReady:
		////zz("simnet got cli")
	case <-s.halt.ReqStop.Chan:
		return
	}

	// main scheduler loop
	for i := int64(0); ; i++ {
		// each scheduler loop tick is an event.
		s.clinode.LC++
		s.srvnode.LC++
		cliLC := s.clinode.LC
		srvLC := s.srvnode.LC
		_, _ = cliLC, srvLC

		now := time.Now()
		_ = now
		////zz("scheduler top cli.LC = %v ; srv.LC = %v", cliLC, srvLC)
		vv("scheduler top %v", s.schedulerReport())

		// easier debugging
		s.clinode.dispatch()
		s.srvnode.dispatch()

		// vs. eventually
		//for _, node := range s.nodes {
		//	node.dispatch()
		//}
		s.armTimer()

		// advance time by one tick
		time.Sleep(s.scenario.tick)
		synctest.Wait()

		select {
		case alert := <-s.nextTimer.C: // soonest timer fires
			_ = alert
			//vv("s.nextTimer -> alerted at %v", alert)
			s.clinode.dispatch()
			s.srvnode.dispatch()
			s.armTimer()

		case scenario := <-s.newScenarioCh:
			s.finishScenario()
			s.initScenario(scenario)

		case timer := <-s.addTimer:
			//vv("addTimer ->  op='%v'", timer)
			s.handleTimer(timer)

		case send := <-s.msgSendCh:
			////zz("msgSendCh ->  op='%v'", send)
			s.handleSend(send)

		case read := <-s.msgReadCh:
			////zz("msgReadCh ->  op='%v'", read)
			s.handleRead(read)

		case <-s.halt.ReqStop.Chan:
			return
		}
	}
}

func (s *simnet) finishScenario() {
	// do any tear down work...

	// at the end
	s.scenario = nil
}
func (s *simnet) initScenario(scenario *scenario) {
	s.scenario = scenario
	// do any init work...
}

func (s *simnet) handleTimer(timer *mop) {

	lc := s.srvnode.LC
	who := "SERVER"
	if timer.originCli {
		who = "CLIENT"
		lc = s.clinode.LC
	}
	_, _ = who, lc
	//vv("handleTimer() %v  TIMER SET; LC = %v", who, lc)

	if timer.seen == 0 {
		if timer.originCli {
			timer.senderLC = s.clinode.LC
			timer.originLC = s.clinode.LC
		} else {
			timer.senderLC = s.srvnode.LC
			timer.originLC = s.srvnode.LC
		}
		timer.timerC = make(chan time.Time)
		defer close(timer.proceed)
	}
	timer.seen++

	if timer.originCli {
		s.clinode.timerQ.add(timer)
		////zz("cli.LC:%v CLIENT set TIMER %v to fire at '%v'; now timerQ: '%v'", s.clinode.LC, timer, timer.completeTm, s.clinode.timerQ)
	} else {
		s.srvnode.timerQ.add(timer)
		////zz("srv.LC:%v SERVER set TIMER %v to fire at '%v'; now timerQ: '%v'", s.srvnode.LC, timer, timer.completeTm, s.srvnode.timerQ)
	}

	s.armTimer()
}

func (s *simnet) armTimer() {

	var minTimer *mop
	for _, node := range s.nodes {
		minTimer = node.soonestTimerLessThan(minTimer)
	}
	if minTimer == nil {
		return
	}
	now := time.Now()
	dur := minTimer.completeTm.Sub(now)
	////zz("dur=%v = when(%v) - now(%v)", dur, minTimer.completeTm, now)
	s.lastArmTm = now
	s.nextTimer.Reset(dur)
}

func (node *simnode) soonestTimerLessThan(bound *mop) *mop {
	it := node.timerQ.tree.Min()
	if it == node.timerQ.tree.Limit() {
		// we have no timers
		return bound
	}
	minTimer := it.Item().(*mop)
	if bound == nil {
		// no lower bound yet
		return minTimer
	}
	if minTimer.completeTm.Before(bound.completeTm) {
		return minTimer
	}
	return bound
}

//=========================================
// The EXTERNAL client access routines are below.
//
// These are goroutine safe; okay to call from
// any goroutine. Implementation note:
//
// createNewTimer(), sendMessage(), readMessage()
//
// must never touch anything internal
// to simnet (else data races).
//
// Communicate over channels only:
//   s.addTimer
//   s.msgReadCh
//   s.msgSendCh
//   s.halt.ReqStop.Chan
//
// and the helper routines that setup the
// channel ops, also below:
//
//   newTimerMop(), newSendMop(), newReadMop()
//
//=========================================

// called by goroutines outside of the scheduler,
// so must not touch s.srvnode, s.clinode, etc.
func (s *simnet) createNewTimer(dur time.Duration, begin time.Time, isCli bool) (timerC chan time.Time, err error) {

	who := "SERVER"
	if isCli {
		who = "CLIENT"
	}
	_ = who
	//vv("top simnet.createNewTimer() %v SETS TIMER dur='%v' begin='%v' => when='%v'", who, dur, begin, begin.Add(dur))

	timer := newTimerMop(isCli)
	timer.timerDur = dur
	timer.initTm = begin
	timer.completeTm = begin.Add(dur)

	select {
	case s.addTimer <- timer:
	case <-s.halt.ReqStop.Chan:
		return nil, ErrShutdown()
	}
	select {
	case <-timer.proceed:
		return timer.timerC, nil
	case <-s.halt.ReqStop.Chan:
		return nil, ErrShutdown()
	}
	return
}

// readMessage reads a framed message from conn.
func (s *simnet) readMessage(conn uConn) (msg *Message, err error) {

	isCli := conn.(*simnetConn).isCli

	//vv("top simnet.readMessage() %v READ", who(isCli))

	read := newReadMop(isCli)
	read.initTm = time.Now()

	select {
	case s.msgReadCh <- read:
	case <-s.halt.ReqStop.Chan:
		return nil, ErrShutdown()
	}
	select {
	case <-read.proceed:
		msg = read.msg
	case <-s.halt.ReqStop.Chan:
		return nil, ErrShutdown()
	}
	return
}

func (s *simnet) sendMessage(conn uConn, msg *Message, timeout *time.Duration) error {

	isCli := conn.(*simnetConn).isCli

	//vv("top simnet.sendMessage() %v SEND  msg.Serial=%v", who(isCli), msg.HDR.Serial)

	send := newSendMop(msg, isCli)
	send.initTm = time.Now()
	select {
	case s.msgSendCh <- send:
	case <-s.halt.ReqStop.Chan:
		return ErrShutdown()
	}
	select {
	case <-send.proceed:
	case <-s.halt.ReqStop.Chan:
		return ErrShutdown()
	}
	return nil
}

func newTimerMop(isCli bool) (op *mop) {
	op = &mop{
		originCli: isCli,
		sn:        simnetNextMopSn(),
		kind:      TIMER,
		proceed:   make(chan struct{}),
	}
	return
}

func newReadMop(isCli bool) (op *mop) {
	op = &mop{
		originCli: isCli,
		sn:        simnetNextMopSn(),
		kind:      READ,
		proceed:   make(chan struct{}),
	}
	return
}

func newSendMop(msg *Message, isCli bool) (op *mop) {
	op = &mop{
		originCli: isCli,
		msg:       msg,
		sn:        simnetNextMopSn(),
		kind:      SEND,
		proceed:   make(chan struct{}),
	}
	return
}
