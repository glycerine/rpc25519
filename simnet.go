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

	cliReady chan *Client
	halt     *idem.Halter // just srv.halt for now.

	msgSendCh     chan *mop
	msgReadCh     chan *mop
	addTimer      chan *mop
	newScenarioCh chan *scenario
	nextTimer     *time.Timer
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
		readQ:   newPQ(),     // ascending LC order
		preArrQ: newPQ(),     // ascending LC order
		timerQ:  newPQtime(), // ascending time.Time order
		net:     s,
	}
}

func (cfg *Config) newSimNetOnServer(simNetConfig *SimNetConfig, srv *Server) *simnet {

	scen := newScenario(time.Second, time.Second, [32]byte{})

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
		// really don't want a spurious timer firing.
		nextTimer: time.NewTimer(time.Hour * 10_000),
	}
	s.nextTimer.Stop()
	s.clinode = s.newSimnode("CLIENT")
	s.srvnode = s.newSimnode("SERVER")

	// let client find the shared simnet in their cfg.
	cfg.simnetRendezvous.simnet = s
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

	tick time.Duration
	hop  time.Duration
}

func newScenario(tick, hop time.Duration, seed [32]byte) *scenario {
	return &scenario{
		seed: seed,
		rng:  mathrand2.NewChaCha8(seed),
		tick: tick,
		hop:  hop,
	}
}

func (pq *pq) String() (r string) {
	i := 0
	r = fmt.Sprintf("\n ------- %v --------\n", pq.name)
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
		r = fmt.Sprintf("empty PQ\n")
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

	timerC       chan time.Time
	timerDur     time.Duration
	timerStarted time.Time

	// when: when the operation completes and
	// control returns to user code.
	// READS: when the read returns to user who called readMessage()
	// SENDS: when the send returns to user who called sendMessage()
	// TIMERS: when they go off.
	when time.Time

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
	var verb string
	switch op.kind {
	case SEND:
		verb = fmt.Sprintf("happend at %v", op.when)
	case READ:
		verb = "initiated"
	case TIMER:
		verb = fmt.Sprintf("%v set for %v ", op.timerDur, op.when)
	}
	return fmt.Sprintf("mop{%v %v %v originLC:%v, senderLC:%v, op.sn:%v, msg.sn:%v}", who, op.kind, verb, op.originLC, op.senderLC, op.sn, msgSerial)
}

func (s *simnet) newReadMsg(isCli bool) (op *mop) {
	op = &mop{
		originCli: isCli,
		sn:        simnetNextMopSn(),
		kind:      READ,
		proceed:   make(chan struct{}),
	}
	return
}

func (s *simnet) newSendMsg(msg *Message, isCli bool) (op *mop) {
	op = &mop{
		originCli: isCli,
		msg:       msg,
		sn:        simnetNextMopSn(),
		kind:      SEND,
		proceed:   make(chan struct{}),
	}
	return
}

// readMessage reads a framed message from conn.
func (s *simnet) readMessage(conn uConn) (msg *Message, err error) {

	isCli := conn.(*simnetConn).isCli

	vvIfPrintOn(func() string {
		var lc int64
		var who string
		if isCli {
			who = "CLIENT"
			lc = atomic.LoadInt64(&s.clinode.LC)
		} else {
			who = "SERVER"
			lc = atomic.LoadInt64(&s.srvnode.LC)
		}
		return fmt.Sprintf("top simnet.readMessage() %v READ  LC = %v", who, lc)
	})

	read := s.newReadMsg(isCli)
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

	vvIfPrintOn(func() string {
		var lc int64
		var who string
		if isCli {
			who = "CLIENT"
			lc = atomic.LoadInt64(&s.clinode.LC)
		} else {
			who = "SERVER"
			lc = atomic.LoadInt64(&s.srvnode.LC)
		}
		return fmt.Sprintf("top simnet.sendMessage() %v SEND  LC = %v; msg.Serial=%v", who, lc, msg.HDR.Serial)
	})

	send := s.newSendMsg(msg, isCli)
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

type pq struct {
	name string
	tree *rb.Tree
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

// order by mop.originLC, then mop.sn;
// for reads and sends (readQ and pre-arrival preArrQ).
func newPQ() *pq {
	return &pq{
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

			if av.originLC < bv.originLC {
				return -1
			}
			if av.originLC > bv.originLC {
				return 1
			}
			// INVAR originLC equal, delivery order should not matter?
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

			// if av.when.Before(bv.when) {
			// 	return -1
			// }
			// if av.when.After(bv.when) {
			// 	return 1
			// }

			// Hopefully not, but just in case...
			// av.msg could be nil (so could bv.msg)
			if av.msg == nil && bv.msg == nil {
				return 0
			}
			if av.msg == nil {
				return -1
			}
			if bv.msg == nil {
				return 1
			}
			// INVAR: a.when == b.when

			if av.msg.HDR.CallID == bv.msg.HDR.CallID {
				if av.msg.HDR.Serial == bv.msg.HDR.Serial {
					return 0
				}
				if av.msg.HDR.Serial < bv.msg.HDR.Serial {
					return -1
				}
				return 1
			}
			if av.msg.HDR.CallID < bv.msg.HDR.CallID {
				return -1
			}
			return 1
		}),
	}
}

// order by mop.when then mop.sn; for timers
func newPQtime() *pq {
	return &pq{
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

			if av.when.Before(bv.when) {
				return -1
			}
			if av.when.After(bv.when) {
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

			// if av.when.Before(bv.when) {
			// 	return -1
			// }
			// if av.when.After(bv.when) {
			// 	return 1
			// }

			// Hopefully not, but just in case...
			// av.msg could be nil (so could bv.msg)
			if av.msg == nil && bv.msg == nil {
				return 0
			}
			if av.msg == nil {
				return -1
			}
			if bv.msg == nil {
				return 1
			}
			// INVAR: a.when == b.when

			if av.msg.HDR.CallID == bv.msg.HDR.CallID {
				if av.msg.HDR.Serial == bv.msg.HDR.Serial {
					return 0
				}
				if av.msg.HDR.Serial < bv.msg.HDR.Serial {
					return -1
				}
				return 1
			}
			if av.msg.HDR.CallID < bv.msg.HDR.CallID {
				return -1
			}
			return 1
		}),
	}
}

func (s *simnet) handleSend(send *mop) {
	//vv("top of handleSend(send = '%v')", send)

	if send.seen == 0 {
		if send.originCli {
			send.senderLC = s.clinode.LC
			send.originLC = s.clinode.LC
		} else {
			send.senderLC = s.srvnode.LC
			send.originLC = s.srvnode.LC
		}
		send.when = time.Now() //.Add(s.scenario.hop)
	}
	send.seen++

	if send.originCli {
		lc := s.clinode.LC
		s.srvnode.preArrQ.add(send)
		vv("cli.LC:%v  SEND TO SERVER %v", lc, send)
		//vv("cli.LC:%v  SEND TO SERVER %v    srvPreArrQ: '%v'", lc, send, s.srvnode.preArrQ)
	} else {
		lc := s.srvnode.LC
		s.clinode.preArrQ.add(send)
		vv("srv.LC:%v  SEND TO CLIENT %v", lc, send)
		//vv("srv.LC:%v  SEND TO CLIENT %v    cliPreArrQ: '%v'", lc, send, s.clinode.preArrQ)
	}
}

func (s *simnet) handleRead(read *mop) {
	//vv("top of handleRead(read = '%v')", read)

	if read.seen == 0 {
		if read.originCli {
			//read.senderLC = s.clinode.LC
			read.originLC = s.clinode.LC
		} else {
			//read.senderLC = s.srvnode.LC
			read.originLC = s.srvnode.LC
		}
		read.when = time.Now() //.Add(s.scenario.hop)
	}
	read.seen++

	if read.originCli {
		s.clinode.readQ.add(read)
		vv("cliLC:%v  READ at CLIENT: %v", s.clinode.LC, read)
		//vv("cliLC:%v  READ %v at CLIENT, now cliReadQ: '%v'", s.clinode.LC, read, s.clinode.readQ)
		s.clinode.dispatchSendsReadsTimers()
	} else {
		s.srvnode.readQ.add(read)
		vv("srvLC:%v  READ at SERVER: %v", s.srvnode.LC, read)
		//vv("srvLC:%v  READ %v at SERVER, now srvReadQ: '%v'", s.srvnode.LC, read, s.srvnode.readQ)
		s.srvnode.dispatchSendsReadsTimers()
	}
}

func (node *simnode) dispatchSendsReadsTimers() {

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
			//vv("delete '%v'", op)
			node.preArrQ.tree.DeleteWithKey(op)
		}
		for _, op := range readDel {
			//vv("delete '%v'", op)
			node.readQ.tree.DeleteWithKey(op)
		}
		for _, op := range timerDel {
			//vv("delete '%v'", op)
			node.timerQ.tree.DeleteWithKey(op)
		}
		//vv("=== end of dispatchSendsReadsTimers %v", node.name)
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
		vv("check TIMER: %v", timer)

		if !now.Before(timer.when) {
			// timer.when <= now
			vv("have TIMER firing")
			timerDel = append(timerDel, timer)
			select {
			case timer.timerC <- now:
				vv("sent on timerC")
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

		// our reads are always <= our node.LC
		if read.originLC > node.LC {
			panic("impossible! read > node.LC ! logic error")
		}
		//if send.originLC < node.LC {
		//}

		//if read.originLC > send.originLC {

		// service this read with this send

		// *** here is where we could re-order
		// messages in a chaos test.

		read.msg = send.msg.CopyForSimNetSend()
		// advance our clock
		node.LC = max(node.LC, send.originLC) + 1
		//vv("servicing cli read: started LC %v -> serviced %v (waited: %v) read.sn=%v", read.originLC, node.LC, node.LC-read.originLC, read.sn)

		// track clocks on either end for this send and read.
		read.readerLC = node.LC
		read.senderLC = send.senderLC
		send.readerLC = node.LC

		// matchmaking
		vv("[1]matchmaking send '%v' -> read '%v'", send, read)
		read.sendmop = send
		send.readmop = read

		preDel = append(preDel, send)
		readDel = append(readDel, read)

		close(read.proceed)
		close(send.proceed)

		readIt = readIt.Next()
		preIt = preIt.Next()
		//} else {
		// INVAR: smallest read.originLC <= smallest send.originLC
		//}
	}
}

func (s *simnet) Start() {
	//vv("simnet.Start() top")

	go func() {

		// init phase

		// get a client before anything else.
		select {
		case s.cli = <-s.cliReady:
			//vv("simnet got cli")
		case <-s.halt.ReqStop.Chan:
			return
		}

		// main scheduler loop
		for i := int64(0); ; i++ {
			// each scheduler loop tick is an event.
			cliLC := atomic.AddInt64(&s.clinode.LC, 1)
			srvLC := atomic.AddInt64(&s.srvnode.LC, 1)
			_, _ = cliLC, srvLC

			// advance time by one tick
			time.Sleep(s.scenario.tick)

			// do we need/want to do this?
			// The pending PQ timer would prevent
			// this from ever returning, I think.
			// And both clients and servers should
			// have outstanding reads open always
			// as they listen for messages.
			synctest.Wait()
			//vv("scheduler top cli.LC = %v ; srv.LC = %v", cliLC, srvLC)

			s.clinode.dispatchSendsReadsTimers()
			s.srvnode.dispatchSendsReadsTimers()

			select {
			case now := <-s.nextTimer.C: // timer fires
				vv("s.nextTimer -> now %v", now)
				s.clinode.dispatchSendsReadsTimers()
				s.srvnode.dispatchSendsReadsTimers()

			case scenario := <-s.newScenarioCh:
				s.finishScenario()
				s.initScenario(scenario)

			case timer := <-s.addTimer:
				vv("addTimer ->  op='%v'", timer)
				s.handleTimer(timer)

			case send := <-s.msgSendCh:
				//vv("msgSendCh ->  op='%v'", send)
				s.handleSend(send)

			case read := <-s.msgReadCh:
				//vv("msgReadCh ->  op='%v'", read)
				s.handleRead(read)

			case <-s.halt.ReqStop.Chan:
				return
			}
		}

	}()
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
	vv("handleTimer() %v  TIMER SET; LC = %v", who, lc)

	now := time.Now()
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

	timerQ := s.srvnode.timerQ
	if timer.originCli {
		timerQ = s.clinode.timerQ
		lc := s.clinode.LC
		s.clinode.timerQ.add(timer)
		vv("cli.LC:%v CLIENT set TIMER %v to fire at '%v'; now timerQ: '%v'", lc, timer, timer.when, s.clinode.timerQ)
	} else {
		lc := s.srvnode.LC
		s.srvnode.timerQ.add(timer)
		vv("srv.LC:%v SERVER set TIMER %v to fire at '%v'; now timerQ: '%v'", lc, timer, timer.when, s.srvnode.timerQ)
	}

	it := timerQ.tree.Min()
	minTimer := it.Item().(*mop)
	dur := minTimer.when.Sub(now)
	vv("dur=%v = when(%v) - now(%v)", dur, minTimer.when, now)
	s.nextTimer.Reset(dur)
}

// called by goroutines outside of the scheduler,
// so must not touch s.srvnode, s.clinode, etc.
func (s *simnet) createNewTimer(dur time.Duration, begin time.Time, isCli bool) (timerC chan time.Time, err error) {

	who := "SERVER"
	if isCli {
		who = "CLIENT"
	}
	_ = who
	vv("top simnet.createNewTimer() %v SETS TIMER dur='%v' begin='%v' => when='%v'", who, dur, begin, begin.Add(dur))

	timer := s.newTimerMop(isCli)
	timer.timerDur = dur
	timer.timerStarted = begin
	timer.when = begin.Add(dur)

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

func (s *simnet) newTimerMop(isCli bool) (op *mop) {
	op = &mop{
		originCli: isCli,
		sn:        simnetNextMopSn(),
		kind:      TIMER,
		proceed:   make(chan struct{}),
	}
	return
}
