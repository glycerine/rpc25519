package rpc25519

// build/run with:
// GOTRACEBACK=all GOEXPERIMENT=synctest go test -v

import (
	"fmt"
	mathrand2 "math/rand/v2"
	"sync/atomic"
	//"testing/synctest" // moved to simnet_synctest.go
	"time"

	"github.com/glycerine/idem"
	rb "github.com/glycerine/rbtree"
)

var globalUseSynctest bool = true

type SimNetConfig struct{}

// a connection between two nodes.
// implements uConn, see simnet_server.go
type simnetConn struct {
	// distinguish cli from srv
	isCli   bool
	net     *simnet
	netAddr *SimNetAddr // local address

	local  *simnode
	remote *simnode
}

// simnet implements the same workspace/blabber interface
// so we can plug in
// netsim and do comms via channels for testing/synctest
// based accelerated timeout testing.
//
// Note that uConn and its Write/Read are
// not actually used; channel sends/reads replace them.
// We still need a dummy uConn to pass to
// readMessage() and sendMessage() which are the
// interception points for the simulated network.
//
// The blabber does check if the uConn is *simnet, and
// configures itself to call through it if present.

type SimNetAddr struct { // implements net.Addr interface
	network string
	addr    string
	name    string
	isCli   bool
}

// name of the network (for example, "tcp", "udp", "simnet")
func (s *SimNetAddr) Network() string {
	return s.network
}

// string form of address (for example, "192.0.2.1:25", "[2001:db8::1]:80")
func (s *SimNetAddr) String() string {
	// keep it simple, as it is our simnet.dns lookup key.
	return s.name
}

// Message operation
type mop struct {
	sn int64

	// so we can handle a network
	// rather than just cli/srv.
	origin *simnode
	target *simnode

	// number of times handleSend() has seen this mop.
	seen int

	originCli bool

	senderLC int64
	readerLC int64
	originLC int64

	timerC        chan time.Time
	timerDur      time.Duration
	timerFileLine string // where was this timer from?

	// discards tell us the corresponding create timer here.
	origTimerMop        *mop
	origTimerCompleteTm time.Time

	// when fired => unarmed. NO timer.Reset support at the moment!
	timerFiredTm time.Time
	// was discarded timer armed?
	wasArmed bool
	// was timer set internally to wake for
	// arrival of pending message?
	internalPendingTimer bool

	// when was the operation initiated?
	// timer started, read begin waiting, send hits the socket.
	// for timer discards, when discarded so usually initTm
	initTm time.Time

	// when did the send message arrive?
	arrivalTm time.Time

	// when: when the operation completes and
	// control returns to user code.
	// READS: when the read returns to user who called readMessage()
	// SENDS: when the send returns to user who called sendMessage()
	// TIMER: when user code <-timerC gets sent the current time.
	// TIMER_DISCARD: zero time.Time{}.
	completeTm time.Time

	kind mopkind
	msg  *Message

	sendmop *mop // for reads, which send did we get?
	readmop *mop // for sends, which read did we go to?

	// clients of scheduler wait on proceed.
	// When the timer is set; the send sent; the read
	// matches a send, the client proceeds because
	// this channel will be closed.
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
	extra := ""
	if op.kind == TIMER {
		extra = " timer set at " + op.timerFileLine
	}
	if op.kind == TIMER_DISCARD {
		extra = " timer discarded at " + op.timerFileLine
	}
	return fmt.Sprintf("mop{%v %v init:%v, arr:%v, complete:%v op.sn:%v, msg.sn:%v%v}", who, op.kind, ini, arr, complete, op.sn, msgSerial, extra)
}

// simnet simulates a network entirely with channels in memory.
type simnet struct {
	useSynctest bool

	scenario *scenario

	cfg       *Config
	simNetCfg *SimNetConfig

	srv *Server
	cli *Client

	// first server, not needed to bootstrap,
	// just for diagnostic reference.
	srvnode0 *simnode

	dns map[string]*simnode

	// for now just clinode and srvnode in nodes;
	// plan is to add full network.
	nodes map[*simnode]map[*simnode]*simnetConn

	cliRegisterCh chan *clientRegistration

	newClientConnCh chan *simnetConn

	// same as srv.halt; we don't need
	// our own, at least for now.
	halt *idem.Halter

	msgSendCh      chan *mop
	msgReadCh      chan *mop
	addTimer       chan *mop
	discardTimerCh chan *mop
	newScenarioCh  chan *scenario
	nextTimer      *time.Timer
	lastArmTm      time.Time
}

// simnode is a single host/server/node
// (really one rpc25519.Client or rpc25519.Server instance)
// in a simnet.
type simnode struct {
	name    string
	LC      int64
	readQ   *pq
	preArrQ *pq
	timerQ  *pq
	net     *simnet
	isCli   bool
	netAddr *SimNetAddr

	tellServerNewConnCh chan *simnetConn
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

func (s *simnet) newSimnodeClient(name string) (node *simnode) {
	node = s.newSimnode(name)
	node.isCli = true
	return
}

func (s *simnet) newSimnodeServer(name string) (node *simnode) {
	node = s.newSimnode(name)
	node.isCli = false
	node.tellServerNewConnCh = make(chan *simnetConn)
	return
}

func (s *simnet) showDNS() {
	i := 0
	for name, node := range s.dns {
		vv("[%2d] showDNS dns[%v] = %p", i, name, node)
		i++
	}
}
func (s *simnet) handleNewClientRegistration(reg *clientRegistration) {

	srvnode, ok := s.dns[reg.dialTo]
	if !ok {
		s.showDNS()
		panic(fmt.Sprintf("cannot find server '%v', requested "+
			"by client registration", reg.dialTo))
	}

	clinode := s.newSimnodeClient(reg.client.name)
	clinode.setNetAddrSameNetAs(reg.localHostPortStr, srvnode.netAddr)

	s.dns[clinode.name] = clinode

	// add node to graph
	clientOutboundEdges := make(map[*simnode]*simnetConn)
	s.nodes[clinode] = clientOutboundEdges

	// add both direction edges
	c2s := s.addEdgeFromCli(clinode, srvnode)
	s2c := s.addEdgeFromSrv(srvnode, clinode)

	// tell server about new edge
	select {
	case srvnode.tellServerNewConnCh <- s2c:
	case <-s.halt.ReqStop.Chan:
		return
	}

	// let client start using the connection/edge.
	reg.conn = c2s
	reg.simnode = clinode
	close(reg.done)
}

func (cfg *Config) newSimNetOnServer(simNetConfig *SimNetConfig, srv *Server, srvNetAddr *SimNetAddr) (tellServerNewConnCh chan *simnetConn) {

	scen := newScenario(time.Millisecond, time.Millisecond, time.Millisecond, [32]byte{})

	// server creates simnet; must start server first.
	s := &simnet{
		useSynctest:     globalUseSynctest,
		cfg:             cfg,
		srv:             srv,
		halt:            srv.halt,
		cliRegisterCh:   make(chan *clientRegistration),
		simNetCfg:       simNetConfig,
		msgSendCh:       make(chan *mop),
		msgReadCh:       make(chan *mop),
		addTimer:        make(chan *mop),
		discardTimerCh:  make(chan *mop),
		newClientConnCh: make(chan *simnetConn),

		newScenarioCh: make(chan *scenario),
		scenario:      scen,

		dns: make(map[string]*simnode),

		// graph of nodes, edges are nodes[from][to]
		nodes: make(map[*simnode]map[*simnode]*simnetConn),

		// high duration b/c no need to fire spuriously
		// and force the Go runtime to do extra work when
		// we are about to s.nextTimer.Stop() just below.
		// Seems slightly inefficient API design to not
		// have a way to create an unarmed timer, but maybe
		// that avoids user code forgetting to set the timer...
		// in exchange for a couple of microseconds of extra work.
		// Fortunately we only need do this once.
		nextTimer: time.NewTimer(time.Hour * 10_000),
	}
	s.nextTimer.Stop()

	srvnode := s.newSimnodeServer(srv.name)
	srvnode.netAddr = srvNetAddr
	s.nodes[srvnode] = make(map[*simnode]*simnetConn)
	s.dns[srvnode.name] = srvnode

	s.srvnode0 = srvnode // first server, for reference

	// let client find the shared simnet in their cfg,
	// when they shallow copy it.
	cfg.simnetRendezvous.mut.Lock()
	cfg.simnetRendezvous.simnet = s
	cfg.simnetRendezvous.mut.Unlock()

	s.Start()
	return srvnode.tellServerNewConnCh
}

func (s *simnode) setNetAddrSameNetAs(addr string, srvNetAddr *SimNetAddr) {
	s.netAddr = &SimNetAddr{
		network: srvNetAddr.network,
		addr:    addr,
		name:    s.name,
		isCli:   true,
	}
}

func (s *simnet) addEdgeFromSrv(srvnode, clinode *simnode) *simnetConn {

	srv, ok := s.nodes[srvnode] // edges from srv to clients
	if !ok {
		srv = make(map[*simnode]*simnetConn)
		s.nodes[srvnode] = srv
	}
	s2c := &simnetConn{
		isCli:   false,
		net:     s,
		local:   srvnode,
		remote:  clinode,
		netAddr: srvnode.netAddr,
	}
	// replace any previous conn
	srv[clinode] = s2c
	return s2c
}

func (s *simnet) addEdgeFromCli(clinode, srvnode *simnode) *simnetConn {

	cli, ok := s.nodes[clinode] // edge from client to one server
	if !ok {
		cli = make(map[*simnode]*simnetConn)
		s.nodes[clinode] = cli
	}
	c2s := &simnetConn{
		isCli:   true,
		net:     s,
		local:   clinode,
		remote:  srvnode,
		netAddr: clinode.netAddr,
	}
	// replace any previous conn
	cli[srvnode] = c2s
	return c2s
}

var simnetLastMopSn int64

func simnetNextMopSn() int64 {
	return atomic.AddInt64(&simnetLastMopSn, 1)
}

type mopkind int

const (
	TIMER         mopkind = 1
	TIMER_DISCARD mopkind = 2
	SEND          mopkind = 3
	READ          mopkind = 4
)

func (k mopkind) String() string {
	switch k {
	case TIMER:
		return "TIMER"
	case TIMER_DISCARD:
		return "TIMER_DISCARD"
	case SEND:
		return "SEND"
	case READ:
		return "READ"
	default:
		return fmt.Sprintf("unknown mopkind %v", int(k))
	}
}

// scenario will, in the future, provide for testing different
// timeout settings under network partition and
// flakiness (partial failure). Stubbed for now.
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

func (s *pq) del(op *mop) (found bool) {
	if op == nil {
		panic("cannot delete nil mop!")
	}
	var it rb.Iterator
	it, found = s.tree.FindGE_isEqual(op)
	if !found {
		return
	}
	s.tree.DeleteWithIterator(it)
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
		}),
	}
}

func (s *simnet) handleSend(send *mop) {
	////zz("top of handleSend(send = '%v')", send)

	origin := send.origin
	if send.seen == 0 {
		send.senderLC = origin.LC
		send.originLC = origin.LC
		send.arrivalTm = send.initTm.Add(s.scenario.rngHop())
	}
	send.seen++
	if send.seen != 1 {
		panic(fmt.Sprintf("should see each send only once now, not %v", send.seen))
	}

	// make a copy _before_ the sendMessage() call returns,
	// so they can recycle or do whatever without data racing with us.
	send.msg = send.msg.CopyForSimNetSend()

	send.target.preArrQ.add(send)
	//vv("LC:%v  SEND TO %v %v", origin.LC, origin.name, send)
	////zz("LC:%v  SEND TO %v %v    srvPreArrQ: '%v'", origin.LC, origin.name, send, s.srvnode.preArrQ)

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

	origin := read.origin
	if read.seen == 0 {
		read.originLC = origin.LC
	}
	read.seen++
	if read.seen != 1 {
		panic(fmt.Sprintf("should see each send only once now, not %v", read.seen))
	}

	origin.readQ.add(read)
	//vv("LC:%v  READ at %v: %v", origin.LC, origin.name, read)
	////zz("LC:%v  READ %v at %v, now cliReadQ: '%v'", origin.LC, origin.name, read, origin.readQ)
	origin.dispatch()
}

func (node *simnode) firstPreArrivalTimeLTE(now time.Time) bool {

	preIt := node.preArrQ.tree.Min()
	if preIt == node.preArrQ.tree.Limit() {
		return false // empty queue
	}
	send := preIt.Item().(*mop)
	return !send.arrivalTm.After(now)
}

// dispatch delivers sends to reads, and fires timers.
// It calls node.net.armTimer() at the end (in the defer).
func (node *simnode) dispatch() { // (bump time.Duration) {

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

		// sanity check that we delivered everything we could.
		narr := node.preArrQ.tree.Len()
		nread := node.readQ.tree.Len()
		// it is normal to have preArrQ if no reads...
		if narr > 0 && nread > 0 {
			// if the first preArr is not due yet, that is the reason

			// if not using fake time, arrival time was probably
			// almost but not quite here when we checked below,
			// but now there is something possible a
			// few microseconds later. In this case, don't freak.
			// So make this conditional on synctest being in use:
			if node.net.useSynctest {
				if node.firstPreArrivalTimeLTE(time.Now()) {
					alwaysPrintf("ummm... why did these not get dispatched? narr = %v, nread = %v; summary node summary:\n%v", narr, nread, node.String())
					panic("should have been dispatchable, no?")
				}
			}
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
			timer.timerFiredTm = now
			timerDel = append(timerDel, timer)
			select {
			case timer.timerC <- now:
			//vv("sent on timerC")
			default:
			// this is what the Go runtime does. otherwise our clock gets 0.01 increments each time...
			//vv("timerC default: giving up on timer immediately!")
			// this works fine, it seems. this was alt:
			//case <-time.After(time.Millisecond * 10):
			// see 8 of these in cli_test 006; since 00:01:07.08 is ending time.
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

			//vv("rejecting delivery of send that has not happened: '%v'", send)
			//vv("scheduler: %v", node.net.schedulerReport())

			// we must set a timer on its delivery then...
			dur := send.arrivalTm.Sub(now)
			pending := newTimerCreateMop(node.isCli)
			pending.origin = node
			pending.timerDur = dur
			pending.initTm = now
			pending.completeTm = now.Add(dur)
			pending.timerFileLine = fileLine(1)
			pending.internalPendingTimer = true
			node.net.handleTimer(pending)
			return
		}
		// INVAR: this send.arrivalTm <= now; good to deliver.

		// Since both have happened, they can be matched.

		// Service this read with this send.

		read.msg = send.msg // safe b/c already copied in handleSend()
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
		// our sends are asynchronous -- not RPCs,
		// so we don't block. This should happen
		// as soon as we have the send assigned
		// to the pre-arrival queue; much earlier.
		// I'm saving this note in case in the
		// future we wanted to emulate RPC semantics
		// that do block, and thus we remove
		// the close at line 708 in handleSend() and
		// then wait for the round trip to complete...
		// but in that case this _still_ not the
		// right place to close send-- as only
		// the first half of the round-trip has
		// finished! We would have to wait for
		// the reply send to arrive, and match
		// this with that. We don't track the
		// CallID inside the simnet like the
		// client does, at the moment. We don't currently
		// need RPC semantics, so defer all that for now.
		//close(send.proceed)

		readIt = readIt.Next()
		preIt = preIt.Next()
	}
}

func (s *simnet) qReport() (r string) {
	for node := range s.nodes {
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
	//vv("simnet.Start() top")
	go s.scheduler()
}

func (s *simnet) schedulerReport() string {
	now := time.Now()
	return fmt.Sprintf("lastArmTm.After(now) = %v [%v out] %v; qReport = '%v'", s.lastArmTm.After(now), s.lastArmTm.Sub(now), s.lastArmTm, s.qReport())
}

func (s *simnet) dispatchAll() {
	for node := range s.nodes {
		node.dispatch()
	}
}

func (s *simnet) tickLogicalClocks() {
	for node := range s.nodes {
		node.LC++
	}
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

	// main scheduler loop
	for i := int64(0); ; i++ {

		// each scheduler loop tick is an event.
		s.tickLogicalClocks()

		now := time.Now()
		_ = now
		////zz("scheduler top cli.LC = %v ; srv.LC = %v", cliLC, srvLC)
		//vv("scheduler top. schedulerReport: \n%v", s.schedulerReport())

		s.dispatchAll()
		s.armTimer()

		// advance time by one tick
		time.Sleep(s.scenario.tick)

		if s.useSynctest {
			synctestWait()
			//vv("back from synctestWait()")
		}

		select {
		case alert := <-s.nextTimer.C: // soonest timer fires
			_ = alert
			//vv("s.nextTimer -> alerted at %v", alert)
			s.dispatchAll()
			s.armTimer()

		case reg := <-s.cliRegisterCh:
			//vv("s.cliRegisterCh got reg = '%#v'", reg)
			s.handleNewClientRegistration(reg)

		case newConn := <-s.newClientConnCh:
			_ = newConn
			panic("TODO <-s.newConnCh for restarted cli or 2nd/later cli")

		case scenario := <-s.newScenarioCh:
			s.finishScenario()
			s.initScenario(scenario)

		case timer := <-s.addTimer:
			//vv("addTimer ->  op='%v'", timer)
			s.handleTimer(timer)

		case discard := <-s.discardTimerCh:
			//vv("discardTimer ->  op='%v'", discard)
			s.handleDiscardTimer(discard)

		case send := <-s.msgSendCh:
			//vv("msgSendCh ->  op='%v'", send)
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

func (s *simnet) handleDiscardTimer(discard *mop) {

	orig := discard.origTimerMop

	found := discard.origin.timerQ.del(discard.origTimerMop)
	if found {
		discard.wasArmed = !orig.timerFiredTm.IsZero()
		discard.origTimerCompleteTm = orig.completeTm
	} // leave wasArmed false, could not have been armed if gone.

	////zz("LC:%v %v TIMER_DISCARD %v to fire at '%v'; now timerQ: '%v'", discard.origin.LC, discard.origin.name, discard, discard.origTimerCompleteTm, s.clinode.timerQ)
	s.armTimer()
	close(discard.proceed)
}

func (s *simnet) handleTimer(timer *mop) {

	lc := timer.origin.LC
	who := timer.origin.name
	_, _ = who, lc
	//vv("handleTimer() %v  TIMER SET; LC = %v", who, lc)

	if timer.seen == 0 {
		timer.senderLC = lc
		timer.originLC = lc
		timer.timerC = make(chan time.Time)
		defer close(timer.proceed)
	}
	timer.seen++
	if timer.seen != 1 {
		panic(fmt.Sprintf("expect each timer mop only once now, not %v", timer.seen))
	}

	timer.origin.timerQ.add(timer)
	////zz("LC:%v %v set TIMER %v to fire at '%v'; now timerQ: '%v'", lc, timer.origin.name, timer, timer.completeTm, s.clinode.timerQ)

	s.armTimer()
}

func (s *simnet) armTimer() {

	var minTimer *mop
	for node := range s.nodes {
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
func (s *simnet) createNewTimer(origin *simnode, dur time.Duration, begin time.Time, isCli bool) (timer *mop) {

	//vv("top simnet.createNewTimer() %v SETS TIMER dur='%v' begin='%v' => when='%v'", origin.name, dur, begin, begin.Add(dur))

	timer = newTimerCreateMop(isCli)
	timer.origin = origin
	timer.timerDur = dur
	timer.initTm = begin
	timer.completeTm = begin.Add(dur)
	timer.timerFileLine = fileLine(3)

	select {
	case s.addTimer <- timer:
	case <-s.halt.ReqStop.Chan:
		return
	}
	select {
	case <-timer.proceed:
		return
	case <-s.halt.ReqStop.Chan:
		return
	}
	return
}

// readMessage reads a framed message from conn.
func (s *simnet) readMessage(conn uConn) (msg *Message, err error) {

	sc := conn.(*simnetConn)
	isCli := sc.isCli

	//vv("top simnet.readMessage() %v READ", read.origin)

	read := newReadMop(isCli)
	read.initTm = time.Now()
	read.origin = sc.local
	read.target = sc.remote
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

	sc := conn.(*simnetConn)
	isCli := sc.isCli

	//vv("top simnet.sendMessage() %v SEND  msg.Serial=%v", send.origin, msg.HDR.Serial)
	//vv("sendMessage\n conn.local = %v (isCli:%v)\n conn.remote = %v (isCli:%v)\n", sc.local.name, sc.local.isCli, sc.remote.name, sc.remote.isCli)
	send := newSendMop(msg, isCli)
	send.origin = sc.local
	send.target = sc.remote
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

func newTimerCreateMop(isCli bool) (op *mop) {
	op = &mop{
		originCli: isCli,
		sn:        simnetNextMopSn(),
		kind:      TIMER,
		proceed:   make(chan struct{}),
	}
	return
}

func newTimerDiscardMop(origTimerMop *mop) (op *mop) {
	op = &mop{
		originCli:    origTimerMop.originCli,
		sn:           simnetNextMopSn(),
		kind:         TIMER_DISCARD,
		proceed:      make(chan struct{}),
		origTimerMop: origTimerMop,
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

func (s *simnet) discardTimer(origin *simnode, origTimerMop *mop, discardTm time.Time) (wasArmed bool) {

	//vv("top simnet.discardTimer() %v SETS TIMER dur='%v' begin='%v' => when='%v'", who, dur, begin, begin.Add(dur))

	discard := newTimerDiscardMop(origTimerMop)
	discard.initTm = time.Now()
	discard.timerFileLine = fileLine(3)
	discard.origin = origin

	select {
	case s.discardTimerCh <- discard:
	case <-s.halt.ReqStop.Chan:
		return
	}
	select {
	case <-discard.proceed:
		return discard.wasArmed
	case <-s.halt.ReqStop.Chan:
		return
	}
	return
}

// clientRegistration: a new client joins the simnet.
// See simnet_client.go
type clientRegistration struct {
	// provide
	client           *Client
	localHostPortStr string // Client.cfg.ClientHostPort

	dialTo        string // preferred, set by tests; Client.cfg.ClientDialToHostPort
	serverAddrStr string // from runSimNetClient() call by cli.go:155

	// wait on
	done chan struct{}

	// receive back
	simnode *simnode    // our identity in the simnet (conn.local)
	conn    *simnetConn // our connection to server (c2s)
}

// external, called by simnet_client.go to
// get a registration ticket to send on simnet.cliRegisterCh
func (s *simnet) newClientRegistration(c *Client, localHostPort, serverAddr, dialTo string) *clientRegistration {
	return &clientRegistration{
		client:           c,
		localHostPortStr: localHostPort,
		dialTo:           dialTo,
		serverAddrStr:    serverAddr,
		done:             make(chan struct{}),
	}
}
