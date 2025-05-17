package rpc25519

// build/run with:
// GOTRACEBACK=all GOEXPERIMENT=synctest go test -v

import (
	"fmt"
	mathrand2 "math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/glycerine/idem"
	rb "github.com/glycerine/rbtree"
)

// NB: all String() methods are now in simnet_string.go

// SimNetConfig provides control parameters.
type SimNetConfig struct {

	// The barrier is the synctest.Wait call
	// the lets the caller resume only when
	// all other goro are durably blocked.
	// (All goroutines in the simulation are/
	// must be in the same bubble with the simnet).
	//
	// The barrier can only be used (or not) if faketime
	// is also used, so this option will have
	// no effect unless the simnet is run
	// in a synctest.Wait bubble (using synctest.Run).
	//
	// Under faketime, BarrierOff true means
	// the scheduler will not wait to know
	// for sure that it is the only active goroutine
	// when doing its scheduling steps, such as firing
	// new timers and matching sends and reads.
	//
	// This introduces more non-determinism --
	// which provides more test coverage --
	// but the tradeoff is that those tests are
	// not reliably repeatable, since the
	// Go runtime's goroutine interleaving order is
	// randomized. The scheduler might take more
	// steps than otherwise to deliver a
	// message or to fire a timer, since we
	// the scheduler can wake alongside us
	// and become active.
	//
	// At the moment this can't happen in our simulation
	// because the simnet controls all
	// timers in rpc25519 tests, and so
	// only the scheduler calls Sleep.
	// However future tests and user code
	// might call Sleep, in which
	// case the
	BarrierOff bool
}

// NewSimNetConfig should be called
// to get an initial SimNetConfig to
// set parameters.
func NewSimNetConfig() *SimNetConfig {
	return &SimNetConfig{}
}

// simnet implements the workspace/blabber interface
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
	//vv("SimNetAddr.Network() returning '%v'", s.network)
	return s.network
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

	readFileLine string
	sendFileLine string

	// discards tell us the corresponding create timer here.
	origTimerMop        *mop
	origTimerCompleteTm time.Time

	// when fired => unarmed. NO timer.Reset support at the moment!
	// (except conceptually for the scheduler's gridStepTimer).
	timerFiredTm time.Time
	// was discarded timer armed?
	wasArmed bool
	// was timer set internally to wake for
	// arrival of pending message?
	internalPendingTimer bool

	// is this our single grid step timer?
	// There should only ever be one
	// timer with this flag true.
	isGridStepTimer bool

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

	unmaskedCompleteTm time.Time
	unmaskedDur        time.Duration

	kind mopkind
	msg  *Message

	sendmop *mop // for reads, which send did we get?
	readmop *mop // for sends, which read did we go to?

	// clients of scheduler wait on proceed.
	// When the timer is set; the send sent; the read
	// matches a send, the client proceeds because
	// this channel will be closed.
	proceed chan struct{}

	isEOF_RST bool
}

// simnet simulates a network entirely with channels in memory.
type simnet struct {
	barrier       bool
	bigbang       time.Time
	gridStepTimer *mop

	backgoundTimerGoroCount int64 // atomic access only

	scenario *scenario

	cfg       *Config
	simNetCfg *SimNetConfig

	srv *Server
	cli *Client

	dns map[string]*simnode

	// for now just clinode and srvnode in nodes;
	// plan is to add full network.
	nodes map[*simnode]map[*simnode]*simnetConn

	cliRegisterCh chan *clientRegistration
	srvRegisterCh chan *serverRegistration

	alterNodeCh chan *nodeAlteration

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
	lc      int64 // logical clock
	readQ   *pq
	preArrQ *pq
	timerQ  *pq
	net     *simnet
	isCli   bool
	netAddr *SimNetAddr
	state   nodestate

	tellServerNewConnCh chan *simnetConn
}

type nodestate int

const (
	HEALTHY     nodestate = 0
	HALTED      nodestate = 1
	PARTITIONED nodestate = 2
)

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
	// buffer so servers don't have to be up to get them.
	node.tellServerNewConnCh = make(chan *simnetConn, 100)
	return
}

// for additional servers after the first.
func (s *simnet) handleServerRegistration(reg *serverRegistration) {

	// srvNetAddr := SimNetAddr{ // implements net.Addr interface
	// 	network: "simnet",
	// 	addr:
	// 	name:    reg.server.name,
	// 	isCli:   false,
	// }

	srvnode := s.newSimnodeServer(reg.server.name)
	srvnode.netAddr = reg.srvNetAddr
	s.nodes[srvnode] = make(map[*simnode]*simnetConn)
	_, already := s.dns[srvnode.name]
	if already {
		panic(fmt.Sprintf("server name already taken: '%v'", srvnode.name))
	}
	s.dns[srvnode.name] = srvnode

	reg.simnode = srvnode
	reg.simnet = s

	//vv("end of handleServerRegistration, srvreg is %v", reg)

	// channel made by newSimnodeServer() above.
	reg.tellServerNewConnCh = srvnode.tellServerNewConnCh
	close(reg.done)
}

func (s *simnet) handleClientRegistration(reg *clientRegistration) {

	srvnode, ok := s.dns[reg.dialTo]
	if !ok {
		s.showDNS()
		panic(fmt.Sprintf("cannot find server '%v', requested "+
			"by client registration from '%v'", reg.dialTo, reg.client.name))
	}

	clinode := s.newSimnodeClient(reg.client.name)
	clinode.setNetAddrSameNetAs(reg.localHostPortStr, srvnode.netAddr)

	_, already := s.dns[clinode.name]
	if already {
		panic(fmt.Sprintf("client name already taken: '%v'", clinode.name))
	}
	s.dns[clinode.name] = clinode

	// add node to graph
	clientOutboundEdges := make(map[*simnode]*simnetConn)
	s.nodes[clinode] = clientOutboundEdges

	// add both direction edges
	c2s := s.addEdgeFromCli(clinode, srvnode)
	s2c := s.addEdgeFromSrv(srvnode, clinode)

	reg.conn = c2s
	reg.simnode = clinode

	// tell server about new edge
	// vv("about to deadlock? stack=\n'%v'", stack())
	// I think this might be a chicken and egg problem.
	// The server cannot register b/c client is here on
	// the scheduler goro, and client here wants to tell the
	// the server about it... try in goro
	go func() {
		select {
		case srvnode.tellServerNewConnCh <- s2c:
			//vv("%v srvnode was notified of new client '%v'; s2c='%#v'", srvnode.name, clinode.name, s2c)

			// let client start using the connection/edge.
			close(reg.done)
		case <-s.halt.ReqStop.Chan:
			return
		}
	}()
}

// idempotent, all servers do this, then register through the same path.
func (cfg *Config) bootSimNetOnServer(simNetConfig *SimNetConfig, srv *Server) *simnet { // (tellServerNewConnCh chan *simnetConn) {

	//vv("%v newSimNetOnServer top, goro = %v", srv.name, GoroNumber())
	cfg.simnetRendezvous.singleSimnetMut.Lock()
	defer cfg.simnetRendezvous.singleSimnetMut.Unlock()

	if cfg.simnetRendezvous.singleSimnet != nil {
		// already started. Still, everyone
		// register separately no matter.
		return cfg.simnetRendezvous.singleSimnet
	}

	tick := time.Millisecond
	minHop := time.Millisecond * 10
	maxHop := minHop
	var seed [32]byte
	scen := newScenario(tick, minHop, maxHop, seed)

	// server creates simnet; must start server first.
	s := &simnet{
		barrier:        !simNetConfig.BarrierOff,
		cfg:            cfg,
		simNetCfg:      simNetConfig,
		srv:            srv,
		halt:           srv.halt,
		cliRegisterCh:  make(chan *clientRegistration),
		srvRegisterCh:  make(chan *serverRegistration),
		alterNodeCh:    make(chan *nodeAlteration),
		msgSendCh:      make(chan *mop),
		msgReadCh:      make(chan *mop),
		addTimer:       make(chan *mop),
		discardTimerCh: make(chan *mop),
		newScenarioCh:  make(chan *scenario),
		scenario:       scen,

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

	cfg.simnetRendezvous.singleSimnet = s
	//vv("newSimNetOnServer: assigned to singleSimnet, releasing lock by  goro = %v", GoroNumber())
	s.Start()

	return s
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
	s2c := newSimnetConn()
	s2c.isCli = false
	s2c.net = s
	s2c.local = srvnode
	s2c.remote = clinode
	s2c.netAddr = srvnode.netAddr

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
	c2s := newSimnetConn()
	c2s.isCli = true
	c2s.net = s
	c2s.local = clinode
	c2s.remote = srvnode
	c2s.netAddr = clinode.netAddr

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

// scenario will, in the future, provide for testing different
// timeout settings under network partition and
// flakiness (partial failure). Stubbed for now.
type scenario struct {
	seed [32]byte
	rng  *mathrand2.ChaCha8

	// we enforce ending in 00_000 ns for all tick
	tick   time.Duration
	minHop time.Duration
	maxHop time.Duration
}

func enforceTickDur(tick time.Duration) time.Duration {
	return tick.Truncate(timeMask0)
}

func newScenario(tick, minHop, maxHop time.Duration, seed [32]byte) *scenario {
	return &scenario{
		seed:   seed,
		rng:    mathrand2.NewChaCha8(seed),
		tick:   enforceTickDur(tick),
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

func (s *pq) deleteAll() {
	s.tree.DeleteAll()
	return
}

// order by arrivalTm; for the pre-arrival preArrQ.
//
// Note: must be deterministic iteration order! Don't
// use random tie breakers in here.
// Otherwise we might decide, as the dispatcher does,
// that the mop we wanted to delete on the first
// pass is not there when we look again, or vice-versa.
// We learned this the hard way.
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

func (s *simnet) shutdownNode(node *simnode) {
	//vv("handleAlterNode: SHUTDOWN %v, going from %v -> HALTED", node.state, node.name)
	node.state = HALTED
	node.readQ.deleteAll()
	node.preArrQ.deleteAll()
	node.timerQ.deleteAll()
	//vv("handleAlterNode: end SHUTDOWN, node is now: %v", node)
}

func (s *simnet) restartNode(node *simnode) {
	//vv("handleAlterNode: RESTART %v, wiping queues, going %v -> HEALTHY", node.state, node.name)
	node.state = HEALTHY
	node.readQ.deleteAll()
	node.preArrQ.deleteAll()
	node.timerQ.deleteAll()
}

func (s *simnet) partitionNode(node *simnode) {
	//vv("handleAlterNode: from %v -> PARTITION %v, wiping pre-arrival, block any future pre-arrivals", node.state, node.name)
	node.state = PARTITIONED
	node.preArrQ.deleteAll()
}
func (s *simnet) unPartitionNode(node *simnode) {
	//vv("handleAlterNode: UNPARTITION %v, going from %v -> HEALTHY", node.state, node.name)
	node.state = HEALTHY
}

func (s *simnet) handleAlterNode(alt *nodeAlteration) {
	node := alt.simnode
	switch alt.alter {
	case SHUTDOWN:
		s.shutdownNode(node)
	case PARTITION:
		s.partitionNode(node)
	case UNPARTITION:
		s.unPartitionNode(node)
	case RESTART:
		s.restartNode(node)
	}
	close(alt.done)
}

func (s *simnet) handleSend(send *mop) {
	////zz("top of handleSend(send = '%v')", send)

	switch send.origin.state {
	case HALTED:
		// cannot send when halted. Hmm.
		// This must be a stray...maybe a race? the
		// node really should not be doing anything.
		alwaysPrintf("yuck: got a SEND from a HALTED node: '%v'", send.origin)
		close(send.proceed) // probably just a shutdown race, don't deadlock them.
		return
	case PARTITIONED, HEALTHY:
	}

	origin := send.origin
	if send.seen == 0 {
		send.senderLC = origin.lc
		send.originLC = origin.lc
		send.arrivalTm = userMaskTime(send.initTm.Add(s.scenario.rngHop()))
	}
	send.seen++
	if send.seen != 1 {
		panic(fmt.Sprintf("should see each send only once now, not %v", send.seen))
	}

	switch send.target.state {
	case HALTED, PARTITIONED:
		//vv("send.target.state == %v, dropping msg = '%v'", send.target.state, send.msg)
	case HEALTHY:

		// make a copy _before_ the sendMessage() call returns,
		// so they can recycle or do whatever without data racing with us.
		// Weird: even with this, the Fragment is getting
		// races, not the Message.
		send.msg = send.msg.CopyForSimNetSend()

		send.target.preArrQ.add(send)
		//vv("LC:%v  SEND TO %v %v", origin.lc, origin.name, send)
		////zz("LC:%v  SEND TO %v %v    srvPreArrQ: '%v'", origin.lc, origin.name, send, s.srvnode.preArrQ)
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

	switch read.origin.state {
	case HALTED:
		// cannot read when halted. Hmm.
		// This must be a stray...maybe a race? the
		// node really should not be doing anything.
		alwaysPrintf("yuck: got a READ from a HALTED node: '%v'", read.origin)
		close(read.proceed) // probably just a shutdown race, don't deadlock them.
		return
	case PARTITIONED, HEALTHY:
	}

	origin := read.origin
	if read.seen == 0 {
		read.originLC = origin.lc
	}
	read.seen++
	if read.seen != 1 {
		panic(fmt.Sprintf("should see each send only once now, not %v", read.seen))
	}

	origin.readQ.add(read)
	//vv("LC:%v  READ at %v: %v", origin.lc, origin.name, read)
	////zz("LC:%v  READ %v at %v, now cliReadQ: '%v'", origin.lc, origin.name, read, origin.readQ)
	now := time.Now()
	origin.dispatch(now)
}

func (node *simnode) firstPreArrivalTimeLTE(now time.Time) bool {

	preIt := node.preArrQ.tree.Min()
	if preIt == node.preArrQ.tree.Limit() {
		return false // empty queue
	}
	send := preIt.Item().(*mop)
	return !send.arrivalTm.After(now)
}

func (node *simnode) optionallyApplyChaos() {

	// based on node.net.scenario
	//
	// *** here is where we could re-order
	// messages in a chaos test.
	// reads can start before sends,
	// the read blocks until they match.
	//
	// reads can start after sends,
	// the kernel buffers the sends
	// until the read attempts (our pre-arrival queue).
}

func lte(a, b time.Time) bool {
	return !a.After(b)
}
func gte(a, b time.Time) bool {
	return !a.Before(b)
}

// does not call armTimer(), so scheduler should
// afterwards.
func (node *simnode) dispatchTimers(now time.Time) (changes int64) {
	if node.state == HALTED {
		return 0
	}
	if node.timerQ.tree.Len() == 0 {
		return
	}

	timerIt := node.timerQ.tree.Min()
	for timerIt != node.timerQ.tree.Limit() {

		timer := timerIt.Item().(*mop)
		//vv("check TIMER: %v", timer)

		if lte(timer.completeTm, now) {
			// timer.completeTm <= now

			//vv("have TIMER firing")
			changes++
			timer.timerFiredTm = now
			// advance, and delete behind us
			delmeIt := timerIt
			timerIt = timerIt.Next()
			node.timerQ.tree.DeleteWithIterator(delmeIt)

			select {
			case timer.timerC <- now:
			case <-node.net.halt.ReqStop.Chan:
				return
			default:
				// The Go runtime will delay the timer channel
				// send until a receiver goro can receive it,
				// but we cannot. Hence we use a goroutine if
				// we didn't get through on the above attempt.
				atomic.AddInt64(&node.net.backgoundTimerGoroCount, 1)
				go node.backgroundFireTimer(timer, now)
			}
		} else {
			// INVAR: smallest timer > now
			return
		}
	}
	return
}

func (node *simnode) backgroundFireTimer(timer *mop, now time.Time) {
	select {
	case timer.timerC <- now:
	case <-node.net.halt.ReqStop.Chan:
	}
	atomic.AddInt64(&node.net.backgoundTimerGoroCount, -1)
}

// does not call armTimer.
func (node *simnode) dispatchReadsSends(now time.Time) (changes int64) {

	switch node.state {
	case HALTED:
		// cannot send, receive, start timers or
		// discard them; when halted.
		return
	case PARTITIONED:
		// timers need to fire.
		// pre-arrival Q will be empty, so
		// no matching will happen anyway.
		// reads are fine.
	case HEALTHY:
	}

	// to be deleted at the end, so
	// we don't dirupt the iteration order
	// and miss something.
	var preDel []*mop
	var readDel []*mop

	// skip the not dispatched assert on shutdown
	shuttingDown := false
	// for assert we dispatched all we could
	var endOn, lastMatchSend, lastMatchRead *mop
	var endOnSituation string

	// We had an early bug from returning early and
	// forgetting about preDel, readDel
	// that were waiting for deletion at
	// the end. Doing the deletes here in
	// a defer allows us to return safely at any
	// point, and still do the required cleanup.
	defer func() {
		// take care of any deferred-to-keep-sane iteration deletes
		nPreDel := len(preDel)
		var delListSN []int64
		for _, op := range preDel {
			////zz("delete '%v'", op)
			// TODO: delete with iterator! should be more reliable and faster.
			found := node.preArrQ.tree.DeleteWithKey(op)
			if !found {
				panic(fmt.Sprintf("failed to delete from preArrQ: '%v'; preArrQ = '%v'", op, node.preArrQ.String()))
			}
			delListSN = append(delListSN, op.sn)
		}
		for _, op := range readDel {
			////zz("delete '%v'", op)
			found := node.readQ.tree.DeleteWithKey(op)
			if !found {
				panic(fmt.Sprintf("failed to delete from readQ: '%v'", op))
			}
		}
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
			// So make this conditional on faketime being in use.
			if !shuttingDown && faketime { // && node.net.barrier {

				now2 := time.Now()
				if node.firstPreArrivalTimeLTE(now2) {
					alwaysPrintf("ummm... why did these not get dispatched? narr = %v, nread = %v; nPreDel = %v; delListSN = '%v', (now2 - now = %v);\n endOn = %v\n lastMatchSend=%v \n lastMatchRead = %v \n\n endOnSituation = %v\n summary node summary:\n%v", narr, nread, nPreDel, delListSN, now2.Sub(now), endOn, lastMatchSend, lastMatchRead, endOnSituation, node.String())
					panic("should have been dispatchable, no?")
				}
			}
		}
	}()

	nR := node.readQ.tree.Len()   // number of reads
	nS := node.preArrQ.tree.Len() // number of sends

	if nR == 0 && nS == 0 {
		return
	}

	readIt := node.readQ.tree.Min()
	preIt := node.preArrQ.tree.Min()

	// matching reads and sends
	for {
		if readIt == node.readQ.tree.Limit() {
			// no reads, no point.
			return
		}
		if preIt == node.preArrQ.tree.Limit() {
			// no sends to match with reads
			return
		}

		read := readIt.Item().(*mop)
		send := preIt.Item().(*mop)

		node.optionallyApplyChaos()

		// Causality also demands that
		// a read can complete (now) only after it was initiated;
		// and so can be matched (now) only to a send already initiated.

		// To keep from violating causality,
		// during our chaos testing, we want
		// to make sure that the read completion (now)
		// cannot happen before the send initiation.
		// Also forbid any reads that have not happened
		// "yet" (now), should they get rearranged by chaos.
		if now.Before(read.initTm) {
			alwaysPrintf("rejecting delivery to read that has not happened: '%v'", read)
			panic("how possible?")
			return
		}
		// INVAR: this read.initTm <= now
		changes++

		if send.arrivalTm.After(now) {
			// send has not arrived yet.

			// are we done? since preArrQ is ordered
			// by arrivalTm, all subsequent pre-arrivals (sends)
			// will have even more _greater_  arrivalTm;
			// so no point in looking.

			// super noisy!
			//vv("rejecting delivery of send that has not happened: '%v'", send)
			//vv("dispatch: %v", node.net.schedulerReport())
			endOn = send
			it2 := preIt
			afterN := 0
			for ; it2 != node.preArrQ.tree.Limit(); it2 = it2.Next() {
				afterN++
			}
			endOnSituation = fmt.Sprintf("after me: %v; preArrQ: %v", afterN, node.preArrQ.String())

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

		read.isEOF_RST = send.isEOF_RST // convey EOF/RST
		if send.isEOF_RST {
			//vv("copied EOF marker from send '%v' \n to read: '%v'", send, read)
		}

		lastMatchSend = send
		lastMatchRead = read
		// advance our logical clock
		node.lc = max(node.lc, send.originLC) + 1
		////zz("servicing cli read: started LC %v -> serviced %v (waited: %v) read.sn=%v", read.originLC, node.lc, node.lc-read.originLC, read.sn)

		// track clocks on either end for this send and read.
		read.readerLC = node.lc
		read.senderLC = send.senderLC
		send.readerLC = node.lc
		read.completeTm = now
		read.arrivalTm = send.arrivalTm // easier diagnostics

		// matchmaking
		//vv("[1]matchmaking: \nsend '%v' -> \nread '%v'", send, read)
		read.sendmop = send
		send.readmop = read

		preDel = append(preDel, send)
		readDel = append(readDel, read)

		close(read.proceed)
		// send already closed in handleSend()

		readIt = readIt.Next()
		preIt = preIt.Next()

	} // end for
}

// dispatch delivers sends to reads, and fires timers.
// It calls node.net.armTimer() at the end (in the defer).
func (node *simnode) dispatch(now time.Time) (changes int64) {

	switch node.state {
	case HALTED:
		// cannot send, receive, start timers or
		// discard them; when halted.
		return
	case PARTITIONED:
		// timers need to fire.
		// pre-arrival Q will be empty, so
		// no matching will happen anyway.
		// reads are fine.
	case HEALTHY:
	}

	// to be deleted at the end, so
	// we don't dirupt the iteration order
	// and miss something.
	var preDel []*mop
	var readDel []*mop
	var timerDel []*mop

	// skip the not dispatched assert on shutdown
	shuttingDown := false
	// for assert we dispatched all we could
	var endOn, lastMatchSend, lastMatchRead *mop
	var endOnSituation string

	// We had an early bug from returning early and
	// forgetting about preDel, readDel, timerDel
	// that were waiting for deletion at
	// the end. Doing the deletes here in
	// a defer allows us to return safely at any
	// point, and still do the required cleanup.
	defer func() {
		// take care of any deferred-to-keep-sane iteration deletes
		nPreDel := len(preDel)
		var delListSN []int64
		for _, op := range preDel {
			////zz("delete '%v'", op)
			// TODO: delete with iterator! should be more reliable and faster.
			found := node.preArrQ.tree.DeleteWithKey(op)
			if !found {
				panic(fmt.Sprintf("failed to delete from preArrQ: '%v'; preArrQ = '%v'", op, node.preArrQ.String()))
			}
			delListSN = append(delListSN, op.sn)
		}
		for _, op := range readDel {
			////zz("delete '%v'", op)
			found := node.readQ.tree.DeleteWithKey(op)
			if !found {
				panic(fmt.Sprintf("failed to delete from readQ: '%v'", op))
			}
		}
		for _, op := range timerDel {
			////zz("delete '%v'", op)
			found := node.timerQ.tree.DeleteWithKey(op)
			if !found {
				panic(fmt.Sprintf("failed to delete from timerQ: '%v'", op))
			}
		}
		//if changes > 0 {
		// let scheduler, to avoid false alarms: node.net.armTimer(now)
		//}
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
			// So make this conditional on faketime being in use.
			if !shuttingDown && faketime { // && node.net.barrier {

				now2 := time.Now()
				if node.firstPreArrivalTimeLTE(now2) {
					alwaysPrintf("ummm... why did these not get dispatched? narr = %v, nread = %v; nPreDel = %v; delListSN = '%v', (now2 - now = %v);\n endOn = %v\n lastMatchSend=%v \n lastMatchRead = %v \n\n endOnSituation = %v\n summary node summary:\n%v", narr, nread, nPreDel, delListSN, now2.Sub(now), endOn, lastMatchSend, lastMatchRead, endOnSituation, node.String())
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

	// do timers first, so we can exit
	// immediately if no sends and reads match.
	for ; timerIt != node.timerQ.tree.Limit(); timerIt = timerIt.Next() {

		timer := timerIt.Item().(*mop)
		//vv("check TIMER: %v", timer)

		if !now.Before(timer.completeTm) {
			// timer.completeTm <= now
			pp("have TIMER firing: %v", timer)
			changes++
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
				shuttingDown = true
				return
			}
		} else {
			// smallest timer > now
			break // check send->read next, don't return yet.
		}
	}

	// done with timers. on to matching reads and sends.

	for {
		if readIt == node.readQ.tree.Limit() {
			// no reads, no point.
			return
		}
		if preIt == node.preArrQ.tree.Limit() {
			// no sends to match with reads
			return
		}

		read := readIt.Item().(*mop)
		send := preIt.Item().(*mop)

		node.optionallyApplyChaos()

		// Causality also demands that
		// a read can complete (now) only after it was initiated;
		// and so can be matched (now) only to a send already initiated.

		// To keep from violating causality,
		// during our chaos testing, we want
		// to make sure that the read completion (now)
		// cannot happen before the send initiation.
		// Also forbid any reads that have not happened
		// "yet" (now), should they get rearranged by chaos.
		if now.Before(read.initTm) {
			alwaysPrintf("rejecting delivery to read that has not happened: '%v'", read)
			panic("how possible?")
			return
		}
		// INVAR: this read.initTm <= now
		changes++

		if send.arrivalTm.After(now) {
			// send has not arrived yet.

			// are we done? since preArrQ is ordered
			// by arrivalTm, all subsequent pre-arrivals (sends)
			// will have even more _greater_  arrivalTm;
			// so no point in looking.

			// super noisy!
			//vv("rejecting delivery of send that has not happened: '%v'", send)
			//vv("dispatch: %v", node.net.schedulerReport())
			endOn = send
			it2 := preIt
			afterN := 0
			for ; it2 != node.preArrQ.tree.Limit(); it2 = it2.Next() {
				afterN++
			}
			endOnSituation = fmt.Sprintf("after me: %v; preArrQ: %v", afterN, node.preArrQ.String())

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

		read.isEOF_RST = send.isEOF_RST // convey EOF/RST
		if send.isEOF_RST {
			//vv("copied EOF marker from send '%v' \n to read: '%v'", send, read)
		}

		lastMatchSend = send
		lastMatchRead = read
		// advance our logical clock
		node.lc = max(node.lc, send.originLC) + 1
		//pp("servicing cli read: started LC %v -> serviced %v (waited: %v) read.sn=%v", read.originLC, node.lc, node.lc-read.originLC, read.sn)

		// track clocks on either end for this send and read.
		read.readerLC = node.lc
		read.senderLC = send.senderLC
		send.readerLC = node.lc
		read.completeTm = now
		read.arrivalTm = send.arrivalTm // easier diagnostics

		// matchmaking
		//pp("[1]matchmaking: \nsend '%v' -> \nread '%v'", send, read)
		read.sendmop = send
		send.readmop = read

		preDel = append(preDel, send)
		readDel = append(readDel, read)

		close(read.proceed)
		// send already closed in handleSend()

		readIt = readIt.Next()
		preIt = preIt.Next()

	} // end for
}

func (s *simnet) qReport() (r string) {
	i := 0
	for node := range s.nodes {
		r += fmt.Sprintf("\n[node %v of %v in qReport]: \n", i+1, len(s.nodes))
		r += node.String() + "\n"
		i++
	}
	return
}

func (s *simnet) schedulerReport() string {
	now := time.Now()
	return fmt.Sprintf("lastArmTm.After(now) = %v [%v out] %v; qReport = '%v'", s.lastArmTm.After(now), s.lastArmTm.Sub(now), s.lastArmTm, s.qReport())
}

func (s *simnet) dispatchAll(now time.Time) (changes int64) {
	for node := range s.nodes {
		changes += node.dispatch(now)
	}
	return
}

// does not call armTimer(), so scheduler should afterwards.
func (s *simnet) dispatchAllTimers(now time.Time) (changes int64) {
	for node := range s.nodes {
		changes += node.dispatchTimers(now)
	}
	return
}

// does not call armTimer(), so scheduler should afterwards.
func (s *simnet) dispatchAllReadsSends(now time.Time) (changes int64) {
	for node := range s.nodes {
		changes += node.dispatchReadsSends(now)
	}
	return
}

func (s *simnet) tickLogicalClocks() {
	for node := range s.nodes {
		node.lc++
	}
}

func (s *simnet) Start() {
	alwaysPrintf("simnet.Start: faketime = %v; s.barrier=%v", faketime, s.barrier)
	go s.scheduler()
}

// durToGridPoint:
// given the time now, return the dur to
// get us to the next grid point, which is goal.
func (s *simnet) durToGridPoint(now time.Time, tick time.Duration) (dur time.Duration, goal time.Time) {

	// this handles both
	// a) we are on a grid point now; and
	// b) we are off a grid point and we want the next one.

	// since tick is already enforceTickDur()
	// we shouldn't need to systemMaskTime, but just in case
	// that changes... it is cheap anyway, and conveys the
	// convention in force.
	goal = systemMaskTime(now.Add(tick).Truncate(tick))

	dur = goal.Sub(now)
	//vv("i=%v; just before dispatchAll(), durToGridPoint computed: dur=%v -> goal:%v to reset the gridTimer; tick=%v", i, dur, goal, s.scenario.tick)
	if dur <= 0 { // i.e. now <= goal
		panic(fmt.Sprintf("why was proposed sleep dur = %v <= 0 ? tick=%v; bigbang=%v; now=%v", dur, tick, s.bigbang, now))
	}
	return
}

// scheduler is the heart of the simnet
// network simulator. It is the central
// goroutine launched by simnet.Start().
// It orders all timer and network
// operations requests by receiving them
// in a select on its various channels, each
// dedicated to one type of call.
//
// After each dispatchAll, our channel closes
// have started node goroutines, so they
// are running and may now be trying
// to do network operations. The
// select below will service those
// operations, or take a time step
// if nextTimer goes off first. Since
// synctest ONLY advances time when
// all goro are blocked, nextTimer
// will go off last, once all other node
// goro have blocked. This is nice: it does
// not required another barrier opreation
// to get to "go last". Client code
// might also be woken at the instant
// that nextTimer fires, but we have no
// a guarantee that they won't race with
// our timer: they could do further network
// operations through our select cases
// before our timer gets to fire. But
// this should be rare and only apply
// to client code using time.Timer/After directly,
// as our mock SimTimer will not be woken:
// that is also already simulated/combined
// into the nextTimer. This is why we would
// like _all_ sim code to use SimTimer
// for a more highly deterministic simulation test.
// Since that invasively requires changing
// user code in some cases, the tradeoff is
// unavoidable. The user client code can stay as is,
// and will work/should test, but errors/red
// tests will not be as reproducible as
// possible, compared to if it were to be
// udpated use the SimTimer instead.
func (s *simnet) scheduler() {
	//vv("scheduler is running on goro = %v", GoroNumber())

	defer func() {
		//vv("scheduler defer shutdown running on goro = %v", GoroNumber())
		r := recover()
		if r != nil {
			alwaysPrintf("scheduler panic-ing: %v", r)
			//alwaysPrintf("scheduler panic-ing: %v", s.schedulerReport())
			panic(r)
		}
	}()

	var nextReport time.Time

	// main scheduler loop
	now := time.Now()

	// bigbang is the "start of time" -- don't change below.
	s.bigbang = now

	var totalSleepDur time.Duration

	// get regular scheduler wakeups on a time
	// grid of step size s.scenario.tick.
	// We can get woken earlier too by
	// sends, reads, and timers. The nextTimer
	// logic below can decide how it wants
	// to handle that.
	timer := newTimerCreateMop(false)
	timer.proceed = nil          // never used, don't leak it.
	timer.isGridStepTimer = true // should be only one.
	timer.initTm = now
	timer.timerDur = s.scenario.tick
	timer.completeTm = now.Add(s.scenario.tick)
	timer.timerFileLine = fileLine(3)

	// As a special case, armTimer always includes
	// the gridStepTimer when computing the minimum
	// next timer to go off, so barrier cannot deadlock.
	s.gridStepTimer = timer

	// always have at least one timer going.
	s.armTimer(now)

restartI:
	for i := int64(0); ; i++ {

		//if i == 42 {
		//	verboseVerbose = true
		//}

		// number of dispatched operations
		var nd0 int64

		now := time.Now()
		if gte(now, nextReport) {
			nextReport = now.Add(time.Second)
			//vv("scheduler top")
			//cli.lc = %v ; srv.lc = %v", clilc, srvlc)
			//vv("scheduler top. schedulerReport: \n%v", s.schedulerReport())
		}

		// To maximize reproducbility, this barrier lets all
		// other goro get durably blocked, then lets just us proceed.
		if faketime && s.barrier {
			synctestWait_LetAllOtherGoroFinish() // 1st barrier
		}
		// under faketime, we are alone now.
		// Time has not advanced. This is the
		// perfect point at which to advance the
		// event/logical clock of each node, as no races.
		s.tickLogicalClocks()

		// only dispatch one place, in nextTimer now.
		// simpler, easier to reason about. this is viable too,
		// but creates less determinism.
		//changed := s.dispatchAll(now) // sends, reads, and timers.
		//nd0 += changed
		//vv("i=%v, dispatchAll changed=%v, total nd0=%v", i, changed, nd0)

		preSelectTm := now
		select { // scheduler main select

		case <-s.nextTimer.C: // time advances when soonest timer fires
			now = time.Now()
			totalSleepDur += now.Sub(preSelectTm)
			//vv("i=%v, nextTimer fired. totalSleepDur = %v; last = %v", i, totalSleepDur, now.Sub(preSelectTm))

			// max determinism: go last
			// among all goro who were woken by other
			// timers that fired at this instant.
			if faketime && s.barrier {
				synctestWait_LetAllOtherGoroFinish() // 2nd barrier
			}
			s.tickLogicalClocks()

			nd0 += s.dispatchAll(now)
			s.refreshGridStepTimer(now)
			s.armTimer(now)

		case reg := <-s.cliRegisterCh:
			// "connect" in network lingo, client reaches out to listening server.
			//vv("i=%v, cliRegisterCh got reg from '%v' = '%#v'", i, reg.client.name, reg)
			s.handleClientRegistration(reg)
			//vv("back from handleClientRegistration for '%v'", reg.client.name)

		case srvreg := <-s.srvRegisterCh:
			// "bind/listen" on a socket, server waits for any client to "connect"
			//vv("i=%v, s.srvRegisterCh got srvreg for '%v'", i, srvreg.server.name)
			s.handleServerRegistration(srvreg)
			// do not vv here, as it is very racey with the server who
			// has been given permission to proceed.

		case scenario := <-s.newScenarioCh:
			s.finishScenario()
			s.initScenario(scenario)
			i = 0
			continue restartI

		case timer := <-s.addTimer:
			//vv("i=%v, addTimer ->  op='%v'", i, timer)
			s.handleTimer(timer)

		case discard := <-s.discardTimerCh:
			//vv("i=%v, discardTimer ->  op='%v'", i, discard)
			s.handleDiscardTimer(discard)

		case send := <-s.msgSendCh:
			//vv("i=%v, msgSendCh ->  op='%v'", i, send)
			s.handleSend(send)

		case read := <-s.msgReadCh:
			//vv("i=%v msgReadCh ->  op='%v'", i, read)
			s.handleRead(read)

		case alt := <-s.alterNodeCh:
			s.handleAlterNode(alt)

		case <-s.halt.ReqStop.Chan:
			bb := time.Since(s.bigbang)
			pct := 100 * float64(totalSleepDur) / float64(bb)
			_ = pct
			//vv("simnet.halt.ReqStop totalSleepDur = %v (%0.2f%%) since bb = %v)", totalSleepDur, pct, bb)
			return
		} // end select

		//vv("i=%v bottom of scheduler loop. num dispatch events = %v", i, nd0)
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
	//now := time.Now()

	switch discard.origin.state {
	case HALTED:
		// cannot set/fire timers when halted. Hmm.
		// This must be a stray...maybe a race? the
		// node really should not be doing anything.
		alwaysPrintf("yuck: got a TIMER_DISCARD from a HALTED node: '%v'", discard.origin)
		close(discard.proceed) // probably just a shutdown race, don't deadlock them.
		return
	case PARTITIONED, HEALTHY:
	}

	orig := discard.origTimerMop

	found := discard.origin.timerQ.del(discard.origTimerMop)
	if found {
		discard.wasArmed = !orig.timerFiredTm.IsZero()
		discard.origTimerCompleteTm = orig.completeTm
	} // leave wasArmed false, could not have been armed if gone.

	////zz("LC:%v %v TIMER_DISCARD %v to fire at '%v'; now timerQ: '%v'", discard.origin.lc, discard.origin.name, discard, discard.origTimerCompleteTm, s.clinode.timerQ)
	// let scheduler, to avoid false alarms: s.armTimer(now)
	close(discard.proceed)
}

func (s *simnet) handleTimer(timer *mop) {
	//now := time.Now()

	switch timer.origin.state {
	case HALTED:
		// cannot start timers when halted. Hmm.
		// This must be a stray...maybe a race? the
		// node really should not be doing anything.
		// This does happen though, e.g. in test
		// Test010_tube_write_new_value_two_replicas,
		// so don't freak. Probably just a shutdown race.
		alwaysPrintf("yuck: got a timer from a HALTED node: '%v'", timer.origin)
		close(timer.proceed) // likely shutdown race, don't deadlock them.
		return
	case PARTITIONED, HEALTHY:
	}

	lc := timer.origin.lc
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

	// mask it up!
	timer.unmaskedCompleteTm = timer.completeTm
	timer.unmaskedDur = timer.timerDur
	timer.completeTm = userMaskTime(timer.completeTm)
	timer.timerDur = timer.completeTm.Sub(timer.initTm)
	//vv("masked timer:\n dur: %v -> %v\n completeTm: %v -> %v\n", timer.unmaskedDur, timer.timerDur, timer.unmaskedCompleteTm, timer.completeTm)

	timer.origin.timerQ.add(timer)
	////zz("LC:%v %v set TIMER %v to fire at '%v'; now timerQ: '%v'", lc, timer.origin.name, timer, timer.completeTm, s.clinode.timerQ)

	// let scheduler, to avoid false alarms: s.armTimer(now) // handleTimer
}

// refreshGridStepTimer context:
// Some dispatch() call just before us
// might have re-armed the nextTmer,
// but equally none might have.
// Also we cannot be the only
// place that armTimer is called, because
// another select case may have changed
// things and set a new nextTimer, and
// we would not have gotten to again
// before that next timer fires us
// to add in our own gridStepTimer.
// So we have to do armTimer here,
// even though it might be redundant
// on occassion, to ensure nextTimer is
// armed. This is cheap anyway, just
// a lookup of the min node in
// the each priority queue, which our
// red-black tree has cached anyway.
// For K simnodes * 3 PQ per node => O(K).
//
// armTimer is not called; keep it as a separate step.
func (s *simnet) refreshGridStepTimer(now time.Time) (dur time.Duration, goal time.Time) {
	if gte(now, s.gridStepTimer.completeTm) {
		s.gridStepTimer.initTm = now
		dur, goal = s.durToGridPoint(now, s.scenario.tick)
		s.gridStepTimer.timerDur = dur
		s.gridStepTimer.completeTm = goal

		if gte(now, s.gridStepTimer.completeTm) {
			panic(fmt.Sprintf("durToGridPoint() gave completeTm(%v) <= now(%v); should be impossible, no? are we servicing all events in order? are we missing a wakeup? oversleeping? wat?", s.gridStepTimer.completeTm, now))
		}
	}
	return
}

func (s *simnet) armTimer(now time.Time) time.Duration {

	var minTimer *mop = s.gridStepTimer
	if s.gridStepTimer.completeTm.Before(now) {
		//panic(fmt.Sprintf("gridStepTimer(%v) not refreshed! < now %v", s.gridStepTimer.completeTm, now))
		// just fix it by refreshing.
		s.refreshGridStepTimer(now)
		minTimer = s.gridStepTimer
	}
	for node := range s.nodes {
		minTimer = node.soonestTimerLessThan(minTimer)
	}
	if minTimer == nil {
		panic("should never happen, s.gridStepTimer should always be active")
		return 0
	}

	dur := minTimer.completeTm.Sub(now)
	////zz("dur=%v = when(%v) - now(%v)", dur, minTimer.completeTm, now)
	if dur <= 0 {
		vv("no timers, what?? minTimerDur = %v", dur)
		panic("must always have at least the grid timer!")
	}
	s.lastArmTm = now
	s.nextTimer.Reset(dur)
	return dur
}

func (node *simnode) soonestTimerLessThan(bound *mop) *mop {

	//if bound != nil {
	//vv("soonestTimerLessThan(bound.completeTm = '%v'", bound.completeTm)
	//} else {
	//vv("soonestTimerLessThan(nil bound)")
	//}
	it := node.timerQ.tree.Min()
	if it == node.timerQ.tree.Limit() {
		//vv("we have no timers, returning bound")
		return bound
	}
	minTimer := it.Item().(*mop)
	if bound == nil {
		// no lower bound yet
		//vv("we have no lower bound, returning min timer: '%#v'", minTimer)
		return minTimer
	}
	if minTimer.completeTm.Before(bound.completeTm) {
		//vv("minTimer.completeTm(%v) < bound.completeTm(%v)", minTimer.completeTm, bound.completeTm)
		return minTimer
	}
	//vv("soonestTimerLessThan end: returning bound")
	return bound
}

const timeMask0 = time.Microsecond * 100
const timeMask9 = time.Microsecond*100 - 1

// maskTime makes the last 5 digits
// of a nanosecond timestamp all 9s: 99_999
// Any digit above 100 microseconds is unchanged.
//
// This can be used to order wake from sleep/timer events.
//
// If we start with
// 2006-01-02T15:04:05.000000000-07:00
// maskTime will return
// 2006-01-02T15:04:05.000099999-07:00
func userMaskTime(tm time.Time) time.Time {
	return tm.Truncate(timeMask0).Add(timeMask9)
}

// system gridTimer points always end in 00_000
func systemMaskTime(tm time.Time) time.Time {
	return tm.Truncate(timeMask0)
}

func CallbackOnNewTimer(
	func(proposedDeadline time.Time,
		pkgFileLine string,
	) (assignedDeadline time.Time)) {

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
// Communicate over channels only: e.g.
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

type serverRegistration struct {
	// provide
	server     *Server
	srvNetAddr *SimNetAddr

	// wait on
	done chan struct{}

	// receive back
	simnode             *simnode // our identity in the simnet (conn.local)
	simnet              *simnet
	tellServerNewConnCh chan *simnetConn
}

// external
func (s *simnet) newServerRegistration(srv *Server, srvNetAddr *SimNetAddr) *serverRegistration {
	return &serverRegistration{
		server:     srv,
		srvNetAddr: srvNetAddr,
		done:       make(chan struct{}),
	}
}

func (s *simnet) registerServer(srv *Server, srvNetAddr *SimNetAddr) (newCliConnCh chan *simnetConn, err error) {

	reg := s.newServerRegistration(srv, srvNetAddr)
	select {
	case s.srvRegisterCh <- reg:
		//vv("sent registration on srvRegisterCh; about to wait on done goro = %v", GoroNumber())
	case <-s.halt.ReqStop.Chan:
		err = ErrShutdown()
		return
	}
	select {
	case <-reg.done:
		//vv("server after first registered: '%v'/'%v' sees  reg.tellServerNewConnCh = %p", srv.name, srvNetAddr, reg.tellServerNewConnCh)
		if reg.tellServerNewConnCh == nil {
			panic("cannot have nil reg.tellServerNewConnCh back!")
		}
		srv.simnode = reg.simnode
		srv.simnet = reg.simnet
		newCliConnCh = reg.tellServerNewConnCh
		return
	case <-s.halt.ReqStop.Chan:
		err = ErrShutdown()
		return
	}
	return
}

// Alteration flags are used in AlterNode() calls
// to specify what change you want to
// a specific network node.
type Alteration int // on clients or servers, any simnode

const (
	SHUTDOWN    Alteration = 1
	PARTITION   Alteration = 2
	UNPARTITION Alteration = 3
	RESTART     Alteration = 4
)

type nodeAlteration struct {
	simnet  *simnet
	simnode *simnode
	alter   Alteration
	done    chan struct{}
}

func (s *simnet) newNodeAlteration(node *simnode, alter Alteration) *nodeAlteration {
	return &nodeAlteration{
		simnet:  s,
		simnode: node,
		alter:   alter,
		done:    make(chan struct{}),
	}
}
func (s *simnet) alterNode(node *simnode, alter Alteration) {

	alt := s.newNodeAlteration(node, alter)
	select {
	case s.alterNodeCh <- alt:
		//vv("sent alt on alterNodeCh; about to wait on done goro = %v", GoroNumber())
	case <-s.halt.ReqStop.Chan:
		return
	}
	select {
	case <-alt.done:
		//vv("server altered: %v", node)
	case <-s.halt.ReqStop.Chan:
		return
	}
	return
}
