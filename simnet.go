package rpc25519

// build/run with:
// GOTRACEBACK=all GOEXPERIMENT=synctest go test -v

import (
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/glycerine/idem"
	rb "github.com/glycerine/rbtree"
)

// NB: all String() methods are now in simnet_string.go

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

	node2server map[*simnode]*simnode

	dns map[string]*simnode

	// circuits[A][B] is the very cyclic (bi-directed?) graph
	// of the network.
	//
	// The simnode.name is the key of the circuits map.
	// Both client and server names are keys in circuits.
	//
	// Each A has a bi-directional network "socket" to each of circuits[A].
	//
	// circuits[A][B] is A's connection to B; that owned by A.
	//
	// circuits[B][A] is B's connection to A; that owned by B.
	//
	// The system guarantees that the keys of circuits
	// (the names of all simnodes) are unique strings,
	// by rejecting any already taken names (panic-ing on attempted
	// registration) during the moral equivalent of
	// server Bind/Listen and client Dial. The go map
	// is not a multi-map anyway, but that is just
	// an implementation detail that happens to provide extra
	// enforcement. Even if we change the map out
	// later, each simnode name in the network must be unique.
	//
	// Users specify their own names for modeling
	// convenience, but assist them by enforcing
	// global name uniqueness.
	//
	// See handleServerRegistration() and
	// handleClientRegistration() where these
	// panics enforce uniqueness.
	//
	// Technically, if the simnode is backed by
	// rpc25519, each simnode has both a rpc25519.Server
	// and a set of rpc25519.Clients, one client per
	// outgoing initated connection, and so there are
	// redundant paths to get a message from
	// A to B through the network. This is necessary
	// because only the Client in TCP is the active initator,
	// and can only talk to one Server. A Server
	// can always talk to any number of Clients,
	// but typically must begin passively and
	// cannot initiate a connection to another
	// server. On TCP, rpc25519 enables active grid
	// creation. Each peer runs a server, and
	// servers establish the grid by automatically creating an
	// internal auto-Client when the server (peer)
	// wishes to initate contact with another server (peer).
	// The QUIC version is... probably similar;
	// since QUIC was slower I have not thought
	// about it in a while--but QUIC can use the
	// same port for client and server (to simplify
	// diagnostics).
	//
	// The simnet tries to not care about these detail,
	// and the rpc25519 peer system is symmetric by design.
	// Thus it will forward a message to the peer no
	// matter where it lives (be it technically on an
	// rpc25519.Client or rpc25519.Server).
	//
	// The isCli flag distinguishes whether a given
	// simnode is on a Client or Server when
	// it matters, but we try to minimize its use.
	// It should not matter for the most part; in
	// the ckt.go code there is even a note that
	// the peerAPI.isCli can be misleading with auto-Clients
	// in play. The simnode.isCli however should be
	// accurate (we think).
	//
	// Each peer-to-peer connection is a network
	// simnode that can send and read messages
	// to exactly one other network simnode.
	//
	// Even during "partition" of the network,
	// or when modeling faulty links or network cards,
	// in general we want to be maintain the
	// most general case of a fully connected
	// network, where any peer can talk to any
	// other peer in the network; like the internet.
	// I think of a simnet as the big single
	// ethernet switch that all circuits plug into.
	//
	// When modeling faults, we try to keep the circuits network
	// as static as possible, and set .deafRead
	// or .dropSend flags to model faults.
	// Reboot/restart should not heal net/net card faults.
	//
	// To recap, both clinode.name and srvnode.name are keys
	// in the circuits map. So circuits[clinode.name] returns
	// the map of who clinode is connected to.
	//
	// In other words, the value of the map circuits[A]
	// is another map, which is the set of circuits that A
	// is connected to by the simconn circuits[A][B].
	circuits map[*simnode]map[*simnode]*simconn
	servers  map[string]*simnode // serverBaseID:srvnode
	allnodes map[*simnode]bool
	orphans  map[*simnode]bool // cli without baseserver

	cliRegisterCh chan *clientRegistration
	srvRegisterCh chan *serverRegistration

	alterSimnodeCh chan *simnodeAlteration
	alterHostCh    chan *simnodeAlteration

	// same as srv.halt; we don't need
	// our own, at least for now.
	halt *idem.Halter

	msgSendCh      chan *mop
	msgReadCh      chan *mop
	addTimer       chan *mop
	discardTimerCh chan *mop

	injectCircuitFaultCh chan *circuitFault
	injectHostFaultCh    chan *hostFault
	repairCircuitCh      chan *circuitRepair
	repairHostCh         chan *hostRepair

	safeStateStringCh chan *simnetSafeStateQuery

	newScenarioCh chan *scenario
	nextTimer     *time.Timer
	lastArmTm     time.Time
}

// a simnode is a client or server in the network.
// Clients talk to only one server, but
// servers can talk to any number of clients.
//
// In grid or cluster simulation, the
// clients are typically the auto-Clients that
// a Server itself has created to initiate
// connections to form a grid.
//
// In this case, by matching
// on serverBaseID, we can group
// the auto-clients and their server node
// together into a simhost: a single
// server peer and all of its auto-clients.
type simnode struct {
	name    string
	lc      int64 // logical clock
	readQ   *pq
	preArrQ *pq
	timerQ  *pq

	deafReadQ    *pq
	droppedSendQ *pq

	net     *simnet
	isCli   bool
	cliConn *simconn

	netAddr      *SimNetAddr
	serverBaseID string

	// state survives power cycling, i.e. rebooting
	// a simnode does not heal the network or repair a
	// faulty network card.
	state    Circuitstate
	powerOff bool

	tellServerNewConnCh chan *simconn

	// servers track their autocli here
	autocli map[*simnode]*simconn
	// autocli + self
	allnode map[*simnode]bool
}

// defean reads/drop sends that were started
// but still in progress with this fault.
func (s *simnet) addFaultsToPQ(now time.Time, origin, target *simnode, dd DropDeafSpec) {

	if !dd.UpdateDeafReads && !dd.UpdateDropSends {
		return
	}
	if dd.UpdateDeafReads && dd.DeafReadsNewProb > 0 {
		s.addFaultsToReadQ(now, origin, target, dd.DeafReadsNewProb)
	}
	if dd.UpdateDropSends && dd.DropSendsNewProb > 0 {
		s.addSendFaults(now, origin, target, dd.DropSendsNewProb)
	}
}

func (s *simnet) addFaultsToReadQ(now time.Time, origin, target *simnode, deafReadProb float64) {

	readIt := origin.readQ.tree.Min()
	for readIt != origin.readQ.tree.Limit() {
		read := readIt.Item().(*mop)
		if target == nil || read.target == target {
			if s.deaf(deafReadProb) {

				//vv("deaf fault enforced on read='%v'", read)
				// don't mark them, so we can restore them later.
				origin.deafReadQ.add(read)

				// advance readIt, and delete behind
				delmeIt := readIt
				readIt = readIt.Next()
				origin.readQ.tree.DeleteWithIterator(delmeIt)
				continue
			}
		}
		readIt = readIt.Next()
	}
}

func (s *simnet) addSendFaults(now time.Time, origin, target *simnode, dropSendProb float64) {

	// have to look for origin's sends in all other pre-arrQ...
	// and check all, in case disconnect happened since...

	for node := range s.circuits {
		if node == origin {
			// No way at present for a TCP client or server
			// to read or send to itself. Different sockets
			// on the same host would be different nodes.
			continue
		}
		if target != nil && node != target {
			continue
		}
		sendIt := node.preArrQ.tree.Min()
		for sendIt != node.preArrQ.tree.Limit() {

			send := sendIt.Item().(*mop)

			if gte(send.arrivalTm, now) {
				// droppable, due to arrive >= now
			} else {
				// INVAR: smallest time send < now.
				//
				// preArrQ is ordered by arrivalTm,
				// but that doesn't let us short-circuit here,
				// since we must fault everything due
				// to arrive >= now. So we keep scanning.
				continue
			}
			if send.origin == origin &&
				s.dropped(dropSendProb) {

				//vv("drop fault enforced on send='%v'", send)
				// don't mark them, so we can restore them later.
				//send.sendIsDropped = true
				//send.isDropDeafFault = true
				node.droppedSendQ.add(send)

				// advance sendIt, and delete behind
				delmeIt := sendIt
				sendIt = sendIt.Next()
				node.preArrQ.tree.DeleteWithIterator(delmeIt)
				continue
			}
			sendIt = sendIt.Next()
		}
	}
}

func (s *simnet) locals(node *simnode) map[*simnode]bool {
	srvnode, ok := s.node2server[node]
	if !ok {
		panic(fmt.Sprintf("not registered in s.node2server: node = '%v'", node.name))
	}
	return srvnode.allnode
}

func (s *simnet) localServer(node *simnode) *simnode {
	srvnode, ok := s.node2server[node]
	if !ok {
		panic(fmt.Sprintf("not registered in s.node2server: node = '%v'", node.name))
	}
	return srvnode
}

func (s *simnet) newSimnode(name, serverBaseID string) *simnode {
	return &simnode{
		name:         name,
		serverBaseID: serverBaseID,
		readQ:        newPQinitTm(name + " readQ "),
		preArrQ:      s.newPQarrivalTm(name + " preArrQ "),
		timerQ:       newPQcompleteTm(name + " timerQ "),
		deafReadQ:    newPQinitTm(name + " deaf reads Q "),
		droppedSendQ: s.newPQarrivalTm(name + " dropped sends Q "),
		net:          s,

		// clients don't need these, so we could make them lazily
		autocli: make(map[*simnode]*simconn),
		allnode: make(map[*simnode]bool),
	}
}

func (s *simnet) newSimnodeClient(name, serverBaseID string) (simnode *simnode) {
	simnode = s.newSimnode(name, serverBaseID)
	simnode.isCli = true
	return
}

func (s *simnet) newCircuitserver(name, serverBaseID string) (simnode *simnode) {
	simnode = s.newSimnode(name, serverBaseID)
	simnode.isCli = false
	// buffer so servers don't have to be up to get them.
	simnode.tellServerNewConnCh = make(chan *simconn, 100)
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

	srvnode := s.newCircuitserver(reg.server.name, reg.serverBaseID)
	srvnode.allnode[srvnode] = true
	srvnode.netAddr = reg.srvNetAddr
	s.circuits[srvnode] = make(map[*simnode]*simconn)
	_, already := s.dns[srvnode.name]
	if already {
		panic(fmt.Sprintf("server name already taken: '%v'", srvnode.name))
	}
	s.dns[srvnode.name] = srvnode
	s.node2server[srvnode] = srvnode

	basesrv, ok := s.servers[reg.serverBaseID]
	if ok {
		panic("what? can't have more than one server for same baseID!")
	} else {
		// expected
		s.servers[reg.serverBaseID] = srvnode
		basesrv = srvnode
	}
	// our auto-cli might have raced and got here first?
	// scan for any we should have
	//for clinode := range s.allnodes {
	for clinode := range s.orphans {
		if clinode.isCli && clinode.serverBaseID == reg.serverBaseID {
			c2s := clinode.cliConn
			// do the same as client registration would have
			// if it could have earlier.
			s.node2server[clinode] = basesrv
			basesrv.autocli[clinode] = c2s
			basesrv.allnode[clinode] = true
			delete(s.orphans, clinode)
		}
	}

	s.allnodes[srvnode] = true

	reg.simnode = srvnode
	reg.simnet = s

	//vv("end of handleServerRegistration, srvreg is %v", reg)

	// channel made by newCircuitserver() above.
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

	clinode := s.newSimnodeClient(reg.client.name, reg.serverBaseID)
	clinode.setNetAddrSameNetAs(reg.localHostPortStr, srvnode.netAddr)
	s.allnodes[clinode] = true

	_, already := s.dns[clinode.name]
	if already {
		panic(fmt.Sprintf("client name already taken: '%v'", clinode.name))
	}
	s.dns[clinode.name] = clinode

	// add simnode to graph
	clientOutboundEdges := make(map[*simnode]*simconn)
	s.circuits[clinode] = clientOutboundEdges

	// add both direction edges
	c2s := s.addEdgeFromCli(clinode, srvnode)
	s2c := s.addEdgeFromSrv(srvnode, clinode)

	// let server reconstruct its autocli if it comes late
	clinode.cliConn = c2s

	if reg.serverBaseID != "" {
		basesrv, ok := s.servers[reg.serverBaseID]
		if ok {
			s.node2server[clinode] = basesrv
			basesrv.autocli[clinode] = c2s
			basesrv.allnode[clinode] = true
		} else {
			s.orphans[clinode] = true
		}
	}

	reg.conn = c2s
	reg.simnode = clinode

	// tell server about new edge
	// //vv("about to deadlock? stack=\n'%v'", stack())
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
func (cfg *Config) bootSimNetOnServer(simNetConfig *SimNetConfig, srv *Server) *simnet { // (tellServerNewConnCh chan *simconn) {

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
	scen := NewScenario(tick, minHop, maxHop, seed)

	// server creates simnet; must start server first.
	s := &simnet{
		barrier:        !simNetConfig.BarrierOff,
		cfg:            cfg,
		simNetCfg:      simNetConfig,
		srv:            srv,
		halt:           srv.halt,
		cliRegisterCh:  make(chan *clientRegistration),
		srvRegisterCh:  make(chan *serverRegistration),
		alterSimnodeCh: make(chan *simnodeAlteration),
		alterHostCh:    make(chan *simnodeAlteration),
		msgSendCh:      make(chan *mop),
		msgReadCh:      make(chan *mop),
		addTimer:       make(chan *mop),
		discardTimerCh: make(chan *mop),
		newScenarioCh:  make(chan *scenario),

		injectCircuitFaultCh: make(chan *circuitFault),
		injectHostFaultCh:    make(chan *hostFault),
		repairCircuitCh:      make(chan *circuitRepair),
		repairHostCh:         make(chan *hostRepair),

		scenario:          scen,
		safeStateStringCh: make(chan *simnetSafeStateQuery),
		dns:               make(map[string]*simnode),
		node2server:       make(map[*simnode]*simnode),

		// graph of circuits, edges are circuits[from][to]
		circuits: make(map[*simnode]map[*simnode]*simconn),

		// just the servers/peers, not their autocli.
		// use locals() to self + all autocli on peer.
		servers:  make(map[string]*simnode),
		allnodes: make(map[*simnode]bool),
		orphans:  make(map[*simnode]bool), // cli without baseserver

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

func (s *simnet) addEdgeFromSrv(srvnode, clinode *simnode) *simconn {

	srv, ok := s.circuits[srvnode] // edges from srv to clients
	if !ok {
		srv = make(map[*simnode]*simconn)
		s.circuits[srvnode] = srv
	}
	s2c := newSimconn()
	s2c.isCli = false
	s2c.net = s
	s2c.local = srvnode
	s2c.remote = clinode
	s2c.netAddr = srvnode.netAddr

	// replace any previous conn
	srv[clinode] = s2c
	return s2c
}

func (s *simnet) addEdgeFromCli(clinode, srvnode *simnode) *simconn {

	cli, ok := s.circuits[clinode] // edge from client to one server
	if !ok {
		cli = make(map[*simnode]*simconn)
		s.circuits[clinode] = cli
	}
	c2s := newSimconn()
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

func enforceTickDur(tick time.Duration) time.Duration {
	return tick.Truncate(timeMask0)
}

type pq struct {
	owner   string
	orderby string
	tree    *rb.Tree
}

func (s *pq) peek() *mop {
	n := s.tree.Len()
	if n == 0 {
		return nil
	}
	it := s.tree.Min()
	if it == s.tree.Limit() {
		panic("n > 0 above, how is this possible?")
		return nil
	}
	return it.Item().(*mop)
}

func (s *pq) pop() *mop {
	n := s.tree.Len()
	//vv("pop n = %v", n)
	if n == 0 {
		return nil
	}
	it := s.tree.Min()
	if it == s.tree.Limit() {
		panic("n > 0 above, how is this possible?")
		return nil
	}
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

func (s *simnet) shutdownSimnode(simnode *simnode) {
	//vv("handleAlterCircuit: SHUTDOWN %v, going to powerOff true, in state %v", simnode.name, simnode.state)
	simnode.powerOff = true
	simnode.readQ.deleteAll()
	simnode.preArrQ.deleteAll()
	simnode.timerQ.deleteAll()
	//vv("handleAlterCircuit: end SHUTDOWN, simnode is now: %v", simnode)
}

func (s *simnet) restartSimnode(simnode *simnode) {
	//vv("handleAlterCircuit: RESTART %v, wiping queues, going %v -> HEALTHY", simnode.state, simnode.name)
	simnode.powerOff = false
	simnode.readQ.deleteAll()
	simnode.preArrQ.deleteAll()
	simnode.timerQ.deleteAll()
}

func (s *simnet) isolateSimnode(simnode *simnode) {
	//vv("handleAlterCircuit: from %v -> ISOLATED %v, wiping pre-arrival, block any future pre-arrivals", simnode.state, simnode.name)
	switch simnode.state {
	case ISOLATED, FAULTY_ISOLATED:
		// already there
	case FAULTY:
		simnode.state = FAULTY_ISOLATED
	case HEALTHY:
		simnode.state = ISOLATED
	}

	simnode.preArrQ.deleteAll()
}
func (s *simnet) unIsolateSimnode(simnode *simnode) {
	//vv("handleAlterCircuit: UNISOLATE %v, going from %v -> HEALTHY", simnode.state, simnode.name)
	switch simnode.state {
	case ISOLATED:
		simnode.state = HEALTHY
	case FAULTY_ISOLATED:
		simnode.state = FAULTY
	case FAULTY:
		// not isolated already
	case HEALTHY:
		// not isolated already
	}
}

func (s *simnet) handleAlterCircuit(alt *simnodeAlteration, closeDone bool) {
	simnode := alt.simnode
	switch alt.alter {
	case SHUTDOWN:
		s.shutdownSimnode(simnode)
	case ISOLATE:
		s.isolateSimnode(simnode)
	case UNISOLATE:
		s.unIsolateSimnode(simnode)
	case RESTART:
		s.restartSimnode(simnode)
	}
	if closeDone {
		close(alt.done)
	}
}

// alter all the auto-cli of a server and the server itself.
func (s *simnet) handleAlterHost(alt *simnodeAlteration) {
	const closeDone = false
	for node := range s.locals(alt.simnode) { // includes srvnode itself
		alt.simnode = node
		s.handleAlterCircuit(alt, closeDone)
	}
	close(alt.done)
}

func (s *simnet) localDeafRead(read *mop) bool {

	// get the local (read) origin conn probability of deafness
	// note: not the remote's deafness, only local.
	prob := s.circuits[read.origin][read.target].deafRead
	return s.deaf(prob)
}

func (s *simnet) deaf(prob float64) bool {
	if prob <= 0 {
		return false
	}
	if prob >= 1 {
		return true
	}
	random01 := s.scenario.rng.Float64() // in [0, 1)
	return random01 < prob
}

func (s *simnet) dropped(prob float64) bool {
	if prob <= 0 {
		return false
	}
	if prob >= 1 {
		return true
	}
	random01 := s.scenario.rng.Float64() // in [0, 1)
	return random01 < prob
}

func (s *simnet) localDropSend(send *mop) bool {
	// get the local origin conn probability of drop
	prob := s.circuits[send.origin][send.target].dropSend
	return s.dropped(prob)
}

func (s *simnet) handleSend(send *mop) {
	////zz("top of handleSend(send = '%v')", send)

	if send.origin.powerOff {
		// cannot send when power is off. Hmm.
		// This must be a stray...maybe a race? the
		// simnode really should not be doing anything.
		//alwaysPrintf("yuck: got a SEND from a powerOff simnode: '%v'", send.origin)
		close(send.proceed) // probably just a shutdown race, don't deadlock them.
		return
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

	if send.target.powerOff || send.target.state == ISOLATED {
		//vv("powerOff or ISOLATED, dropping msg = '%v'", send.msg)
	} else {
		if s.localDropSend(send) {
			//vv("DROP SEND %v", send)
			//send.sendIsDropped = true
			//send.isDropDeafFault = true
			send.origin.droppedSendQ.add(send)

			// advance and delete behind? not needed.
			// send has never been added to any pre-arrival Q.
			return
		}
		// make a copy _before_ the sendMessage() call returns,
		// so they can recycle or do whatever without data racing with us.
		// Weird: even with this, the Fragment is getting
		// races, not the Message.
		msg1 := send.msg // copy not needed now? newSendMop() now does: .CopyForSimNetSend()

		//msg1 := send.msg.CopyForSimNetSend() // race read vs srv.go:517
		// how is a race possible? we have not closed the proceed chan yet!?!
		// ah: maybe the send was non-blocking. do the copy earlier
		// on the sender side in sendMessage() during newSendMop().

		// split into two parts to try and understand the shutdown data race here.
		// we've got to try and have shutdown not read send.msg
		send.msg = msg1

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

	if read.origin.powerOff {
		// cannot read when off. Hmm.
		// This must be a stray...maybe a race? the
		// simnode really should not be doing anything.
		//alwaysPrintf("yuck: got a READ from a powerOff simnode: '%v'", read.origin)
		close(read.proceed) // probably just a shutdown race, don't deadlock them.
		return
	}

	origin := read.origin
	if read.seen == 0 {
		read.originLC = origin.lc
	}
	read.seen++
	if read.seen != 1 {
		panic(fmt.Sprintf("should see each send only once now, not %v", read.seen))
	}

	if s.localDeafRead(read) {
		//vv("DEAF READ %v", read)
		origin.deafReadQ.add(read)
	} else {
		origin.readQ.add(read)
	}
	//vv("LC:%v  READ at %v: %v", origin.lc, origin.name, read)
	////zz("LC:%v  READ %v at %v, now cliReadQ: '%v'", origin.lc, origin.name, read, origin.readQ)
	now := time.Now()
	origin.dispatch(now)

}

func (simnode *simnode) firstPreArrivalTimeLTE(now time.Time) bool {

	preIt := simnode.preArrQ.tree.Min()
	if preIt == simnode.preArrQ.tree.Limit() {
		return false // empty queue
	}
	send := preIt.Item().(*mop)
	return !send.arrivalTm.After(now)
}

func (simnode *simnode) optionallyApplyChaos() {

	// based on simnode.net.scenario
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
func (simnode *simnode) dispatchTimers(now time.Time) (changes int64) {
	if simnode.powerOff {
		return 0
	}
	if simnode.timerQ.tree.Len() == 0 {
		return
	}

	timerIt := simnode.timerQ.tree.Min()
	for timerIt != simnode.timerQ.tree.Limit() { // advance, and delete behind below

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
			simnode.timerQ.tree.DeleteWithIterator(delmeIt)

			select {
			case timer.timerC <- now:
			case <-simnode.net.halt.ReqStop.Chan:
				return
			default:
				// The Go runtime will delay the timer channel
				// send until a receiver goro can receive it,
				// but we cannot. Hence we use a goroutine if
				// we didn't get through on the above attempt.
				// TODO: maybe time.AfterFunc could help here to
				// avoid a goro?
				atomic.AddInt64(&simnode.net.backgoundTimerGoroCount, 1)
				go simnode.backgroundFireTimer(timer, now)
			}
		} else {
			// INVAR: smallest timer > now
			return
		}
	}
	return
}

func (simnode *simnode) backgroundFireTimer(timer *mop, now time.Time) {
	select {
	case timer.timerC <- now:
	case <-simnode.net.halt.ReqStop.Chan:
	}
	atomic.AddInt64(&simnode.net.backgoundTimerGoroCount, -1)
}

// does not call armTimer.
func (simnode *simnode) dispatchReadsSends(now time.Time) (changes int64) {

	if simnode.powerOff {
		return
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
			found := simnode.preArrQ.tree.DeleteWithKey(op)
			if !found {
				panic(fmt.Sprintf("failed to delete from preArrQ: '%v'; preArrQ = '%v'", op, simnode.preArrQ.String()))
			}
			delListSN = append(delListSN, op.sn)
		}
		for _, op := range readDel {
			////zz("delete '%v'", op)
			found := simnode.readQ.tree.DeleteWithKey(op)
			if !found {
				panic(fmt.Sprintf("failed to delete from readQ: '%v'", op))
			}
		}
		//vv("=== end of dispatch %v", simnode.name)

		if true { // was off for deafDrop development, separate queues now back on.
			// sanity check that we delivered everything we could.
			narr := simnode.preArrQ.tree.Len()
			nread := simnode.readQ.tree.Len()
			// it is normal to have preArrQ if no reads...
			if narr > 0 && nread > 0 {
				// if the first preArr is not due yet, that is the reason

				// if not using fake time, arrival time was probably
				// almost but not quite here when we checked below,
				// but now there is something possible a
				// few microseconds later. In this case, don't freak.
				// So make this conditional on faketime being in use.
				if !shuttingDown && faketime { // && simnode.net.barrier {

					now2 := time.Now()
					if simnode.firstPreArrivalTimeLTE(now2) {
						alwaysPrintf("ummm... why did these not get dispatched? narr = %v, nread = %v; nPreDel = %v; delListSN = '%v', (now2 - now = %v);\n endOn = %v\n lastMatchSend=%v \n lastMatchRead = %v \n\n endOnSituation = %v\n summary simnode summary:\n%v", narr, nread, nPreDel, delListSN, now2.Sub(now), endOn, lastMatchSend, lastMatchRead, endOnSituation, simnode.String())
						panic("should have been dispatchable, no?")
					}
				}
			}
		}
	}()

	nR := simnode.readQ.tree.Len()   // number of reads
	nS := simnode.preArrQ.tree.Len() // number of sends

	if nR == 0 && nS == 0 {
		return
	}

	readIt := simnode.readQ.tree.Min()
	preIt := simnode.preArrQ.tree.Min()

	// matching reads and sends
	for {
		if readIt == simnode.readQ.tree.Limit() {
			// no reads, no point.
			return
		}
		if preIt == simnode.preArrQ.tree.Limit() {
			// no sends to match with reads
			return
		}

		read := readIt.Item().(*mop)
		send := preIt.Item().(*mop)

		simnode.optionallyApplyChaos()

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
			//vv("dispatch: %v", simnode.net.schedulerReport())
			endOn = send
			it2 := preIt
			afterN := 0
			for ; it2 != simnode.preArrQ.tree.Limit(); it2 = it2.Next() {
				afterN++
			}
			endOnSituation = fmt.Sprintf("after me: %v; preArrQ: %v", afterN, simnode.preArrQ.String())

			// we must set a timer on its delivery then...
			dur := send.arrivalTm.Sub(now)
			pending := newTimerCreateMop(simnode.isCli)
			pending.origin = simnode
			pending.timerDur = dur
			pending.initTm = now
			pending.completeTm = now.Add(dur)
			pending.timerFileLine = fileLine(1)
			pending.internalPendingTimer = true
			simnode.net.handleTimer(pending)
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
		simnode.lc = max(simnode.lc, send.originLC) + 1
		////zz("servicing cli read: started LC %v -> serviced %v (waited: %v) read.sn=%v", read.originLC, simnode.lc, simnode.lc-read.originLC, read.sn)

		// track clocks on either end for this send and read.
		read.readerLC = simnode.lc
		read.senderLC = send.senderLC
		send.readerLC = simnode.lc
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
// It calls simnode.net.armTimer() at the end (in the defer).
func (simnode *simnode) dispatch(now time.Time) (changes int64) {

	changes += simnode.dispatchTimers(now)
	changes += simnode.dispatchReadsSends(now)
	return
}

func (s *simnet) qReport() (r string) {
	i := 0
	for simnode := range s.circuits {
		r += fmt.Sprintf("\n[simnode %v of %v in qReport]: \n", i+1, len(s.circuits))
		r += simnode.String() + "\n"
		i++
	}
	return
}

func (s *simnet) schedulerReport() string {
	now := time.Now()
	return fmt.Sprintf("lastArmTm.After(now) = %v [%v out] %v; qReport = '%v'", s.lastArmTm.After(now), s.lastArmTm.Sub(now), s.lastArmTm, s.qReport())
}

func (s *simnet) dispatchAll(now time.Time) (changes int64) {
	// notice here we only use the key of s.circuits
	for simnode := range s.circuits {
		changes += simnode.dispatch(now)
	}
	return
}

// does not call armTimer(), so scheduler should afterwards.
func (s *simnet) dispatchAllTimers(now time.Time) (changes int64) {
	for simnode := range s.circuits {
		changes += simnode.dispatchTimers(now)
	}
	return
}

// does not call armTimer(), so scheduler should afterwards.
func (s *simnet) dispatchAllReadsSends(now time.Time) (changes int64) {
	for simnode := range s.circuits {
		changes += simnode.dispatchReadsSends(now)
	}
	return
}

func (s *simnet) tickLogicalClocks() {
	for simnode := range s.circuits {
		simnode.lc++
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
// have started simnode goroutines, so they
// are running and may now be trying
// to do network operations. The
// select below will service those
// operations, or take a time step
// if nextTimer goes off first. Since
// synctest ONLY advances time when
// all goro are blocked, nextTimer
// will go off last, once all other simnode
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
		// event/logical clock of each simnode, as no races.
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

		case alt := <-s.alterSimnodeCh:
			s.handleAlterCircuit(alt, true)

		case alt := <-s.alterHostCh:
			s.handleAlterHost(alt)

		case cktFault := <-s.injectCircuitFaultCh:
			//vv("i=%v injectCircuitFaultCh ->  dd='%v'", i, cktFault)
			s.injectCircuitFault(cktFault, true)

		case hostFault := <-s.injectHostFaultCh:
			//vv("i=%v injectHostFaultCh ->  dd='%v'", i, cktFault)
			s.injectHostFault(hostFault)

		case repairCkt := <-s.repairCircuitCh:
			//vv("i=%v repairCircuitCh ->  repairCkt='%v'", i, repairCkt)
			s.handleCircuitRepair(repairCkt, true)

		case repairHost := <-s.repairHostCh:
			//vv("i=%v repairHostCh ->  repairHost='%v'", i, repairHost)
			s.handleHostRepair(repairHost)

		case safe := <-s.safeStateStringCh:
			// prints internal state to string, without data races.
			s.handleSafeStateString(safe)

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

	if discard.origin.powerOff {
		// cannot set/fire timers when halted. Hmm.
		// This must be a stray...maybe a race? the
		// simnode really should not be doing anything.
		//alwaysPrintf("yuck: got a TIMER_DISCARD from a powerOff simnode: '%v'", discard.origin)
		close(discard.proceed) // probably just a shutdown race, don't deadlock them.
		return
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

	if timer.origin.powerOff {
		// cannot start timers when halted. Hmm.
		// This must be a stray...maybe a race? the
		// simnode really should not be doing anything.
		// This does happen though, e.g. in test
		// Test010_tube_write_new_value_two_replicas,
		// so don't freak. Probably just a shutdown race.
		//
		// Very very common at test shutdown, so we comment.
		//alwaysPrintf("yuck: got a timer from a powerOff simnode: '%v'", timer.origin)
		close(timer.proceed) // likely shutdown race, don't deadlock them.
		return
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
// a lookup of the min simnode in
// the each priority queue, which our
// red-black tree has cached anyway.
// For K circuits * 3 PQ per simnode => O(K).
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
	for simnode := range s.circuits {
		minTimer = simnode.soonestTimerLessThan(minTimer)
	}
	if minTimer == nil {
		panic("should never happen, s.gridStepTimer should always be active")
		return 0
	}

	dur := minTimer.completeTm.Sub(now)
	////zz("dur=%v = when(%v) - now(%v)", dur, minTimer.completeTm, now)
	if dur <= 0 {
		//vv("no timers, what?? minTimerDur = %v", dur)
		panic("must always have at least the grid timer!")
	}
	s.lastArmTm = now
	s.nextTimer.Reset(dur)
	return dur
}

func (simnode *simnode) soonestTimerLessThan(bound *mop) *mop {

	//if bound != nil {
	//vv("soonestTimerLessThan(bound.completeTm = '%v'", bound.completeTm)
	//} else {
	//vv("soonestTimerLessThan(nil bound)")
	//}
	it := simnode.timerQ.tree.Min()
	if it == simnode.timerQ.tree.Limit() {
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

type byName []*simnode

func (s byName) Len() int {
	return len(s)
}
func (s byName) Less(i, j int) bool {
	return s[i].name < s[j].name
}
func (s byName) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// input s.servers is map[string]*simnode
func valNameSort[K comparable](m map[K]*simnode) byName {
	var nodes []*simnode
	for _, srvnode := range m {
		nodes = append(nodes, srvnode)
	}
	sort.Sort(byName(nodes))
	return nodes
}

// input s.locals(srvnode) is map[*simnode]bool
func keyNameSort[V any](m map[*simnode]V) byName {
	var nodes []*simnode
	for srvnode := range m {
		nodes = append(nodes, srvnode)
	}
	sort.Sort(byName(nodes))
	return nodes
}

func (s *simnet) allConnString() (r string) {

	i := 0
	for _, srvnode := range valNameSort(s.servers) {
		r += fmt.Sprintf("srvnode [%v] has locals:\n", srvnode.name)

		for _, node := range keyNameSort(s.locals(srvnode)) {

			r += fmt.Sprintf("   [localServer:'%v'] [%02d] %v  \n",
				s.localServer(node).name, i, node.name)
			i++
		}
	}
	return
}

func (s *simnet) handleSafeStateString(safe *simnetSafeStateQuery) {
	safe.str = s.String() + "\n" + s.allConnString()
	close(safe.proceed)
}
