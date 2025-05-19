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
	// in a synctest.Wait bubble (using synctest.Run
	// or synctest.Test, the upcomming rename).
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
	// timers in rpc25519 tests, and so (unless we missed
	// a real time.Sleep which we tried to purge)
	// only the scheduler calls time.Sleep.
	// However future tests and user code
	// might call Sleep... we want to use
	// cli/srv.U.Sleep() instead for more
	// deterministic, repeatable tests whenever possible.
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
	origin *simckt
	target *simckt

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

	// on sends/reads
	// model network/card faults, nil means no change.
	sendIsDropped   bool
	readIsDeaf      bool
	isDropDeafFault bool // either sendIsDropped or readIsDeaf

	// clients of scheduler wait on proceed.
	// When the timer is set; the send sent; the read
	// matches a send, the client proceeds because
	// this channel will be closed.
	proceed chan struct{}

	isEOF_RST bool
}

func (s *simnet) handleRepairCircuit(repair *repair, closeProceed bool) (err error) {

	if repair.allHealthy && repair.justOriginHealed {
		panic("confused caller: only set one of repair.allHealthy and repair.justOriginHealed")
	}
	if closeProceed {
		defer func() {
			repair.err = err
			close(repair.proceed)
		}()
	}

	if repair.justOriginHealed {
		simckt, ok := s.dns[repair.originName]
		if !ok {
			return fmt.Errorf("error on justOriginHealed: originName not found: '%v'", repair.originName)
		}
		s.repairAllFaults(simckt)
		if repair.unIsolate {
			simckt.state = HEALTHY
		}
		if repair.justOriginPowerOn {
			simckt.powerOff = false
		}
		return
	}

	for simckt := range s.circuits {
		vv("handleRepairCircuit: simckt '%v' goes from %v to HEALTHY", simckt.name, simckt.state)
		simckt.state = HEALTHY
		s.repairAllFaults(simckt)
		if repair.powerOnAnyOff {
			simckt.powerOff = false
		}
	}
	return
}

// This is a central place to handle repairs to a circuit;
// undoing all deaf/drop faults on a single circuit.
//
// We are called by handleRepairCircuit and recheckHealthState.
// state can be ISOLATED or HEALTHY, we do not change these.
// If state is FAULTY, we go to HEALTHY.
// If state is FAULTY_ISOLATED, we go to ISOLATED.
func (s *simnet) repairAllCircuitFaults(simckt *simckt) {

	switch simckt.state {
	case HEALTHY:
		// fine.
	case ISOLATED:
		// fine.
	case FAULTY:
		simckt.state = HEALTHY
	case FAULTY_ISOLATED:
		simckt.state = ISOLATED
	}

	// might want to keep these for testing? for
	// now let's observe that the allHealthy request worked.
	simckt.deafReadsQ.deleteAll()
	simckt.droppedSendsQ.deleteAll()

	for rem, conn := range s.circuits[simckt] {
		_ = rem
		//vv("repairAllCircuitFaults: before 0 out deafRead and deafSend, conn=%v", conn)
		conn.deafRead = 0 // zero prob of deaf read.
		conn.dropSend = 0 // zero prob of dropped send.
		//vv("repairAllCircuitFaults: after deafRead=0 and deafSend=0, conn=%v", conn)
	}
}

func (s *simnet) handleRepairHost(repair *repair) (err error) {
	if !repair.allHealthy {
		panic("why call here in not a healing request?")
	}
	if repair.justOriginHealed {
		panic("confused caller: repair.justOriginHealed makes no sense in handleRepairHost")
	}
	defer func() {
		repair.err = err
		close(repair.proceed)
	}()

	origin, ok := s.dns[repair.originName]
	if !ok {
		panic(fmt.Sprintf("not avail in dns repair.origName = '%v'", repair.originName))
	}
	host, ok := s.simckt2host[origin]
	if !ok {
		panic(fmt.Sprintf("origin not registered in s.simckt2host: origin.name = '%v'", origin.name))
	}
	for end := range host.port2host {
		repair.originName = end.name
		s.handleRepairCircuit(repair, false)
	}
	host.state = HEALTHY
	if repair.powerOnAnyOff {
		host.powerOff = false
	}
	return
}

func (s *simnet) injectFaultWholeHost(fault *fault) (err error) {

	defer func() {
		fault.err = err
		close(fault.proceed)
	}()

	origin, ok := s.dns[fault.originName]
	if !ok {
		panic(fmt.Sprintf("not avail in dns fault.origName = '%v'", fault.originName))
	}
	host, ok := s.simckt2host[origin]
	if !ok {
		panic(fmt.Sprintf("not registered in s.simckt2host: origin.name = '%v'", origin.name))
	}
	for end := range host.port2host {
		fault.originName = end.name
		s.injectFault(fault, false)
	}
	return
}

func (s *simnet) injectFault(fault *fault, closeProceed bool) (err error) {

	if closeProceed {
		defer func() {
			fault.err = err
			close(fault.proceed)
		}()
	}

	origin, ok := s.dns[fault.originName]
	_ = origin
	if !ok {
		err = fmt.Errorf("could not find originName = '%v' in dns: '%v'", fault.originName, s.stringDNS())
		return
	}
	var target *simckt
	if fault.targetName != "" {
		target, ok = s.dns[fault.targetName]
		if !ok {
			err = fmt.Errorf("could not find targetName = '%v' in dns: '%v'", fault.targetName, s.stringDNS())
			return
		}
	}
	remotes, ok := s.circuits[origin]
	if !ok {
		// no remote conn to adjust
		return
	}
	recheckHealth := false
	addedFault := false
	for rem, conn := range remotes {
		if target == nil || target == rem {
			if fault.updateDeafReads {
				//vv("setting conn(%v).deafRead = fault.deafReadsNewProb = %v", conn, fault.deafReadsNewProb)
				conn.deafRead = fault.deafReadsNewProb
				if conn.deafRead > 0 {
					origin.state = FAULTY
					addedFault = true
				} else {
					recheckHealth = true
				}
			}
			if fault.updateDropSends {
				//vv("setting conn(%v).dropSend = fault.dropSendsNewProb = %v", conn, fault.dropSendsNewProb)
				conn.dropSend = fault.dropSendsNewProb
				if conn.dropSend > 0 {
					origin.state = FAULTY
					addedFault = true
				} else {
					recheckHealth = true
				}
			}
		}
	}
	if !addedFault && recheckHealth {
		// simckt may be healthy now, if faults are all gone.
		// but, an early fault may still be installed; full scan needed.
		s.recheckHealthState(origin)
	}
	return
}

func (s *simnet) recheckHealthState(simckt *simckt) {
	remotes, ok := s.circuits[simckt]
	if !ok {
		return
	}
	for rem, conn := range remotes {
		_ = rem
		if conn.deafRead > 0 {
			return // not healthy
		}
		if conn.dropSend > 0 {
			return // not healthy
		}
	}
	s.repairAllCircuitFaults(simckt)
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

	hosts       map[string]*simhost // key is serverBaseID
	simckt2host map[*simckt]*simhost

	dns map[string]*simckt

	// circuits[A][B] is the very cyclic (bi-directed?) graph
	// of the network.
	//
	// The simckt.name is the key of the circuits map.
	// Both client and server names are keys in circuits.
	//
	// Each A has a bi-directional network "socket" to each of circuits[A].
	//
	// circuits[A][B] is A's connection to B; that owned by A.
	//
	// circuits[B][A] is B's connection to A; that owned by B.
	//
	// The system guarantees that the keys of circuits
	// (the names of all endpoints) are unique strings,
	// by rejecting any already taken names (panic-ing on attempted
	// registration) during the moral equivalent of
	// server Bind/Listen and client Dial. The go map
	// is not a multi-map anyway, but that is just
	// an implementation detail that happens to provide extra
	// enforcement. Even if we change the map out
	// later, each endpoint name in the network must be unique.
	//
	// Users specify their own names for modeling
	// convenience, but assist them by enforcing
	// global name uniqueness.
	//
	// See handleServerRegistration() and
	// handleClientRegistration() where these
	// panics enforce uniqueness.
	//
	// Technically, if the simckt is backed by
	// rpc25519, each simckt has both a rpc25519.Server
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
	// simckt is on a Client or Server when
	// it matters, but we try to minimize its use.
	// It should not matter for the most part; in
	// the ckt.go code there is even a note that
	// the peerAPI.isCli can be misleading with auto-Clients
	// in play. The simckt.isCli however should be
	// accurate (we think).
	//
	// Each peer-to-peer connection is a network
	// endpoint that can send and read messages
	// to exactly one other network endpoint.
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
	// To recap, both clisimckt.name and srvsimckt.name are keys
	// in the circuits map. So circuits[clisimckt.name] returns
	// the map of who clisimckt is connected to.
	//
	// In other words, the value of the map circuits[A]
	// is another map, which is the set of circuits that A
	// is connected to by the simconn circuits[A][B].
	circuits map[*simckt]map[*simckt]*simconn

	cliRegisterCh chan *clientRegistration
	srvRegisterCh chan *serverRegistration

	alterSimcktCh chan *simcktAlteration
	alterHostCh   chan *simcktAlteration

	// same as srv.halt; we don't need
	// our own, at least for now.
	halt *idem.Halter

	msgSendCh      chan *mop
	msgReadCh      chan *mop
	addTimer       chan *mop
	discardTimerCh chan *mop

	injectCircuitFaultCh chan *faultCircuit
	injectHostFaultCh    chan *faultHost
	repairCircuitCh      chan *repairCircuit
	repairHostCh         chan *repairHost

	safeStateStringCh chan *simnetSafeStateQuery

	newScenarioCh chan *scenario
	nextTimer     *time.Timer
	lastArmTm     time.Time
}

// update: really at the moment it may
// be that simckt is a single
// network endpoint, or net.Conn, on either
// a client or server; like a host:port pair.
//
// For instance, in a three host cluster, each of the three
// servers needs a local net.Conn to talk
// to two other servers. Thus there are
// 6 total network endpoints at minimum
// in a 3 server cluster. We are registering
// the auto-clients currently as circuits
// as well... maybe we only want/need to
// register the servers? but we need to
// manage and supervise all the network
// end points at each step. So maybe
// TODO: rename simckt to something
// clearer? but distinct from simconn
// which is our net.Conn implementation...?
//
// for applying isolation how do I know which
// simckt are on the same host? serverBaseID
type simckt struct {
	name    string
	lc      int64 // logical clock
	readQ   *pq
	preArrQ *pq
	timerQ  *pq

	deafReadsQ    *pq
	droppedSendsQ *pq

	net          *simnet
	isCli        bool
	netAddr      *SimNetAddr
	serverBaseID string

	// state survives power cycling, i.e. rebooting
	// a simckt does not heal the network or repair a
	// faulty network card.
	state    Circuitstate
	powerOff bool

	tellServerNewConnCh chan *simconn
}

// Circuitstate is one of HEALTHY, FAULTY,
// ISOLATED, or FAULTY_ISOLATED.
//
// FAULTY models network card problems. These
// can be either dropped sends or deaf reads.
// The probability of each can be set independently.
// A card might be able to send messages but
// only read incoming messages half the time,
// for example; an asymmetric and intermittent failure mode.
//
// ISOLATED models a dead network switch.
// While this can be modelled more orthogonally
// as a collection of individual card faults on either side
// of switch (and may be implemented
// as such internally) it is a common enough
// failure mode and test scenario that giving
// it a distinct name enhances the API's usability and
// clarifies the scenario being simulated.
//
// FAULTY_ISOLATED models network card problems
// alongside network switch problem.
//
// HEALTHY means the network and card are
// fully operational with respect to this circuit.
// This does not imply that other circuits
// ending at the same host, or between the same
// pair of hosts, are healthy too.
// A simhost server will typically host
// many circuit endpoints; at least one per
// connected peer server.
//
// A circuit's powerOff status is independent
// of its Circuitstate, so that circuit
// faults like flakey network cards and
// network isolatation (dead switches) survive
// (are not repaired by) a simple host or circuit reboot.
//
// We reuse Circuitstate for the whole host state,
// to keep things simple and to summarize
// the status of all circuit endpoints at a host.
type Circuitstate int

const (
	HEALTHY Circuitstate = 0

	ISOLATED Circuitstate = 1 // cruder than FAULTY. no comms with anyone else

	// If a (deaf/drop) fault is applied to a HEALTHY circuit,
	// then the circuit is marked FAULTY.
	// If a repair removes the last fault, we change it back to HEALTHY.
	FAULTY Circuitstate = 2 // some conn may drop sends, be deaf to reads

	// If a (deaf/drop) fault is applied to an ISOLATED circuit,
	// then the circuit is marked FAULTY_ISOLATED.
	// if a reapir removes the last fault, we change it back to ISOLATED.
	FAULTY_ISOLATED Circuitstate = 3
)

func (s *simnet) newSimckt(name, serverBaseID string) *simckt {
	return &simckt{
		name:          name,
		serverBaseID:  serverBaseID,
		readQ:         newPQinitTm(name + " readQ "),
		preArrQ:       s.newPQarrivalTm(name + " preArrQ "),
		timerQ:        newPQcompleteTm(name + " timerQ "),
		deafReadsQ:    newPQinitTm(name + " deaf reads Q "),
		droppedSendsQ: s.newPQarrivalTm(name + " dropped sends Q "),
		net:           s,
	}
}

func (s *simnet) newSimcktClient(name, serverBaseID string) (simckt *simckt) {
	simckt = s.newSimckt(name, serverBaseID)
	simckt.isCli = true
	return
}

func (s *simnet) newCircuitserver(name, serverBaseID string) (simckt *simckt) {
	simckt = s.newSimckt(name, serverBaseID)
	simckt.isCli = false
	// buffer so servers don't have to be up to get them.
	simckt.tellServerNewConnCh = make(chan *simconn, 100)
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

	srvsimckt := s.newCircuitserver(reg.server.name, reg.serverBaseID)
	srvsimckt.netAddr = reg.srvNetAddr
	s.circuits[srvsimckt] = make(map[*simckt]*simconn)
	_, already := s.dns[srvsimckt.name]
	if already {
		panic(fmt.Sprintf("server name already taken: '%v'", srvsimckt.name))
	}
	s.dns[srvsimckt.name] = srvsimckt

	host, ok := s.hosts[reg.serverBaseID]
	if !ok {
		host = newSimhost(reg.server.name, reg.serverBaseID)
		s.hosts[reg.serverBaseID] = host
	}
	s.simckt2host[srvsimckt] = host

	reg.simckt = srvsimckt
	reg.simnet = s

	//vv("end of handleServerRegistration, srvreg is %v", reg)

	// channel made by newCircuitserver() above.
	reg.tellServerNewConnCh = srvsimckt.tellServerNewConnCh
	close(reg.done)
}

func (s *simnet) handleClientRegistration(reg *clientRegistration) {

	srvsimckt, ok := s.dns[reg.dialTo]
	if !ok {
		s.showDNS()
		panic(fmt.Sprintf("cannot find server '%v', requested "+
			"by client registration from '%v'", reg.dialTo, reg.client.name))
	}

	clisimckt := s.newSimcktClient(reg.client.name, reg.serverBaseID)
	clisimckt.setNetAddrSameNetAs(reg.localHostPortStr, srvsimckt.netAddr)

	_, already := s.dns[clisimckt.name]
	if already {
		panic(fmt.Sprintf("client name already taken: '%v'", clisimckt.name))
	}
	s.dns[clisimckt.name] = clisimckt

	// add simckt to graph
	clientOutboundEdges := make(map[*simckt]*simconn)
	s.circuits[clisimckt] = clientOutboundEdges

	// add both direction edges
	c2s := s.addEdgeFromCli(clisimckt, srvsimckt)
	s2c := s.addEdgeFromSrv(srvsimckt, clisimckt)

	srvhost, ok := s.hosts[srvsimckt.serverBaseID]
	if !ok {
		panic(fmt.Sprintf("why no host for server? serverBaseID = '%v'; s.hosts='%#v'", srvsimckt.serverBaseID, s.hosts))
		// happened on server registration:
		//srvhost = newSimhost(srvsimckt.name, srvsimckt.serverBaseID)
		//s.hosts[srvsimckt.serverBaseID] = srvhost
		//s.simckt2host[srvsimckt] = srvhost
	}

	clihost, ok := s.hosts[clisimckt.serverBaseID] // host for the remote
	if !ok {
		clihost = newSimhost(clisimckt.name, clisimckt.serverBaseID)
		s.hosts[clisimckt.serverBaseID] = clihost
	}
	s.simckt2host[clisimckt] = clihost

	srvhost.host2port[clihost] = clisimckt
	srvhost.port2host[clisimckt] = clihost
	srvhost.host2conn[clihost] = s2c

	clihost.host2port[srvhost] = srvsimckt
	clihost.port2host[srvsimckt] = srvhost
	clihost.host2conn[srvhost] = c2s

	reg.conn = c2s
	reg.simckt = clisimckt

	// tell server about new edge
	// vv("about to deadlock? stack=\n'%v'", stack())
	// I think this might be a chicken and egg problem.
	// The server cannot register b/c client is here on
	// the scheduler goro, and client here wants to tell the
	// the server about it... try in goro
	go func() {
		select {
		case srvsimckt.tellServerNewConnCh <- s2c:
			//vv("%v srvsimckt was notified of new client '%v'; s2c='%#v'", srvsimckt.name, clisimckt.name, s2c)

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
		alterSimcktCh:  make(chan *simcktAlteration),
		alterHostCh:    make(chan *simcktAlteration),
		msgSendCh:      make(chan *mop),
		msgReadCh:      make(chan *mop),
		addTimer:       make(chan *mop),
		discardTimerCh: make(chan *mop),
		newScenarioCh:  make(chan *scenario),

		injectCircuitFaultCh: make(chan *circuitFault),
		injectHostFaultCh:    make(chan *hostFault),
		repairCircuitCh:      make(chan *repairCircuit),
		repairHostCh:         make(chan *repairHost),

		scenario:          scen,
		safeStateStringCh: make(chan *simnetSafeStateQuery),
		dns:               make(map[string]*simckt),
		hosts:             make(map[string]*simhost), // key is serverBaseID
		simckt2host:       make(map[*simckt]*simhost),

		// graph of circuits, edges are circuits[from][to]
		circuits: make(map[*simckt]map[*simckt]*simconn),

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

func (s *simckt) setNetAddrSameNetAs(addr string, srvNetAddr *SimNetAddr) {
	s.netAddr = &SimNetAddr{
		network: srvNetAddr.network,
		addr:    addr,
		name:    s.name,
		isCli:   true,
	}
}

func (s *simnet) addEdgeFromSrv(srvsimckt, clisimckt *simckt) *simconn {

	srv, ok := s.circuits[srvsimckt] // edges from srv to clients
	if !ok {
		srv = make(map[*simckt]*simconn)
		s.circuits[srvsimckt] = srv
	}
	s2c := newSimconn()
	s2c.isCli = false
	s2c.net = s
	s2c.local = srvsimckt
	s2c.remote = clisimckt
	s2c.netAddr = srvsimckt.netAddr

	// replace any previous conn
	srv[clisimckt] = s2c
	return s2c
}

func (s *simnet) addEdgeFromCli(clisimckt, srvsimckt *simckt) *simconn {

	cli, ok := s.circuits[clisimckt] // edge from client to one server
	if !ok {
		cli = make(map[*simckt]*simconn)
		s.circuits[clisimckt] = cli
	}
	c2s := newSimconn()
	c2s.isCli = true
	c2s.net = s
	c2s.local = clisimckt
	c2s.remote = srvsimckt
	c2s.netAddr = clisimckt.netAddr

	// replace any previous conn
	cli[srvsimckt] = c2s
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
	seed   [32]byte
	chacha *mathrand2.ChaCha8
	rng    *mathrand2.Rand

	// we enforce ending in 00_000 ns for all tick
	tick   time.Duration
	minHop time.Duration
	maxHop time.Duration
}

func enforceTickDur(tick time.Duration) time.Duration {
	return tick.Truncate(timeMask0)
}

func newScenario(tick, minHop, maxHop time.Duration, seed [32]byte) *scenario {
	s := &scenario{
		seed:   seed,
		chacha: mathrand2.NewChaCha8(seed),
		tick:   enforceTickDur(tick),
		minHop: minHop,
		maxHop: maxHop,
	}
	s.rng = mathrand2.New(s.chacha)
	return s
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

func (s *simnet) shutdownSimckt(simckt *simckt) {
	//vv("handleAlterCircuit: SHUTDOWN %v, going to powerOff true, in state %v", simckt.name, simckt.state)
	simckt.powerOff = true
	simckt.readQ.deleteAll()
	simckt.preArrQ.deleteAll()
	simckt.timerQ.deleteAll()
	//vv("handleAlterCircuit: end SHUTDOWN, simckt is now: %v", simckt)
}

func (s *simnet) restartSimckt(simckt *simckt) {
	//vv("handleAlterCircuit: RESTART %v, wiping queues, going %v -> HEALTHY", simckt.state, simckt.name)
	simckt.powerOff = false
	simckt.readQ.deleteAll()
	simckt.preArrQ.deleteAll()
	simckt.timerQ.deleteAll()
}

func (s *simnet) isolateSimckt(simckt *simckt) {
	//vv("handleAlterCircuit: from %v -> ISOLATED %v, wiping pre-arrival, block any future pre-arrivals", simckt.state, simckt.name)
	switch simckt.state {
	case ISOLATED, FAULTY_ISOLATED:
		// already there
	case FAULTY:
		simckt.state = FAULTY_ISOLATED
	case HEALTHY:
		simckt.state = ISOLATED
	}

	simckt.preArrQ.deleteAll()
}
func (s *simnet) unIsolateSimckt(simckt *simckt) {
	//vv("handleAlterCircuit: UNISOLATE %v, going from %v -> HEALTHY", simckt.state, simckt.name)
	switch simckt.state {
	case ISOLATED:
		simckt.state = HEALTHY
	case FAULTY_ISOLATED:
		simckt.state = FAULTY
	case FAULTY:
		// not isolated already
	case HEALTHY:
		// not isolated already
	}
}

func (s *simnet) handleAlterCircuit(alt *simcktAlteration, closeDone bool) {
	simckt := alt.simckt
	switch alt.alter {
	case SHUTDOWN:
		s.shutdownSimckt(simckt)
	case ISOLATE:
		s.isolateSimckt(simckt)
	case UNISOLATE:
		s.unIsolateSimckt(simckt)
	case RESTART:
		s.restartSimckt(simckt)
	}
	if closeDone {
		close(alt.done)
	}
}

func (s *simnet) handleAlterHost(alt *simcktAlteration) {

	host, ok := s.simckt2host[alt.simckt]
	if !ok {
		panic(fmt.Sprintf("not registered in s.simckt2host: alt.simckt = '%v'", alt.simckt))
	}
	for end := range host.port2host {
		alt.simckt = end
		s.handleAlterCircuit(alt, false)
	}
	switch alt.alter {
	case SHUTDOWN:
		host.powerOff = true
	case ISOLATE:
		switch host.state {
		case HEALTHY:
			host.state = ISOLATED
		case FAULTY:
			host.state = FAULTY_ISOLATED
		case ISOLATED:
			// noop
		case FAULTY_ISOLATED:
			// noop
		}
	case UNISOLATE:
		switch host.state {
		case HEALTHY:
			// no-op, not isolated
		case FAULTY:
			// no-op, not isolated
		case ISOLATED:
			host.state = HEALTHY
		case FAULTY_ISOLATED:
			host.state = FAULTY
		}
	case RESTART:
		host.powerOff = false
	}
	close(alt.done)
}

func (s *simnet) localDeafRead(read *mop) bool {

	// get the local (read) origin conn probability of deafness
	// note: not the remote's deafness, only local.
	prob := s.circuits[read.origin][read.target].deafRead

	if prob == 0 {
		return false
	}
	random01 := s.scenario.rng.Float64() // in [0, 1)
	return random01 < prob
}

func (s *simnet) localDropSend(send *mop) bool {
	// get the local origin conn probability of drop
	prob := s.circuits[send.origin][send.target].dropSend
	if prob == 0 {
		return false
	}
	random01 := s.scenario.rng.Float64() // in [0, 1)
	return random01 < prob
}

func (s *simnet) handleSend(send *mop) {
	////zz("top of handleSend(send = '%v')", send)

	if send.origin.powerOff {
		// cannot send when power is off. Hmm.
		// This must be a stray...maybe a race? the
		// simckt really should not be doing anything.
		//alwaysPrintf("yuck: got a SEND from a powerOff simckt: '%v'", send.origin)
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
			vv("DROP SEND %v", send)
			send.sendIsDropped = true
			send.isDropDeafFault = true
			send.origin.droppedSendsQ.add(send)

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
		////zz("LC:%v  SEND TO %v %v    srvPreArrQ: '%v'", origin.lc, origin.name, send, s.srvsimckt.preArrQ)

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
		// simckt really should not be doing anything.
		//alwaysPrintf("yuck: got a READ from a powerOff simckt: '%v'", read.origin)
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
		vv("DEAF READ %v", read)
		read.readIsDeaf = true
		read.isDropDeafFault = true
	}
	origin.readQ.add(read)
	//vv("LC:%v  READ at %v: %v", origin.lc, origin.name, read)
	////zz("LC:%v  READ %v at %v, now cliReadQ: '%v'", origin.lc, origin.name, read, origin.readQ)
	now := time.Now()
	origin.dispatch(now)

}

func (simckt *simckt) firstPreArrivalTimeLTE(now time.Time) bool {

	preIt := simckt.preArrQ.tree.Min()
	if preIt == simckt.preArrQ.tree.Limit() {
		return false // empty queue
	}
	send := preIt.Item().(*mop)
	return !send.arrivalTm.After(now)
}

func (simckt *simckt) optionallyApplyChaos() {

	// based on simckt.net.scenario
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
func (simckt *simckt) dispatchTimers(now time.Time) (changes int64) {
	if simckt.powerOff {
		return 0
	}
	if simckt.timerQ.tree.Len() == 0 {
		return
	}

	timerIt := simckt.timerQ.tree.Min()
	for timerIt != simckt.timerQ.tree.Limit() { // advance, and delete behind below

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
			simckt.timerQ.tree.DeleteWithIterator(delmeIt)

			select {
			case timer.timerC <- now:
			case <-simckt.net.halt.ReqStop.Chan:
				return
			default:
				// The Go runtime will delay the timer channel
				// send until a receiver goro can receive it,
				// but we cannot. Hence we use a goroutine if
				// we didn't get through on the above attempt.
				// TODO: maybe time.AfterFunc could help here to
				// avoid a goro?
				atomic.AddInt64(&simckt.net.backgoundTimerGoroCount, 1)
				go simckt.backgroundFireTimer(timer, now)
			}
		} else {
			// INVAR: smallest timer > now
			return
		}
	}
	return
}

func (simckt *simckt) backgroundFireTimer(timer *mop, now time.Time) {
	select {
	case timer.timerC <- now:
	case <-simckt.net.halt.ReqStop.Chan:
	}
	atomic.AddInt64(&simckt.net.backgoundTimerGoroCount, -1)
}

// does not call armTimer.
func (simckt *simckt) dispatchReadsSends(now time.Time) (changes int64) {

	if simckt.powerOff {
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
			found := simckt.preArrQ.tree.DeleteWithKey(op)
			if !found {
				panic(fmt.Sprintf("failed to delete from preArrQ: '%v'; preArrQ = '%v'", op, simckt.preArrQ.String()))
			}
			delListSN = append(delListSN, op.sn)
		}
		for _, op := range readDel {
			////zz("delete '%v'", op)
			found := simckt.readQ.tree.DeleteWithKey(op)
			if !found {
				panic(fmt.Sprintf("failed to delete from readQ: '%v'", op))
			}
		}
		//vv("=== end of dispatch %v", simckt.name)

		if true { // was off for deafDrop development, separate queues now back on.
			// sanity check that we delivered everything we could.
			narr := simckt.preArrQ.tree.Len()
			nread := simckt.readQ.tree.Len()
			// it is normal to have preArrQ if no reads...
			if narr > 0 && nread > 0 {
				// if the first preArr is not due yet, that is the reason

				// if not using fake time, arrival time was probably
				// almost but not quite here when we checked below,
				// but now there is something possible a
				// few microseconds later. In this case, don't freak.
				// So make this conditional on faketime being in use.
				if !shuttingDown && faketime { // && simckt.net.barrier {

					now2 := time.Now()
					if simckt.firstPreArrivalTimeLTE(now2) {
						alwaysPrintf("ummm... why did these not get dispatched? narr = %v, nread = %v; nPreDel = %v; delListSN = '%v', (now2 - now = %v);\n endOn = %v\n lastMatchSend=%v \n lastMatchRead = %v \n\n endOnSituation = %v\n summary simckt summary:\n%v", narr, nread, nPreDel, delListSN, now2.Sub(now), endOn, lastMatchSend, lastMatchRead, endOnSituation, simckt.String())
						panic("should have been dispatchable, no?")
					}
				}
			}
		}
	}()

	nR := simckt.readQ.tree.Len()   // number of reads
	nS := simckt.preArrQ.tree.Len() // number of sends

	if nR == 0 && nS == 0 {
		return
	}

	readIt := simckt.readQ.tree.Min()
	preIt := simckt.preArrQ.tree.Min()

	// matching reads and sends
	for {
		if readIt == simckt.readQ.tree.Limit() {
			// no reads, no point.
			return
		}
		if preIt == simckt.preArrQ.tree.Limit() {
			// no sends to match with reads
			return
		}

		read := readIt.Item().(*mop)
		send := preIt.Item().(*mop)

		simckt.optionallyApplyChaos()

		// check readIsDeaf these
		// should not be matched to simulate the fault.
		// We move them into their own queues so their
		// timestamps don't mess up the sort order of live queues.
		if read.readIsDeaf {
			simckt.deafReadsQ.add(read)
			// leave done chan open. a deaf read does not complete.

			// advance and delete behind.
			delmeIt := readIt
			readIt = readIt.Next()
			simckt.readQ.tree.DeleteWithIterator(delmeIt)

			continue
		}

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
			//vv("dispatch: %v", simckt.net.schedulerReport())
			endOn = send
			it2 := preIt
			afterN := 0
			for ; it2 != simckt.preArrQ.tree.Limit(); it2 = it2.Next() {
				afterN++
			}
			endOnSituation = fmt.Sprintf("after me: %v; preArrQ: %v", afterN, simckt.preArrQ.String())

			// we must set a timer on its delivery then...
			dur := send.arrivalTm.Sub(now)
			pending := newTimerCreateMop(simckt.isCli)
			pending.origin = simckt
			pending.timerDur = dur
			pending.initTm = now
			pending.completeTm = now.Add(dur)
			pending.timerFileLine = fileLine(1)
			pending.internalPendingTimer = true
			simckt.net.handleTimer(pending)
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
		simckt.lc = max(simckt.lc, send.originLC) + 1
		////zz("servicing cli read: started LC %v -> serviced %v (waited: %v) read.sn=%v", read.originLC, simckt.lc, simckt.lc-read.originLC, read.sn)

		// track clocks on either end for this send and read.
		read.readerLC = simckt.lc
		read.senderLC = send.senderLC
		send.readerLC = simckt.lc
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
// It calls simckt.net.armTimer() at the end (in the defer).
func (simckt *simckt) dispatch(now time.Time) (changes int64) {

	changes += simckt.dispatchTimers(now)
	changes += simckt.dispatchReadsSends(now)
	return
}

func (s *simnet) qReport() (r string) {
	i := 0
	for simckt := range s.circuits {
		r += fmt.Sprintf("\n[simckt %v of %v in qReport]: \n", i+1, len(s.circuits))
		r += simckt.String() + "\n"
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
	for simckt := range s.circuits {
		changes += simckt.dispatch(now)
	}
	return
}

// does not call armTimer(), so scheduler should afterwards.
func (s *simnet) dispatchAllTimers(now time.Time) (changes int64) {
	for simckt := range s.circuits {
		changes += simckt.dispatchTimers(now)
	}
	return
}

// does not call armTimer(), so scheduler should afterwards.
func (s *simnet) dispatchAllReadsSends(now time.Time) (changes int64) {
	for simckt := range s.circuits {
		changes += simckt.dispatchReadsSends(now)
	}
	return
}

func (s *simnet) tickLogicalClocks() {
	for simckt := range s.circuits {
		simckt.lc++
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
// have started simckt goroutines, so they
// are running and may now be trying
// to do network operations. The
// select below will service those
// operations, or take a time step
// if nextTimer goes off first. Since
// synctest ONLY advances time when
// all goro are blocked, nextTimer
// will go off last, once all other simckt
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
		// event/logical clock of each simckt, as no races.
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

		case alt := <-s.alterSimcktCh:
			s.handleAlterCircuit(alt, true)

		case alt := <-s.alterHostCh:
			s.handleAlterHost(alt)

		case fault := <-s.injectCircuitFaultCh:
			//vv("i=%v injectCircuitFaultCh ->  dd='%v'", i, fault)
			s.injectCircuitFault(fault, true)

		case fault := <-s.injectHostFaultCh:
			//vv("i=%v injectHostFaultCh ->  dd='%v'", i, fault)
			s.injectHostFault(fault)

		case repairCkt := <-s.repairCircuitCh:
			//vv("i=%v repairCircuitCh ->  repairCkt='%v'", i, repairCkt)
			s.handleRepairCircuit(repairCkt, true)

		case repairHost := <-s.repairHostCh:
			//vv("i=%v repairHostCh ->  repairHost='%v'", i, repairHost)
			s.handleRepairHost(repairHost)

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

// simhost collects all the circuits on
// a given host, to make e.g. isolation easier.
// enables applying an action like powerOff
// to all ports on the simulated host.
// the name should correspond to servers,
// not isCli auto-clients.
type simhost struct {
	name         string
	serverBaseID string
	state        Circuitstate
	powerOff     bool
	port2host    map[*simckt]*simhost
	host2port    map[*simhost]*simckt
	host2conn    map[*simhost]*simconn
}

func newSimhost(name, serverBaseID string) *simhost {
	return &simhost{
		name:         name,
		serverBaseID: serverBaseID,
		port2host:    make(map[*simckt]*simhost),
		host2port:    make(map[*simhost]*simckt),
		host2conn:    make(map[*simhost]*simconn),
	}
}

func (s *simnet) allConnString() (r string) {

	for _, host := range s.hosts {
		r += fmt.Sprintf("host:%v has host2conn:\n", host.name)
		for h, conn := range host.host2conn {
			r += fmt.Sprintf("    host2conn[%v] conn.remote.name: %v\n",
				h.name, conn.remote.name)
		}
		if false {
			r += fmt.Sprintf("host:%v has host2port:\n", host.name)
			for h, end := range host.host2port {
				r += fmt.Sprintf("    host2port[%v] = %v\n", h.name, end.name)
			}
			r += fmt.Sprintf("host:%v has port2host:\n", host.name)
			for end, h := range host.port2host {
				r += fmt.Sprintf("    port2host[%v] = %v\n", end.name, h.name)
			}
		}
	}
	return
}

func (s *simnet) handleSafeStateString(safe *simnetSafeStateQuery) {
	safe.str = s.String() + "\n" + s.allConnString()
	close(safe.proceed)
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
		// simckt really should not be doing anything.
		//alwaysPrintf("yuck: got a TIMER_DISCARD from a powerOff simckt: '%v'", discard.origin)
		close(discard.proceed) // probably just a shutdown race, don't deadlock them.
		return
	}

	orig := discard.origTimerMop

	found := discard.origin.timerQ.del(discard.origTimerMop)
	if found {
		discard.wasArmed = !orig.timerFiredTm.IsZero()
		discard.origTimerCompleteTm = orig.completeTm
	} // leave wasArmed false, could not have been armed if gone.

	////zz("LC:%v %v TIMER_DISCARD %v to fire at '%v'; now timerQ: '%v'", discard.origin.lc, discard.origin.name, discard, discard.origTimerCompleteTm, s.clisimckt.timerQ)
	// let scheduler, to avoid false alarms: s.armTimer(now)
	close(discard.proceed)
}

func (s *simnet) handleTimer(timer *mop) {
	//now := time.Now()

	if timer.origin.powerOff {
		// cannot start timers when halted. Hmm.
		// This must be a stray...maybe a race? the
		// simckt really should not be doing anything.
		// This does happen though, e.g. in test
		// Test010_tube_write_new_value_two_replicas,
		// so don't freak. Probably just a shutdown race.
		//
		// Very very common at test shutdown, so we comment.
		//alwaysPrintf("yuck: got a timer from a powerOff simckt: '%v'", timer.origin)
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
	////zz("LC:%v %v set TIMER %v to fire at '%v'; now timerQ: '%v'", lc, timer.origin.name, timer, timer.completeTm, s.clisimckt.timerQ)

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
// a lookup of the min simckt in
// the each priority queue, which our
// red-black tree has cached anyway.
// For K circuits * 3 PQ per simckt => O(K).
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
	for simckt := range s.circuits {
		minTimer = simckt.soonestTimerLessThan(minTimer)
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

func (simckt *simckt) soonestTimerLessThan(bound *mop) *mop {

	//if bound != nil {
	//vv("soonestTimerLessThan(bound.completeTm = '%v'", bound.completeTm)
	//} else {
	//vv("soonestTimerLessThan(nil bound)")
	//}
	it := simckt.timerQ.tree.Min()
	if it == simckt.timerQ.tree.Limit() {
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
// so must not touch s.srvsimckt, s.clisimckt, etc.
func (s *simnet) createNewTimer(origin *simckt, dur time.Duration, begin time.Time, isCli bool) (timer *mop) {

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
		if s.halt.ReqStop.IsClosed() {
			timer = nil
		}
		return
	case <-s.halt.ReqStop.Chan:
		return
	}
	return
}

// readMessage reads a framed message from conn.
func (s *simnet) readMessage(conn uConn) (msg *Message, err error) {

	sc := conn.(*simconn)
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
		// this could be data racey on shutdown. double
		// check we are not shutting down.
		if s.halt.ReqStop.IsClosed() {
			// avoid .msg race on shutdown, CopyForSimNetSend vs sendLoop
			return nil, ErrShutdown()
		}
		msg = read.msg
	case <-s.halt.ReqStop.Chan:
		return nil, ErrShutdown()
	}
	return
}

func (s *simnet) sendMessage(conn uConn, msg *Message, timeout *time.Duration) error {

	sc := conn.(*simconn)
	isCli := sc.isCli

	//vv("top simnet.sendMessage() %v SEND  msg.Serial=%v", send.origin, msg.HDR.Serial)
	//vv("sendMessage\n conn.local = %v (isCli:%v)\n conn.remote = %v (isCli:%v)\n", sc.local.name, sc.local.isCli, sc.remote.name, sc.remote.isCli)
	send := newSendMop(msg, isCli) // clones msg to prevent race with srv.go:517
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
		// this could be data racey on shutdown. double
		// check we are not shutting down.
		if s.halt.ReqStop.IsClosed() {
			// avoid .msg race on shutdown, CopyForSimNetSend vs sendLoop
			return ErrShutdown()
		}
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

// clones msg to prevent race with srv.go:517
func newSendMop(msg *Message, isCli bool) (op *mop) {
	op = &mop{
		originCli: isCli,
		msg:       msg.CopyForSimNetSend(),
		sn:        simnetNextMopSn(),
		kind:      SEND,
		proceed:   make(chan struct{}),
	}
	return
}

func (s *simnet) discardTimer(origin *simckt, origTimerMop *mop, discardTm time.Time) (wasArmed bool) {

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

	// group endpoints on the same server by serverBaseID
	serverBaseID string

	// wait on
	done chan struct{}

	// receive back
	simckt *simckt  // our identity in the simnet (conn.local)
	conn   *simconn // our connection to server (c2s)
}

// external, called by simnet_client.go to
// get a registration ticket to send on simnet.cliRegisterCh
func (s *simnet) newClientRegistration(
	c *Client,
	localHostPort, serverAddr, dialTo, serverBaseID string,
) *clientRegistration {

	return &clientRegistration{
		client:           c,
		localHostPortStr: localHostPort,
		dialTo:           dialTo,
		serverAddrStr:    serverAddr,
		serverBaseID:     serverBaseID,
		done:             make(chan struct{}),
	}
}

type serverRegistration struct {
	// provide
	server       *Server
	srvNetAddr   *SimNetAddr
	serverBaseID string

	// wait on
	done chan struct{}

	// receive back
	simckt              *simckt // our identity in the simnet (conn.local)
	simnet              *simnet
	tellServerNewConnCh chan *simconn
}

// external
func (s *simnet) newServerRegistration(srv *Server, srvNetAddr *SimNetAddr) *serverRegistration {
	return &serverRegistration{
		server:       srv,
		serverBaseID: srv.cfg.serverBaseID,
		srvNetAddr:   srvNetAddr,
		done:         make(chan struct{}),
	}
}

func (s *simnet) registerServer(srv *Server, srvNetAddr *SimNetAddr) (newCliConnCh chan *simconn, err error) {

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
		srv.simckt = reg.simckt
		srv.simnet = reg.simnet
		newCliConnCh = reg.tellServerNewConnCh
		return
	case <-s.halt.ReqStop.Chan:
		err = ErrShutdown()
		return
	}
	return
}

// Alteration flags are used in AlterCircuit() calls
// to specify what change you want to
// a specific network simckt.
type Alteration int // on clients or servers, any simckt

const (
	SHUTDOWN  Alteration = 1
	ISOLATE   Alteration = 2
	UNISOLATE Alteration = 3
	RESTART   Alteration = 4
)

type simcktAlteration struct {
	simnet      *simnet
	simckt      *simckt
	alter       Alteration
	isHostAlter bool
	done        chan struct{}
}

func (s *simnet) newCircuitAlteration(simckt *simckt, alter Alteration, isHostAlter bool) *simcktAlteration {
	return &simcktAlteration{
		simnet:      s,
		simckt:      simckt,
		alter:       alter,
		isHostAlter: isHostAlter,
		done:        make(chan struct{}),
	}
}

func (s *simnet) alterCircuit(simckt *simckt, alter Alteration, wholeHost bool) {
	if wholeHost {
		s.alterHost(simckt, alter)
		return
	}

	alt := s.newCircuitAlteration(simckt, alter, wholeHost)
	select {
	case s.alterSimcktCh <- alt:
		//vv("sent alt on alterSimcktCh; about to wait on done goro = %v", GoroNumber())
	case <-s.halt.ReqStop.Chan:
		return
	}
	select {
	case <-alt.done:
		//vv("server altered: %v", simckt)
	case <-s.halt.ReqStop.Chan:
		return
	}
	return
}

func (s *simnet) alterHost(simckt *simckt, alter Alteration) {

	alt := s.newCircuitAlteration(simckt, alter, true)
	select {
	case s.alterHostCh <- alt:
		//vv("sent alt on alterHostCh; about to wait on done goro = %v", GoroNumber())
	case <-s.halt.ReqStop.Chan:
		return
	}
	select {
	case <-alt.done:
		//vv("host altered: %v", simckt)
	case <-s.halt.ReqStop.Chan:
		return
	}
	return
}

type circuitFault struct {
	originName string
	targetName string // empty string means all targets/ remote conn.
	err        error  // e.g. cannot find name.

	updateDeafReads bool
	updateDropSends bool

	deafReadsNewProb float64
	dropSendsNewProb float64

	sn      int64
	proceed chan struct{}
}

type hostFault struct {
	originName string
	targetName string // empty string means all targets/ remote conn.
	err        error  // e.g. cannot find name.

	updateDeafReads bool
	updateDropSends bool

	deafReadsNewProb float64
	dropSendsNewProb float64

	sn      int64
	proceed chan struct{}
}

type circuitRepair struct {
	originName string
	targetName string // empty string means all targets/ remote conn.
	err        error  // e.g. cannot find name.

	allHealthy        bool
	unIsolate         bool // if true and simckt is isolated, it goes to healthy.
	justOriginPowerOn bool // also power on origin?

	sn      int64
	proceed chan struct{}
}

type hostRepair struct {
	originName string
	targetName string // empty string means all targets/ remote conn.
	err        error  // e.g. cannot find name.

	allHealthy    bool
	powerOnAnyOff bool // for allHealthy, also power on any powerOff circuits?

	justOriginHealed  bool // heal drop/deaf faults on just the originName simckt.
	unIsolate         bool // if true and simckt is isolated, it goes to healthy.
	justOriginPowerOn bool // also power on origin?
	wholeHost         bool

	sn      int64
	proceed chan struct{}
}

// false updateDeaf or false updateDrop means no
// change in that state.
func newCircuitFault(originName, targetName string, updateDeaf, updateDrop bool, deafProb, dropProb float64, wholeHost bool) *circuitFault {

	f := &circuitFault{
		wholeHost:        wholeHost,
		originName:       originName,
		targetName:       targetName,
		updateDeafReads:  updateDeaf,
		deafReadsNewProb: deafProb,
		updateDropSends:  updateDrop,
		dropSendsNewProb: dropProb,

		sn:      simnetNextMopSn(),
		proceed: make(chan struct{}),
	}

	return f
}

func newHostFault(originName, targetName string, updateDeaf, updateDrop bool, deafProb, dropProb float64) *hostFault {

	f := &hostFault{
		originName:       originName,
		targetName:       targetName,
		updateDeafReads:  updateDeaf,
		deafReadsNewProb: deafProb,
		updateDropSends:  updateDrop,
		dropSendsNewProb: dropProb,

		sn:      simnetNextMopSn(),
		proceed: make(chan struct{}),
	}

	return f
}

func (s *simnet) newHostRepair(powerOnAnyOff bool) *hostRepair {
	m := &repair{
		powerOnAnyOff: powerOnAnyOff,
		sn:            simnetNextMopSn(),
		proceed:       make(chan struct{}),
	}
	return m
}

func (s *simnet) newCircuitRepair(originName string, unIsolate, powerOnIfOff bool) *circuitRepair {
	m := &circuitRepair{
		justOriginHealed:  true,
		unIsolate:         unIsolate,
		justOriginPowerOn: powerOnIfOff,
		originName:        originName,
		sn:                simnetNextMopSn(),
		proceed:           make(chan struct{}),
	}
	return m
}

// empty string target means all possible targets
func (s *simnet) CircuitDeafToReads(origin, target string, deafProb float64) (err error) {

	updateDeaf, updateDrop, dropProb := true, false, 0.0
	fault := newCircuitFault(origin, target, updateDeaf, updateDrop, deafProb, dropProb)

	select {
	case s.injectCircuitFaultCh <- fault:
		//vv("sent DeafToReads fault on injectFaultCh; about to wait on proceed")
	case <-s.halt.ReqStop.Chan:
		return
	}
	select {
	case <-fault.proceed:
		err = fault.err
		if target == "" {
			target = "(any and all)"
		}
		//vv("server '%v' DeafToReads from '%v'; err = '%v'", origin, target, err)
	case <-s.halt.ReqStop.Chan:
		return
	}
	return

}

// empty target string means all possible targets
func (s *simnet) CircuitDropSends(origin, target string, dropProb float64) (err error) {

	updateDeaf, updateDrop, deafProb := false, true, 0.0
	fault := newCircuitFault(origin, target, updateDeaf, updateDrop, deafProb, dropProb)

	select {
	case s.injectFaultCh <- fault:
		//vv("sent DropSends fault on injectFaultCh; about to wait on proceed")
	case <-s.halt.ReqStop.Chan:
		return
	}
	select {
	case <-fault.proceed:
		err = fault.err
		if target == "" {
			target = "(any and all)"
		}
		//vv("server '%v' will DropSends to '%v'; err = '%v'", origin, target, err)
	case <-s.halt.ReqStop.Chan:
		return
	}
	return
}

// AllHealthy heal all partitions, undo all faults, network wide.
// All circuits are returned to HEALTHY status. Their powerOff status
// is not updated unless powerOnAnyOff is also true.
// See also RepairSimckt for single simckt repair.
func (s *simnet) AllHealthy(powerOnAnyOff bool) (err error) {

	allGood := s.newAllHealthy(powerOnAnyOff)

	select {
	case s.makeRepairCh <- allGood:
		vv("sent AllHealthy allGood on injectFaultCh; about to wait on proceed")
	case <-s.halt.ReqStop.Chan:
		return
	}
	select {
	case <-allGood.proceed:
		err = allGood.err
		vv("all healthy processed. err = '%v'", err)
	case <-s.halt.ReqStop.Chan:
		return
	}
	return
}

// RepairCircuit restores the local circuit to
// full working order. It undoes the effects of
// prior deafDrop actions, if any. It does not
// change an isolated simckt's isolation unless unIsolate
// is also true. See also AllHealthy.
func (s *simnet) RepairCircuit(originName string, unIsolate bool, powerOnIfOff bool) (err error) {

	oneGood := s.newCircuitRepair(originName, unIsolate, powerOnIfOff)

	select {
	case s.repairCircuitCh <- oneGood:
		vv("RepairCircuit sent oneGood on repairCircuitCh; about to wait on proceed")
	case <-s.halt.ReqStop.Chan:
		return
	}
	select {
	case <-oneGood.proceed:
		err = oneGood.err
		vv("one healthy '%v' processed. err = '%v'", originName, err)
	case <-s.halt.ReqStop.Chan:
		return
	}
	return
}

type simnetSafeStateQuery struct {
	sn      int64
	str     string
	err     error
	proceed chan struct{}
}

// SafeStateString lets clients not race but still view
// the simnet's internal state for diagnostics.
// Calling simnet.String() directly is super data racey; avoid this.
// We would lowercase simnet.String but the standard Go interface
// to fmt.Printf/fmt.Sprintf requires an uppercased String method.
func (s *simnet) SafeStateString() (r string) {

	requestState := &simnetSafeStateQuery{
		sn:      simnetNextMopSn(),
		proceed: make(chan struct{}),
	}
	select {
	case s.safeStateStringCh <- requestState:
		vv("sent AllHealthy requestState on safeStateStringCh; about to wait on proceed")
	case <-s.halt.ReqStop.Chan:
		return
	}
	select {
	case <-requestState.proceed:
		panicOn(requestState.err)
		r = requestState.str
		return
	case <-s.halt.ReqStop.Chan:
		return
	}
	return
}
