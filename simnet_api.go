package rpc25519

import (
	//"fmt"
	mathrand2 "math/rand/v2"
	"runtime"
	"time"
)

//=========================================
// The EXTERNAL client access routines are
// in this file simnet_api.go
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

// scenario will, in the future, provide for testing different
// timeout settings under network partition and
// flakiness (partial failure). Stubbed for now.
type scenario struct {
	num    int
	seed   [32]byte
	chacha *mathrand2.ChaCha8
	rng    *mathrand2.Rand

	// we enforce ending in 00_000 ns for all tick
	tick   time.Duration
	minHop time.Duration
	maxHop time.Duration

	reqtm   time.Time
	proceed chan struct{}
	who     int
}

func NewScenario(tick, minHop, maxHop time.Duration, seed [32]byte) *scenario {
	s := &scenario{
		seed:    seed,
		chacha:  mathrand2.NewChaCha8(seed),
		tick:    enforceTickDur(tick),
		minHop:  minHop,
		maxHop:  maxHop,
		reqtm:   time.Now(),
		proceed: make(chan struct{}),
		who:     int(runtime.GoID()),
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

// Faultstate is one of HEALTHY, FAULTY,
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
// many circuit connections; at least one per
// connected peer server.
//
// A circuit's powerOff status is independent
// of its Faultstate, so that circuit
// faults like flakey network cards and
// network isolatation (dead switches) survive
// (are not repaired by) a simple host or circuit reboot.
//
// We reuse Faultstate for the whole server state,
// to keep things simple and to summarize
// the status of all circuits therein.
// If a simnode or Server is in powerOff, then
// all circuits terminating there are also
// in powerOff.
type Faultstate int

const (
	HEALTHY Faultstate = 0

	ISOLATED Faultstate = 1 // cruder than FAULTY. no comms with anyone else

	// If a (deaf/drop) fault is applied to a HEALTHY circuit,
	// then the circuit is marked FAULTY.
	// If a repair removes the last fault, we change it back to HEALTHY.
	FAULTY Faultstate = 2 // some conn may drop sends, be deaf to reads

	// If a (deaf/drop) fault is applied to an ISOLATED circuit,
	// then the circuit is marked FAULTY_ISOLATED.
	// if a reapir removes the last fault, we change it back to ISOLATED.
	FAULTY_ISOLATED Faultstate = 3
)

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

// SimNetAddr implements net.Addr interface
// needed to implement net.Conn
type SimNetAddr struct {
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

// DropDeafSpec specifies a network/netcard fault
// with a given probability.
type DropDeafSpec struct {

	// false UpdateDeafReads means no change to deafRead
	// probability. The DeafReadsNewProb field is ignored.
	// This allows setting DeafReadsNewProb to 0 only
	// when you want to.
	UpdateDeafReads bool

	// probability of ignoring (being deaf) to a read.
	// 0 => never be deaf to a read (healthy).
	// 1 => ignore all reads (dead hardware).
	DeafReadsNewProb float64

	// false UpdateDropSends means the DropSendsNewProb
	// is ignored, and there is no change to the dropSend
	// probability.
	UpdateDropSends bool

	// probability of dropping a send.
	// 0 => never drop a send (healthy).
	// 1 => always drop a send (dead hardware).
	DropSendsNewProb float64
}

// Alteration flags are used in AlterCircuit() calls
// to specify what change you want to
// a specific network simnode.
type Alteration int // on clients or servers, any simnode

const (
	UNDEFINED Alteration = 0
	SHUTDOWN  Alteration = 1
	POWERON   Alteration = 2
	ISOLATE   Alteration = 3
	UNISOLATE Alteration = 4
)

// empty string target means all possible targets
func (s *simnet) FaultCircuit(origin, target string, dd DropDeafSpec, deliverDroppedSends bool) (err error) {

	fault := newCircuitFault(origin, target, dd, deliverDroppedSends)

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
		//vv("server '%v' CircuitFault from '%v'; err = '%v'", origin, target, err)
	case <-s.halt.ReqStop.Chan:
		return
	}
	return
}

func (s *simnet) FaultHost(hostName string, dd DropDeafSpec, deliverDroppedSends bool) (err error) {

	fault := newHostFault(hostName, dd, deliverDroppedSends)

	select {
	case s.injectHostFaultCh <- fault:
		//vv("sent fault on injectHostFaultCh; about to wait on proceed")
	case <-s.halt.ReqStop.Chan:
		return
	}
	select {
	case <-fault.proceed:
		err = fault.err
		//vv("server '%v' hostFault from '%v'; dd='%v'; err = '%v'", hostName, dd, err)
	case <-s.halt.ReqStop.Chan:
		return
	}
	return
}

// RepairCircuit restores the local circuit to
// full working order. It undoes the effects of
// prior deafDrop actions, if any. It does not
// change an isolated simnode's isolation unless unIsolate
// is also true. See also RepairHost, AllHealthy.
// .
func (s *simnet) RepairCircuit(originName string, unIsolate bool, powerOnIfOff, deliverDroppedSends bool) (err error) {

	targetName := "" // all corresponding targets
	const justOrigin_NO = false
	oneGood := s.newCircuitRepair(originName, targetName, unIsolate, powerOnIfOff, justOrigin_NO, deliverDroppedSends)

	select {
	case s.repairCircuitCh <- oneGood:
		//vv("RepairCircuit sent oneGood on repairCircuitCh; about to wait on proceed")
	case <-s.halt.ReqStop.Chan:
		return
	}
	select {
	case <-oneGood.proceed:
		err = oneGood.err
		//vv("RepairCircuit '%v' done. err = '%v'", originName, err)
	case <-s.halt.ReqStop.Chan:
		return
	}
	return
}

// RepairHost repairs all the circuits on the host.
func (s *simnet) RepairHost(originName string, unIsolate bool, powerOnIfOff, allHosts, deliverDroppedSends bool) (err error) {
	//vv("top of RepairHost, originName = '%v'; unIsolate=%v, powerOnIfOff=%v, allHosts=%v", originName, unIsolate, powerOnIfOff, allHosts)
	//defer func() {
	//vv("end of RepairHost('%v')", originName)
	//}()

	repair := s.newHostRepair(originName, unIsolate, powerOnIfOff, allHosts, deliverDroppedSends)

	select {
	case s.repairHostCh <- repair:
		//vv("RepairHost sent repair on repairHostCh; about to wait on proceed")
	case <-s.halt.ReqStop.Chan:
		return
	}
	select {
	case <-repair.proceed:
		err = repair.err
		//vv("RepairHost '%v' done. err = '%v'", originName, err)
	case <-s.halt.ReqStop.Chan:
		return
	}
	return
}

// AllHealthy heal all partitions, undo all faults, network wide.
// All circuits are returned to HEALTHY status. Their powerOff status
// is not updated unless powerOnIfOff is also true.
// See also RepairSimnode for single simnode repair.
// .
func (s *simnet) AllHealthy(powerOnIfOff bool, deliverDroppedSends bool) (err error) {
	//vv("AllHealthy(powerOnIfOff=%v) called.", powerOnIfOff)

	const allHealthy_YES = true
	return s.RepairHost("", true, powerOnIfOff, allHealthy_YES, deliverDroppedSends)
}

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
		err = read.err

	case <-s.halt.ReqStop.Chan:
		return nil, ErrShutdown()
	}
	return
}

func (s *simnet) sendMessage(conn uConn, msg *Message, timeout *time.Duration) (err error) {

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
		err = send.err
	case <-s.halt.ReqStop.Chan:
		return ErrShutdown()
	}
	return
}

func newTimerCreateMop(isCli bool) (op *mop) {
	op = &mop{
		originCli: isCli,
		sn:        simnetNextMopSn(),
		kind:      TIMER,
		proceed:   make(chan struct{}),
		reqtm:     time.Now(),
		who:       int(runtime.GoID()),
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
		reqtm:        time.Now(),
		who:          int(runtime.GoID()),
	}
	return
}

func newReadMop(isCli bool) (op *mop) {
	op = &mop{
		originCli: isCli,
		sn:        simnetNextMopSn(),
		kind:      READ,
		proceed:   make(chan struct{}),
		reqtm:     time.Now(),
		who:       int(runtime.GoID()),
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
		reqtm:     time.Now(),
		who:       int(runtime.GoID()),
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

	// group simnodes on the same server by serverBaseID
	serverBaseID string

	// wait on
	done  chan struct{}
	reqtm time.Time

	// receive back
	simnode *simnode // our identity in the simnet (conn.local)
	conn    *simconn // our connection to server (c2s)
	who     int
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
		reqtm:            time.Now(),
		done:             make(chan struct{}),
		who:              int(runtime.GoID()),
	}
}

type serverRegistration struct {
	// provide
	server       *Server
	srvNetAddr   *SimNetAddr
	serverBaseID string

	// wait on
	done  chan struct{}
	reqtm time.Time

	// receive back
	simnode             *simnode // our identity in the simnet (conn.local)
	simnet              *simnet
	tellServerNewConnCh chan *simconn
	who                 int
}

func (s *simnet) newServerRegistration(srv *Server, srvNetAddr *SimNetAddr) *serverRegistration {
	return &serverRegistration{
		server:       srv,
		serverBaseID: srv.cfg.serverBaseID,
		srvNetAddr:   srvNetAddr,
		done:         make(chan struct{}),
		reqtm:        time.Now(),
		who:          int(runtime.GoID()),
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

type simnodeAlteration struct {
	simnet *simnet

	simnodeName string
	err         error // e.g. simnodeName not found

	alter Alteration
	undo  Alteration // how to reverse the alter

	isHostAlter bool
	done        chan struct{}
	reqtm       time.Time
	who         int
}

func (s *simnet) newCircuitAlteration(simnodeName string, alter Alteration, isHostAlter bool) *simnodeAlteration {
	return &simnodeAlteration{
		simnet: s,
		//simnode:     simnode,
		simnodeName: simnodeName,
		alter:       alter,
		isHostAlter: isHostAlter,
		done:        make(chan struct{}),
		reqtm:       time.Now(),
		who:         int(runtime.GoID()),
	}
}

func (s *simnet) AlterCircuit(simnodeName string, alter Alteration, wholeHost bool) (undo Alteration, err error) {

	if wholeHost {
		undo, err = s.AlterHost(simnodeName, alter)
		return
	}

	alt := s.newCircuitAlteration(simnodeName, alter, wholeHost)
	select {
	case s.alterSimnodeCh <- alt:
		//vv("sent alt on alterSimnodeCh; about to wait on done goro = %v", GoroNumber())
	case <-s.halt.ReqStop.Chan:
		return
	}
	select {
	case <-alt.done:
		undo = alt.undo
		err = alt.err
		//vv("server altered: %v", simnode)
	case <-s.halt.ReqStop.Chan:
		return
	}
	return
}

// we cannot guarantee that the undo will reverse all the
// changes if fine grained faults are in place; e.g. if
// only one auto-cli was down and we shutdown
// the host, the undo of restart will also bring up that
// auto-cli too. The undo is still very useful for tests
// even without that guarantee.
func (s *simnet) AlterHost(simnodeName string, alter Alteration) (undo Alteration, err error) {

	alt := s.newCircuitAlteration(simnodeName, alter, true)
	select {
	case s.alterHostCh <- alt:
		//vv("sent alt on alterHostCh; about to wait on done goro = %v", GoroNumber())
	case <-s.halt.ReqStop.Chan:
		return
	}
	select {
	case <-alt.done:
		undo = alt.undo
		err = alt.err
		//vv("host altered: %v", simnode)
	case <-s.halt.ReqStop.Chan:
		return
	}
	return
}

type circuitFault struct {
	originName string
	targetName string
	DropDeafSpec
	deliverDroppedSends bool

	sn      int64
	proceed chan struct{}
	reqtm   time.Time
	who     int

	err error
}

func newCircuitFault(originName, targetName string, dd DropDeafSpec, deliverDroppedSends bool) *circuitFault {
	return &circuitFault{
		originName:          originName,
		targetName:          targetName,
		DropDeafSpec:        dd,
		deliverDroppedSends: deliverDroppedSends,
		sn:                  simnetNextMopSn(),
		proceed:             make(chan struct{}),
		reqtm:               time.Now(),
		who:                 int(runtime.GoID()),
	}
}

type hostFault struct {
	hostName string
	DropDeafSpec
	deliverDroppedSends bool
	sn                  int64
	proceed             chan struct{}
	reqtm               time.Time
	err                 error
	who                 int
}

func newHostFault(hostName string, dd DropDeafSpec, deliverDroppedSends bool) *hostFault {
	return &hostFault{
		hostName:            hostName,
		DropDeafSpec:        dd,
		deliverDroppedSends: deliverDroppedSends,
		sn:                  simnetNextMopSn(),
		proceed:             make(chan struct{}),
		reqtm:               time.Now(),
		who:                 int(runtime.GoID()),
	}
}

type circuitRepair struct {
	originName string
	targetName string

	deliverDroppedSends bool
	justOriginHealed    bool
	unIsolate           bool
	powerOnIfOff        bool
	sn                  int64
	proceed             chan struct{}
	reqtm               time.Time
	err                 error
	who                 int
}

func (s *simnet) newCircuitRepair(originName, targetName string, unIsolate, powerOnIfOff, justOrigin, deliverDroppedSends bool) *circuitRepair {
	return &circuitRepair{
		deliverDroppedSends: deliverDroppedSends,
		justOriginHealed:    justOrigin,
		originName:          originName,
		targetName:          targetName,
		unIsolate:           unIsolate,
		powerOnIfOff:        powerOnIfOff,
		sn:                  simnetNextMopSn(),
		proceed:             make(chan struct{}),
		reqtm:               time.Now(),
		who:                 int(runtime.GoID()),
	}
}

type hostRepair struct {
	hostName            string
	powerOnIfOff        bool
	unIsolate           bool
	allHosts            bool
	deliverDroppedSends bool
	sn                  int64
	proceed             chan struct{}
	reqtm               time.Time
	err                 error
	who                 int
}

func (s *simnet) newHostRepair(hostName string, unIsolate, powerOnIfOff, allHosts, deliverDroppedSends bool) *hostRepair {
	m := &hostRepair{
		deliverDroppedSends: deliverDroppedSends,
		hostName:            hostName,
		powerOnIfOff:        powerOnIfOff,
		unIsolate:           unIsolate,
		allHosts:            allHosts,
		sn:                  simnetNextMopSn(),
		proceed:             make(chan struct{}),
		reqtm:               time.Now(),
		who:                 int(runtime.GoID()),
	}
	return m
}

func CallbackOnNewTimer(
	func(proposedDeadline time.Time,
		pkgFileLine string,
	) (assignedDeadline time.Time)) {

}

type SimnetConnSummary struct {
	OriginIsCli      bool
	Origin           string
	OriginState      Faultstate
	OriginConnClosed bool
	OriginPoweroff   bool
	Target           string
	TargetState      Faultstate
	TargetConnClosed bool
	TargetPoweroff   bool
	DropSendProb     float64
	DeafReadProb     float64

	// origin Q summary
	Qs string

	// origin priority queues:
	// Qs is the convenient/already stringified form of
	// these origin queues.
	// These allow stronger test assertions.  They are deep clones
	// and so mostly race free except for the
	// pointers mop.{origin,target,origTimerMop,msg,sendmop,readmop},
	// access those only after the simnet has been shutdown.
	// The proceed channel is always nil.
	DroppedSendQ *pq
	DeafReadQ    *pq
	ReadQ        *pq
	PreArrQ      *pq
	TimerQ       *pq
}

type SimnetPeerStatus struct {
	Name         string
	Conn         []*SimnetConnSummary
	Connmap      map[string]*SimnetConnSummary
	ServerState  Faultstate
	Poweroff     bool
	LC           int64
	ServerBaseID string
	IsLoneCli    bool // and not really a peer server with auto-cli
}

type SimnetSnapshot struct {
	Asof               time.Time
	Loopi              int64
	NetClosed          bool
	GetSimnetStatusErr error
	Cfg                SimNetConfig
	PeerConnCount      int
	LoneCliConnCount   int

	ScenarioNum    int
	ScenarioSeed   [32]byte
	ScenarioTick   time.Duration
	ScenarioMinHop time.Duration
	ScenarioMaxHop time.Duration

	Peer    []*SimnetPeerStatus
	Peermap map[string]*SimnetPeerStatus
	LoneCli map[string]*SimnetPeerStatus // not really a peer but meh.

	reqtm   time.Time
	proceed chan struct{}
	who     int
}

func (s *simnet) GetSimnetSnapshot() (snap *SimnetSnapshot) {
	snap = &SimnetSnapshot{
		reqtm:   time.Now(),
		proceed: make(chan struct{}),
		who:     int(runtime.GoID()),
	}
	select {
	case s.simnetSnapshotRequestCh <- snap:
	case <-s.halt.ReqStop.Chan:
		return
	}
	select {
	case <-snap.proceed:
	case <-s.halt.ReqStop.Chan:
	}
	return
}

type SimnetSnapshotter struct {
	simnet *simnet
}

func (s *SimnetSnapshotter) GetSimnetSnapshot() *SimnetSnapshot {
	return s.simnet.GetSimnetSnapshot()
}

// SimnetBatch is a proposed design for
// sending in a batch of network fault/repair/config changes
// at once. Currently a prototype; not really finished/tested yet.
type SimnetBatch struct {
	net          *simnet
	batchSn      int64
	batchSz      int64
	batchSubWhen time.Time
	batchSubAsap bool
	reqtm        time.Time
	proceed      chan struct{}
	err          error
	batchOps     []*mop
	who          int
}

func (s *simnet) NewSimnetBatch(subwhen time.Time, subAsap bool) *SimnetBatch {
	return &SimnetBatch{
		net:          s,
		batchSn:      simnetNextBatchSn(),
		batchSubWhen: subwhen,
		batchSubAsap: subAsap,
		reqtm:        time.Now(),
		proceed:      make(chan struct{}),
		who:          int(runtime.GoID()),
	}
}

// SubmitBatch does not block.
func (s *simnet) SubmitBatch(batch *SimnetBatch) {
	op := &mop{
		kind:    BATCH,
		sn:      simnetNextMopSn(),
		batch:   batch,
		proceed: batch.proceed,
		reqtm:   time.Now(),
		who:     int(runtime.GoID()),
	}
	select {
	case s.submitBatchCh <- op:
	case <-s.halt.ReqStop.Chan:
		return
	}
	//select {
	//case <-batch.proceed:
	//	err = batch.err
	//case <-s.halt.ReqStop.Chan:
	//}
	return
}

func (b *SimnetBatch) add(op *mop) {
	op.batchPart = int64(len(b.batchOps))
	op.batchSn = b.batchSn
	b.batchOps = append(b.batchOps, op)
}

// empty string target means all possible targets
func (b *SimnetBatch) FaultCircuit(origin, target string, dd DropDeafSpec, deliverDroppedSends bool) {
	//s := b.net

	cktFault := newCircuitFault(origin, target, dd, deliverDroppedSends)
	b.add(newCktFaultMop(cktFault))
}

func (b *SimnetBatch) FaultHost(hostName string, dd DropDeafSpec, deliverDroppedSends bool) {
	//s := b.net

	hostFault := newHostFault(hostName, dd, deliverDroppedSends)
	b.add(newHostFaultMop(hostFault))
}

func (b *SimnetBatch) RepairCircuit(originName string, unIsolate bool, powerOnIfOff, deliverDroppedSends bool) {
	s := b.net

	targetName := "" // all corresponding targets
	const justOrigin_NO = false
	repairCkt := s.newCircuitRepair(originName, targetName, unIsolate, powerOnIfOff, justOrigin_NO, deliverDroppedSends)
	b.add(newRepairCktMop(repairCkt))
}

// RepairHost repairs all the circuits on the host.
func (b *SimnetBatch) RepairHost(originName string, unIsolate bool, powerOnIfOff, allHosts, deliverDroppedSends bool) {
	repairHost := b.net.newHostRepair(originName, unIsolate, powerOnIfOff, allHosts, deliverDroppedSends)
	b.add(newRepairHostMop(repairHost))
}

func (b *SimnetBatch) AllHealthy(powerOnIfOff bool, deliverDroppedSends bool) {
	const allHealthy_YES = true
	repairHost := b.net.newHostRepair("", true, powerOnIfOff, allHealthy_YES, deliverDroppedSends)
	b.add(newRepairHostMop(repairHost))
}
func (b *SimnetBatch) registerServer(srv *Server, srvNetAddr *SimNetAddr) {
	s := b.net

	srvreg := s.newServerRegistration(srv, srvNetAddr)
	b.add(newServerRegMop(srvreg))
}

func (b *SimnetBatch) AlterCircuit(simnodeName string, alter Alteration, wholeHost bool) {
	s := b.net
	if wholeHost {
		b.AlterHost(simnodeName, alter)
		return
	}
	alt := s.newCircuitAlteration(simnodeName, alter, wholeHost)
	b.add(newAlterNodeMop(alt))
}

// we cannot guarantee that the undo will reverse all the
// changes if fine grained faults are in place; e.g. if
// only one auto-cli was down and we shutdown
// the host, the undo of restart will also bring up that
// auto-cli too. The undo is still very useful for tests
// even without that guarantee.
func (b *SimnetBatch) AlterHost(simnodeName string, alter Alteration) {
	s := b.net

	alt := s.newCircuitAlteration(simnodeName, alter, true)
	b.add(newAlterHostMop(alt))
}

func (b *SimnetBatch) GetSimnetSnapshot() {
	snapReq := &SimnetSnapshot{
		reqtm: time.Now(),
		who:   int(runtime.GoID()),
	}
	b.add(newSnapReqMop(snapReq))
}
