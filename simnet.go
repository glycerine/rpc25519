package rpc25519

// build/run with:
// GOTRACEBACK=all GOEXPERIMENT=synctest go test -v

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"weak"

	cristalbase64 "github.com/cristalhq/base64"
	"github.com/glycerine/blake3"
	"github.com/glycerine/idem"
	rb "github.com/glycerine/rbtree"
)

// NB: all String() methods are now in simnet_string.go

type GoroControl struct {
	Mut  sync.Mutex
	Tm   time.Time
	Goro int
}

// Message operation
type mop struct {
	sn    int64
	who   int    // goro number
	where string // generic fileLine (send,read,timer have their atm)

	proceedMop *mop // in meq, should close(proceed) at completeTm

	batchSn   int64
	batchPart int64

	// API origin submission to simnet, consistently
	// on all requests like client/server registration,
	// snapshots, everything. probably redundant with
	// the earlier fields below, but okay; this might
	// be realtime if we are not under faketime.
	reqtm time.Time

	// so we can handle a network
	// rather than just cli/srv.
	origin *simnode
	target *simnode

	originCli bool

	senderLC int64
	readerLC int64
	originLC int64

	//	timerC              chan time.Time
	timerC              weak.Pointer[chan time.Time]
	timerCstrong        chan time.Time
	timerDur            time.Duration
	timerFileLine       string // where was this timer from?
	timerReseenCount    int
	handleDiscardCalled bool

	readFileLine string
	sendFileLine string

	// discards tell us the corresponding create timer here.
	origTimerMop        *mop
	origTimerCompleteTm time.Time

	// when fired => unarmed. NO timer.Reset support at the moment!
	timerFiredTm time.Time // first fire time, count in timerReseenCount
	// was discarded timer armed?
	wasArmed bool
	// was timer set internally to wake for
	// arrival of pending message?
	internalPendingTimer bool
	// so we can cleanup the timers from dropped sends.
	internalPendingTimerForSend *mop

	// is this our single grid step timer?
	// There should only ever be one
	// timer with this flag true.
	isGridStepTimer bool // TODO possible delete.

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

	// timers
	unmaskedCompleteTm time.Time
	unmaskedDur        time.Duration

	// sends
	unmaskedSendArrivalTm time.Time

	kind mopkind
	msg  *Message

	sendmop *mop // for reads, which send did we get?
	readmop *mop // for sends, which read did we go to?

	err error // read/send errors

	// clients of scheduler wait on proceed.
	// When the timer is set; the send sent; the read
	// matches a send, the client proceeds because
	// this channel will be closed.
	proceed chan struct{}

	isEOF_RST bool

	// have to adjust the probabililistic drop/deaf
	// probability by the number of attempts at delivery/read
	readAttempt int64
	sendAttempt int64

	// true if drop probability already applied (=> ok to send)
	sendDropFilteringApplied bool

	readDeafDueToProb int64 // >0 => read is deaf due to flaky net
	readOKDueToProb   int64 // >0 => read is fine despite flaky net

	lastIsDeafTrueTm time.Time
	lastP            float64

	cliReg  *clientRegistration
	srvReg  *serverRegistration
	snapReq *SimnetSnapshot

	scen       *scenario
	cktFault   *circuitFault
	hostFault  *hostFault
	repairCkt  *circuitRepair
	repairHost *hostRepair
	alterHost  *simnodeAlteration
	alterNode  *simnodeAlteration
	batch      *SimnetBatch
}

func (op *mop) whence() string {
	switch op.kind {
	case SCENARIO:
	case CLIENT_REG:
	case SERVER_REG:
	case FAULT_CKT:
	case FAULT_HOST:
	case REPAIR_CKT:
	case REPAIR_HOST:
	case ALTER_HOST:
	case ALTER_NODE:
	case BATCH:
	case SEND:
		return op.sendFileLine
	case READ:
		return op.readFileLine
	case TIMER:
		return op.timerFileLine
	case TIMER_DISCARD:
		return op.timerFileLine
	case SNAPSHOT:
	}
	return op.where
}

// increase determinism by processing
// the meq in priority order, after
// giving each mop a different priority.
// note: keeping the priority numbers as 100 x the mopking
// just makes it easy to be sure we have not
// missed a priority if we add a new mopkind.
func (s *mop) priority() int64 {
	switch s.kind {
	case SCENARIO:
		return 100

	case CLIENT_REG:
		return 200
	case SERVER_REG:
		return 300

	case FAULT_CKT:
		return 400
	case FAULT_HOST:
		return 500
	case REPAIR_CKT:
		return 600
	case REPAIR_HOST:
		return 700

	case ALTER_HOST:
		return 800
	case ALTER_NODE:
		return 900

	case BATCH:
		return 1000 // not sure. how strongly prioritized should batches be?

	case SEND:
		return 1100
	case READ:
		return 1200
	case TIMER:
		return 1300
	case TIMER_DISCARD:
		return 1400

	case SNAPSHOT:
		return 1500
	}
	panic(fmt.Sprintf("mop kind '%v' needs priority", int(s.kind)))
}
func (s *mop) tm() time.Time {
	switch s.kind {
	case SEND:
		if s.arrivalTm.IsZero() {
			return s.reqtm // not yet hit handleSend(); s.initTm same == s.reqtm
		}
		return s.arrivalTm
	case READ:
		if s.initTm.IsZero() {
			return s.reqtm // not yet hit handleRead()
		}
		// not completeTm: when the read returns to user who called readMessage()
		// initTm: timer started, read begin waiting, send hits the socket.
		// initTm: for timer discards, when discarded so usually initTm.
		return s.initTm
	case TIMER:
		if s.unmaskedCompleteTm.IsZero() {
			return s.reqtm // not yet hit handleTimer()
		}
		return s.completeTm
	case TIMER_DISCARD:
		if !s.handleDiscardCalled {
			return s.reqtm // not yet hit handleDiscardTimer()
		}
		return s.initTm

	case SNAPSHOT:
		return s.reqtm
	case CLIENT_REG:
		return s.reqtm
	case SERVER_REG:
		return s.reqtm

	case FAULT_CKT:
		return s.reqtm
	case FAULT_HOST:
		return s.reqtm
	case REPAIR_CKT:
		return s.reqtm
	case REPAIR_HOST:
		return s.reqtm
	case SCENARIO:
		return s.reqtm

	case ALTER_HOST:
		return s.reqtm
	case ALTER_NODE:
		return s.reqtm
	case PROCEED:
		return s.completeTm
	case TIMER_FIRES:
		return s.completeTm
	}
	panic(fmt.Sprintf("mop kind '%v' needs tm", int(s.kind)))
}

// mop.sn assignment by client code is
// non-deterministic. the client or the
// server could get their read in first,
// for example. so avoid using .sn to break ties
// until after looking at origin:target.
func newMasterEventQueue(owner string) *pq {

	cmp := func(a, b rb.Item) int {
		av := a.(*mop)
		bv := b.(*mop)

		if av == nil {
			panic("no nil events allowed in the meq")
			return -1
		}
		if bv == nil {
			panic("no nil events allowed in the meq")
			return 1
		}
		asn := av.sn
		bsn := bv.sn
		// be sure to keep delete by sn working
		if asn == bsn {
			return 0
		}
		atm := av.tm()
		btm := bv.tm()
		if atm.Before(btm) {
			return -1
		}
		if atm.After(btm) {
			return 1
		}
		apri := av.priority()
		bpri := bv.priority()
		if apri < bpri {
			return -1
		}
		if apri > bpri {
			return 1
		}
		// same priority, i.e. both reads

		// some alt from <-s.alterHostCh, newAlterHostMop(alt)) had
		// mop.origin nil, so try not to seg fault on it.
		if av.origin == nil && bv.origin != nil {
			return -1
		}
		if av.origin != nil && bv.origin == nil {
			return 1
		}
		if av.origin != nil && bv.origin != nil {
			if av.origin.name < bv.origin.name {
				return -1
			}
			if av.origin.name > bv.origin.name {
				return 1
			}
		}
		if av.target == nil && bv.target != nil {
			return -1
		}
		if av.target != nil && bv.target == nil {
			return 1
		}
		if av.target != nil && bv.target != nil {
			if av.target.name < bv.target.name {
				return -1
			}
			if av.target.name > bv.target.name {
				return 1
			}
		}
		// same origin, same target.
		// maybe these next?
		//senderLC int64
		//readerLC int64
		//originLC int64

		if asn < bsn {
			return -1
		}
		if asn > bsn {
			return 1
		}
		return 0
	}
	return &pq{
		Owner:   owner,
		Orderby: "tm() then priority() then sn()",
		Tree:    rb.NewTree(cmp),
		cmp:     cmp,
	}
}

func (s *mop) clone() (c *mop) {
	cp := *s
	c = &cp
	c.proceed = nil // not cloned
	return
}

// want decentralized userMaskTime instead, mostly...
// but try again for more deterministic scheduling.
// simnet wide next deadline assignment for all
// timers on all nodes, and all send and read points.
func (s *Simnet) nextUniqTm(atleast time.Time, who int) time.Time {
	if who == 0 {
		panic("who 0 not set!")
	}
	if atleast.After(s.lastTimerDeadline) {
		s.lastTimerDeadline = atleast
	} else {
		// INVAR: atleast <= s.lastTimerDeadline
		// I think userMaskTime alone might suffice now.
		// We know that s.lastTimerDeadline is acceptable.

		//const bump = timeMask0 // more than mask
		//s.lastTimerDeadline = s.lastTimerDeadline.Add(bump)
	}
	s.lastTimerDeadline = userMaskTime(s.lastTimerDeadline, who)
	return s.lastTimerDeadline
}

// fin records details of a finished mop
// into our mop tracking slices.
func (s *Simnet) fin(op *mop) {
	// gets called by api on different goro.
	now := time.Now()
	s.xmut.Lock()
	defer s.xmut.Unlock()

	s.xfinorder[op.sn] = s.nextMopSn
	s.xfintm[op.sn] = now
	w := op.whence() // file:line where created.
	s.xwhence[op.sn] = w
	s.xkind[op.sn] = op.kind
	s.xwho[op.sn] = op.who

	i := op.sn
	var b [9]byte
	b[0] = byte(i >> 56)
	b[1] = byte(i >> 48)
	b[2] = byte(i >> 40)
	b[3] = byte(i >> 32)
	b[4] = byte(i >> 24)
	b[5] = byte(i >> 16)
	b[6] = byte(i >> 8)
	b[7] = byte(i)
	b[8] = byte(op.kind)
	s.xb3hash.Write(append(b[:], []byte(w)...))
}

func (s *Simnet) simnetNextMopSn() (sn int64) {
	s.xmut.Lock()
	defer s.xmut.Unlock()
	sn = s.nextMopSn
	s.nextMopSn++
	s.xissuetm = append(s.xissuetm, time.Now())
	s.xfintm = append(s.xfintm, time.Time{})
	s.xwhence = append(s.xwhence, "")
	s.xkind = append(s.xkind, -1)
	s.xfinorder = append(s.xfinorder, -1)
	s.xwho = append(s.xwho, -1)

	return
}

func (s *Simnet) simnetNextBatchSn() int64 {
	return atomic.AddInt64(&s.simnetLastBatchSn, 1)
}

// simnet simulates a network entirely with channels in memory.
type Simnet struct {
	ndtotPrev int64
	ndtot     int64 // num dispatched total.

	nextMopSn         int64
	simnetLastBatchSn int64

	// fin records execution/finishing order
	// for mop sn into xorder.
	xfinorder []int64
	xwhence   []string
	xkind     []mopkind
	xissuetm  []time.Time
	xfintm    []time.Time
	xwho      []int

	xmut    sync.Mutex
	xb3hash *blake3.Hasher

	barrier bool
	bigbang time.Time
	//gridStepTimer *mop
	who int

	// upon request, we can be noisy if we
	// have nothing to do, for diagnostics.
	noisyNothing atomic.Bool

	// lastTimerDeadline: issue all timers
	// must be greater than lastTimerDeadline,
	// and then they must set their time to it.
	// by induction we just have to
	// record each timer deadline, and make
	// sure each new one is
	// distinct from the last.
	// then all timers will have
	// distinct firing times.
	lastTimerDeadline time.Time

	// for assertGoroAlone in simnet_synctest
	singleGoroMut sync.Mutex
	singleGoroTm  time.Time
	singleGoroID  int

	scenario *scenario

	meq *pq // the master event queue.

	cfg       *Config
	simNetCfg *SimNetConfig

	srv *Server
	cli *Client

	node2server map[*simnode]*simnode

	dns map[string]*simnode
	//dns *dmap[string, *simnode]

	// circuits[A][B] is the bi-directed, very cyclic,
	// graph of the network. A sparse matrix, circuits[A]
	// is a map of circuits that A has locally, keyed
	// by remote node; circuits[A][B] is A's local
	// simconn to B. The corresponding simmconn connection
	// endpoint owned by B is found in circuits[B][A].
	//
	// I use bi-directed to mean each A -> B is always
	// paired with a B -> A connection. Thus one-way
	// faults can be modelled or assigned probability
	// independent of the other direction's fault status.

	// need a deterministic iteration order to
	// make the simulation more repeatable, so
	// map is a problem. try dmap.
	circuits *dmap[*simnode, *dmap[*simnode, *simconn]]
	servers  map[string]*simnode // serverBaseID:srvnode
	allnodes map[*simnode]bool
	orphans  map[*simnode]bool // cli without baseserver

	cliRegisterCh chan *clientRegistration
	srvRegisterCh chan *serverRegistration

	alterSimnodeCh chan *simnodeAlteration
	alterHostCh    chan *simnodeAlteration
	submitBatchCh  chan *mop

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

	simnetSnapshotRequestCh chan *SimnetSnapshot

	newScenarioCh chan *scenario
	nextTimer     *time.Timer
	lastArmToFire time.Time
	lastArmDur    time.Duration
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

	net     *Simnet
	isCli   bool
	cliConn *simconn

	netAddr      *SimNetAddr
	serverBaseID string

	// state survives power cycling, i.e. rebooting
	// a simnode does not heal the network or repair a
	// faulty network card.
	state    Faultstate
	powerOff bool

	tellServerNewConnCh chan *simconn

	// servers track their autocli here
	autocli map[*simnode]*simconn
	// autocli + self
	allnode map[*simnode]bool

	// count of probabilistically dropped/deaf
	droppedSendDueToProb int64
	deafReadDueToProb    int64
	okReadDueToProb      int64
}

func (s *simnode) id() string {
	if s == nil {
		return ""
	}
	return s.name
}
func (s *Simnet) locals(node *simnode) map[*simnode]bool {
	srvnode, ok := s.node2server[node]
	if !ok {
		// must be lone cli, e.g. 771 simnet_test.
		return map[*simnode]bool{node: true}
		//panic(fmt.Sprintf("not registered in s.node2server: node = '%v'", node.name))
	}
	return srvnode.allnode
}

func (s *Simnet) localServer(node *simnode) *simnode {
	srvnode, ok := s.node2server[node]
	if !ok {
		panic(fmt.Sprintf("not registered in s.node2server: node = '%v'", node.name))
	}
	return srvnode
}

func (s *Simnet) newSimnode(name, serverBaseID string) *simnode {
	return &simnode{
		name:         name,
		serverBaseID: serverBaseID,
		readQ:        newPQinitTm(name+" readQ ", false),
		preArrQ:      s.newPQarrivalTm(name + " preArrQ "),
		timerQ:       newPQcompleteTm(name + " timerQ "),
		deafReadQ:    newPQinitTm(name+" deaf reads Q ", true),
		droppedSendQ: s.newPQarrivalTm(name + " dropped sends Q "),
		net:          s,

		// clients don't need these, so we could make them lazily
		autocli: make(map[*simnode]*simconn),
		allnode: make(map[*simnode]bool),
	}
}

func (s *Simnet) newSimnodeClient(name, serverBaseID string) (simnode *simnode) {
	simnode = s.newSimnode(name, serverBaseID)
	simnode.isCli = true
	return
}

func (s *Simnet) newCircuitserver(name, serverBaseID string) (simnode *simnode) {
	simnode = s.newSimnode(name, serverBaseID)
	simnode.isCli = false
	// buffer so servers don't have to be up to get them.
	simnode.tellServerNewConnCh = make(chan *simconn, 100)
	return
}

// for additional servers after the first.
func (s *Simnet) handleServerRegistration(op *mop) {

	var reg *serverRegistration = op.srvReg

	// srvNetAddr := SimNetAddr{ // implements net.Addr interface
	// 	network: "simnet",
	// 	addr:
	// 	name:    reg.server.name,
	// 	isCli:   false,
	// }

	srvnode := s.newCircuitserver(reg.server.name, reg.serverBaseID)
	srvnode.allnode[srvnode] = true
	srvnode.netAddr = reg.srvNetAddr
	s.circuits.set(srvnode, newDmap[*simnode, *simconn]())
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

	//vv("end of handleServerRegistration, about to close(reg.done). srvreg is %v", reg)

	// channel made by newCircuitserver() above.
	reg.tellServerNewConnCh = srvnode.tellServerNewConnCh
	s.fin(op)
	close(reg.proceed)
	if false {
		now := time.Now()
		closer := &mop{
			completeTm: userMaskTime(now, reg.who), // what tm() refers to.
			kind:       PROCEED,
			//proceedMop:  reg,
			sn:      s.simnetNextMopSn(),
			proceed: reg.proceed,
			reqtm:   reg.reqtm,
			who:     reg.who,
		}
		s.add2meq(closer, -1)
	}
}

func (s *Simnet) handleClientRegistration(regop *mop) {

	var reg *clientRegistration = regop.cliReg

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
	clientOutboundEdges := newDmap[*simnode, *simconn]()
	s.circuits.set(clinode, clientOutboundEdges)

	// add both direction edges
	c2s := s.addEdgeFromCli(clinode, srvnode)
	s2c := s.addEdgeFromSrv(srvnode, clinode)

	// let server reconstruct its autocli if it comes late
	clinode.cliConn = c2s

	if reg.serverBaseID != "" {
		basesrv, ok := s.servers[reg.serverBaseID]
		if ok {
			//vv("cli is auto-cli of basesrv='%v'", basesrv.name)
			s.node2server[clinode] = basesrv
			basesrv.autocli[clinode] = c2s
			basesrv.allnode[clinode] = true
		} else {
			//vv("cli is orphan")
			s.orphans[clinode] = true
		}
	} else {
		//vv("no reg.serverBaseID -- expected for lone/orphan cli")
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
			//vv("%v srvnode was notified of new client '%v'; s2c='%v'", srvnode.name, clinode.name, s2c)

			// let client start using the connection/edge.
			s.fin(regop)
			close(reg.proceed)
		case <-s.halt.ReqStop.Chan:
			return
		}
	}()
}

// idempotent, all servers do this, then register through the same path.
func (cfg *Config) bootSimNetOnServer(simNetConfig *SimNetConfig, srv *Server) *Simnet { // (tellServerNewConnCh chan *simconn) {

	//vv("%v newSimNetOnServer top, goro = %v", srv.name, GoroNumber())
	cfg.simnetRendezvous.singleSimnetMut.Lock()
	defer cfg.simnetRendezvous.singleSimnetMut.Unlock()

	if cfg.simnetRendezvous.singleSimnet != nil {
		// already started. Still, everyone
		// register separately no matter.
		net := cfg.simnetRendezvous.singleSimnet
		net.halt.AddChild(srv.halt)
		return net
	}

	tick := time.Millisecond
	minHop := time.Millisecond * 10
	maxHop := minHop
	var seed [32]byte
	scen := NewScenario(tick, minHop, maxHop, seed)

	// server creates simnet; must start server first.
	s := &Simnet{
		//uniqueTimerQ:   newPQcompleteTm("simnet uniquetimerQ "),
		xb3hash:        blake3.New(64, nil),
		meq:            newMasterEventQueue("scheduler"),
		barrier:        !simNetConfig.BarrierOff,
		cfg:            cfg,
		simNetCfg:      simNetConfig,
		srv:            srv,
		cliRegisterCh:  make(chan *clientRegistration),
		srvRegisterCh:  make(chan *serverRegistration),
		alterSimnodeCh: make(chan *simnodeAlteration),
		alterHostCh:    make(chan *simnodeAlteration),
		submitBatchCh:  make(chan *mop),
		msgSendCh:      make(chan *mop),
		msgReadCh:      make(chan *mop),
		addTimer:       make(chan *mop),
		discardTimerCh: make(chan *mop),
		newScenarioCh:  make(chan *scenario),

		injectCircuitFaultCh: make(chan *circuitFault),
		injectHostFaultCh:    make(chan *hostFault),
		repairCircuitCh:      make(chan *circuitRepair),
		repairHostCh:         make(chan *hostRepair),

		scenario:                scen,
		simnetSnapshotRequestCh: make(chan *SimnetSnapshot),
		dns:                     make(map[string]*simnode),
		node2server:             make(map[*simnode]*simnode),

		// graph of circuits, edges are circuits[from][to]
		circuits: newDmap[*simnode, *dmap[*simnode, *simconn]](),

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
	s.halt = idem.NewHalterNamed(fmt.Sprintf("simnet %p", s))
	s.halt.AddChild(srv.halt)

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

func (s *Simnet) addEdgeFromSrv(srvnode, clinode *simnode) *simconn {

	srv, ok := s.circuits.get2(srvnode) // edges from srv to clients
	if !ok {
		srv = newDmap[*simnode, *simconn]()
		s.circuits.set(srvnode, srv)
	}
	s2c := newSimconn()
	s2c.isCli = false
	s2c.net = s
	s2c.local = srvnode
	s2c.remote = clinode
	s2c.netAddr = srvnode.netAddr

	// replace any previous conn
	srv.set(clinode, s2c)
	return s2c
}

func (s *Simnet) addEdgeFromCli(clinode, srvnode *simnode) *simconn {

	cli, ok := s.circuits.get2(clinode) // edge from client to one server
	if !ok {
		cli = newDmap[*simnode, *simconn]()
		s.circuits.set(clinode, cli)
	}
	c2s := newSimconn()
	c2s.isCli = true
	c2s.net = s
	c2s.local = clinode
	c2s.remote = srvnode
	c2s.netAddr = clinode.netAddr

	// replace any previous conn
	//cli[srvnode] = c2s
	cli.set(srvnode, c2s)
	return c2s
}

type mopkind int

const (
	MOP_UNDEFINED mopkind = 0

	SCENARIO mopkind = 1

	CLIENT_REG mopkind = 2
	SERVER_REG mopkind = 3

	FAULT_CKT   mopkind = 4
	FAULT_HOST  mopkind = 5
	REPAIR_CKT  mopkind = 6
	REPAIR_HOST mopkind = 7
	ALTER_HOST  mopkind = 8
	ALTER_NODE  mopkind = 9

	// atomically apply a set of the above.
	BATCH mopkind = 10

	// core network primitives
	SEND          mopkind = 11
	READ          mopkind = 12
	TIMER         mopkind = 13
	TIMER_DISCARD mopkind = 14

	// report a snapshot of the entire network/state.
	SNAPSHOT    mopkind = 15
	PROCEED     mopkind = 16 // not currently used.
	TIMER_FIRES mopkind = 17 // not currently used.
)

func enforceTickDur(tick time.Duration) time.Duration {
	return tick.Truncate(timeMask0)
}

type pq struct {
	Owner   string
	Orderby string
	Tree    *rb.Tree

	// don't export so user does not
	// accidentally mess with it.
	cmp func(a, b rb.Item) int

	isDeafQ bool
}

func (s *pq) Len() int {
	return s.Tree.Len()
}

// deep meaning we clone the *mop contents as
// well as the tree. the *mop themselves are a
// shallow cloned to avoid infinite loop on cycles
// of pointers.
func (s *pq) deepclone() (c *pq) {

	c = &pq{
		Owner:   s.Owner,
		Orderby: s.Orderby,
		Tree:    rb.NewTree(s.cmp),
		// cmp is shared by simnet and out to user goro
		// without locking; it is a pure function
		// though, so this should be fine--also this saves us
		// from having to know exactly which of thee
		// three possible PQ ordering functions we have.
		cmp: s.cmp,
	}
	for it := s.Tree.Min(); !it.Limit(); it = it.Next() {
		c.Tree.Insert(it.Item().(*mop).clone())
	}
	return
}

func (s *pq) peek() *mop {
	n := s.Tree.Len()
	if n == 0 {
		return nil
	}
	it := s.Tree.Min()
	if it.Limit() {
		panic("n > 0 above, how is this possible?")
		return nil
	}
	return it.Item().(*mop)
}

func (s *pq) pop() *mop {
	n := s.Tree.Len()
	//vv("pop n = %v", n)
	if n == 0 {
		return nil
	}
	it := s.Tree.Min()
	if it.Limit() {
		panic("n > 0 above, how is this possible?")
		return nil
	}
	top := it.Item().(*mop)
	s.Tree.DeleteWithIterator(it)
	return top
}

func (s *pq) add(op *mop) (added bool, it rb.Iterator) {
	if op == nil {
		panic("do not put nil into pq!")
	}
	//if s.isDeafQ && op.kind == READ { // sends go in too.
	//	panic(fmt.Sprintf("where read added to deafReadQ: '%v'", op))
	//}
	added, it = s.Tree.InsertGetIt(op)
	return
}

func (s *pq) del(op *mop) (found bool) {
	if op == nil {
		panic("cannot delete nil mop!")
	}
	var it rb.Iterator
	it, found = s.Tree.FindGE_isEqual(op)
	if !found {
		return
	}
	s.Tree.DeleteWithIterator(it)
	return
}

func (s *pq) deleteAll() {
	s.Tree.DeleteAll()
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
func (s *Simnet) newPQarrivalTm(owner string) *pq {
	cmp := func(a, b rb.Item) int {
		av := a.(*mop)
		bv := b.(*mop)

		if av == bv {
			return 0 // points to same memory (or both nil)
		}
		if av == nil {
			// just a is nil; b is not. sort nils to the front
			// so they get popped and GC-ed sooner (and
			// don't become temporary memory leaks by sitting at the
			// back of the queue.
			panic("no nils in pq")
			return -1
		}
		if bv == nil {
			panic("no nils in pq")
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
	}
	return &pq{
		Owner:   owner,
		Orderby: "arrivalTm",
		Tree:    rb.NewTree(cmp),
		cmp:     cmp,
	}
}

// order by mop.initTm, then mop.sn;
// for reads (readQ).
func newPQinitTm(owner string, isDeafQ bool) *pq {

	q := &pq{
		Owner:   owner,
		Orderby: "initTm",
		isDeafQ: isDeafQ,
	}
	cmp := func(a, b rb.Item) int {
		av := a.(*mop)
		bv := b.(*mop)

		if av == bv {
			return 0 // points to same memory (or both nil)
		}
		if av == nil {
			// just a is nil; b is not. sort nils to the front
			// so they get popped and GC-ed sooner (and
			// don't become temporary memory leaks by sitting at the
			// back of the queue.
			panic("no nils allowed in tree")
			return -1
		}
		if bv == nil {
			panic("no nils allowed in tree")
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
	}
	q.Tree = rb.NewTree(cmp)
	q.cmp = cmp
	return q
}

// order by mop.completeTm then mop.sn; for timers
func newPQcompleteTm(owner string) *pq {
	cmp := func(a, b rb.Item) int {
		av := a.(*mop)
		bv := b.(*mop)

		if av == bv {
			return 0 // points to same memory (or both nil)
		}
		if av == nil {
			// just a is nil; b is not. sort nils to the front
			// so they get popped and GC-ed sooner (and
			// don't become temporary memory leaks by sitting at the
			// back of the queue.
			panic("no nils in pq")
			return -1
		}
		if bv == nil {
			panic("no nils in pq")
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
	}
	return &pq{
		Owner:   owner,
		Orderby: "completeTm",
		Tree:    rb.NewTree(cmp),
		cmp:     cmp,
	}
}

func (s *Simnet) shutdownSimnode(simnode *simnode) (undo Alteration) {
	if simnode.powerOff {
		// no-op. already off.
		return UNDEFINED
	}
	//vv("handleAlterCircuit: SHUTDOWN %v, going to powerOff true, in state %v", simnode.name, simnode.state)
	simnode.powerOff = true
	undo = POWERON

	simnode.readQ.deleteAll()
	simnode.preArrQ.deleteAll()
	simnode.timerQ.deleteAll()
	simnode.deafReadQ.deleteAll()
	// leave droppedSendQ, useful to simulate a slooow network
	// that eventually delivers messages from a server after several
	// power cycles.

	//vv("handleAlterCircuit: end SHUTDOWN, simnode is now: %v", simnode)
	return
}

func (s *Simnet) powerOnSimnode(simnode *simnode) (undo Alteration) {
	if !simnode.powerOff {
		// no-op. already on.
		return UNDEFINED
	}
	undo = SHUTDOWN
	//vv("handleAlterCircuit: POWERON %v, wiping queues, going %v -> HEALTHY", simnode.state, simnode.name)
	simnode.powerOff = false

	// leave these as-is, to allow tests to manipulate the
	// network before starting a node.
	//simnode.readQ.deleteAll()
	//simnode.preArrQ.deleteAll()
	//simnode.timerQ.deleteAll()
	//simnode.deafReadQ.deleteAll()
	// leave droppedSendQ, useful to simulate a slooow network.
	return
}

func (s *Simnet) isolateSimnode(simnode *simnode) (undo Alteration) {
	//vv("handleAlterCircuit: from %v -> ISOLATED %v, wiping pre-arrival, block any future pre-arrivals", simnode.state, simnode.name)
	switch simnode.state {
	case ISOLATED, FAULTY_ISOLATED:
		// already there
		return UNDEFINED
	case FAULTY:
		simnode.state = FAULTY_ISOLATED
		undo = UNISOLATE
	case HEALTHY:
		simnode.state = ISOLATED
		undo = UNISOLATE
	}

	// now we allow reads to stay in readQ, and put
	// any sends corresponding to deafReads into the
	// reader-side deafReadQ.
	//s.transferReadsQ_to_deafReadsQ(simnode)
	s.transferPreArrQ_to_droppedSendQ(simnode)

	return
}

// make all current reads deaf.
func (s *Simnet) transferReadsQ_to_deafReadsQ(simnode *simnode) {

	it := simnode.readQ.Tree.Min()
	for !it.Limit() {
		read := it.Item().(*mop)
		simnode.deafReadQ.add(read)
		it = it.Next()
	}
	simnode.readQ.deleteAll()
}

// transferDeafReadsQ_to_readsQ models
// network repair that unisolates a node.
//
// The state of all nodes in the network should
// be accurate before calling here. We will
// only un-deafen reads that are now
// possible between connected nodes.
//
// Deaf reads at origin can hear again.
// If target == nil, we move all of origin.deafReadsQ to origin.readQ.
// If target != nil, only those reads from target
// will hear again, and deafReads from other targets
// are still deaf.
func (s *Simnet) transferDeafReadsQ_to_readsQ(origin, target *simnode) {

	it := origin.deafReadQ.Tree.Min()

	for !it.Limit() {
		read := it.Item().(*mop)
		if target == nil || target == read.target {

			if s.statewiseConnected(read.origin, read.target) {
				origin.readQ.add(read)
				delit := it
				it = it.Next()
				origin.deafReadQ.Tree.DeleteWithIterator(delit)
				continue
			}
		}
		it = it.Next()
	}
}

// transferPreArrQ_to_droppedSendQ models a network isolation or
// network card going down. move pending arrivals into at
// simnode back into their origin's droppedSendQ (not simnode's).
// This makes it easier to understand the dropped sends by
// inspecting the queues, and also easier to re-send them even if
// the target has since been power cycled.
func (s *Simnet) transferPreArrQ_to_droppedSendQ(simnode *simnode) {
	for it := simnode.preArrQ.Tree.Min(); !it.Limit(); it = it.Next() {
		send := it.Item().(*mop)
		// not: simnode.droppedSendQ.add(send)
		// but back on the origin:
		send.origin.droppedSendQ.add(send)
	}
	simnode.preArrQ.deleteAll()
}

// dramatic network fault simulation: now deliver all "lost"
// messages sitting in the droppedSendQ for origin, to
// each message target's preArrQ. If target is nil we
// do this for all targets. If the origin and the
// target are not now statewiseConnected, the send
// will not be recovered and will stay in the origin.droppedSendQ.
// Since we always respect isolation state, the user
// should unIsolate nodes first if they want to
// hit them with time-warped (previously dropped) messsages.
func (s *Simnet) timeWarp_transferDroppedSendQ_to_PreArrQ(origin, target *simnode) {

	it := origin.droppedSendQ.Tree.Min()
	for !it.Limit() {
		send := it.Item().(*mop)
		if target == nil || target == send.target {
			// deliver to the target, if we are now connected.
			if s.statewiseConnected(send.origin, send.target) {
				send.target.preArrQ.add(send)
				delit := it
				it = it.Next()
				origin.droppedSendQ.Tree.DeleteWithIterator(delit)
				continue
			}
		}
		it = it.Next()
	}
}

func (s *Simnet) timeWarp_transferDeafReadQsends_to_PreArrQ(origin, target *simnode) {

	for remote := range s.circuits.get(origin).all() {
		if target != nil && target != remote {
			continue
		}
		it := remote.deafReadQ.Tree.Min()
		for !it.Limit() {
			send := it.Item().(*mop)
			// might also be storing some deaf reads, so only look at sends.
			if send.kind != SEND {
				it = it.Next()
				continue
			}
			// sanity check
			if send.target != remote {
				panic(fmt.Sprintf("huh? this send ('%v') was in remotes deafReadQ, should only be also from remotes preArrQ?? '%v'", send, remote))
			}
			// deliver to the target, if we are now connected.
			if s.statewiseConnected(send.origin, send.target) {
				remote.preArrQ.add(send)
				delit := it
				it = it.Next()
				remote.deafReadQ.Tree.DeleteWithIterator(delit)

				// already advanced it, avoid double advance below
				continue
			}
			it = it.Next()
		}
	} // end for range over all remote
}

func (s *Simnet) markFaulty(simnode *simnode) (was Faultstate) {
	was = simnode.state

	switch simnode.state {
	case FAULTY_ISOLATED:
		// no-op.
	case FAULTY:
		// no-op
	case ISOLATED:
		simnode.state = FAULTY_ISOLATED
	case HEALTHY:
		simnode.state = FAULTY
	}
	return
}

func (s *Simnet) markNotFaulty(simnode *simnode) (was Faultstate) {
	was = simnode.state

	switch simnode.state {
	case FAULTY_ISOLATED:
		simnode.state = ISOLATED
	case FAULTY:
		simnode.state = HEALTHY
	case ISOLATED:
		// no-op
	case HEALTHY:
		// no-op
	}
	return
}

func (s *Simnet) unIsolateSimnode(simnode *simnode) (undo Alteration) {
	was := simnode.state
	_ = was
	//defer vv("handleAlterCircuit: UNISOLATE %v, went from %v -> %v", simnode.name, was, simnode.state)
	switch simnode.state {
	case ISOLATED:
		simnode.state = HEALTHY
		undo = ISOLATE
	case FAULTY_ISOLATED:
		simnode.state = FAULTY
		undo = ISOLATE
	case FAULTY:
		// not isolated already
		undo = UNDEFINED
		return
	case HEALTHY:
		// not isolated already
		undo = UNDEFINED
		return
	}
	// user will issue separate deliverDroppedSends flag
	// on a fault/repair if they want to deliver "timewarped" lost
	// messages from simnode.droppedSendQ. Leave it alone here.
	//s.timeWarp_transferDroppedSendQ_to_PreArrQ(origin, target)

	// have to bring back the reads that went deaf during isolation.
	// nil target means from all read targets.
	// two choices here:
	// 1) does everything:
	//s.transferDeafReadsQ_to_readsQ(simnode, nil)
	// 2) leaves probabilistic faulty conn alone:
	s.equilibrateReads(simnode, nil)

	// same needed on each connection to simnode
	for remote := range s.circuits.get(simnode).all() {
		// 1) does everything:
		//    s.transferDeafReadsQ_to_readsQ(remote, simnode)
		// vs 2) leaves probabilistically faulty conn alone:
		s.equilibrateReads(remote, simnode)
	}

	return
}

func (s *Simnet) handleAlterCircuit(altop *mop, closeDone bool) (undo Alteration) {

	var alt *simnodeAlteration
	switch altop.kind {
	case ALTER_NODE:
		alt = altop.alterNode
	case ALTER_HOST:
		alt = altop.alterHost
	}

	defer func() {
		if closeDone {
			alt.undo = undo
			s.fin(altop)
			close(alt.proceed)
			return
		}
		// else we are a part of a larger host
		// alteration set, don't close proceed
		// prematurely.
	}()

	simnode, ok := s.dns[alt.simnodeName]
	if !ok {
		alt.err = fmt.Errorf("error: handleAlterCircuit could not find simnodeName '%v' in dns: '%v'", alt.simnodeName, s.dns)
		return
	}

	switch alt.alter {
	case UNDEFINED:
		undo = UNDEFINED // idempotent
	case SHUTDOWN:
		undo = s.shutdownSimnode(simnode)
	case POWERON:
		undo = s.powerOnSimnode(simnode)
	case ISOLATE:
		undo = s.isolateSimnode(simnode)
	case UNISOLATE:
		undo = s.unIsolateSimnode(simnode)
	}
	return
}

func (s *Simnet) reverse(alt Alteration) (undo Alteration) {
	switch alt {
	case UNDEFINED:
		undo = UNDEFINED // idemopotent
	case ISOLATE:
		undo = UNISOLATE
	case UNISOLATE:
		undo = ISOLATE
	case SHUTDOWN:
		undo = POWERON
	case POWERON:
		undo = SHUTDOWN
	default:
		panic(fmt.Sprintf("unknown Alteration %v in reverse()", int(alt)))
	}
	return
}

// alter all the auto-cli of a server and the server itself.
func (s *Simnet) handleAlterHost(altop *mop) (undo Alteration) {

	var alt *simnodeAlteration = altop.alterHost

	node, ok := s.dns[alt.simnodeName]
	if !ok {
		alt.err = fmt.Errorf("error: handleAlterHost could not find simnodeName '%v' in dns: '%v'", alt.simnodeName, s.dns)
		return
	}

	undo = s.reverse(alt.alter)

	// alter all auto-cli and the peer's server.
	// note that s.locals(node) now returns a single
	// node map for lone clients, so this works for them too.
	const closeDone_NO = false
	for node := range s.locals(node) { // includes srvnode itself
		alt.simnodeName = node.name
		// notice that we reuse alt, but set the final undo
		// based on the host level state seen in the above reverse.

		_ = s.handleAlterCircuit(altop, closeDone_NO)
	}
	alt.undo = undo
	s.fin(altop)
	close(alt.proceed)
	return
}

func (s *Simnet) localDeafReadProb(read *mop) float64 {
	return s.circuits.get(read.origin).get(read.target).deafRead
}

// insight: deaf reads must create dropped sends for the sender.
// or the moral equivalent, maybe a lost message bucket.
// the send can't keep coming back if the read is deaf; else
// its like an auto-retry that will eventually get through.
func (s *Simnet) localDeafRead(read, send *mop) (isDeaf bool) {
	//vv("localDeafRead called by '%v'", fileLine(2))
	//defer func() {
	//	vv("defer localDeafRead returning %v, called by '%v', read='%v'", isDeaf, fileLine(3), read)
	//}()

	// get the local (read) origin conn probability of deafness
	// note: not the remote's deafness, only local.
	conn := s.circuits.get(read.origin).get(read.target)
	prob := conn.deafRead

	conn.attemptedRead++ // at least 1.
	read.readAttempt++

	if prob == 0 {
		//vv("localDeafRead top: prob = 0, not deaf for sure: read='%v'; send='%v'", read, send)
	} else {
		random01 := s.scenario.rng.Float64() // in [0, 1)

		//vv("localDeafRead top: random01 = %v < prob(%v) = %v ; read='%v'; send='%v'", random01, prob, random01 < prob, read, send) // not seen 1002
		isDeaf = random01 < prob
	}

	if isDeaf {
		conn.attemptedReadDeaf++
	} else {
		conn.attemptedReadOK++
	}
	return
}

func (s *Simnet) deaf(prob float64) bool {
	if prob <= 0 {
		return false
	}
	if prob >= 1 {
		return true
	}
	random01 := s.scenario.rng.Float64() // in [0, 1)
	return random01 < prob
}

func (s *Simnet) dropped(prob float64) bool {
	if prob <= 0 {
		return false
	}
	if prob >= 1 {
		return true
	}
	random01 := s.scenario.rng.Float64() // in [0, 1)
	return random01 < prob
}

//func (s *Simnet) localDropSendProb(send *mop) float64 {
//	return s.circuits[send.origin][send.target].dropSend
//}

func (s *Simnet) localDropSend(send *mop) (isDropped bool, connAttemptedSend int64) {
	// get the local origin conn probability of drop

	conn := s.circuits.get(send.origin).get(send.target)
	prob := conn.dropSend

	conn.attemptedSend++ // at least 1.
	connAttemptedSend = conn.attemptedSend
	freq := float64(conn.attemptedSendDropped) / float64(conn.attemptedSend)
	isDropped = freq < prob

	// trials are not independent, for prob to
	// converge, must be multiplied by number of attempts.
	send.sendAttempt++
	//isDropped2 := s.dropped(prob * float64(send.sendAttempt))

	//isDropped = s.dropped(prob / float64(send.origin.attemptedSend))
	//vv("localDropSend: prob=%v; freq=%v, isDropped=%v; conn.attemptedSend=%v; conn.attemptedSendDropped=%v; isDropped2=%v; prob*float64(send.sendAttempt)=%v; send.sendAttempt=%v", prob, freq, isDropped, conn.attemptedSend, conn.attemptedSendDropped, isDropped2, prob*float64(send.sendAttempt), send.sendAttempt)

	if isDropped {
		conn.attemptedSendDropped++
	} else {
		send.sendDropFilteringApplied = true // don't apply prob again!
		conn.attemptedSendOK++
	}
	return
}

// ignores FAULTY, check that with localDropSend if need be.
func (s *Simnet) statewiseConnected(origin, target *simnode) bool {
	if origin.powerOff ||
		target.powerOff {
		return false
	}
	switch origin.state {
	case ISOLATED, FAULTY_ISOLATED:
		return false
	}
	switch target.state {
	case ISOLATED, FAULTY_ISOLATED:
		return false
	}
	return true
}

func (s *Simnet) handleSend(send *mop, limit, loopi int64) (changed int64) {
	now := time.Now()
	//vv("top of handleSend(send = '%v')", send)
	defer func() {
		s.fin(send)
		close(send.proceed)
	}()

	origin := send.origin
	send.senderLC = origin.lc
	send.originLC = origin.lc

	send.unmaskedSendArrivalTm = send.initTm.Add(s.scenario.rngHop())

	// make sure send happens before receive by doing
	// this first.
	send.completeTm = userMaskTime(now, send.who) // send complete on the sender side.
	// handleSend
	send.arrivalTm = userMaskTime(send.unmaskedSendArrivalTm, send.who)
	//vv("send.arrivalTm = '%v'", send.arrivalTm)
	// note that read matching time will be unique based on
	// send arrival time.

	probDrop := s.circuits.get(send.origin).get(send.target).dropSend
	if !s.statewiseConnected(send.origin, send.target) ||
		probDrop >= 1 { // s.localDropSend(send) {

		changed++
		limit--
		//vv("handleSend DROP SEND %v", send)
		send.origin.droppedSendQ.add(send)
		return
	}
	send.target.preArrQ.add(send)
	//vv("handleSend SEND send = %v", send)
	////zz("LC:%v  SEND TO %v %v    srvPreArrQ: '%v'", origin.lc, origin.name, send, s.srvnode.preArrQ)

	if false { // we put back in the above close(proceed) for now
		// Do we need to s.nextUniqTm() when we close(send.proceed)?
		// I think we do want to queue up the send.proceed into
		// the meq and do it at a unique time we just assigned to
		// send.completeTm.
		closer := &mop{
			// completeTm is what tm() refers to for PROCEED.
			completeTm: userMaskTime(send.completeTm, send.who),
			kind:       PROCEED,
			proceedMop: send,
			sn:         s.simnetNextMopSn(),
			proceed:    send.proceed,
			reqtm:      now,
			who:        send.who,
		}
		s.add2meq(closer, -1)
	}
	// delta := s.dispatch(send.target, now, limit, loopi) // needed?
	// changed += delta
	// limit -= delta

	// just this? no, the meq is what called us.
	// s.add2meq(send, -1)

	if false {
		// allow delivery between scheduling quantum...?
		// copied from dispatchSendsReads but not deployed
		// dur := send.arrivalTm.Sub(now)
		// pending := s.newTimerCreateMop(simnode.isCli)
		// pending.origin = simnode
		// pending.timerDur = dur
		// pending.initTm = now
		// pending.completeTm = now.Add(dur)
		// pending.timerFileLine = fileLine(1)
		// pending.internalPendingTimer = true
		// send.internalPendingTimerForSend = pending

		// vv("dur=%v, queue new timer (sn:%v) for delivery of send in that has not happened (arrivalTm: %v): '%v'", dur, pending.sn, nice9(send.arrivalTm), send)
		// s.handleTimer(pending)

		//changes++
		//limit--
		//return
	}

	return
}

// note that reads that fail with a probability
// have to result in the corresponding send
// also failing, or else the send is "auto-retrying"
// forever until it gets through!?!
func (s *Simnet) handleRead(read *mop, limit, loopi int64) (changed int64) {
	//vv("top of handleRead(read = '%v')", read)
	// don't want this! only when read matches with send!
	//defer close(read.proceed)

	if s.halt.ReqStop.IsClosed() {
		read.err = ErrShutdown()
		s.fin(read)
		close(read.proceed)
		return
	}

	origin := read.origin
	read.originLC = origin.lc

	probDeaf := s.circuits.get(read.origin).get(read.target).deafRead
	if !s.statewiseConnected(read.origin, read.target) ||
		probDeaf >= 1 {

		//vv("handleRead: DEAF READ %v BUT LEAVING it in the readQ so we can possibly make this same read viable in the future", read)
		//origin.deafReadQ.add(read) // I think we do want this...
		origin.readQ.add(read) // is this really what we want now?
	} else {
		origin.readQ.add(read)
	}
	changed++
	limit--
	//vv("LC:%v  READ at %v: %v", origin.lc, origin.name, read)
	////zz("LC:%v  READ %v at %v, now cliReadQ: '%v'", origin.lc, origin.name, read, origin.readQ)

	//now := time.Now()
	//delta := s.dispatch(origin, now, limit, loopi) // needed?
	//changed += delta
	//limit -= delta
	return
}

func (simnode *simnode) firstPreArrivalTimeLTE(now time.Time) bool {

	preIt := simnode.preArrQ.Tree.Min()
	if preIt.Limit() {
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
// afterwards. We don't worry about powerOff b/c
// when set it deletes all timers.
func (s *Simnet) dispatchTimers(simnode *simnode, now time.Time, limit, loopi int64) (changes int64) {
	if limit == 0 {
		return
	}
	if simnode.timerQ.Tree.Len() == 0 {
		return
	}

	var reQtimer []*mop
	defer func() {
		for _, timer := range reQtimer {
			vv("defer dispatchTimer re-queuing undeliverable timer: %v", timer)
			simnode.timerQ.add(timer)
		}
	}()

	timerIt := simnode.timerQ.Tree.Min()
	for !timerIt.Limit() { // advance, and delete behind below

		if limit == 0 {
			return
		}

		timer := timerIt.Item().(*mop)
		//vv("check TIMER: %v", timer)

		if lte(timer.completeTm, now) {
			// timer.completeTm <= now

			if !timer.isGridStepTimer && !timer.internalPendingTimer {
				//vv("have TIMER firing: '%v'; report = %v", timer, s.schedulerReport())
			}
			changes++
			limit--
			if timer.timerFiredTm.IsZero() {
				// only mark the first firing
				timer.timerFiredTm = now
				s.fin(timer)
			} else {
				timer.timerReseenCount++
				if timer.timerReseenCount > 5 {
					panic(fmt.Sprintf("why was timerReseenCount > 5 ? '%v'", timer))
				}
			}
			// advance, and delete behind us
			delmeIt := timerIt
			timerIt = timerIt.Next()
			simnode.timerQ.Tree.DeleteWithIterator(delmeIt)

			if timer.internalPendingTimer {
				// this was our own, just discard!
			} else {
				// user timer

				if false {
					// deliver with the meq
					closer := &mop{
						// completeTm is what tm() refers to for PROCEED.
						completeTm: userMaskTime(timer.completeTm, timer.who),
						kind:       TIMER_FIRES,
						proceedMop: timer,
						sn:         s.simnetNextMopSn(),
						//proceed:    send.proceed,
						reqtm: timer.reqtm,
						who:   timer.who,
					}
					s.add2meq(closer, loopi)
					continue
				}

				tC := timer.timerC.Value()
				if tC == nil {
					vv("weak pointer to timer already collected: %v", timer)
					// user code does not have any references
					// so we don't bother to try and fire it.

					// this means we already tried once and below we
					// could not deliver, so we deleted our timerCstrong
					// strong reference. Then we are trying at most 2x.
					// TODO: do we need more than 2x?
					continue
				}
				select {
				//case timer.timerC <- now:
				case *tC <- now:
				case <-simnode.net.halt.ReqStop.Chan:
					return
					// inherently race wrt shutdown though, right?
				default:
					vv("arg! could not deliver timer? '%v'  requeue or what? ...assume this was just a shutdown race...", timer)
					reQtimer = append(reQtimer, timer)
					// Q TODO: should we also? as client user might be
					// long gone... could do:
					//if timer.timerReseenCount > 10 {
					// timer.timerCstrong = nil
					//}
					continue
					//panic("why not deliverable? hopefully we never hit this and can just delete the backup attempt below")
				}
			}
		} else {
			// INVAR: smallest timer > now
			return
		}
	}
	return
}

// does not call armTimer.
func (s *Simnet) dispatchReadsSends(simnode *simnode, now time.Time, limit, loopi int64) (changes int64) {
	defer func() {
		//vv("=== end of dispatch %v", simnode.name)
	}()
	if limit == 0 {
		return
	}

	//vv("dispatchReadsSends top: %v", simnode.net.schedulerReport())

	nR := simnode.readQ.Tree.Len()   // number of reads
	nS := simnode.preArrQ.Tree.Len() // number of sends

	if nR == 0 && nS == 0 {
		return
	}

	readIt := simnode.readQ.Tree.Min()
	preIt := simnode.preArrQ.Tree.Min()

	// matching reads and sends
	for {
		if limit == 0 {
			return
		}
		if readIt.Limit() {
			// no reads, no point.
			return
		}
		if preIt.Limit() {
			// no sends to match with reads
			return
		}

		read := readIt.Item().(*mop)
		send := preIt.Item().(*mop)

		//vv("eval match: read = '%v'; connected = %v; s.localDeafRead(read)=%v", read, s.statewiseConnected(read.origin, read.target), s.localDeafRead(read))
		//vv("eval match: send = '%v'; connected = %v; s.localDropSend(send)=%v", send, s.statewiseConnected(send.origin, send.target), s.localDropSend(send))

		simnode.optionallyApplyChaos()

		// TODO Q: should we wait to drop-or-not until arrival time comes?

		var connAttemptedSend int64 // for logging below
		var drop bool
		// insist on !sendDropFilteringApplied, so we only
		// call localDropSend once per send, and let
		// them through if they've already survived
		// one roll of the dice. localDropSend increments it.
		if send.sendDropFilteringApplied {
			// don't double filter! dispatchAll calls in here alot.
		} else {
			drop, connAttemptedSend = s.localDropSend(send)
			_ = connAttemptedSend
			if drop {
				send.origin.droppedSendDueToProb++
			}
			connected := s.statewiseConnected(send.origin, send.target)
			if !connected || drop {

				//vv("dispatchReadsSends DROP SEND %v; drop=%v; send.origin.droppedSendDueToProb=%v, connected=%v", send, drop, send.origin.droppedSendDueToProb, connected)
				// note that the dropee is stored on the send.origin
				// in the droppedSendQ, which is never the same
				// as simnode here which supplied from its preArrQ.
				send.origin.droppedSendQ.add(send)
				delit := preIt
				preIt = preIt.Next()
				simnode.preArrQ.Tree.DeleteWithIterator(delit)
				// cleanup the timer that scheduled this send, if any.
				if send.internalPendingTimerForSend != nil {
					send.origin.timerQ.del(send.internalPendingTimerForSend)
					s.fin(send.internalPendingTimerForSend)
				}
				changes++
				limit--
				continue
			}
		} // end else !send.sendDropFilteringApplied

		if send.arrivalTm.After(now) {
			// send has not arrived yet.

			// are we done? since preArrQ is ordered
			// by arrivalTm, all subsequent pre-arrivals (sends)
			// will have even more _greater_ arrivalTm;
			// so no point in looking.

			// super noisy!
			//vv("dispatch: %v", simnode.net.schedulerReport())

			// the send is still in the send queue.
			// problem with a timer is we set a new one
			// each time through here!?! we don't need 10 timers
			// for each millisecond between now and delivery in 10msec.
			// If we need a timer, we should be setting just
			// one when it hits to preArrQ
			// in handleSend. The only reason we would need a timer
			// would be to deliver before a scheduling time quantum
			// is up.

			if false { // atgrs without this. 2025 June 07. 2b8022e.
				// we must set a timer on its delivery then...
				dur := send.arrivalTm.Sub(now)
				pending := s.newTimerCreateMop(simnode.isCli)
				pending.origin = simnode
				pending.timerDur = dur
				pending.initTm = now
				pending.completeTm = now.Add(dur)
				pending.timerFileLine = fileLine(1)
				pending.internalPendingTimer = true
				send.internalPendingTimerForSend = pending

				vv("dur=%v, queue new timer (sn:%v) for delivery of send in that has not happened (arrivalTm: %v): '%v'", dur, pending.sn, nice9(send.arrivalTm), send)
				s.handleTimer(pending)

				changes++
				limit--
			}
			return
		}
		// INVAR: this send.arrivalTm <= now

		//vv("send okay to deliver (below deaf read might drop it though): conn.attemptedSend=%v, send.sendAttempt=%v; send.sendDropFilteringApplied=%v", connAttemptedSend, send.sendAttempt, send.sendDropFilteringApplied)
		// INVAR: send is okay to deliver wrt faults.

		connectedR := s.statewiseConnected(read.origin, read.target)
		if !connectedR || s.localDeafRead(read, send) {
			//vv("dispatchReadsSends DEAF READ %v; connectedR=%v", read, connectedR)

			// why would we just skip past this read? It doesn't fail
			// back to the client; its like a silent network problem.
			// if it is flaky, it might work the next time.
			// but should other reads get a chance? typically only
			// one open at a time.
			//readIt = readIt.Next()

			// 2) but we actually have to drop _the send_ on a deaf read,
			// otherwise the send is "auto-retried" until success.

			// classic, above, from send drop prob
			//send.origin.droppedSendQ.add(send)

			// different, for read deaf -> send drop.
			//vv("adding the _send_ of a deaf read into the reader side deaf Q, and deleting it from the preArrQ: '%v'\n node before: '%v'", send, simnode)
			// simnode is the reader, so target of the send from
			// retreived from its own preArrQ.
			simnode.deafReadQ.add(send)

			delit := preIt
			preIt = preIt.Next()
			simnode.preArrQ.Tree.DeleteWithIterator(delit)
			// cleanup the timer that scheduled this send, if any.
			if send.internalPendingTimerForSend != nil {
				send.origin.timerQ.del(send.internalPendingTimerForSend)
			}

			//vv("node after adding deaf-read-send to the deafReadQ: '%v'", simnode)
			changes++
			limit--
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
		// INVAR: this send.arrivalTm <= now
		changes++
		limit--

		// INVAR: this send.arrivalTm <= now; good to deliver.

		// Since both have happened, they can be matched.

		// Service this read with this send.

		read.msg = send.msg // safe b/c already copied in handleSend()

		read.isEOF_RST = send.isEOF_RST // convey EOF/RST
		if send.isEOF_RST {
			//vv("copied EOF marker from send '%v' \n to read: '%v'", send, read)
		}

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
		//vv("[1]matchmaking: \nsend '%v' -> \nread '%v' \nread.sn=%v, readAttempt=%v, read.lastP=%v, lastIsDeafTrueTm=%v", send, read, read.sn, read.readAttempt, read.lastP, nice(read.lastIsDeafTrueTm))
		read.sendmop = send
		send.readmop = read

		// advance, and delete behind. on both.
		delit := preIt
		preIt = preIt.Next()
		simnode.preArrQ.Tree.DeleteWithIterator(delit)

		delit = readIt
		readIt = readIt.Next()
		simnode.readQ.Tree.DeleteWithIterator(delit)

		// cleanup the timer that scheduled this send, if any.
		if send.internalPendingTimerForSend != nil {
			send.origin.timerQ.del(send.internalPendingTimerForSend)
		}

		s.fin(read)
		close(read.proceed)
		// send already closed in handleSend()

	} // end for
}

// dispatch delivers sends to reads, and fires timers.
// It no longer calls simnode.net.armTimer().
func (s *Simnet) dispatch(simnode *simnode, now time.Time, limit, loopi int64) (changes int64) {
	if limit == 0 {
		return
	}
	changes += s.dispatchTimers(simnode, now, limit, loopi)
	changes += s.dispatchReadsSends(simnode, now, limit, loopi)
	return
}

func (s *Simnet) qReport() (r string) {
	i := 0
	for simnode := range s.circuits.all() {
		r += fmt.Sprintf("\n[simnode %v of %v in qReport]: \n", i+1, s.circuits.Len())
		r += simnode.String() + "\n"
		i++
	}
	return
}

func (s *Simnet) schedulerReport() (r string) {
	now := time.Now()
	r = fmt.Sprintf("lastArmToFire.After(now) = %v [%v out] %v; qReport = '%v'", s.lastArmToFire.After(now), s.lastArmToFire.Sub(now), s.lastArmToFire, s.qReport())
	sz := s.meq.Len()
	if sz == 0 {
		r += "\n empty meq\n"
		return
	}
	r += fmt.Sprintf("\n meq size %v:\n%v", sz, s.meq)
	return
}

func (s *Simnet) dispatchAll(now time.Time, limit, loopi int64) (changes int64) {
	if limit == 0 {
		return
	}
	// notice here we only use the key of s.circuits
	for simnode := range s.circuits.all() {
		if limit == 0 {
			return
		}
		delta := s.dispatch(simnode, now, limit, loopi)
		changes += delta
		limit -= delta
	}
	return
}

// does not call armTimer(), so scheduler should afterwards.
func (s *Simnet) dispatchAllTimers(now time.Time, limit, loopi int64) (changes int64) {
	if limit == 0 {
		return
	}
	for simnode := range s.circuits.all() {
		if limit == 0 {
			return
		}
		delta := s.dispatchTimers(simnode, now, limit, loopi)
		changes += delta
		limit -= delta
	}
	return
}

// does not call armTimer(), so scheduler should afterwards.
func (s *Simnet) dispatchAllReadsSends(now time.Time, limit, loopi int64) (changes int64) {
	if limit == 0 {
		return
	}
	for simnode := range s.circuits.all() {
		if limit == 0 {
			return
		}
		delta := s.dispatchReadsSends(simnode, now, limit, loopi)
		changes += delta
		limit -= delta
	}
	return
}

func (s *Simnet) tickLogicalClocks() {
	for simnode := range s.circuits.all() {
		simnode.lc++
	}
}

func (s *Simnet) Start() {
	if !s.cfg.QuietTestMode {
		if faketime {
			alwaysPrintf("simnet.Start: faketime = %v; s.barrier=%v", faketime, s.barrier)
		} else {
			alwaysPrintf("simnet.Start: faketime = %v", faketime) // barrier is irrelevant
		}
	}
	go s.scheduler()
}

// durToGridPoint:
// given the time now, return the dur to
// get us to the next grid point, which is goal.
func (s *Simnet) durToGridPoint(now time.Time, tick time.Duration) (dur time.Duration, goal time.Time) {

	// this handles both
	// a) we are on a grid point now; and
	// b) we are off a grid point and we want the next one.

	// since tick is already enforceTickDur()
	// we shouldn't need to systemMaskTime, but just in case
	// that changes... it is cheap anyway, and conveys the
	// convention in force.
	//goal = systemMaskTime(now.Add(tick).Truncate(tick))
	goal = userMaskTime(now.Add(tick).Truncate(tick), s.who)

	dur = goal.Sub(now)
	//vv("i=%v; just before dispatchAll(), durToGridPoint computed: dur=%v -> goal:%v to reset the gridTimer; tick=%v", i, dur, goal, s.scenario.tick)
	if dur <= 0 { // i.e. now <= goal
		panic(fmt.Sprintf("why was proposed sleep dur = %v <= 0 ? tick=%v; bigbang=%v; now=%v", dur, tick, s.bigbang, now))
	}
	return
}

func (s *Simnet) add2meq(op *mop, loopi int64) (armed bool) {
	//vv("i=%v, add2meq %v", loopi, op)
	// need to bump up the time... with nextUniqTm,
	// so deliveries are all at a unique time point.
	if !op.reqtm.IsZero() {
		// works fine, but non-determ?
		//op.reqtm = userMaskTime(op.reqtm, op.who)
		// experiment with:
		op.reqtm = s.nextUniqTm(op.reqtm, op.who)
	}

	s.meq.add(op)
	armed = s.armTimer(time.Now(), loopi)
	//vv("i=%v, end of add2meq. meq sz %v; armed = %v -> s.lastArmDur: %v; caller %v; op = %v\n\n meq=%v\n", loopi, s.meq.Len(), armed, s.lastArmDur, fileLine(2), op, s.meq)
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
// have unblocked simnode goroutines, so they
// are running and may now be trying
// to do network operations. The
// select below will service those
// operations, or take a time step forward
// if they are all blocked.
func (s *Simnet) scheduler() {
	//vv("scheduler is running on goro = %v", GoroNumber())

	defer func() {
		//vv("scheduler defer shutdown running on goro = %v", GoroNumber())
		s.halt.ReqStop.Close()
		s.halt.Done.Close()
		r := recover()
		if r != nil {
			alwaysPrintf("scheduler panic-ing: %v", r)
			//alwaysPrintf("scheduler panic-ing: %v", s.schedulerReport())
			panic(r)
		}
	}()
	s.who = goID()
	var nextReport time.Time

	// main scheduler loop
	now := time.Now()

	// bigbang is the "start of time" -- don't change below.
	s.bigbang = now

	var totalSleepDur time.Duration

	// not in use atm. comment out for possible delete.
	// get regular scheduler wakeups on a time
	// grid of step size s.scenario.tick.
	// We can get woken earlier too by
	// sends, reads, and timers. The nextTimer
	// logic below can decide how it wants
	// to handle that.
	// timer := newTimerCreateMop(false)
	// timer.proceed = nil          // never used, don't leak it.
	// timer.isGridStepTimer = true // should be only one.
	// timer.initTm = now
	// timer.timerDur = s.scenario.tick
	// timer.completeTm = now.Add(s.scenario.tick)
	// timer.timerFileLine = fileLine(3)
	//
	// As a special case, armTimer always includes
	// the gridStepTimer when computing the minimum
	// next timer to go off, so barrier cannot deadlock.
	// s.gridStepTimer = timer

	//restartI:
	for i := int64(0); ; i++ {

		now := time.Now()
		if gte(now, nextReport) {
			nextReport = now.Add(time.Second)
			//sz := s.meq.Len()
			//if sz > 0 {
			//vv("scheduler top with meq len %v", sz)
			//cli.lc = %v ; srv.lc = %v", clilc, srvlc)
			//vv("i=%v scheduler top. schedulerReport: \n%v", i, s.schedulerReport())
			//}
			//vv("i=%v scheduler top. ndtot=%v", i, s.ndtot)
		}

		// let goroutines get blocked waiting on the select arms below.
		if faketime && s.barrier {
			synctestWait_LetAllOtherGoroFinish() // 1st barrier // already here?? two goro: 26(group 23), goro 131(group 23)
		}
		//vv("i=%v, about to select", i)
		preSelectTm := now
		preSelMeqSz := s.meq.Len()
		_ = preSelMeqSz

		// what we want to maximize determinism:
		// single goro at time, see if we can make any
		// progress, make all the progress we can,
		// only once no further progress, then allow
		// the next goro. Try to release them with
		// a clock that corresponds to their goID.

		select { // scheduler main select

		case <-s.haveNextTimer(preSelectTm):
			now = time.Now()
			elap := now.Sub(preSelectTm)
			slept := elap > 0
			if elap == 0 {
				if faketime && s.lastArmDur > 0 {
					panic(fmt.Sprintf("why awake when elap=0 but lastArmDur=%v", s.lastArmDur))
				}
				// A timer of dur zero is equivalent to synctest.Wait;
				// so time has not advanced, but the rest of the
				// bubble is blocked now. Do we want to sleep to
				// advance time now? Only if the meq is empty,
				// otherwise try to dispatch again before advancing
				// time, with the design aim of maximizing
				// determinism and reproducibility. Released goro will
				// likely set some timers etc.
				sz := s.meq.Len()
				if sz == 0 {
					// durToGridPoint does userMaskTime for us now.
					dur, _ := s.durToGridPoint(now, s.scenario.tick)

					//vv("i=%v, elap=0 and no work, just advance time by dur='%v' and try to dispatch below.", i, dur)

					time.Sleep(dur)
					// should we barrier now? no other selects
					// are possible in here, so...pointless? But
					// might give a small increase in determinism
					// from background goro racing to meet our next
					// select call --
					// so try it if we have actually slept at all.
					slept = true
				} else {
					//vv("i=%v, elap=0 but have sz meq=%v :\n %v", i, sz, s.meq)
					// just continue? nope. we get bubble deadlock.
				}
			}
			totalSleepDur += elap
			//vv("i=%v, nextTimer fired. s.lastArmDur=%v; s.lastArmToFire = %v; elap = '%v'", i, s.lastArmDur, s.lastArmToFire, elap)
			//if elap == 0 {
			//vv("i=%v, cool: elap was 0, nice. single stepping the next goro... nextTimer fired. totalSleepDur = %v; last = %v", i, totalSleepDur, now.Sub(preSelectTm))
			//}

			// problem: if we barrier here, we
			// cannot allow other goro to wake us
			// on our select case arms, to queue up their mops.
			// BUT, we still might get a tiny increase in
			// determinism, and it should be fast anyway...
			if slept {
				if faketime && s.barrier {
					synctestWait_LetAllOtherGoroFinish() // 2nd barrier
				}
			}
			s.tickLogicalClocks()

			s.distributeMEQ(now, i)
			// end case wakeup <-s.haveNextTimer

		case batch := <-s.submitBatchCh:
			s.add2meq(batch, i)
		case timer := <-s.addTimer:
			//vv("i=%v, addTimer ->  timer='%v'", i, timer)
			//s.handleTimer(timer)
			s.add2meq(timer, i)

		case discard := <-s.discardTimerCh:
			//vv("i=%v, discardTimer ->  discard='%v'", i, discard)
			//s.handleDiscardTimer(discard)
			s.add2meq(discard, i)

		case send := <-s.msgSendCh:
			//vv("i=%v, msgSendCh ->  send='%v'", i, send)
			//s.handleSend(send)
			s.add2meq(send, i)

		case read := <-s.msgReadCh:
			//vv("i=%v msgReadCh ->  read='%v'", i, read)
			//s.handleRead(read)
			s.add2meq(read, i)

		case reg := <-s.cliRegisterCh:
			// "connect" in network lingo, client reaches out to listening server.
			//vv("i=%v, cliRegisterCh got reg from '%v' = '%v'", i, reg.client.name, reg)
			//s.handleClientRegistration(reg)
			s.add2meq(s.newClientRegMop(reg), i)
			//vv("back from handleClientRegistration for '%v'", reg.client.name)

		case srvreg := <-s.srvRegisterCh:
			// "bind/listen" on a socket, server waits for any client to "connect"
			//vv("i=%v, s.srvRegisterCh got srvreg for '%v'", i, srvreg.server.name)
			//s.handleServerRegistration(srvreg)
			// do not vv here, as it is very racey with the server who
			// has been given permission to proceed.
			s.add2meq(s.newServerRegMop(srvreg), i)

		case scenario := <-s.newScenarioCh:
			//vv("i=%v, newScenarioCh ->  scenario='%v'", i, scenario)
			s.add2meq(s.newScenarioMop(scenario), i)

		case alt := <-s.alterSimnodeCh:
			//vv("i=%v alterSimnodeCh ->  alt='%v'", i, alt)
			//s.handleAlterCircuit(alt, true)
			s.add2meq(s.newAlterNodeMop(alt), i)

		case alt := <-s.alterHostCh:
			//vv("i=%v alterHostCh ->  alt='%v'", i, alt)
			//s.handleAlterHost(op.alt)
			s.add2meq(s.newAlterHostMop(alt), i)

		case cktFault := <-s.injectCircuitFaultCh:
			//vv("i=%v injectCircuitFaultCh ->  cktFault='%v'", i, cktFault)
			//s.injectCircuitFault(cktFault, true)
			s.add2meq(s.newCktFaultMop(cktFault), i)

		case hostFault := <-s.injectHostFaultCh:
			//vv("i=%v injectHostFaultCh ->  hostFault='%v'", i, hostFault)
			//s.injectHostFault(hostFault)
			s.add2meq(s.newHostFaultMop(hostFault), i)

		case repairCkt := <-s.repairCircuitCh:
			//vv("i=%v repairCircuitCh ->  repairCkt='%v'", i, repairCkt)
			//s.handleCircuitRepair(repairCkt, true)
			s.add2meq(s.newRepairCktMop(repairCkt), i)

		case repairHost := <-s.repairHostCh:
			//vv("i=%v repairHostCh ->  repairHost='%v'", i, repairHost)
			//s.handleHostRepair(repairHost)
			s.add2meq(s.newRepairHostMop(repairHost), i)

		case snapReq := <-s.simnetSnapshotRequestCh:
			//vv("i=%v simnetSnapshotRequestCh -> snapReq", i)
			// user can confirm/view all current faults/health
			//s.handleSimnetSnapshotRequest(snapReq, now, i)
			s.add2meq(s.newSnapReqMop(snapReq), i)

		case <-s.halt.ReqStop.Chan:
			//vv("i=%v <-s.halt.ReqStop.Chan", i)
			bb := time.Since(s.bigbang)
			pct := 100 * float64(totalSleepDur) / float64(bb)
			_ = pct
			//vv("simnet.halt.ReqStop totalSleepDur = %v (%0.2f%%) since bb = %v)", totalSleepDur, pct, bb)
			return

			/*		default:
					vv("i=%v, default: nobody else wanted to use our services.", i)
					now = time.Now()
					sz := s.meq.Len()
					var npop int
					if sz > 0 {
						npop = s.distributeMEQ(now, i)
					} // else {
					//vv("i=%v, sz=%v, just advance time and try to dispatch on next loop", i, sz)
					_ = npop
					//if npop == 0 {
					s.armTimer(now, i)
					if s.lastArmToFire.IsZero() {
						// durToGridPoint does userMaskTime for us now.
						dur, _ := s.durToGridPoint(now, s.scenario.tick)
						vv("dur = %v; tick=%v", dur, s.scenario.tick)
						time.Sleep(dur)
					}
					//}
			*/
		} // end select
		if false {
			if i > 0 && i%2000 == 0 {
				if s.ndtot > s.ndtotPrev {
					s.ndtotPrev = s.ndtot
				} else {
					vv("stalled? i=%v no new dispatches in last 2000 iterataioins... bottom of scheduler loop. since bb: %v; faketime=%v", i, time.Since(s.bigbang), faketime)
					alwaysPrintf("schedulerReport %v", s.schedulerReport())
					panic("stalled?")
				}
			}
		}
	}
}

func (s *Simnet) distributeMEQ(now time.Time, i int64) (npop int) {
	//vv("i=%v, top distributeMEQ", i)
	// meq is trying for
	// more deterministic event ordering. we have
	// accumulated and held any events from the
	// the read/send/timer/discard select cases
	// into the meq during the last <-s.haveNextTimer, and
	// now we act on them.

	// do we have more than one goro sending us stuff
	// in a single tick?
	// that would imply non-determinism, but sadly
	// this happens for sure on startup though, when
	// cli/srv spin up their read loops/send loops.
	// Once we hit steady state we could try to
	// get more determinism, but start up is inherently
	// parallel at the moment.
	who := make(map[int]bool)

	// if we don't process all the meq and assign
	// them timepoints in the future to run, then
	// the subsequent queued events might get to run first?
	var op *mop
	for {
		top := s.meq.peek()
		if top == nil {
			break
		}
		if top.tm().After(now) {
			break
		}
		op = s.meq.pop()
		npop++
		who[op.who] = true

		switch op.kind {
		case PROCEED: // not currently used.
			vv("PROCEED releasing op.completeTm = %v", op.completeTm)
			s.fin(op)
			close(op.proceed)

		case TIMER_FIRES: // not currently used.
			vv("TIMER_FIRES: %v", op)
			tC := op.proceedMop.timerC.Value()
			if tC != nil {
				select {
				//case op.proceedMop.timerC <- now: // might need to make buffered?
				case *tC <- now: // might need to make buffered?
					vv("TIMER_FIRES delivered to timer: %v", op.proceedMop)
				case <-s.halt.ReqStop.Chan:
					vv("i=%v <-s.halt.ReqStop.Chan", i)
					return
				}
			}
			// let op be GC-ed.

		case TIMER:
			//vv("i=%v, meq sees timer='%v'", i, op)
			s.handleTimer(op)

		case TIMER_DISCARD:
			//vv("i=%v, meq sees discard='%v'", i, op)
			s.handleDiscardTimer(op)

		case SEND:
			//vv("i=%v, meq sees send='%v'", i, op)
			s.handleSend(op, 1, i)

		case READ:
			//vv("i=%v meq sees read='%v'", i, op)
			s.handleRead(op, 1, i)
		case SNAPSHOT:
			s.handleSimnetSnapshotRequest(op, now, i)
		case CLIENT_REG:
			s.handleClientRegistration(op)
		case SERVER_REG:
			s.handleServerRegistration(op)

		case SCENARIO:
			s.finishScenario()
			s.initScenario(op.scen)
			i = 0
			panic("TODO, impossible to restart network???")
			//continue restartI

		case FAULT_CKT:
			s.injectCircuitFault(op, true)
		case FAULT_HOST:
			s.injectHostFault(op)
		case REPAIR_CKT:
			s.handleCircuitRepair(op, true)
		case REPAIR_HOST:
			s.handleHostRepair(op)
		case ALTER_HOST:
			s.handleAlterHost(op)
		case ALTER_NODE:
			s.handleAlterCircuit(op, true)
		case BATCH:
			s.handleBatch(op)
		default:
			panic(fmt.Sprintf("why in our meq this mop kind? '%v'", int(op.kind)))
		}
	} // end for

	//vv("i=%v; who count = %v", i, len(who))
	//if len(who) > 1 {
	//	panic("arg, non-determinism with two callers?!")
	//}
	// limit of -1 means no limit on number of dispatched mop.
	//nd0 += s.dispatchAll(now, -1, i)
	nd := s.dispatchAll(now, -1, i)
	_ = nd
	s.ndtot += nd

	//s.refreshGridStepTimer(now) // not really used atm.
	armed := s.armTimer(now, i)
	_ = armed

	if !armed && nd == 0 && npop == 0 {
		// can see alot of this while waiting for stuff
		// e.g. a first leader election. Let user test code
		// activate it with a simnet.NoisyNothing call.
		if s.noisyNothing.Load() {
			alwaysPrintf("timer not armed, simnet npop=0 and num dispatched = 0, quiescent?")
		}
	}
	return
}

// If we don't have a next timer, do
// s.nextTimer.Reset(0) to get a synctest.Wait effect.
// https://github.com/golang/go/issues/73876#issuecomment-2920758263
func (s *Simnet) haveNextTimer(now time.Time) <-chan time.Time {
	if s.lastArmToFire.IsZero() {
		//s.nextTimer.Reset(0) // faketime: 5.9s, 5.53s, 5.48 sec. realtime: 7.333s, 7.252s, 7.262s, 7.4s
		dur, _ := s.durToGridPoint(now, s.scenario.tick) // 5.589s, 5.3s, 5.54s, 5.55sec faketime. realtime 7.411s, 7.02s, 7.1s
		s.nextTimer.Reset(dur)
		if dur == 0 {
			vv("dur was 0 !!!") // never seen. good.
		}
		//vv("haveNextTimer: no timer at the moment, don't wait on it.")
		//return nil
	}
	//vv("haveNextTimer: s.lastArmToFire = %v; s.lastArmDur = %v", s.lastArmToFire, s.lastArmDur)
	return s.nextTimer.C
}

func (s *Simnet) handleBatch(batchop *mop) {
	// submit now?

	var batch *SimnetBatch = batchop.batch
	_ = batch

	// else set timer for when to submit
	panic("TODO handleBatch")
}

func (s *Simnet) finishScenario() {
	// do any tear down work...

	// at the end
	s.scenario = nil
}
func (s *Simnet) initScenario(scenario *scenario) {
	s.scenario = scenario
	// do any init work...
}

func (s *Simnet) handleDiscardTimer(discard *mop) {
	//now := time.Now()

	//if discard.origin.powerOff {
	// cannot set/fire timers when halted. Hmm.
	// This must be a stray...maybe a race? the
	// simnode really should not be doing anything.
	//alwaysPrintf("yuck: got a TIMER_DISCARD from a powerOff simnode: '%v'", discard.origin)

	// probably just a shutdown race, don't deadlock them.
	// but also! cleanup the timer below to GC it, still!

	//s.fin(discard)
	//close(discard.proceed)
	//return
	//}

	discard.handleDiscardCalled = true
	orig := discard.origTimerMop

	found := discard.origin.timerQ.del(discard.origTimerMop)
	if found {
		discard.wasArmed = !orig.timerFiredTm.IsZero()
		discard.origTimerCompleteTm = orig.completeTm
	} // leave wasArmed false, could not have been armed if gone.

	////zz("LC:%v %v TIMER_DISCARD %v to fire at '%v'; now timerQ: '%v'", discard.origin.lc, discard.origin.name, discard, discard.origTimerCompleteTm, s.clinode.timerQ)
	s.fin(discard)
	close(discard.proceed)
}

func (s *Simnet) handleTimer(timer *mop) {
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
		s.fin(timer)
		close(timer.proceed) // likely shutdown race, don't deadlock them.
		return
	}

	lc := timer.origin.lc
	who := timer.origin.name
	_, _ = who, lc
	//vv("handleTimer() %v  TIMER SET: %v", who, timer) // not seen!?!

	timer.senderLC = lc
	timer.originLC = lc
	timer.timerCstrong = make(chan time.Time)
	timer.timerC = weak.Make(&timer.timerCstrong)
	defer func() {
		// not yet! s.fin(timer)
		close(timer.proceed)
	}()

	// mask it up!
	timer.unmaskedCompleteTm = timer.completeTm
	timer.unmaskedDur = timer.timerDur
	timer.completeTm = userMaskTime(timer.completeTm, timer.who) // handle timer
	timer.timerDur = timer.completeTm.Sub(timer.initTm)
	//vv("masked timer(sn %v):\n dur: %v -> %v\n completeTm: %v -> %v\n timer.who: %v", timer.sn, timer.unmaskedDur, timer.timerDur, nice9(timer.unmaskedCompleteTm), nice9(timer.completeTm), timer.who)

	timer.origin.timerQ.add(timer)
	//vv("LC:%v %v set TIMER %v to fire at '%v'; now timerQ: '%v'", lc, timer.origin.name, timer, timer.completeTm, s.clinode.timerQ)

}

// commented for possible deletion later if we
// really don't end up needing a grid-step timer.
//
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
// func (s *Simnet) refreshGridStepTimer(now time.Time) (dur time.Duration, goal time.Time) {
// 	if gte(now, s.gridStepTimer.completeTm) {
// 		s.gridStepTimer.initTm = now
// 		dur, goal = s.durToGridPoint(now, s.scenario.tick)
// 		s.gridStepTimer.timerDur = dur
// 		s.gridStepTimer.completeTm = goal

// 		if gte(now, s.gridStepTimer.completeTm) {
// 			panic(fmt.Sprintf("durToGridPoint() gave completeTm(%v) <= now(%v); should be impossible, no? are we servicing all events in order? are we missing a wakeup? oversleeping? wat?", s.gridStepTimer.completeTm, now))
// 		}
// 	} else {
// 		//vv("refreshGridStepTimer does nothing; sees now < s.gridStepTimer.completeTm(%v)", nice(s.gridStepTimer.completeTm)) // seen alot
// 		goal = s.gridStepTimer.completeTm
// 		dur = s.gridStepTimer.completeTm.Sub(now)
// 	}
// 	return
// }

func (s *Simnet) armTimer(now time.Time, loopi int64) (armed bool) {

	// Ah! We only want to scheduler to sleep if
	// we have some work to do in the meq.
	// then while we are sleeping in the timer arm,
	// the live goro can wake us to do work
	// for them on a select branch. We should put
	// that into the meq, and after each such
	// addition, arm the timer for the min next
	// meq event.
	// does that work? should. the scheduler should "wake"
	// on select from another goro channel op. We want
	// to assign each goro time by their ID too.
	op := s.meq.peek()
	if op != nil {
		// assume this has already been uniquified in the past
		// when it entered the meq. would be hard to do here
		// since much time may have passed in the interim,
		// and we'd only end up jumping it forward by alot.
		when := op.tm()
		if when.IsZero() {
			// e.g. a send that has not yet had handleSend called on it.
			panic(fmt.Sprintf("why is when zero time? %v", op))
		}
		if !when.IsZero() {

			when2 := when

			// arg. if a mop is already masked, it should not
			// be re-masked again. lets assume that the times
			// in the meq are to be honored. any add to meq must
			// add the right tm.
			// if lte(when, now) {
			// 	when2 = userMaskTime(now, op.who)
			// } else {
			// 	when2 = userMaskTime(when, op.who)
			// }

			dur := when2.Sub(now)
			//if dur < 0 {
			//old: vv("times were not all unique, but they should be! op='%v'", op)
			//	panic(fmt.Sprintf("negative dur will collide with s.lastArmDur = -1 ?!?: %v", dur))
			//}
			if dur > s.scenario.tick {
				//dur = s.scenario.tick
				//when2 = now.Add(dur)
				//when2 = userMaskTime(now.Add(dur), s.who)
				dur, when2 = s.durToGridPoint(now, s.scenario.tick)
			}

			s.lastArmToFire = when2
			s.lastArmDur = dur
			// ouch: what happens when you reset a timer
			// to be sooner than the previous, say, 20s?
			// should be okay, since we can't be here and
			// also waiting on the timer.
			s.nextTimer.Reset(dur) // this should be the only such reset.
			//vv("i=%v, arm timer: armed. when=%v, nextTimer dur=%v; into future(when - now): %v;  op='%v'", loopi, when2, dur, when2.Sub(now), op)
			//return dur
			return true
		}
	}
	//vv("i=%v, okay, so meq is empty. hmm. tick='%v'; caller='%v'", loopi, s.scenario.tick, fileLine(2))
	//vv("okay, so meq is empty. hmm. goal: '%v'; dur='%v'; tick='%v'; caller='%v'", goal, dur, s.scenario.tick, fileLine(2))

	// tell haveNextTimer that we don't have one; it should return nil.
	s.lastArmToFire = time.Time{}
	s.lastArmDur = math.MinInt64
	s.nextTimer.Stop()
	return false
}

func (simnode *simnode) soonestTimerLessThan(bound *mop) *mop {

	//if bound != nil {
	//vv("soonestTimerLessThan(bound.completeTm = '%v'", bound.completeTm)
	//} else {
	//vv("soonestTimerLessThan(nil bound)")
	//}
	it := simnode.timerQ.Tree.Min()
	if it.Limit() {
		//vv("we have no timers, returning bound")
		return bound
	}
	minTimer := it.Item().(*mop)
	if bound == nil {
		// no lower bound yet
		//vv("we have no lower bound, returning min timer: '%v'", minTimer)
		return minTimer
	}
	if minTimer.completeTm.Before(bound.completeTm) {
		//vv("minTimer.completeTm(%v) < bound.completeTm(%v)", minTimer.completeTm, bound.completeTm)
		return minTimer
	}
	//vv("soonestTimerLessThan end: returning bound")
	return bound
}

// We assume atm that all goID are < 100_000.
// So this suffices to avoid collissions.
// We validate this assumption in userMaskTime too.
const timeMask0 = time.Microsecond * 100 // then add GoID on top.
const timeMask9 = time.Microsecond*100 - 1

// maskTime makes the last 5 digits
// of a nanosecond timestamp all 9s: 99_999
// Any digit above 100 microseconds is unchanged.
//
// This can be used to order wake from sleep/timer events.
//
// We assert before returning that both
// (newtm >= tm) and (newtm > now).
//
// If we start with
// 2006-01-02T15:04:05.000000000-07:00
// maskTime will return
// 2006-01-02T15:04:05.000099999-07:00
func userMaskTime(tm time.Time, who int) (newtm time.Time) {
	if who <= 0 {
		panic(fmt.Sprintf("who %v not set or negative!", who))
	}
	if who >= 100_000 {
		panic(fmt.Sprintf("who goID = %v > 100_000 limit. simnet currently has a max goroutine limit of 100_000.", who))
	}
	// always bump to next 100 usec, so we are
	// for sure after tm.
	now := time.Now()
	newtm = tm.Truncate(timeMask0).Add(timeMask0 + time.Duration(who))
	if newtm.Before(tm) {
		panic(fmt.Sprintf("arg. want newtm(%v) >= tm(%v)", newtm, tm))
	}
	if newtm.After(now) {
		return
	} else {
		newtm = now.Truncate(timeMask0).Add(timeMask0 + time.Duration(who))
		//if newtm.Before(now) {
		if lte(newtm, now) || newtm.Before(tm) {
			// arg. our correction did not help.
			// it should have for any who > 0, so wat?
			panic(fmt.Sprintf("arg! newtm(%v) <= now(%v) || newtm.Before(tm='%v'); who=%v", nice(newtm), nice(now), nice(tm), who))
		}
	}
	return
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

func (s *Simnet) allConnString() (r string) {

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

// in hdr/vprint
//const rfc3339MsecTz0 = "2006-01-02T15:04:05.000Z07:00"
//const rfc3339NanoNumericTZ0pad = "2006-01-02T15:04:05.000000000-07:00"

// user/tests can confirm/view all current faults/health,
// without data races inherent in just printing the simnet
// fields, by asking the simnet nicely to return a snapshot
// of the internal state of the network in a SimnetSnapshot.
func (s *Simnet) handleSimnetSnapshotRequest(reqop *mop, now time.Time, loopi int64) {
	var req *SimnetSnapshot = reqop.snapReq
	defer func() {
		s.fin(reqop)
		close(req.proceed)
	}()
	s.xmut.Lock()
	defer s.xmut.Unlock()

	req.Asof = now
	req.Loopi = loopi
	req.Cfg = *s.simNetCfg
	req.ScenarioNum = s.scenario.num
	req.ScenarioSeed = s.scenario.seed
	req.ScenarioTick = s.scenario.tick
	req.ScenarioMinHop = s.scenario.minHop
	req.ScenarioMaxHop = s.scenario.maxHop
	req.Peermap = make(map[string]*SimnetPeerStatus)

	req.Xcountsn = s.nextMopSn
	req.Xfinorder = append([]int64{}, s.xfinorder...)
	req.Xwhence = append([]string{}, s.xwhence...)
	req.Xkind = append([]mopkind{}, s.xkind...)
	req.Xissuetm = append([]time.Time{}, s.xissuetm...)
	req.Xfintm = append([]time.Time{}, s.xfintm...)
	req.Xwho = append([]int{}, s.xwho...)

	sum := s.xb3hash.Sum(nil)
	req.Xhash = "blake3.33B-" + cristalbase64.URLEncoding.EncodeToString(sum[:33])

	req.NetClosed = s.halt.ReqStop.IsClosed()
	if len(s.servers) == 0 {
		req.GetSimnetStatusErr = fmt.Errorf("no servers found in simnet; "+
			"len(allnodes)=%v", len(s.allnodes))
		return
	}
	// detect standalone cli (not auto-cli of server/peer) and report them too.
	// start with everything, delete what we see, then report on the rest.
	alone := make(map[*simnode]bool)
	for node := range s.circuits.all() {
		if node.isCli { // only consider clients, non-auto-cli ones
			if strings.HasPrefix(node.name, auto_cli_recognition_prefix) {
				// has the "auto-cli-from-" prefix, so
				// treat it as an autocli and not a lonecli.
				// Its okay if we mis-classify something in an
				// edge case that happens to share our prefix,
				// as this does not impact correctness, just test
				// convenience.
			} else {
				// possibly alone, but if we see it associated
				// with a server peer below, delete from the alone set.
				alone[node] = true
			}
		}
	}
	for _, srvnode := range valNameSort(s.servers) {
		delete(alone, srvnode)
		sps := &SimnetPeerStatus{
			Name:         srvnode.name,
			ServerState:  srvnode.state,
			Poweroff:     srvnode.powerOff,
			LC:           srvnode.lc,
			ServerBaseID: srvnode.serverBaseID,
			Connmap:      make(map[string]*SimnetConnSummary),
		}
		req.Peer = append(req.Peer, sps)
		req.Peermap[srvnode.name] = sps

		// s.locals() gives srvnode.allnode, includes server's simnode itself.
		// srvnode.autocli also available for just local cli simnode.
		for _, origin := range keyNameSort(s.locals(srvnode)) {
			delete(alone, origin)
			for target, conn := range s.circuits.get(origin).all() {
				req.PeerConnCount++
				connsum := &SimnetConnSummary{
					OriginIsCli: origin.isCli,
					// origin details
					Origin:           origin.name,
					OriginState:      origin.state,
					OriginConnClosed: conn.localClosed.IsClosed(),
					OriginPoweroff:   origin.powerOff,
					// target details
					Target:           target.name,
					TargetState:      target.state,
					TargetConnClosed: conn.remoteClosed.IsClosed(),
					TargetPoweroff:   target.powerOff,
					// specific faults on the connection
					DeafReadProb: conn.deafRead,
					DropSendProb: conn.dropSend,
					// readQ, preArrQ, etc in summary string form.
					Qs: origin.String(),
					// origin queues
					DroppedSendQ: origin.droppedSendQ.deepclone(),
					DeafReadQ:    origin.deafReadQ.deepclone(),
					ReadQ:        origin.readQ.deepclone(),
					PreArrQ:      origin.preArrQ.deepclone(),
					TimerQ:       origin.timerQ.deepclone(),
				}
				sps.Conn = append(sps.Conn, connsum)
				sps.Connmap[origin.name] = connsum
				//vv("sps.Connmap['%v'] set", origin.name)
			}
		}
		if len(alone) > 0 {
			// lone cli are not really a peer but meh.
			// not worth a separate struct type that has
			// the exact same fields. So we re-use SimnetPeerStatus
			// for lone clients too, even though the name
			// of the struct has Peer. This stuff is for
			// diagnostics and tests not correctness,
			// and uniform handling of all clients and servers
			// in tests is much easier this way.
			req.LoneCli = make(map[string]*SimnetPeerStatus)
		}
		for origin := range alone {
			if strings.HasPrefix(origin.name, auto_cli_recognition_prefix) {
				panic(fmt.Sprintf("arg! logic error: we are declaring an autocli to be alone?!?! '%v'", origin))
			}
			// note, each cli can only have one target, but
			// a for-range is simply convenient here, even though
			// this loop will only iterate once. We sanity assert that below.
			for target, conn := range s.circuits.get(origin).all() {
				req.LoneCliConnCount++

				connsum := &SimnetConnSummary{
					OriginIsCli: origin.isCli,
					// origin details
					Origin:           origin.name,
					OriginState:      origin.state,
					OriginConnClosed: conn.localClosed.IsClosed(),
					OriginPoweroff:   origin.powerOff,
					// target details
					Target:           target.name,
					TargetState:      target.state,
					TargetConnClosed: conn.remoteClosed.IsClosed(),
					TargetPoweroff:   target.powerOff,
					// specific faults on the connection
					DeafReadProb: conn.deafRead,
					DropSendProb: conn.dropSend,
					// readQ, preArrQ, etc in summary string form.
					Qs: origin.String(),
					// origin queues
					DroppedSendQ: origin.droppedSendQ.deepclone(),
					DeafReadQ:    origin.deafReadQ.deepclone(),
					ReadQ:        origin.readQ.deepclone(),
					PreArrQ:      origin.preArrQ.deepclone(),
					TimerQ:       origin.timerQ.deepclone(),
				}
				// not really a peer but meh. not worth its
				// own separate struct.
				sps := &SimnetPeerStatus{
					Name:         origin.name,
					ServerState:  origin.state,
					Poweroff:     origin.powerOff,
					LC:           origin.lc,
					ServerBaseID: origin.serverBaseID,
					Conn:         []*SimnetConnSummary{connsum},
					Connmap:      map[string]*SimnetConnSummary{origin.name: connsum},
					IsLoneCli:    true,
				}
				_, impos := req.LoneCli[origin.name]
				if impos {
					panic(fmt.Sprintf("should be impossible to have more than one connection per lone cli, they are clients. origin='%v'", origin))
				}
				req.LoneCli[origin.name] = sps
			}
		} // end alone
	}
	// end handleSimnetSnapshotRequest
}

func (s *Simnet) newClientRegMop(clireg *clientRegistration) (op *mop) {
	op = &mop{
		cliReg:    clireg,
		originCli: true,
		sn:        s.simnetNextMopSn(),
		kind:      CLIENT_REG,
		proceed:   clireg.proceed,
		who:       clireg.who,
		reqtm:     clireg.reqtm,
	}
	return
}

func (s *Simnet) newServerRegMop(srvreg *serverRegistration) (op *mop) {
	op = &mop{
		srvReg:    srvreg,
		originCli: false,
		sn:        s.simnetNextMopSn(),
		kind:      SERVER_REG,
		proceed:   srvreg.proceed,
		who:       srvreg.who,
		reqtm:     srvreg.reqtm,
	}
	return
}

func (s *Simnet) newSnapReqMop(snapReq *SimnetSnapshot) (op *mop) {
	op = &mop{
		snapReq: snapReq,
		sn:      s.simnetNextMopSn(),
		kind:    SNAPSHOT,
		proceed: snapReq.proceed,
		who:     snapReq.who,
		reqtm:   snapReq.reqtm,
		where:   snapReq.where,
	}
	return
}

func (s *Simnet) newScenarioMop(scen *scenario) (op *mop) {
	op = &mop{
		scen:    scen,
		sn:      s.simnetNextMopSn(),
		kind:    SCENARIO,
		proceed: scen.proceed,
		who:     scen.who,
		reqtm:   scen.reqtm,
	}
	return
}

func (s *Simnet) newAlterNodeMop(alt *simnodeAlteration) (op *mop) {
	op = &mop{
		alterNode: alt,
		sn:        s.simnetNextMopSn(),
		kind:      ALTER_NODE,
		proceed:   alt.proceed,
		who:       alt.who,
		reqtm:     alt.reqtm,
	}
	return
}

func (s *Simnet) newAlterHostMop(alt *simnodeAlteration) (op *mop) {
	op = &mop{
		alterHost: alt,
		sn:        s.simnetNextMopSn(),
		kind:      ALTER_HOST,
		proceed:   alt.proceed,
		who:       alt.who,
		reqtm:     alt.reqtm,
	}
	return
}

func (s *Simnet) newCktFaultMop(cktFault *circuitFault) (op *mop) {
	op = &mop{
		cktFault: cktFault,
		sn:       s.simnetNextMopSn(),
		kind:     FAULT_CKT,
		proceed:  cktFault.proceed,
		who:      cktFault.who,
		reqtm:    cktFault.reqtm,
	}
	return
}

func (s *Simnet) newHostFaultMop(hostFault *hostFault) (op *mop) {
	op = &mop{
		hostFault: hostFault,
		sn:        s.simnetNextMopSn(),
		kind:      FAULT_HOST,
		proceed:   hostFault.proceed,
		who:       hostFault.who,
		reqtm:     hostFault.reqtm,
	}
	return
}

func (s *Simnet) newRepairCktMop(cktRepair *circuitRepair) (op *mop) {
	op = &mop{
		repairCkt: cktRepair,
		sn:        s.simnetNextMopSn(),
		kind:      REPAIR_CKT,
		proceed:   cktRepair.proceed,
		who:       cktRepair.who,
		reqtm:     cktRepair.reqtm,
	}
	return
}

func (s *Simnet) newRepairHostMop(hostRepair *hostRepair) (op *mop) {
	op = &mop{
		repairHost: hostRepair,
		sn:         s.simnetNextMopSn(),
		kind:       REPAIR_HOST,
		proceed:    hostRepair.proceed,
		who:        hostRepair.who,
		reqtm:      hostRepair.reqtm,
	}
	return
}

// NoisyNothing makes simnet print/log if it appears
// to have nothing to do at the end of each
// scheduling loop.
func (s *Simnet) NoisyNothing(oldval, newval bool) (swapped bool) {
	return s.noisyNothing.CompareAndSwap(oldval, newval)
}

func (s *Simnet) Close() {
	//vv("simnet.Close() called.")
	if s == nil || s.halt == nil {
		return
	}
	s.halt.ReqStop.Close()
}
