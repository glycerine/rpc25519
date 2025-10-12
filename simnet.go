package rpc25519

// build/run with:
// GOTRACEBACK=all GOEXPERIMENT=synctest go test -v

import (
	"fmt"
	"math"
	//"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"weak"

	cristalbase64 "github.com/cristalhq/base64"
	"github.com/glycerine/blake3"
	"github.com/glycerine/idem"
	//rb "github.com/glycerine/rbtree"
)

const minClientBackpressureDur = time.Millisecond * 2

// NB: all String() methods are now in simnet_string.go

type GoroControl struct {
	Mut  sync.Mutex
	Tm   time.Time
	Goro int
}

// Message operation
type mop struct {
	releaseBatch int64
	issueBatch   int64
	sn           int64
	who          int    // goro number
	where        string // generic fileLine (send,read,timer have their own atm)
	earlyName    string // before origin assigned, let mop.String print

	proceedMop *mop // in meq, should close(proceed) at completeTm

	cmdBatchSn   int64
	cmdBatchPart int64

	// API origin submission to simnet, consistently
	// on all requests like client/server registration,
	// snapshots, everything. probably redundant with
	// the earlier fields below, but okay; this might
	// be realtime if we are not under faketime.
	// Note: reqtm is not deterministic or reproducible.
	reqtm time.Time

	// when distributeMEQ sent this mop out for dispatch
	dispatchTm time.Time

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
	timerFileLine       string // where was this timer from
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

	// Possible TODO: use the go 1.24 weak pointers to dispose of timers?
	// We would have to periodically try to clean up our timer mop(s)
	// that have fired by checking if the weak.Pointer.Value() == nil,
	// meaning the GC has disposed of it--which tells us that the
	// client code no longer has the timer and we the simnet can
	// also dispose of it.

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
	proceed chan time.Duration

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

	scen         *scenario
	cktFault     *circuitFault
	hostFault    *hostFault
	repairCkt    *circuitRepair
	repairHost   *hostRepair
	alterHost    *simnodeAlteration
	alterNode    *simnodeAlteration
	batch        *SimnetBatch
	closeSimnode *closeSimnode
	newGoroReq   *newGoroRequest

	pseudorandom uint64
}

// set a non-zero op.pseudorandom value.
func (s *Simnet) setPseudorandom(op *mop) {
	var x uint64
	for x == 0 {
		x = s.scenario.rngUint64()
	}
	op.pseudorandom = x
}

func (s *Simnet) setPseudorandomRelease(op *mop) {
	var x uint64
	for x == 0 {
		x = s.scenario.rngReleaseUint64()
	}
	op.pseudorandom = x
}

func (op *mop) bestName() string {
	if op.kind == NEW_GORO {
		return op.newGoroReq.name
	}
	if op.origin != nil {
		return op.origin.name
	}
	return op.earlyName
}

// whence reports where in the source code this client
// call originates: the file:line number.
func (op *mop) whence() string {
	switch op.kind {
	case NEW_GORO:
		return op.where
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
	case CLOSE_SIMNODE:
	case SNAPSHOT:
	}
	return op.where
}

// increase determinism by processing
// the meq in priority order, after
// giving each mop a different priority.
// note: keeping the priority numbers as 100 x the mopkind
// just makes it easy to be sure we have not
// missed a priority if we add a new mopkind.
func (s *mop) priority() int64 {
	switch s.kind {
	case NEW_GORO:
		return 100

	case SCENARIO:
		return 200

	case CLIENT_REG:
		return 300
	case SERVER_REG:
		return 400

	case FAULT_CKT:
		return 500
	case FAULT_HOST:
		return 600
	case REPAIR_CKT:
		return 700
	case REPAIR_HOST:
		return 800

	case ALTER_HOST:
		return 900
	case ALTER_NODE:
		return 1000

	case BATCH:
		return 1100 // not sure. how strongly prioritized should batches be?

	case SEND:
		return 1200
	case READ:
		return 1300
	case TIMER:
		return 1400
	case TIMER_DISCARD:
		return 1500
	case CLOSE_SIMNODE:
		return 1600
	case SNAPSHOT:
		return 1700
	}
	panic(fmt.Sprintf("mop kind '%v' needs priority", int(s.kind)))
}
func (s *mop) tm() time.Time {
	switch s.kind {

	case NEW_GORO:
		return s.reqtm

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
	case CLOSE_SIMNODE:
		return s.reqtm
	}
	panic(fmt.Sprintf("mop kind '%v' needs tm", int(s.kind)))
}

// Aim for deterministic release of operations,
// (the close of the op.proceed channel), by
// following a pseudo randomized schedule:
//
// a) collect all the reads matched before the clock advances;
// b) put them into a releasableQ
// c) 1st sort by dispatch,batch,priority,tm,... the usual (avoid sn)
// d) then pseudo random sort them
// e) then release them in that order.
//
// called by scheduler goro.
func (s *Simnet) releaseReady() {
	// lock or else external mop sn allocators
	// calling simnetNextMopSn will data race
	// with all the stuff fin2 does with x fields.
	s.xmut.Lock()
	defer s.xmut.Unlock()

	ready := s.releasableQ.Len()
	if ready == 0 {
		//vv("releaseReady(): nothing to release")
		return
	}
	//vv("releaseReady(): releasing %v", ready)

	releaseBatch := s.nextReleaseBatch
	s.nextReleaseBatch++

	//fmt.Printf("\nreleaseReady for releaseBatch = %v\n", releaseBatch)
	//begPrng := s.scenario.rngReleaseUint64()

	//fmt.Printf("\nreleaseBatch = %v was at begPrng = %v ; has %v ops\n", releaseBatch, begPrng, ready)
	//fmt.Printf("fin hash: %v\n", asBlake33B(s.xb3hashFin))

	pseudoRandomQ := newOneTimeSliceQ("releaseReady")
	for s.releasableQ.Len() > 0 {
		op := s.releasableQ.pop()
		s.setPseudorandomRelease(op)
		op.releaseBatch = releaseBatch
		pseudoRandomQ.add(op)
	}

	for pseudoRandomQ.Len() > 0 {
		op := pseudoRandomQ.pop()
		//fmt.Printf("        op: %v\n", op)
		s.fin2(op)
		s.release(op)
	}
}

func (s *Simnet) release(op *mop) {

	switch op.kind {
	case TIMER:
		// already closed at :3727 in handleTimer
		return
		//case NEW_GORO:
	default:
		select {
		case op.proceed <- s.scenario.tick:
		case <-s.halt.ReqStop.Chan:
			return
		default:
			panicf("all proceed channels are buffered now, should never block")
		}
		return
	}
	// case
	// CLOSE_SIMNODE,
	// SNAPSHOT,
	// TIMER_DISCARD,
	// READ,
	// SEND,
	// ALTER_HOST,
	// ALTER_NODE,
	// CLIENT_REG,
	// SERVER_REG,
	// FAULT_CKT,
	// FAULT_HOST,
	// REPAIR_CKT,
	// REPAIR_HOST,
	// SCENARIO,
	// BATCH:

	close(op.proceed)

	// panic(fmt.Sprintf("mop kind '%v' needs release() policy", int(s.kind)))
}

func (s *mop) clone() (c *mop) {
	cp := *s
	c = &cp
	c.proceed = nil // not cloned
	return
}

// try again for more deterministic scheduling.
// simnet wide next deadline assignment for all
// timers on all nodes, and all send and read points.
func (s *Simnet) nextUniqTm(atleast time.Time, who string) time.Time {

	if atleast.After(s.lastTimerDeadline) {
		s.lastTimerDeadline = atleast
	} else {
		// INVAR: atleast <= s.lastTimerDeadline

		//const bump = timeMask0 // more than mask
		//s.lastTimerDeadline = s.lastTimerDeadline.Add(bump)
	}
	s.lastTimerDeadline = s.bumpTime(s.lastTimerDeadline)

	return s.lastTimerDeadline
}

func (s *Simnet) fin(op *mop) {
	// the simnet.go:1516 handleClientRegistration
	// new goroutine will call us and
	// give us a data race vs oursevles here
	// if we don't lock. Update: no longer in
	// a new goro (new goro is bad for determinism!)
	//s.xmut.Lock()
	//defer s.xmut.Unlock()

	s.releasableQ.add(op)
}

// fin2 records details of a finished mop
// into our mop tracking slices.
// called by releaseReady() just before
// closing proceed/releasing the operation
// back to the client.
// Must lock xmut b/c all the x fields are also
// accessed by external routine simnetNextMopSn().
func (s *Simnet) fin2(op *mop) {
	now := time.Now()
	//s.xmut.Lock()
	//defer s.xmut.Unlock()

	sn := op.sn
	s.xfintm[sn] = now
	s.xreleaseBatch[sn] = op.releaseBatch

	v, _ := s.xsn2descDebug.Load(sn)
	orig := v.(string)
	orig += fmt.Sprintf("[elap:%v]", now.Sub(s.xissuetm[sn]))
	s.xsn2descDebug.Store(sn, orig)

	w := op.whence() // file:line where created.
	s.xwhence[sn] = w
	s.xkind[sn] = op.kind
	if op.origin != nil {
		s.xorigin[sn] = op.origin.name
	}
	if op.target != nil {
		s.xtarget[sn] = op.target.name
	}

	s.xfinPrevHasherSn[sn] = s.xprevHasherSn
	s.xprevHasherSn = sn

	rep1 := fmt.Sprintf("%v", op.repeatable(now))
	s.xb3hashFin.Write([]byte(rep1))
	s.xfinRepeatable[sn] = rep1
	curhash := asBlake33B(s.xb3hashFin)
	s.xfinHash[sn] = curhash

	// the essential debugging print: which extra sn
	// is getting injected when the fin hashes vary?
	//fmt.Printf("s.xfinHash[sn:%v] = %v (disp: %v; batch: %v)\n", op.sn, s.xfinHash[sn], s.xsn2dis[op.sn], s.xissueBatch[op.sn])
	// sn messes up diffs:
	//fmt.Printf("s.xfinHash[sn:xx] = %v (disp: %v; batch: %v)\n", s.xfinHash[sn], s.xsn2dis[op.sn], s.xissueBatch[op.sn])

	// we cannot accurately check online, since
	// many operations finishing at the same time point
	// will initially appear in a non-deterministic
	// order. Hence let 709 rely on fully post-run
	// analysis (of the snapshots on disk).
	return

	// below is code for online comparison of
	// the 2nd run to the 1st run using
	// an informal custom trace of our own
	// execution. Used to be used from test 709.
	// It was a good chunk of work to write,
	// so keeping it here in case we need to revive it
	// for troubleshooting.

	if s.cfg.repeatTrace == nil {
		return
	}
	// do we match the previous trace?
	prev := s.cfg.repeatTrace
	// 0 = previous trace
	// 1 = ours
	sn1 := sn
	dis1 := s.xsn2dis[sn1]
	batch1 := s.xissueBatch[sn1]

	dis0 := dis1
	sn0 := prev.Xdis2sn[dis0]
	batch0 := prev.XissueBatch[sn0]

	rep0 := prev.XfinRepeatable[sn0]
	hash0 := prev.XfinHash[sn0]
	fin0 := prev.Xfintm[sn0]
	_ = fin0
	// different elapsed?
	//if !fin0.Equal(now) {
	//	panicf("previously we finished at %v, but now = %v", nice9(fin0), nice9(now))
	//}
	if rep0 != rep1 {
		panicf("previously we had rep0 = '%v'; but now rep1 = '%v'", rep0, rep1)
	}
	if hash0 != curhash {
		// a read is failing here 1st!?!
		// grab a snapshot
		cursnap := s.getInternalSnapshot(now)
		xpath := s.cfg.repeatTraceViolatedOutpath
		cursnap.ToFile(xpath)
		err := snapFilesDifferent(xpath, false) // like 707
		alwaysPrintf("snapFilesDifferent err = %v", err)
		dis0prev := dis0 - 1
		sn0prev := prev.Xdis2sn[dis0prev]
		hash0prev := prev.XfinHash[sn0prev]

		dis1prev := dis1 - 1
		sn1prev := s.xdis2sn[dis1prev]
		hash1prev := s.xfinHash[sn1prev]

		var curPrevHasher1sn int64
		var curPrevHasher1snRep string

		var oldPrevHasher0sn int64
		var oldPrevHasher0snRep string

		if sn1prev == -1 {
			alwaysPrintf("sn1prev == -1 -- yikes? sn1 = %v", sn1)
		} else {
			curPrevHasher1sn = s.xfinPrevHasherSn[sn1prev]
			if curPrevHasher1sn == -1 {
				alwaysPrintf("yikes? curPrevHasher1sn == -1; sn1prev =%v; sn1 = %v", sn1prev, sn1)
			} else {
				curPrevHasher1snRep = s.xfinRepeatable[curPrevHasher1sn]
			}
		}
		if sn0prev == -1 {
			alwaysPrintf("sn0prev == -1 -- yikes? sn0 = %v", sn0)
		} else {
			oldPrevHasher0sn = prev.XfinPrevHasherSn[sn0prev]
			if oldPrevHasher0sn == -1 {
				alwaysPrintf("yikes? oldPrevHasher0sn == -1; sn0prev = %v; sn0 = %v", sn0prev, sn0)
			} else {
				oldPrevHasher0snRep = prev.XfinRepeatable[oldPrevHasher0sn]
			}
		}
		alwaysPrintf(`
currrent trace up now is at (dis1 = %v; sn1 = %v; batch1 = %v)
                 prev trace (dis0 = %v; sn0 = %v; batch0 = %v)`,
			dis1, sn1, batch1, dis0, sn0, batch0)
		alwaysPrintf("previous trace previous (dis=%v; sn=%v) (equal to cur prev:%v):\n  cur-trace hash:'%v'\n prev-trace hash:'%v'\n curPrevHasher1sn(%v) rep = %v\n oldPrevHasher0sn(%v) rep = %v", dis0prev, sn0prev, hash0prev == hash1prev, hash0prev, hash1prev, curPrevHasher1sn, curPrevHasher1snRep, oldPrevHasher0sn, oldPrevHasher0snRep)
		alwaysPrintf("previously our accum hash0 = '%v', but curhash = '%v'", hash0, curhash)
		panic("eager panic, see above")
	}
}

func (s *Simnet) getInternalSnapshot(now time.Time) (cursnap *SimnetSnapshot) {
	cursnap = &SimnetSnapshot{
		reqtm:   now,
		who:     goID(),
		proceed: make(chan time.Duration),
		where:   fileLine(1),
	}
	// make our own mop so as not to allocate another sn
	// and thus disturb the current state of the system.
	req := &mop{
		kind:    SNAPSHOT,
		snapReq: cursnap,
	}
	req.proceed = req.snapReq.proceed
	s.handleSimnetSnapshotRequest(req, now, -1, true)
	return
}

// repeatable tries to report the dispatch or fin() completing
// of an operation at the given time (now) in a loggable
// string that is not tied to goroutine identity; to
// test for determinism and diagnose issues during replay.
func (op *mop) repeatable(now time.Time) string {

	// We chomp off the long random suffix of nm
	// to try and make repeated runs more deterministic.

	return fmt.Sprintf("%v_%v_%v_%v_%v", now.Format(rfc3339NanoTz0), op.cliOrSrvString(), op.kind, chompAnyUniqSuffix(op.bestName()), op.whence())
}

func (op *mop) cliOrSrvString() (cs string) {
	cs = "SERVER"
	if op.originCli {
		cs = "CLIENT"
	}
	return
}

// remember: issuing sn is inherently race-y since
// external client goroutines which model
// clients and servers -- especially at startup --
// are all trying to start sending and reading from
// the simnet network at the same point in time.
// We cannot count on the sn serial number order
// to be reproducible; it regularly varies.
func (s *Simnet) simnetNextMopSn(desc string) (sn int64) {
	// to get a consistent read for snapshot, dns etc,
	// we still need to lock xmut even with the atomics below.
	s.xmut.Lock()
	defer s.xmut.Unlock()

	sn = s.nextMopSn.Add(1)
	//vv("issue sn: %v  %v  at %v", sn, desc, fileLine(2))
	s.xsn2descDebug.Store(sn, desc)

	if sn >= maxX {
		panicf("out of sn, must increase maxX! sn=%v; maxX = %v", sn, maxX)
	}

	return
}

// maxX says how big to allocate our x tracking arrays
// for the execution history of the network.
const maxX = 1_000_000 // 100K too small for 707/709 test

func (s *Simnet) preallocateX() {
	limit := maxX

	s.xissuetm = make([]time.Time, limit)
	s.xissueOrder = make([]int64, limit)
	s.xissueBatch = make([]int64, limit)
	s.xissueHash = make([]string, limit)
	s.xdispatchRepeatable = make([]string, limit)
	s.xfinPrevHasherSn = make([]int64, limit)
	s.xreleaseBatch = make([]int64, limit)

	s.xfintm = make([]time.Time, limit)
	s.xfinHash = make([]string, limit)
	s.xfinRepeatable = make([]string, limit)
	s.xwhence = make([]string, limit)
	s.xkind = make([]mopkind, limit)

	s.xorigin = make([]string, limit)
	s.xtarget = make([]string, limit)

}

func (s *Simnet) simnetNextBatchSn() int64 {
	return atomic.AddInt64(&s.simnetLastBatchSn, 1)
}

// simnet simulates a network entirely with channels in memory.
type Simnet struct {
	nextMopSn        atomic.Int64
	xmax             int64
	nextReleaseBatch int64
	releasableQ      *pq
	xprevHasherSn    int64
	xfinPrevHasherSn []int64

	xsn2dis       map[int64]int64
	xdis2sn       map[int64]int64
	nextDispatch  int64
	xsn2descDebug sync.Map
	mintick       time.Duration
	ndtotPrev     int64
	ndtot         int64 // num dispatched total.

	simnetLastBatchSn int64

	// fin records execution/finishing order
	// for mop sn into xorder.
	xwhence     []string
	xkind       []mopkind
	xissuetm    []time.Time
	xissueOrder []int64
	xissueBatch []int64
	xissueHash  []string

	xdispatchRepeatable []string
	xfintm              []time.Time
	xfinHash            []string
	xfinRepeatable      []string
	xreleaseBatch       []int64

	xorigin []string
	xtarget []string

	// when we break ties in the meq for
	// same time-stamp operations, we
	// want to get as deterministic order
	// as possible. We use the sort of
	// the (simnet fake DNS) names in the
	// system. meq does same:
	// tm, priority of operation, origin.name
	// sender.name, ...
	// e.g. on the second run of the load test
	// in simgrid_test 707.

	xmut       sync.Mutex
	xb3hashFin *blake3.Hasher // ordered by fin() call time
	xb3hashDis *blake3.Hasher // ordered by dispatch time

	bigbang time.Time

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

	meq    *pq        // the master event queue.
	meqMut sync.Mutex // must hold for meq actions

	// one time slice worth of meq events
	// queue, for distributeMEQ()
	curSliceQ *pq

	cfg *Config

	srv *Server
	cli *Client

	node2server map[*simnode]*simnode

	// Ugh. We might need per-client dns because in general
	// it is impossible for a new auto-cli to know
	// that its name will be unique? as a 2nd connection
	// attempt may be made when in reality the first
	// connection attempt succeeded but just has been
	// slow to return. We don't want the simnode to
	// enforce a unique name when the real network
	// can only know what is locally unique, not
	// globally. First we'll try adding random suffixes
	// to the names externally. Seems to suffice for
	// now, along with trimming for reproduciblity
	// check in 709 using chompAnyUniqSuffix().

	dns        map[string]*simnode
	dnsOrdered *omap[string, *simnode]

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

	simnetNewGoroCh chan *newGoroRequest

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
	simnetCloseNodeCh       chan *closeSimnode

	newScenarioCh chan *scenario
	nextTimer     *time.Timer
	lastArmToFire time.Time
	lastArmDur    time.Duration

	curBatchNum int64
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

func (s *simnode) newGoro(purpose string) {
	// s.name will have uniquifying randomness at the
	// end, which chomp will truncate off, so put
	// purpose in front rather than at the end where
	// it could be lost.
	s.net.NewGoro("newGoro_" + purpose + "_" + s.name)
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

func (s *Simnet) newCircuitServer(name, serverBaseID string) (simnode *simnode) {
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

	srvnode := s.newCircuitServer(reg.server.name, reg.serverBaseID)
	srvnode.allnode[srvnode] = true
	srvnode.netAddr = reg.srvNetAddr
	s.circuits.set(srvnode, newDmap[*simnode, *simconn]())
	_, already := s.dns[srvnode.name]
	if already {
		panic(fmt.Sprintf("server name already taken: '%v'", srvnode.name))
	}

	s.dns[srvnode.name] = srvnode
	s.node2server[srvnode] = srvnode
	s.dnsOrdered.set(srvnode.name, srvnode)
	op.origin = srvnode

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

	// channel made by newCircuitServer() above.
	reg.tellServerNewConnCh = srvnode.tellServerNewConnCh
	s.fin(op)
}

func (s *Simnet) handleClientRegistration(regop *mop) {

	var reg *clientRegistration = regop.cliReg

	srvnode, ok := s.dns[reg.dialTo]
	if !ok {
		// node might simply be down at the moment,
		// and fully closed and offline, out of the simnet.
		//s.showDNS()
		//panic(fmt.Sprintf("cannot find server '%v', requested "+
		//	"by client registration from '%v'", reg.dialTo, reg.client.name))
		reg.err = fmt.Errorf("client dialTo name not found: '%v'", reg.dialTo)
		s.fin(regop)
		return
	}

	_, already := s.dns[reg.client.name]
	if already {
		// to work around some rare/hard to fix logical races
		// e.g. reboot_test 055, just
		// return an error rather than panic. Per-node
		// dns or moniker/facade names vs underlying nodes
		// are expensive alternatives.
		// Think of it as inducing more client error
		// recovery testing; which is a good thing.
		//
		//vv("simnet: client name already taken: '%v'", reg.client.name)
		panic(fmt.Sprintf("client name already taken: '%v'", reg.client.name))

		// or, recognizing that the "real world" network
		// can never know about global uniqueness of names...
		reg.err = fmt.Errorf("simnet handleClientRegistration error: client name already taken: '%v'", reg.client.name)
		s.fin(regop)
		return
	}

	clinode := s.newSimnodeClient(reg.client.name, reg.serverBaseID)
	clinode.setNetAddrSameNetAs(reg.localHostPortStr, srvnode.netAddr)
	s.allnodes[clinode] = true
	s.dns[clinode.name] = clinode
	s.dnsOrdered.set(clinode.name, clinode)

	regop.origin = clinode

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
	// the server about it... try in goro.
	// Ugh to races... it is buffered 100 now; plus we
	// have meq accept goro. try without background goroutine.
	// AND starting a goro makes the scheduler non-deterministic.
	// So avoid starting a new goroutine now for this:
	select {
	case srvnode.tellServerNewConnCh <- s2c:
		//vv("%v srvnode was notified of new client '%v'; s2c='%v'", srvnode.name, clinode.name, s2c)

		// let client start using the connection/edge.
		s.fin(regop)

	default:
		panicf("ugh. no room in srvnode.tellServerNewConnCh channel: cap = %v", cap(srvnode.tellServerNewConnCh))

	case <-s.halt.ReqStop.Chan:
		return
	}
}

// idempotent, all servers do this, then register through the same path.
func (cfg *Config) bootSimNetOnServer(srv *Server) *Simnet { // (tellServerNewConnCh chan *simconn) {

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
	// 5 msec is 2x - 3x faster sim than 1msec
	// (not dst: 25.4 sec on tube test suite); 49 sec
	// with DST-able level reproducbility.
	//tick := time.Millisecond * 5

	// 1msec tick: (not dst: 33 sec on tube) now
	// DST-able:151 sec to test tube.
	tick := time.Millisecond

	// 100 microsecond tick: (not dst: 74 sec on tube test suite);
	// versus with DST-able levels of reproducibility: > 10 minutes.
	//tick := time.Duration(minTickNanos) // 100_000 nanoseconds
	if tick < time.Duration(minTickNanos) {
		panicf("must have tick >= minTickNanos(%v)", time.Duration(minTickNanos))
	}
	if minClientBackpressureDur < tick {
		panicf("should have tick(%v) <= minClientBackpressureDur(%v)", tick, minClientBackpressureDur)
	}
	// minHop := time.Millisecond * 10
	// maxHop := minHop
	// var seed [32]byte
	// scen := NewScenario(tick, minHop, maxHop, seed)

	scen := NewScenarioBaseline(tick)

	// server creates simnet; must start server first.
	s := &Simnet{
		releasableQ: newReleasableQueue("scheduler"),
		xsn2dis:     make(map[int64]int64),
		xdis2sn:     make(map[int64]int64),
		//xsn2descDebug: make(map[int64]string),
		mintick: time.Duration(minTickNanos),
		//uniqueTimerQ:   newPQcompleteTm("simnet uniquetimerQ "),
		xb3hashFin:      blake3.New(64, nil),
		xb3hashDis:      blake3.New(64, nil),
		meq:             newMasterEventQueue("scheduler"),
		curSliceQ:       newOneTimeSliceQ("scheduler"),
		cfg:             cfg,
		srv:             srv,
		cliRegisterCh:   make(chan *clientRegistration),
		srvRegisterCh:   make(chan *serverRegistration),
		alterSimnodeCh:  make(chan *simnodeAlteration),
		alterHostCh:     make(chan *simnodeAlteration),
		submitBatchCh:   make(chan *mop),
		msgSendCh:       make(chan *mop),
		msgReadCh:       make(chan *mop),
		addTimer:        make(chan *mop),
		discardTimerCh:  make(chan *mop),
		newScenarioCh:   make(chan *scenario),
		simnetNewGoroCh: make(chan *newGoroRequest),

		injectCircuitFaultCh: make(chan *circuitFault),
		injectHostFaultCh:    make(chan *hostFault),
		repairCircuitCh:      make(chan *circuitRepair),
		repairHostCh:         make(chan *hostRepair),

		scenario:                scen,
		simnetSnapshotRequestCh: make(chan *SimnetSnapshot),
		simnetCloseNodeCh:       make(chan *closeSimnode),

		dns:        make(map[string]*simnode),
		dnsOrdered: newOmap[string, *simnode](),

		node2server: make(map[*simnode]*simnode),

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
	s.preallocateX()

	if s.cfg.SimnetGOMAXPROCS > 0 {
		runtime.GOMAXPROCS(s.cfg.SimnetGOMAXPROCS)
	}

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

	NEW_GORO mopkind = 1

	SCENARIO mopkind = 2

	CLIENT_REG mopkind = 3
	SERVER_REG mopkind = 4

	FAULT_CKT   mopkind = 5
	FAULT_HOST  mopkind = 6
	REPAIR_CKT  mopkind = 7
	REPAIR_HOST mopkind = 8
	ALTER_HOST  mopkind = 9
	ALTER_NODE  mopkind = 10

	// atomically apply a set of the above.
	BATCH mopkind = 11

	// core network primitives
	SEND          mopkind = 12
	READ          mopkind = 13
	TIMER         mopkind = 14
	TIMER_DISCARD mopkind = 15

	// remove simnode from system, it has shutdown for real.
	CLOSE_SIMNODE mopkind = 16

	// report a snapshot of the entire network/state.
	SNAPSHOT mopkind = 17
)

func enforceTickDur(tick time.Duration) time.Duration {
	return tick.Truncate(timeMask0)
}

// TODO: maybe unify with handleCloseSimnode?
// This was originally designed to allow easy power back on.
func (s *Simnet) shutdownSimnode(target *simnode) (undo Alteration) {
	if target.powerOff {
		// no-op. already off.
		return UNDEFINED
	}
	//vv("handleAlterCircuit: SHUTDOWN %v, going to powerOff true, in state %v", target.name, target.state)
	target.powerOff = true
	undo = POWERON

	for _, node := range keyNameSort(s.locals(target)) {
		node.readQ.deleteAll()
		node.preArrQ.deleteAll()
		node.timerQ.deleteAll()
		node.deafReadQ.deleteAll()
		// leave droppedSendQ, useful to simulate a slooow network
		// that eventually delivers messages from a server after several
		// power cycles?
		node.droppedSendQ.deleteAll()
		if node.isCli && node.cliConn != nil {
			// a peer should have forgotten all
			// local clients after a reboot.

			// other side should not know they
			// are sending their messages to a black hole.
			//node.cliConn.Close()
			node.cliConn = nil

			//vv("deleting from s.node2server '%v'", node.name)
			delete(s.node2server, node)
			delete(s.dns, node.name)
			s.dnsOrdered.delkey(node.name)

			delete(s.servers, node.serverBaseID)
			delete(s.allnodes, node)
			delete(s.orphans, node)

			delete(target.allnode, node)
		} else {
			// server: only self left after reboot
			node.allnode = map[*simnode]bool{node: true}
		}
		// no more circuits from this node
		others, ok := s.circuits.get2(node)
		if ok {
			for rem, conn := range others.all() {
				_ = rem
				// close all simconn

				// Make the local Read return EOF, which should
				// shutdown all read loops in the local Server/Client.
				conn.localClosed.Close()
			}
		}
		s.circuits.delkey(node)

		//vv("handleAlterCircuit: we just SHUTDOWN node: %v", node.name)
	}
	//s.node2server[target] = target

	//vv("handleAlterCircuit: end SHUTDOWN, target is now: %v", target)
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
		//vv("adding to deafReadQ = '%v'", read)
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
		//vv("transfer from '%v' -> '%v' send = %v", simnode.name, send.origin.name, send) // not seen, so not involved with the 055 weirdness
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
		//vv("markFault going from HEALTHY to FAULTY")
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
		//vv("markNotFaulty going from FAULTY to HEALTHY")
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
	//vv("top handleAlterCircuit, altop=%v", altop)
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
			return
		}
		// else we are a part of a larger host
		// alteration set, don't close proceed
		// prematurely.
	}()

	simnode, ok := s.dns[alt.simnodeName]
	if !ok {
		// happens commonly in crash/reboot scenarios. keep it short.
		//alt.err = fmt.Errorf("error: handleAlterCircuit could not find simnodeName '%v' in dns: '%v'", alt.simnodeName, s.dns)
		alt.err = fmt.Errorf("error: handleAlterCircuit could not find simnodeName '%v'", alt.simnodeName)
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
	//vv("top handleAlterHost altop='%v'", altop)

	var alt *simnodeAlteration = altop.alterHost

	node, ok := s.dns[alt.simnodeName]
	if !ok {
		alt.err = fmt.Errorf("error: handleAlterHost could not find simnodeName '%v' in dns: '%v'", alt.simnodeName, s.dns)
		// well huh: power on of previously poweroff and closed node gets here.
		// but that is kind of expected with a powered off node.
		// Because we want to power back on later with the same name,
		// we have to clear out the dns when it goes offline.
		//vv("early return on alt.err='%v'", alt.err)
		return
	}

	undo = s.reverse(alt.alter)

	// alter all auto-cli and the peer's server.
	// note that s.locals(node) now returns a single
	// node map for lone clients, so this works for them too.
	const closeDone_NO = false
	locals := s.locals(node) // includes srvnode itself map[*simnode]bool
	nlocal := len(locals)
	i := 0
	for node := range locals {
		alt.simnodeName = node.name
		// notice that we reuse alt, but set the final undo
		// based on the host level state seen in the above reverse.

		_ = nlocal
		//vv("in range s.locals, i=%v out of %v, for altop='%v'", i, nlocal, altop)
		_ = s.handleAlterCircuit(altop, closeDone_NO)
		i++
	}
	//vv("past range s.locals for altop='%v'", altop)
	alt.undo = undo
	s.fin(altop)
	return
}

func (s *Simnet) localDeafReadProb(read *mop) float64 {
	//return s.circuits.get(read.origin).get(read.target).deafRead
	fromMap := s.circuits.get(read.origin)
	if fromMap == nil {
		return 1 // a dead endpoint is always deaf.
	}
	return fromMap.get(read.target).deafRead
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
	//conn := s.circuits.get(read.origin).get(read.target)
	fromMap := s.circuits.get(read.origin)
	if fromMap == nil {
		return true // dead node always deaf
	}
	conn := fromMap.get(read.target)
	prob := conn.deafRead

	conn.attemptedRead++ // at least 1.
	read.readAttempt++

	if prob == 0 {
		//vv("localDeafRead top: prob = 0, not deaf for sure: read='%v'; send='%v'", read, send)
	} else {
		random01 := s.scenario.rngFloat64() // in [0, 1)

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
	random01 := s.scenario.rngFloat64() // in [0, 1)
	return random01 < prob
}

func (s *Simnet) dropped(prob float64) bool {
	if prob <= 0 {
		return false
	}
	if prob >= 1 {
		return true
	}
	random01 := s.scenario.rngFloat64() // in [0, 1)
	return random01 < prob
}

//func (s *Simnet) localDropSendProb(send *mop) float64 {
//	return s.circuits[send.origin][send.target].dropSend
//}

func (s *Simnet) localDropSend(send *mop) (isDropped bool, connAttemptedSend int64) {
	// get the local origin conn probability of drop

	// if send.origin has been terminated circuits will
	// return nil, so we cannot just chain like this:
	//conn := s.circuits.get(send.origin).get(send.target)
	fromNodeMap := s.circuits.get(send.origin)
	if fromNodeMap == nil {
		isDropped = true // cannot send from dead node
		return
	}
	conn := fromNodeMap.get(send.target)
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
func (s *Simnet) statewiseConnected(origin, target *simnode) (linked bool) {

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
	}()

	origin := send.origin
	send.senderLC = origin.lc
	send.originLC = origin.lc

	// do we want to always advance by 0.1 msec to allow
	// any amount of extra sleep to get the reader goro
	// alone when they pick up the read?
	hop := s.scenario.rngHop()
	send.unmaskedSendArrivalTm = send.initTm.Add(hop)

	// make sure send happens before receive by doing
	// this first.
	//send.completeTm = s.bumpTime(now)
	send.completeTm = now.Add(s.scenario.tick)
	//vv("hop = %v; send.completeTm = %v  now = %v\n  send='%v'", hop, send.completeTm, now, send)

	// handleSend
	//send.arrivalTm = s.bumpTime(send.unmaskedSendArrivalTm)
	send.arrivalTm = send.unmaskedSendArrivalTm.Add(s.scenario.tick)

	//vv("set send.arrivalTm = '%v' for send = '%v'", send.arrivalTm, send)

	// note that read matching time will be unique based on
	// send arrival time.

	// cktOrigin can be nil now with shutdowns
	var probDrop float64
	cktOrigin := s.circuits.get(send.origin)
	if cktOrigin == nil {
		//probDrop = 0 // a guess. not sure. is this the last close/RST though?
		probDrop = 1 // a better guess.
	} else {
		tarconn := cktOrigin.get(send.target)
		probDrop = tarconn.dropSend

		// so we want the reader to have a unique wake
		// time of their own, right? used to think
		// we wanted this... but goroID are not repeatable
		// from run to run, so its very hard to
		// pick the same one to be awake on each run.
	}
	if !s.statewiseConnected(send.origin, send.target) ||
		probDrop >= 1 { // s.localDropSend(send) {

		changed++
		limit--
		//vv("send.origin='%v'; send.target='%v'; handleSend DROP SEND %v", send.origin.name, send.target.name, send)
		send.origin.droppedSendQ.add(send)
		return
	}
	send.target.preArrQ.add(send)
	//vv("handleSend SEND added to preArrQ: for target='%v'; send = %v", send.target.name, send)
	////zz("LC:%v  SEND TO %v %v    srvPreArrQ: '%v'", origin.lc, origin.name, send, s.srvnode.preArrQ)

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
		return
	}

	origin := read.origin
	read.originLC = origin.lc

	//probDeaf := s.circuits.get(read.origin).get(read.target).deafRead
	fromMap := s.circuits.get(read.origin)
	if fromMap == nil {
		// this read is from a simnode that has been crashed.
		// probably just a logical race.
		read.err = ErrShutdown()
		s.fin(read)
		return
	}
	probDeaf := fromMap.get(read.target).deafRead
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

			if !timer.internalPendingTimer {
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
					vv("arg! could not deliver timer? '%v'  requeue or what? methinks this mean the goroutine who made it is gone, as the runtime otherwise would wait to schedule us... (or could be a shutdown race...)", timer)
					//reQtimer = append(reQtimer, timer)
					// Q TODO: should we also? as client user might be
					// long gone... could do:
					//if timer.timerReseenCount > 10 {
					// timer.timerCstrong = nil
					//}
					//continue
					//panic("why not deliverable? hopefully we never hit this and can just delete the backup attempt below")
				}
			} // end else
		} else {
			// INVAR: smallest timer > now
			return
		}
	} // end for
	return
}

// does not call armTimer.
func (s *Simnet) dispatchReadsSends(simnode *simnode, now time.Time, limit, loopi int64) (changes int64) {
	//defer func() {
	//vv("=== end of dispatch %v", simnode.name)
	//}()
	if limit == 0 {
		//vv("limit is 0, returning from dispatchReadsSends")
		return
	}

	//vv("dispatchReadsSends top: %v", simnode.net.schedulerReport())

	nR := simnode.readQ.Tree.Len()   // number of reads
	nS := simnode.preArrQ.Tree.Len() // number of sends

	if nR == 0 && nS == 0 {
		//vv("nR == 0 && ns == 0, returning from dispatchReadsSends")
		return
	}

	// matching reads and sends
	for readIt := simnode.readQ.Tree.Min(); !readIt.Limit(); {
		// do the preIt and readIt Next manually in various places so
		// we can advance and delete behind
		for preIt := simnode.preArrQ.Tree.Min(); !preIt.Limit(); {
			if limit == 0 {
				return
			}
			read := readIt.Item().(*mop)
			send := preIt.Item().(*mop)

			// We used to think of each simnode as being the endpoint
			// for a single circuit, but that only applies to
			// client simnodes; not server simnodes.
			// A server can be reading from many remote clients
			// in its readQ.
			// TODO: consider going back to using simnode per
			// network endpoint, so we don't have to guess
			// and double-loop scan. Everything in the
			// readQ and preArrQ on the same simnode
			// should always be potentially matchable.
			// The current advantage is that reads are
			// first-come first-served across all possible
			// senders. I guess the queues should technically
			// be associated with circuits instead of nodes...
			if read.target != send.origin {
				//vv("ignore: at simnode='%v' the send.origin not from this read's target: read.target='%v' != send.origin='%v'\n\n read='%v'\n\n send='%v'", simnode.name, read.target.name, send.origin.name, read, send)
				preIt = preIt.Next()
				continue
			}

			//vv("eval match: read = '%v'; connected = %v; s.localDeafRead(read)=%v", read, s.statewiseConnected(read.origin, read.target), s.localDeafRead(read, send))
			//vv("eval match: send = '%v'; connected = %v; s.localDropSend(send)=%v", send, s.statewiseConnected(send.origin, send.target), s.localDropSend(send))

			//simnode.optionallyApplyChaos()

			// We wait to drop-or-not until arrival time.
			// This allows changes to the network in
			// the interim. So we first
			// have to establish time-wise that the match
			// can be made, and only then decide on drop or not.

			if send.arrivalTm.After(now) {
				//vv("send has not arrived yet.")

				// are we done? since preArrQ is ordered
				// by arrivalTm, all subsequent pre-arrivals (sends)
				// will have even more _greater_ arrivalTm;
				// so no point in looking.
				// But do we need to allow other reads
				// to maybe match against the sends? yep, so:
				break // not return
			}
			// INVAR: this send.arrivalTm <= now

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

			//vv("send okay to deliver (below deaf read might drop it though): conn.attemptedSend=%v, send.sendAttempt=%v; send.sendDropFilteringApplied=%v", connAttemptedSend, send.sendAttempt, send.sendDropFilteringApplied)
			// INVAR: send is okay to deliver wrt faults.

			connectedR := s.statewiseConnected(read.origin, read.target)
			if !connectedR || s.localDeafRead(read, send) {
				//vv("dispatchReadsSends DEAF READ %v; connectedR=%v", read, connectedR)

				// we actually have to drop _the send_ on a deaf read,
				// otherwise the send is "auto-retried" until success.

				// different, for read deaf -> send drop.
				//vv("adding to deafReadQ the _send_ of a deaf read into the reader side deaf Q, and deleting it from the preArrQ: '%v'\n simnode.name: '%v'", send, simnode.name)
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
			//vv("servicing read: started LC %v -> serviced %v (waited: %v) read.sn=%v", read.originLC, simnode.lc, simnode.lc-read.originLC, read.sn) // 707 not seen

			// track clocks on either end for this send and read.
			read.readerLC = simnode.lc
			read.senderLC = send.senderLC
			send.readerLC = simnode.lc
			read.completeTm = now
			read.arrivalTm = send.arrivalTm // easier diagnostics

			// TRY? should we be bumping to reader goro ID? rough sketch would be:
			// but should we not be dispatching all possible matches first,
			// and then sorting them by time? so another 2-step process
			// to let the readers go... or we could have the reading
			// goroutine itself do its own sleep accoring to is ID?
			//
			// read.completeTm = s.bumpTime(now)
			// dur := read.completeTm.Sub(now)
			// if dur > 0 {
			// 	time.Sleep(dur)
			// 	if faketime {
			// 		synctestWait_LetAllOtherGoroFinish() // 2nd barrier
			// 	}
			// }

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
			// send already closed in handleSend()

			// we already advanced readIt so continue
			// to skip the one just below.
			if readIt.Limit() {
				return
			}
			continue
		} // end for sends in preArrQ
		if readIt.Limit() {
			return
		}
		readIt = readIt.Next()
	} // end for reads in readQ
	return
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

	s.meqMut.Lock()
	defer s.meqMut.Unlock()

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
	//if !s.cfg.QuietTestMode {
	//alwaysPrintf("simnet.Start: faketime = %v", faketime)
	//}
	go s.scheduler()
}

// durToGridPoint:
// given the time now, return the dur to
// get us to the next grid point, which is goal.
func (s *Simnet) durToGridPoint(now time.Time, tick time.Duration) (dur time.Duration, goal time.Time) {

	// this handles both
	// a) we are on a grid point now; and
	// b) we are off a grid point and we want the next one.

	goal = s.bumpTime(now.Add(tick).Truncate(tick))

	dur = goal.Sub(now)
	//vv("i=%v; just before dispatchAll(), durToGridPoint computed: dur=%v -> goal:%v to reset the gridTimer; tick=%v", i, dur, goal, s.scenario.tick)
	if dur <= 0 { // i.e. now <= goal
		panic(fmt.Sprintf("why was proposed sleep dur = %v <= 0 ? tick=%v; bigbang=%v; now=%v", dur, tick, s.bigbang, now))
	}
	return
}

func (s *Simnet) add2meq(op *mop, loopi int64) (armed bool) {
	s.meqMut.Lock()
	defer s.meqMut.Unlock()

	//vv("i=%v, add2meq %v", loopi, op)

	// give deterministic reqtm, not client view.
	op.reqtm = time.Now()
	s.meq.add(op)

	armed = s.armTimer(time.Now(), loopi, false)
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
		r := recover()
		//vv("scheduler defer shutdown running on goro = %v; recover='%v'", GoroNumber(), r)
		s.halt.ReqStop.Close()
		s.halt.Done.Close()
		if r != nil {
			alwaysPrintf("simnet scheduler panic-ing: %v", r)
			//alwaysPrintf("simnet scheduler panic-ing: %v", s.schedulerReport())
			panic(r)
		}
	}()
	s.who = goID()

	//var nextReport time.Time

	// main scheduler loop
	now := time.Now()

	// bigbang is the "start of time" -- don't change below.
	s.bigbang = now

	//var totalSleepDur time.Duration

	// accept into meq on its own goro,
	// so synctest.Wait does not interfere.
	s.StartMEQacceptor()

	tick := s.scenario.tick

	// This is the new scheduler loop.
	// It allows goroutines
	// to queue up as much as they can into
	// the meq via the StartMEQacceptor
	// goroutine, all while we are sleeping and
	// waiting at the barrier.
	//
	// The only thing that we didn't bring
	// over from the old scheduler approach is
	// sleeping until the next armed timer
	// expires. That might be still viable,
	// and might be faster, but this is
	// really simple and passes 709 so we are
	// keeping it basic for now. Timers
	// still work, but they might
	// be delayed a little longer than
	// initially set for. This is kind
	// of realistic anyway.
	var j int64
	for ; ; j++ {

		if s.halt.ReqStop.IsClosed() {
			return
		}

		// We have to shutdown promptly or the synctest
		// deadlock detector will yell at us. Thus we
		// use of time.After instead of a simple time.Sleep().
		select {
		case <-time.After(tick):
			// slept for 1 tick of the clock.
		case <-s.halt.ReqStop.Chan:
			return
		}
		if faketime {
			synctestWait_LetAllOtherGoroFinish() // barrier
		}
		now = time.Now()
		//vv("j loop; after Wait. j = %v", j)
		s.tickLogicalClocks()

		npop, restartNewScenario, shutdown := s.distributeMEQ(now, j)
		_ = npop
		if shutdown {
			return
		}
		s.tickLogicalClocks()

		if restartNewScenario {
			//vv("restartNewScenario: scenario applied: '%#v'", s.scenario)
			tick = s.scenario.tick
		}

		//vv("scheduler loop j = %v saw npop = %v, calling releaseReady()", j, npop)

		s.releaseReady() // release in deterministic order

		// an old check... from the old scheduler loop,
		// keep just in case we need to check for stalls again.
		// Will probably need to be reworked a little.
		// if false {
		// 	if j > 0 && j%2000 == 0 {
		// 		if s.ndtot > s.ndtotPrev {
		// 			s.ndtotPrev = s.ndtot
		// 		} else {
		// 			vv("stalled? j=%v no new dispatches in last 2000 iterataioins... bottom of scheduler loop. since bb: %v; faketime=%v", j, time.Since(s.bigbang), faketime)
		// 			alwaysPrintf("schedulerReport %v", s.schedulerReport())
		// 			panic("stalled?")
		// 		}
		// 	}
		// }
	}
}

// One aspect that was introducing non-determinism
// was that we were using the reqtm to sort mop in the meq.
// The reqtm is allocated from a client goroutine,
// and thus non-deterministic.
// Also: meq was sorted on reqtm to determine how to
// dispatch the meq. Yikes. Avoid that now.
func (s *Simnet) distributeMEQ(now time.Time, i int64) (npop int, restartNewScenario, shutdown bool) {

	s.meqMut.Lock()

	//sz := s.meq.Len()
	//vv("i=%v, top distributeMEQ: %v", i, s.showQ(s.meq, "meq"))
	//defer func() {
	//vv("i=%v, end of distributeMEQ: %v", i, s.showQ(s.meq, "meq"))
	//}()
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
	//
	// If we try and add a small clock increment
	// after every operation we process, then we
	// end up being driven by first come first served
	// which is threading non-deterministic based on
	// which client goro gets scheduled first, not good.
	// We want our tie breaking as it is more deterministic.
	//who := make(map[int]*mop)

	// always starts empty
	sliceQlen := s.curSliceQ.Len()
	if sliceQlen != 0 {
		panicf("s.curSliceQ.Len() = %v, but should always start 0", sliceQlen)
	}

	// if we don't process all the meq and assign
	// them timepoints in the future to run, then
	// the subsequent queued events might get to run first?
	var op *mop
	for j := 0; ; j++ {

		top := s.meq.peek()
		if top == nil {
			//vv("top of meq is nil, empty, break early")
			break
		}
		topTm := top.tm()
		if topTm.After(now) {
			//vv("top of meq tm (%v) is after now(%v), break early", topTm, now)
			break
		}
		op = s.meq.pop()
		npop++

		// we can randomize dispatch order
		// within the time slice using the
		// controlled/reproducible scenario.rng pseudo RNG.
		// s.scen.rng.Uint64() ... if we are sure
		// that would not violate causality. but we
		// do check that at the matchmaker.
		//vv("dispatchMEQ calling setPseudorandom for batch %v, npop %v", s.curBatchNum+1, npop)
		s.setPseudorandom(op)

		added, _ := s.curSliceQ.add(op)
		if !added {
			panicf("should never have conflict adding to curSliceQ: why could we not add op='%v' to curSliceQ = '%v'", op, s.showQ(s.curSliceQ, "curSliceQ"))
		}
	} // end for j
	s.meqMut.Unlock()

	if npop == 0 {
		// still have to dispatch below! might be
		// time to match sender and receiver after
		// sender has "traversed" the network
	} else {
		s.curBatchNum++
		var batch string
		//fmt.Printf("distributeMEQ: s.curBatchNum = %v is size %v    %v\n", s.curBatchNum, npop, nice9(now))
		//vv("have npop = %v, curSliceQ = %v", npop, s.showQ(s.curSliceQ, "curSliceQ"))

		for s.curSliceQ.Len() > 0 {
			op = s.curSliceQ.pop()
			// get the batch number (and size) into the hash too.
			xdis := fmt.Sprintf("%v_[batch_%v_with_batchSize_%v]", op.repeatable(now), s.curBatchNum, npop)

			sn := op.sn
			// must hold xmut.Lock else race vs simnetNextMopSn()
			s.xmut.Lock()

			// assert that we only issue each sn once
			prev, already := s.xsn2dis[sn]
			if already {
				panicf("cannot dispatch sn %v twice! already have (prev: %v) in s.xsn2dis.", sn, prev)
			}

			op.issueBatch = s.curBatchNum
			v, ok := s.xsn2descDebug.Load(sn)
			if !ok {
				panicf("why no sn = %v entry in xsn2descDebug?", sn)
			}
			desc := v.(string)
			next := s.nextDispatch
			s.nextDispatch++
			s.xsn2dis[sn] = next
			s.xdis2sn[next] = sn
			batch += fmt.Sprintf("[sn %v][disp %v]:%v, ", sn, next, desc)

			// fill these early for better print out;
			// fin() does these too... but maybe does not need to?
			// ------- begin maybe redundant with fin()
			w := op.whence() // file:line where created.
			s.xwhence[sn] = w
			s.xkind[sn] = op.kind
			if op.origin != nil {
				s.xorigin[sn] = op.origin.name
			}
			// ------- end maybe redundant with fin()

			s.xdispatchRepeatable[sn] = xdis
			// update xissuetm, since original was by client
			// in simnetNextMopSn() and not deterministic.
			s.xissuetm[sn] = now

			// start xissueOrder at 0 (just re-use next now.)
			// -1 means client got in simnetNextMopSn()
			// but we have not seen yet (so not in hash).

			s.xissueOrder[sn] = next
			s.xissueBatch[sn] = s.curBatchNum
			s.xb3hashDis.Write([]byte(xdis))
			s.xissueHash[sn] = asBlake33B(s.xb3hashDis)

			//fmt.Printf("on cur dispatch = %v; (for sn = %v) hashDis = %v\n", next, sn, asBlake33B(s.xb3hashDis))
			s.xmut.Unlock()

			//vv("in distributeMEQ, curSliceQ has op = '%v'\n  ->  xdis = '%v'", op, xdis)

			op.dispatchTm = now

			switch op.kind {
			case NEW_GORO:
				s.handleNewGoro(op, now, i)

			case CLOSE_SIMNODE:
				//vv("CLOSE_SIMNODE '%v'", op.closeSimnode.simnodeName)
				s.handleCloseSimnode(op, now, i)

			// case TIMER_FIRES: // not currently used.
			// 	vv("TIMER_FIRES: %v", op)
			// 	tC := op.proceedMop.timerC.Value()
			// 	if tC != nil {
			// 		select {
			// 		//case op.proceedMop.timerC <- now: // might need to make buffered?
			// 		case *tC <- now: // might need to make buffered?
			// 			vv("TIMER_FIRES delivered to timer: %v", op.proceedMop)
			// 		case <-s.halt.ReqStop.Chan:
			// 			vv("i=%v <-s.halt.ReqStop.Chan", i)
			// 			return
			// 		}
			// 	}
			// 	// let op be GC-ed.

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
				s.handleSimnetSnapshotRequest(op, now, i, false)
			case CLIENT_REG:
				s.handleClientRegistration(op)
			case SERVER_REG:
				s.handleServerRegistration(op)

			case SCENARIO:
				s.finishScenario()
				s.initScenario(op)

				restartNewScenario = true
				return

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

			//if strings.Contains(xdis, "CLIENT_SEND_auto-cli-from-srv_grid_node_0-to-srv_grid_node_1_cli.go") {
			//vv("about to panic at 1st common variance point")
			//panic("stopping after our 1st common variance point at line 6: sometimes dispatches at 00.0005, sometimes at 00.0004; sometimes 00.0006")
			//}
		} // end for

		//fmt.Printf("RRRRRRRR simnet.go:3241 in distributeMEQ, batchNum:%v (size %v) batch = '%v'\n\n", s.curBatchNum, npop, batch)

	}

	//if len(who) > 1 {
	//	vv("i=%v; who count = %v", i, len(who))
	//	panic(fmt.Sprintf("arg, non-determinism with two callers?! who='%#v'", who))
	//}
	// limit of -1 means no limit on number of dispatched mop.

	//nd := s.dispatchAll(now, 1, i)
	//vv("distributeMEQ calling dispatchAll")
	nd := s.dispatchAll(now, -1, i)
	_ = nd
	s.ndtot += nd

	armed := s.armTimer(now, i, true)
	_ = armed

	if !armed {
		//vv("timer NOT armed")
	}

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
	//comment := ""
	//if s.lastArmDur == math.MinInt64 {
	//	comment = "(no timer armed on last attempt)"
	//}
	//vv("ADVANCE time, in haveNextTimer(): s.lastArmToFire = %v; s.lastArmDur = %v %v", s.lastArmToFire, s.lastArmDur, comment)
	return s.nextTimer.C
}

func (s *Simnet) handleBatch(batchop *mop) {
	// submit now?

	var batch *SimnetBatch = batchop.batch
	_ = batch

	// else set timer for when to submit
	panic("TODO handleBatch")

	s.fin(batchop)
}

func (s *Simnet) finishScenario() {
	// do any tear down work...

	// at the end
	s.scenario = nil
}
func (s *Simnet) initScenario(op *mop) {
	scenario := op.scen
	s.scenario = scenario
	// do any init work...
	s.fin(op)
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

	discard.handleDiscardCalled = true
	orig := discard.origTimerMop

	found := discard.origin.timerQ.del(discard.origTimerMop)
	if found {
		discard.wasArmed = !orig.timerFiredTm.IsZero()
		discard.origTimerCompleteTm = orig.completeTm
	} // leave wasArmed false, could not have been armed if gone.

	////zz("LC:%v %v TIMER_DISCARD %v to fire at '%v'; now timerQ: '%v'", discard.origin.lc, discard.origin.name, discard, discard.origTimerCompleteTm, s.clinode.timerQ)
	s.fin(discard)
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

	timer.completeTm = s.bumpTime(timer.completeTm) // handle timer
	timer.timerDur = timer.completeTm.Sub(timer.initTm)
	//vv("masked timer(sn %v):\n dur: %v -> %v\n completeTm: %v -> %v\n timer.who: %v", timer.sn, timer.unmaskedDur, timer.timerDur, nice9(timer.unmaskedCompleteTm), nice9(timer.completeTm), timer.who)

	timer.origin.timerQ.add(timer)
	//vv("LC:%v %v set TIMER %v to fire at '%v'; now timerQ: '%v'", lc, timer.origin.name, timer, timer.completeTm, s.clinode.timerQ)

}

func (s *Simnet) armTimer(now time.Time, loopi int64, needMeqLock bool) (armed bool) {
	if needMeqLock {
		s.meqMut.Lock()
		defer s.meqMut.Unlock()
	}

	// Ah! We only want the scheduler to sleep if
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

			dur := when2.Sub(now)
			//if dur < 0 {
			//old: vv("times were not all unique, but they should be! op='%v'", op)
			//	panic(fmt.Sprintf("negative dur will collide with s.lastArmDur = -1 ?!?: %v", dur))
			//}
			if dur > s.scenario.tick {
				dur, when2 = s.durToGridPoint(now, s.scenario.tick)
				//vv("called durToGridPoint, when2 = '%v'", when2)
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

const minTickNanos = 100_000

func (s *Simnet) bumpTime(tm time.Time) (newtm time.Time) {
	// always bump to next 100 usec, so we are
	// for sure after tm.
	now := time.Now()
	newtm = tm.Truncate(timeMask0).Add(timeMask0)
	if newtm.Before(tm) {
		panic(fmt.Sprintf("arg. want newtm(%v) >= tm(%v)", newtm, tm))
	}
	if newtm.After(now) {
		return
	} else {
		newtm = now.Truncate(timeMask0).Add(timeMask0)
		//if newtm.Before(now) {
		if lte(newtm, now) || newtm.Before(tm) {
			// arg. our correction did not help.
			// it should have for any who > 0, so wat?
			panic(fmt.Sprintf("arg! newtm(%v) <= now(%v) || newtm.Before(tm='%v')", nice(newtm), nice(now), nice(tm)))
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
func (s *Simnet) handleSimnetSnapshotRequest(reqop *mop, now time.Time, loopi int64, internal bool) {
	var req *SimnetSnapshot = reqop.snapReq
	defer func() {
		if !internal {
			s.fin(reqop)
		}
	}()
	if !internal {
		s.xmut.Lock()
		defer s.xmut.Unlock()
	}

	req.Asof = now
	req.Loopi = loopi
	req.ScenarioNum = s.scenario.num
	req.ScenarioSeed = s.scenario.seed
	req.ScenarioTick = s.scenario.tick
	req.ScenarioMinHop = s.scenario.minHop
	req.ScenarioMaxHop = s.scenario.maxHop
	req.Peermap = make(map[string]*SimnetPeerStatus)
	req.DNS = make(map[string]string)
	for k, v := range s.dns {
		req.DNS[k] = v.name
	}

	n := s.nextMopSn.Load()
	req.XnextMopSn = n
	req.Xcountsn = n

	req.Xwhence = append([]string{}, s.xwhence[:n]...)
	req.Xkind = append([]mopkind{}, s.xkind[:n]...)
	req.Xissuetm = append([]time.Time{}, s.xissuetm[:n]...)
	req.XissueOrder = append([]int64{}, s.xissueOrder[:n]...)
	req.XissueBatch = append([]int64{}, s.xissueBatch[:n]...)
	req.XissueHash = append([]string{}, s.xissueHash[:n]...)
	req.XdispatchRepeatable = append([]string{}, s.xdispatchRepeatable[:n]...)
	req.Xfintm = append([]time.Time{}, s.xfintm[:n]...)
	req.XfinHash = append([]string{}, s.xfinHash[:n]...)
	req.XfinRepeatable = append([]string{}, s.xfinRepeatable[:n]...)
	req.XreleaseBatch = append([]int64{}, s.xreleaseBatch[:n]...)

	req.Xorigin = append([]string{}, s.xorigin[:n]...)
	req.Xtarget = append([]string{}, s.xtarget[:n]...)

	req.XhashFin = asBlake33B(s.xb3hashFin)
	req.XhashDis = asBlake33B(s.xb3hashDis)

	req.XprevHasherSn = s.xprevHasherSn
	req.XfinPrevHasherSn = append([]int64{}, s.xfinPrevHasherSn[:n]...)
	req.XnextDispatch = s.nextDispatch

	req.Xsn2dis = make(map[int64]int64)
	for k, v := range s.xsn2dis {
		req.Xsn2dis[k] = v
	}
	req.Xdis2sn = make(map[int64]int64)
	for k, v := range s.xdis2sn {
		req.Xdis2sn[k] = v
	}
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
		health := srvnode.state
		for autocli := range srvnode.allnode {
			if autocli.state != HEALTHY {
				health = autocli.state
			}
		}
		sps := &SimnetPeerStatus{
			Name:          srvnode.name,
			ServerState:   health,
			Poweroff:      srvnode.powerOff,
			LC:            srvnode.lc,
			ServerBaseID:  srvnode.serverBaseID,
			ConnmapOrigin: make(map[string]*SimnetConnSummary),
			ConnmapTarget: make(map[string]*SimnetConnSummary),
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
				sps.ConnmapOrigin[origin.name] = connsum
				sps.ConnmapTarget[target.name] = connsum
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
					Name:          origin.name,
					ServerState:   origin.state,
					Poweroff:      origin.powerOff,
					LC:            origin.lc,
					ServerBaseID:  origin.serverBaseID,
					Conn:          []*SimnetConnSummary{connsum},
					ConnmapOrigin: map[string]*SimnetConnSummary{origin.name: connsum},
					ConnmapTarget: map[string]*SimnetConnSummary{target.name: connsum},
					IsLoneCli:     true,
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

func asBlake33B(h *blake3.Hasher) string {
	sum := h.Sum(nil)
	return "blake3.33B-" + cristalbase64.URLEncoding.EncodeToString(sum[:33])
}

// TODO: maybe unify with shutdownSimnode?
// That was originally designed to allow easy power back on,
// but we find we need to fully take down the node
// in order to bring up its replacement cleanly and
// not have intermediate layer Server/Client read/send loops
// still going on old goroutines.
func (s *Simnet) handleCloseSimnode(clop *mop, now time.Time, iloop int64) {
	//vv("CLOSE_SIMNODE '%v'; reason='%v'", clop.closeSimnode.simnodeName, clop.closeSimnode.reason)

	defer func() {
		s.fin(clop)
	}()

	req := clop.closeSimnode
	target := req.simnodeName

	node0, ok := s.dns[target]
	if !ok {
		req.err = fmt.Errorf("simnodeName not found: '%v'; dns is '%#v'", target, s.dns)
		return
	}

	// the api CloseSimnode() no longer does SHUTDOWN
	// because we got races that split that and this--
	// so we do it for atomicity.
	// note: would noop the shutdownSimnode:
	node0.powerOff = true
	// we do all the same now: defer s.shutdownSimnode(node0)

	for _, node := range keyNameSort(s.locals(node0)) {

		others, ok := s.circuits.get2(node)
		_ = others

		if ok {
			// close all simconn
			for rem, conn := range others.all() {

				// actually we just want to close
				// the local without transmitting
				// EOF to the remote, at least for now.
				if req.processCrashNotHostCrash {
					// the host is up, so the
					// kernel will send RST/EOF to
					// remotes:

					// conn.Close() is external,
					// it does:
					m := NewMessage()
					m.EOF = true
					//vv("Close sending EOF in msgWrite, on %v", s.local.name)
					//conn.msgWrite(m, nil, 0)
					// which does

					send := s.newSendMop(m, false) // false for isCli (matters?)
					send.origin = node
					send.sendFileLine = fileLine(2)
					send.target = rem
					send.initTm = now
					send.isEOF_RST = true
					// s.net.msgSendCh <- send:
					// which does
					s.handleSend(send, -1, iloop)
					// but I'm not even sure if the above will
					// work from here, but try and see...
				}

				// This is the important part.
				// Make the local Read return EOF, which should
				// shutdown all read loops in the local Server/Client.
				conn.localClosed.Close()
			} // range over all conn
		} // end if ok we have circuits

		s.circuits.delkey(node)
		delete(s.node2server, node)
		delete(s.dns, node.name)
		s.dnsOrdered.delkey(node.name)

		//vv("handleCloseSimnode deleted node.name '%v' from dns", node.name)
		delete(s.servers, node.serverBaseID)
		delete(s.allnodes, node)
		delete(s.orphans, node)
	}
	delete(s.dns, target)
	s.dnsOrdered.delkey(target)

	//vv("handleCloseSimnode deleted target '%v' from dns", target)
	// set req.err if need be
}

func (s *Simnet) newClientRegMop(clireg *clientRegistration) (op *mop) {
	op = &mop{
		cliReg:    clireg,
		originCli: true,
		sn:        s.simnetNextMopSn("clientRegistration"),
		kind:      CLIENT_REG,
		proceed:   clireg.proceed,
		who:       clireg.who,
		reqtm:     clireg.reqtm,
		earlyName: clireg.client.name,
	}
	return
}

func (s *Simnet) newServerRegMop(srvreg *serverRegistration) (op *mop) {
	op = &mop{
		srvReg:    srvreg,
		originCli: false,
		sn:        s.simnetNextMopSn("serverRegistration"),
		kind:      SERVER_REG,
		proceed:   srvreg.proceed,
		who:       srvreg.who,
		reqtm:     srvreg.reqtm,
		earlyName: srvreg.server.name,
	}
	//vv("newServerRegMop(%p) has reqtm='%v' from srvreq.reqtm = '%v'; op = '%v'", op, op.reqtm, srvreg.reqtm, op)
	return
}

func (s *Simnet) newSnapReqMop(snapReq *SimnetSnapshot) (op *mop) {
	op = &mop{
		snapReq: snapReq,
		sn:      s.simnetNextMopSn("newSnap"),
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
		sn:      s.simnetNextMopSn("newScenario"),
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
		sn:        s.simnetNextMopSn("alterNode"),
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
		sn:        s.simnetNextMopSn("alterHost"),
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
		sn:       s.simnetNextMopSn("circuitFault"),
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
		sn:        s.simnetNextMopSn("hostFault"),
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
		sn:        s.simnetNextMopSn("repairCkt"),
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
		sn:         s.simnetNextMopSn("hostRepair"),
		kind:       REPAIR_HOST,
		proceed:    hostRepair.proceed,
		who:        hostRepair.who,
		reqtm:      hostRepair.reqtm,
	}
	return
}

func (s *Simnet) newCloseSimnodeMop(closeSimnodeReq *closeSimnode) (op *mop) {
	op = &mop{
		closeSimnode: closeSimnodeReq,
		//sn:           closeSimnodeReq.sn,
		sn:      s.simnetNextMopSn("closeSimnode"),
		kind:    CLOSE_SIMNODE,
		proceed: closeSimnodeReq.proceed,
		who:     closeSimnodeReq.who,
		reqtm:   closeSimnodeReq.reqtm,
		where:   closeSimnodeReq.where,
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

func (s *Simnet) handleNewGoro(op *mop, now time.Time, i int64) {
	//vv("top handleNewGoro: '%v'", op.newGoroReq.name)
	s.fin(op)
}

// call add2meq on as much additional work
// from blocked client goroutines as we
// can without advancing time. thus we try
// to increase the determinism of may
// clients contending for the simnet: let
// as many in as want in (in this time slice),
// then sort them into a consistently repeatable order,
// and execute their requests in that order.
func (s *Simnet) add2meqUntilSelectDefault(i int64) (shouldExit bool, saw int) {
	for ; ; saw++ {
		//if faketime {
		//	synctestWait_LetAllOtherGoroFinish() // barrier
		//}
		select {
		//default:
		//	return
		case <-time.After(1):
			if faketime {
				synctestWait_LetAllOtherGoroFinish() // barrier
			}
			return
		case newGoroReq := <-s.simnetNewGoroCh:
			s.add2meq(s.newGoroMop(newGoroReq), i)

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

		case closeSimnodeReq := <-s.simnetCloseNodeCh:
			//vv("i=%v simnetCloseNodeCh -> closeNodeReq", i)
			s.add2meq(s.newCloseSimnodeMop(closeSimnodeReq), i)

		case <-s.halt.ReqStop.Chan:
			//vv("i=%v <-s.halt.ReqStop.Chan", i)
			//bb := time.Since(s.bigbang)
			//pct := 100 * float64(totalSleepDur) / float64(bb)
			//_ = pct
			//vv("simnet.halt.ReqStop totalSleepDur = %v (%0.2f%%) since bb = %v)", totalSleepDur, pct, bb)
			shouldExit = true
			return
		}
	}
	return
}

// want to keep the scheduler as active as possible,
// but: only step in and dispatch and then release
// right at the time tick boundary.
//
// scheduler wants to ask: should I meq this event,
// or should I make it wait until the next time quantum?
//
// scheduler wants to know: when should I
// process all my meq that I have to this point?
// How can I make this as deterministic and
// repeatable as possible?
//
// We want to process when we know that all
// goroutines are durably blocked on
// somewhere else that is NOT _us_ ourselves the scheduler!
// -> either sleeping or some internal logic...
//    like waiting for a network read or send to finish.
//    or for their own timer. what else?
//  reads, sends, timers, is that it?
//
// scheduler wants to know: when should I
// release all of the ready events I have
// as a result of the meq processing?

// always accept events into the MEQ, to avoid
// having stragglers that are split apart from
// their pack by arbitrary time slicing. This
// design keeps synctest.Wait from interfering
// with goro that could make progress, and insures
// that synctest.Wait returns only when the
// client goro are self-blocked and not blocked
// on any MEQ acceptance.
func (s *Simnet) StartMEQacceptor() {
	go func() {
		//defer func() {
		//	vv("StartMEQacceptor defer/exit")
		//}()
		var i int64
		for ; ; i++ {
			//vv("meq acceptor goro about to select at i = %v", i)
			select {
			case newGoroReq := <-s.simnetNewGoroCh:
				s.add2meq(s.newGoroMop(newGoroReq), i)

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

			case closeSimnodeReq := <-s.simnetCloseNodeCh:
				//vv("i=%v simnetCloseNodeCh -> closeNodeReq", i)
				s.add2meq(s.newCloseSimnodeMop(closeSimnodeReq), i)

			case <-s.halt.ReqStop.Chan:
				return
			}
		}
	}()
}
