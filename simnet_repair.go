package rpc25519

import (
	"fmt"
	"time"
)

func (s *simnet) injectCircuitFault(fault *circuitFault, closeProceed bool) (err error) {
	if closeProceed {
		defer func() {
			fault.err = err
			close(fault.proceed)
		}()
	}

	origin, ok := s.dns[fault.originName]
	_ = origin
	if !ok {
		err = fmt.Errorf("injectCircuitFault could not find originName = '%v' in dns: '%v'", fault.originName, s.stringDNS())
		return
	}
	var target *simnode
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
	// this is all "local/origin only" because
	// our conn are just our local net.Conn equivalent.
	// We don't adjust the other end at all.
	recheckHealth := false
	addedFault := false
	for rem, conn := range remotes {
		if target == nil || target == rem {
			if fault.UpdateDeafReads {
				conn.deafRead = fault.DeafReadsNewProb
				if conn.deafRead > 0 {
					origin.state = FAULTY
					addedFault = true
					vv("set deafRead = fault.DeafReadsNewProb = %v; now conn = '%v'", fault.DeafReadsNewProb, conn)

				} else {
					recheckHealth = true
				}
			}
			if fault.UpdateDropSends {
				conn.dropSend = fault.DropSendsNewProb
				if conn.dropSend > 0 {
					origin.state = FAULTY
					addedFault = true
					vv("set dropSend = fault.DropSendsNewProb = %v; now conn='%v'", fault.DropSendsNewProb, conn)
				} else {
					recheckHealth = true
				}
			}
		}
	}
	if !addedFault && recheckHealth {
		// simnode may be healthy now, if faults are all gone.
		// but, an early fault may still be installed; full scan needed.
		s.recheckHealthState(origin)
	}
	vv("after injectCircuitFault '%v', simnet: '%v'", fault.String(), s.schedulerReport())
	now := time.Now() // TODO thread from caller in.
	s.addFaultsToPQ(now, origin, target, fault.DropDeafSpec)
	return
}

func (s *simnet) injectHostFault(fault *hostFault) (err error) {

	defer func() {
		fault.err = err
		close(fault.proceed)
	}()

	origin, ok := s.dns[fault.hostName]
	if !ok {
		panic(fmt.Sprintf("not avail in dns fault.origName = '%v'", fault.hostName))
	}
	for node := range s.locals(origin) {
		cktFault := newCircuitFault(node.name, "", fault.DropDeafSpec)
		s.injectCircuitFault(cktFault, false)
	}
	return
}

func (s *simnet) handleCircuitRepair(repair *circuitRepair, closeProceed bool) (err error) {
	if closeProceed {
		defer func() {
			repair.err = err
			close(repair.proceed)
		}()
	}

	origin, ok := s.dns[repair.originName]
	if !ok {
		return fmt.Errorf("error in handleCircuitRepair: originName not found: '%v'", repair.originName)
	}

	s.repairAllCircuitFaults(origin)
	if repair.powerOnIfOff {
		origin.powerOff = false
	}
	if repair.justOriginHealed {
		return
	}

	var target *simnode
	if repair.targetName != "" {
		target, ok = s.dns[repair.targetName]
		if !ok {
			err = fmt.Errorf("handleCircuitRepair could not find targetName = '%v' in dns: '%v'", repair.targetName, s.stringDNS())
			return
		}
	}
	for remote := range s.circuits[origin] {
		if target == nil || target == remote {
			s.repairAllCircuitFaults(remote)
			if repair.powerOnIfOff {
				remote.powerOff = false
			}
		}
	}
	return
}

// This is a central place to handle repairs to a circuit;
// undoing all deaf/drop faults on a single circuit.
//
// We are called by handleCircuitRepair and recheckHealthState.
// state can be ISOLATED or HEALTHY, we do not change these.
// If state is FAULTY, we go to HEALTHY.
// If state is FAULTY_ISOLATED, we go to ISOLATED.
func (s *simnet) repairAllCircuitFaults(simnode *simnode) {

	switch simnode.state {
	case HEALTHY:
		// fine.
	case ISOLATED:
		// fine.
	case FAULTY:
		simnode.state = HEALTHY
	case FAULTY_ISOLATED:
		simnode.state = ISOLATED
	}

	// might want to keep these for testing? for
	// now let's observe that the repair worked.
	simnode.deafReadsQ.deleteAll()
	simnode.droppedSendsQ.deleteAll()

	for _, conn := range s.circuits[simnode] {
		//vv("repairAllCircuitFaults: before 0 out deafRead and deafSend, conn=%v", conn)
		conn.deafRead = 0 // zero prob of deaf read.
		conn.dropSend = 0 // zero prob of dropped send.
		//vv("repairAllCircuitFaults: after deafRead=0 and deafSend=0, conn=%v", conn)
	}
}

func (s *simnet) handleHostRepair(repair *hostRepair) (err error) {
	defer func() {
		repair.err = err
		close(repair.proceed)
	}()

	const closeProceed_NO = false

	// default to repair allHosts, then revise.
	// repair.hostName will be empty to repair them all.
	justOrigin := false
	group := s.allnodes

	if !repair.allHosts {
		justOrigin = true
		origin, ok := s.dns[repair.hostName]
		if !ok {
			panic(fmt.Sprintf("not avail in dns repair.origName = '%v'", repair.hostName))
		}
		group = s.locals(origin)
	}

	for node := range group {
		cktRepair := s.newCircuitRepair(node.name, "",
			repair.unIsolate, repair.powerOnIfOff, justOrigin)
		s.handleCircuitRepair(cktRepair, closeProceed_NO)
	}
	return
}

func (s *simnet) recheckHealthState(simnode *simnode) {
	remotes, ok := s.circuits[simnode]
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
	s.repairAllCircuitFaults(simnode)
}

// DropDeafSpec specifies a network/netcard fault
// with a given probability.
type DropDeafSpec struct {

	// false UpdateDeafReads means no change to deafRead probability.
	UpdateDeafReads bool

	// false UpdateDropSends means no change to dropSend probability.
	UpdateDropSends bool

	// probability of ignoring (being deaf) to a read.
	// 0 => never be deaf to a read (healthy).
	// 1 => ignore all reads (dead).
	DeafReadsNewProb float64

	// probability of dropping a send.
	// 0 => never drop a send (healthy).
	// 1 => always drop a send (dead).
	DropSendsNewProb float64
}

type circuitFault struct {
	originName string
	targetName string
	DropDeafSpec
	sn      int64
	proceed chan struct{}
	err     error
}

func newCircuitFault(originName, targetName string, dd DropDeafSpec) *circuitFault {
	return &circuitFault{
		originName:   originName,
		targetName:   targetName,
		DropDeafSpec: dd,
		sn:           simnetNextMopSn(),
		proceed:      make(chan struct{}),
	}
}

type hostFault struct {
	hostName string
	DropDeafSpec
	sn      int64
	proceed chan struct{}
	err     error
}

func newHostFault(hostName string, dd DropDeafSpec) *hostFault {
	return &hostFault{
		hostName:     hostName,
		DropDeafSpec: dd,
		sn:           simnetNextMopSn(),
		proceed:      make(chan struct{}),
	}
}

type circuitRepair struct {
	originName string
	targetName string

	justOriginHealed bool
	unIsolate        bool
	powerOnIfOff     bool
	sn               int64
	proceed          chan struct{}
	err              error
}

func (s *simnet) newCircuitRepair(originName, targetName string, unIsolate, powerOnIfOff, justOrigin bool) *circuitRepair {
	return &circuitRepair{
		justOriginHealed: justOrigin,
		originName:       originName,
		targetName:       targetName,
		unIsolate:        unIsolate,
		powerOnIfOff:     powerOnIfOff,
		sn:               simnetNextMopSn(),
		proceed:          make(chan struct{}),
	}
}

type hostRepair struct {
	hostName     string
	powerOnIfOff bool
	unIsolate    bool
	allHosts     bool
	sn           int64
	proceed      chan struct{}
	err          error
}

func (s *simnet) newHostRepair(hostName string, unIsolate, powerOnIfOff, allHosts bool) *hostRepair {
	m := &hostRepair{
		hostName:     hostName,
		powerOnIfOff: powerOnIfOff,
		unIsolate:    unIsolate,
		allHosts:     allHosts,
		sn:           simnetNextMopSn(),
		proceed:      make(chan struct{}),
	}
	return m
}

// empty string target means all possible targets
func (s *simnet) CircuitFault(origin, target string, dd DropDeafSpec) (err error) {

	fault := newCircuitFault(origin, target, dd)

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

func (s *simnet) HostFault(hostName string, dd DropDeafSpec) (err error) {

	fault := newHostFault(hostName, dd)

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
func (s *simnet) RepairCircuit(originName string, unIsolate bool, powerOnIfOff bool) (err error) {

	targetName := "" // all corresponding targets
	const justOrigin_NO = false
	oneGood := s.newCircuitRepair(originName, targetName, unIsolate, powerOnIfOff, justOrigin_NO)

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

// RepairHost repairs all the circuits on the host.
func (s *simnet) RepairHost(originName string, unIsolate bool, powerOnIfOff, allHosts bool) (err error) {

	repair := s.newHostRepair(originName, unIsolate, powerOnIfOff, allHosts)

	select {
	case s.repairHostCh <- repair:
		vv("RepairHost sent repair on repairHostCh; about to wait on proceed")
	case <-s.halt.ReqStop.Chan:
		return
	}
	select {
	case <-repair.proceed:
		err = repair.err
		vv("one healthy '%v' processed. err = '%v'", originName, err)
	case <-s.halt.ReqStop.Chan:
		return
	}
	return
}

// AllHealthy heal all partitions, undo all faults, network wide.
// All circuits are returned to HEALTHY status. Their powerOff status
// is not updated unless powerOnIfOff is also true.
// See also RepairSimnode for single simnode repair.
func (s *simnet) AllHealthy(powerOnIfOff bool) (err error) {
	return s.RepairHost("", true, powerOnIfOff, true)
}
