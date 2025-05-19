package rpc25519

import (
	"fmt"
	"time"
)

func (s *simnet) injectCircuitFault(fault *circuitFault, closeProceed bool) (err error) {
	defer func() {
		if err != nil {
			if fault.err == nil {
				fault.err = err
			} else {
				fault.err = fmt.Errorf("%v [AND] %v", fault.err, err)
			}
		}
		if closeProceed {
			close(fault.proceed)
		}
	}()

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
					was := origin.state
					_ = was
					origin.state = FAULTY
					addedFault = true
					vv("set deafRead = fault.DeafReadsNewProb = %v; (%v -> FAULTY) now conn = '%v'", fault.DeafReadsNewProb, was, conn)

				} else {
					recheckHealth = true
				}
			}
			if fault.UpdateDropSends {
				conn.dropSend = fault.DropSendsNewProb
				if conn.dropSend > 0 {
					was := origin.state
					_ = was
					origin.state = FAULTY
					addedFault = true
					vv("set dropSend = fault.DropSendsNewProb = %v; (%v -> FAULTY) now conn='%v'", fault.DropSendsNewProb, was, conn)
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
	now := time.Now() // TODO thread from caller in.
	s.addFaultsToPQ(now, origin, target, fault.DropDeafSpec)
	vv("after injectCircuitFault '%v', simnet: '%v'", fault.String(), s.schedulerReport())
	return
}

func (s *simnet) injectHostFault(fault *hostFault) (err error) {

	defer func() {
		if err != nil {
			if fault.err == nil {
				fault.err = err
			} else {
				fault.err = fmt.Errorf("%v [AND] %v", fault.err, err)
			}
		}
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

func (s *simnet) handleHostRepair(repair *hostRepair) (err error) {

	//vv("top of handleHostRepair; repair = '%v'", repair)

	defer func() {
		if err != nil {
			if repair.err == nil {
				repair.err = err
			} else {
				repair.err = fmt.Errorf("%v [AND] %v", repair.err, err)
			}
		}
		//vv("end of handleHostRepair, closing repair proceed. err = '%v'", err)
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

	//vv("group is len %v", len(group))
	for node := range group {
		cktRepair := s.newCircuitRepair(node.name, "",
			repair.unIsolate, repair.powerOnIfOff, justOrigin)
		//vv("handleHostRepair about to call handleCircuitRepair with cktRepair='%v'", cktRepair)
		s.handleCircuitRepair(cktRepair, closeProceed_NO)
	}
	return
}

func (s *simnet) handleCircuitRepair(repair *circuitRepair, closeProceed bool) (err error) {
	//vv("top of handleCircuitRepair; closeProceed = %v; repair = '%v'", closeProceed, repair)
	defer func() {
		//vv("end of handleCircuitRepair")
		if err != nil {
			if repair.err == nil {
				repair.err = err
			} else {
				repair.err = fmt.Errorf("%v [AND] %v", repair.err, err)
			}
		}
		if closeProceed {
			close(repair.proceed)
		}
	}()

	origin, ok := s.dns[repair.originName]
	if !ok {
		return fmt.Errorf("error in handleCircuitRepair: originName not found: '%v'", repair.originName)
	}

	//vv("handleCircuitRepair about self-repair, repairAllCircuitFaults('%v')", origin.name)
	s.repairAllCircuitFaults(origin)
	//vv("handleCircuitRepair back from self-repair, repairAllCircuitFaults('%v')", origin.name)
	if repair.powerOnIfOff {
		origin.powerOff = false
	}
	if repair.justOriginHealed {
		//vv("handleCircuitRepair sees justOriginHealed, returning w/o touching targets")
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
			//vv("handleCircuitRepair about clear target remote '%v'", remote.name)
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
	//vv("top of repairAllCircuitFaults, simnode = '%v'", simnode.name)
	//defer func() {
	//vv("end of repairAllCircuitFaults")
	//}()

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

	// ==================   restore reads   ===============
	//
	// restore reads after un-partitioning/removing faults
	// b/c e.g. b/c typically our peer nodes won't know to
	// start a new read; they don't
	// use timeouts to avoid tearing messages. The kernel
	// will have been polling for us anyway... and we assume
	// that if the kernel did give us an error, we would
	// retry the read just the same.

	nDeaf := simnode.deafReadQ.tree.Len()
	if nDeaf > 0 {
		for it := simnode.deafReadQ.tree.Min(); it != simnode.deafReadQ.tree.Limit(); it = it.Next() {
			read := it.Item().(*mop)
			vv("transferring read = %v' from deafReadQ to readQ on '%v'", read, simnode.name)
			simnode.readQ.add(read)
		}
		simnode.deafReadQ.deleteAll()
	}
	// =================   restore sends?   ===============
	//
	// don't restore sends, they are lost (for now).
	//simnode.droppedSendsQ.deleteAll() ??? leave visible for tests for now.

	if false { // if we do want to restore and delete them as lost...
		for node := range s.circuits {
			nDrop := node.droppedSendQ.tree.Len()
			if nDrop > 0 {
				for it := node.droppedSendQ.tree.Min(); it != node.droppedSendQ.tree.Limit(); it = it.Next() {
					send := it.Item().(*mop)
					vv("transferring send = %v' from droppedSendQ to preArrQ on '%v'", send, node.name)
					node.preArrQ.add(send)
				}
				node.droppedSendQ.deleteAll()
			}
		}
	}

	// ================  conn repair  ==================
	//
	// clear the deaf/drop probabilities from each conn.

	for _, conn := range s.circuits[simnode] {
		if conn.deafRead <= 0 && conn.dropSend <= 0 {
			continue
		}
		//vv("repairAllCircuitFaults: before zero out, conn=%v", conn)
		conn.deafRead = 0 // zero prob of deaf read.
		conn.dropSend = 0 // zero prob of dropped send.
	}
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
	//vv("recheckHealthState sees no conn faults in replace for '%v'", simnode.name)
	s.repairAllCircuitFaults(simnode)
}

// ===========================================
// internal simnet scheduler loop code above.
// above is safe to call.
// ===========================================

// ===========================================
// external API below, safe for client use.
// ===========================================

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
// .
func (s *simnet) RepairCircuit(originName string, unIsolate bool, powerOnIfOff bool) (err error) {

	targetName := "" // all corresponding targets
	const justOrigin_NO = false
	oneGood := s.newCircuitRepair(originName, targetName, unIsolate, powerOnIfOff, justOrigin_NO)

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
func (s *simnet) RepairHost(originName string, unIsolate bool, powerOnIfOff, allHosts bool) (err error) {
	//vv("top of RepairHost, originName = '%v'; unIsolate=%v, powerOnIfOff=%v, allHosts=%v", originName, unIsolate, powerOnIfOff, allHosts)
	//defer func() {
	//vv("end of RepairHost('%v')", originName)
	//}()

	repair := s.newHostRepair(originName, unIsolate, powerOnIfOff, allHosts)

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
func (s *simnet) AllHealthy(powerOnIfOff bool) (err error) {
	vv("AllHealthy(powerOnIfOff=%v) called.", powerOnIfOff)

	return s.RepairHost("", true, powerOnIfOff, true)
}
