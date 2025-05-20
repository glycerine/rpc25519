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
					//vv("set deafRead = fault.DeafReadsNewProb = %v; (%v -> FAULTY) now conn = '%v'", fault.DeafReadsNewProb, was, conn)

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
					//vv("set dropSend = fault.DropSendsNewProb = %v; (%v -> FAULTY) now conn='%v'", fault.DropSendsNewProb, was, conn)
				} else {
					recheckHealth = true
				}
			}
		}
	}
	if !addedFault && recheckHealth {
		// simnode may be healthy now, if faults are all gone.
		// but, an early fault may still be installed; full scan needed.
		s.recheckHealthState(origin, fault.deliverDroppedSends)
	}
	now := time.Now() // TODO thread from caller in.
	s.addFaultsToPQ(now, origin, target, fault.DropDeafSpec)
	//vv("after injectCircuitFault '%v', simnet: '%v'", fault.String(), s.schedulerReport())
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
		cktFault := newCircuitFault(node.name, "", fault.DropDeafSpec, fault.deliverDroppedSends)
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
			repair.unIsolate, repair.powerOnIfOff, justOrigin, repair.deliverDroppedSends)
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
	s.repairAllCircuitFaults(origin, repair.deliverDroppedSends)
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
			s.repairAllCircuitFaults(remote, repair.deliverDroppedSends)
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
func (s *simnet) repairAllCircuitFaults(simnode *simnode, deliverDroppedSends bool) {
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
			//vv("transferring read = %v' from deafReadQ to readQ on '%v'", read, simnode.name)
			simnode.readQ.add(read)
		}
		simnode.deafReadQ.deleteAll()
	}

	if deliverDroppedSends {
		for node := range s.circuits {
			nDrop := node.droppedSendQ.tree.Len()
			if nDrop > 0 {
				for it := node.droppedSendQ.tree.Min(); it != node.droppedSendQ.tree.Limit(); it = it.Next() {
					send := it.Item().(*mop)
					//vv("transferring send = %v' from droppedSendQ to preArrQ on '%v'", send, send.target.name)
					send.target.preArrQ.add(send)
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

func (s *simnet) recheckHealthState(simnode *simnode, deliverDroppedSends bool) {
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
	s.repairAllCircuitFaults(simnode, deliverDroppedSends)
}

// ===========================================
// internal simnet scheduler loop code above.
// above is safe to call.
// ===========================================
