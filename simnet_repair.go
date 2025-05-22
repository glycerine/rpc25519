package rpc25519

import (
	"fmt"
	"time"
)

// we _only_ update the conn ends at fault.originName.
// The corresponding remote conn are not changed.
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

	if fault.deliverDroppedSends {
		// at the end, after all other fault adjustments
		defer s.deliverDroppedSends(origin)
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
	for rem, conn := range remotes {
		if target == nil || target == rem {
			if fault.UpdateDeafReads {
				conn.deafRead = fault.DeafReadsNewProb
			}
			if fault.UpdateDropSends {
				conn.dropSend = fault.DropSendsNewProb
			}
		}
	}

	if s.circuitFaultsPresent(origin) {
		s.markFaulty(origin)
	} else {
		s.markNotFaulty(origin)
	}
	now := time.Now() // TODO thread from caller in.
	s.applyFaultsToPQ(now, origin, target, fault.DropDeafSpec)
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

	if repair.deliverDroppedSends {
		defer s.deliverDroppedSends(origin)
	}

	//vv("handleCircuitRepair about self-repair, repairAllCircuitFaults('%v')", origin.name)
	s.repairAllCircuitFaults(origin)
	//vv("handleCircuitRepair back from self-repair, repairAllCircuitFaults('%v')", origin.name)
	if repair.powerOnIfOff {
		s.powerOnSimnode(origin)
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
				s.powerOnSimnode(remote)
			}
		}
	}
	return
}

// This is a central place to handle repairs to a circuit;
// undoing all deaf/drop faults on a single circuit.
//
// We change simnode.state only with respect to faults,
// not isolation. POST invariant is that we are either
// ISOLATED or HEALTHY. We will not be FAULTY or FAULTY_ISOLATED.
//
// We are called by handleCircuitRepair and recheckHealthState.
//
// If state is FAULTY, we go to HEALTHY.
// If state is FAULTY_ISOLATED, we go to ISOLATED.
// If state is HEALTHY, this is a no-op.
// If state is ISOLATED, this is a no-op.
func (s *simnet) repairAllCircuitFaults(simnode *simnode) {
	vv("top of repairAllCircuitFaults, simnode = '%v'", simnode.name)
	//defer func() {
	//vv("end of repairAllCircuitFaults")
	//}()

	switch simnode.state {
	case HEALTHY:
		// fine.
		return
	case ISOLATED:
		// fine.
		return
	case FAULTY:
		simnode.state = HEALTHY
	case FAULTY_ISOLATED:
		simnode.state = ISOLATED
	}

	// ================== restore reads ===============
	// restore reads after un-partitioning/removing faults
	// b/c e.g. b/c typically our peer nodes won't know to
	// start a new read; they don't
	// use timeouts to avoid tearing messages. The kernel
	// will have been polling for us anyway... and we assume
	// that if the kernel did give us an error, we would
	// retry the read just the same.
	s.transferDeafReadsQ_to_readsQ(simnode, nil)

	// ================ repair connections ==================
	// clear the deaf/drop probabilities from each conn.
	for _, conn := range s.circuits[simnode] {
		conn.repair()
	}
}

func (s *simnet) deliverDroppedSends(origin *simnode) {
	// can we do this instead?
	// s.timeWarp_transferDroppedSendQ_to_PreArrQ(simnode, nil)

	for node := range s.circuits {
		nDrop := node.droppedSendQ.Tree.Len()
		if nDrop > 0 {
			for it := node.droppedSendQ.Tree.Min(); it != node.droppedSendQ.Tree.Limit(); {
				send := it.Item().(*mop)
				if s.statewiseConnected(send.origin, send.target) {
					//vv("transferring send = %v' from droppedSendQ to preArrQ on '%v'", send, send.target.name)
					send.target.preArrQ.add(send)
					// advance and delete behind
					delit := it
					node.droppedSendQ.Tree.DeleteWithIterator(delit)
					it = it.Next()
					continue
				}
				it = it.Next()
			}
		}
	}
}

func (conn *simconn) repair() (changed int) {
	//vv("simconn.repair: before zero out, conn=%v", conn)
	if conn.deafRead > 0 {
		changed++
		conn.deafRead = 0 // zero prob of deaf read.
	}
	if conn.dropSend > 0 {
		changed++
		conn.dropSend = 0 // zero prob of dropped send.
	}
	return
}

func (s *simnet) circuitFaultsPresent(simnode *simnode) bool {
	remotes, ok := s.circuits[simnode]
	if !ok {
		return false
	}
	for rem, conn := range remotes {
		_ = rem
		if conn.deafRead > 0 {
			return true // not healthy
		}
		if conn.dropSend > 0 {
			return true // not healthy
		}
	}
	return false
	//s.repairAllCircuitFaults(simnode)
}

// defean reads/drop sends that were started
// but still in progress with this fault.
func (s *simnet) applyFaultsToPQ(now time.Time, origin, target *simnode, dd DropDeafSpec) {

	if !dd.UpdateDeafReads && !dd.UpdateDropSends {
		return
	}
	if dd.UpdateDeafReads && dd.DeafReadsNewProb > 0 {
		s.applyFaultsToReadQ(now, origin, target, dd.DeafReadsNewProb)
	}
	if dd.UpdateDropSends && dd.DropSendsNewProb > 0 {
		s.applySendFaults(now, origin, target, dd.DropSendsNewProb)
	}
}

func (s *simnet) applyFaultsToReadQ(now time.Time, origin, target *simnode, deafReadProb float64) {

	readIt := origin.readQ.Tree.Min()
	for readIt != origin.readQ.Tree.Limit() {
		read := readIt.Item().(*mop)
		if target == nil || read.target == target {
			if s.deaf(deafReadProb) {

				//vv("deaf fault enforced on read='%v'", read)
				// don't mark them, so we can restore them later.
				origin.deafReadQ.add(read)

				// advance readIt, and delete behind
				delmeIt := readIt
				readIt = readIt.Next()
				origin.readQ.Tree.DeleteWithIterator(delmeIt)
				continue
			}
		}
		readIt = readIt.Next()
	}
}

func (s *simnet) applySendFaults(now time.Time, originNowFaulty, target *simnode, dropSendProb float64) {

	// have to look for origin's sends in all other pre-arrQ...
	// and check all, in case disconnect happened since the send.
	for other := range s.circuits {
		if other == originNowFaulty {
			// No way at present for a TCP client or server
			// to read or send to itself. Different sockets
			// on the same host would be different nodes.
			continue
		}
		if target != nil && other != target {
			continue
		}
		// INVAR: target == nil || other == target
		// target == nil means add faults to all of originNowFaulty conns

		sendIt := other.preArrQ.Tree.Min()
		for sendIt != other.preArrQ.Tree.Limit() {

			send := sendIt.Item().(*mop)

			if gte(send.arrivalTm, now) {
				// droppable, due to arrive >= now, keep going below.
			} else {
				// INVAR: smallest time send < now.
				//
				// preArrQ is ordered by arrivalTm,
				// but that doesn't let us short-circuit here,
				// since we must fault everything due
				// to arrive >= now. So we keep scanning.
				continue
			}
			if send.origin == originNowFaulty {

				if !s.statewiseConnected(send.origin, send.target) ||
					s.dropped(dropSendProb) {

					// easier to understand if we store on origin,
					// not in targets pre-arr Q.
					//vv("addSendFaults DROP SEND %v", send)
					originNowFaulty.droppedSendQ.add(send)

					// advance sendIt, and delete behind
					delmeIt := sendIt
					sendIt = sendIt.Next()
					other.preArrQ.Tree.DeleteWithIterator(delmeIt)
					continue
				}
			}
			sendIt = sendIt.Next()
		}
	}
}
