package rpc25519

import (
	"fmt"
	"time"
)

// we _only_ update the conn ends at fault.originName.
// The corresponding remote conn are not changed.
func (s *Simnet) injectCircuitFault(faultop *mop, closeProceed bool) (err error) {

	var fault *circuitFault = faultop.cktFault
	//vv("top injectCircuitFault: fault = '%v'", fault) // seen 1002, good.
	defer func() {
		if err != nil {
			if fault.err == nil {
				fault.err = err
			} else {
				fault.err = fmt.Errorf("%v [AND] %v", fault.err, err)
			}
		}
		if closeProceed {
			s.fin(faultop)
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
	if fault.deliverDroppedSends {
		// at the end, after all other fault adjustments
		defer s.deliverDroppedSends(origin, target)
	}

	remotes, ok := s.circuits.get2(origin)
	if !ok {
		// no remote conn to adjust
		return
	}
	defer s.equilibrateReads(origin, target) // allow any newly possible reads too.

	// this is all "local/origin only" because
	// our conn are just our local net.Conn equivalent.
	// We don't adjust the other end at all.
	for rem, conn := range remotes.all() {
		if target == nil || target == rem {
			if fault.UpdateDeafReads {
				//vv("origin %v, setting conn.deafRead = %v", origin.name, fault.DeafReadsNewProb) // seen, good 1002.
				conn.deafRead = fault.DeafReadsNewProb
			}
			if fault.UpdateDropSends {
				conn.dropSend = fault.DropSendsNewProb
			}
		}
	}

	// use nil target so we check everything and
	// can accurately set our state before
	// doing the apply faults below and the
	// equilibrate reads when defer runs.
	if s.localCircuitFaultsPresent(origin, nil) {
		//vv("localCircuitFaultsPresent true")
		s.markFaulty(origin)
	} else {
		//vv("localCircuitFaultsPresent false")
		s.markNotFaulty(origin)
	}

	now := time.Now() // TODO thread from caller in.
	s.applyFaultsToPQ(now, origin, target, fault.DropDeafSpec)
	//vv("after injectCircuitFault '%v', simnet: '%v'", fault.String(), s.schedulerReport())
	return
}

// equilibrateReads conservatively moves
// reads between the readQ and the deafReadQ,
// based on the current connection state of
// the end points of each origin read
// (calling statewiseConnected). Only if
// we know for sure now that the two
// nodes are not isolated from each other,
// and that there are no deaf read
// probability left, do we transfer from
// deafReadQ to readQ.
//
// We want to allow healing,
// but in the case of only partial reapir,
// we don't want to allow reads that were
// assigned deaf with some probability before.
// Otherwise our flakey card simulation
// gets mooted.
//
// Conversely, the other direction is also
// conservative, from readQ to deafRead.
// So equilibrateReads only moves reads
// to deafRead if the origin and the target
// are structurally isolated or if the
// local conn is definitely, fully, 100%
// blocked to reads.
//
// In all other cases, we assume that the
// client code wants to keep the probabilistic
// "flaky network" in place. Code that
// wants to clear all deaf reads into the readQ
// should simply do that directly, by
// calling the imperative transferDeafReadsQ_to_readsQ.
//
// called at the end of injectCircuitFault, before
// any time warp delivery. We need to set and
// clear deaf reads based on the current state
// of the connection faults and each end's
// isolation state.
func (s *Simnet) equilibrateReads(origin, target *simnode) {
	//if target == nil {
	//	//vv("top equilibrateReads(origin='%v', target='nil')", origin.name)
	//} else {
	//	//vv("top equilibrateReads(origin='%v', target='%v')", origin.name, target.name)
	//}
	//defer func() {
	//	//vv("end equilibrateReads, simnet = %v", s.qReport())
	//}()

	var addToReadQ, addToDeafReadQ []*mop

	// scan deafReadQ first
	it := origin.deafReadQ.Tree.Min()
	for !it.Limit() {
		read := it.Item().(*mop)
		if target == nil || target == read.target {

			// this will allow previously deaf reads
			// to proceed, only if neither is isolated and there are
			// no local circuit faults at all (even
			// probabilitistically) at the origin; i.e.
			// the probability of deaf reads on the
			// connection is now zero.
			if s.statewiseConnected(read.origin, read.target) &&
				s.localCircuitNotDeafForSure(read.origin, read.target) {

				addToReadQ = append(addToReadQ, read)
				delit := it
				it = it.Next()
				origin.deafReadQ.Tree.DeleteWithIterator(delit)
				continue
			}
		}
		it = it.Next()
	}
	// scan readQ second
	it = origin.readQ.Tree.Min()
	for !it.Limit() {
		read := it.Item().(*mop)
		if target == nil || target == read.target {

			// can we do this accurately without rolling
			// the dice again? that could make determinism
			// very hard. What can we reason about for sure?
			// We can for sure deafen reads where the nodes
			// are no longer statewise connected, and those
			// where the probabilty of deaf read is 1.
			// We will assume that if the prob is in (0,1)
			// that we should not change any reads deaf/non-deaf
			// state.
			if !s.statewiseConnected(read.origin, read.target) ||
				s.localCircuitDeafForSure(read.origin, read.target) {

				addToDeafReadQ = append(addToDeafReadQ, read)
				delit := it
				it = it.Next()
				origin.readQ.Tree.DeleteWithIterator(delit)
				continue
			}
		}
		it = it.Next()
	}
	for _, read := range addToReadQ {
		origin.readQ.add(read)
	}
	for _, read := range addToDeafReadQ {
		//pp("equilibrateReads adding '%v'", read)
		origin.deafReadQ.add(read)
	}
}

func (s *Simnet) injectHostFault(faultop *mop) (err error) {
	//vv("top of injectHostFault; faultop = '%v'", faultop) // seen
	var fault *hostFault = faultop.hostFault
	defer func() {
		if err != nil {
			if fault.err == nil {
				fault.err = err
			} else {
				fault.err = fmt.Errorf("%v [AND] %v", fault.err, err)
			}
		}
		s.fin(faultop)
		//vv("end of injectHostFault; faultop = '%v'", faultop) // seen
	}()

	origin, ok := s.dns[fault.hostName]
	if !ok {
		panic(fmt.Sprintf("not avail in dns fault.origName = '%v'", fault.hostName))
	}
	for node := range s.locals(origin) {
		cktFault := s.newCircuitFault(node.name, "", fault.DropDeafSpec, fault.deliverDroppedSends)
		cktFaultOp := s.newCktFaultMop(cktFault)
		s.injectCircuitFault(cktFaultOp, false)
	}
	// try to get fault applied to all new connections as well,
	// modeling a network card fault on a server rather than
	// a single network path with a faulty middlebox that always
	// gets circumvented on a new TCP connection...
	basesrv, ok := s.node2server[origin]
	if !ok {
		// lone client, e.g. Test1001_simnetonly_drop_prob
		origin.allNewCircuitsInjectFault = append(origin.allNewCircuitsInjectFault, fault)
	} else {
		basesrv.allNewCircuitsInjectFault = append(basesrv.allNewCircuitsInjectFault, fault)
	}
	return
}

func (s *Simnet) handleHostRepair(repairop *mop) (err error) {

	var repair *hostRepair = repairop.repairHost

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
		s.fin(repairop)
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
		//vv("handleHostRepair about to call handleCircuitRepair on node '%v' with cktRepair='%v'", node.name, cktRepair)
		cktRepairOp := s.newRepairCktMop(cktRepair)
		s.handleCircuitRepair(cktRepairOp, closeProceed_NO)
	}

	return
}

func (s *Simnet) handleCircuitRepair(repairop *mop, closeProceed bool) (err error) {

	var repair *circuitRepair = repairop.repairCkt
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
			s.fin(repairop)
		}
	}()

	origin, ok := s.dns[repair.originName]
	if !ok {
		return fmt.Errorf("error in handleCircuitRepair: originName not found: '%v'", repair.originName)
	}

	var target *simnode
	if repair.targetName != "" {
		target, ok = s.dns[repair.targetName]
		if !ok {
			err = fmt.Errorf("handleCircuitRepair could not find targetName = '%v' in dns: '%v'", repair.targetName, s.stringDNS())
			return
		}
	}

	if repair.deliverDroppedSends {
		defer s.deliverDroppedSends(origin, target)
	}

	//vv("handleCircuitRepair about self-repair, repairAllCircuitFaults('%v')", origin.name)
	s.repairAllCircuitFaults(origin)
	//vv("handleCircuitRepair back from self-repair, repairAllCircuitFaults('%v')", origin.name)
	if repair.powerOnIfOff {
		s.powerOnSimnode(origin)
	}
	if repair.justOriginHealed {
		//vv("handleCircuitRepair sees justOriginHealed, returning w/o touching targets")
		s.equilibrateReads(origin, nil) // allow any newly possible reads too.
		return
	}

	defer s.equilibrateReads(origin, target) // allow any newly possible reads too.

	for remote := range s.circuits.get(origin).all() {
		if target == nil || target == remote {
			//vv("handleCircuitRepair about clear target remote '%v'", remote.name)
			s.repairAllCircuitFaults(remote)
			if repair.powerOnIfOff {
				s.powerOnSimnode(remote)
			}
		}
	}
	if repair.unIsolate {
		//vv("in handleCircuitRepair about to unIsolate '%v'", origin.name)
		s.unIsolateSimnode(origin)
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
// We are called by handleCircuitRepair and injectCircuitFault,
// since the later can be used to remove faults too.
//
// If state is FAULTY, we go to HEALTHY.
// If state is FAULTY_ISOLATED, we go to ISOLATED.
// If state is HEALTHY, this is a no-op.
// If state is ISOLATED, this is a no-op.
func (s *Simnet) repairAllCircuitFaults(simnode *simnode) {
	//vv("top of repairAllCircuitFaults, simnode = '%v'; state = %v", simnode.name, simnode.state)
	//defer func() {
	//	//vv("end of repairAllCircuitFaults, simnode = '%v'; state = %v", simnode.name, simnode.state)
	//}()

	simnode.allNewCircuitsInjectFault = nil

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
	for _, conn := range s.circuits.get(simnode).all() {
		conn.repair()
	}
}

func (s *Simnet) deliverDroppedSends(origin, target *simnode) {
	s.timeWarp_transferDroppedSendQ_to_PreArrQ(origin, target)
	s.timeWarp_transferDeafReadQsends_to_PreArrQ(origin, target)
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

// only from origin conn point of view. if
// target is nil, we check all conn/circuits starting
// at origin. otherwise just the conn from origin -> target.
func (s *Simnet) localCircuitFaultsPresent(origin, target *simnode) (ans bool) {
	//vv("top localCircuitFaultsPresent")
	//defer func() {
	//	//vv("end localCircuitFaultsPresent, returning %v", ans)
	//}()
	for rem, conn := range s.circuits.get(origin).all() {
		if target == nil || target == rem {
			if conn.deafRead > 0 {
				return true // not healthy
			}
			if conn.dropSend > 0 {
				return true // not healthy
			}
		}
	}
	return false
}

// only from origin conn point of view. if
// target is nil, we check all conn/circuits starting
// at origin. otherwise just the conn from origin -> target.
func (s *Simnet) localCircuitDeafForSure(origin, target *simnode) bool {

	// try to address segfault on nil back
	// from s.circuits.get(origin) below.
	if origin == nil {
		return false // really don't know for sure.
	}
	got := s.circuits.get(origin)
	if got == nil {
		return false
	}
	for rem, conn := range got.all() {
		if target == nil || target == rem {
			if conn.deafRead < 1 {
				return false
			}
		}
	}
	// all are >= 1
	return true
}

// only from origin conn point of view. if
// target is nil, we check all conn/circuits starting
// at origin. otherwise just the conn from origin -> target.
func (s *Simnet) localCircuitNotDeafForSure(origin, target *simnode) bool {

	for rem, conn := range s.circuits.get(origin).all() {
		if target == nil || target == rem {
			if conn.deafRead > 0 {
				return false
			}
		}
	}
	// all are <= 0
	return true
}

// deafen reads/drop sends that were started
// but still in progress with this fault. Applies
// to any sends that we arrive >= now.
func (s *Simnet) applyFaultsToPQ(now time.Time, origin, target *simnode, dd DropDeafSpec) {

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

func (s *Simnet) applyFaultsToReadQ(now time.Time, origin, target *simnode, deafReadProb float64) {

	readIt := origin.readQ.Tree.Min()
	for !readIt.Limit() {
		read := readIt.Item().(*mop)
		if target == nil || read.target == target {
			if !s.statewiseConnected(read.origin, read.target) ||
				//s.deaf(deafReadProb) { wrong! makes 1002 red, 50% deaf cli.
				s.localDeafReadProb(read) >= 1 {

				//pp("deaf fault enforced on read='%v'", read)
				origin.deafReadQ.add(read) // this is defeaning all in 1002!

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

func (s *Simnet) applySendFaults(now time.Time, originNowFaulty, target *simnode, dropSendProb float64) {

	// have to look for origin's sends in all other pre-arrQ...
	// and check all, in case disconnect happened since the send.
	for other := range s.circuits.all() {
		if other == originNowFaulty {
			// No way at present for a TCP client or server
			// to read or send to itself. Different sockets
			// on the same host would be different nodes, so
			// this does not suppress "loop back".
			continue
		}
		if target != nil && other != target {
			continue
		}
		// INVAR: target == nil || other == target
		// target == nil means add faults to all of originNowFaulty conns

		sendIt := other.preArrQ.Tree.Min()
		for !sendIt.Limit() {

			send := sendIt.Item().(*mop)

			if send.arrivalTm.Before(now) {
				// INVAR: smallest time send < now.
				//
				// Since this message is due to arrive
				// before the fault at now, we let
				// it be delivered.
				//
				// preArrQ is ordered by arrivalTm,
				// but that doesn't let us short-circuit here,
				// since we must fault everything due
				// to arrive >= now. So we must continue
				// scanning rather than return.
				sendIt = sendIt.Next()
				continue
			}
			// INVAR: message is droppable, due to arrive >= now

			if send.origin == originNowFaulty {

				if !s.statewiseConnected(send.origin, send.target) ||
					s.dropped(dropSendProb) {

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
