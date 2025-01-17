package rpc25519

import (
	"context"
	"time"
)

// background goro to read all PeerID *Messages and sort them
// to all the circuits live in this peer.
func (pb *localPeerback) peerbackPump() {

	defer func() {
		vv("localPeerback.peerbackPump all-finished; pb= %p", pb)
	}()

	name := pb.peerServiceName
	vv("%v: peerbackPump top.", name)

	// key: CallID (circuit ID)
	m := make(map[string]*Circuit)

	cleanupCkt := func(ckt *Circuit) {
		vv("%v: cleanupCkt running for ckt '%v'", name, ckt.Name)
		// Politely tell our peer we are going down,
		// in case they are staying up.
		frag := NewFragment()
		frag.Typ = CallPeerEndCircuit
		pb.SendOneWay(ckt, frag, nil)

		ckt.canc()
		delete(m, ckt.callID)
		pb.u.UnregisterChannel(ckt.callID, CallIDReadMap)
		pb.u.UnregisterChannel(ckt.callID, CallIDErrorMap)

		ckt.Halt.ReqStop.Close()
		ckt.Halt.Done.Close()
		pb.halt.ReqStop.RemoveChild(ckt.Halt.ReqStop)

		if pb.autoShutdownWhenNoMoreCircuits && len(m) == 0 {
			vv("%v: peerbackPump exiting on autoShutdownWhenNoMoreCircuits", name)
			pb.halt.ReqStop.Close()
		}
	}
	defer func() {
		vv("%v: peerbackPump exiting. closing all remaining circuits (%v).", name, len(m))
		var all []*Circuit
		for _, ckt := range m {
			all = append(all, ckt)
		}
		for _, ckt := range all {
			cleanupCkt(ckt)
		}
		m = nil
		vv("%v: peerbackPump cleanup done... telling peers were are down", name)

		// tell all remotes we are going down
		remotesSlice := pb.remotes.getValSlice()
		for _, rem := range remotesSlice {
			pb.tellRemoteWeShutdown(rem)
		}
		vv("%v: peerbackPump done telling peers we are down.", name)
		pb.halt.Done.Close()
	}()

	done := pb.ctx.Done()
	for {
		vv("%v %p: pump loop top of select. pb.handleChansNewCircuit = %p", name, pb, pb.handleChansNewCircuit)
		select {
		case <-pb.halt.ReqStop.Chan:
			return

		case ckt := <-pb.handleChansNewCircuit:
			m[ckt.callID] = ckt
			pb.halt.ReqStop.AddChild(ckt.Halt.ReqStop)

		case ckt := <-pb.handleCircuitClose:
			vv("%v pump: ckt := <-pb.handleCircuitClose: for ckt='%v'", name, ckt.Name)
			cleanupCkt(ckt)

		case msg := <-pb.readsIn:

			callID := msg.HDR.CallID
			ckt, ok := m[callID]
			vv("pump %v: sees readsIn msg, ckt ok=%v", name, ok)
			if !ok {
				// we expect the ckt close ack-back to be dropped if we initiated it.
				alwaysPrintf("%v: arg. no circuit avail for callID = '%v';"+
					" pump dropping this msg.", name, callID)

				if callID == "" {
					// we have a legit PeerID but no CallID, which means that
					// we have not yet instantiated a circuit. Do so? No.
					// For now we have client do a CallPeerStartCircuit call.
				}
				continue
			}
			vv("pump %v: (ckt %v) sees msg='%v'", name, ckt.Name, msg)

			if msg.HDR.Typ == CallPeerEndCircuit {
				vv("pump %v: (ckt %v) sees msg CallPeerEndCircuit in msg: '%v'", name, ckt.Name, msg) // seen in crosstalk test server hung log line 311
				cleanupCkt(ckt)
				vv("pump %v: (ckt %v) sees msg CallPeerEndCircuit in msg. back from cleanupCkt, about to continue: '%v'", name, ckt.Name, msg)
				continue
			}
			if msg.HDR.Typ == CallPeerFromIsShutdown {
				rpb, n, ok := pb.remotes.getValNDel(msg.HDR.FromPeerID)
				if ok {
					vv("%v: got notice of shutdown of peer '%v'", name, aliasDecode(msg.HDR.FromPeerID))
					_ = rpb
					vv("what more do we need to do with rpb on its shutdown?")
				}
				if n == 0 {
					vv("no remote peers left ... we could shut ourselves down to save memory?")
					if pb.autoShutdownWhenNoMorePeers {
						vv("%v: lbp.autoShutdownWhenNoMorePeers true, closing up", name)
						return
					}
				}
			}

			frag := ckt.convertMessageToFragment(msg)
			select {
			case ckt.Reads <- frag: // server should be hung here, if peer code not servicing
			case <-ckt.Halt.ReqStop.Chan:
				cleanupCkt(ckt)
				// otherwise hang if circuit is shutting down.
				continue
			case <-pb.halt.ReqStop.Chan:
				return
			case <-done:
				return
			}
		case msgerr := <-pb.errorsIn:

			callID := msgerr.HDR.CallID
			ckt, ok := m[callID]
			vv("pump %v: ckt ok=%v on errorsIn", name, ok)
			if !ok {
				vv("%v: arg. no ckt avail for callID = '%v' on msgerr", name, callID)
				continue
			}
			vv("pump %v: (ckt %v) sees msgerr='%v'", name, ckt.Name, msgerr)

			if msgerr.HDR.Typ == CallPeerEndCircuit {
				vv("pump %v: (ckt %v) sees msgerr CallPeerEndCircuit in msgerr: '%v'", name, ckt.Name, msgerr)
				cleanupCkt(ckt)
				continue
			}

			fragerr := ckt.convertMessageToFragment(msgerr)
			select {
			case ckt.Errors <- fragerr:
			case <-ckt.Halt.ReqStop.Chan:
				cleanupCkt(ckt)
				continue
			case <-pb.halt.ReqStop.Chan:
				return
			case <-done:
				return
			}
		}
	}
}

// only do this if msg has no DoneCh and no HDR.Args
func shallowCloneMessage(msg *Message) *Message {
	cp := *msg
	return &cp
}

func (pb *localPeerback) tellRemoteWeShutdown(rem *remotePeerback) {

	shut := &Message{}
	shut.HDR.Created = time.Now()
	shut.HDR.From = pb.netAddr
	shut.HDR.Typ = CallPeerFromIsShutdown
	shut.HDR.FromPeerID = pb.peerID

	// pb.ctx is probably unusable by now as already cancelled.
	ctxB := context.Background()

	shut.HDR.To = rem.netAddr
	shut.HDR.ToPeerID = rem.peerID
	shut.HDR.Serial = issueSerial()
	shut.HDR.ServiceName = rem.remoteServiceName

	pb.u.SendOneWayMessage(ctxB, shut, nil)
}
