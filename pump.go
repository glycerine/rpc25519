package rpc25519

// background goro to read all PeerID *Messages and sort them
// to all the circuits live in this peer.
func (pb *localPeerback) peerbackPump() {

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
		vv("%v: peerbackPump cleanup done.", name)
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
