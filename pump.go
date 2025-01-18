package rpc25519

import (
	"context"
	"fmt"
	"time"
)

func prettyPrintCircuitMap(m map[string]*Circuit) (s string) {
	s = fmt.Sprintf("circuit map holds %v circuilts:\n", len(m))
	i := 0
	for k, v := range m {
		s += fmt.Sprintf("[%03d] CircuitID '%v' -> \n%v\n\n", i, k, v.String())
		i++
	}
	return
}

// background goro to read all PeerID *Messages and sort them
// to all the circuits live in this peer.
func (pb *LocalPeer) peerbackPump() {

	//defer func() {
	//zz("LocalPeer.PeerbackPump all-finished; pb= %p", pb)
	//}()

	name := pb.PeerServiceName
	_ = name
	//zz("%v: PeerbackPump top.", name)

	// key: CallID (circuit ID)
	m := make(map[string]*Circuit)

	cleanupCkt := func(ckt *Circuit, notifyPeer bool) {

		// Note: in normal operations, we may well be called *several times*
		// for each circuit shutdown. Once when the user code
		// defer ckt.Close() happens as it sees the shutdown,
		// and other times if the test/user code cancels it;
		// as well as if the remote side sends us a cancel.
		// All of this is fine and expected and creates fault-
		// tolerance if one part goes down. The upshot is: be
		// sure the following is idempotent (and not generating errors)
		// if the Circuit is cleaned-up multiple times.
		// notifyPeer will be false if we got a cancel from a
		// remote peer. In that case there is no need to tell
		// them again about the shutdown.

		vv("%v: cleanupCkt running for ckt '%v'. notifyPeer=%v; len(m)=%v before cleanup. CircuitID='%v';\n m = '%v'", name, ckt.Name, notifyPeer, len(m), ckt.CircuitID, prettyPrintCircuitMap(m))
		if notifyPeer {
			// Politely tell our peer we are going down,
			// in case they are staying up.
			frag := NewFragment()
			frag.Typ = CallPeerEndCircuit
			pb.SendOneWay(ckt, frag, -1) // no blocking
		}
		ckt.Canc(fmt.Errorf("pump cleanupCkt(notifyPeer=%v) cancelling ckt.Ctx.", notifyPeer))
		delete(m, ckt.CircuitID)
		pb.U.UnregisterChannel(ckt.CircuitID, CallIDReadMap)
		pb.U.UnregisterChannel(ckt.CircuitID, CallIDErrorMap)

		ckt.Halt.ReqStop.Close()
		ckt.Halt.Done.Close()
		pb.Halt.ReqStop.RemoveChild(ckt.Halt.ReqStop)

		if pb.AutoShutdownWhenNoMoreCircuits && len(m) == 0 {
			//zz("%v: peerbackPump exiting on autoShutdownWhenNoMoreCircuits", name)
			pb.Halt.ReqStop.Close()
		}
	}
	defer func() {
		zz("%v: peerbackPump exiting. closing all remaining circuits (%v).", name, len(m))
		var all []*Circuit
		for _, ckt := range m {
			all = append(all, ckt)
		}
		for _, ckt := range all {
			cleanupCkt(ckt, true)
		}
		m = nil
		//zz("%v: peerbackPump cleanup done... telling peers were are down", name)

		// tell all remotes we are going down
		remotesSlice := pb.Remotes.GetValSlice()
		for _, rem := range remotesSlice {
			pb.TellRemoteWeShutdown(rem)
		}
		//zz("%v: peerbackPump done telling peers we are down.", name)
		pb.Halt.Done.Close()
	}()

	done := pb.Ctx.Done()
	for {
		//zz("%v %p: pump loop top of select. pb.handleChansNewCircuit = %p", name, pb, pb.handleChansNewCircuit)
		select {
		case <-pb.Halt.ReqStop.Chan:
			return

		case query := <-pb.QueryCh:
			// query is &queryLocalPeerPump{}
			query.OpenCircuitCount = len(m)
			close(query.Ready)

		case ckt := <-pb.HandleChansNewCircuit:
			vv("%v pump: ckt := <-pb.HandleChansNewCircuit: for ckt='%v'", name, ckt.Name)
			m[ckt.CircuitID] = ckt
			pb.Halt.AddChild(ckt.Halt)

		case ckt := <-pb.HandleCircuitClose:
			//vv("%v pump: ckt := <-pb.HandleCircuitClose: for ckt='%v'", name, ckt.Name)
			cleanupCkt(ckt, true)

		case msg := <-pb.ReadsIn:

			if msg.HDR.Typ == CallPeerFromIsShutdown && msg.HDR.FromPeerID != pb.PeerID {
				rpb, n, ok := pb.Remotes.GetValNDel(msg.HDR.FromPeerID)
				if ok {
					//zz("%v: got notice of shutdown of peer '%v'", name, aliasDecode(msg.HDR.FromPeerID))
					_ = rpb
					//zz("what more do we need to do with rpb on its shutdown?")
				}
				if n == 0 {
					//zz("no remote peers left ... we could shut ourselves down to save memory?")
					if pb.AutoShutdownWhenNoMorePeers {
						//zz("%v: lbp.autoShutdownWhenNoMorePeers true, closing up", name)
						return
					}
				}
			}

			callID := msg.HDR.CallID
			ckt, ok := m[callID]
			////zz("pump %v: sees readsIn msg, ckt ok=%v", name, ok)
			if !ok {
				// we expect the ckt close ack-back to be dropped if we initiated it.
				//alwaysPrintf("%v: arg. no circuit avail for callID = '%v'/Typ:'%v';"+
				//	" pump dropping this msg.", name, aliasDecode(callID),
				//	msg.HDR.Typ.String())

				if callID == "" {
					// we have a legit PeerID but no CallID, which means that
					// we have not yet instantiated a circuit. Do so? No.
					// For now we have client do a CallPeerStartCircuit call.
				}
				continue
			}
			////zz("pump %v: (ckt %v) sees msg='%v'", name, ckt.Name, msg)

			if msg.HDR.Typ == CallPeerEndCircuit {
				vv("pump %v: (ckt %v) sees msg CallPeerEndCircuit in msg: '%v'", name, ckt.Name, msg) // seen in crosstalk test server hung log line 311
				cleanupCkt(ckt, false)
				//zz("pump %v: (ckt %v) sees msg CallPeerEndCircuit in msg. back from cleanupCkt, about to continue: '%v'", name, ckt.Name, msg)
				continue
			}

			frag := ckt.ConvertMessageToFragment(msg)
			select {
			case ckt.Reads <- frag: // server should be hung here, if peer code not servicing
			case <-ckt.Halt.ReqStop.Chan:
				cleanupCkt(ckt, true)
				continue
			case <-pb.Halt.ReqStop.Chan:
				return
			case <-done:
				return
			}
		case msgerr := <-pb.ErrorsIn:
			// per srv.go:670 handleReply_to_CallID_ToPeerID()
			// CallError, CallPeerError get here.

			callID := msgerr.HDR.CallID
			ckt, ok := m[callID]
			vv("pump %v: ckt ok=%v on errorsIn", name, ok)
			if !ok {
				vv("%v: arg. no ckt avail for callID = '%v' on msgerr", name, callID)
				continue
			}
			////zz("pump %v: (ckt %v) sees msgerr='%v'", name, ckt.Name, msgerr)

			// these are on ReadsIn above, not ErrorsIn, per handleReply_to_CallID_ToPeerID.
			// if msgerr.HDR.Typ == CallPeerEndCircuit {
			// 	////zz("pump %v: (ckt %v) sees msgerr CallPeerEndCircuit in msgerr: '%v'", name, ckt.Name, msgerr)
			// 	cleanupCkt(ckt, false)
			// 	continue
			// }

			fragerr := ckt.ConvertMessageToFragment(msgerr)
			select {
			case ckt.Errors <- fragerr:
			case <-ckt.Halt.ReqStop.Chan:
				cleanupCkt(ckt, true)
				continue
			case <-pb.Halt.ReqStop.Chan:
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

func (pb *LocalPeer) TellRemoteWeShutdown(rem *RemotePeer) {

	shut := &Message{}
	shut.HDR.Created = time.Now()
	shut.HDR.From = pb.NetAddr
	shut.HDR.Typ = CallPeerFromIsShutdown
	shut.HDR.FromPeerID = pb.PeerID

	// pb.ctx is probably unusable by now as already cancelled.
	ctxB := context.Background()

	shut.HDR.To = rem.NetAddr
	shut.HDR.ToPeerID = rem.PeerID
	shut.HDR.Serial = issueSerial()
	shut.HDR.ServiceName = rem.RemoteServiceName

	pb.U.SendOneWayMessage(ctxB, shut, -1) // -1 => no blocking
}
