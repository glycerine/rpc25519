package rpc25519

import (
	"context"
	"fmt"
	"time"

	"github.com/glycerine/idem"
)

func prettyPrintCircuitMap(m map[string]*Circuit) (s string) {
	s = fmt.Sprintf("circuit map holds %v circuits:\n", len(m))
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

	defer func() {
		//vv("%v LocalPeer.PeerbackPump all-finished; pb= %p", pb.PeerServiceName, pb) // 2x seen, "simgrid"
		pb.Halt.ReqStop.Close()
		pb.Halt.Done.Close()
	}()

	name := pb.PeerServiceName
	_ = name
	//vv("%v: PeerbackPump top.", name)

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

		_, inMap := m[ckt.CircuitID]

		//vv("%v: cleanupCkt running for ckt '%v'. notifyPeer=%v; len(m)=%v before cleanup. CircuitID='%v'; inMap = %v\n m = '%v'", name, ckt.LocalCircuitURL(), notifyPeer, len(m), ckt.CircuitID, inMap, prettyPrintCircuitMap(m))

		if !inMap {
			// only send to peer if it is still in our map, to avoid sending
			// more than once if we can... may be futile (there is shutdown
			// race to see who notifies us first, the notifyPeer or the !notifyPeer)
			// but we can try.
			return
		}

		if notifyPeer {
			// Tell our peer we are going down.
			frag := pb.NewFragment()
			frag.Typ = CallPeerEndCircuit
			// Transmit back reason for shutdown if we can.
			// Q: will this mess up delivery (to Errors instead of Reads?)
			// A: seems okay for now. Not extensively tested though.
			if reason, ok := ckt.Halt.ReqStop.Reason(); ok && reason != nil {
				frag.Err = reason.Error()
			}
			// this is blocking, so we cannot finish circuits,
			// and then we are not servicing reads. Thus both cli
			// and srv can be blocked waiting to send, resulting
			// deadlock. Implement the errWaitdur -2 and background
			// close mechanism below to prevent deadlock.
			//pb.SendOneWay(ckt, frag, -1) // no blocking

			// to enable background close, get independent of ckt:
			frag.CircuitID = ckt.CircuitID
			frag.FromPeerID = ckt.LocalPeerID
			frag.ToPeerID = ckt.RemotePeerID
			msg := ckt.ConvertFragmentToMessage(frag)
			pb.FreeFragment(frag)

			// note srv.go might panic if the peer port
			// is closed, as they might already also
			// be down on system shutdown/test cleanup.
			// Don't freak out.
			func() {
				defer func() {
					r := recover()
					if r != nil {
						alwaysPrintf("%v: cleanupCircuit, ignoring common "+
							"panic on system shutdown: '%v' %v", name, r, stack())
					}
				}()
				err, queueSendCh := pb.U.SendOneWayMessage(pb.Ctx, msg, -2)
				if err == ErrAntiDeadlockMustQueue {
					go closeCktInBackgroundToAvoidDeadlock(queueSendCh, msg, pb.Halt)
				}
			}()
		}
		ckt.Canc(fmt.Errorf("pump cleanupCkt(notifyPeer=%v) cancelling ckt.Context.", notifyPeer))
		pb.U.UnregisterChannel(ckt.CircuitID, CallIDReadMap)
		pb.U.UnregisterChannel(ckt.CircuitID, CallIDErrorMap)

		ckt.Halt.ReqStop.Close()
		ckt.Halt.Done.Close()
		pb.Halt.ReqStop.RemoveChild(ckt.Halt.ReqStop)

		if pb.AutoShutdownWhenNoMoreCircuits && len(m) == 0 {
			//zz("%v: peerbackPump exiting on autoShutdownWhenNoMoreCircuits", name)
			pb.Halt.ReqStop.Close()
		}
		delete(m, ckt.CircuitID)
	}
	defer func() {
		//vv("%v: peerbackPump exiting. closing all remaining circuits (%v).", name, len(m))
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

		r := recover()
		if r != nil {
			alwaysPrintf("arg. LocalPeer.peerbackPump() exiting on panic: '%v'", r)
			panic(r)
		}
	}()

	done := pb.Ctx.Done()
	for {
		//vv("%v %p: pump loop top of select. pb.handleChansNewCircuit = %p", name, pb, pb.TellPumpNewCircuit)
		select {
		case <-pb.Halt.ReqStop.Chan:
			//vv("%v %p: pump loop pb.Halt.ReqStop.Chan shutdown received; pb = %p", name, pb, pb)
			return

		case query := <-pb.QueryCh:
			// query is &queryLocalPeerPump{}
			query.OpenCircuitCount = len(m)
			close(query.Ready)

		case ckt := <-pb.TellPumpNewCircuit:
			//vv("%v pump: ckt := <-pb.TellPumpNewCircuit: for ckt='%v'", name, ckt.Name)
			m[ckt.CircuitID] = ckt
			pb.Halt.AddChild(ckt.Halt)

		case ckt := <-pb.HandleCircuitClose:
			//vv("%v pump: ckt := <-pb.HandleCircuitClose: for ckt='%v'", name, ckt.Name) // not seen
			cleanupCkt(ckt, true)

		case msg := <-pb.ReadsIn:

			if msg.HDR.Typ == CallPeerFromIsShutdown && msg.HDR.FromPeerID != pb.PeerID {
				rpb, n, ok := pb.Remotes.GetValNDel(msg.HDR.FromPeerID)
				if ok {
					//vv("%v: got notice of shutdown of peer '%v'", name, AliasDecode(msg.HDR.FromPeerID)) // not seen
					_ = rpb
					//zz("what more do we need to do with rpb on its shutdown?")
				}
				if n == 0 {
					//vv("no remote peers left ... we could shut ourselves down to save memory?")
					if pb.AutoShutdownWhenNoMorePeers {
						//vv("%v: lbp.autoShutdownWhenNoMorePeers true, closing up", name)
						return
					}
				}
			}

			callID := msg.HDR.CallID
			ckt, ok := m[callID]
			//vv("pump %v: sees readsIn msg, ckt ok=%v", name, ok)
			//if msg.HDR.FragOp == 15 {
			//vv("pump %v: sees FragOp==OpRsync_AckBackFIN_ToTaker, readsIn msg, ckt ok=%v", name, ok)
			//}
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
			//vv("pump %v: (ckt %v) sees msg='%v'", name, ckt.Name, msg)

			if msg.HDR.Typ == CallPeerEndCircuit {
				//vv("pump %v: (ckt %v) sees msg CallPeerEndCircuit in msg: '%v'", name, ckt.Name, msg)
				cleanupCkt(ckt, false)
				//zz("pump %v: (ckt %v) sees msg CallPeerEndCircuit in msg. back from cleanupCkt, about to continue: '%v'", name, ckt.Name, msg)
				continue
			}

			frag := ckt.ConvertMessageToFragment(msg)
			//vv("got frag = '%v'", frag)

		wait5MoreSecondsBeforeCktShutdown:
			var delayedShutCtkTimeout *SimTimer
			var delayedShutCtkTimeoutC <-chan time.Time // nil typically
			var ckt5s []*Circuit
			var doReturn bool
			select {
			// was hung here on shutdown... tried adding this first case...
			// BUT VERY BAD: this makes the pump exit too early!
			// causes drop/loss of fragment we need in jysnc
			// dir_test 440 test; and 220 jsync/e2e_test.go too,
			// when the ckt shutdown races with the message send;
			// This was on frag.FragOp == OpRsync_AckBackFIN_ToTaker.
			// SO BIG WARNING: DO NOT ADD THIS CASE:
			//case ckt2 := <-pb.HandleCircuitClose:
			//	vv("%v pump: ckt2 := <-pb.HandleCircuitClose: for ckt2='%v'", name, ckt2.Name)
			//cleanupCkt(ckt2, true)

			// BUT we don't want to hang forever on ckt shutdown!
			// So try to pass the frag for another 5 seconds...
			// Any subsequent <-pb.HandleCircuitClose
			// will also get shutdown after the single
			// 5 seconds of additional time/attemp to
			// pass on the (likely AckBackFIN_ToTaker) frag.
			// Sigh: shutdown logical races are a pain.
			case <-delayedShutCtkTimeoutC: // nil unless len(ckt5s)>0
				// do the cleanup below
			case ckt5 := <-pb.HandleCircuitClose:
				// give 5 more seconds of trying to send on
				// case ckt.Reads <- frag below until
				// we actually do shutdown the ckt5
				//if ckt5 == ckt  // cannot do this, red 220 jsync test!
				// So actually they are different circuits(!)
				ckt5s = append(ckt5s, ckt5)
				if delayedShutCtkTimeout == nil { // only one 5s delay
					delayedShutCtkTimeout = pb.U.NewTimer(time.Second * 5)
					if delayedShutCtkTimeout == nil {
						// shutting down fully
						doReturn = true
					} else {
						delayedShutCtkTimeoutC = delayedShutCtkTimeout.C
						// try our select again, this time only
						// waiting for 5 seconds. We don't do this
						// universally to allow proper backpressure.
						goto wait5MoreSecondsBeforeCktShutdown
					}
				} else {
					// handle several case ckt5 := <-pb.HandleCircuitClose
					// in a row before our first 5 seconds are up.
					goto wait5MoreSecondsBeforeCktShutdown
				}
			case ckt.Reads <- frag: // server should be hung here, if peer code not servicing
				vv("pump sent frag on ckt! ckt='%v'; frag='%v'", ckt, frag)
			case <-ckt.Halt.ReqStop.Chan:
				//vv("<-ckt.Halt.ReqStop.Chan:")
				cleanupCkt(ckt, true)
				//continue // cleanup any delayedShutCtkTimeout below
			case <-pb.Halt.ReqStop.Chan:
				//vv("<-pb.Halt.ReqStop.Chan:")
				doReturn = true
			case <-done:
				//vv("<-done:")
				doReturn = true
			}
			// "continue"/cleanup
			if delayedShutCtkTimeout != nil {
				delayedShutCtkTimeout.Discard()
				delayedShutCtkTimeout = nil
			}
			for len(ckt5s) > 0 {
				ckt5 := ckt5s[0]
				ckt5s = ckt5s[1:]
				cleanupCkt(ckt5, true)
			}
			if doReturn {
				return
			}
		case msgerr := <-pb.ErrorsIn:
			// per srv.go:670 handleReply_to_CallID_ToPeerID()
			// CallError, CallPeerError get here.

			callID := msgerr.HDR.CallID
			ckt, ok := m[callID]
			//vv("pump %v: ckt ok=%v on errorsIn", name, ok)
			if !ok {
				//vv("%v: arg. no ckt avail for callID = '%v' on msgerr", name, callID)
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
	shut.HDR.ToPeerName = rem.PeerName
	shut.HDR.Serial = issueSerial()
	shut.HDR.ServiceName = rem.RemoteServiceName

	// -2 version => almost no blocking; err below if cannot send in 1 msec.
	err, queueSendCh := pb.U.SendOneWayMessage(ctxB, shut, -2)
	if err == ErrAntiDeadlockMustQueue {
		//vv("err == ErrAntiDeadlockMustQueue, closing in background goro")
		go closeCktInBackgroundToAvoidDeadlock(queueSendCh, shut, pb.Halt)
	}
}

func closeCktInBackgroundToAvoidDeadlock(queueSendCh chan *Message, msg *Message, halt *idem.Halter) {
	//vv("top closeCktInBackgroundToAvoidDeadlock b/c ErrAntiDeadlockMustQueue seen, closing ckt in background.")
	//defer vv("end closeCktInBackgroundToAvoidDeadlock b/c ErrAntiDeadlockMustQueue seen, closing ckt in background.")
	select {
	case queueSendCh <- msg:
	case <-halt.ReqStop.Chan:
	}
}
