package rpc25519

import (
	"context"
	"fmt"
	"github.com/glycerine/idem"
	"net/url"
	"time"
)

// PreferExtantRemotePeerGetCircuit is similar
// to StartRemotePeerAndGetCircuit, but we
// try to avoid starting a new peer because
// we assume that one is already running from
// server/process bootup. We just need
// to discover it by searching for the remotePeerServiceName.
//
// We will start one if there is not an existing
// peer, but otherwise we will talk to the
// extant peer on the remote. We always
// wait to hear from remote before returning,
// as as to supply the correct RemotePeerID
// or an error. This is similar to using
// WaitForAck=true in a call to the
// sibling method StartRemotePeerAndGetCircuit.
//
// remoteAddr only needs to provide
// "tcp://host:port" but we will trim
// it down if it is a longer URL.
//
// If autoSendNewCircuitCh != nil, then we automatically
// send to autoSendNewCircuitCh <- ckt if get
// a circuit back without error.
func (p *peerAPI) PreferExtantRemotePeerGetCircuit(callCtx context.Context, lpb *LocalPeer, circuitName string, frag *Fragment, remotePeerServiceName, remotePeerServiceNameVersion, remoteAddr string, errWriteDur time.Duration, autoSendNewCircuitCh chan *Circuit, waitForAck bool) (ckt *Circuit, ackMsg *Message, madeNewAutoCli bool, onlyPossibleAddr string, err error) {

	preferExtant := true

	ckt, ackMsg, madeNewAutoCli, onlyPossibleAddr, err = p.implRemotePeerAndGetCircuit(callCtx, lpb, circuitName, frag, remotePeerServiceName, remotePeerServiceNameVersion, remoteAddr, errWriteDur, waitForAck, preferExtant)

	if err == nil && ckt != nil && autoSendNewCircuitCh != nil {
		// automatically send along the new ckt
		select {
		case autoSendNewCircuitCh <- ckt:
			//vv("PreferExtantRemotePeerGetCircuit sent new ckt to '%v' on autoSendNewCircuitCh = %p", ckt.RemotePeerName, autoSendNewCircuitCh)
		case <-ckt.Halt.ReqStop.Chan:
		case <-ckt.Context.Done():
		case <-lpb.Ctx.Done():
		case <-callCtx.Done():
		case <-p.u.GetHostHalter().ReqStop.Chan:
		}
	}
	return
}

// StartRemotePeerAndGetCircuit is a combining/compression of
// StartRemotePeer and NewCircuitToPeerURL together into one
// compact roundtrip.
//
// Typically when we start a remote peer we
// also want to get a circuit, to save a 2nd
// network round trip. This call provides that.
//
// When waitForAck is true we wait for an ack back
// (or an error) from the remote peer -- just
// like PreferExtantRemotePeerGetCircuit
// always does. One possible error is that
// the requested remotePeerServiceName is
// not registered -- perhaps because of a
// typo in the name -- or we may
// be talking to the wrong host.
//
// remoteAddr only needs to provide
// "tcp://host:port" but we will trim
// it down if it is a longer URL.
func (p *peerAPI) StartRemotePeerAndGetCircuit(lpb *LocalPeer, circuitName string, frag *Fragment, remotePeerServiceName, remotePeerServiceNameVersion, remoteAddr string, errWriteDur time.Duration, waitForAck bool, autoSendNewCircuitCh chan *Circuit, preferExtant bool) (ckt *Circuit, ackMsg *Message, madeNewAutoCli bool, onlyPossibleAddr string, err error) {

	ckt, ackMsg, madeNewAutoCli, onlyPossibleAddr, err = p.implRemotePeerAndGetCircuit(lpb.Ctx, lpb, circuitName, frag, remotePeerServiceName, remotePeerServiceNameVersion, remoteAddr, errWriteDur, waitForAck, preferExtant)

	if err == nil && ckt != nil {
		if ckt.loopy != nil {
			select {
			case ckt.loopy.cktServedAdd <- ckt:
				//vv("peerAPI.StartRemotePeerAndGetCircuit sent cktServedAdd <- ckt")
			case <-ckt.Halt.ReqStop.Chan:
			case <-ckt.Context.Done():
			case <-lpb.Ctx.Done():
			case <-p.u.GetHostHalter().ReqStop.Chan:
			}
		}
		if autoSendNewCircuitCh != nil {
			// automatically send along the new ckt
			select {
			case autoSendNewCircuitCh <- ckt:
				//vv("peerAPI.StartRemotePeerAndGetCircuit sent new ckt to '%v' on autoSendNewCircuitCh = %p", ckt.RemotePeerName, autoSendNewCircuitCh)
			case <-ckt.Halt.ReqStop.Chan:
			case <-ckt.Context.Done():
			case <-lpb.Ctx.Done():
			case <-p.u.GetHostHalter().ReqStop.Chan:
			}
		}
	}
	return
}

// if we need it and can now, set ckt.loopy
func (lpb *LocalPeer) setLoopy(ckt *Circuit) {
	if ckt.loopy != nil {
		return
	}
	if lpb.PeerAPI.isCli && lpb.PeerAPI.cliLoopy != nil {
		ckt.loopy = lpb.PeerAPI.cliLoopy
		return
	}
	if lpb.PeerAPI.remote2pair == nil {
		panicf("need ckt.loopy set here! how do we do ckt.loopy, _ = s.remote2pair.Get(ckt.RpbTo.NetAddr='%v') from here?? ", ckt.RpbTo.NetAddr)
	}
	pair, ok := lpb.PeerAPI.remote2pair.Get(ckt.RpbTo.NetAddr)
	if !ok {
		panicf("need ckt.loopy set here! remote2pair did NOT have ckt.RpbTo.NetAddr='%v'. how do we do it?", ckt.RpbTo.NetAddr)
	}
	ckt.loopy = pair.loopy
}

func (p *peerAPI) implRemotePeerAndGetCircuit(callCtx context.Context, lpb *LocalPeer, circuitName string, frag *Fragment, remotePeerServiceName, remotePeerServiceNameVersion, remoteAddr string, errWriteDur time.Duration, waitForAck bool, preferExtant bool) (ckt *Circuit, ackMsg *Message, madeNewAutoCli bool, onlyPossibleAddr string, err0 error) {

	if lpb.Halt.ReqStop.IsClosed() {
		return nil, nil, false, "", ErrHaltRequested
	}

	//vv("top implRemotePeerAndGetCircuit()")

	// this next looks to be the line that causes the jsync tests
	// to hang? yep! don't do this! somehow
	// destroys the info that tests like jsync/440 wait for.
	// frag = p.u.newFragment()

	circuitID := NewCallID(circuitName)
	if frag == nil {
		frag = p.u.newFragment()
	}
	frag.Created = time.Now()
	frag.CircuitID = circuitID
	frag.ToPeerServiceName = remotePeerServiceName
	frag.FromPeerServiceName = lpb.PeerServiceName
	frag.SetSysArg("fromServiceName", lpb.PeerServiceName)
	frag.SetSysArg("fromBaseServerName", lpb.BaseServerName)
	frag.SetSysArg("fromBaseServerAddr", lpb.BaseServerAddr)
	frag.SetSysArg("circuitName", circuitName)
	frag.FromPeerID = lpb.PeerID
	frag.FromPeerName = lpb.PeerName

	var pleaseAssignNewRemotePeerID string
	if preferExtant {
		// leave pleaseAssignNewRemotePeerID as empty string
		frag.Typ = CallPeerStartCircuitAtMostOne
	} else {
		frag.Typ = CallPeerStartCircuitTakeToID

		// For CallPeerStartCircuitTakeToID we can
		// reliably make up a PeerID for them because
		// they aren't live yet anyhow, and save it locally,
		// and just have the remote side adopt it!

		// We *could* do this (make up a PeerID, and
		// set pleaseAssignNewPeerID on the frag) for
		// CallPeerStartCircuitAtMostOne calls too,
		// but then the logging gets confusing because
		// we have to understand which is the "guess" and
		// which is the final RemotePeerID. We have
		// to update to our final one when it arrives.
		//
		// That is, the made-up RemotePeerID guess will be correct
		// only if CallPeerStartCircuitAtMostOne
		// was forced to spin up a new PeerID
		// on the remote because there was not an extant one.
		//
		// But otherwise (99.9% of the time we expect) the
		// guess would just be wrong and thus confusing
		// in the logs. Better to log an empty RemotePeerID
		// such that we can know it is not realiably known
		// until the CallPeerCircuitEstablishedAck comes
		// back and confirms it for us.

		pleaseAssignNewRemotePeerID = NewCallID(remotePeerServiceName)
		frag.ToPeerID = pleaseAssignNewRemotePeerID
		frag.SetSysArg("pleaseAssignNewPeerID", pleaseAssignNewRemotePeerID)
		//AliasRegister(frag.ToPeerID, frag.ToPeerID+" ("+remotePeerServiceName+")")
	}

	var responseCh chan *Message
	var responseErrCh chan *Message
	var hhalt *idem.Halter
	_ = hhalt
	if waitForAck {
		//vv("waitForAck true in StartRemotePeerAndGetCircuit") // seen
		responseCh = make(chan *Message, 10)
		responseErrCh = make(chan *Message, 10)
		responseID := NewCallID("responseCallID for cktID(" + circuitID + ") in StartRemotePeerAndGetCircuit")
		//vv("responseID = '%v'; alias='%v'; responseCh=%p", responseID, AliasDecode(responseID), responseCh)
		frag.SetSysArg("fragRPCtoken", responseID)
		p.u.GetReadsForCallID(responseCh, responseID)
		p.u.GetErrorsForCallID(responseErrCh, responseID)

		hhalt = p.u.GetHostHalter()
		defer p.u.UnregisterChannel(responseID, CallIDReadMap)
		defer p.u.UnregisterChannel(responseID, CallIDErrorMap)
	}

	msg := frag.ToMessage()

	msg.HDR.Created = time.Now()
	msg.HDR.From = p.u.LocalAddr()

	// fix up if remoteAddr happens to be a full/partial URL
	// instead of just the netAddr part
	netAddr, _, _, _, err := ParsePeerURL(remoteAddr)
	panicOn(err)
	//vv("got netAddr = '%v' from remoteAddr = '%v'", netAddr, remoteAddr)
	msg.HDR.To = netAddr
	// was: (no ParsePeerURL prior):
	//msg.HDR.To = remoteAddr

	// server will return "" because many possible clients,
	// but this can still help out the user on the client
	// by getting the right address.
	r := p.u.RemoteAddr()
	if r != "" {
		// we are on the client
		if r != remoteAddr {
			if portsAgree(r, remoteAddr) {
				// try it... we want to accept
				// tcp://100.114.32.72:7000  and tcp://127.0.0.1:7000
				// which are both local host port 7000.
			} else {
				err0 = fmt.Errorf("client peer error on implRemotePeerAndGetCircuit: remoteAddr should be '%v' (that we are connected to), rather than the '%v' which was requested. Otherwise your request will fail. stack=\n%v\n", r, remoteAddr, stack())
				ckt = nil
				ackMsg = nil
				onlyPossibleAddr = r
				return
			}
		}
	}

	//vv("in implRemotePeerAndGetCircuit(waitForAck='%v', preferExtant='%v'): remoteAddr() -> r='%v'; msg='%v'", waitForAck, preferExtant, r, msg)

	// this effectively is all that happens to set
	// up the circuit.
	ctx := lpb.Ctx
	if callCtx != nil {
		ctx = callCtx
	}
	var loopy *LoopComm
	madeNewAutoCli, loopy, err = p.u.SendOneWayMessage(ctx, msg, errWriteDur)
	_ = loopy
	if err != nil {
		err0 = fmt.Errorf("error requesting CallPeerStartCircuit from remote: '%v'; netAddr='%v'; remoteAddr='%v'", err, netAddr, remoteAddr)
		ckt = nil
		ackMsg = nil
		return
	}

	rpb := &RemotePeer{
		LocalPeer: lpb,
		PeerID:    pleaseAssignNewRemotePeerID,
		//PeerName:         unknown as of yet
		NetAddr:                      netAddr,
		RemoteServiceName:            remotePeerServiceName,
		RemotePeerServiceNameVersion: remotePeerServiceNameVersion,
	}
	if pleaseAssignNewRemotePeerID != "" {
		lpb.Remotes.Set(pleaseAssignNewRemotePeerID, rpb)
	}

	// set firstFrag here
	ckt, _, _, err = lpb.newCircuit(circuitName, rpb, circuitID, frag, errWriteDur, false, onOriginLocalSide, preferExtant)
	if err != nil {
		err0 = err
		ckt = nil
		ackMsg = nil
		return
	}
	//vv("madeNewAutoCli = %v", madeNewAutoCli)

	// try hard to get ckt.loopy set.
	if ckt.loopy == nil {
		if loopy != nil {
			ckt.loopy = loopy
		} else {
			lpb.setLoopy(ckt)
		}
	}

	var timeoutCh <-chan time.Time
	if errWriteDur > 0 {
		timeoutCh = time.After(errWriteDur)
	}
	//vv("%v start waiting for ack at '%v' from netAddr='%v' remotePeerServiceName='%v'", lpb.PeerName, lpb.NetAddr, netAddr, remotePeerServiceName)
	if waitForAck {
		select {
		case <-timeoutCh:
			err0 = ErrTimeout
			return
		case errMsg := <-responseErrCh:
			// errMsg is still our ackMsg with
			// "#LimitedExistingPeerID_first_url" details, so return it.
			//vv("got errMsg = '%v'", errMsg)
			err0 = fmt.Errorf("responseErrCh in implRemotePeerAndGetCircuit sending to remote(frag.Typ='%v'; netAddr='%v'); errMsg.JobErrs='%v'; errMsg='%v'", frag.Typ, netAddr, errMsg.JobErrs, errMsg)
			ackMsg = errMsg
			ckt = nil
			return
		case ackMsg = <-responseCh:
			//vv("got ackMsg! yay! = '%v'", ackMsg)
			if ackMsg.LocalErr != nil {
				err0 = ackMsg.LocalErr
				return
			}
			if ackMsg.JobErrs != "" {
				err0 = fmt.Errorf("error on ackMsg := <-responseCh: response.JobErrs = '%v'", ackMsg.JobErrs)
				return
			}
			// this is the a minor point of the WaitForAck, (1)
			// since we already likely have the name elsewhere,
			// like the a peer-naming config file.
			rpb.PeerName = ackMsg.HDR.FromPeerName

			if ackMsg.HDR.FromPeerID == "" {
				panic(fmt.Sprintf("ackMsg.FromPeerID was empty string; should never happen! ackMsg = '%v'", ackMsg))
			}
			if ackMsg.HDR.FromPeerID != pleaseAssignNewRemotePeerID {
				// this is the whole point of the WaitForAck (2),
				// to get an accurate rpb.PeerID if the
				// peer was already running and had
				// already assigned a PeerID to itself.
				//vv("setting actual PeerID from guess '%v' -> actual '%v'", rpb.PeerID, ackMsg.HDR.FromPeerID)
				rpb.PeerID = ackMsg.HDR.FromPeerID
				rpb.PeerName = ackMsg.HDR.FromPeerName
				ckt.RemotePeerID = ackMsg.HDR.FromPeerID
				ckt.RemotePeerName = ackMsg.HDR.FromPeerName

				if pleaseAssignNewRemotePeerID != "" {
					lpb.Remotes.Del(pleaseAssignNewRemotePeerID)
				}
				lpb.Remotes.Set(rpb.PeerID, rpb)
			}
		case <-ckt.Context.Done():
			return nil, nil, madeNewAutoCli, "", ErrContextCancelled
		case <-lpb.Ctx.Done():
			return nil, nil, madeNewAutoCli, "", ErrContextCancelled
		case <-hhalt.ReqStop.Chan:
			return nil, nil, madeNewAutoCli, "", ErrHaltRequested
		case <-callCtx.Done():
			return nil, nil, madeNewAutoCli, "", ErrContextCancelled
		}
	}
	return
}

func (s *LocalPeer) PreferExtantRemotePeerGetCircuit(callCtx context.Context, circuitName string, frag *Fragment, remotePeerServiceName, remotePeerServiceNameVersion, remoteAddr string, errWriteDur time.Duration, autoSendNewCircuitCh chan *Circuit, waitForAck bool) (ckt *Circuit, ackMsg *Message, madeNewAutoCli bool, onlyPossibleAddr string, err error) {

	preferExtant := true

	ckt, ackMsg, madeNewAutoCli, onlyPossibleAddr, err = s.PeerAPI.implRemotePeerAndGetCircuit(callCtx, s, circuitName, frag, remotePeerServiceName, remotePeerServiceNameVersion, remoteAddr, errWriteDur, waitForAck, preferExtant)

	//vv("LocalPeer.PreferExtantRemotePeerGetCircuit back, have err='%v'; autoSendNewCircuitCh = %p; ckt='%v'", err, autoSendNewCircuitCh, ckt)
	if err == nil && ckt != nil && autoSendNewCircuitCh != nil {
		// automatically send along the new ckt
		select {
		case autoSendNewCircuitCh <- ckt:
			//vv("LocalPeer.PreferExtantRemotePeerGetCircuit sent new ckt to '%v' on autoSendNewCircuitCh = %p", ckt.RemotePeerName, autoSendNewCircuitCh)
		case <-ckt.Halt.ReqStop.Chan:
		case <-ckt.Context.Done():
		case <-s.Halt.ReqStop.Chan:
		case <-callCtx.Done():
		}
	}
	return
}

func (s *LocalPeer) StartRemotePeerAndGetCircuit(lpb *LocalPeer, circuitName string, frag *Fragment, remotePeerServiceName, remotePeerServiceNameVersion, remoteAddr string, errWriteDur time.Duration, waitForAck bool, autoSendNewCircuitCh chan *Circuit) (ckt *Circuit, ackMsg *Message, madeNewAutoCli bool, onlyPossibleAddr string, err error) {

	ckt, ackMsg, madeNewAutoCli, onlyPossibleAddr, err = s.PeerAPI.implRemotePeerAndGetCircuit(s.Ctx, lpb, circuitName, frag, remotePeerServiceName, remotePeerServiceNameVersion, remoteAddr, errWriteDur, waitForAck, false)

	if err == nil && ckt != nil && autoSendNewCircuitCh != nil {
		// automatically send along the new ckt
		select {
		case autoSendNewCircuitCh <- ckt:

		case <-ckt.Halt.ReqStop.Chan:
		case <-ckt.Context.Done():
		case <-s.Halt.ReqStop.Chan:
		case <-s.Ctx.Done():
		}
	}

	return
}

func portsAgree(rs, ts string) bool {
	rp, err := url.Parse(rs)
	panicOn(err)
	tp, err := url.Parse(ts)
	panicOn(err)
	rport := rp.Port()
	tport := tp.Port()
	return rport == tport
}
