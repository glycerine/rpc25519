package rpc25519

import (
	"fmt"
	"github.com/glycerine/idem"
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
// (similar to using WaitForAck true
// in StartRemotePeerAndGetCircuit).
func (p *peerAPI) PreferExtantRemotePeerGetCircuit(lpb *LocalPeer, circuitName string, frag *Fragment, remotePeerServiceName, remoteAddr string, errWriteDur time.Duration) (ckt *Circuit, err error) {
	waitForAck := true
	preferExtant := true
	return p.implRemotePeerAndGetCircuit(lpb, circuitName, frag, remotePeerServiceName, remoteAddr, errWriteDur, waitForAck, preferExtant)
}

// StartRemotePeerAndGetCircuit is a combining/compression of
// StartRemotePeer and NewCircuitToPeerURL together into one
// compact roundtrip.
// We actually want, when we start a remote peer, to also get a
// circuit, to save a 2nd round trip!
//
// waitForAck true => wait for an ack back from the remote peer.
func (p *peerAPI) StartRemotePeerAndGetCircuit(lpb *LocalPeer, circuitName string, frag *Fragment, remotePeerServiceName, remoteAddr string, errWriteDur time.Duration, waitForAck bool) (ckt *Circuit, err error) {

	return p.implRemotePeerAndGetCircuit(lpb, circuitName, frag, remotePeerServiceName, remoteAddr, errWriteDur, waitForAck, false)
}

func (p *peerAPI) implRemotePeerAndGetCircuit(lpb *LocalPeer, circuitName string, frag *Fragment, remotePeerServiceName, remoteAddr string, errWriteDur time.Duration, waitForAck bool, preferExtant bool) (ckt *Circuit, err error) {

	if lpb.Halt.ReqStop.IsClosed() {
		return nil, ErrHaltRequested
	}

	//firstFrag := frag
	// this next looks to be the line that causes the jsync tests
	// to hang? yep! don't do this! somehow
	// destroys the info that tests like 440 wait for.
	// frag = p.u.newFragment()

	circuitID := NewCallID(circuitName)
	if frag == nil {
		frag = p.u.newFragment()
	}
	frag.CircuitID = circuitID
	frag.ServiceName = remotePeerServiceName
	frag.SetSysArg("fromServiceName", lpb.PeerServiceName)
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
		vv("waitForAck true in StartRemotePeerAndGetCircuit") // seen
		responseCh = make(chan *Message, 10)
		responseErrCh = make(chan *Message, 10)
		responseID := NewCallID("responseCallID for cktID(" + circuitID + ") in StartRemotePeerAndGetCircuit")
		vv("responseID = '%v'; alias='%v'; responseCh=%p", responseID, AliasDecode(responseID), responseCh)
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
	msg.HDR.To = remoteAddr

	// server will return "" because many possible clients,
	// but this can still help out the user on the client
	// by getting the right address.
	r := p.u.RemoteAddr()
	if r != "" {
		// we are on the client
		if r != remoteAddr {
			return nil, fmt.Errorf("client peer error on StartRemotePeer: remoteAddr should be '%v' (that we are connected to), rather than the '%v' which was requested. Otherwise your request will fail.", r, remoteAddr)
		}
	}

	vv("in implRemotePeerAndGetCircuit(waitForAck='%v', preferExtant='%v'): msg.HDR='%v'", waitForAck, preferExtant, msg.HDR.String())

	// this effectively is all that happens to set
	// up the circuit.
	ctx := lpb.Ctx
	err, _ = p.u.SendOneWayMessage(ctx, msg, errWriteDur)
	if err != nil {
		return nil, fmt.Errorf("error requesting CallPeerStartCircuit from remote: '%v'", err)
	}

	rpb := &RemotePeer{
		LocalPeer: lpb,
		PeerID:    pleaseAssignNewRemotePeerID,
		//PeerName:         unknown as of yet
		NetAddr:           remoteAddr,
		RemoteServiceName: remotePeerServiceName,
	}
	if pleaseAssignNewRemotePeerID != "" {
		lpb.Remotes.Set(pleaseAssignNewRemotePeerID, rpb)
	}

	// set firstFrag here
	ckt, _, err = lpb.newCircuit(circuitName, rpb, circuitID, frag, errWriteDur, false, onOriginLocalSide)
	if err != nil {
		return nil, err
	}

	var waitCh <-chan time.Time
	if errWriteDur > 0 {
		waitCh = time.After(errWriteDur)
	}
	if waitForAck {
		select { // server->client 410 hung here.
		case <-waitCh:
			return nil, ErrTimeout

		case errMsg := <-responseErrCh:
			vv("got errMsg = '%v'", errMsg)
			return nil, fmt.Errorf("error sending '%v' to remote: %v", frag.Typ, errMsg.JobErrs)
		case responseMsg := <-responseCh:
			vv("got responseMsg! yay! = '%v'", responseMsg)
			if responseMsg.LocalErr != nil {
				err = responseMsg.LocalErr
				return
			}
			if responseMsg.JobErrs != "" {
				err = fmt.Errorf("error on responseMsg := <-responseCh: response.JobErrs = '%v'", responseMsg.JobErrs)
				return
			}
			// this is the a minor point of the WaitForAck, (1)
			// since we already likely have the name elsewhere,
			// like the a peer-naming config file.
			rpb.PeerName = responseMsg.HDR.FromPeerName

			if responseMsg.HDR.FromPeerID == "" {
				panic(fmt.Sprintf("responseMsg.FromPeerID was empty string; should never happen! responseMsg = '%v'", responseMsg))
			}
			if responseMsg.HDR.FromPeerID != pleaseAssignNewRemotePeerID {
				// this is the whole point of the WaitForAck (2),
				// to get an accurate rpb.PeerID if the
				// peer was already running and had
				// already assigned a PeerID to itself.
				vv("setting actual PeerID from guess '%v' -> actual '%v'", rpb.PeerID, responseMsg.HDR.FromPeerID)
				rpb.PeerID = responseMsg.HDR.FromPeerID
				rpb.PeerName = responseMsg.HDR.FromPeerName
				ckt.RemotePeerID = responseMsg.HDR.FromPeerID
				ckt.RemotePeerName = responseMsg.HDR.FromPeerName

				if pleaseAssignNewRemotePeerID != "" {
					lpb.Remotes.Del(pleaseAssignNewRemotePeerID)
				}
				lpb.Remotes.Set(rpb.PeerID, rpb)
			}
		case <-ckt.Context.Done():
			return nil, ErrContextCancelled
		case <-lpb.Ctx.Done():
			return nil, ErrContextCancelled
		case <-hhalt.ReqStop.Chan:
			return nil, ErrHaltRequested
		}
	}
	return
}
