package rpc25519

import (
	"fmt"
	"github.com/glycerine/idem"
	"time"
)

// StartRemotePeerAndGetCircuit is a combining/compression of
// StartRemotePeer and NewCircuitToPeerURL together into one
// compact roundtrip.
// We actually want, when we start a remote peer, to also get a
// circuit, to save a 2nd round trip!
//
// waitForAck true => wait for an ack back from the remote peer.
func (p *peerAPI) StartRemotePeerAndGetCircuit(lpb *LocalPeer, circuitName string, firstFrag *Fragment, remotePeerServiceName, remoteAddr string, errWriteDur time.Duration, waitForAck bool) (ckt *Circuit, err error) {

	vv("top StartRemotePeerAndGetCircuit(%v)", circuitName) // seen 410

	if lpb.Halt.ReqStop.IsClosed() {
		return nil, ErrHaltRequested
	}

	frag := p.u.newFragment()
	circuitID := NewCallID(circuitName)
	frag.CircuitID = circuitID
	frag.Typ = CallPeerStartCircuitTakeToID

	frag.ServiceName = remotePeerServiceName
	frag.SetSysArg("fromServiceName", lpb.PeerServiceName)
	frag.SetSysArg("circuitName", circuitName)
	frag.FromPeerID = lpb.PeerID

	// we can make up a PeerID for them because
	// they aren't live yet anyhow, and save it locally,
	// and just have the remote side adopt it!

	pleaseAssignNewRemotePeerID := NewCallID(remotePeerServiceName)
	frag.ToPeerID = pleaseAssignNewRemotePeerID
	frag.SetSysArg("pleaseAssignNewPeerID", pleaseAssignNewRemotePeerID)
	//AliasRegister(frag.ToPeerID, frag.ToPeerID+" ("+remotePeerServiceName+")")

	var responseCh chan *Message
	var hhalt *idem.Halter
	if waitForAck {
		vv("waitForAck true in StartRemotePeerAndGetCircuit") // seen
		responseCh = make(chan *Message, 10)
		responseID := NewCallID("responseCallID for cktID(" + circuitID + ") in StartRemotePeerAndGetCircuit")
		vv("responseID = '%v'; alias='%v'", responseID, AliasDecode(responseID))
		frag.SetSysArg("RemotePeerID_ready_responseCallID", responseID)
		p.u.GetReadsForCallID(responseCh, responseID)
		hhalt = p.u.GetHostHalter()
		defer p.u.UnregisterChannel(responseID, CallIDReadMap)
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

	vv("StartRemotePeerAndGetCircuit(): msg.HDR='%v'", msg.HDR.String()) // seen 410

	// this effectively is all that happens to set
	// up the circuit.
	ctx := lpb.Ctx
	err, _ = p.u.SendOneWayMessage(ctx, msg, errWriteDur)
	if err != nil {
		return nil, fmt.Errorf("error requesting CallPeerStartCircuit from remote: '%v'", err)
	}

	peerID := pleaseAssignNewRemotePeerID
	rpb := &RemotePeer{
		LocalPeer: lpb,
		PeerID:    peerID,
		//PeerName:         ? unknown as of yet ? not sure.
		NetAddr:           remoteAddr, //netAddr,
		RemoteServiceName: remotePeerServiceName,
	}

	// allows pump.go to tell remotes we have shutdown
	lpb.Remotes.Set(peerID, rpb)

	// set firstFrag. send frag (but only if tellRemote true).
	ckt, ctx, err = lpb.newCircuit(circuitName, rpb, circuitID, firstFrag, errWriteDur, false, false)
	vv("back from lpb.newCircuit; waitForAck=%v, responseCh = %p", waitForAck, responseCh) // seen, nil!
	defer vv("defered print running StartRemotePeerAndGetCircuit")                         // not seen
	if err != nil {
		return nil, err
	}
	if waitForAck {
		select { // 410 hung here
		case responseMsg := <-responseCh:
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
			if responseMsg.HDR.FromPeerID != peerID {
				// this is the whole point of the WaitForAck (2),
				// to get an accurate rpb.PeerID if the
				// peer was already running and had
				// already assigned a PeerID to itself.
				rpb.PeerID = responseMsg.HDR.FromPeerID

				lpb.Remotes.Set(rpb.PeerID, rpb)
				lpb.Remotes.Del(peerID)
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
