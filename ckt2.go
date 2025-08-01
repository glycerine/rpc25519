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
func (p *peerAPI) StartRemotePeerAndGetCircuit(lpb *LocalPeer, circuitName string, frag *Fragment, remotePeerServiceName, remoteAddr string, errWriteDur time.Duration, waitForAck bool) (ckt *Circuit, err error) {

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
	_ = hhalt
	if waitForAck {
		vv("waitForAck true in StartRemotePeerAndGetCircuit") // seen
		responseCh = make(chan *Message, 10)
		responseID := NewCallID("responseCallID for cktID(" + circuitID + ") in StartRemotePeerAndGetCircuit")
		vv("responseID = '%v'; alias='%v'; responseCh=%p", responseID, AliasDecode(responseID), responseCh)
		frag.SetSysArg("fragRPCtoken", responseID)
		p.u.GetReadsForCallID(responseCh, responseID)
		// verify retreivable
		ch2 := p.u.GetChanInterestedInCallID(responseID)
		if ch2 != responseCh {
			panic("GetChanInterestedInCallID borked")
		}
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
	lpb.Remotes.Set(peerID, rpb)

	// set firstFrag here
	ckt, _, err = lpb.newCircuit(circuitName, rpb, circuitID, frag, errWriteDur, false, onOriginLocalSide)
	if err != nil {
		return nil, err
	}

	if waitForAck {
		select { // 410 hung here
		case <-time.After(10 * time.Second): // DEBUG TODO remove this.
			panic("did not get response in 10 seconds")
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
			if responseMsg.HDR.FromPeerID != peerID {
				// this is the whole point of the WaitForAck (2),
				// to get an accurate rpb.PeerID if the
				// peer was already running and had
				// already assigned a PeerID to itself.
				rpb.PeerID = responseMsg.HDR.FromPeerID

				lpb.Remotes.Del(peerID)
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
