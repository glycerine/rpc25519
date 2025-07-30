package rpc25519

import (
	"fmt"
	"time"
	//"github.com/glycerine/idem"
)

// StartRemotePeerAndGetCircuit is a combining/compression of
// StartRemotePeer and NewCircuitToPeerURL together into one
// compact roundtrip.
// We actually want, when we start a remote peer, to also get a
// circuit, to save a 2nd round trip!
func (p *peerAPI) StartRemotePeerAndGetCircuit(lpb *LocalPeer, circuitName string, frag *Fragment, remotePeerServiceName, remoteAddr string, errWriteDur time.Duration) (ckt *Circuit, err error) {

	if lpb.Halt.ReqStop.IsClosed() {
		return nil, ErrHaltRequested
	}
	ctx := lpb.Ctx

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

	//vv("StartRemotePeerAndGetCircuit(): msg.HDR='%v'", msg.HDR.String()) // "Typ":

	// this effectively is all that happens to set
	// up the circuit.
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

	// we want to be setting/sending firstFrag (frag) here, right?
	// we were not before...
	ckt, _, err = lpb.newCircuit(circuitName, rpb, circuitID, frag, errWriteDur, false)
	//ckt, _, err = lpb.newCircuit(circuitName, rpb, circuitID, nil, errWriteDur, false)
	if err != nil {
		return nil, err
	}
	return
}
