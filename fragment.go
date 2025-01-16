package rpc25519

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	//"sync/atomic"
	"time"
	//"github.com/glycerine/loquet"
)

//go:generate greenpack

// Circuit is a handle to the two-way,
// asynchronous, communication channel
// between two Peers.
//
// It is returned from RemotePeer.NewCircuit(), or
// from LocalPeer.NewCircuitToPeerURL().
type Circuit struct {
	pbFrom *localPeerback
	pbTo   *remotePeerback

	localPeerID  string
	remotePeerID string

	localServiceName  string
	remoteServiceName string

	callID string
	ctx    context.Context
	canc   context.CancelFunc

	Reads  chan *Fragment // users should treat as read-only.
	Errors chan *Fragment // ditto.
}

// CircuitURL format: tcp://x.x.x.x:port/peerServiceName/peerID/circuitID
// where peerID and circuitID (same as our CallID type), and are
// base64 URL encoded. The IDs do not include the '/' character,
// and thus are "URL safe". This can be verified here:
// https://github.com/cristalhq/base64/blob/5cfa89a12c5dbd0e52b1da082a33dce278c52cdf/utils.go#L65
//
//	(circuitID is the CallID in the Message.HDR)
func (ckt *Circuit) LocalCircuitURL() string {
	return ckt.pbFrom.netAddr + "/" +
		ckt.localServiceName + "/" +
		ckt.localPeerID + "/" +
		ckt.callID
}

func (ckt *Circuit) RemoteCircuitURL() string {
	return ckt.pbTo.netAddr + "/" +
		ckt.remoteServiceName + "/" +
		ckt.remotePeerID + "/" +
		ckt.callID
}

// ID2 supplies the local and remote PeerIDs.
func (ckt *Circuit) ID2() (localPeerID, remotePeerID string) {
	return ckt.pbFrom.peerID, ckt.pbTo.peerID
}

func NewFragment() *Fragment {
	return &Fragment{
		Serial: issueSerial(),
	}
}

// Fragments are sent to, and read from,
// a Circuit by implementers of
// PeerServiceFunc. They are a simplified
// version of the underlying Message infrastructure.
//
// Note the first three fields are set by the
// sending machinery; any user settings will
// be overridden for FromPeerID, ToPeerID,
// and CircuitID.
type Fragment struct {
	// system metadata
	FromPeerID  string   `zid:"0"` // who sent us this Fragment.
	ToPeerID    string   `zid:"1"`
	CircuitID   string   `zid:"2"` // maps to Message.HDR.CallID.
	Serial      int64    `zid:"3"`
	Typ         CallType `zid:"4"` // one of the CallPeer CallTypes of hdr.go
	ServiceName string   `zid:"5"` // the registered PeerServiceName.

	// user supplied data
	FragOp      int               `zid:"6"`
	FragSubject string            `zid:"7"`
	FragPart    int64             `zid:"8"`
	Args        map[string]string `zid:"9"` // nil by default; make() it if you need it.
	Payload     []byte            `zid:"10"`
	Err         string            `zid:"11"` // distinguished field for error messages.
}

func (f *Fragment) String() string {
	return fmt.Sprintf(`&rpc25519.Fragment{
    "FromPeerID": %q,
    "ToPeerID": %q,
    "CircuitID": %q,
    "Serial": %v,
    "ServiceName": %q,
    "Typ": %s,
    "FragOp": %v,
    "FragSubject": %q,
    "FragPart": %v,
    "Args": %#v,
    "Payload": %v,
}`,
		aliasDecode(f.FromPeerID),
		aliasDecode(f.ToPeerID),
		f.CircuitID,
		f.Serial,
		f.ServiceName,
		f.Typ,
		f.FragOp,
		f.FragSubject,
		f.FragPart,
		f.Args,
		fmt.Sprintf("(len %v bytes)", len(f.Payload)),
		//string(f.Payload), // for debugging
	)

}

// remotePeerback is over the newPeerCh channel
// to a PeerServiceFunc.
// The "remote" are just the local proxy to the actual remote Peer
// that, on the remote node, is local to that node.
// Thus they are always children of localPeerbacks, created
// either deliberately locally with NewCircuit or received on newPeerChan
// from the remote who did NewCircuit.
type remotePeerback struct {
	localPeerback     *localPeerback
	netAddr           string // remote network://host:port (e.g. tcp://x.x.x.x:30238)
	peerID            string // the remote's PeerID
	incomingCkt       *Circuit
	peerURL           string
	remoteServiceName string
}

func (rpb *remotePeerback) PeerID() string {
	return rpb.peerID
}
func (rpb *remotePeerback) PeerServiceName() string {
	return rpb.localPeerback.peerServiceName
}
func (rpb *remotePeerback) PeerURL() string {
	return rpb.peerURL
}
func (rpb *remotePeerback) IncomingCircuit() (ckt *Circuit, ctx context.Context) {
	return rpb.incomingCkt, rpb.incomingCkt.ctx
}

// localPeerback in the backing behind each local instantiation of a PeerServiceFunc.
// local peers do reads on ch, get notified of new connections on newPeerChan.
// and create new outgoing connections with
type localPeerback struct {
	// local peers do reads, "remote" peers are for sending to.
	netAddr         string
	peerServiceName string
	peerAPI         *peerAPI
	u               UniversalCliSrv

	peerID    string // could be local or remote, only
	ctx       context.Context
	canc      context.CancelFunc
	newPeerCh chan RemotePeer

	readsIn  chan *Message
	errorsIn chan *Message

	handleChansNewCircuit chan *Circuit
	handleCircuitClose    chan *Circuit

	// mut protect the remotes map below
	mut sync.Mutex

	// remotes key is the remote PeerID
	remotes map[string]*remotePeerback
}

func (s *localPeerback) Close() {
	s.canc()
}
func (s *localPeerback) ServiceName() string {
	return s.peerServiceName
}
func (s *localPeerback) ID() string {
	return s.peerID
}
func (s *localPeerback) URL() string {
	return s.netAddr + "/" +
		s.peerServiceName + "/" +
		s.peerID
}

func (s *localPeerback) NewCircuitToPeerURL(
	peerURL string,
	frag *Fragment,
	errWriteDur *time.Duration,
) (ckt *Circuit, ctx context.Context, err error) {

	netAddr, serviceName, peerID, circuitID, err := parsePeerURL(peerURL)
	if circuitID != "" {
		panic(fmt.Sprintf("NewCircuitToPeerURL() use error: peerURL "+
			"should not have a circuitID "+
			"in it, as we don't support that below (yet atm): '%v'",
			peerURL))
	}
	if err != nil {
		return nil, nil, fmt.Errorf("NewCircuitToPeerURL could not "+
			"parse peerURL: '%v': '%v'", peerURL, err)
	}
	if frag == nil {
		frag = NewFragment()
	}
	frag.FromPeerID = s.peerID
	frag.ToPeerID = peerID

	// circuitID will be empty, want to create a new CallID.
	// allow joining a extant circuit? let's not for now.
	// Its just much simpler to start with.
	circuitID = NewCallID()
	frag.CircuitID = circuitID
	frag.ServiceName = serviceName

	rpb := &remotePeerback{
		localPeerback:     s,
		peerID:            peerID,
		netAddr:           netAddr,
		remoteServiceName: serviceName,
	}
	s.mut.Lock()
	s.remotes[peerID] = rpb
	s.mut.Unlock()

	ckt, ctx = s.newCircuit(rpb, circuitID)
	msg := frag.ToMessage()
	msg.HDR.To = netAddr
	msg.HDR.From = s.netAddr
	msg.HDR.Typ = CallPeerStartCircuit

	// tell the remote which serviceName we are coming from;
	// so the URL back can be correct.
	msg.HDR.Args = map[string]string{"fromServiceName": s.peerServiceName}
	//msg.HDR.From will be overwritten by sender.

	//vv("about to SendOneWayMessage = '%v'", msg)
	return ckt, ctx, s.u.SendOneWayMessage(ctx, msg, errWriteDur)
}

func parsePeerURL(peerURL string) (netAddr, serviceName, peerID, circuitID string, err error) {
	var u *url.URL
	u, err = url.Parse(peerURL)
	if err != nil {
		return
	}
	netAddr = u.Scheme + "://" + u.Host
	splt := strings.Split(u.Path, "/")
	for i, s := range splt {
		switch i {
		case 0:
			// path starts with a /, so this is
			// typically emptry string.
			// e.g. Path:"/serviceName/peerID/circuitID"
			if s != "" {
				panic(fmt.Sprintf("URL Path did not start with '/'; "+
					"How are we to parse path '%v' ???", u.Path))
			}
		case 1:
			serviceName = s
		case 2:
			peerID = s
		case 3:
			circuitID = s
		default:
			break
		}
	}
	//vv("u = '%#v'", u)
	return
}

// SendOneWay sends a Frament on the given Circuit.
func (s *remotePeerback) SendOneWay(
	ckt *Circuit, frag *Fragment, errWriteDur *time.Duration) error {

	return s.localPeerback.SendOneWay(ckt, frag, errWriteDur)
}

// SendOneWayMessage sends a Frament on the given Circuit.
func (s *localPeerback) SendOneWay(ckt *Circuit, frag *Fragment, errWriteDur *time.Duration) error {
	if frag == nil {
		return fmt.Errorf("frag cannot be nil")
		//frag = NewFragment()
	}
	frag.CircuitID = ckt.callID
	frag.FromPeerID = ckt.localPeerID
	frag.ToPeerID = ckt.remotePeerID

	msg := ckt.convertFragmentToMessage(frag)
	return s.u.SendOneWayMessage(s.ctx, msg, errWriteDur)
}

func (peerAPI *peerAPI) newLocalPeerback(
	ctx context.Context,
	cancelFunc context.CancelFunc,
	u UniversalCliSrv,
	peerID string,
	newPeerCh chan RemotePeer,
	peerServiceName,
	netAddr string,

) (pb *localPeerback) {

	pb = &localPeerback{
		netAddr:         netAddr,
		peerServiceName: peerServiceName,
		peerAPI:         peerAPI,
		ctx:             ctx,
		canc:            cancelFunc,
		peerID:          peerID,
		u:               u,
		newPeerCh:       newPeerCh,
		// has to sort the Message to each circuit and convert to Fragment.
		readsIn:  make(chan *Message, 1),
		errorsIn: make(chan *Message, 1),

		remotes:               make(map[string]*remotePeerback),
		handleChansNewCircuit: make(chan *Circuit),
		handleCircuitClose:    make(chan *Circuit),
	}

	// service reads for local.
	u.GetReadsForToPeerID(pb.readsIn, peerID)
	u.GetErrorsForToPeerID(pb.errorsIn, peerID)
	go pb.peerbackPump()

	return pb
}

// background goro to read all PeerID *Messages and sort them
// to all the circuits live in this peer.
func (pb *localPeerback) peerbackPump() {

	// key: CallID (circuit ID)
	m := make(map[string]*Circuit)

	cleanupCkt := func(ckt *Circuit) {

		// Politely tell our peer we are going down,
		// in case they are staying up.
		frag := NewFragment()
		frag.Typ = CallPeerEndCircuit
		pb.SendOneWay(ckt, frag, nil)

		ckt.canc()
		delete(m, ckt.callID)
		pb.u.UnregisterChannel(ckt.callID, CallIDReadMap)
		pb.u.UnregisterChannel(ckt.callID, CallIDErrorMap)
	}
	defer func() {
		vv("peerbackPump exiting. closing all remaining circuits.")
		var all []*Circuit
		for _, ckt := range m {
			all = append(all, ckt)
		}
		for _, ckt := range all {
			cleanupCkt(ckt)
		}
		m = nil
		vv("peerbackPump cleanup done.")
	}()

	done := pb.ctx.Done()
	for {
		select {
		case ckt := <-pb.handleChansNewCircuit:
			m[ckt.callID] = ckt

		case ckt := <-pb.handleCircuitClose:
			cleanupCkt(ckt)

		case msg := <-pb.readsIn:
			callID := msg.HDR.CallID
			ckt, ok := m[callID]
			//vv("peerbackPump sees readsIn msg: '%v' payload '%v'", msg, string(msg.JobSerz))
			if !ok {
				alwaysPrintf("arg. no circuit avail for callID = '%v';"+
					" pump dropping this msg.", callID)

				if callID == "" {
					// we have a legit PeerID but no CallID, which means that
					// we have not yet instantiated a circuit. Do so? No.
					// For now we have client do a CallPeerStartCircuit call.
				}
				continue
			}
			frag := ckt.convertMessageToFragment(msg)
			select {
			case ckt.Reads <- frag:
			case <-done:
				return
			}
		case msgerr := <-pb.errorsIn:
			callID := msgerr.HDR.CallID
			ckt, ok := m[callID]
			if !ok {
				vv("arg. no ckt avail for callID = '%v' on msgerr", callID)
				continue
			}
			fragerr := ckt.convertMessageToFragment(msgerr)
			select {
			case ckt.Errors <- fragerr:
			case <-done:
				return
			}
		}
	}
}

// incoming
func (ckt *Circuit) convertMessageToFragment(msg *Message) (frag *Fragment) {
	frag = &Fragment{
		FromPeerID:  msg.HDR.FromPeerID,
		ToPeerID:    msg.HDR.ToPeerID,
		CircuitID:   msg.HDR.CallID,
		Serial:      msg.HDR.Serial,
		Typ:         msg.HDR.Typ,
		ServiceName: msg.HDR.ServiceName,

		FragOp:      msg.HDR.FragOp,
		FragSubject: msg.HDR.Subject,
		FragPart:    msg.HDR.StreamPart,

		Args:    msg.HDR.Args,
		Payload: msg.JobSerz,
		Err:     msg.JobErrs,
	}
	return
}

func (frag *Fragment) ToMessage() (msg *Message) {
	msg = NewMessage()

	msg.HDR.Created = time.Now()

	msg.HDR.FromPeerID = frag.FromPeerID
	msg.HDR.ToPeerID = frag.ToPeerID
	msg.HDR.CallID = frag.CircuitID
	//msg.HDR.Serial = issueSerial()
	msg.HDR.Serial = frag.Serial

	if frag.Typ == 0 {
		msg.HDR.Typ = CallPeerTraffic
	} else {
		msg.HDR.Typ = frag.Typ
	}
	msg.HDR.ServiceName = frag.ServiceName

	msg.HDR.FragOp = frag.FragOp
	msg.HDR.Subject = frag.FragSubject
	msg.HDR.StreamPart = frag.FragPart

	if frag.Args != nil {
		msg.HDR.Args = frag.Args
	}
	msg.JobSerz = frag.Payload
	msg.JobErrs = frag.Err

	//vv("ToMessage did frag='%v' -> msg='%v'", frag, msg)

	return
}

// outgoing messages:
// If frag.ToPeerID,FromPeerID,CallD are not
// set, they will be filled in from the ckt.
func (ckt *Circuit) convertFragmentToMessage(frag *Fragment) (msg *Message) {

	msg = frag.ToMessage()

	if msg.HDR.ToPeerID == "" {
		msg.HDR.ToPeerID = ckt.remotePeerID
	}
	if msg.HDR.FromPeerID == "" {
		msg.HDR.FromPeerID = ckt.localPeerID
	}
	if msg.HDR.CallID == "" {
		msg.HDR.CallID = ckt.callID
	}

	if msg.HDR.To == "" {
		msg.HDR.To = ckt.pbTo.netAddr
	}
	if msg.HDR.From == "" {
		msg.HDR.From = ckt.pbFrom.netAddr
	}
	if msg.HDR.ServiceName == "" {
		msg.HDR.ServiceName = ckt.remoteServiceName
	}
	if msg.HDR.Args == nil {
		msg.HDR.Args = make(map[string]string)
	}
	msg.HDR.Args["fromServiceName"] = ckt.localServiceName

	return
}

// allow cID to specify the Call/CircuitID if desired, or empty to get a new one.
func (rpb *remotePeerback) NewCircuit() (ckt *Circuit, ctx2 context.Context) {
	return rpb.localPeerback.newCircuit(rpb, "")
}

func (lpb *localPeerback) newCircuit(
	rpb *remotePeerback,
	cID string,
) (ckt *Circuit, ctx2 context.Context) {

	var canc2 context.CancelFunc
	ctx2, canc2 = context.WithCancel(lpb.ctx)
	reads := make(chan *Fragment)
	errors := make(chan *Fragment)
	ckt = &Circuit{
		localServiceName:  lpb.peerServiceName,
		remoteServiceName: rpb.remoteServiceName,
		pbFrom:            lpb,
		pbTo:              rpb,
		callID:            cID,
		localPeerID:       lpb.peerID,
		remotePeerID:      rpb.peerID,
		Reads:             reads,
		Errors:            errors,
		ctx:               ctx2,
		canc:              canc2,
	}
	if ckt.callID == "" {
		ckt.callID = NewCallID()
	}

	lpb.handleChansNewCircuit <- ckt
	return
}

func (h *Circuit) CircuitID() string { return h.callID }

func (h *Circuit) Close() {
	h.pbFrom.handleCircuitClose <- h
}

// RemotePeer is the user facing interface to
// communicating with Peers. Peers exchange
// Fragments over Circuits, and
// generally implement finite-state-machine
// behavior more complex than can be
// efficiently modeled with simple
// call-and-response RPC.
//
// In particular, we support infinite streams of
// Fragments in order to convey large
// files and filesystem (r)sync operations.
//
// RemotePeer is a proxy. It is the local representation of
// a remote peer.
type RemotePeer interface {

	// IncomingCircuit is the first one that arrives with
	// with an incoming remote peer connection.
	IncomingCircuit() (ckt *Circuit, ctx context.Context)

	// NewCircuit generates a Circuit between two Peers,
	// and tells the SendOneWay machinery
	// how to reply to you. It makes a new CircuitID (CallID),
	// and manages it for you. It gives you two
	// channels to get normal and error replies on. Using this Circuit,
	// you can make as many one way calls as you like
	// to the remote Peer. The returned ctx will be
	// cancelled in case of broken/shutdown connection
	// or this application shutting down.
	//
	// You must call Close() on the ckt when you are done with it.
	//
	// When selecting ckt.Reads and ckt.Errors, always also
	// select on ctx.Done() in order to shutdown gracefully.
	//
	NewCircuit() (ckt *Circuit, ctx context.Context)

	// SendOneWay sends a Frament on the given Circuit.
	SendOneWay(ckt *Circuit, frag *Fragment, errWriteDur *time.Duration) error

	PeerServiceName() string
	PeerID() string
	PeerURL() string
}

type LocalPeer interface {

	// NewCircuitToPeerURL sets up a persistent communication path called a Circuit.
	// The frag can be nil, or set to send it immediately.
	NewCircuitToPeerURL(
		peerURL string,
		frag *Fragment,
		errWriteDur *time.Duration,
	) (ckt *Circuit, ctx context.Context, err error)

	// how we were registered/invoked.
	ServiceName() string
	ID() string

	// URL give the network address, the service name, and the PeerID
	// in a URL safe string, suitable for contacting the peer.
	// e.g. tcp://x.x.x.x:port/peerServiceName/peerID
	URL() string
}

// one line version of the below, for ease of copying.
// type PeerServiceFunc func(myPeer LocalPeer, ctx0 context.Context, newPeerCh <-chan RemotePeer) error

// PeerServiceFunc is implemented by user's peer services,
// and registered on a Client or a Server under a
// specific peerServiceName by using the
// PeerAPI.RegisterPeerServiceFunc() call.
type PeerServiceFunc func(

	// our local Peer interface, can do SendToPeer() to send to URL.
	myPeer LocalPeer,

	// ctx0 supplies the overall context of the
	// Client/Server host. If our hosts starts
	// shutdown, this will cancel, and
	// we should cleanup and return; as should any
	// child goroutines.
	ctx0 context.Context,

	// first on newPeerCh will be the remote client
	// or server who invoked us.
	newPeerCh <-chan RemotePeer,

) error

// A peerAPI is provided under the Client and Server PeerAPI
// members. They use the same peerAPI
// implementation. It is designed for symmetry.
type peerAPI struct {
	u   UniversalCliSrv
	mut sync.Mutex

	// peerServiceName key
	localServiceNameMap map[string]*knownLocalPeer

	isCli bool
}

func newPeerAPI(u UniversalCliSrv, isCli bool) *peerAPI {
	return &peerAPI{
		u:                   u,
		localServiceNameMap: make(map[string]*knownLocalPeer),
		isCli:               isCli,
	}
}

type knownRemotePeer struct {
	peerServiceName string
	peerID          string
	netAddress      string // tcp://ip:port, or udp://ip:port
}

type knownLocalPeer struct {
	mut             sync.Mutex
	peerServiceFunc PeerServiceFunc
	peerServiceName string

	active map[string]*localPeerback
}

// RegisterPeerServiceFunc registers a user's
// PeerServiceFunc implementation under the given
// peerServiceName. There can only be one
// such name on a given Client or Server.
// Registering the same name again will discard
// any earlier registration.
func (p *peerAPI) RegisterPeerServiceFunc(peerServiceName string, peer PeerServiceFunc) error {

	if peerServiceName == "" || peer == nil {
		panic("peerServiceName cannot be empty, peer cannot be nil")
	}
	p.mut.Lock()
	defer p.mut.Unlock()
	p.localServiceNameMap[peerServiceName] = &knownLocalPeer{
		peerServiceFunc: peer, peerServiceName: peerServiceName}
	return nil
}

func (p *peerAPI) StartLocalPeer(
	ctx context.Context,
	peerServiceName string,

) (localPeerURL, localPeerID string, err error) {
	p.mut.Lock()
	defer p.mut.Unlock()
	knownLocalPeer, ok := p.localServiceNameMap[peerServiceName]
	if !ok {
		return "", "", fmt.Errorf("no local peerServiceName '%v' available", peerServiceName)
	}

	newPeerCh := make(chan RemotePeer, 1) // must be buffered >= 1, see below.
	ctx1, canc1 := context.WithCancel(ctx)
	localPeerID = NewCallID()

	localAddr := p.u.LocalAddr()
	//vv("localAddr = '%v'", localAddr)
	lpb := p.newLocalPeerback(ctx1, canc1, p.u, localPeerID, newPeerCh, peerServiceName, localAddr)

	knownLocalPeer.mut.Lock()
	if knownLocalPeer.active == nil {
		knownLocalPeer.active = make(map[string]*localPeerback)
	}
	knownLocalPeer.active[localPeerID] = lpb
	knownLocalPeer.mut.Unlock()

	go func() {
		knownLocalPeer.peerServiceFunc(lpb, ctx, newPeerCh)

		// do cleanup
		lpb.Close()
		knownLocalPeer.mut.Lock()
		delete(knownLocalPeer.active, localPeerID)
		knownLocalPeer.mut.Unlock()

		canc1()
	}()

	localPeerURL = lpb.URL()
	//vv("lpb.PeerURL() = '%v'", localPeerURL)

	return localPeerURL, localPeerID, nil
}

// StartRemotePeer boots up a peer a remote node.
// It must already have been registered on the
// client or server running there.
//
// If a waitUpTo duration is provided, we will poll in the
// event of an error, since there can be races when
// doing simultaneous client and server setup. We will
// only return an error after waitUpTo has passed. To
// disable polling set waitUpTo to zero. We poll up
// to 50 times, pausing waitUpTo/50 after each.
// If SendAndGetReply succeeds, then we immediately
// cease polling and return the remotePeerID.
func (p *peerAPI) StartRemotePeer(ctx context.Context, peerServiceName, remoteAddr string, waitUpTo time.Duration) (remotePeerURL, remotePeerID string, err error) {

	// retry until deadline, if waitUpTo is > 0
	deadline := time.Now().Add(waitUpTo)

	msg := NewMessage()

	// server will return "" because many possible clients,
	// but this can still help out the user on the client
	// by getting the right address.
	r := p.u.RemoteAddr()
	if r != "" {
		// we are on the client
		if r != remoteAddr {
			return "", "", fmt.Errorf("client peer error on StartRemotePeer: remoteAddr should be '%v' (that we are connected to), rather than the '%v' which was requested. Otherwise your request will fail.", r, remoteAddr)
		}
	}

	hdr := NewHDR(p.u.LocalAddr(), remoteAddr, peerServiceName, CallPeerStart, 0)
	//hdr.ServiceName = peerServiceName
	//callID := NewCallID()
	//hdr.CallID = callID
	msg.HDR = *hdr
	callID := msg.HDR.CallID

	vv("msg.HDR='%v'", msg.HDR.String()) // "Typ": CallPeerStart seen.

	ch := make(chan *Message, 100)

	// can't use the peerID/ObjID yet because we have no PeerID
	// yet, we are bootstrapping.
	p.u.GetReadsForCallID(ch, callID)
	// be sure to cleanup. We won't need this channel again.
	defer p.u.UnregisterChannel(callID, CallIDReadMap)

	pollInterval := waitUpTo / 50

	for i := 0; i < 50; i++ {
		err = p.u.SendOneWayMessage(ctx, msg, nil)
		if err == nil {
			//vv("SendOneWayMessage retried %v times before succeess; pollInterval: %v",
			//	i, pollInterval)
			break
		}
		// INVAR: err != nil
		left := deadline.Sub(time.Now())
		if left <= 0 || waitUpTo <= 0 {
			return
		} else {
			dur := pollInterval
			if left < dur {
				// don't sleep past our deadline
				dur = left
			}
			time.Sleep(dur)
			continue
		}
	}

	vv("isCli=%v, StartRemotePeer about to wait for reply on ch to callID = '%v'", p.isCli, callID)
	var reply *Message
	select {
	case reply = <-ch:
		vv("got reply to CallPeerStart: '%v'", reply.String())
	case <-ctx.Done():
		return "", "", ErrContextCancelled
	}
	var ok bool
	remotePeerID, ok = reply.HDR.Args["peerID"]
	if !ok {
		return "", "", fmt.Errorf("remote '%v', peerServiceName '%v' did "+
			"not respond with peerID in Args", remoteAddr, peerServiceName)
	}
	vv("got remotePeerID from Args[peerID]: '%v'", remotePeerID)
	remotePeerURL, ok = reply.HDR.Args["peerURL"]
	if !ok {
		return "", "", fmt.Errorf("remote '%v', peerServiceName '%v' did "+
			"not respond with peerURL in Args", remoteAddr, peerServiceName)
	}
	vv("got remotePeerURL from Args[peerURL]: '%v'", remotePeerURL)
	return remotePeerURL, remotePeerID, nil
}

// bootstrapCircuit: handle CallPeerStartCircuit.
//
// The goal of bootstrapCircuit is to enable the user
// peer code to interact with circuits and remote peers.
// We want this user PeerImpl.Start() code to work now:
//
//	(This is taken from the actual the PeerImpl.Start() code
//	 here in fragment.go at the moment.)
//
//	select {
//	    // new Circuit connection arrives
//	    case peer := <-newPeerCh:  // this needs to be enabled.
//		   wg.Add(1)
//
//		   vv("got from newPeerCh! '%v' sees new peerURL: '%v'",
//		       peer.PeerServiceName(), peer.URL())
//
//		   // talk to this peer on a separate goro if you wish:
//		   go func(peer RemotePeer) {
//			    defer wg.Done()
//			    ckt, ctx := peer.IncomingCircuit()  // enable this.
//
// .
// Okay. On with the show:
func (s *peerAPI) bootstrapCircuit(isCli bool, msg *Message, ctx context.Context, sendCh chan *Message) error {
	//vv("isCli=%v, bootstrapCircuit called with msg='%v'; JobSerz='%v'", isCli, msg.String(), string(msg.JobSerz))

	// So here we go:

	// find the localPeerback corresponding to the ToPeerID
	localPeerID := msg.HDR.ToPeerID
	peerServiceName := msg.HDR.ServiceName

	s.mut.Lock()
	defer s.mut.Unlock()
	knownLocalPeer, ok := s.localServiceNameMap[peerServiceName]
	if !ok {
		msg.HDR.Typ = CallPeerError
		msg.JobErrs = fmt.Sprintf("no local peerServiceName '%v' available", peerServiceName)
		msg.JobSerz = nil
		return s.replyHelper(isCli, msg, ctx, sendCh)
	}

	knownLocalPeer.mut.Lock()

	var lpb *localPeerback
	ok = false

	if knownLocalPeer.active != nil {
		lpb, ok = knownLocalPeer.active[localPeerID]
	}

	if !ok { // either none active or could not find PeerID.
		// report error and return nil.
		msg.HDR.Typ = CallPeerError
		msg.JobErrs = fmt.Sprintf("have peerServiceName '%v', but none active OR nothing for requested peerID='%v'; perhaps it died?", peerServiceName, localPeerID)
		msg.JobSerz = nil
		return s.replyHelper(isCli, msg, ctx, sendCh)
	}
	knownLocalPeer.mut.Unlock()

	// success:
	msg.HDR.Typ = CallPeerTraffic

	// enable user code in the select below: case peer := <-newPeerCh:
	rpb := &remotePeerback{
		localPeerback: lpb,
		peerID:        msg.HDR.FromPeerID, // the remote's PeerID
		netAddr:       msg.HDR.From,
	}
	if msg.HDR.Args != nil {
		rpb.remoteServiceName = msg.HDR.Args["fromServiceName"]
	}
	// for tracing continuity, might as well use
	// the CallID that initiated the circuit from the start.

	ckt, ctx2 := lpb.newCircuit(rpb, msg.HDR.CallID)

	// enable user code: ckt, ctx := peer.IncomingCircuit()
	rpb.incomingCkt = ckt

	// for logging
	remoteCktURL := ckt.RemoteCircuitURL()
	localCktURL := ckt.LocalCircuitURL()
	rpb.peerURL = remoteCktURL // or is it localCktURL ?

	// don't block the readLoop up the stack.
	go func() {
		select {
		case lpb.newPeerCh <- rpb:
			asFrag := ckt.convertMessageToFragment(msg)
			vv("user got remote interface, give them the message: msg='%v' -> asFrag = '%v'", msg.String(), asFrag.String())
			select {
			case ckt.Reads <- asFrag:
			case <-ctx2.Done():
			}
		case <-ctx2.Done():
			//return ErrShutdown()
		case <-time.After(10 * time.Second):
			alwaysPrintf("warning: peer did not accept the circuit after 10 seconds. remoteCircuitURL = '%v'\n localCktURL = '%v'", remoteCktURL, localCktURL)
		}
	}()

	// do we need any other reply to the starting peer?
	// naw; they will get contacted over the cirtcuit.
	//return s.replyHelper(isCli, msg, ctx.Context, sendCh)

	return nil
}

func (s *peerAPI) replyHelper(isCli bool, msg *Message, ctx context.Context, sendCh chan *Message) error {

	// reply with the same msg; save an allocation.
	msg.HDR.From, msg.HDR.To = msg.HDR.To, msg.HDR.From

	msg.HDR.FromPeerID, msg.HDR.ToPeerID = msg.HDR.ToPeerID, msg.HDR.FromPeerID

	// but update the essentials
	msg.HDR.Serial = issueSerial()
	msg.HDR.Created = time.Now()
	msg.HDR.LocalRecvTm = time.Time{}
	msg.HDR.Deadline = time.Time{}

	msg.DoneCh = nil // no need now, save allocation. loquet.NewChan(msg)

	select {
	case sendCh <- msg:
	case <-ctx.Done():
		return ErrShutdown()
	}
	return nil // error means shut down the client.
}

// handle HDR.Typ == CallPeerStart  messages
// requesting to bootstrap a PeerServiceFunc.
//
// This needs special casing because the inital call API
// is different. See fragment.go; PeerServiceFunc is
// very different from TwoWayFunc or OneWayFunc.
//
// Note: we should only return an error if the shutdown request was received,
// which will kill the readLoop and connection.
// func (s *peerAPI) bootstrapPeerService(msg *Message, halt *idem.Halter, sendCh chan *Message) error {
func (s *peerAPI) bootstrapPeerService(isCli bool, msg *Message, ctx context.Context, sendCh chan *Message) error {

	vv("top of bootstrapPeerService(): isCli=%v; msg.HDR='%v'", isCli, msg.HDR.String())

	// starts its own goroutine or return with an error (both quickly).
	localPeerURL, localPeerID, err := s.StartLocalPeer(ctx, msg.HDR.ServiceName)

	// reply with the same msg; save an allocation.
	msg.HDR.From, msg.HDR.To = msg.HDR.To, msg.HDR.From

	// but update the essentials
	msg.HDR.Serial = issueSerial()
	msg.HDR.Created = time.Now()
	msg.HDR.LocalRecvTm = time.Time{}
	msg.HDR.Deadline = time.Time{}

	msg.DoneCh = nil // no need now, save allocation: loquet.NewChan(msg)

	if err != nil {
		msg.HDR.Typ = CallPeerError
		msg.JobErrs = err.Error()
	} else {
		msg.HDR.Typ = CallPeerTraffic
		// tell them our peerID, this is the critical desired info.
		msg.HDR.Args = map[string]string{
			"peerURL":         localPeerURL,
			"peerID":          localPeerID,
			"fromServiceName": msg.HDR.ServiceName}
	}
	msg.HDR.FromPeerID = localPeerID

	select {
	case sendCh <- msg:
	case <-ctx.Done():
		return ErrShutdown()
	}
	return nil
}
