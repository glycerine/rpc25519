package rpc25519

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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

	peerServiceName string

	callID string
	ctx    context.Context
	canc   context.CancelFunc

	Reads  chan *Fragment // users should treat as read-only.
	Errors chan *Fragment // ditto.
}

// CircuitURL format: tcp://x.x.x.x:port/peerServiceName/peerID/circuitID
// where peerID and circuitID are our CallID that are
// base64 URL encoded. The IDs do not include the '/' character,
// and thus are "URL safe". This can be verified here:
// https://github.com/cristalhq/base64/blob/5cfa89a12c5dbd0e52b1da082a33dce278c52cdf/utils.go#L65
//
//	(circuitID is the CallID in the Message.HDR)
func (ckt *Circuit) CircuitURL() string {
	return ckt.pbFrom.netAddr + "/" +
		ckt.peerServiceName + "/" +
		ckt.localPeerID + "/" +
		ckt.callID
}

// ID2 supplies the local and remote PeerIDs.
func (ckt *Circuit) ID2() (localPeerID, remotePeerID string) {
	return ckt.pbFrom.peerID, ckt.pbTo.peerID
}

func NewFragment() *Fragment {
	return &Fragment{}
}

// Fragments are sent to, and read from,
// a Circuit by implementers of
// PeerServiceFunc
type Fragment struct {

	// These first three are set by the
	// sending machinery; any user settings will
	// be overridden.
	//
	// FromPeerID tells the receiver who sent us this Fragment.
	FromPeerID string `zid:"0"`
	ToPeerID   string `zid:"1"`

	// CircuitID was assigned in the NewCircuit() call. Equivalent to Message.HDR.CallID.
	CircuitID string `zid:"2"`

	// ServiceName is the remote peer's PeerServiceName.
	ServiceName string `zid:"3"`

	FragType string `zid:"4"` // can be a message type, sub-service name, other useful context.
	FragPart int64  `zid:"5"` // built in multi-part handling for the same CallID and FragType.

	Args map[string]string `zid:"6"` // nil/unallocated to save space. User should alloc if the need it.

	Payload []byte `zid:"7"`

	Err string `zid:"8"` // distinguished field for error messages.
}

// remotePeerback is over the newPeerCh channel
// to a PeerServiceFunc.
// The "remote" are just the local proxy to the actual remote Peer
// that, on the remote node, is local to that node.
// Thus they are always children of localPeerbacks, created
// either deliberately locally with NewCircuit or received on newPeerChan
// from the remote who did NewCircuit.
type remotePeerback struct {
	localPeerback *localPeerback

	peerID string // the remote's PeerID
}

func (rpb *remotePeerback) IncomingCircuit() (ckt *Circuit, ctx context.Context) {
	panic("remotePeerback IncomingCircuit() requested! TODO!")
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

	mut sync.Mutex // protect remotes map
	// key is the remote PeerID
	remotes map[string]*remotePeerback
}

func (s *localPeerback) PeerServiceName() string {
	return s.peerServiceName
}
func (s *localPeerback) PeerID() string {
	return s.peerID
}
func (s *localPeerback) PeerURL() string {
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
	if err != nil {
		return nil, nil, fmt.Errorf("NewCircuitToPeerURL could not "+
			"parse peerURL: '%v': '%v'", peerURL, err)
	}
	if frag == nil {
		frag = NewFragment()
	}
	frag.FromPeerID = s.peerID
	frag.ToPeerID = peerID
	frag.CircuitID = circuitID
	frag.ServiceName = serviceName

	rpb := &remotePeerback{
		localPeerback: s,
		peerID:        peerID,
	}
	s.mut.Lock()
	s.remotes[peerID] = rpb
	s.mut.Unlock()

	ckt, ctx, err = s.newCircuit(rpb)
	if err != nil {
		return nil, nil, fmt.Errorf("error in NewCircuitToPeerURL(): "+
			"newCircuit returned error: '%v'", err)
	}
	msg := frag.ToMessage()
	msg.HDR.To = netAddr
	//msg.HDR.From will be overwritten by sender.

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

// SendOneWayMessage sends a Frament on the given Circuit.
func (s *localPeerback) SendOneWayMessage(ckt *Circuit, frag *Fragment, errWriteDur *time.Duration) error {

	if frag != nil {
		frag = NewFragment()
	}
	frag.CircuitID = ckt.callID
	frag.FromPeerID = ckt.localPeerID
	frag.ToPeerID = ckt.remotePeerID

	msg := ckt.convertFragmentToMessage(frag)
	return s.u.SendOneWayMessage(s.ctx, msg, errWriteDur)
}

func (peerAPI *peerAPI) newLocalPeerback(ctx context.Context, cancelFunc context.CancelFunc, u UniversalCliSrv, peerID string, newPeerCh chan RemotePeer, peerServiceName, netAddr string) (pb *localPeerback) {
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
	done := pb.ctx.Done()
	for {
		select {
		case ckt := <-pb.handleChansNewCircuit:
			m[ckt.callID] = ckt

		case ckt := <-pb.handleCircuitClose:
			ckt.canc()
			delete(m, ckt.callID)

		case msg := <-pb.readsIn:
			callID := msg.HDR.CallID
			ckt, ok := m[callID]
			if !ok {
				vv("arg. no ckt avail for callID = '%v'", callID)
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
		ToPeerID:   msg.HDR.ToPeerID,
		FromPeerID: msg.HDR.FromPeerID,
		CircuitID:  msg.HDR.CallID,

		FragType: msg.HDR.Subject,
		FragPart: msg.HDR.StreamPart,

		Args:    msg.HDR.Args,
		Payload: msg.JobSerz,
		Err:     msg.JobErrs,
	}
	return
}

func (frag *Fragment) ToMessage() (msg *Message) {
	msg = NewMessage()

	msg.HDR.Typ = CallOneWay
	msg.HDR.Created = time.Now()
	msg.HDR.Serial = atomic.AddInt64(&lastSerial, 1)
	msg.HDR.ServiceName = frag.ServiceName

	msg.HDR.ToPeerID = frag.ToPeerID
	msg.HDR.FromPeerID = frag.FromPeerID
	msg.HDR.CallID = frag.CircuitID
	msg.HDR.Subject = frag.FragType

	if frag.Args != nil {
		msg.HDR.Args = frag.Args
	}

	msg.HDR.StreamPart = frag.FragPart
	msg.JobSerz = frag.Payload
	msg.JobErrs = frag.Err

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

	return
}

func (rpb *remotePeerback) NewCircuit() (ckt *Circuit, ctx2 context.Context, err error) {
	return rpb.localPeerback.newCircuit(rpb)
}

func (lpb *localPeerback) newCircuit(rpb *remotePeerback) (ckt *Circuit, ctx2 context.Context, err error) {

	var canc2 context.CancelFunc
	ctx2, canc2 = context.WithCancel(lpb.ctx)
	reads := make(chan *Fragment)
	errors := make(chan *Fragment)
	ckt = &Circuit{
		peerServiceName: lpb.peerServiceName,
		pbFrom:          lpb,
		pbTo:            rpb,
		localPeerID:     lpb.peerID,
		remotePeerID:    rpb.peerID,
		callID:          NewCallID(),
		Reads:           reads,
		Errors:          errors,
		ctx:             ctx2,
		canc:            canc2,
	}
	lpb.handleChansNewCircuit <- ckt
	return
}

func (h *Circuit) CircuitID() string { return h.callID }

func (h *Circuit) Close() {
	h.pbFrom.handleCircuitClose <- h
	// done in the pump: h.canc()
	// unregister the *Frag channels?
	//h.fp.u.UnregisterChannel(ID string, whichmap int)
}

// Peers exchange Fragments over Circuits, and
// generally implement finite-state-machine
// behavior more complex than can be
// efficiently modeled with simple
// call-and-response RPC.
//
// In particular, we support infinite streams of
// Fragments in order to convey large
// files and filesystem sync operations.
//
// RemotePeer is a proxy. It is the local representation of
// a remote peer.
type RemotePeer interface {

	// IncomingCircuit is the first one that arrives with
	// with an incoming remote peer connections.
	IncomingCircuit() (ckt *Circuit, ctx context.Context)

	// NewCircuit generates a Circuit between two Peers,
	// and tells the SendOneWayMessage machinery
	// how to reply to you. It makes a new CallID,
	// and manages it for you. It gives you two
	// channels to get normal/error replies on. Using this Circuit,
	// you can make as many one way calls as you like
	// to the remote Peer. The returned ctx will be
	// cancelled in case of broken/shutdown connection
	// or this application shutting down.
	//
	// You must call Close() on the ckt when you are done with it.
	//
	// When selecting ckt.Reads and ckt.Errors, always also
	// select on ctx.Done() in order to shutdown gracefully.
	NewCircuit() (ckt *Circuit, ctx context.Context, err error)

	// SendOneWayMessage sends a Frament on the given Circuit.
	SendOneWayMessage(ckt *Circuit, frag *Fragment, errWriteDur *time.Duration) error

	PeerServiceName() string
	PeerID() string
	PeerURL() string
}

type LocalPeer interface {

	// NewCircuitToPeerURL sets up a persistent communication path called a Circuit.
	// The frag can be nil, or set for efficiency.
	NewCircuitToPeerURL(
		peerURL string,
		frag *Fragment,
		errWriteDur *time.Duration,
	) (ckt *Circuit, ctx context.Context, err error)

	// how we were registered/invoked.
	PeerServiceName() string
	PeerID() string

	// PerlURL give the network address, the service name, and the PeerID
	// in a URL safe string, suitable for contacting the peer.
	// e.g. tcp://x.x.x.x:port/peerServiceName/peerID
	PeerURL() string
}

// aka: for ease of copying,
// type PeerServiceFunc func(peerServiceName string, ourPeerID string, ctx0 context.Context, newPeerCh <-chan Peer) error

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

// Users write a PeerImpl and PeerServiceFunc, like this: TODO move to example.go

// PeerImpl demonstrates how a user can implement a Peer.
//
//msgp:ignore PeerImpl
type PeerImpl struct {
	KnownPeers          []string
	StartCount          atomic.Int64 `msg:"-"`
	DoEchoToThisPeerURL chan string
}

func (me *PeerImpl) Start(

	// how we were registered/invoked.
	// our local Peer interface, can do SendToPeer() to send to URL.
	myPeer LocalPeer,

	// overall context of Client/Server host, if
	// it starts shutdown, this will cancel, and
	// we should cleanup and return; as should any
	// child goroutines.
	ctx0 context.Context,

	// first on newPeerCh might be the client or server who invoked us,
	// if a remote wanted to start a circuit.
	newPeerCh <-chan RemotePeer,

) error {

	nStart := me.StartCount.Add(1)
	vv("PeerImpl.Start() top. ourPeerID = '%v'; peerServiceName='%v'; StartCount = %v", myPeer.PeerID(), myPeer.PeerServiceName(), nStart)

	var wg sync.WaitGroup
	defer wg.Wait() // wait for everyone to shutdown/catch stragglers that don't by hanging.

	done0 := ctx0.Done()

	for {
		select {
		// URL format: tcp://x.x.x.x:port/peerServiceName/peerID/circuitID
		case echoToURL := <-me.DoEchoToThisPeerURL:

			go func(echoToURL string) {

				outFrag := NewFragment()
				outFrag.Payload = []byte(fmt.Sprintf("echo request from peerID='%v' to peerID '%v' on 'echo circuit'", myPeer.PeerID(), echoToURL))

				ckt, ctx, err := myPeer.NewCircuitToPeerURL(echoToURL, outFrag, nil)
				panicOn(err)
				defer ckt.Close() // close when echo heard.
				done := ctx.Done()

				select {
				case frag := <-ckt.Reads:
					vv("echo circuit got read frag back: '%#v'", frag)
				case fragerr := <-ckt.Errors:
					vv("echo circuit got error fragerr back: '%#v'", fragerr)

				case <-done:
					return
				case <-done0:
					return
				}

			}(echoToURL)
		// new Circuit connection arrives
		case peer := <-newPeerCh:
			wg.Add(1)

			vv("got from newPeerCh! '%v' sees new peerURL: '%v'",
				peer.PeerServiceName(), peer.PeerURL())

			// talk to this peer on a separate goro if you wish:
			go func(peer RemotePeer) {
				defer wg.Done()

				ckt, ctx := peer.IncomingCircuit()
				vv("IncomingCircuit got circuitURL = '%v'", ckt.CircuitURL())
				done := ctx.Done()

				outFrag := NewFragment()
				// set Payload, other details...
				outFrag.Payload = []byte(fmt.Sprintf("hello from '%v'", myPeer.PeerURL()))

				err := peer.SendOneWayMessage(ckt, outFrag, nil)
				panicOn(err)

				for {
					select {
					case frag := <-ckt.Reads:
						_ = frag
					case fragerr := <-ckt.Errors:
						_ = fragerr

					case <-done:
						return
					case <-done0:
						return
					}
				}

			}(peer)

		case <-done0:
			return ErrContextCancelled
		}
	}
	return nil
}

// A peerAPI is provided under the Client and Server PeerAPI
// members. They use the same peerAPI
// implementation. It is designed for symmetry.
type peerAPI struct {
	u   UniversalCliSrv
	mut sync.Mutex

	// peerServiceName key
	localServiceNameMap map[string]*knownLocalPeer

	// PeerID key
	remoteKnownPeers map[string]*knownRemotePeer
}

func newPeerAPI(u UniversalCliSrv) *peerAPI {
	return &peerAPI{
		u:                   u,
		localServiceNameMap: make(map[string]*knownLocalPeer),
		remoteKnownPeers:    make(map[string]*knownRemotePeer),
	}
}

type knownRemotePeer struct {
	peerServiceName string
	peerID          string
	netAddress      string // tcp://ip:port, or udp://ip:port
}

/*
type remotePeer struct {
	peerServiceName string
	peerID          string
}
*/

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
	p.localServiceNameMap[peerServiceName] = &knownLocalPeer{peerServiceFunc: peer, peerServiceName: peerServiceName}
	return nil
}

func (p *peerAPI) StartLocalPeer(ctx context.Context, peerServiceName string, knownPeerIDs ...string) (localPeerURL string, err error) {
	p.mut.Lock()
	defer p.mut.Unlock()
	knownLocalPeer, ok := p.localServiceNameMap[peerServiceName]
	if !ok {
		return "", fmt.Errorf("no local peerServiceName '%v' available", peerServiceName)
	}

	for _, knownID := range knownPeerIDs {
		known, ok := p.remoteKnownPeers[knownID]
		if !ok {
			known = &knownRemotePeer{
				//peerServiceName string
				//netAddress         string
				peerID: knownID,
			}
			p.remoteKnownPeers[knownID] = known
		}
	}

	newPeerCh := make(chan RemotePeer, 1) // must be buffered >= 1, see below.
	ctx1, canc1 := context.WithCancel(ctx)
	localPeerID := NewCallID()

	lpb := p.newLocalPeerback(ctx1, canc1, p.u, localPeerID, newPeerCh, peerServiceName, p.u.LocalAddr())

	knownLocalPeer.mut.Lock()
	if knownLocalPeer.active == nil {
		knownLocalPeer.active = make(map[string]*localPeerback)
	}
	knownLocalPeer.active[localPeerID] = lpb
	knownLocalPeer.mut.Unlock()

	// nope! don't send to ourselves! :)
	// newPeerCh <- backer // newPeerCh is buffered above, so this cannot block.

	go func() {

		knownLocalPeer.peerServiceFunc(lpb, ctx, newPeerCh)

	}()

	return lpb.PeerURL(), nil
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
func (p *peerAPI) StartRemotePeer(ctx context.Context, peerServiceName, remoteAddr string, waitUpTo time.Duration) (remotePeerID string, err error) {

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
			return "", fmt.Errorf("client peer error on StartRemotePeer: remoteAddr should be '%v' (that we are connected to), rather than the '%v' which was requested. Otherwise your request will fail.", r, remoteAddr)
		}
	}

	hdr := NewHDR(p.u.LocalAddr(), remoteAddr, peerServiceName, CallStartPeerCircuit, 0)
	//hdr.ServiceName = peerServiceName
	//callID := NewCallID()
	//hdr.CallID = callID
	msg.HDR = *hdr
	callID := msg.HDR.CallID

	ch := make(chan *Message, 100)
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

	var reply *Message
	select {
	case reply = <-ch:
	case <-ctx.Done():
		return "", ErrContextCancelled
	}
	var ok bool
	remotePeerID, ok = reply.HDR.Args["peerID"]
	if !ok {
		return "", fmt.Errorf("remote '%v', peerServiceName '%v' did "+
			"not respond with peerID in Args", remoteAddr, peerServiceName)
	}
	vv("got remotePeerID from Args[peerID]: '%v'", remotePeerID)
	return remotePeerID, nil
}
