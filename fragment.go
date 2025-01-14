package rpc25519

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

//go:generate greenpack

// Circuit is a handle to the two-way,
// asynchronous, communication channel
// between two Peers.
//
// It is returned when a Peer calls NewCircuit().
type Circuit struct {
	pbFrom *localPeerback
	pbTo   *remotePeerback

	localPeerID  string
	remotePeerID string

	peerServiceName string
	circuitName     string

	callID string
	ctx    context.Context
	canc   context.CancelFunc

	Reads  chan *Fragment // users should treat as read-only.
	Errors chan *Fragment // ditto.
}

// ID2 supplies the local and remote PeerIDs.
func (ckt *Circuit) ID2() (localPeerID, remotePeerID string) {
	return ckt.pbFrom.peerID, ckt.pbTo.peerID
}

func (ckt *Circuit) NewFragment() *Fragment {
	return &Fragment{
		CallID:      ckt.callID,
		FromPeerID:  ckt.localPeerID,
		ToPeerID:    ckt.remotePeerID,
		CircuitName: ckt.circuitName,
	}
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

	// CallID was assigned in the NewCircuit() call.
	CallID string `zid:"2"`

	// CircuitName is what was established by NewCircuit(circuitName)
	CircuitName string `zid:"3"`

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

// localPeerback in the backing behind each local instantiation of a PeerServiceFunc.
// local peers do reads on ch, get notified of new connections on newPeerChan.
// and create new outgoing connections with
type localPeerback struct {
	// local peers do reads, "remote" peers are for sending to.
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

	// key is the remote PeerID
	remotes map[string]*remotePeerback
}

// SendOneWayMessage sends a Frament on the given Circuit.
func (s *localPeerback) SendOneWayMessage(ckt *Circuit, frag *Fragment, errWriteDur *time.Duration) error {
	msg := ckt.convertFragmentToMessage(frag)
	return s.u.SendOneWayMessage(s.ctx, msg, errWriteDur)
}

// PeerID supplies the local and remote PeerIDs, whatever the peerback represents.
func (s *localPeerback) PeerID() string { return s.peerID }

func (peerAPI *peerAPI) newLocalPeerback(ctx context.Context, cancelFunc context.CancelFunc, u UniversalCliSrv, peerID string, newPeerCh chan RemotePeer, peerServiceName string) (pb *localPeerback) {
	pb = &localPeerback{
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
	}

	// service reads for local. remote will only send.
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
		ToPeerID:    msg.HDR.ToPeerID,
		FromPeerID:  msg.HDR.FromPeerID,
		CallID:      msg.HDR.CallID,
		CircuitName: ckt.circuitName,

		FragType: msg.HDR.Subject,    // right?
		FragPart: msg.HDR.StreamPart, // right?

		Args:    msg.HDR.Args,
		Payload: msg.JobSerz,
		//Err:         "",
	}
	return
}

// outgoing messages
func (ckt *Circuit) convertFragmentToMessage(frag *Fragment) (msg *Message) {
	msg = NewMessage()

	var from, to string
	hdr := NewHDR(from, to, ckt.peerServiceName, CallOneWay, 0)
	hdr.Subject = ckt.circuitName
	hdr.ToPeerID = ckt.remotePeerID
	hdr.FromPeerID = ckt.localPeerID
	msg.HDR = *hdr
	return
}

// RegisterPeer(peerServiceName string, peerStreamFunc PeerServiceFunc)

func (rpb *remotePeerback) NewCircuit(circuitName string) (ckt *Circuit, ctx2 context.Context, err error) {

	var canc2 context.CancelFunc
	ctx2, canc2 = context.WithCancel(rpb.localPeerback.ctx)
	reads := make(chan *Fragment)
	errors := make(chan *Fragment)
	ckt = &Circuit{
		peerServiceName: rpb.localPeerback.peerServiceName,
		pbFrom:          rpb.localPeerback,
		pbTo:            rpb,
		circuitName:     circuitName,
		localPeerID:     rpb.localPeerback.peerID,
		remotePeerID:    rpb.peerID, // yes this is right.
		callID:          NewCallID(),
		Reads:           reads,
		Errors:          errors,
		ctx:             ctx2,
		canc:            canc2,
	}
	rpb.localPeerback.handleChansNewCircuit <- ckt
	//fp.u.GetReadsForCallID(h.Reads, h.callID)
	//fp.u.GetErrorsForCallID(h.Errors, h.callID)
	return
}

// backgroun goroutine to translate *Message -> *Fragment
func (h *Circuit) pump() {

}

func (h *Circuit) CallID() string { return h.callID }

func (h *Circuit) Close() {
	h.canc()
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
	IncomingCircuit() (circuitName string, ckt *Circuit, ctx context.Context, err error)

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
	NewCircuit(circuitName string) (ckt *Circuit, ctx context.Context, err error)

	// SendOneWayMessage sends a Frament on the given Circuit.
	SendOneWayMessage(ckt *Circuit, frag *Fragment, errWriteDur *time.Duration) error

	PeerID() string
}

// aka: for ease of copying,
// type PeerServiceFunc func(peerServiceName string, ourPeerID string, ctx0 context.Context, newPeerCh <-chan Peer) error

// PeerServiceFunc is implemented by user's peer services,
// and registered on a Client or a Server under a
// specific peerServiceName by using the
// PeerAPI.RegisterPeerServiceFunc() call.
type PeerServiceFunc func(

	// how we were registered/invoked.
	peerServiceName string,

	// our PeerID
	ourPeerID string,

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

// Users write a PeerImpl and PeerServiceFunc, like this: TODO move to example.go?

// PeerImpl demonstrates how a user can implement a Peer.
//
//msgp:ignore PeerImpl
type PeerImpl struct {
	KnownPeers []string
	StartCount atomic.Int64
}

func (me *PeerImpl) Start(

	// how we were registered/invoked.
	peerServiceName string,

	// our own PeerID
	ourPeerID string,

	// overall context of Client/Server host, if
	// it starts shutdown, this will cancel, and
	// we should cleanup and return; as should any
	// child goroutines.
	ctx0 context.Context,

	// first on newPeerCh will be the client or server who invoked us.
	newPeerCh <-chan RemotePeer,

) error {

	nStart := me.StartCount.Add(1)
	vv("PeerImpl.Start() top. ourPeerID = '%v'; peerServiceName='%v'; StartCount = %v", ourPeerID, peerServiceName, nStart)

	var wg sync.WaitGroup
	defer wg.Wait() // wait for everyone to shutdown/catch stragglers that don't by hanging.

	done0 := ctx0.Done()

	for {
		select {
		// new connection arrives, will be the remote that asked for us first.
		case peer := <-newPeerCh:
			wg.Add(1)

			vv("got from newPeerCh! '%v' sees new peer: '%#v'", peerServiceName, peer)

			// talk to this peer on a separate goro if you wish:
			go func(peer RemotePeer) {
				defer wg.Done()

				// new circuit on that connection
				ckt, ctx, err := peer.NewCircuit("circuitName")
				panicOn(err)
				defer ckt.Close()
				done := ctx.Done()

				outFrag := ckt.NewFragment()
				// set Payload, other details ... then:

				err = peer.SendOneWayMessage(ckt, outFrag, nil)
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
	peerID string
}

type remotePeer struct {
	peerServiceName string
	peerID          string
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
	p.localServiceNameMap[peerServiceName] = &knownLocalPeer{peerServiceFunc: peer, peerServiceName: peerServiceName}
	return nil
}

func (p *peerAPI) StartLocalPeer(ctx context.Context, peerServiceName string, knownPeerIDs ...string) (localPeerID string, err error) {
	p.mut.Lock()
	defer p.mut.Unlock()
	local, ok := p.localServiceNameMap[peerServiceName]
	if !ok {
		return "", fmt.Errorf("no local peerServiceName '%v' available", peerServiceName)
	}

	for _, knownID := range knownPeerIDs {
		known, ok := p.remoteKnownPeers[knownID]
		if !ok {
			known = &knownRemotePeer{
				peerID: knownID,
			}
			p.remoteKnownPeers[knownID] = known
		}
	}

	newPeerCh := make(chan RemotePeer, 1) // must be buffered >= 1, see below.
	ctx1, canc1 := context.WithCancel(ctx)
	localPeerID = NewCallID()

	backer := p.newLocalPeerback(ctx1, canc1, p.u, localPeerID, newPeerCh, peerServiceName)

	local.mut.Lock()
	if local.active == nil {
		local.active = make(map[string]*localPeerback)
	}
	local.active[localPeerID] = backer
	local.mut.Unlock()

	// nope! don't send to ourselves! :)
	// newPeerCh <- backer // newPeerCh is buffered above, so this cannot block.

	go func() {

		local.peerServiceFunc(peerServiceName, localPeerID, ctx, newPeerCh)

	}()

	return
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
	hdr.ServiceName = peerServiceName
	callID := NewCallID()
	hdr.CallID = callID
	msg.HDR = *hdr

	ch := make(chan *Message, 100)
	p.u.GetReadsForCallID(ch, callID)
	// be sure to cleanup. We won't need this channel again.
	defer p.u.UnregisterChannel(callID, CallIDReadMap)

	pollInterval := waitUpTo / 50

	for i := 0; i < 50; i++ {
		err = p.u.SendOneWayMessage(ctx, msg, nil)
		if err != nil {
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
		} else {
			//vv("SendOneWayMessage retried %v times before succeess; pollInterval: %v",
			//	i, pollInterval)
			break
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
	return remotePeerID, nil
}
