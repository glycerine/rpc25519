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
	pb *peerback

	circuitName string
	peerID      string
	callID      string
	ctx         context.Context
	canc        context.CancelFunc

	Reads  <-chan *Fragment
	Errors <-chan *Fragment
}

type pairchanfrag struct {
	ckt       *Circuit
	callID    string
	readsOut  chan *Fragment
	errorsOut chan *Fragment
}

func (ckt *Circuit) NewFragment() *Fragment {
	return &Fragment{
		CallID: ckt.callID,
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

	// CallID was assigned in the NewCircuit() call.
	CallID string `zid:"1"`

	// CircuitName is what was established by NewCircuit(circuitName)
	CircuitName string `zid:"2"`

	FragType string `zid:"3"` // can be a message type, sub-service name, other useful context.
	FragPart int64  `zid:"4"` // built in multi-part handling for the same CallID and FragType.

	Args map[string]string `zid:"5"` // nil/unallocated to save space. User should alloc if the need it.

	Payload []byte `zid:"6"`

	Err string `zid:"7"` // distinguished field for error messages.
}

// peerback in the backing behind each Peer interface
// provided over the newPeerCh channel.
// to a PeerServiceFunc.
type peerback struct {
	peerAPI *peerAPI
	u       UniversalCliSrv

	peerID    string
	ctx       context.Context
	canc      context.CancelFunc
	newPeerCh chan Peer

	readsIn  chan *Message
	errorsIn chan *Message

	handleChansNewCircuit chan *pairchanfrag

	// has to be a pair per Circuit, not just a pair here!
	//readsOut  chan *Fragment
	//errorsOut chan *Fragment
}

func (peerAPI *peerAPI) newPeerback(ctx context.Context, cancelFunc context.CancelFunc, u UniversalCliSrv, peerID string, newPeerCh chan Peer) (pb *peerback) {
	pb = &peerback{
		peerAPI:   peerAPI,
		ctx:       ctx,
		canc:      cancelFunc,
		peerID:    peerID,
		u:         u,
		newPeerCh: newPeerCh,
		// has to sort the Message to each circuit and convert to Fragment.
		readsIn:  make(chan *Message, 1),
		errorsIn: make(chan *Message, 1),

		handleChansNewCircuit: make(chan *pairchanfrag),
	}
	u.GetReadsForObjID(pb.readsIn, pb.peerID)
	u.GetErrorsForObjID(pb.errorsIn, pb.peerID)
	go pb.peerbackPump()

	return pb
}

// background goro to read all PeerID *Messages and sort them
// to all the circuits live in this peer.
func (pb *peerback) peerbackPump() {

	// key: CallID (circuit ID)
	m := make(map[string]*pairchanfrag)
	done := pb.ctx.Done()
	for {
		select {
		case pcf := <-pb.handleChansNewCircuit:
			m[pcf.callID] = pcf

		case msg := <-pb.readsIn:
			callID := msg.HDR.CallID
			pcf, ok := m[callID]
			if !ok {
				vv("arg. no ckt avail for callID = '%v'", callID)
				continue
			}
			frag := pcf.ckt.convertMessageToFragment(msg)
			select {
			case pcf.readsOut <- frag:
			case <-done:
				return
			}
		case msgerr := <-pb.errorsIn:
			_ = msgerr
		}
	}
}

func (ckt *Circuit) convertMessageToFragment(msg *Message) (frag *Fragment) {
	frag = &Fragment{
		FromPeerID: msg.HDR.ObjID,
		CallID:     msg.HDR.CallID,

		// CircuitName is what was established by NewCircuit(circuitName)
		CircuitName: ckt.circuitName,
		FragType:    msg.HDR.Subject,    // right?
		FragPart:    msg.HDR.StreamPart, // right?
		Args:        msg.HDR.Args,
		Payload:     msg.JobSerz,
		//Err:         "",
	}
	return
}

// RegisterPeer(peerServiceName string, peerStreamFunc PeerServiceFunc)

func (pb *peerback) NewCircuit(circuitName string) (ckt *Circuit, ctx2 context.Context, err error) {

	var canc2 context.CancelFunc
	ctx2, canc2 = context.WithCancel(pb.ctx)
	reads := make(chan *Fragment)
	errors := make(chan *Fragment)
	ckt = &Circuit{
		pb:          pb,
		circuitName: circuitName,
		peerID:      pb.peerID,
		callID:      NewCallID(),
		Reads:       reads,
		Errors:      errors,
		ctx:         ctx2,
		canc:        canc2,
	}
	pcf := &pairchanfrag{
		ckt:       ckt,
		callID:    ckt.callID,
		readsOut:  reads,
		errorsOut: errors,
	}
	pb.handleChansNewCircuit <- pcf
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
type Peer interface {

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

	// ID2 supplies the local and remote PeerIDs.
	ID2() (localPeerID, remotePeerID string)
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
	newPeerCh <-chan Peer,

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
	newPeerCh <-chan Peer,

) error {

	nStart := me.StartCount.Add(1)
	vv("PeerImpl.Start() top. ourPeerID = '%v'; peerServiceName='%v'; StartCount = %v", ourPeerID, peerServiceName, nStart)

	var wg sync.WaitGroup
	defer wg.Wait() // wait for everyone to shutdown/catch stragglers that don't by hanging.

	done0 := ctx0.Done()

	for {
		select {
		case peer := <-newPeerCh:
			wg.Add(1)

			vv("got from newPeerCh! '%v' sees new peer: '%#v'", peerServiceName, peer)

			// talk to this peer on a separate goro if you wish:
			go func(peer Peer) {
				defer wg.Done()

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

	active map[string]*peerback
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
	p.localServiceNameMap[peerServiceName] = &knownLocalPeer{peerServiceFunc: peer}
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

	newPeerCh := make(chan Peer, 1) // must be buffered >= 1, see below.
	ctx1, canc1 := context.WithCancel(ctx)
	peerID := NewCallID()
	localPeerID = peerID

	backer := p.newPeerback(ctx1, canc1, p.u, peerID, newPeerCh)

	local.mut.Lock()
	if local.active == nil {
		local.active = make(map[string]*peerback)
	}
	local.active[peerID] = backer
	local.mut.Unlock()

	newPeerCh <- backer // newPeerCh is buffered above, so this cannot block.

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

// SendOneWayMessage sends a Frament on the given Circuit.
func (s *peerback) SendOneWayMessage(ckt *Circuit, frag *Fragment, errWriteDur *time.Duration) error {
	return nil

}

// ID2 supplies the local and remote PeerIDs.
func (s *peerback) ID2() (localPeerID, remotePeerID string) { return }
