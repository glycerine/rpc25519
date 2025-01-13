package rpc25519

import (
	"context"
	"fmt"
	"sync"
	"time"
)

//go:generate greenpack

// Circuit is a handle to the two-way,
// asynchronous, communication channel
// between two Peers.
//
// It is returned when a Peer calls NewCircuit().
type Circuit struct {
	fp *fragPair

	circuitName string
	peerID      string
	callID      string
	ctx         context.Context

	Reads  <-chan *Fragment
	Errors <-chan *Fragment
}

func (h *Circuit) NewFragment() *Fragment {
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

	// CallID was assigned in the NewCircuit() call.
	CallID string `zid:"1"`

	// CircuitName is what was established by NewCircuit(circuitName)
	CircuitName string `zid:"2"`

	FragType string `zid:"3"` // can be a message type, sub-service name, other useful context.
	FragPart int    `zid:"4"` // built in multi-part handling for the same CallID and FragType.

	Args map[string]string `zid:"5"` // nil/unallocated to save space. User should alloc if the need it.

	Payload []byte `zid:"6"`

	Err string `zid:"7"` // distinguished field for error messages.
}

// provides the Peer interface to PeerServiceFunc.
type fragPair struct {
	u UniversalCliSrv

	myPeerID string
	reads    chan *Message
	errors   chan *Message
}

func newFragPair(u UniversalCliSrv) (fp *fragPair) {
	fp = &fragPair{
		myPeerID: NewCallID(),
		u:        u,
		reads:    make(chan *Message),
		errors:   make(chan *Message),
	}

	u.GetReadsForObjID(fp.reads, fp.myPeerID)
	u.GetErrorsForObjID(fp.errors, fp.myPeerID)

	return fp
}

// RegisterPeer(peerServiceName string, peerStreamFunc PeerServiceFunc)

func (fp *fragPair) NewCircuit(circuitName string) *Circuit {

	h := &Circuit{
		fp:          fp,
		circuitName: circuitName,
		peerID:      fp.myPeerID,
		callID:      NewCallID(),
		Reads:       make(chan *Fragment),
		Errors:      make(chan *Fragment),
	}
	//fp.u.GetReadsForCallID(h.Reads, h.callID)
	//fp.u.GetErrorsForCallID(h.Errors, h.callID)
	return h
}
func (h *Circuit) CallID() string { return h.callID }

func (h *Circuit) Close() {
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
	// You must call Close() on the crkt when you are done with it.
	//
	// When selecting crkt.Reads and crkt.Errors, always also
	// select on ctx.Done() in order to shutdown gracefully.
	NewCircuit(circuitName string) (crkt *Circuit, ctx context.Context, err error)

	// SendOneWayMessage sends a Frament on the given Circuit.
	SendOneWayMessage(crkt *Circuit, frag *Fragment, errWriteDur *time.Duration) error

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
type PeerImpl struct {
	KnownPeers []string
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

	var wg sync.WaitGroup
	defer wg.Wait() // wait for everyone to shutdown/catch stragglers that don't by hanging.

	done0 := ctx0.Done()

	for {
		select {
		case peer := <-newPeerCh:
			wg.Add(1)

			// talk to this peer on a separate goro if you wish:
			go func(peer Peer) {
				defer wg.Done()

				crkt, ctx, err := peer.NewCircuit("circuitName")
				panicOn(err)
				defer crkt.Close()
				done := ctx.Done()

				outFrag := crkt.NewFragment()
				// set Payload, other details ... then:

				err = peer.SendOneWayMessage(crkt, outFrag, nil)
				panicOn(err)

				for {
					select {
					case frag := <-crkt.Reads:
						_ = frag
					case fragerr := <-crkt.Errors:
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

	localServiceNameMap map[string]*knownLocalPeer
	remoteKnownPeers    map[string]*knownRemotePeer
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

type knownLocalPeer struct {
	peerServiceFunc PeerServiceFunc
	peerID          string
	canc            context.CancelFunc
	newPeerCh       chan Peer
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

func (p *peerAPI) StartLocalPeer(peerServiceName string, knownPeerIDs ...string) (localPeerID string, err error) {
	p.mut.Lock()
	defer p.mut.Unlock()
	local, ok := p.localServiceNameMap[peerServiceName]
	if !ok {
		return "", fmt.Errorf("no local peerServiceName '%v' available", peerServiceName)
	}

	for _, knownID := range knownPeerIDs {
		known, ok := p.remoteKnownPeers[knownID]
		if !ok {
			known = &knownRemotePeer{peerID: knownID}
			p.remoteKnownPeers[knownID] = known
		}
	}

	newPeerCh := make(chan Peer)
	ctx, canc := context.WithCancel(context.Background())
	local.canc = canc
	localPeerID = NewCallID()
	local.newPeerCh = newPeerCh

	go local.peerServiceFunc(
		peerServiceName,
		localPeerID,
		ctx,
		newPeerCh)

	return
}

func (p *peerAPI) StartRemotePeer(ctx context.Context, peerServiceName, remoteAddr string) (remotePeerID string, err error) {
	msg := NewMessage()

	msg.HDR.Typ = CallStartPeerCircuit
	msg.HDR.ServiceName = peerServiceName
	msg.HDR.To = remoteAddr
	msg.HDR.From = p.u.LocalAddr()

	callID := NewCallID()
	msg.HDR.CallID = callID

	ch := make(chan *Message, 1)
	p.u.GetReadsForCallID(ch, callID)
	// be sure to cleanup.
	defer p.u.UnregisterChannel(callID, CallIDReadMap)

	err = p.u.SendOneWayMessage(ctx, msg, nil)
	if err != nil {
		return
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
