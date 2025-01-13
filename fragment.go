package rpc25519

import (
	"context"
	"sync"
	"time"
)

//go:generate greenpack

// Circuit is a handle to the two-way,
// asynchronous, communication channel
// between two Peers.

//msgp:ignore Circuit
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

// provides the Peer interface to PeerStreamFunc.
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

// RegisterPeer(peerServiceName string, peerStreamFunc PeerStreamFunc)

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

type Peer interface {

	// handle tells the SendOneWayMessage machinery
	// how to reply to you. It makes a new CallID,
	// and manages it for you. It gives you two
	// channels to get normal/error replies on. Using this handle,
	// you can make as many one way calls as you like
	// to the remote Peer. The returned ctx will be
	// cancelled in case of broken/shutdown connection
	// or this application shutting down.
	//
	// You must call Close() on the hdl when you are done with it.
	//
	// When selecting h.Reads and h.Errors, always also
	// select on ctx.Done().
	NewCircuit(circuitName string) (hdl *Circuit, ctx context.Context, err error)

	SendOneWayMessage(hdl *Circuit, frag *Fragment, errWriteDur *time.Duration) error

	ID2() (localPeerID, remotePeerID string)
}

type PeerStreamFunc func(

	// how we were registered/invoked.
	peerServiceName string,

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

// which should get priority, the Obj or the Call ID?

//ObjID : possibly ephemeral instantiation of a Peer service, so maybe
// it should be named PeerID !

// Users write a PeerStreamFunc, like this:

type PeerImpl struct {
}

func (me *PeerImpl) PeerStream(

	// how we were registered/invoked.
	peerServiceName string,

	// overall context of Client/Server host, if
	// it starts shutdown, this will cancel, and
	// we should cleanup and return; as should any
	// child goroutines.
	ctx0 context.Context,

	// first on newPeerCh will be the client or server who invoked us.
	newPeerCh <-chan Peer,

) error {

	var wg sync.WaitGroup
	defer wg.Wait() // wait for everyone to shutdown gracefully.

	done0 := ctx0.Done()

	for {
		select {
		case peer := <-newPeerCh:
			wg.Add(1)

			// talk to this peer on a separate goro if you wish:
			go func(peer Peer) {
				defer wg.Done()

				hdl, ctx, err := peer.NewCircuit("circuitName")
				panicOn(err)
				done := ctx.Done()

				outFrag := hdl.NewFragment()
				// set Payload, other details ... then:

				err = peer.SendOneWayMessage(hdl, outFrag, nil)
				panicOn(err)

				for {
					select {
					case frag := <-hdl.Reads:
						_ = frag
					case fragerr := <-hdl.Errors:
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

// ideally both Client and Server can use the same.
type peerAPI struct {
	u   UniversalCliSrv
	mut sync.Mutex

	serviceNameMap map[string]PeerStreamFunc
}

func newPeerAPI(u UniversalCliSrv) *peerAPI {
	return &peerAPI{
		u:              u,
		serviceNameMap: make(map[string]PeerStreamFunc),
	}
}

func (p *peerAPI) RegisterPeerStreamFunc(peerServiceName string, peer PeerStreamFunc) {

	if peerServiceName == "" || peer == nil {
		panic("peerServiceName cannot be empty, peer cannot be nil")
	}
	p.mut.Lock()
	defer p.mut.Unlock()
	p.serviceNameMap[peerServiceName] = peer
}
