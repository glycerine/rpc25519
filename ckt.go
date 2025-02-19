package rpc25519

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"

	//"sync/atomic"
	"time"

	"github.com/glycerine/idem"
)

//go:generate greenpack

// Circuit is a handle to the two-way,
// asynchronous, communication channel
// between two Peers.
//
// It is returned from RemotePeer.NewCircuit(), or
// from LocalPeer.NewCircuitToPeerURL().
//
//msgp:ignore Circuit
type Circuit struct {
	LpbFrom *LocalPeer
	RpbTo   *RemotePeer

	LocalPeerID  string
	RemotePeerID string

	LocalServiceName  string
	RemoteServiceName string

	CircuitID string // aka Message.HDR.CallID
	Context   context.Context
	Canc      context.CancelCauseFunc

	Name   string
	Reads  chan *Fragment // users should treat as read-only.
	Errors chan *Fragment // ditto.

	Halt *idem.Halter

	// racey! use ckt.Halt.ReqStop.CloseWithReason() and Reason() instead.
	// CloseReasonErr error
}

func (ckt *Circuit) String() string {
	return fmt.Sprintf(`&Circuit{
	Name: "%v",
	CircuitID: "%v",

	LocalPeerID: "%v",
	RemotePeerID: "%v",

	LocalServiceName: "%v",
	RemoteServiceName: "%v",

    // LocalCircuitURL: "%v",
    // RemoteCircuitURL: "%v",
}`, ckt.Name,
		ckt.CircuitID,
		ckt.LocalPeerID,
		ckt.RemotePeerID,
		ckt.LocalServiceName,
		ckt.RemoteServiceName,
		ckt.LocalCircuitURL(),
		ckt.RemoteCircuitURL(),
	)
}

// CircuitURL format: tcp://x.x.x.x:port/peerServiceName/peerID/circuitID
// where peerID and circuitID (same as our CallID type), and are
// base64 URL encoded. The IDs do not include the '/' character,
// and thus are "URL safe".
//
//	(CircuitID is the CallID in the Message.HDR)
func (ckt *Circuit) LocalCircuitURL() string {
	return ckt.LpbFrom.NetAddr + "/" +
		ckt.LocalServiceName + "/" +
		ckt.LocalPeerID + "/" +
		ckt.CircuitID
}

func (ckt *Circuit) RemoteCircuitURL() string {
	return ckt.RpbTo.NetAddr + "/" +
		ckt.RemoteServiceName + "/" +
		ckt.RemotePeerID + "/" +
		ckt.CircuitID
}

func (ckt *Circuit) IsClosed() bool {
	return ckt.Halt.ReqStop.IsClosed()
}

// ID2 supplies the local and remote PeerIDs.
func (ckt *Circuit) ID2() (LocalPeerID, RemotePeerID string) {
	return ckt.LpbFrom.PeerID, ckt.RpbTo.PeerID
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
	FragOp      int    `zid:"6"`
	FragSubject string `zid:"7"`
	FragPart    int64  `zid:"8"`

	// Args whose keys start with '#' are reserved for the system.
	// Use frag.SetUserArg() to set Args safely. This will auto-
	// allocate the map if need be; for efficiency it is nil
	// by default as it may not always be in use.
	Args    map[string]string `zid:"9"`
	Payload []byte            `zid:"10"`
	Err     string            `zid:"11"` // distinguished field for error messages.
}

// SetUserArg should be used in user code to set
// Args key/values on the Fragments Args. It will
// allocate the map if need be, and prevents collisions
// with system args in use by disallowing keys
// that start with '#'.
// Empty string keys are not allowed, and will panic.
func (f *Fragment) SetUserArg(key, val string) {
	if key == "" {
		panic("empty string keys are not allowed in Args")
	}
	if key[0] == '#' {
		panic(fmt.Sprintf("Fragment.GetUserArg error: "+
			"user keys cannot start with '#': bad key '%v'", key))
	}
	if f.Args == nil {
		f.Args = make(map[string]string)
	}
	f.Args[key] = val
}

// GetUserArg should be used by user code to get
// Args key/value pairs.
func (f *Fragment) GetUserArg(key string) (val string, ok bool) {
	if key == "" || f.Args == nil {
		return "", false
	}
	if key[0] == '#' {
		panic(fmt.Sprintf("Fragment.GetUserArg error: "+
			"user keys cannot start with '#': bad key '%v'", key))
	}
	val, ok = f.Args[key]
	return
}

// SetSysArgs is for rpc25519 internals. User code
// should use SetUserArg instead to avoid colliding
// with system keys.
// Empty string keys are not allowed, and will panic.
func (f *Fragment) SetSysArg(key, val string) {
	if key == "" {
		panic("empty string keys are not allowed in Args")
	}
	if f.Args == nil {
		f.Args = make(map[string]string)
	}
	f.Args["#"+key] = val
}

// GetSysArg is for rpc25519 internals. User code
// should use GetUserArg instead. This avoids
// collisions between user and system keys.
func (f *Fragment) GetSysArg(key string) (val string, ok bool) {
	if key == "" || f.Args == nil {
		return "", false
	}
	val, ok = f.Args["#"+key]
	return
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
    "Err": %q,
}`,
		AliasDecode(f.FromPeerID),
		AliasDecode(f.ToPeerID),
		AliasDecode(f.CircuitID),
		f.Serial,
		f.ServiceName,
		f.Typ,
		FragOpDecode(f.FragOp),
		f.FragSubject,
		f.FragPart,
		f.Args,
		fmt.Sprintf("(len %v bytes)", len(f.Payload)),
		//string(f.Payload), // for debugging
		f.Err,
	)

}

// RemotePeer is the user facing interface to
// communicating with network-remote Peers. Peers exchange
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
//
// RemotePeer is passed over the newCircuitCh channel
// to a PeerServiceFunc.
//
// The adjective "remote" means we a handle/proxy to the actual remote Peer
// living on a remote node.
//
// Locally, a RemotePeer is always a child of a LocalPeers.
// A RemotePeer can be requested by calling NewCircuit, or received
// on newCircuitChan from a remote peer who called NewCircuitToPeerURL.
//
//msgp:ignore RemotePeer
type RemotePeer struct {
	LocalPeer         *LocalPeer
	PeerID            string
	NetAddr           string
	RemoteServiceName string
	PeerURL           string
	IncomingCkt       *Circuit // first one to arrive
}

// LocalPeer in the backing behind each local instantiation of a PeerServiceFunc.
// local peers do reads on ch, get notified of new connections on newCircuitChan.
// and create new outgoing connections with
//
//msgp:ignore LocalPeer
type LocalPeer struct {
	Halt            *idem.Halter
	NetAddr         string
	PeerServiceName string
	PeerAPI         *peerAPI
	Ctx             context.Context
	Canc            context.CancelCauseFunc
	PeerID          string
	U               UniversalCliSrv
	NewPeerCh       chan *Circuit
	ReadsIn         chan *Message
	ErrorsIn        chan *Message

	Remotes               *Mutexmap[string, *RemotePeer]
	HandleChansNewCircuit chan *Circuit
	HandleCircuitClose    chan *Circuit
	QueryCh               chan *QueryLocalPeerPump

	// should we shut ourselves down when no more peers?
	AutoShutdownWhenNoMorePeers    bool
	AutoShutdownWhenNoMoreCircuits bool
}

// ServiceName is the string used when we were registered/invoked.
func (s *LocalPeer) ServiceName() string {
	return s.PeerServiceName
}

// URL give the network address, the service name, and the PeerID
// in a URL safe string, suitable for contacting the peer.
// e.g. tcp://x.x.x.x:port/peerServiceName/peerID
func (s *LocalPeer) URL() string {
	return s.NetAddr + "/" +
		s.PeerServiceName + "/" +
		s.PeerID
}

func (s *LocalPeer) Close() {
	s.Canc(fmt.Errorf("LocalPeer.Close() called. stack='%v'", stack()))
	s.Halt.ReqStop.Close()
	//<-s.Halt.Done.Chan // hung here so often. just seems a bad idea.
}

// NewCircuitToPeerURL sets up a persistent communication path called a Circuit.
// The frag can be nil, or set to send it immediately.
func (s *LocalPeer) NewCircuitToPeerURL(
	circuitName string,
	peerURL string,
	frag *Fragment,
	errWriteDur time.Duration,
) (ckt *Circuit, ctx context.Context, err error) {

	if s.Halt.ReqStop.IsClosed() {
		return nil, nil, ErrHaltRequested
	}

	netAddr, serviceName, peerID, circuitID, err := ParsePeerURL(peerURL)
	//vv("netAddr from ParsePeerURL = '%v' (peerURL = '%v');", netAddr, peerURL)
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
		frag = s.U.NewFragment()
	}
	frag.FromPeerID = s.PeerID
	frag.ToPeerID = peerID

	// circuitID will be empty, want to create a new CallID.
	// allow joining a extant circuit? let's not for now.
	// Its just much simpler to start with.
	circuitID = NewCallID(serviceName)
	frag.CircuitID = circuitID
	frag.ServiceName = serviceName

	rpb := &RemotePeer{
		LocalPeer:         s,
		PeerID:            peerID,
		NetAddr:           netAddr,
		RemoteServiceName: serviceName,
	}
	//vv("rpb = '%#v'", rpb)
	s.Remotes.Set(peerID, rpb)

	ckt, ctx, err = s.newCircuit(circuitName, rpb, circuitID, frag, errWriteDur, true)
	if err != nil {
		return nil, nil, err
	}
	return
}

func ParsePeerURL(peerURL string) (netAddr, serviceName, peerID, circuitID string, err error) {
	//vv("ParsePeerURL(peerURL = '%v') top.", peerURL)
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
	////vv("u = '%#v'", u)
	return
}

func (ckt *Circuit) SendOneWay(frag *Fragment, errWriteDur time.Duration) error {
	return ckt.LpbFrom.SendOneWay(ckt, frag, errWriteDur)
}

// SendOneWayMessage sends a Frament on the given Circuit.
// We check for cancelled ckt and LocalPeer and return an error
// rather than send if they are shutting down.
func (s *LocalPeer) SendOneWay(ckt *Circuit, frag *Fragment, errWriteDur time.Duration) (err error) {

	// regular quick check if shutdown is requested.
	select {
	case <-s.Halt.ReqStop.Chan:
		return ErrHaltRequested
	case <-ckt.Context.Done():
		return ErrContextCancelled
	case <-s.Ctx.Done():
		return ErrContextCancelled
	default:
	}

	if frag == nil {
		return fmt.Errorf("frag cannot be nil")
	}
	frag.CircuitID = ckt.CircuitID
	frag.FromPeerID = ckt.LocalPeerID
	frag.ToPeerID = ckt.RemotePeerID

	//vv("sending frag='%v'", frag)
	msg := ckt.ConvertFragmentToMessage(frag)
	s.U.FreeFragment(frag)

	err = s.U.SendOneWayMessage(s.Ctx, msg, errWriteDur)
	if err != nil {
		return err
	}

	if errWriteDur >= 0 {
		// wait for sendloop to get message into OS buffers.
		select {
		case <-msg.DoneCh.WhenClosed():
			return msg.LocalErr
		case <-s.Halt.ReqStop.Chan:
			return ErrHaltRequested
		case <-ckt.Context.Done():
			return ErrContextCancelled
		case <-s.Ctx.Done():
			return ErrContextCancelled
		}
	}
	return
}

func (peerAPI *peerAPI) newLocalPeer(
	ctx context.Context,
	cancelFunc context.CancelCauseFunc,
	u UniversalCliSrv,
	peerID string,
	newCircuitCh chan *Circuit,
	peerServiceName,
	netAddr string,

) (pb *LocalPeer) {

	pb = &LocalPeer{
		Halt:            idem.NewHalter(),
		NetAddr:         netAddr,
		PeerServiceName: peerServiceName,
		PeerAPI:         peerAPI,
		Ctx:             ctx,
		Canc:            cancelFunc,
		PeerID:          peerID,
		U:               u,
		NewPeerCh:       newCircuitCh,
		ReadsIn:         make(chan *Message, 1),
		ErrorsIn:        make(chan *Message, 1),

		Remotes:               NewMutexmap[string, *RemotePeer](),
		HandleChansNewCircuit: make(chan *Circuit),
		HandleCircuitClose:    make(chan *Circuit),
		QueryCh:               make(chan *QueryLocalPeerPump),
	}

	AliasRegister(peerID, peerID+" ("+peerServiceName+")")

	// on host shutdown, they will call
	// hhalt.StopTreeAndWaitTilDone() so
	// we will get a ReqStop and wait until Done (or 500 msec)
	// by adding our halter as a child of theirs.
	hhalt := u.GetHostHalter()
	hhalt.AddChild(pb.Halt)

	// service reads for local.
	u.GetReadsForToPeerID(pb.ReadsIn, peerID)
	u.GetErrorsForToPeerID(pb.ErrorsIn, peerID)
	go pb.peerbackPump()

	return pb
}

// incoming
func (ckt *Circuit) ConvertMessageToFragment(msg *Message) (frag *Fragment) {
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
	if frag.Serial == 0 {
		msg.HDR.Serial = issueSerial()
	} else {
		msg.HDR.Serial = frag.Serial
	}

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

	if frag.Err != "" {
		msg.HDR.Typ = CallPeerError
	}

	////vv("ToMessage did frag='%v' -> msg='%v'", frag, msg)

	return
}

// ConvertFragmentToMessage creates outgoing messages
// from the LocalPeer over the Circuit.
// If frag.{ToPeerID,FromPeerID,CircuitID} are not
// set on frag, they will be filled in from the ckt.
func (ckt *Circuit) ConvertFragmentToMessage(frag *Fragment) (msg *Message) {

	msg = frag.ToMessage()

	if msg.HDR.ToPeerID == "" {
		msg.HDR.ToPeerID = ckt.RemotePeerID
	}
	if msg.HDR.FromPeerID == "" {
		msg.HDR.FromPeerID = ckt.LocalPeerID
	}
	if msg.HDR.CallID == "" {
		msg.HDR.CallID = ckt.CircuitID
	}

	if msg.HDR.To == "" {
		msg.HDR.To = ckt.RpbTo.NetAddr
	}
	if msg.HDR.From == "" {
		msg.HDR.From = ckt.LpbFrom.NetAddr
	}
	if msg.HDR.ServiceName == "" {
		msg.HDR.ServiceName = ckt.RemoteServiceName
	}
	if msg.HDR.Args == nil {
		msg.HDR.Args = make(map[string]string)
	}
	msg.HDR.Args["#fromServiceName"] = ckt.LocalServiceName

	return
}

// NewCircuit generates a Circuit between the same two Peers
// as the origCkt.
//
// General Circuit functionality:
// A Circuit ckt gives you two channels, ckt.Reads and ckt.Errors,
// to get normal and error replies on. Using this Circuit,
// you can make as many one way calls as you like
// to the remote Peer. The returned ctx will be
// cancelled in case of broken/shutdown connection
// or this application shutting down.
//
// You must call Close() on the ckt when you are done with it.
//
// The circuitName is a convenience and debugging aid.
// The CircuitID (a.k.a. CallID in Message) determines delivery.
//
// When select{}-ing on ckt.Reads and ckt.Errors, always also
// select on ctx.Done() and in order to shutdown gracefully.
func (origCkt *Circuit) NewCircuit(circuitName string, firstFrag *Fragment) (ckt *Circuit, ctx2 context.Context, err error) {
	return origCkt.RpbTo.LocalPeer.newCircuit(circuitName, origCkt.RpbTo, "", firstFrag, -1, true)
}

// IsClosed returns true if the LocalPeer is shutting down
// or has already been closed/shut down.
func (lpb *LocalPeer) IsClosed() bool {
	return lpb.Halt.ReqStop.IsClosed()
}

// QueryLocalPeerPump asks the LocalPeer about
// its OpenCircuitCount.
type QueryLocalPeerPump struct {
	OpenCircuitCount int
	Ready            chan struct{}
}

// NewQueryLocalPeerPump creates a new QueryLocalPeerPump
// to enquire about the number of open Circuits.
func NewQueryLocalPeerPump() *QueryLocalPeerPump {
	return &QueryLocalPeerPump{
		Ready: make(chan struct{}),
	}
}

// OpenCircuitCount returns the number of
// open circuits or -1 if this was unavailable
// because of shutdown. Inherently this count
// is a point in time snapshot and may be stale by the
// time it is actually returned.
func (lpb *LocalPeer) OpenCircuitCount() int {
	query := NewQueryLocalPeerPump()
	select {
	case lpb.QueryCh <- query:
	case <-lpb.Halt.ReqStop.Chan:
		return -1
	}
	select {
	case <-query.Ready:
		return query.OpenCircuitCount
	case <-lpb.Halt.ReqStop.Chan:
		return -1
	}
}

func (lpb *LocalPeer) newCircuit(
	circuitName string,
	rpb *RemotePeer,
	cID string,
	firstFrag *Fragment,
	errWriteDur time.Duration,
	tellRemote bool, // send new circuit to remote?

) (ckt *Circuit, ctx2 context.Context, err error) {

	if lpb.Halt.ReqStop.IsClosed() {
		return nil, nil, ErrHaltRequested
	}

	var canc2 context.CancelCauseFunc
	ctx2, canc2 = context.WithCancelCause(lpb.Ctx)
	reads := make(chan *Fragment)
	errors := make(chan *Fragment)
	ckt = &Circuit{
		Halt:              idem.NewHalter(),
		Name:              circuitName,
		LocalServiceName:  lpb.PeerServiceName,
		RemoteServiceName: rpb.RemoteServiceName,
		LpbFrom:           lpb,
		RpbTo:             rpb,
		CircuitID:         cID,
		LocalPeerID:       lpb.PeerID,
		RemotePeerID:      rpb.PeerID,
		Reads:             reads,
		Errors:            errors,
		Context:           ctx2,
		Canc:              canc2,
	}
	if ckt.CircuitID == "" {
		ckt.CircuitID = NewCallID(circuitName)
	}
	//AliasRegister(ckt.CircuitID, ckt.CircuitID+" ("+circuitName+")")

	//lpb.Halt.AddChild(ckt.Halt) // no worries: pump will do this.

	sendCkt := true
	select {
	case lpb.HandleChansNewCircuit <- ckt:
		sendCkt = false
	case <-lpb.Halt.ReqStop.Chan:
		return nil, nil, ErrHaltRequested
	case <-time.After(time.Millisecond * 10):
		// leave sendCkt true so we
		// do it in the background on the goroutine next:
	}

	go func(sendCkt bool) { // instead of in cli read loop
		if sendCkt {
			select {
			case lpb.HandleChansNewCircuit <- ckt:

			case <-lpb.Halt.ReqStop.Chan:
				//return nil, nil, ErrHaltRequested
				return

				//case <-time.After(time.Second * 10):
				//	panic(fmt.Sprintf("problem: could not access pump loop to create newCircuit after 10 sec; trying to make '%v'", circuitName))
			}
		}
		//vv("tellRemote = %v", tellRemote)
		if tellRemote {
			var msg *Message
			if firstFrag != nil {
				msg = firstFrag.ToMessage()
			} else {
				msg = NewMessage()
			}
			msg.HDR.To = rpb.NetAddr
			//vv("rpb.NetAddr = '%v'", rpb.NetAddr)
			msg.HDR.From = lpb.NetAddr
			msg.HDR.Typ = CallPeerStartCircuit
			msg.HDR.Created = time.Now()
			msg.HDR.FromPeerID = lpb.PeerID
			msg.HDR.ToPeerID = rpb.PeerID
			msg.HDR.CallID = ckt.CircuitID
			msg.HDR.Serial = issueSerial()
			msg.HDR.ServiceName = ckt.RemoteServiceName

			// tell the remote which serviceName we are coming from;
			// so the URL back can be correct.
			msg.HDR.Args = map[string]string{
				"#fromServiceName": lpb.PeerServiceName,
				"#circuitName":     circuitName}
			err = lpb.U.SendOneWayMessage(ctx2, msg, errWriteDur)
		}
	}(sendCkt)

	return
}

// Close must be called on a Circuit to release resources
// when you are done with it. use reason nil for normal
// all-good shutdown of a circuit, or to report an error.
func (h *Circuit) Close(reason error) {
	// must be idemopotent. Often called many times
	// during normal Circuit shutdown.

	// set reason atomically.
	h.Halt.ReqStop.CloseWithReason(reason)

	select {
	case h.LpbFrom.HandleCircuitClose <- h:
	case <-h.LpbFrom.Halt.ReqStop.Chan:
	}
}

// one line version of the below, for ease of copying.
// type PeerServiceFunc func(myPeer LocalPeer, ctx0 context.Context, newCircuitCh <-chan *Circuit) error

// PeerServiceFunc is implemented by user's peer services,
// and registered on a Client or a Server under a
// specific peerServiceName by using the
// PeerAPI.RegisterPeerServiceFunc() call.
type PeerServiceFunc func(

	// our local Peer interface, can do NewCircuitToPeerURL() to send to URL.
	myPeer *LocalPeer,

	// ctx0 supplies the overall context of the
	// Client/Server host. If our hosts starts
	// shutdown, this will cancel, and
	// we should cleanup and return; as should any
	// child goroutines.
	ctx0 context.Context,

	// first on newCircuitCh will be the remote client
	// or server who invoked us.
	newCircuitCh <-chan *Circuit,

) error

// A peerAPI is provided under the Client and Server PeerAPI
// members. They use the same peerAPI
// implementation. It is designed for symmetry.
type peerAPI struct {
	u   UniversalCliSrv
	mut sync.Mutex

	// peerServiceName key
	localServiceNameMap *Mutexmap[string, *knownLocalPeer]

	isCli bool
}

func newPeerAPI(u UniversalCliSrv, isCli bool) *peerAPI {
	return &peerAPI{
		u:                   u,
		localServiceNameMap: NewMutexmap[string, *knownLocalPeer](),
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

	active *Mutexmap[string, *LocalPeer]
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

	p.localServiceNameMap.Set(peerServiceName, &knownLocalPeer{
		peerServiceFunc: peer, peerServiceName: peerServiceName})

	return nil
}

// StartLocalPeer runs the registered peerServiceName's PeerServiceFunc
// on its own goroutine, and starts a background "pump" goroutine to
// support to the user's service function.
//
// Users must call lpb.Close() when done with the LocalPeer here obtained.
//
// That is, users should typically do a `defer lpb.Close()` immediately after
// obtaining their lpb in a call such as `lpb, err := StartLocalPeer()`.
//
// If lpb.Close() is not called in a defer, the user must
// otherwise ensure that the LocalPeer.Close() function is
// eventually called.
//
// The requestedCircuit parameter can be nil. It is used by the system
// when StartRemotePeer() sends a request to start the service
// and establish a new Circuit.
func (p *peerAPI) StartLocalPeer(
	ctx context.Context,
	peerServiceName string,
	requestedCircuit *Message,

) (lpb *LocalPeer, err error) {

	p.mut.Lock()
	defer p.mut.Unlock()

	return p.unlockedStartLocalPeer(ctx, peerServiceName, requestedCircuit, false, nil, "")
}

func (p *peerAPI) unlockedStartLocalPeer(
	ctx context.Context,
	peerServiceName string,
	requestedCircuit *Message,
	isUpdatedPeerID bool,
	sendCh chan *Message,
	pleaseAssignNewPeerID string,

) (lpb *LocalPeer, err error) {

	knownLocalPeer, ok := p.localServiceNameMap.Get(peerServiceName)
	if !ok {
		return nil, fmt.Errorf("no local peerServiceName '%v' available", peerServiceName)
	}

	newCircuitCh := make(chan *Circuit, 1) // must be buffered >= 1, see below.
	ctx1, canc1 := context.WithCancelCause(ctx)

	var localPeerID string
	if pleaseAssignNewPeerID == "" {
		localPeerID = NewCallID(peerServiceName)
	} else {
		localPeerID = pleaseAssignNewPeerID
	}
	AliasRegister(localPeerID, localPeerID+" ("+peerServiceName+")")

	localAddr := p.u.LocalAddr()
	//vv("unlockedStartLocalPeer: localAddr = '%v'", localAddr)
	lpb = p.newLocalPeer(ctx1, canc1, p.u, localPeerID, newCircuitCh, peerServiceName, localAddr)

	knownLocalPeer.mut.Lock()
	if knownLocalPeer.active == nil {
		knownLocalPeer.active = NewMutexmap[string, *LocalPeer]()
	}
	knownLocalPeer.active.Set(localPeerID, lpb)
	knownLocalPeer.mut.Unlock()

	go func() {
		////vv("launching new peerServiceFunc invocation for '%v'", peerServiceName)
		err := knownLocalPeer.peerServiceFunc(lpb, ctx1, newCircuitCh)

		////vv("peerServiceFunc has returned: '%v'; clean up the lbp!", peerServiceName)
		canc1(fmt.Errorf("peerServiceFunc '%v' finished. returned err = '%v'", peerServiceName, err))
		lpb.Close()
		// this handles locking on its own.
		knownLocalPeer.active.Del(localPeerID)

	}()

	//localPeerURL := lpb.URL()
	vv("unlockedStartLocalPeer: lpb.URL() = '%v'; peerServiceName='%v', isUpdatedPeerID='%v'; pleaseAssignNewPeerID='%v'; \nstack=%v\n", lpb.URL(), peerServiceName, isUpdatedPeerID, pleaseAssignNewPeerID, stack())

	if requestedCircuit != nil {
		return lpb, lpb.provideRemoteOnNewPeerCh(p.isCli, requestedCircuit, ctx1, sendCh, isUpdatedPeerID)
	}

	return lpb, nil
}

// StartRemotePeer boots up a peer a remote node.
// It must already have been registered on the
// client or server running there.
//
// If a waitUpTo duration is provided, we will poll in the
// event of an error, since there can be races when
// doing simultaneous client and server setup (in
// tests in particular!). We will
// only return an error after waitUpTo has passed. To
// disable polling set waitUpTo to zero. We poll up
// to 50 times, pausing waitUpTo/50 after each.
// If SendAndGetReply succeeds, then we immediately
// cease polling and return the RemotePeerID.
func (p *peerAPI) StartRemotePeer(ctx context.Context, peerServiceName, remoteAddr string, waitUpTo time.Duration) (remotePeerURL, RemotePeerID string, err error) {

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

	//vv("msg.HDR='%v'", msg.HDR.String()) // "Typ": CallPeerStart seen.

	ch := make(chan *Message, 100)

	// can't use the peerID/ObjID yet because we have no PeerID
	// yet, we are bootstrapping.
	p.u.GetReadsForCallID(ch, callID)
	// be sure to cleanup. We won't need this channel again.
	defer p.u.UnregisterChannel(callID, CallIDReadMap)

	pollInterval := waitUpTo / 50

	for i := 0; i < 50; i++ {
		err = p.u.SendOneWayMessage(ctx, msg, 0)
		if err == nil {
			////vv("SendOneWayMessage retried %v times before succeess; pollInterval: %v",
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

	hhalt := p.u.GetHostHalter()

	//vv("isCli=%v, StartRemotePeer about to wait for reply on ch to callID = '%v'", p.isCli, callID)
	var reply *Message
	select {
	case reply = <-ch:
		//vv("got reply to CallPeerStart: '%v'", reply.String())
	case <-ctx.Done():
		//vv("ctx.Done() seen, cause: '%v'\n\n stack: '%v'", context.Cause(ctx), stack())
		return "", "", ErrContextCancelled
	case <-hhalt.ReqStop.Chan:
		return "", "", ErrHaltRequested
	}
	var ok bool
	RemotePeerID, ok = reply.HDR.Args["#peerID"]
	if !ok {
		return "", "", fmt.Errorf("remote '%v', peerServiceName '%v' did "+
			"not respond with peerID in Args", remoteAddr, peerServiceName)
	}
	////vv("got RemotePeerID from Args[peerID]: '%v'", RemotePeerID)
	remotePeerURL, ok = reply.HDR.Args["#peerURL"]
	if !ok {
		return "", "", fmt.Errorf("remote '%v', peerServiceName '%v' did "+
			"not respond with peerURL in Args", remoteAddr, peerServiceName)
	}
	vv("StartRemotePeer got remotePeerURL from Args[peerURL]: '%v'", remotePeerURL)
	return remotePeerURL, RemotePeerID, nil
}

// bootstrapCircuit: handle CallPeerStartCircuit
//
// The Client/Server readLoops call us directly when
// they see a CallPeerStart, CallPeerStartCircuit, or CallPeerStartCircuitTakeToID.
//
// The goal of bootstrapCircuit is to enable the user
// peer code to interact with circuits and remote peers.
// We want this user PeerImpl.Start() code to work now:
//
//	select {
//	    // new Circuit connection arrives
//	    case ckt := <-newCircuitCh:  // this needs to be enabled.
//		   wg.Add(1)
//
//		   //vv("got from newCircuitCh! '%v' sees new peerURL: '%v'",
//		       ckt.RemoteServiceName, ckt.RemoteCircuitURL())
//
//		   // talk to this peer on a separate goro if you wish:
//		   go func(ckt *Circuit) {
//			    defer wg.Done()
//			    ctx := ckt.Context
//			    ...
//
// .
func (s *peerAPI) bootstrapCircuit(isCli bool, msg *Message, ctx context.Context, sendCh chan *Message) error {
	//vv("isCli=%v, bootstrapCircuit called with msg='%v'; JobSerz='%v'", isCli, msg.String(), string(msg.JobSerz))

	// find the localPeerback corresponding to the ToPeerID
	localPeerID := msg.HDR.ToPeerID
	peerServiceName := msg.HDR.ServiceName

	s.mut.Lock()
	defer s.mut.Unlock()

	knownLocalPeer, ok := s.localServiceNameMap.Get(peerServiceName)
	if !ok {
		msg.HDR.Typ = CallPeerError
		msg.JobErrs = fmt.Sprintf("no local peerServiceName '%v' available", peerServiceName)
		msg.JobSerz = nil
		//vv("bootstrapCircuit returning early: '%v'", msg.JobErrs)
		return s.replyHelper(isCli, msg, ctx, sendCh)
	}
	//vv("good: bootstrapCircuit found registered peerServiceName: '%v'", peerServiceName)

	needNew := false
	knownLocalPeer.mut.Lock()
	if knownLocalPeer.active == nil {
		knownLocalPeer.active = NewMutexmap[string, *LocalPeer]()
		needNew = true
	}

	pleaseAssignNewRemotePeerID, assignReqOk := msg.HDR.Args["#pleaseAssignNewPeerID"]

	if msg.HDR.Typ == CallPeerStartCircuitTakeToID {
		if msg.HDR.ToPeerID != pleaseAssignNewRemotePeerID {
			panic("inconsistent internal logic! should have msg.HDR.ToPeerID == pleaseAssignNewRemotePeerID")
		}
		pleaseAssignNewRemotePeerID = msg.HDR.ToPeerID
		assignReqOk = true
	}

	var lpb *LocalPeer

	isUpdatedPeerID := false
	if localPeerID == "" || assignReqOk {
		needNew = true
	} else {
		var ok bool
		lpb, ok = knownLocalPeer.active.Get(localPeerID)
		if !ok {
			// start a new instance
			isUpdatedPeerID = true
			needNew = true
		}
	}
	knownLocalPeer.mut.Unlock()

	if needNew {
		// spin one up!
		//vv("needNew true! spinning up a peer for peerServicename '%v'", peerServiceName)
		//lpb2, localPeerURL, localPeerID, err := s.StartLocalPeer(ctx, peerServiceName, msg)
		lpb2, err := s.unlockedStartLocalPeer(ctx, peerServiceName, msg, isUpdatedPeerID, sendCh, pleaseAssignNewRemotePeerID)
		if err != nil {
			// we are probably shutting down; Test408 gets here with
			// "rpc25519 error: halt requested".
			return err
		}
		lpb = lpb2
		// unlockedStartLocalPeer already called provideRemoteOnNewPeerCh.

		// Test400: if we were bootstrapped without a remote local peer, just
		// with a callID, they are waiting for an ack.
		// 	msg.HDR.Typ = CallPeerError
		// 	msg.JobErrs = fmt.Sprintf("have peerServiceName '%v', but none "+
		// 		"active for peerID='%v'; perhaps it died?", peerServiceName, localPeerID)
		// 	msg.HDR.Args = map[string]string{"unknownPeerID": localPeerID}
		// 	msg.JobSerz = nil
		// 	return s.replyHelper(isCli, msg, ctx, sendCh)

		// should? not be needed? for CallPeerStartCircuitTakeToID or CallPeerStartCircuit?
		// only for CallPeerStart... like Test400 without a remote peer yet...
		if msg.HDR.FromPeerID == "" && msg.HDR.Typ == CallPeerStart {
			//vv("bootstrap sees no remote FromPeerID, sending callID ack")
			ack := NewMessage()
			ack.HDR.CallID = msg.HDR.CallID
			ack.HDR.FromPeerID = lpb.PeerID
			ack.HDR.Typ = CallOneWay // not peer/ckt traffic yet, only bootstrapping peer

			ack.HDR.Args = map[string]string{
				"#peerURL":         lpb.URL(),
				"#peerID":          lpb.PeerID,
				"#fromServiceName": lpb.PeerServiceName}

			//ack.HDR.ToPeerID = "" // no remote peer expected!
			//ack.HDR.ServiceName =
			return s.replyHelper(isCli, ack, ctx, sendCh)
		}
		return nil
	}

	return lpb.provideRemoteOnNewPeerCh(isCli, msg, ctx, sendCh, isUpdatedPeerID)
}

func (lpb *LocalPeer) provideRemoteOnNewPeerCh(isCli bool, msg *Message, ctx context.Context, sendCh chan *Message, isUpdatedPeerID bool) error {
	rpb := &RemotePeer{
		LocalPeer: lpb,
		PeerID:    msg.HDR.FromPeerID,
		NetAddr:   msg.HDR.From,
	}
	circuitName := ""
	if msg.HDR.Args != nil {
		rpb.RemoteServiceName = msg.HDR.Args["#fromServiceName"]
		if rpb.RemoteServiceName != "" && rpb.PeerID != "" {
			AliasRegister(rpb.PeerID, rpb.PeerID+" ("+rpb.RemoteServiceName+")")
		}
		circuitName = msg.HDR.Args["#circuitName"]
	}

	ckt, ctx2, err := lpb.newCircuit(circuitName, rpb, msg.HDR.CallID, nil, -1, false)
	if err != nil {
		return err
	}

	rpb.IncomingCkt = ckt

	_ = ckt.LocalCircuitURL() // Keep the call but avoid unused variable
	rpb.PeerURL = ckt.RemoteCircuitURL()

	asFrag := ckt.ConvertMessageToFragment(msg)

	if isUpdatedPeerID {
		// let the remote know that the old peer disappeared
		// and we are the updated version/taking over.
		asFrag.SetSysArg("oldPeerID", msg.HDR.ToPeerID)
		asFrag.SetSysArg("newPeerID", lpb.PeerID)
		asFrag.SetSysArg("newPeerURL", lpb.URL())
	}

	// now we go directly to the NewPeerCh, so user
	// does not need a second step to call IncommingCircuit!
	select {
	case lpb.NewPeerCh <- ckt:
		select {
		case ckt.Reads <- asFrag:
		case <-ckt.Halt.ReqStop.Chan:
		case <-ctx2.Done():
		}
	case <-ctx2.Done():
		//vv("ctx2 cancelled, cause: '%v'", context.Cause(ctx2))
		return ErrContextCancelled
	case <-lpb.Halt.ReqStop.Chan:
		return ErrHaltRequested
	}

	return nil
}

// replyHelper helps bootstrapCircuit with replying, keeping its
// code more compact.
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

// bootstrapPeerService handles HDR.Typ == CallPeerStart
// requests to bootstrap a PeerServiceFunc.
//
// This needs special casing because the inital call API
// is different. See ckt.go; PeerServiceFunc is
// very different from TwoWayFunc or OneWayFunc.
//
// Note: we should only return an error if the shutdown request was received,
// which will kill the readLoop and connection.
func (s *peerAPI) bootstrapPeerService(isCli bool, msg *Message, ctx context.Context, sendCh chan *Message) error {

	////vv("top of bootstrapPeerService(): isCli=%v; msg.HDR='%v'", isCli, msg.HDR.String())

	// starts its own goroutine or return with an error (both quickly).
	lpb, err := s.StartLocalPeer(ctx, msg.HDR.ServiceName, msg)
	localPeerURL := lpb.URL()
	localPeerID := lpb.PeerID

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
			"#peerURL":         localPeerURL,
			"#peerID":          localPeerID,
			"#fromServiceName": msg.HDR.ServiceName}
	}
	msg.HDR.FromPeerID = localPeerID

	select {
	case sendCh <- msg:
	case <-ctx.Done():
		return ErrShutdown()
	}
	return nil
}
