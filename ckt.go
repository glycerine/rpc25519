package rpc25519

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"

	"sync/atomic"
	"time"

	"github.com/glycerine/idem"
)

//go:generate greenpack

var nextCircuitSN int64

// Circuit is a handle to the two-way,
// asynchronous, communication channel
// between two Peers.
//
// It is returned from RemotePeer.NewCircuit(), or
// from LocalPeer.NewCircuitToPeerURL().
//
//msgp:ignore Circuit
type Circuit struct {
	CircuitSN int64

	LpbFrom *LocalPeer
	RpbTo   *RemotePeer

	LocalPeerID   string
	LocalPeerName string

	RemotePeerID   string
	RemotePeerName string

	LocalServiceName  string
	RemoteServiceName string

	LocalPeerServiceNameVersion  string
	RemotePeerServiceNameVersion string

	CircuitID string // aka Message.HDR.CallID
	Context   context.Context
	Canc      context.CancelCauseFunc

	Name   string
	Reads  chan *Fragment // users should treat as read-only.
	Errors chan *Fragment // ditto.

	Halt *idem.Halter

	// racy! use ckt.Halt.ReqStop.CloseWithReason() and Reason() instead.
	// CloseReasonErr error

	// allow user frameworks to convey
	// info through NewCircuitCh
	UserString string

	FirstFrag *Fragment

	// send/support CallPeerTrafficWithServiceFallback
	// instead of just CallPeerTraffic.
	// We have to have registered the peerServiceName
	// with the notifies for this fallback to
	// work. The use case is to allow a
	// peer to recover after reboot. It's
	// random PeerID will have changed, but
	// its peerServiceName can remain the same.
	// Thus remote incoming fragments can still
	// find the default (singleton; if extant) peer
	// if they mark their circuits as PreferExtant.
	// The madeNewAutoCli return bool can also
	// convey when the PeerID will have changed.
	PreferExtant bool

	// report whether the creation of this circuit
	// involved spinning up a new auto-client.
	MadeNewAutoCli bool

	loopy *LoopComm
}

//msgp:ignore LoopComm

// LoopComm is a handle to the underlying stream (TCP/QUIC)
// communication loops in rpc25519. It helps the system to tell
// the user of a Circuit when they can safely garbage collect
// their own Circuit monitoring goroutines by closing
// the Circuit.Halt.ReqStop.Chan when the TCP/QUIC stream
// socket goes down.
//
// Thus far LoopComm can remain opaque to the user, who
// just monitors the returned Circuit.Halt.ReqStop for
// the events that LoopComm facilitates under the API.
type LoopComm struct {
	sendCh chan *Message // talks to a send loop (on Client or a rwPair on Server)

	cktServedAdd chan *Circuit // talks to a read loop
	cktServedDel chan *Circuit // talks to a read loop
}

func (ckt *Circuit) String() string {
	return fmt.Sprintf(`&Circuit{
     CircuitSN: %v,
          Name: "%v",
     CircuitID: "%v",

   LocalPeerID: "%v" %v,
 LocalPeerName: "%v",

  RemotePeerID: "%v" %v,
RemotePeerName: "%v",

 LocalServiceName: "%v",
RemoteServiceName: "%v",

 LocalPeerServiceNameVersion: "%v",
RemotePeerServiceNameVersion: "%v",

     PreferExtant: %v,
   MadeNewAutoCli: %v,

 // LocalCircuitURL: "%v",
 // RemoteCircuitURL: "%v",

   UserString: "%v",
    FirstFrag: %v
}`, ckt.CircuitSN, ckt.Name,
		AliasDecode(ckt.CircuitID),
		ckt.LocalPeerID, AliasDecode(ckt.LocalPeerID),
		ckt.LocalPeerName,
		ckt.RemotePeerID, AliasDecode(ckt.RemotePeerID),
		ckt.RemotePeerName,
		ckt.LocalServiceName,
		ckt.RemoteServiceName,
		ckt.LocalPeerServiceNameVersion,
		ckt.RemotePeerServiceNameVersion,
		ckt.PreferExtant,
		ckt.MadeNewAutoCli,
		ckt.LocalCircuitURL(),
		ckt.RemoteCircuitURL(),
		ckt.UserString,
		ckt.FirstFrag,
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
		// ckt.LocalPeerServiceNameVersion + "/" +
		ckt.LocalPeerID + "/" +
		ckt.CircuitID
}

func (ckt *Circuit) RemoteCircuitURL() string {
	return ckt.RpbTo.NetAddr + "/" +
		ckt.RemoteServiceName + "/" +
		// ckt.RemotePeerServiceNameVersion + "/" +
		ckt.RemotePeerID + "/" +
		ckt.CircuitID
}

// leave off the circuitID -- the last part -- compared to the above.
func (ckt *Circuit) RemotePeerURL() string {
	return ckt.RpbTo.NetAddr + "/" +
		ckt.RemoteServiceName + "/" +
		// ckt.RemotePeerServiceNameVersion + "/" +
		ckt.RemotePeerID
}

func (ckt *Circuit) LocalBaseServerName() string {
	return ckt.LpbFrom.BaseServerName
}

func (ckt *Circuit) RemoteBaseServerName() string {
	return ckt.RpbTo.BaseServerName
}

func (ckt *Circuit) LocalBaseServerAddr() string {
	return ckt.LpbFrom.BaseServerAddr
}

func (ckt *Circuit) RemoteBaseServerAddr() string {
	return ckt.RpbTo.BaseServerAddr
}

// RemoteBaseServerNameURL is like RemoteCircuitURL,
// but uses the BaseServerName instead of
// any auto-cli name for the host name. It tries
// to keep the port if that is a part of RpbTo.NetAddr.
func (ckt *Circuit) RemoteBaseServerNameURL() string {

	host := ckt.RpbTo.BaseServerName
	u, err := url.Parse(ckt.RpbTo.NetAddr)
	panicOn(err)
	port := u.Port()
	if port != "" {
		host = net.JoinHostPort(host, port)
	}
	return u.Scheme + "://" + host + "/" +
		ckt.RemoteServiceName + "/" +
		// ckt.RemotePeerServiceNameVersion + "/" +
		ckt.RemotePeerID + "/" +
		ckt.CircuitID
}

func (ckt *Circuit) RemoteServerURL(serverNetAddr string) string {

	if serverNetAddr == "" {
		serverNetAddr = ckt.RpbTo.BaseServerAddr
	}
	return serverNetAddr + "/" +
		ckt.RemoteServiceName + "/" +
		// ckt.RemotePeerServiceNameVersion + "/" +
		ckt.RemotePeerID + "/" +
		ckt.CircuitID
}

func (ckt *Circuit) RemoteBaseServerNoCktURL(serverNetAddr string) string {

	if serverNetAddr == "" {
		serverNetAddr = ckt.RpbTo.BaseServerAddr
	}
	return serverNetAddr + "/" +
		ckt.RemoteServiceName + "/" +
		// ckt.RemotePeerServiceNameVersion + "/" +
		ckt.RemotePeerID
}

func (ckt *Circuit) IsClosed() bool {
	return ckt.Halt.ReqStop.IsClosed()
}

// ID2 supplies the local and remote PeerIDs.
func (ckt *Circuit) ID2() (LocalPeerID, RemotePeerID string) {
	return ckt.LpbFrom.PeerID, ckt.RpbTo.PeerID
}

func NewFragment() (frag *Fragment) {
	frag = &Fragment{
		Serial: issueSerial(),
	}
	return
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
	FromPeerID          string `zid:"0"` // who sent us this Fragment.
	FromPeerName        string `zid:"1"`
	FromPeerServiceName string `zid:"2"` // from registered PeerServiceName.

	FromPeerServiceNameVersion string `zid:"3"`

	ToPeerID          string `zid:"4"`
	ToPeerName        string `zid:"5"`
	ToPeerServiceName string `zid:"6"` // was ServiceName.

	ToPeerServiceNameVersion string `zid:"7"`

	CircuitID string   `zid:"8"` // maps to Message.HDR.CallID.
	Serial    int64    `zid:"9"`
	Typ       CallType `zid:"10"` // one of the CallPeer CallTypes of hdr.go

	// user supplied data
	FragOp      int    `zid:"11"`
	FragSubject string `zid:"12"`
	FragPart    int64  `zid:"13"`

	// Args whose keys start with '#' are reserved for the system.
	// Use frag.SetUserArg() to set Args safely. This will auto-
	// allocate the map if need be; for efficiency it is nil
	// by default as it may not always be in use.
	Args    map[string]string `zid:"14"`
	Payload []byte            `zid:"15"`
	Err     string            `zid:"16"` // distinguished field for error messages.
	Created time.Time         `zid:"17"` // from Message.HDR.Created
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
		panic(fmt.Sprintf("Fragment.SetUserArg error: "+
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
    "Created": %v,
    "FromPeerID": %q %v,
    "FromPeerName": "%v",
    "FromPeerServiceName": "%v",
    "ToPeerID": %q %v,
    "ToPeerName": "%v",
    "ToPeerServiceName": "%v",
    "CircuitID": %q,
    "Serial": %v,
    "Typ": %s,
    "FragOp": %v,
    "FragSubject": %q,
    "FragPart": %v,
    "Args": %#v,
    "Payload": %v,
    "Err": %q,
}`,
		f.Created.Format(rfc3339MsecTz0),
		f.FromPeerID, AliasDecode(f.FromPeerID),
		f.FromPeerName,
		f.FromPeerServiceName,
		f.ToPeerID, AliasDecode(f.ToPeerID),
		f.ToPeerName,
		f.ToPeerServiceName,
		AliasDecode(f.CircuitID),
		f.Serial,
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
	LocalPeer                    *LocalPeer
	PeerID                       string
	PeerName                     string // map to ckt.RemotePeerName
	NetAddr                      string
	RemoteServiceName            string
	RemotePeerServiceNameVersion string
	PeerURL                      string
	BaseServerName               string   // for auto-cli, what is base server?
	BaseServerAddr               string   // for auto-cli, what is base server addr?
	IncomingCkt                  *Circuit // first one to arrive
}

// LocalPeer in the backing behind each local instantiation of a PeerServiceFunc.
// local peers do reads on ch, get notified of new connections on newCircuitChan.
// and create new outgoing connections with
//
//msgp:ignore LocalPeer
type LocalPeer struct {
	Halt                   *idem.Halter
	NetAddr                string
	PeerServiceName        string
	PeerServiceNameVersion string
	BaseServerName         string // for auto-cli, what is base server?
	// when servers create auto-cli, what was the
	// corresponding base server address.
	BaseServerAddr string

	PeerAPI      *peerAPI
	Ctx          context.Context
	Canc         context.CancelCauseFunc
	PeerID       string
	PeerName     string
	U            UniversalCliSrv
	NewCircuitCh chan *Circuit
	ReadsIn      chan *Message
	ErrorsIn     chan *Message

	Remotes            *Mutexmap[string, *RemotePeer]
	TellPumpNewCircuit chan *Circuit
	HandleCircuitClose chan *Circuit
	QueryCh            chan *QueryLocalPeerPump

	// should we shut ourselves down when no more peers?
	AutoShutdownWhenNoMorePeers    bool
	AutoShutdownWhenNoMoreCircuits bool

	// put this in b/c the pump and the peer service
	// func were racing on recycled new frag. might have
	// been solved since then. is safe to leave in, but
	// might be perf optimization to see if can do without now.
	peerLocalFragMut sync.Mutex
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
		// s.PeerServiceNameVersion + "/" +
		s.PeerID
}

func (s *LocalPeer) Close() {
	s.Canc(fmt.Errorf("LocalPeer.Close() called. stack='%v'", stack()))
	s.Halt.ReqStop.Close()
	//<-s.Halt.Done.Chan // hung here so often. just seems a bad idea.
}

func (s *LocalPeer) CloseWithReason(why error) {
	s.Canc(fmt.Errorf("LocalPeer.CloseWithReason('%v') called. stack='%v'", why, stack()))
	s.Halt.ReqStop.CloseWithReason(why)
	//<-s.Halt.Done.Chan // hung here so often. just seems a bad idea.
}

func (s *LocalPeer) NewFragment() (f *Fragment) {
	s.peerLocalFragMut.Lock()
	defer s.peerLocalFragMut.Unlock()
	f = s.U.newFragment()
	f.FromPeerName = s.PeerName
	return
}

func (s *LocalPeer) FreeFragment(f *Fragment) {
	s.peerLocalFragMut.Lock()
	defer s.peerLocalFragMut.Unlock()
	s.U.freeFragment(f)
}

// NewCircuitToPeerURL sets up a persistent communication path called a Circuit.
// The frag can be nil, or set to send it immediately.
// Note: this does not automatically send the new ckt onto the channel NewCircuitCh
// since it might be called inside the PeerServiceFunc. If called
// outside of that goroutine, you'll need to manually do NewCircuitCh <- ckt
// to tell the PeerServiceFunc goroutine about it (it will get it on
// its newCircuitCh channel).
func (s *LocalPeer) NewCircuitToPeerURL(
	circuitName string,
	peerURL string,
	frag *Fragment,
	errWriteDur time.Duration,

) (ckt *Circuit, ctx context.Context, madeNewAutoCli bool, err error) {

	if s.Halt.ReqStop.IsClosed() {
		return nil, nil, false, ErrHaltRequested
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
		return nil, nil, false, fmt.Errorf("NewCircuitToPeerURL could not "+
			"parse peerURL: '%v': '%v'", peerURL, err)
	}

	if frag == nil {
		frag = s.NewFragment()
	}
	frag.FromPeerID = s.PeerID
	frag.FromPeerName = s.PeerName
	frag.ToPeerID = peerID
	//frag.ToPeerName = ?unknown?maybe

	// circuitID will be empty, want to create a new CallID.
	// allow joining a extant circuit? let's not for now.
	// Its just much simpler to start with.
	circuitID = NewCallID(serviceName)
	frag.CircuitID = circuitID
	frag.ToPeerServiceName = serviceName
	frag.FromPeerServiceName = s.PeerServiceName

	rpb := &RemotePeer{
		LocalPeer: s,
		PeerID:    peerID,
		//PeerName:        // unknown?
		NetAddr:           netAddr,
		RemoteServiceName: serviceName,
		//RemotePeerServiceNameVersion: ?
		//BaseServerName:   what on remote? s.BaseServerName,
	}
	//vv("rpb = '%#v'", rpb)

	preferExtant := false // can we know?
	ckt, ctx, madeNewAutoCli, err = s.newCircuit(circuitName, rpb, circuitID, frag, errWriteDur, true, onOriginLocalSide, preferExtant)
	if err != nil {
		return nil, nil, madeNewAutoCli, err
	}
	rpb.IncomingCkt = ckt
	s.Remotes.Set(peerID, rpb) // arg. _was_ only called this once. need to symmetrically set on the remote side too. addedckt.go:808 for that.
	return
}

func ParsePeerURL(peerURL string) (netAddr, serviceName, peerID, circuitID string, err error) {
	//vv("ParsePeerURL(peerURL = '%v') top.", peerURL)
	var u *url.URL
	u, err = url.Parse(peerURL)
	if err != nil {
		errs := err.Error()
		if strings.Contains(errs,
			"first path segment in URL cannot contain colon") &&
			!strings.Contains(peerURL, "://") {

			// it is common to hit this when the host:port
			// was given in the config but without a scheme prefix.
			//vv("guess that we meant tcp, so guess '%v' -> '%v'", peerURL, "tcp://"+peerURL)
			//if faketime { // wait until we actually need it.
			//	peerURL = "tcp://" + peerURL
			//} else {
			peerURL = "tcp://" + peerURL
			//}
			u, err = url.Parse(peerURL)
			if err != nil {
				// still no luck. surface the error.
				return
			}
		} else {
			return
		}
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

func (ckt *Circuit) SendOneWay(frag *Fragment, errWriteDur time.Duration, keepFragIfPositive int) (madeNewAutoCli bool, err error) {
	return ckt.LpbFrom.SendOneWay(ckt, frag, errWriteDur, keepFragIfPositive)
}

// SendOneWay sends a Frament on the given Circuit.
// We check for cancelled ckt and LocalPeer and return an error
// rather than send if they are shutting down.
// keepFragIfPositive > 0 means we will not recycle
// this fragment. Applications can set this to send
// the same fragment to many destinations.
func (s *LocalPeer) SendOneWay(ckt *Circuit, frag *Fragment, errWriteDur time.Duration, keepFragIfPositive int) (madeNewAutoCli bool, err error) {

	// regular quick check if shutdown is requested.
	select {
	case <-s.Halt.ReqStop.Chan:
		err = ErrHaltRequested
		return
	case <-ckt.Context.Done():
		err = ErrContextCancelled
		return
	case <-s.Ctx.Done():
		err = ErrContextCancelled
		return
	default:
	}

	if frag == nil {
		err = fmt.Errorf("frag cannot be nil")
		return
	}
	frag.CircuitID = ckt.CircuitID

	frag.FromPeerID = ckt.LocalPeerID
	if frag.FromPeerName == "" {
		frag.FromPeerName = ckt.LocalPeerName
	}
	frag.ToPeerID = ckt.RemotePeerID
	if frag.ToPeerName == "" {
		frag.ToPeerName = ckt.RemotePeerName
	}
	if frag.ToPeerServiceName == "" {
		frag.ToPeerServiceName = ckt.RemoteServiceName
	}
	if frag.ToPeerServiceNameVersion == "" {
		frag.ToPeerServiceNameVersion = ckt.RemotePeerServiceNameVersion
	}
	if frag.FromPeerServiceName == "" {
		frag.FromPeerServiceName = ckt.LocalServiceName
	}
	if frag.FromPeerServiceNameVersion == "" {
		frag.FromPeerServiceNameVersion = ckt.LocalPeerServiceNameVersion
	}

	//vv("sending frag='%v' to (if To empty, send to:) ckt.RpbTo.NetAddr='%v'", frag, ckt.RpbTo.NetAddr)
	msg := ckt.ConvertFragmentToMessage(frag)
	if keepFragIfPositive <= 0 {
		s.FreeFragment(frag)
	} else {
		// else user plans to re-use the frag on the next message.
		// The only prob with re-use is that the Args
		// map is a pointer internally. When the simnet
		// tries to copy that map, we have a problem (data race).
		// We fixed this by forcing the ConvertFragmentToMessage(frag)
		// just above to copy the frag.Args map rather than
		// share it between the origin frag and the new Message.
	}

	madeNewAutoCli, _, err = s.U.SendOneWayMessage(s.Ctx, msg, errWriteDur)
	if err != nil {
		return
	}

	if errWriteDur >= 0 {
		// wait for sendloop to get message into OS buffers.
		select {
		case <-msg.DoneCh.WhenClosed():
			err = msg.LocalErr
			return
		case <-s.Halt.ReqStop.Chan:
			err = ErrHaltRequested
			return
		case <-ckt.Context.Done():
			err = ErrContextCancelled
			return
		case <-s.Ctx.Done():
			err = ErrContextCancelled
			return
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
	peerName string,
	baseServerAddr string,

) (pb *LocalPeer) {

	//vv("newLocalPeer called with baseServerAddr='%v'; peerAPI.baseServerAddr = '%v'", baseServerAddr, peerAPI.baseServerAddr)
	if baseServerAddr == "" {
		baseServerAddr = peerAPI.baseServerAddr
		if baseServerAddr == "" {
			// fill for servers too... did not seem to work though.
			baseServerAddr = netAddr
		}
	}
	pb = &LocalPeer{
		NetAddr:         netAddr,
		PeerServiceName: peerServiceName,
		PeerAPI:         peerAPI,
		Ctx:             ctx,
		Canc:            cancelFunc,
		PeerID:          peerID,
		PeerName:        peerName,
		BaseServerName:  peerAPI.baseServerName,
		BaseServerAddr:  baseServerAddr,
		U:               u,
		NewCircuitCh:    newCircuitCh,
		ReadsIn:         make(chan *Message, 1),
		ErrorsIn:        make(chan *Message, 1),

		Remotes:            NewMutexmap[string, *RemotePeer](),
		TellPumpNewCircuit: make(chan *Circuit),
		HandleCircuitClose: make(chan *Circuit),
		QueryCh:            make(chan *QueryLocalPeerPump),
	}
	pb.Halt = idem.NewHalterNamed(fmt.Sprintf("LocalPeer(%v %p)", peerServiceName, pb))

	//AliasRegister(peerID, peerID+" ("+peerServiceName+")")

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
	return convertMessageToFragment(msg)
}

func convertMessageToFragment(msg *Message) (frag *Fragment) {
	frag = &Fragment{
		Created:                    msg.HDR.Created,
		FromPeerID:                 msg.HDR.FromPeerID,
		FromPeerName:               msg.HDR.FromPeerName,
		FromPeerServiceName:        msg.HDR.FromServiceName,
		FromPeerServiceNameVersion: msg.HDR.FromPeerServiceNameVersion,
		ToPeerID:                   msg.HDR.ToPeerID,
		ToPeerName:                 msg.HDR.ToPeerName,
		ToPeerServiceName:          msg.HDR.ToServiceName,
		ToPeerServiceNameVersion:   msg.HDR.ToPeerServiceNameVersion,
		CircuitID:                  msg.HDR.CallID,
		Serial:                     msg.HDR.Serial,
		Typ:                        msg.HDR.Typ,

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
	msg.HDR.FromPeerName = frag.FromPeerName
	msg.HDR.FromServiceName = frag.FromPeerServiceName
	msg.HDR.FromPeerServiceNameVersion = frag.FromPeerServiceNameVersion

	msg.HDR.ToPeerID = frag.ToPeerID
	msg.HDR.ToPeerName = frag.ToPeerName
	msg.HDR.ToServiceName = frag.ToPeerServiceName
	msg.HDR.ToPeerServiceNameVersion = frag.ToPeerServiceNameVersion

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

	msg.HDR.FragOp = frag.FragOp
	msg.HDR.Subject = frag.FragSubject
	msg.HDR.StreamPart = frag.FragPart

	if frag.Args != nil {
		// arg! about Args(hah!) drat:
		// a straight map re-use leads
		// to a data race on simnet, when
		// the tube.go code tries to broadcast
		// the same fragment to many peers. Instead of
		//msg.HDR.Args = frag.Args
		// make a copy:
		msg.HDR.Args = copyArgsMap(frag.Args)
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
	if msg.HDR.ToPeerName == "" {
		msg.HDR.ToPeerName = ckt.RemotePeerName
	}
	if msg.HDR.ToServiceName == "" { // was HDR.ServiceName
		msg.HDR.ToServiceName = ckt.RemoteServiceName
	}
	if msg.HDR.ToPeerServiceNameVersion == "" {
		msg.HDR.ToPeerServiceNameVersion = ckt.RemotePeerServiceNameVersion
	}
	if msg.HDR.FromPeerID == "" {
		msg.HDR.FromPeerID = ckt.LocalPeerID
	}
	if msg.HDR.FromPeerName == "" {
		msg.HDR.FromPeerName = ckt.LocalPeerName
	}
	if msg.HDR.FromServiceName == "" {
		msg.HDR.FromServiceName = ckt.LocalServiceName
	}
	if msg.HDR.FromPeerServiceNameVersion == "" {
		msg.HDR.FromPeerServiceNameVersion = ckt.LocalPeerServiceNameVersion
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
	if msg.HDR.Args == nil {
		msg.HDR.Args = make(map[string]string)
	}
	msg.HDR.Args["#fromServiceName"] = ckt.LocalServiceName
	msg.HDR.Args["#fromPeerServiceNameVersion"] = ckt.LocalPeerServiceNameVersion
	msg.HDR.Args["#fromBaseServerName"] = ckt.LpbFrom.BaseServerName
	msg.HDR.Args["#fromBaseServerAddr"] = ckt.LpbFrom.BaseServerAddr

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
func (origCkt *Circuit) NewCircuit(circuitName string, firstFrag *Fragment) (ckt *Circuit, ctx2 context.Context, madeNewAutoCli bool, err error) {
	return origCkt.RpbTo.LocalPeer.newCircuit(circuitName, origCkt.RpbTo, "", firstFrag, -1, true, onOriginLocalSide, false)
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
// to enaquire about the number of open Circuits.
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

// flag values for onRemoteSide argument to newCircuit().
type onRemoteSideVal bool

const (
	onRemote2ndSide   onRemoteSideVal = true
	onOriginLocalSide onRemoteSideVal = false
)

// newCircuit always adds a circuit to the peer on the side it
// is called on. It does this with lpb.TellPumpNewCircuit <- ckt;
//
// Does tellRemote==false guarantee we are on the remote side?
// (tellRemote==true means we always tell the other side
// about the new circuit, but does the converse hold?)
// No, (a) below gives an example that violates that conjecture.
// So therefore we must add a flag: onRemoteSide to distinguish.
//
// (this was line ~ 795 when the line numbers below were written).
//
// newCircuit is called by:
// a) ckt2.go:77 StartRemotePeerAndGetCircuit (tellRemote=false, atMost=false)
// -- here newCircuit is used to add the ckt to the local peer.
// ---- and just previously it sent CallPeerStartCircuitTakeToID to the remote
// ---- which means goto (d) below on the remote side.
//
// b) NewCircuitToPeerURL calls newCircuit(tellRemote=true) at ckt.go:447
// ---- it sets atMostOne true if the PeerID in the URL is blank.
// ---- which means goto (d) on the remote
//
// c) NewCircuit calls newCircuit(tellRemote=true) at ckt.go:745
// ---- atMostOne is always false in the newCircuit() call.
// ---- which means goto (d) on the remote
//
// d) readLoops on cli and srv call
// bootstrapCircuit ckt.go:1331 (:1258)
// -- which calls provideRemoteOnNewCircuitCh :1555 at ckt.go:1550 (:1379)
// ----- which calls newCircuit(tellRemote=false) at ckt.go:1573 (:)
//
// when we, newCircuit(), are invoked with tellRemote=true
// from b) or c), we send to the remote either
// -- CallPeerStartCircuit; or
// -- CallPeerStartCircuitAtMostOne if atMostOnePeer (CallPeerStartCircuitAtMostOne was sent from (b) above).
// .
func (lpb *LocalPeer) newCircuit(
	circuitName string,
	rpb *RemotePeer,
	cID string,
	firstFrag *Fragment,
	errWriteDur time.Duration,
	tellRemote bool, // send new circuit to remote?
	isRemoteSide onRemoteSideVal,
	preferExtant bool,

) (ckt *Circuit, ctx2 context.Context, madeNewAutoCli bool, err error) {

	//vv("newCircuit() called. isRemoteSide = %v", isRemoteSide) //, stack())

	if lpb.Halt.ReqStop.IsClosed() {
		return nil, nil, false, ErrHaltRequested
	}

	var canc2 context.CancelCauseFunc
	ctx2, canc2 = context.WithCancelCause(lpb.Ctx)
	reads := make(chan *Fragment, 100)
	errors := make(chan *Fragment, 100)
	ckt = &Circuit{
		CircuitSN:                    atomic.AddInt64(&nextCircuitSN, 1),
		Name:                         circuitName,
		LocalServiceName:             lpb.PeerServiceName,
		LocalPeerServiceNameVersion:  lpb.PeerServiceNameVersion,
		RemoteServiceName:            rpb.RemoteServiceName,
		RemotePeerServiceNameVersion: rpb.RemotePeerServiceNameVersion,
		LpbFrom:                      lpb,
		RpbTo:                        rpb,
		CircuitID:                    cID,
		LocalPeerID:                  lpb.PeerID,
		LocalPeerName:                lpb.PeerName,
		RemotePeerID:                 rpb.PeerID,
		RemotePeerName:               rpb.PeerName,
		Reads:                        reads,
		Errors:                       errors,
		Context:                      ctx2,
		Canc:                         canc2,
		FirstFrag:                    firstFrag,
		PreferExtant:                 preferExtant,
	}
	ckt.Halt = idem.NewHalterNamed(fmt.Sprintf("Circuit(%v %p)", circuitName, ckt))
	if ckt.CircuitID == "" {
		ckt.CircuitID = NewCallID(circuitName)
	}

	//if ckt.CircuitSN == 7 {
	//panic("where?")
	//}
	//AliasRegister(ckt.CircuitID, ckt.CircuitID+" ("+circuitName+")")

	//lpb.Halt.AddChild(ckt.Halt) // no worries: pump will do this.

	//vv("newCircuit is telling pump about ckt=%p", ckt) // , ckt.Errors) //, stack())
	select { // blocked here
	case lpb.TellPumpNewCircuit <- ckt:

	case <-lpb.Halt.ReqStop.Chan:
		return nil, nil, false, ErrHaltRequested
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
		msg.HDR.FromPeerName = lpb.PeerName
		msg.HDR.FromServiceName = lpb.PeerServiceName
		msg.HDR.ToPeerID = rpb.PeerID
		msg.HDR.ToPeerName = rpb.PeerName
		msg.HDR.ToServiceName = rpb.RemoteServiceName
		msg.HDR.ToPeerServiceNameVersion = rpb.RemotePeerServiceNameVersion

		msg.HDR.CallID = ckt.CircuitID
		msg.HDR.Serial = issueSerial()

		// tell the remote which serviceName we are coming from;
		// so the URL back can be correct.
		// Don't make a new HDR.Args map here since the firstFrag.Args
		// may be carrying important information and a new
		// map would lose that; like #fragRPCtoken and #UserString
		msg.HDR.Args["#fromServiceName"] = lpb.PeerServiceName
		msg.HDR.Args["#toServiceName"] = rpb.RemoteServiceName
		msg.HDR.Args["#toPeerServiceNameVersion"] = rpb.RemotePeerServiceNameVersion
		msg.HDR.Args["#circuitName"] = circuitName
		msg.HDR.Args["#fromBaseServerName"] = lpb.BaseServerName
		msg.HDR.Args["#fromBaseServerAddr"] = lpb.BaseServerAddr

		madeNewAutoCli, ckt.loopy, err = lpb.U.SendOneWayMessage(ctx2, msg, errWriteDur)
		ckt.MadeNewAutoCli = madeNewAutoCli
		if err != nil {
			alwaysPrintf("arg: tried to tell remote, but: err='%v'", err)
		}
	} else {
		// !tellRemote
		// update "the remote's remote" list: symmetric to ckt.go:406

		// also, this seems to be needed... makes 401 membership_test green.
		// Just add a check in case something else was already there;
		// assert nothing already there:
		if rpb.IncomingCkt != nil && ckt != rpb.IncomingCkt {
			panic(fmt.Sprintf("hmm... are we blowing away a circuit setting on IncomingCkt that we need??? rpb.IncomingCkt = '%v'; new ckt = '%v'", rpb.IncomingCkt, ckt))
		}
		rpb.IncomingCkt = ckt

		// rpb.PeerID can be "", the empty string.
		// We figure it is better to save
		// it at least under empty string for now, and try to
		// fix later. although of course collisions and
		// overwrites then become possible.
		lpb.Remotes.Set(rpb.PeerID, rpb)
	}

	if isRemoteSide {
		//vv("onRemoteSide, sending CallPeerCircuitEstablishedAck; lpb='%#v'", lpb)

		// complete a round trip for ckt establishment by
		// sending back CallPeerCircuitEstablishedAck here,
		// for clients who want to wait for an ack back
		// that a circuit has been established. The cannonical
		// use is to contact an extant peer whose PeerID they
		// don't know yet; but they know it is up and running.

		// That wait is off the critical path, typically, since
		// most peers won't do what ckt2 does (when waitForAck true)
		// and establish a fragRPCtoken to wait for, and then wait for it.

		// note: we expect tellRemote to (always) be false, and
		// even when it is false, onRemoteSide will not
		// always be true, as ckt2.go shows. So, this is why
		// onRemoteSide and tellRemote cannot be the same flag.

		var msg *Message
		if firstFrag != nil {
			msg = firstFrag.ToMessage()
		} else {
			msg = NewMessage()
		}
		msg.HDR.To = rpb.NetAddr
		msg.HDR.From = lpb.NetAddr
		msg.HDR.Typ = CallPeerCircuitEstablishedAck
		msg.HDR.Created = time.Now()
		msg.HDR.FromPeerID = lpb.PeerID
		msg.HDR.FromPeerName = lpb.PeerName
		msg.HDR.FromServiceName = lpb.PeerServiceName
		msg.HDR.ToPeerID = rpb.PeerID
		msg.HDR.ToPeerName = rpb.PeerName
		msg.HDR.ToServiceName = rpb.RemoteServiceName
		msg.HDR.ToPeerServiceNameVersion = rpb.RemotePeerServiceNameVersion
		msg.HDR.CallID = ckt.CircuitID
		msg.HDR.Serial = issueSerial()
		msg.HDR.Args["#fromServiceName"] = lpb.PeerServiceName
		msg.HDR.Args["#fromBaseServerName"] = lpb.BaseServerName
		msg.HDR.Args["#fromBaseServerAddr"] = lpb.BaseServerAddr
		msg.HDR.Args["#toServiceName"] = rpb.RemoteServiceName
		msg.HDR.Args["#toPeerServiceNameVersion"] = rpb.RemotePeerServiceNameVersion
		msg.HDR.Args["#circuitName"] = circuitName
		// No need to explicity set the
		// msg.HDR.Args["#fragRPCtoken"] as it was
		// copied above in firstFrag.ToMessage()

		madeNewAutoCli, ckt.loopy, err = lpb.U.SendOneWayMessage(ctx2, msg, -1)
		ckt.MadeNewAutoCli = madeNewAutoCli
	}
	if err != nil && ckt != nil {
		if ckt.loopy != nil {
			select {
			case ckt.loopy.cktServedAdd <- ckt:
				// this communication finally stopped the
				// leak of server side circuit support goro.
				//vv("in lbp.newCircuit: ckt.loopy available and ckt.loopy.cktServedAdd <- ckt ok.")
			case <-lpb.Halt.ReqStop.Chan:
			}
		} else {
			// assert that our development set ckt.loopy if it could
			if ckt.RpbTo != nil {
				panicf("why do we not have ckt.loopy set now?? there might be a good reason, it might need to be lazy but we want to know what that is... as we want to have it set if at all possible, in all Circuit creation scenarios!! ckt.RpbTo.NetAddr='%v' so just set ckt.loopy, _ = s.remote2pair.Get(ckt.RpbTo.NetAddr)", ckt.RpbTo.NetAddr)
			} else {
				panicf("ahem: is development of LoopComm helper incomplete? why do we not have ckt.loopy set now?? there might be a good reason, but we want to have it set if at all possible, in all Circuit creation scenarios!")
			}
		}
	}
	return
}

// Close must be called on a Circuit to release resources
// when you are done with it. use reason nil for normal
// all-good shutdown of a circuit, or to report an error.
func (h *Circuit) Close(reason error) {
	// must be idemopotent. Often called many times
	// during normal Circuit shutdown.

	if h.loopy != nil {
		select {
		case h.loopy.cktServedDel <- h:
		case <-h.LpbFrom.Halt.ReqStop.Chan:
		}
	}

	// set reason atomically.
	h.Halt.ReqStop.CloseWithReason(reason)

	select { // stalled here on shutdown, 401 membr test in tube.
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
	u UniversalCliSrv

	// where is mut used: StartLocalPeer holds it...
	// all while doing its thing. Hence need fragMut separately.
	mut sync.Mutex

	// lock over the recycled frag to prevent data race.
	fragMut      sync.Mutex
	recycledFrag []*Fragment

	// peerServiceName key
	localServiceNameMap *Mutexmap[string, *knownLocalPeer]

	isSim bool // using SimNet instead of actual network calls

	// just for logging. do not depend on this because
	// it might not be true in a cluster/grid.
	// e.g. the Server will start auto-clients for
	// each new connection, but the frag will
	// still get sent to a server peer.
	isCli bool

	baseServerName string
	baseServerAddr string
}

func newPeerAPI(u UniversalCliSrv, isCli, isSim bool, baseServerName, baseServerAddr string) *peerAPI {
	return &peerAPI{
		u:                   u,
		localServiceNameMap: NewMutexmap[string, *knownLocalPeer](),
		isCli:               isCli,
		isSim:               isSim,
		baseServerName:      baseServerName,
		baseServerAddr:      baseServerAddr,
	}
}

func (s *peerAPI) newFragment() (f *Fragment) {
	s.fragMut.Lock()

	if len(s.recycledFrag) == 0 {
		f = NewFragment()
		s.fragMut.Unlock()
		return
	} else {
		f = s.recycledFrag[0]
		s.recycledFrag = s.recycledFrag[1:]
		f.Serial = issueSerial()
		s.fragMut.Unlock()
		return
	}
}

func (s *peerAPI) freeFragment(frag *Fragment) {
	s.fragMut.Lock()
	defer s.fragMut.Unlock()
	*frag = Fragment{}
	s.recycledFrag = append(s.recycledFrag, frag)
}

func (s *peerAPI) recycleFragLen() int {
	s.fragMut.Lock()
	defer s.fragMut.Unlock()
	return len(s.recycledFrag)
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
	peerServiceNameVersion string,
	requestedCircuit *Message,
	peerName string,
	preferExtant bool,

) (lpb *LocalPeer, err error) {

	p.mut.Lock()
	defer p.mut.Unlock()

	return p.unlockedStartLocalPeer(ctx, peerServiceName, peerServiceNameVersion, requestedCircuit, false, nil, "", peerName, onOriginLocalSide, preferExtant)
}

// PRE: p.mut must be held by caller (and released when
// we return), where p = peerAPI; ourselves. This
// pre-condition insures that we don't go over the
// copy limit for this peerServiceName when
// limits are in force -- which enables rendezvous
// without talking to say two different instances
// by mistake.
//
// unlockedStartLocalPeer is
// called by StartLocalPeer ckt.go:1185 just above. onRemote=false/onOriginLocalSide
// called by bootstrapCircuit ckt.go:1581 below. onRemote=true/onRemote2ndSide
func (p *peerAPI) unlockedStartLocalPeer(
	ctx context.Context,
	peerServiceName string,
	peerServiceNameVersion string,
	requestedCircuit *Message,
	isUpdatedPeerID bool,
	sendCh *LoopComm,
	pleaseAssignNewPeerID string,
	peerName string,
	isRemoteSide onRemoteSideVal,
	preferExtant bool,

) (lpb *LocalPeer, err error) {
	knownLocalPeer, ok := p.localServiceNameMap.Get(peerServiceName)
	if !ok {
		return nil, fmt.Errorf("no local peerServiceName '%v' available", peerServiceName)
	}

	// enforce cfg.ServiceLimit
	cfg := p.u.GetConfig()
	lim := cfg.GetLimitMax(peerServiceName)
	if lim > 0 {
		knownLocalPeer.mut.Lock()
		if knownLocalPeer.active != nil {
			ncur := knownLocalPeer.active.Len()
			if ncur >= lim {
				// at limit, reject making another
				knownLocalPeer.mut.Unlock()
				return nil, fmt.Errorf("unlockedStartLocalPeer error: peerServiceName '%v' is listed in cfg.LimitedServiceNames and is already at cfg.LimitedServiceMax = %v, (ncur=%v) refusing to make another. Method lpb.PeerAPI.GetLocalPeers(peerServiceName) will list them.", peerServiceName, lim, ncur)
			}
		}
		// we still hold p.mut so there cannot be a
		// logical race to start another of this name
		// at this Client/Server.
		knownLocalPeer.mut.Unlock()
	}

	// made this from 1 -> 100 bufferred for tube.
	newCircuitCh := make(chan *Circuit, 100) // must be buffered >= 1, see below.
	ctx1, canc1 := context.WithCancelCause(ctx)

	var localPeerID string
	if pleaseAssignNewPeerID == "" {
		localPeerID = NewCallID(peerServiceName)
	} else {
		localPeerID = pleaseAssignNewPeerID
	}
	//AliasRegister(localPeerID, localPeerID+" ("+peerServiceName+")")

	localAddr := p.u.LocalAddr()
	//vv("unlockedStartLocalPeer: localAddr = '%v'", localAddr)
	lpb = p.newLocalPeer(ctx1, canc1, p.u, localPeerID, newCircuitCh, peerServiceName, localAddr, peerName, cfg.BaseServerAddr)

	knownLocalPeer.mut.Lock()
	if knownLocalPeer.active == nil {
		knownLocalPeer.active = NewMutexmap[string, *LocalPeer]()
	}
	knownLocalPeer.active.Set(localPeerID, lpb)
	knownLocalPeer.mut.Unlock()

	go func() {
		//vv("launching new peerServiceFunc invocation for '%v'", peerServiceName)
		if p.isSim {
			cfg.GetSimnet().NewGoro(peerName + "_peerServiceFunc")
		}

		err := knownLocalPeer.peerServiceFunc(lpb, ctx1, newCircuitCh)

		//vv("peerServiceFunc has returned: '%v'; clean up the lbp!", peerServiceName)
		canc1(fmt.Errorf("peerServiceFunc '%v' finished. returned err = '%v'", peerServiceName, err))
		lpb.Close()
		// this handles locking on its own.
		knownLocalPeer.active.Del(localPeerID)

	}()

	//localPeerURL := lpb.URL()
	//vv("unlockedStartLocalPeer: lpb.URL() = '%v'; peerServiceName='%v', isUpdatedPeerID='%v'; pleaseAssignNewPeerID='%v'; \nstack=%v\n", lpb.URL(), peerServiceName, isUpdatedPeerID, pleaseAssignNewPeerID, stack())

	if requestedCircuit != nil {
		return lpb, lpb.provideRemoteOnNewCircuitCh(p.isCli, requestedCircuit, ctx1, sendCh, isUpdatedPeerID, isRemoteSide, preferExtant, nil, nil)
	}

	return lpb, nil
}

// retreive all local peer(s) under peerServiceName,
// without starting a new one.
func (p *peerAPI) GetLocalPeers(
	peerServiceName string,
) (lpbs []*LocalPeer) {

	knownLocalPeer, ok := p.localServiceNameMap.Get(peerServiceName)
	if !ok {
		return nil
	}
	knownLocalPeer.mut.Lock()
	if knownLocalPeer.active == nil {

		knownLocalPeer.mut.Unlock()
		return nil
	}
	lpbs = knownLocalPeer.active.GetValSlice()
	knownLocalPeer.mut.Unlock()
	return
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
func (p *peerAPI) StartRemotePeer(ctx context.Context, peerServiceName, peerServiceNameVersion, remoteAddr string, waitUpTo time.Duration, preferExtant bool) (remotePeerURL, RemotePeerID string, madeNewAutoCli bool, onlyPossibleAddr string, err error) {

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
			return "", "", false, r, fmt.Errorf("client peer error on StartRemotePeer: remoteAddr should be '%v' (that we are connected to), rather than the '%v' which was requested. Otherwise your request will fail.", r, remoteAddr)
		}
	}

	hdr := NewHDR(p.u.LocalAddr(), remoteAddr, peerServiceName, CallPeerStart, 0)
	hdr.ToPeerServiceNameVersion = peerServiceNameVersion
	//hdr.ToServiceName = peerServiceName
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
	hhalt := p.u.GetHostHalter()

	var madeNewAutoCli0 bool
	for i := 0; i < 50; i++ {
		madeNewAutoCli0, _, err = p.u.SendOneWayMessage(ctx, msg, 0)
		if madeNewAutoCli0 {
			madeNewAutoCli = true // latch and stick at true
		}
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
			ti := p.u.NewTimer(dur)
			if ti == nil {
				// simnet shutdown
				return "", "", madeNewAutoCli, "", ErrHaltRequested
			}
			select {
			case <-ti.C:
				ti.Discard()
			case <-hhalt.ReqStop.Chan:
				ti.Discard()
				return
			}
			continue
		}
	}

	//vv("isCli=%v, StartRemotePeer about to wait for reply on ch to callID = '%v'", p.isCli, callID)
	var reply *Message
	select {
	case reply = <-ch:
		//vv("got reply to CallPeerStart: '%v'", reply.String())
	case <-ctx.Done():
		//vv("ctx.Done() seen, cause: '%v'\n\n stack: '%v'", context.Cause(ctx), stack())
		return "", "", madeNewAutoCli, "", ErrContextCancelled
	case <-hhalt.ReqStop.Chan:
		return "", "", madeNewAutoCli, "", ErrHaltRequested
	}
	var ok bool
	RemotePeerID, ok = reply.HDR.Args["#peerID"]
	if !ok {
		return "", "", madeNewAutoCli, "", fmt.Errorf("remote '%v', peerServiceName '%v' did "+
			"not respond with peerID in Args", remoteAddr, peerServiceName)
	}
	////vv("got RemotePeerID from Args[peerID]: '%v'", RemotePeerID)
	remotePeerURL, ok = reply.HDR.Args["#peerURL"]
	if !ok {
		return "", "", madeNewAutoCli, "", fmt.Errorf("remote '%v', peerServiceName '%v' did "+
			"not respond with peerURL in Args", remoteAddr, peerServiceName)
	}
	//vv("StartRemotePeer got remotePeerURL from Args[peerURL]: '%v'", remotePeerURL)
	return remotePeerURL, RemotePeerID, madeNewAutoCli, "", nil
}

// bootstrapCircuit: handle CallPeerStartCircuit
//
// The Client/Server readLoops call us directly when they
// (who are our only callers) see one of:
// CallPeerStart
// CallPeerStartCircuit
// CallPeerStartCircuitTakeToID
// CallPeerStartCircuitAtMostOne
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
func (s *peerAPI) bootstrapCircuit(isCli bool, msg *Message, ctx context.Context, sendCh *LoopComm) (err0 error) {
	//vv("isCli=%v, bootstrapCircuit called with msg='%v'; JobSerz='%v'", isCli, msg.String(), string(msg.JobSerz))

	// defer func() {
	// 	if err0 != nil {
	// 		vv("arg: bootstrapCircuit returning err0='%v'; this will shutdown the conn", err0) // not seen 410. good.
	// 	}
	// }()

	// find the localPeerback corresponding to the ToPeerID
	localPeerID := msg.HDR.ToPeerID
	peerServiceName := msg.HDR.ToServiceName
	peerServiceNameVersion := msg.HDR.ToPeerServiceNameVersion

	// what are we protecting here? PreferExtant and
	// making singleton instances... Can we release lock
	// before calling into lpb.provideRemoteOnNewCircuitCh
	// where after lpb.NewCircuitCh <- ckt: the
	// case ckt.Reads <- asFrag: might be blocked away on the pump.
	s.mut.Lock() // wait 2 sec stacks show a wait here vs :1366 StartLocalPeer top
	var skipUnlock bool
	defer func() {
		if !skipUnlock {
			s.mut.Unlock()
		}
	}()

	knownLocalPeer, ok := s.localServiceNameMap.Get(peerServiceName)
	if !ok {
		//vv("could not find peerServiceName='%v'", peerServiceName)
		return s.rejectWith(fmt.Sprintf("no local peerServiceName '%v' available", peerServiceName), isCli, msg, ctx, sendCh)
	}
	//vv("good: bootstrapCircuit found registered peerServiceName: '%v'", peerServiceName)

	var curServiceCount int
	needNewLocalPeer := false
	var noPriorPeers bool
	var extantPeerIDs []string
	var extantPeerIDsURL0 string
	var lpbsClone map[string]*LocalPeer

	knownLocalPeer.mut.Lock()
	if knownLocalPeer.active == nil {
		noPriorPeers = true
		knownLocalPeer.active = NewMutexmap[string, *LocalPeer]()
		needNewLocalPeer = true
	} else {
		lpbsClone = knownLocalPeer.active.GetMapCloneAtomic()
		curServiceCount = len(lpbsClone)
		if curServiceCount > 0 {
			i := 0
			for k, v := range lpbsClone {
				extantPeerIDs = append(extantPeerIDs, k)
				if i == 0 {
					extantPeerIDsURL0 = v.URL() // give the user a full URL on error
				}
				i++
			}
		}
	}

	pleaseAssignNewRemotePeerID, assignReqOk := msg.HDR.Args["#pleaseAssignNewPeerID"]

	var lpb *LocalPeer
	isUpdatedPeerID := false
	preferExtant := false

	switch msg.HDR.Typ {
	case CallPeerStartCircuitTakeToID:
		if !assignReqOk {
			panic(fmt.Sprintf("internal consistency error: all CallPeerStartCircuitTakeToID messages must have the HDR.Args['#pleaseAssignNewPeerID'] set too."))
		}
		if msg.HDR.ToPeerID != pleaseAssignNewRemotePeerID {
			panic("inconsistent internal logic! should have msg.HDR.ToPeerID == pleaseAssignNewRemotePeerID")
		}
		pleaseAssignNewRemotePeerID = msg.HDR.ToPeerID
		assignReqOk = true
		needNewLocalPeer = true
		// INVAR: lpb set, or needNewLocalPeer true.
	case CallPeerStartCircuitAtMostOne:
		preferExtant = true
		if noPriorPeers {
			//vv("CallPeerStartCircuitAtMostOne handling: no prior peers, will start new")
			// cannot re-connect with existing, must make new peer.
			// needNewLocalPeer = true was set above.
		} else {
			//vv("CallPeerStartCircuitAtMostOne handling: try to find extant")
			// try to find an existing local peer

			//vv("we see msg.HDR.Typ == CallPeerStartCircuitAtMostOne with availLpb count = %v under peerServiceName '%v'", curServiceCount, peerServiceName) // seen 410
			switch curServiceCount {
			case 0:
				// we did hit this: probably we had one but then it died;
				// but we still had a map entry. don't freak out.
				needNewLocalPeer = true
			case 1:
				lpb = knownLocalPeer.active.GetValSlice()[0]
				if lpb == nil {
					panic("not allowed to store nil lpb in knownLocalPeer!")
				}
				needNewLocalPeer = false // just for emphasis
				//vv("good, one existant peerServiceName='%v' lpb=%p lpb.PeerName='%v'; lpb.PeerID='%v'; when CallPeerStartCircuitAtMostOne requested.", peerServiceName, lpb, lpb.PeerName, lpb.PeerID)
			default:
				panic(fmt.Sprintf("more than 1 started peer service(%v) '%v' so which one? we could a random one...but thats bad for determinism.", curServiceCount, peerServiceName))
			}
			// INVAR: lpb set, or needNewLocalPeer true.
		}
	}

	if lpb == nil && !needNewLocalPeer {
		// CallPeerStartCircuitAtMostOne above did not find existing peer.
		// We have not seen anything yet that forces us to make a new peer.

		if localPeerID == "" {
			// On its own, localPeerID == "" can also be seen when
			// CallPeerStartCircuitAtMostOne. Nonetheless, if peer re-use
			// is viable, we have already set lpb above based on
			// peerServiceName matching alone, so we won't get in here
			// (as lpb != nil && !needNewLocalPeer in that case).
			needNewLocalPeer = true

		} else {
			var ok bool
			lpb, ok = knownLocalPeer.active.Get(localPeerID)
			if !ok {
				// have to start a new instance
				isUpdatedPeerID = true
				needNewLocalPeer = true
			} else {
				// good, lpb should be set.
				if lpb == nil {
					panic("internal logic error: never store nil in knownLocalPeer.active map")
				}
			}
		}
	}
	// INVAR: lpb set, or needNewLocalPeer true.
	knownLocalPeer.mut.Unlock()

	cfg := s.u.GetConfig()
	lim := cfg.GetLimitMax(peerServiceName)

	//vv("needNewLocalPeer=%v, curServiceCount(%v); cfg.ServiceLimit(%v)", needNewLocalPeer, curServiceCount, lim)
	if needNewLocalPeer {
		if lim > 0 && curServiceCount >= lim {
			// at limit, reject making another. But tell
			// them who it is -- so they can find/use us.
			msg.HDR.Args["#LimitedServiceMax"] = fmt.Sprintf("%v", lim)
			msg.HDR.Args["#LimitedServiceCurrentCount"] = fmt.Sprintf("%v", curServiceCount)
			msg.HDR.Args["#LimitedServiceName"] = peerServiceName
			msg.HDR.Args["#LimitedExistingPeerIDs_comma_sep"] = strings.Join(extantPeerIDs, ",")
			msg.HDR.Args["#LimitedExistingPeerID_first"] = extantPeerIDs[0]
			msg.HDR.Args["#LimitedExistingPeerID_first_url"] = extantPeerIDsURL0
			return s.rejectWith(fmt.Sprintf("bootstrapCircuit error: peerServiceName '%v' is listed in cfg.LimitedServiceNames and is already at cfg.LimitedServiceMax = %v (curServiceCount = %v)", peerServiceName, lim, curServiceCount), isCli, msg, ctx, sendCh)
		}
		// spin one up!
		//vv("needNewLocalPeer true! spinning up a peer for peerServicename '%v'; Typ='%v'", peerServiceName, msg.HDR.Typ)
		//lpb2, localPeerURL, localPeerID, err := s.StartLocalPeer(ctx, peerServiceName, msg)
		lpb2, err := s.unlockedStartLocalPeer(ctx, peerServiceName, peerServiceNameVersion, msg, isUpdatedPeerID, sendCh, pleaseAssignNewRemotePeerID, "", onRemote2ndSide, preferExtant)
		if err != nil {
			// we are probably shutting down; Test408 gets here with
			// "rpc25519 error: halt requested".
			return err
		}
		lpb = lpb2
		// unlockedStartLocalPeer already called provideRemoteOnNewCircuitCh.

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

			ack.HDR.From = msg.HDR.To
			ack.HDR.To = msg.HDR.From

			ack.HDR.CallID = msg.HDR.CallID
			ack.HDR.FromPeerID = lpb.PeerID
			ack.HDR.FromPeerName = lpb.PeerName
			ack.HDR.Typ = CallOneWay // not peer/ckt traffic yet, only bootstrapping peer

			ack.HDR.Args = map[string]string{
				"#peerURL":            lpb.URL(),
				"#peerID":             lpb.PeerID,
				"#fromServiceName":    lpb.PeerServiceName,
				"#fromBaseServerName": lpb.BaseServerName,
				"#fromBaseServerAddr": lpb.BaseServerAddr,
				//"#fragRPCtoken": msg.HDR.Args["#fragRPCtoken"] // but only CallPeerStartCircuitTakeToID atm, so here maybe not.
			}

			ack.HDR.ToServiceName = msg.HDR.FromServiceName
			ack.HDR.ToPeerServiceNameVersion = msg.HDR.FromPeerServiceNameVersion
			ack.HDR.FromServiceName = lpb.PeerServiceName
			ack.HDR.FromPeerServiceNameVersion = lpb.PeerServiceNameVersion
			// these might be best effort/empty... b/c of bootstrapping/CallOneWay
			ack.HDR.ToPeerID = msg.HDR.FromPeerID
			ack.HDR.ToPeerName = msg.HDR.FromPeerName

			return s.replyHelper(isCli, ack, ctx, sendCh)
		}
		return nil
	}

	//vv("bootstrapCircuit ending, about to call lpb.provideRemoteOnNewCircuitCh '%v'; Typ='%v'; isUpdatedPeerID=%v", peerServiceName, msg.HDR.Typ, isUpdatedPeerID)

	// Providing s.mut and skipUnlock allows provideRemote to
	// unlock s.mut before doing
	// so potentially long blocking <-ckt and read <- frag stuff;
	// in an attempt to avoid the 2 sec pump apparent deadlock.
	return lpb.provideRemoteOnNewCircuitCh(isCli, msg, ctx, sendCh, isUpdatedPeerID, onRemote2ndSide, preferExtant, &s.mut, &skipUnlock)
}

// called just above :1833 in bootstrapCircuit() which holds lpb.mut;
// and :1459 unlockedStartLocalPeer() which is called
// from two places :1369 (StartLocalPeer, which holds mut at top),
// :1778 (bootstrapCircuit, which as in the first case, holds mut).
func (lpb *LocalPeer) provideRemoteOnNewCircuitCh(isCli bool, msg *Message, ctx context.Context, sendCh *LoopComm, isUpdatedPeerID bool, isRemoteSide onRemoteSideVal, preferExtant bool, callerMut *sync.Mutex, callerSkipUnlock *bool) error {
	rpb := &RemotePeer{
		LocalPeer: lpb,
		PeerID:    msg.HDR.FromPeerID,
		PeerName:  msg.HDR.FromPeerName,
		NetAddr:   msg.HDR.From,
	}
	circuitName := ""
	gotServiceName := false
	gotServiceNameVersion := false
	if msg.HDR.Args != nil {
		rpb.RemoteServiceName, gotServiceName = msg.HDR.Args["#fromServiceName"]
		rpb.RemotePeerServiceNameVersion, gotServiceNameVersion = msg.HDR.Args["#fromPeerServiceNameVersion"]
		//if rpb.RemoteServiceName != "" && rpb.PeerID != "" {
		//	AliasRegister(rpb.PeerID, rpb.PeerID+" ("+rpb.RemoteServiceName+")")
		//}
		circuitName = msg.HDR.Args["#circuitName"]
		rpb.BaseServerName = msg.HDR.Args["#fromBaseServerName"]
		rpb.BaseServerAddr = msg.HDR.Args["#fromBaseServerAddr"]
		//vv("setting rpb.BaseServerAddr='%v'; rpb.BaseServerName = '%v'", rpb.BaseServerAddr, rpb.BaseServerName)
	}
	if !gotServiceName {
		rpb.RemoteServiceName = msg.HDR.FromServiceName
	}
	if !gotServiceNameVersion {
		rpb.RemotePeerServiceNameVersion = msg.HDR.FromPeerServiceNameVersion
	}

	asFrag := convertMessageToFragment(msg)
	if isUpdatedPeerID {
		// let the remote know that the old peer disappeared
		// and we are the updated version/taking over.
		asFrag.SetSysArg("oldPeerID", msg.HDR.ToPeerID)
		asFrag.SetSysArg("newPeerID", lpb.PeerID)
		asFrag.SetSysArg("newPeerURL", lpb.URL())
	}

	ckt, ctx2, madeNewAutoCli, err := lpb.newCircuit(circuitName, rpb, msg.HDR.CallID, asFrag, -1, false, isRemoteSide, preferExtant)
	_ = madeNewAutoCli // not sure if we need to surface or not
	if err != nil {
		return err
	}

	if msg.HDR.Args != nil {
		ckt.UserString = msg.HDR.Args["#UserString"]
	}

	rpb.IncomingCkt = ckt

	_ = ckt.LocalCircuitURL() // Keep the call but avoid unused variable
	rpb.PeerURL = ckt.RemoteCircuitURL()

	// set inside newCircuit now: ckt.FirstFrag = asFrag

	// now we go directly to the NewCircuitCh, so user
	// does not need a second step to call IncommingCircuit!
	if callerSkipUnlock != nil && callerMut != nil {
		*callerSkipUnlock = true
		(*callerMut).Unlock()
	}
	select {
	case sendCh.cktServedAdd <- ckt:
		//vv("provideRemoteOnNewCircuitCh: sent ckt on LoopComm.cktServedAdd")
	case <-ckt.Halt.ReqStop.Chan:
	case <-ctx2.Done():
	}

	select {
	case lpb.NewCircuitCh <- ckt: // should this be sendCh ?? nope, but early it did not seem used.
		select { // might be blocked here in 2 sec pump stack dump. called by :1833 peerAPI.bootstrapCircuit where s.mut.Lock is held
		case ckt.Reads <- asFrag:
		case <-ckt.Halt.ReqStop.Chan:
		case <-ctx2.Done():
		}
	case <-ctx2.Done():
		//vv("ctx2 cancelled, cause: '%v'", context.Cause(ctx2))
		return ErrContextCancelled
	case <-lpb.Halt.ReqStop.Chan:
		return ErrHaltRequested // ErrShutdown() ?
	}

	return nil
}

// helper for bootstapCircuit rejects
func (s *peerAPI) rejectWith(errString string, isCli bool, msg *Message, ctx context.Context, sendCh *LoopComm) error {
	// moved out from replyHelper since did not apply
	// to the other use at ckt.go:1302
	// Re-use same msg in error reply:
	msg.HDR.From, msg.HDR.To = msg.HDR.To, msg.HDR.From
	msg.HDR.FromPeerID, msg.HDR.ToPeerID = msg.HDR.ToPeerID, msg.HDR.FromPeerID
	msg.HDR.FromPeerName, msg.HDR.ToPeerName = msg.HDR.ToPeerName, msg.HDR.FromPeerName
	msg.HDR.FromServiceName, msg.HDR.ToServiceName = msg.HDR.ToServiceName, msg.HDR.FromServiceName
	msg.HDR.FromPeerServiceNameVersion, msg.HDR.ToPeerServiceNameVersion = msg.HDR.ToPeerServiceNameVersion, msg.HDR.FromPeerServiceNameVersion
	msg.JobErrs = errString
	msg.JobSerz = nil
	fromService, ok := msg.HDR.Args["#fromServiceName"]
	if ok {
		msg.HDR.Args["#toServiceName"] = fromService
		delete(msg.HDR.Args, "#fromServiceName")
	}
	fromServiceVersion, ok := msg.HDR.Args["#fromPeerServiceNameVersion"]
	if ok {
		msg.HDR.Args["#toPeerServiceNameVersion"] = fromServiceVersion
		delete(msg.HDR.Args, "#fromPeerServiceNameVersion")
	}
	fromServiceBaseServerName, ok := msg.HDR.Args["#fromBaseServerName"]
	if ok {
		msg.HDR.Args["#toBaseServerName"] = fromServiceBaseServerName
		delete(msg.HDR.Args, "#fromBaseServerName")
	}
	// don't think we need the corresponding addr? TODO delete:
	fromServiceBaseServerAddr, ok := msg.HDR.Args["#fromBaseServerAddr"]
	if ok {
		msg.HDR.Args["#toBaseServerAddr"] = fromServiceBaseServerAddr
		delete(msg.HDR.Args, "#fromBaseServerAddr")
	}

	//vv("bootstrapCircuit returning early (isCli=%v): '%v'", isCli, msg.JobErrs)

	// having this be CallPeerError was creating a hang
	// at the remote (client in 410 part 2 fragrpc_test);
	// because the peerServiceFunc there does not know
	// there is a circuit to service yet... inherently
	// a formula for a deadlocked peer pump, so just
	// leave that out and only report an error under
	// Typ CallPeerCircuitEstablishedAck.

	// CallPeerCircuitEstablishedAck is the
	// always expected on the happy path, since
	// the #fragRPCtoken machinery needs it.
	// Cf 410 fragrpc_test checks for this error.
	msg.HDR.Typ = CallPeerCircuitEstablishedAck
	return s.replyHelper(isCli, msg, ctx, sendCh)
}

// replyHelper helps bootstrapCircuit with replying, keeping its
// code more compact. Only return errors here that
// should shut down the whole client/connection; like
// host-shutdown errors.
func (s *peerAPI) replyHelper(isCli bool, msg *Message, ctx context.Context, sendCh *LoopComm) error {

	// assume these are correctly set not:
	//msg.HDR.From, msg.HDR.To
	//msg.HDR.FromPeerID, msg.HDR.ToPeerID

	// but update the essentials
	msg.HDR.Serial = issueSerial()
	msg.HDR.Created = time.Now()
	msg.HDR.LocalRecvTm = time.Time{}
	msg.HDR.Deadline = time.Time{}

	msg.DoneCh = nil // no need now, save allocation. loquet.NewChan(msg)

	select {
	case sendCh.sendCh <- msg:
	case <-ctx.Done():
		return nil // ErrShutdown() but that would shut down whole client.
	case <-s.u.GetHostHalter().ReqStop.Chan:
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
func (s *peerAPI) bootstrapPeerService(isCli bool, msg *Message, ctx context.Context, sendCh *LoopComm, localPeerName string) error {

	////vv("top of bootstrapPeerService(): isCli=%v; msg.HDR='%v'", isCli, msg.HDR.String())

	preferExtant := false
	if msg != nil && msg.HDR.Typ == CallPeerStartCircuitAtMostOne {
		preferExtant = true
	}

	// starts its own goroutine or return with an error (both quickly).
	lpb, err := s.StartLocalPeer(ctx, msg.HDR.ToServiceName, msg.HDR.ToPeerServiceNameVersion, msg, localPeerName, preferExtant)
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
			"#peerURL":                    localPeerURL,
			"#peerID":                     localPeerID,
			"#fromServiceName":            msg.HDR.ToServiceName,
			"#fromPeerServiceNameVersion": msg.HDR.ToPeerServiceNameVersion,
			"#fromBaseServerName":         lpb.BaseServerName,
			"#fromBaseServerAddr":         lpb.BaseServerAddr,
			"#toServiceName":              msg.HDR.FromServiceName,
			"#toPeerServiceNameVersion":   msg.HDR.FromPeerServiceNameVersion,
		}
	}
	msg.HDR.FromServiceName, msg.HDR.ToServiceName = msg.HDR.ToServiceName, msg.HDR.FromServiceName
	msg.HDR.FromPeerServiceNameVersion, msg.HDR.ToPeerServiceNameVersion = msg.HDR.ToPeerServiceNameVersion, msg.HDR.FromPeerServiceNameVersion

	msg.HDR.FromPeerID = localPeerID

	select {
	case sendCh.sendCh <- msg:
	case <-ctx.Done():
		return ErrShutdown()
	}
	return nil
}

// EpochCoordID lets PingStat report the
// highest epoch seen. The ID2 string is independent
// of the PeerID in ckt, since that subsystem
// may not be use, but has the same format.
type EpochVers struct {
	EpochID         int64  `zid:"0"`
	EpochTieBreaker string `zid:"1"`
}

// 0 => logically equals (same content); not
// necessarily the same pointer (a==b would
// be an easier test if that's all that is needed).
// -1 means a < b
// +1 means a > b
func (a *Fragment) Compare(b *Fragment) int {
	if a == nil || b == nil {
		// if both nil, should NaN == NaN? not usually
		panic("Fragment.Equals does not compare to nil, since two nil pointers may not mean identical things")
	}
	if a == b {
		return 0 // same object in memory
	}
	// try to short ciruit as fast as possible,
	// highest entropy things first.
	if a.CircuitID < b.CircuitID {
		return -1
	}
	if a.CircuitID > b.CircuitID {
		return 1
	}
	if a.FromPeerID < b.FromPeerID {
		return -1
	}
	if a.FromPeerID > b.FromPeerID {
		return 1
	}
	if a.ToPeerID < b.ToPeerID {
		return -1
	}
	if a.ToPeerID > b.ToPeerID {
		return 1
	}
	if a.Serial < b.Serial {
		return -1
	}
	if a.Serial > b.Serial {
		return 1
	}
	if a.ToPeerServiceName < b.ToPeerServiceName {
		return -1
	}
	if a.ToPeerServiceName > b.ToPeerServiceName {
		return 1
	}
	if a.FromPeerServiceName < b.FromPeerServiceName {
		return -1
	}
	if a.FromPeerServiceName > b.FromPeerServiceName {
		return 1
	}

	// really that should have sufficed,
	// if the chacha8+blake3 CallID are
	// in use, as they should be.
	// For more than good measure,
	// and because we want to use
	// this to catch implementation
	// errors in tests of things like clone()...
	if a.Typ < b.Typ {
		return -1
	}
	if a.Typ > b.Typ {
		return 1
	}
	if a.FragOp < b.FragOp {
		return -1
	}
	if a.FragOp > b.FragOp {
		return 1
	}
	if a.FragPart < b.FragPart {
		return -1
	}
	if a.FragPart > b.FragPart {
		return 1
	}
	if a.FragSubject < b.FragSubject {
		return -1
	}
	if a.FragSubject > b.FragSubject {
		return 1
	}
	if a.Err < b.Err {
		return -1
	}
	if a.Err > b.Err {
		return 1
	}
	cmp := bytes.Compare(a.Payload, b.Payload)
	if cmp < 0 {
		return -1
	}
	if cmp > 1 {
		return 1
	}
	// a.Args, b.Args:
	// map order is non-deterministic, skip
	// anything related to the content of
	// the user Args. Really, we are into
	// way, way overkill here anyway.
	return 0
}

// CallPeerCircuitEstablishedAck seen.
// This is called directly by srv/cli readLoop: an ack
// of a circuit setup has been received.
// Only return an error here if it is a shutdown request;
// it will shutdown the callers read loop.
//
// It may seem like this does not allow end-to-end
// contact with an existing peer, and you are
// correct in that surmise. However for the
// remote side address an existing peer, it
// needs to discover its PeerID, and this
// callback gives exactly that needed
// information: the answering msg.HDR.FromPeerID.
// Thus we provide an essential means of setting
// up contact with an already running peer.
// Especially combined with Typ=CallPeerStartCircuitAtMostOne.
//
// A more complex alternative means would be
// to start a remote whole separate "service discovery
// peer service" that is always a new peer, but
// itself can survey the existing other
// peers on the same machine, and tell the
// caller about them. For now we start simple.
func (s *peerAPI) gotCircuitEstablishedAck(isCli bool, msg *Message, ctx context.Context, sendCh *LoopComm) error {
	//vv("gotCircuitEstablishedAck seen. isCli=%v; msg='%v'", isCli, msg) // seen 1x in 410, isCli = true
	token, ok := msg.HDR.Args["#fragRPCtoken"]
	if ok {
		chGood, chErr := s.u.GetChanInterestedInCallID(token)
		if chGood == nil && chErr == nil {
			//vv("nobody interested in fragRPC token '%v', dropping: '%v'", token, msg)
			return nil
		}
		if chGood != nil && cap(chGood) == 0 {
			panic("chGood done channel is unbuffered")
		}
		if chErr != nil && cap(chErr) == 0 {
			panic("chErr done channel is unbuffered")
		}

		if msg.JobErrs == "" {
			if chGood != nil {
				// happy path
				//vv("good: about to notify ch=%p about token '%v'", chGood, token)
				select {
				case chGood <- msg:
					//vv("good: notification of ch=%p about token '%v' sent", chGood, token)
					return nil
				case <-ctx.Done():
					return nil
				case <-s.u.GetHostHalter().ReqStop.Chan:
					return ErrHaltRequested
				default:
					panic(fmt.Sprintf("error: out of space in chGood to report gotCircuitEstablishedAck (these chan really must be buffered): len(chGood)=%v, cap(chGood)=%v; msg='%v'", len(chGood), cap(chGood), msg))
				}
			} else {
				//vv("dropping msg GetChanInterestedInCallID found no happy chan for #fragRPCtoken = '%v'", token)
				return nil
			}
			return nil
		}

		// have msg.JobErrs
		if chErr == nil {
			if chGood != nil {
				alwaysPrintf("warning: delivering error on chGood, no separate error registration found.")
				chErr = chGood
			}
		}
		//vv("about to notify chErr=%p about error re token '%v'", chErr, token)
		select {
		case chErr <- msg:
			//vv("good: chErr notification of ch=%p about token '%v' sent", chErr, token)
		case <-ctx.Done():
		case <-s.u.GetHostHalter().ReqStop.Chan:
			return ErrHaltRequested
		default:
			panic(fmt.Sprintf("error: out of space in chErr to report gotCircuitEstablishedAck (these chan really must be buffered): len(chErr)=%v, cap(chErr)=%v; msg='%v'", len(chErr), cap(chErr), msg))
		}

	}
	return nil
}
