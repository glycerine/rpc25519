package hermes

import (
	"context"
	"fmt"
	"time"

	"github.com/glycerine/idem"
	rpc "github.com/glycerine/rpc25519"
	"github.com/glycerine/rpc25519/tube"
)

type HermesNode struct {
	cfg *HermesConfig

	// hermes protocol
	lease     time.Time
	EpochID   int64
	liveNodes []string
	member    *tube.RMember

	// the main key/value store.
	store map[Key]*keyMeta

	// pending reads/writes are stored as HermesTickets in
	// the timeoutPQ priority queue. The pq is sorted by messageLossTimeout,
	// and indexed by tkt2items, key2items, and buffered.
	// Use deleteHermesTicket() to remove from all of these conveniently.
	timeoutPQ *pqTime

	// index to find stuff in timeoutPQ,
	// with HermesTicket.TicketID as the key
	tkt2item map[string]*pqTimeItem
	// lookup all the items for a given Key
	key2items map[Key]*pqitems
	// order of arrival
	buffered []*pqTimeItem

	// comms
	URL           string
	PeerID        string
	pushToPeerURL chan string
	myPeer        *rpc.LocalPeer
	halt          *idem.Halter

	// ckt tracks all our peer replicas (does not include ourselves)
	ckt        map[string]*rpc.Circuit
	rpccfg     *rpc.Config
	srv        *rpc.Server
	name       string // "hermes_node_2" vs. srvServiceName which is always "hermes"
	writeReqCh chan *HermesTicket
	readReqCh  chan *HermesTicket

	// let main set this during config if they want to
	// send in membership changes; NewHermesNode does not allocate it
	// at present as we assume that the reliable membership service
	// already does.
	UpcallMembershipChangeCh chan *tube.PingReply

	nextWake       time.Time
	nextWakeCh     <-chan time.Time
	nextWakeTicket *HermesTicket

	// convenience in debug printing
	me string
}

type HermesConfig struct {
	// how many total nodes
	ReplicationDegree int

	// when should we do recovery logic to
	// compensate for a failed node.
	MessageLossTimeout time.Duration

	// skip encryption? (used to simplify and speed up tests)
	TCPonly_no_TLS bool

	// for internal failure recovery testing,
	// e.g. to drop or ignore messages.
	// The int key hould correspond to the test number,
	// and the string value describes the condition.
	testScenario map[string]bool
}

func NewHermesNode(name string, cfg *HermesConfig) *HermesNode {
	return &HermesNode{
		cfg:   cfg,
		name:  name,
		ckt:   make(map[string]*rpc.Circuit),
		store: make(map[Key]*keyMeta),

		// comms
		pushToPeerURL: make(chan string),
		halt:          idem.NewHalter(),
		writeReqCh:    make(chan *HermesTicket),
		readReqCh:     make(chan *HermesTicket),

		// pending reads/writes are stored as HermesTickets in
		// a priority queue sorted by messageLossTimeout,
		// and indexed by tkt2items, key2items, and buffered.
		tkt2item:  make(map[string]*pqTimeItem),
		key2items: make(map[Key]*pqitems),

		timeoutPQ: newPqTime(),
	}
}

// HermesTicket is how Read/Write operations are submitted
// to hermes in a goroutine safe way such that the
// caller can simply wait until the Done channel is closed
// to resume. It is exported for serialization purposes.
type HermesTicket struct {
	Key Key `zid:"0"`
	Val Val `zid:"1"`
	TS  TS  `zid:"2"`

	FromID string `zid:"3"`
	Err    error  `zid:"4"`

	// TicketID is a unique identifier for each HermesTicket.
	TicketID string `zid:"5"`

	IsWrite                 bool `zid:"6"`
	IsRMW                   bool `zid:"7"`
	PseudoTicketForAckAccum bool `zid:"8"`

	Done *idem.IdemCloseChan `msg:"-"`

	keym *keyMeta

	// much easier to dedup acks with a map
	ackVector map[string]bool

	// should we return early and stop waiting
	// after a preset duration? 0 means wait forever.
	waitForValid time.Duration

	messageLossTimeout time.Time // mlt int
	// about messageLossTimeout, also known as mlt:
	//
	// "Write Replays: A request that finds a key in the Invalid state
	// for an extended period of time (determined via the mlt timer,
	// described in §3.4) triggers a write replay.
	//
	// The node servicing the request takes on the
	// coordinator role, transitions the key to the Replay
	// state and begins a write replay by re-executing
	// steps Coord_INV through Coord_VAL using the TS and value received
	// with the INV message. Note that the original TS is used in the
	// replay (i.e., the coordID is that of original coordinator) to allow the
	// write to be correctly linearized. Once the replay is completed,
	// the key transitions to the Valid state after which the initial
	// request is serviced."
	//
	// From section 3.4:
	// "Hermes uses the same idea of replaying writes if any of its
	// INV, ACK, or VAL messages is suspected to be lost. A message
	// is suspected to be lost for a key if the request’s message-loss
	// timeout (mlt), within which every write request is expected
	// to be completed, is exceeded. To detect the loss of an INV or
	// ACK for a particular write, the coordinator of the write resets
	// the request's mlt once it broadcasts INV messages. If the mlt
	// of a key is exceeded before its write completion, then the
	// coordinator suspects a potential message loss and resets the
	// request's mlt before retransmitting the write's INV broadcast.

}

func (t *HermesTicket) String() string {
	var dur time.Duration
	if !t.messageLossTimeout.IsZero() {
		dur = t.messageLossTimeout.Sub(time.Now())
	}
	av := "map[string]bool{"
	extra := ""
	for a, v := range t.ackVector {
		av += fmt.Sprintf("%v '%v':%v", extra, rpc.AliasDecode(a), v)
		extra = ","
	}
	av += "}"
	return fmt.Sprintf(`HermesTicket{
               Key: "%v",
               Val: "%v",
                TS: %v,
            FromID: %v,
               Err: %v,
          TicketID: %v,
           IsWrite: %v,
PseudoTicketForAckAccum: %v,
              keym: %v,
         ackVector: %v,
      waitForValid: %v,
messageLossTimeout: %v (in %v),
}`, string(t.Key), string(t.Val), t.TS.String(), rpc.AliasDecode(t.FromID), t.Err,
		t.TicketID, t.IsWrite, t.PseudoTicketForAckAccum, t.keym.String(), av, t.waitForValid,
		t.messageLossTimeout, dur)
}

func (s *HermesNode) NewHermesTicket(
	key Key,
	val Val,
	fromID string,
	isWrite bool,
	waitForDur time.Duration,
) (tkt *HermesTicket) {

	tkt = &HermesTicket{
		IsWrite: isWrite,
		Key:     key,
		Val:     val,
		//TS:       must be filled in by readReq/writeReq from the current keym.
		Done:         idem.NewIdemCloseChan(),
		FromID:       fromID,
		TicketID:     rpc.NewCallID(""),
		ackVector:    make(map[string]bool),
		waitForValid: waitForDur,
	}
	return
}

// Write a new value under key, or update key's existing
// value to val if key already exists.
//
// Setting waitForDur = 0 means the Write will
// wait indefinitely for the write to complete, and
// is a reasonable default. This provides strong consistency
// (linearizability) from all live replicas.
//
// Key versioning is used to make Write's action
// linearizable over a global total
// order of writes and reads to all live replicas. Any node can
// issue a Write, and any node can issue a Read, and
// both are linearizable with respect to each other.
// This provides the strongest and most intuitive
// consistency. It also gives us composability --
// a synonym for ease of use.
func (s *HermesNode) Write(key Key, val Val, waitForDur time.Duration) error {

	tkt := s.NewHermesTicket(key, val, s.PeerID, writer, waitForDur)
	select {
	case s.writeReqCh <- tkt:
		// proceed to wait below for txt.done
	case <-s.halt.ReqStop.Chan:
		return ErrTimeOut
	}

	select {
	case <-tkt.Done.Chan:
		return tkt.Err
	case <-s.halt.ReqStop.Chan:
		return ErrShutDown
	}
}

// Read a key's value. Setting waitForDur = 0
// means the Read will wait indefinitely for a valid key.
//
// For production use, you may want a non-zero waitForDur
// timeout, if you are able to (and/or don't want to) wait
// for some tail events like the replica recovery
// protocol to finish.
//
// A waitForDur of -1 means try locally, but return
// ErrNotFound quickly if there is nothing here.
//
// The default waitForDur of 0 avoids
// races with writers, and still provides the fastest
// possible local read if the key is valid (not
// being written at the moment).
//
// Read only ever returns a linearizable, replicated value
// when the returned error is nil. Non-nil errors can
// include ErrTimeOut, ErrShutDown, and ErrNotFound, in
// which case val will be undefined but typically nil.
func (s *HermesNode) Read(key Key, waitForDur time.Duration) (val Val, err error) {

	tkt := s.NewHermesTicket(key, val, s.PeerID, reader, waitForDur)
	select {
	case s.readReqCh <- tkt:
		// proceed to wait below for txt.Done
	case <-s.halt.ReqStop.Chan:
		return nil, ErrTimeOut
	}

	select {
	case <-tkt.Done.Chan:
		return tkt.Val, tkt.Err
	case <-s.halt.ReqStop.Chan:
		return nil, ErrShutDown
	}
}

func (s *HermesNode) Init() error {
	cfg := rpc.NewConfig()
	cfg.TCPonly_no_TLS = s.cfg.TCPonly_no_TLS

	cfg.ServerAddr = "127.0.0.1:0"
	cfg.ServerAutoCreateClientsToDialOtherServers = true
	s.srv = rpc.NewServer("hermes_srv_"+s.name, cfg)
	s.rpccfg = cfg

	serverAddr, err := s.srv.Start()
	panicOn(err)

	cfg.ClientDialToHostPort = serverAddr.String()

	err = s.srv.PeerAPI.RegisterPeerServiceFunc("hermes", s.Start)
	panicOn(err)

	const preferExtant = true
	peerName := s.name
	s.myPeer, err = s.srv.PeerAPI.StartLocalPeer(context.Background(), "hermes", "", nil, peerName, preferExtant)
	panicOn(err)
	s.URL = s.myPeer.URL()
	s.PeerID = s.myPeer.PeerID
	rpc.AliasRegister(s.PeerID, s.PeerID+" ("+s.name+")")

	vv("HermesNode.Init() started '%v' with url = '%v'; s.PeerID = '%v'", s.name, s.URL, s.PeerID)

	return nil
}

func (s *HermesNode) Start(
	myPeer *rpc.LocalPeer,
	ctx0 context.Context,
	newCircuitCh <-chan *rpc.Circuit,

) (err0 error) {

	defer func() {
		//zz("%v: (%v) end of syncer.Start() inside defer, about the return/finish", s.name, myPeer.ServiceName())
		s.halt.Done.Close()
	}()

	vv("%v: HermesNode.Start() top.", s.name)

	// vv() debug print convenience
	s.me = rpc.AliasDecode(s.PeerID)
	//vv("%v: ourID = '%v'; peerServiceName='%v';", s.name, myPeer.PeerID, myPeer.ServiceName())

	//rpc.AliasRegister(myPeer.PeerID, myPeer.PeerID+" ("+s.name+")")

	done0 := ctx0.Done()

	// centralize all peer incomming messages here, to avoid locking in a million places.
	arrivingNetworkFrag := make(chan *rpc.Fragment)

	for {
		//zz("%v: top of select", s.name) // client only seen once, since peer_test acts as cli
		select {

		case reply := <-s.UpcallMembershipChangeCh:
			vv("%v hermes node sees membership change upcall: '%v'", s.name, reply)

		case <-s.nextWakeCh:
			vv("=================== nextWakeCh fired ================")
			s.checkCoordOrFollowerFailed()

		case tkt := <-s.writeReqCh:
			s.writeReq(tkt)

		case tkt := <-s.readReqCh:
			s.readReq(tkt)

		case frag := <-arrivingNetworkFrag:

			// RM (Reliable Membership) protocol, from the paper:
			// "a receiver drops any message tagged with a different
			// epoch_id than its local epoch_id." (Section 2.4, p203)
			// This is implemented below.
			switch frag.FragOp {

			case INVmsg:
				//vv("unmarshal INVmsg")
				inv := &INV{}
				_, err := inv.UnmarshalMsg(frag.Payload)
				panicOn(err)
				if inv.EpochID == s.EpochID {
					err = s.recvInvalidate(inv)
					panicOn(err)
				}

			case VALIDATEmsg:
				valid := &VALIDATE{}
				_, err := valid.UnmarshalMsg(frag.Payload)
				panicOn(err)
				if valid.EpochID == s.EpochID {
					err = s.recvValidate(valid)
					panicOn(err)
				}
			case ACKmsg:
				ack := &ACK{}
				_, err := ack.UnmarshalMsg(frag.Payload)
				panicOn(err)
				if ack.EpochID == s.EpochID {
					err = s.recvAck(ack)
					panicOn(err)
				}
			case 0:
				switch frag.Typ {
				case rpc.CallPeerStartCircuit:
					vv("do stuff for new replica joining here. TODO.")
					continue
				case rpc.CallPeerTraffic:
					vv("TODO what means CallPeerTraffic here?")
					continue
				}

				panic(fmt.Sprintf("unknown frag meaning: '%v'", frag.String())) //
			default:
				panic(fmt.Sprintf("unknown hermes message: not CallPeerStartCircuit and "+
					"not ACK, INV, or VALIDATE: frag.FragOp='%v'; frag='%v'", frag.FragOp, frag.String()))
			}

		// URL format: tcp://x.x.x.x:port/peerServiceName
		case pushToURL := <-s.pushToPeerURL:
			//zz("%v: sees pushToURL '%v'", s.name, pushToURL)

			//zz("%v: about to new up the server. pushToURL='%v'", s.name, pushToURL)
			var firstFrag *rpc.Fragment
			var errWriteDur time.Duration
			userString := ""
			remotePeerName := ""
			ckt, ctx, _, err := myPeer.NewCircuitToPeerURL("hermes-node", pushToURL, firstFrag, errWriteDur, userString, remotePeerName)

			//zz("%v: back from myPeer.NewCircuitToPeerURL(pushToURL: '%v'): err='%v'", s.name, pushToURL, err)
			panicOn(err)
			//zz("%v: got ckt = '%v' back from NewCircuitToPeerURL '%v'", s.name, ckt.Name, pushToURL)
			defer func() {
				//zz("%v: (ckt '%v') defer running; closing ckt for pushToURL.", s.name, ckt.Name)
				ckt.Close(err0)
			}()

			done := ctx.Done()
			_ = done

			for {
				//zz("%v: (ckt '%v'): top of select", s.name, ckt.Name)
				select {
				case frag := <-ckt.Reads:
					//zz("%v: (ckt '%v') got from ckt.Reads ->  '%v'", s.name, ckt.Name, frag.String())
					_ = frag

				case fragerr := <-ckt.Errors:
					//zz("%v: (ckt '%v') got error fragerr back: '%#v'", s.name, ckt.Name, fragerr)
					_ = fragerr
				case <-ckt.Halt.ReqStop.Chan:
					//zz("%v: (ckt '%v') ckt halt requested.", s.name, ckt.Name)
					return nil

				case <-done:
					//zz("%v: (ckt '%v') done seen", s.name, ckt.Name)
					return nil
				case <-done0:
					//zz("%v: (ckt '%v') done0 seen", s.name, ckt.Name)
					return nil
				case <-s.halt.ReqStop.Chan:
					//zz("%v: (ckt '%v') topp func halt.ReqStop seen", s.name, ckt.Name)
					return nil
				}
			}

			// new Circuit connection arrives: a replica joins the cluster.
		case ckt := <-newCircuitCh:

			s.ckt[ckt.RemotePeerID] = ckt
			vv("%v: got from newCircuitCh! service '%v' sees new peerURL: '%v'; s.ckt sz=%v", s.name, myPeer.PeerServiceName, myPeer.URL(), len(s.ckt))

			// talk to this peer on a separate goro
			go func(ckt *rpc.Circuit) (err0 error) {

				ctx := ckt.Context
				//zz("%v: (ckt '%v') got incoming ckt", s.name, ckt.Name)

				defer func() {
					//zz("%v: (ckt '%v') defer running! finishing RemotePeer goro. stack = '%v'", s.name, ckt.Name, stack()) // seen on server
					ckt.Close(err0)
				}()

				//zz("%v: (ckt '%v') <- got new incoming ckt", s.name, ckt.Name)
				////zz("incoming ckt has RemoteCircuitURL = '%v'", ckt.RemoteCircuitURL())
				////zz("incoming ckt has LocalCircuitURL = '%v'", ckt.LocalCircuitURL())
				done := ctx.Done()

				for {
					select {
					case frag := <-ckt.Reads:
						//vv("%v: (ckt %v) ckt.Reads sees frag:'%s'; my PeerID='%v'", s.name, ckt.Name, frag, s.PeerID)
						// test failure scenarios that cause message loss
						// (equivalent to machine failure).
						if s.cfg.testScenario != nil {
							if frag.FragOp == VALIDATEmsg && s.cfg.testScenario["ignore VALIDATE"] {
								vv("testScenario: ignore VALIDATE => dropping '%v'", frag.String())
								continue
							}
							if frag.FragOp == ACKmsg && s.cfg.testScenario["ignore ACK"] {
								vv("testScenario: ignore ACK => dropping '%v'", frag.String())
								continue
							}
						}

						// centralize to avoid locking in a bajillion places
						arrivingNetworkFrag <- frag
					case fragerr := <-ckt.Errors:
						//zz("%v: (ckt '%v') fragerr = '%v'", s.name, ckt.Name, fragerr)
						_ = fragerr

					case <-ckt.Halt.ReqStop.Chan:
						//zz("%v: (ckt '%v') ckt halt requested.", s.name, ckt.Name)
						return

					case <-done:
						//zz("%v: (ckt '%v') done!", s.name, ckt.Name)
						return // server finishing on done!
					case <-done0:
						//zz("%v: (ckt '%v') done0!", s.name, ckt.Name)
						return
					case <-s.halt.ReqStop.Chan:
						//zz("%v: (ckt '%v') top func halt.ReqStop seen", s.name, ckt.Name)
						return
					}
				}

			}(ckt)

		case <-done0:
			//zz("%v: done0!", s.name)
			return rpc.ErrContextCancelled
		case <-s.halt.ReqStop.Chan:
			//zz("%v: halt.ReqStop seen", s.name)
			return rpc.ErrHaltRequested
		}
	}
	return nil
}
