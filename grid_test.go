package rpc25519

import (
	"context"
	"fmt"
	"time"

	"github.com/glycerine/idem"
	"testing"
)

// Test202 verifies that wiring up a simple grid of servers
// that talk to each other works. This taught us that we
// need two extra things to make it work:
//
// a) set cfg.ServerAutoCreateClientsToDialOtherServers = true
// which means we will start a client to the remote server
// if no other existing communication path is available;
//
// and
//
// b) do n0.lpb.NewCircuitCh <- ckt
// manually with the ckt obtained from NewCircuitToPeerURL().
//
// This addition is needed because the ckt might (typically;
// in the original design and use) be generated inside the
// PeerServiceFunc goroutine and then it would be
// redundant to automatically also send it in on the newCircuitCh.
// Since in grid setup we call NewCircuitToPeerURL()
// externally (not inside the Start PeerServiceFunc), we have
// to tell the PeerServiceFunc goroutine
// about it with that send on NewCircuitCh.
//
// Also note that all the grid service func should be
// registered under the same name during the call
// to RegisterPeerServiceFunc(). Here that name is "grid".
func Test202_grid_peer_to_peer_works(t *testing.T) {

	if globalUseSynctest {
		t.Skip("skip under synctest, net calls will never settle.")
		return
	}

	//n := 20 // 20*19/2 = 190 tcp conn to setup. ok/green but 35 seconds.
	n := 3 // 2.7 sec
	cfg := &gridConfig{
		ReplicationDegree: n,
		Timeout:           time.Second * 5,
	}

	var nodes []*gridNode
	for i := range n {
		name := fmt.Sprintf("grid_node_%v", i)
		nodes = append(nodes, newGridNode(name, cfg))
	}
	c := newGrid(cfg, nodes)
	c.Start()
	defer c.Close()

	for i, g := range nodes {
		select {
		case <-g.node.peersNeededSeen.Chan:
			vv("i=%v all peer connections need have been seen(%v) by '%v': '%#v'", i, g.node.peersNeeded, g.node.name, g.node.seen.GetKeySlice())
		}
	}
}

type node struct {
	cfg  *gridConfig
	name string

	// comms
	PushToPeerURL          chan string
	halt                   *idem.Halter
	lpb                    *LocalPeer
	gotIncomingCktReadFrag chan *Fragment

	seen *Mutexmap[string, bool]

	peersNeeded     int
	peersNeededSeen *idem.IdemCloseChan
}

func newNode(srv *Server, name string, cfg *gridConfig) *node {
	return &node{
		peersNeeded:     cfg.ReplicationDegree - 1,
		peersNeededSeen: idem.NewIdemCloseChan(),
		name:            name,
		seen:            NewMutexmap[string, bool](),
		// comms
		PushToPeerURL:          make(chan string),
		halt:                   srv.halt,
		gotIncomingCktReadFrag: make(chan *Fragment),
	}
}

type gridNode struct {
	cfg  *gridConfig
	node *node

	rpccfg *Config
	srv    *Server
	name   string // same as srvServiceName

	lpb *LocalPeer

	URL    string
	PeerID string

	ckts []*Circuit
}

type gridConfig struct {
	ReplicationDegree int
	Timeout           time.Duration
}

type grid struct {
	Cfg   *gridConfig
	Nodes []*gridNode
}

func newGrid(cfg *gridConfig, nodes []*gridNode) *grid {
	return &grid{Cfg: cfg, Nodes: nodes}
}

func (s *grid) Start() {
	for i, n := range s.Nodes {
		_ = i
		err := n.Start()
		panicOn(err)
	}
	//time.Sleep(time.Second)
	// now that they are all started, form a complete mesh
	// by connecting each to all the others
	sz := len(s.Nodes)
	for i, n0 := range s.Nodes {
		for j, n1 := range s.Nodes {
			if j <= i || i == sz-1 {
				continue
			}
			// tell a fresh client to connect to server and then pass the
			// conn to the existing server.

			//vv("about to connect i=%v to j=%v", i, j)
			ckt, _, err := n0.lpb.NewCircuitToPeerURL("grid-ckt", n1.URL, nil, 0)
			panicOn(err)
			n0.lpb.NewCircuitCh <- ckt
			n0.ckts = append(n0.ckts, ckt)
			//vv("created ckt between n0 '%v' and n1 '%v': '%v'", n0.name, n1.name, ckt.String())
		}
	}
}

func (s *grid) Close() {
	for _, n := range s.Nodes {
		n.Close()
	}
}

func (s *gridNode) Close() {
	s.srv.Close()
}

func newGridNode(name string, cfg *gridConfig) *gridNode {
	return &gridNode{
		cfg:  cfg,
		name: name,
	}
}

func (s *gridNode) Start() error {
	cfg := NewConfig()
	cfg.TCPonly_no_TLS = true

	cfg.ServerAddr = "127.0.0.1:0"

	// key setting under test here:
	cfg.ServerAutoCreateClientsToDialOtherServers = true

	s.srv = NewServer("srv_"+s.name, cfg)
	s.rpccfg = cfg

	serverAddr, err := s.srv.Start()
	panicOn(err)

	cfg.ClientDialToHostPort = serverAddr.String()
	vv("serverAddr = '%#v' -> '%v'", serverAddr, cfg.ClientDialToHostPort)

	s.node = newNode(s.srv, s.name, s.cfg)

	err = s.srv.PeerAPI.RegisterPeerServiceFunc("grid", s.node.Start)
	panicOn(err)

	s.lpb, err = s.srv.PeerAPI.StartLocalPeer(context.Background(), "grid", nil)
	panicOn(err)
	s.node.lpb = s.lpb
	s.URL = s.lpb.URL()
	s.PeerID = s.lpb.PeerID

	vv("gridNode.Start() started '%v' as 'grid' with url = '%v'", s.name, s.URL)

	return nil
}

func (s *node) Start(
	myPeer *LocalPeer,
	ctx0 context.Context,
	newCircuitCh <-chan *Circuit,

) (err0 error) {

	defer func() {
		//vv("%v: (%v) end of node.Start() inside defer, about the return/finish", s.name, myPeer.ServiceName())
		s.halt.Done.Close()
	}()

	//vv("%v: node.Start() top.", s.name)
	//vv("%v: node.Start() top. ourID = '%v'; peerServiceName='%v';", s.name, myPeer.PeerID, myPeer.ServiceName())

	AliasRegister(myPeer.PeerID, myPeer.PeerID+" ("+myPeer.ServiceName()+")")

	done0 := ctx0.Done()

	for {
		//vv("%v: top of select", s.name) // client only seen once, since peer_test acts as cli
		select {
		// new Circuit connection arrives
		case ckt := <-newCircuitCh:

			//vv("%v: got from newCircuitCh! service '%v' sees new peerURL: '%v'", s.name, myPeer.PeerServiceName, myPeer.URL())

			// talk to this peer on a separate goro if you wish:
			go func(ckt *Circuit) (err0 error) {

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
						//vv("%v: (ckt %v) ckt.Reads sees frag:'%s'", s.name, ckt.Name, frag)

						s.seen.Set(AliasDecode(frag.FromPeerID), true)

						peersSeen := s.seen.Len()
						if peersSeen >= s.peersNeeded {
							s.peersNeededSeen.Close()
						}

						//s.gotIncomingCktReadFrag <- frag
						//vv("%v: (ckt %v) past s.gotIncomingCktReadFrag <- frag. frag:'%s'", s.name, ckt.Name, frag)

						if frag.Typ == CallPeerStartCircuit {

							outFrag := myPeer.NewFragment()
							outFrag.Payload = frag.Payload
							outFrag.FragSubject = "start reply"
							outFrag.ServiceName = myPeer.ServiceName()
							err := ckt.SendOneWay(outFrag, 0, 0)
							panicOn(err)
							//vv("%v: (ckt '%v') sent start reply='%v'", s.name, ckt.Name, outFrag)
						}

						if frag.FragSubject == "start reply" {
							//vv("we see start reply") // seen.
						}

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
			return ErrContextCancelled
		case <-s.halt.ReqStop.Chan:
			//zz("%v: halt.ReqStop seen", s.name)
			return ErrHaltRequested
		}
	}
	return nil
}
