package rpc25519

import (
	"context"
	"fmt"
	"time"

	"github.com/glycerine/idem"
	"testing"
)

func Test002_grid_peer_to_peer_works(t *testing.T) {
	n := 2
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

	time.Sleep(time.Second)

	for i, n := range nodes {
		vv("i=%v has n.seen = %#v", i, n.node.seen)
	}
	select {}
}

type node struct {
	cfg  *gridConfig
	name string

	// comms
	PushToPeerURL chan string
	halt          *idem.Halter
	lpb           *LocalPeer

	seen map[string]bool
}

func newNode(name string, cfg *gridConfig) *node {
	return &node{
		name: name,
		seen: make(map[string]bool),
		// comms
		PushToPeerURL: make(chan string),
		halt:          idem.NewHalter(),
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
	time.Sleep(time.Second)
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

			vv("about to connect i=%v to j=%v", i, j)
			ckt, _, err := n0.lpb.NewCircuitToPeerURL("grid-ckt", n1.URL, nil, 0)
			panicOn(err)
			n0.ckts = append(n0.ckts, ckt)
			vv("created ckt between n0 '%v' and n1 '%v': '%v'", n0.name, n1.name, ckt.String())
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

	s.node = newNode(s.name, s.cfg)

	err = s.srv.PeerAPI.RegisterPeerServiceFunc(s.name, s.node.Start)
	panicOn(err)

	s.lpb, err = s.srv.PeerAPI.StartLocalPeer(context.Background(), s.name, nil)
	panicOn(err)
	s.node.lpb = s.lpb
	s.URL = s.lpb.URL()
	s.PeerID = s.lpb.PeerID

	vv("gridNode.Start() started '%v' with url = '%v'", s.name, s.URL)

	return nil
}

func (s *node) Start(
	myPeer *LocalPeer,
	ctx0 context.Context,
	newCircuitCh <-chan *Circuit,

) (err0 error) {

	defer func() {
		vv("%v: (%v) end of node.Start() inside defer, about the return/finish", s.name, myPeer.ServiceName())
		s.halt.Done.Close()
	}()

	vv("%v: node.Start() top.", s.name)
	//vv("%v: ourID = '%v'; peerServiceName='%v';", s.name, myPeer.PeerID, myPeer.ServiceName())

	AliasRegister(myPeer.PeerID, myPeer.PeerID+" ("+myPeer.ServiceName()+")")

	done0 := ctx0.Done()

	for {
		//zz("%v: top of select", s.name) // client only seen once, since peer_test acts as cli
		select {
		// new Circuit connection arrives
		case ckt := <-newCircuitCh:

			vv("%v: got from newCircuitCh! service '%v' sees new peerURL: '%v'", s.name, myPeer.PeerServiceName, myPeer.URL())

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
						vv("%v: (ckt %v) ckt.Reads sees frag:'%s'", s.name, ckt.Name, frag)

						s.seen[AliasDecode(frag.FromPeerID)] = true
						if frag.Typ == CallPeerStartCircuit {

							outFrag := myPeer.U.NewFragment()
							outFrag.Payload = frag.Payload
							outFrag.FragSubject = "echo reply"
							outFrag.ServiceName = myPeer.ServiceName()
							//vv("%v: (ckt '%v') sending 'echo reply'='%v'", s.name, ckt.Name, frag)
							err := ckt.SendOneWay(outFrag, 0)
							panicOn(err)
						}

						if frag.FragSubject == "echo reply" {
							vv("we see echo reply")
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
