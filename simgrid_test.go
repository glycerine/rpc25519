package rpc25519

import (
	"context"
	"fmt"
	"time"

	"github.com/glycerine/idem"
	"testing"
)

// 702 is the Same as 202 in grid_test, but now under simnet.
func Test702_simnet_grid_peer_to_peer_works(t *testing.T) {

	//n := 20 // 20*19/2 = 190 tcp conn to setup. ok/green but 35 seconds.
	n := 3 // 2.7 sec
	cfg := &simGridConfig{
		ReplicationDegree: n,
		Timeout:           time.Second * 5,
	}

	var nodes []*simGridNode
	for i := range n {
		name := fmt.Sprintf("grid_node_%v", i)
		nodes = append(nodes, newSimGridNode(name, cfg))
	}
	c := newSimGrid(cfg, nodes)
	c.Start()
	defer c.Close()

	time.Sleep(1 * time.Second)

	for i, g := range nodes {
		_ = i
		//vv("i=%v has n.node.seen = %#v", i, g.node.seen)
		if g.node.seen.Len() != n-1 {
			t.Fatalf("expected n-1=%v nodes contacted, saw '%v'", n-1, g.node.seen.Len())
		}
	}
}

type node2 struct {
	cfg  *simGridConfig
	name string

	// comms
	PushToPeerURL          chan string
	halt                   *idem.Halter
	lpb                    *LocalPeer
	gotIncomingCktReadFrag chan *Fragment

	seen *Mutexmap[string, bool]
}

func newNode2(name string, cfg *simGridConfig) *node2 {
	return &node2{
		name: name,
		seen: NewMutexmap[string, bool](),
		// comms
		PushToPeerURL:          make(chan string),
		halt:                   idem.NewHalter(),
		gotIncomingCktReadFrag: make(chan *Fragment),
	}
}

type simGridNode struct {
	cfg  *simGridConfig
	node *node2

	rpccfg *Config
	srv    *Server
	name   string // same as srvServiceName

	lpb *LocalPeer

	URL    string
	PeerID string

	ckts []*Circuit
}

type simGridConfig struct {
	ReplicationDegree int
	Timeout           time.Duration
}

type simGrid struct {
	Cfg   *simGridConfig
	Nodes []*simGridNode
}

func newSimGrid(cfg *simGridConfig, nodes []*simGridNode) *simGrid {
	return &simGrid{Cfg: cfg, Nodes: nodes}
}

func (s *simGrid) Start() {
	for i, n := range s.Nodes {
		_ = i
		err := n.Start() // Server.Start()
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
			n0.lpb.NewCircuitCh <- ckt
			n0.ckts = append(n0.ckts, ckt)
			//vv("created ckt between n0 '%v' and n1 '%v': '%v'", n0.name, n1.name, ckt.String())
		}
	}
}

func (s *simGrid) Close() {
	for _, n := range s.Nodes {
		n.Close()
	}
}

func (s *simGridNode) Close() {
	s.srv.Close()
}

func newSimGridNode(name string, cfg *simGridConfig) *simGridNode {
	return &simGridNode{
		cfg:  cfg,
		name: name,
	}
}

func (s *simGridNode) Start() error {
	cfg := NewConfig()
	//cfg.TCPonly_no_TLS = true
	cfg.UseSimNet = true

	cfg.ServerAddr = "127.0.0.1:0"

	// key setting under test here:
	cfg.ServerAutoCreateClientsToDialOtherServers = true

	s.srv = NewServer("srv_"+s.name, cfg)
	s.rpccfg = cfg

	vv("past NewServer()")
	serverAddr, err := s.srv.Start()
	panicOn(err)
	vv("past s.srv.Start()")

	cfg.ClientDialToHostPort = serverAddr.String()

	s.node = newNode2(s.name, s.cfg)

	err = s.srv.PeerAPI.RegisterPeerServiceFunc("simgrid", s.node.Start)
	panicOn(err)

	s.lpb, err = s.srv.PeerAPI.StartLocalPeer(context.Background(), "simgrid", nil)
	panicOn(err)
	s.node.lpb = s.lpb
	s.URL = s.lpb.URL()
	s.PeerID = s.lpb.PeerID

	vv("simGridNode.Start() started '%v' as 'grid' with url = '%v'", s.name, s.URL)

	//grid_test sees grid_test.go:193 2025-05-06 02:40:13.362 +0000 UTC gridNode.Start() started 'grid_node_0' as 'grid' with url = 'tcp://127.0.0.1:65215/grid/Cog7DtZtQdA_EVt6JseOCdhJ3pxW'
	// simgrid_test: simgrid_test.go:172 2025-05-06 02:40:46.309 +0000 UTC simGridNode.Start() started 'grid_node_0' as 'grid' with url = '/simgrid/w5L6aGyAmYcVhAC9K2EZhLDMS5w_'
	return nil
}

func (s *node2) Start(
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

						//s.gotIncomingCktReadFrag <- frag
						//vv("%v: (ckt %v) past s.gotIncomingCktReadFrag <- frag. frag:'%s'", s.name, ckt.Name, frag)

						if frag.Typ == CallPeerStartCircuit {

							outFrag := myPeer.U.NewFragment()
							outFrag.Payload = frag.Payload
							outFrag.FragSubject = "start reply"
							outFrag.ServiceName = myPeer.ServiceName()
							err := ckt.SendOneWay(outFrag, 0)
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
