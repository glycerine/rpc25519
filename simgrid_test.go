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
	//return

	bubbleOrNot(func() {
		//n := 20 // 20*19/2 = 190 tcp conn to setup. ok/green but 35 seconds.
		//n := 10 // 4.4 sec synctest
		n := 3
		gridCfg := &simGridConfig{
			ReplicationDegree: n,
			Timeout:           time.Second * 5,
		}

		cfg := NewConfig()
		// key setting under test here:
		cfg.ServerAutoCreateClientsToDialOtherServers = true
		cfg.UseSimNet = true
		cfg.ServerAddr = "127.0.0.1:0"
		cfg.QuietTestMode = true
		gridCfg.RpcCfg = cfg

		var nodes []*simGridNode
		for i := range n {
			name := fmt.Sprintf("grid_node_%v", i)
			nodes = append(nodes, newSimGridNode(name, gridCfg))
		}
		c := newSimGrid(gridCfg, nodes)
		c.Start()
		defer c.Close()

		for i, g := range nodes {
			_ = i
			select {
			case <-g.node.peersNeededSeen.Chan:
				//vv("i=%v all peer connections need have been seen(%v) by '%v': '%#v'", i, g.node.peersNeeded, g.node.name, g.node.seen.GetKeySlice())

				// failing test will just hang above.
				// we cannot really do case <-time.After(time.Minute) with faketime.
			}
		}
	})
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

	peersNeeded     int
	peersNeededSeen *idem.IdemCloseChan
}

func newNode2(srv *Server, name string, cfg *simGridConfig) *node2 {
	return &node2{
		peersNeeded:     cfg.ReplicationDegree - 1,
		peersNeededSeen: idem.NewIdemCloseChan(),
		cfg:             cfg,
		name:            name,
		seen:            NewMutexmap[string, bool](),
		// comms
		PushToPeerURL:          make(chan string),
		halt:                   srv.halt,
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
	RpcCfg            *Config
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
		err := n.Start(s.Cfg) // Server.Start()
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

func (s *simGridNode) Start(gridCfg *simGridConfig) error {
	//cfg.TCPonly_no_TLS = true

	cfg := gridCfg.RpcCfg
	//vv("making NewServer %v", s.name)
	s.srv = NewServer("srv_"+s.name, cfg)

	//vv("past NewServer()")
	serverAddr, err := s.srv.Start()
	panicOn(err)
	//vv("past s.srv.Start(); serverAddr = '%#v'/ '%v'", serverAddr, serverAddr)

	cfg.ClientDialToHostPort = serverAddr.String()
	//vv("serverAddr = '%#v' -> '%v'", serverAddr, cfg.ClientDialToHostPort)

	//vv("cfg.ClientDialToHostPort = '%v'", cfg.ClientDialToHostPort)

	s.node = newNode2(s.srv, s.name, s.cfg)

	err = s.srv.PeerAPI.RegisterPeerServiceFunc("simgrid", s.node.Start)
	panicOn(err)

	// appears that under simnet, the PeerAPI is not getting the server base address with simnet://127... in it.
	s.lpb, err = s.srv.PeerAPI.StartLocalPeer(context.Background(), "simgrid", nil)
	panicOn(err)
	s.node.lpb = s.lpb
	s.URL = s.lpb.URL()
	s.PeerID = s.lpb.PeerID

	//vv("simGridNode.Start() started '%v' as 'grid' with url = '%v'", s.name, s.URL)

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

	//vv("%v: node.Start() top. ourID = '%v'; peerServiceName='%v';", s.name, myPeer.PeerID, myPeer.ServiceName())

	AliasRegister(myPeer.PeerID, fmt.Sprintf("%v (%v %v)", myPeer.PeerID, myPeer.ServiceName(), s.name))
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
				//vv("%v: (ckt '%v') got incoming ckt", s.name, ckt.Name)

				defer func() {
					//vv("%v: (ckt '%v') defer running! finishing RemotePeer goro.", s.name, ckt.Name)
					ckt.Close(err0)
				}()

				//vv("%v: (ckt '%v') <- got new incoming ckt", s.name, ckt.Name) // grid-ckt
				//vv("incoming ckt has RemoteCircuitURL = '%v'", ckt.RemoteCircuitURL())
				//vv("incoming ckt has LocalCircuitURL = '%v'", ckt.LocalCircuitURL()) // seen 3x
				done := ctx.Done()

				for {
					select {
					case frag := <-ckt.Reads:
						//vv("%v: (ckt %v) ckt.Reads sees frag:'%s'", s.name, ckt.Name, frag) // not seen!!!

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
							err := ckt.SendOneWay(outFrag, 0)
							if err != nil {
								// typically a normal shutdown, don't freak.
								if err == ErrShutdown2 {
									return
								}
								panicOn(err)
							}
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
