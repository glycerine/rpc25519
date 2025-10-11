package rpc25519

import (
	//"bytes"
	"context"
	"fmt"
	//"os"
	//"path/filepath"
	"runtime"
	//"strings"
	"sync"
	"time"

	"github.com/glycerine/idem"
	"testing"
)

// 702 is the Same as 202 in grid_test, but now under simnet.
func Test702_simnet_grid_peer_to_peer_works(t *testing.T) {
	//return

	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	// regular, no wasm: m0 Alloc:2349128 HeapSys:7897088
	//vv("m0 Alloc:%v HeapSys:%v", int(m0.Alloc), int(m0.HeapSys))
	defer func() {
		runtime.ReadMemStats(&m1)
		// regular, no wasm: m0 Alloc:1837775728 HeapSys:1844772864
		//vv("m1 Alloc:%v HeapSys:%v", int(m1.Alloc), int(m1.HeapSys))
		//vv("diff Alloc:%v MB HeapSys:%v MB", int(m1.Alloc-m0.Alloc)/(1<<20), int(m1.HeapSys-m0.HeapSys)/(1<<20))
		// non wasm: diff Alloc:1750 MB HeapSys:1751 MB
	}()

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
		//vv("end of 702")
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

	ckts *syncomap[string, *Circuit]
	seen *Mutexmap[string, *Circuit]

	peersNeeded     int
	peersNeededSeen *idem.IdemCloseChan

	load           *gridLoadTestTicket
	gridLoadTestCh chan *gridLoadTestTicket

	// leave nil initially, then
	// load.proceed for a moment
	// to start the load test.
	startLoadTestCh chan struct{}
	// leave nil initially
	timeToSendLoadTimer  *SimTimer
	timeToSendLoadTimerC <-chan time.Time
}

type gridLoadTestTicket struct {
	mut sync.Mutex

	nmsgSend        int
	nmsgRead        int
	wantRead        int
	wantSendPerPeer int
	npeer           int

	sendEvery time.Duration // how often to send.

	started bool

	// close done when both reads and sends are done.
	readsDone bool
	sendsDone bool
	done      *idem.IdemCloseChan

	ready   chan struct{} // acknowledge the load test
	proceed chan struct{} // begin load test
}

func newGridLoadTestTicket(npeer, wantRead, wantSendPerPeer int, sendEvery time.Duration) *gridLoadTestTicket {
	return &gridLoadTestTicket{
		npeer:           npeer,
		sendEvery:       sendEvery,
		wantSendPerPeer: wantSendPerPeer,
		wantRead:        wantRead,
		ready:           make(chan struct{}),
		// let test make just one proceed for all.
		done: idem.NewIdemCloseChan(),
	}
}

func newNode2(srv *Server, name string, cfg *simGridConfig) *node2 {
	return &node2{
		gridLoadTestCh:  make(chan *gridLoadTestTicket),
		peersNeeded:     cfg.ReplicationDegree - 1,
		peersNeededSeen: idem.NewIdemCloseChan(),
		cfg:             cfg,
		name:            name,
		seen:            NewMutexmap[string, *Circuit](),
		ckts:            newSyncomap[string, *Circuit](),
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

	hist *gridhistory
}

type simGrid struct {
	Cfg   *simGridConfig
	Nodes []*simGridNode
	net   *Simnet // for halting with net.Close()
}

func newSimGrid(cfg *simGridConfig, nodes []*simGridNode) *simGrid {
	cfg.hist = newGridHistory(nodes)
	return &simGrid{
		Cfg:   cfg,
		Nodes: nodes,
	}
}

func (s *simGrid) Start() {
	//vv("simGrid.Start on goro %v", GoroNumber())
	for i, n := range s.Nodes {
		_ = i
		err := n.Start(s) // Server.Start()
		panicOn(err)
		if i == 0 {
			s.net = s.Cfg.RpcCfg.GetSimnet()
			if s.net == nil && faketime {
				panic("nil simnet from GetSimnet() arg")
			}
			//vv("faketime = %v; simGrid.net = %p", faketime, s.net)
		}
	}

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
			ckt, _, _, err := n0.lpb.NewCircuitToPeerURL("grid-ckt", n1.URL, nil, 0)
			panicOn(err)
			n0.lpb.NewCircuitCh <- ckt
			n0.ckts = append(n0.ckts, ckt)
			//vv("created ckt between n0 '%v' and n1 '%v': '%v'", n0.name, n1.name, ckt.String())
		}
	}
}

func (s *simGrid) Close() {
	if s.net != nil {
		s.net.Close()
	}
	for _, n := range s.Nodes {
		n.Close()
	}
}

func (s *simGridNode) Close() {
	//vv("simGridNode.Close: tree:\n %v", s.srv.halt.RootString())
	s.srv.Close() // calls s.halt.StopTreeAndWaitTilDone(500*time.Millisecond, nil, nil); then s.halt.ReqStop.Close() when that finishes. then closes autoclients.
}

func newSimGridNode(name string, cfg *simGridConfig) *simGridNode {
	return &simGridNode{
		cfg:  cfg,
		name: name,
	}
}

func (s *simGridNode) Start(grid *simGrid) error {
	//cfg.TCPonly_no_TLS = true

	gridCfg := grid.Cfg
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
	peerName := s.node.name //+ "_simgrid_test_StartLocalPeer"
	s.lpb, err = s.srv.PeerAPI.StartLocalPeer(context.Background(), "simgrid", "", nil, peerName, false)
	panicOn(err)
	s.node.lpb = s.lpb
	s.URL = s.lpb.URL()
	s.PeerID = s.lpb.PeerID

	AliasRegister(s.PeerID, s.name)
	//vv("simGridNode.Start() started '%v' as 'grid' with url = '%v'", s.name, s.URL)

	return nil
}

func (s *node2) Start(
	lpb *LocalPeer,
	ctx0 context.Context,
	newCircuitCh <-chan *Circuit,

) (err0 error) {

	defer func() {
		//vv("%v: (%v) end of node.Start() inside defer, about the return/finish. reason=%v", s.name, lpb.ServiceName())
		s.halt.Done.Close()
	}()

	//vv("%v: node.Start() top. ourID = '%v'; peerServiceName='%v';", s.name, lpb.PeerID, lpb.ServiceName())

	me := s.name

	AliasRegister(lpb.PeerID, me)
	//AliasRegister(lpb.PeerID, fmt.Sprintf("%v (%v %v)", lpb.PeerID, lpb.ServiceName(), s.name))

	done0 := ctx0.Done()

	for {
		//vv("%v: top of select", s.name) // client only seen once, since peer_test acts as cli
		select {

		case <-s.timeToSendLoadTimerC:
			s.timeToSendLoadTimer.Discard()

			if s.load.done.IsClosed() {
				continue
			}
			ckts_cached := s.ckts.cached()

			goal := s.load.wantSendPerPeer * len(ckts_cached)

			s.load.mut.Lock()
			nmsgSent := s.load.nmsgSend
			wantSendPerPeer := s.load.wantSendPerPeer
			s.load.mut.Unlock()
			if nmsgSent == goal {
				// no more sends needed
				continue
			}
			if nmsgSent > goal {
				panic(fmt.Sprintf("should never seen nmsgSent(%v) > [wantSendPerPeer(%v) * peers(%v) = %v]", nmsgSent, wantSendPerPeer, len(ckts_cached), goal))
			}
			//vv("%v timeToSendLoadTimerC went off. PRE: nmsgSend(%v); wantSendPerPeer(%v)", me, s.load.nmsgSend, s.load.wantSendPerPeer)
			//if s.load.k != s.ckts.Len() {
			//	panic(fmt.Sprintf("short on destinations. s.load.k=%v while s.ckts.Len = %v", s.load.k, s.ckts.Len()))
			//}
			sends := 0
			for _, ckt := range ckts_cached {
				frag := lpb.NewFragment()
				frag.FragSubject = "load test"
				ckt.val.SendOneWay(frag, -1, 0)
				s.cfg.hist.addSend(me, AliasDecode(ckt.key), frag)
				sends++
				if sends%2000 == 0 {
					// show some progress
					//vv("%v send to %v", me, AliasDecode(ckt.key))
				}
			}
			if s.loadDone(me, sends, 0) {
				continue
			}
			ti := lpb.U.NewTimer(s.load.sendEvery)
			if ti == nil {
				//vv("NewTimer nil, presummably shutting down...")
				return // rather than derefer the nil pointer below.
			}
			s.timeToSendLoadTimer = ti
			s.timeToSendLoadTimerC = ti.C

		case <-s.startLoadTestCh:
			s.load.started = true
			s.startLoadTestCh = nil

			ti := lpb.U.NewTimer(s.load.sendEvery)
			s.timeToSendLoadTimer = ti
			s.timeToSendLoadTimerC = ti.C

		case load := <-s.gridLoadTestCh:
			s.load = load
			s.startLoadTestCh = load.proceed
			close(s.load.ready)

		// new Circuit connection arrives
		case ckt := <-newCircuitCh:

			//vv("%v: got from newCircuitCh! service '%v' sees new peerURL: '%v'", s.name, lpb.PeerServiceName, lpb.URL())
			s.ckts.set(ckt.RemotePeerID, ckt)

			// talk to this peer on a separate goro if you wish:
			go func(ckt *Circuit) (err0 error) {
				s.cfg.RpcCfg.GetSimnet().NewGoro(s.name)

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

						s.seen.Set(AliasDecode(frag.FromPeerID), ckt)

						peersSeen := s.seen.Len()
						if peersSeen >= s.peersNeeded {
							s.peersNeededSeen.Close()
						}

						//s.gotIncomingCktReadFrag <- frag
						//vv("%v: (ckt %v) past s.gotIncomingCktReadFrag <- frag. frag:'%s'", s.name, ckt.Name, frag)

						if frag.Typ == CallPeerStartCircuit {

							outFrag := lpb.NewFragment()
							outFrag.Payload = frag.Payload
							outFrag.FragSubject = "start reply"
							_, err := ckt.SendOneWay(outFrag, 0, 0)
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

						if frag.FragSubject == "load test" {
							//vv("we see load test frag")
							if s.load.done.IsClosed() {
								vv("s.cfg.hist = %v", s.cfg.hist)
								panic(fmt.Sprintf("arg. got load test frag after we are finished. me = %v; frag='%v'", me, frag)) // seen. why??? on 9 x 100,100, 1sec. double delivery?
							}
							if s.load == nil {
								panic("arg. got load test frag without current load test")
							}
							s.cfg.hist.addRead(me, AliasDecode(ckt.RemotePeerID), frag)
							if s.loadDone(me, 0, 1) {
								continue
							}
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

func (s *node2) loadDone(me string, addSends, addReads int) bool {
	s.load.mut.Lock()
	defer s.load.mut.Unlock()

	if s.load.done.IsClosed() {
		return true
	}

	s.load.nmsgRead += addReads
	s.load.nmsgSend += addSends

	if !s.load.readsDone && s.load.nmsgRead == s.load.wantRead {
		//vv("%v peer done full reads %v. wantRead: %v", me, s.load.nmsgRead, s.load.wantRead)
		s.load.readsDone = true
	}

	if !s.load.sendsDone && s.load.nmsgSend == s.load.wantSendPerPeer*s.load.npeer {
		//vv("%v peer done full sends %v. wantSend = %v", me, s.load.nmsgSend, s.load.wantSendPerPeer*s.load.npeer)
		s.load.sendsDone = true
	}

	if s.load.readsDone && s.load.sendsDone {
		//vv("node %v closing load.done", me)
		s.load.done.Close()
		return true
	}
	return false
}

func Test707_simnet_grid_does_not_lose_messages(t *testing.T) {
	//return
	// At one point, tube raft grid had sporadic
	// read loss resulting in a hung client. It
	// could have been at the tube layer, but to
	// be extra careful, here we stress test a
	// simnet grid, lower level, and see that we
	// deliver everything sent with
	// no faults injected.

	// We later added a determinism/reproducible
	// test check below with the call to
	// panicIfFinalHashDifferent(xorderPath).

	loadtest := func(prevSnap *SimnetSnapshot, nNodes, wantSendPerPeer int, sendEvery time.Duration, xorderPath string) (snap *SimnetSnapshot) {

		nPeer := nNodes - 1
		wantRead := nPeer * wantSendPerPeer

		// realtime timestamp diffs will cause false
		// alarms, so only under bubble.
		onlyBubbled(t, func() {

			n := nNodes
			gridCfg := &simGridConfig{
				ReplicationDegree: n,
				Timeout:           time.Second * 5,
			}
			//histShown := false
			//defer func() {
			//	if !histShown {
			//		vv("in defer, history: %v", gridCfg.hist)
			//	}
			//}()

			cfg := NewConfig()
			// key setting under test here:
			cfg.ServerAutoCreateClientsToDialOtherServers = true
			cfg.UseSimNet = true
			//cfg.UseSimNet = faketime
			cfg.ServerAddr = "127.0.0.1:0"
			cfg.QuietTestMode = true
			cfg.repeatTrace = prevSnap
			cfg.repeatTraceViolatedOutpath = xorderPath
			gridCfg.RpcCfg = cfg
			cfg.SimnetGOMAXPROCS = 8

			var nodes []*simGridNode
			for i := range n {
				name := fmt.Sprintf("grid_node_%v", i)
				nodes = append(nodes, newSimGridNode(name, gridCfg))
			}
			c := newSimGrid(gridCfg, nodes)
			c.Start()
			//vv("c.net = %p", c.net) // yes, new simnet each time.
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

			npeer := nNodes - 1
			var loads []*gridLoadTestTicket
			proceed := make(chan struct{})
			for _, g := range nodes {
				lo := newGridLoadTestTicket(npeer, wantRead, wantSendPerPeer, sendEvery)
				lo.proceed = proceed
				loads = append(loads, lo)
				g.node.gridLoadTestCh <- lo
				<-lo.ready
			}
			close(proceed)
			for i, g := range nodes {
				_ = g
				<-loads[i].done.Chan
			}

			//time.Sleep(time.Second)
			//vv("after load all done, history: %v", gridCfg.hist)
			//histShown = true

			for i := range nodes {
				gotSent := gridCfg.hist.sentBy(nodes[i].name)
				if gotSent != wantSendPerPeer*nPeer {
					t.Fatalf("node %v sent %v but wanted %v", i, gotSent, wantSendPerPeer*nPeer)
				}
				gotRead := gridCfg.hist.readBy(nodes[i].name)
				if gotRead != wantRead {
					t.Fatalf("node %v read %v but wanted %v", i, gotRead, wantRead)
				}
			}

			snap = c.net.GetSimnetSnapshot()
			//vv("snap.Xfinorder len = '%v'; Xhash='%v'", len(snap.Xfinorder), snap.Xhash) // 53343
			snap.ToFile(xorderPath)
		}) // end bubbleOrNot
		return
	} // end loadtest func definition

	// 15 nodes, 100 frag: 60 seconds testtime for realtime. 70sec faketime
	// 21 nodes, 1k frag: 105s test-time under simnet/synctest-faketime.
	const nNode1 = 7
	const wantSendPerPeer1 = 1000
	sendEvery1 := time.Millisecond
	xorderPath := homed("~/rpc25519/snap707")
	removeAllFilesWithPrefix(xorderPath)
	snap0 := loadtest(nil, nNode1, wantSendPerPeer1, sendEvery1, xorderPath)

	// more rigorous checking, not really
	// accurate/reliable for the batch though.
	//snap1 := loadtest(snap0, nNode1, wantSendPerPeer1, sendEvery1, xorderPath)
	snap1 := loadtest(nil, nNode1, wantSendPerPeer1, sendEvery1, xorderPath)
	_, _ = snap0, snap1
	err := snapFilesDifferent(xorderPath, false)
	if err != nil {
		panic(err)
	}
	return

	//loadtest(2, 1, 1, time.Second, "707 loadtest 1")
	//vv("done with first")

	//loadtest(9, 100, 100, time.Second, "707 loadtest 2")
	// 5 nodes, 100 msgs = 1.7sec test-time under faketime, 1.1s under realtime.
	//const nNode = 7
	//const wantSendPerPeer = 100_000
	//sendEvery := time.Millisecond
	//loadtest(nil, nNode, wantSendPerPeer, sendEvery, "707 loadtest 2")

	//vv("done with second loadtest")

	//loadtest(5, 1, 1, time.Second, "707 loadtest 3")

	//vv("end of 707")
}

func TestDiffPos(t *testing.T) {
	if pos, _ := diffpos("ab", "ab"); pos != -1 {
		panic("expected -1")
	}
	if pos, _ := diffpos("ab", "abc"); pos != 2 {
		panic("expected 2")
	}
	if pos, _ := diffpos("abc", "ab"); pos != 2 {
		panic("expected 2")
	}
	if pos, _ := diffpos("ac", "ab"); pos != 1 {
		panicf("expected 1, got %v", pos)
	}
}
