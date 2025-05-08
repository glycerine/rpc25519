package rpc25519

import (
	"fmt"
	"net"
	//"sync/atomic"
	"time"
)

func (s *Server) runSimNetServer(serverAddr string, boundCh chan net.Addr, simNetConfig *SimNetConfig) {
	vv("top of runSimnetServer, serverAddr = '%v'; name='%v'", serverAddr, s.name)
	defer func() {
		r := recover()
		//vv("defer running, end of runSimNetServer() for '%v' r='%v'", s.name, r)
		s.halt.ReqStop.Close()
		s.halt.Done.Close()
		//vv("exiting Server.runSimNetServer('%v')", serverAddr) // seen, yes, on shutdown test.
		if r != nil {
			panic(r)
		}
	}()

	s.mut.Lock()
	AliasRegister(serverAddr, serverAddr+" (simnet_server: "+s.name+")")
	s.mut.Unlock()

	// satisfy uConn interface; don't crash cli/tests that check
	netAddr := &SimNetAddr{network: "simnet", addr: serverAddr, name: s.name, isCli: false}

	// idempotent, so all new servers can try;
	// only the first will boot it up (still pass s for s.halt);
	// second and subsequent will get back the cfg.simnetRendezvous.singleSimnet
	// per config shared simnet.
	simnet := s.cfg.bootSimNetOnServer(simNetConfig, s)

	// sets s.simnode, s.simnet
	serverNewConnCh, err := simnet.registerServer(s, netAddr)
	if err != nil {
		if err == ErrShutdown2 {
			//vv("simnet_server sees shutdown in progress")
			return
		}
		panicOn(err)
	}
	if serverNewConnCh == nil {
		panic(fmt.Sprintf("%v got a nil serverNewConnCh, should not be allowed!", s.name))
	}

	defer func() {
		simnet.alterNode(s.simnode, SHUTDOWN)
		//vv("simnet.alterNode(s.simnode, SHUTDOWN) done for %v", s.name)
	}()

	s.mut.Lock() // avoid data races
	addrs := netAddr.Network() + "://" + netAddr.String()
	s.boundAddressString = addrs
	AliasRegister(addrs, addrs+" (server: "+s.name+")")
	s.mut.Unlock()

	if boundCh != nil {
		select {
		case boundCh <- netAddr:
			// like the srv comment, this exception to
			// using the simnet TimeAfter is fine,
			// as obvious it gets created just below and
			// the server is not up yet.
		case <-time.After(100 * time.Millisecond):
		}
	}

	for {
		select { // wait for a new client to connect
		case conn := <-serverNewConnCh:
			//s.simnode = conn.local

			//vv("%v simnet server got new conn '%#v', about to start read/send loops", netAddr, conn) // not seen
			pair := s.newRWPair(conn)
			go pair.runSendLoop(conn)
			go pair.runReadLoop(conn)

		case <-s.halt.ReqStop.Chan:
			return
		}
	}
}

// not actually used though, much.
func (s *simnetConn) Write(p []byte) (n int, err error)   { return }
func (s *simnetConn) SetWriteDeadline(t time.Time) error  { return nil }
func (s *simnetConn) Read(data []byte) (n int, err error) { return }
func (s *simnetConn) SetReadDeadline(t time.Time) error   { return nil }
func (s *simnetConn) Close() error                        { return nil }
func (s *simnetConn) LocalAddr() net.Addr                 { return s.local.netAddr }
func (s *simnetConn) RemoteAddr() net.Addr {
	return s.remote.netAddr
}
func (s *simnetConn) SetDeadline(t time.Time) error { return nil }
