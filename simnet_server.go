package rpc25519

import (
	"net"
	"time"
)

func (s *Server) runSimNetServer(serverAddr string, boundCh chan net.Addr, simNetConfig *SimNetConfig) {
	//vv("top of runSimnetServer")
	defer func() {
		r := recover()
		//vv("defer running, end of runSimNetServer() r='%v'", r)
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

	//vv("about to call s.cfg.newSimNetOnServer()")
	serverNewConnCh := s.cfg.newSimNetOnServer(simNetConfig, s, netAddr)

	for {
		select {
		case conn := <-serverNewConnCh:
			s.simnet = conn.net
			s.simnode = conn.local
			s.simconn = conn

			vv("simnet server '%v': got new conn '%#v', about to start read/send loops", netAddr, conn)
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
