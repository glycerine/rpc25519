package rpc25519

import (
	//"context"
	//"crypto/ed25519"
	//"crypto/tls"
	//"fmt"
	//"io"
	//"log"
	"net"
	//"strings"
	"time"
	//"github.com/glycerine/idem"
)

// simnet implements the same workspace/blabber interface
// so we can plug in
// netsim and do comms via channels for testing/synctest
// based accelerated timeout testing.
//
// Note that uConn and its Write/Read are
// not actually used; channel sends/reads replace them.
// We still need a dummy uConn to pass to
// readMessage() and sendMessage() which are the
// interception points for the simulated network.
//
// The blabber does check if the uConn is *simnet, and
// configures itself to call through it if present.

type SimNetAddr struct { // net.Addr
	network string
}

func (s *SimNetAddr) Network() string { // name of the network (for example, "tcp", "udp")
	return "simnet"
}
func (s *SimNetAddr) String() string { // string form of address (for example, "192.0.2.1:25", "[2001:db8::1]:80")
	return s.network
}

func (s *Server) runSimNetServer(serverAddr string, boundCh chan net.Addr, simNetConfig *SimNetConfig) {
	vv("top of runSimnetServer")
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
	netAddr := &SimNetAddr{network: "srv simnet@" + serverAddr}

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
	s.simnet = s.cfg.newSimNetOnServer(simNetConfig, s)
	s.simnet.netAddr = netAddr

	conn := &simnetConn{
		isCli:   false,
		simnet:  s.simnet,
		netAddr: netAddr,
	}
	//vv("simnet server: about to start read/send loops")
	pair := s.newRWPair(conn)
	go pair.runSendLoop(conn)
	go pair.runReadLoop(conn)

}

// distinguish cli from srv
type simnetConn struct {
	isCli   bool
	simnet  *simnet
	netAddr *SimNetAddr
}

// not actually used though, much.
func (s *simnetConn) Write(p []byte) (n int, err error)   { return }
func (s *simnetConn) SetWriteDeadline(t time.Time) error  { return nil }
func (s *simnetConn) Read(data []byte) (n int, err error) { return }
func (s *simnetConn) SetReadDeadline(t time.Time) error   { return nil }
func (s *simnetConn) Close() error                        { return nil }
func (s *simnetConn) LocalAddr() net.Addr                 { return s.netAddr }
func (s *simnetConn) RemoteAddr() net.Addr {
	return s.netAddr
}
func (s *simnetConn) SetDeadline(t time.Time) error { return nil }

//func (s *simnetConn) SetReadDeadline(t time.Time) error  { return nil }
//func (s *simnetConn) SetWriteDeadline(t time.Time) error { return nil }
