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

	"github.com/glycerine/idem"
)

type SimNetConfig struct{}

type SimNetAddr struct { // net.Addr
	network string
}

func (s *SimNetAddr) Network() string { // name of the network (for example, "tcp", "udp")
	return "simnet"
}
func (s *SimNetAddr) String() string { // string form of address (for example, "192.0.2.1:25", "[2001:db8::1]:80")
	return s.network
}

func (s *Server) runSimNetServer(serverAddr string, boundCh chan net.Addr) {
	defer func() {
		s.halt.Done.Close()
		//vv("exiting Server.runQUICServer('%v')", quicServerAddr) // seen, yes, on shutdown test.
	}()

	netAddr := &SimNetAddr{network: "simnet@" + serverAddr}

	s.mut.Lock()
	AliasRegister(serverAddr, serverAddr+" (simnet_server: "+s.name+")")
	s.haltSimNet = idem.NewHalter()
	s.mut.Unlock()

	if boundCh != nil {
		select {
		case boundCh <- netAddr:
		case <-time.After(100 * time.Millisecond):
		}
	}

	var simNetConfig *SimNetConfig
	for {
		if simNetConfig != nil {
			vv("shut down old simnet first??")
		}
		select {
		case <-s.halt.ReqStop.Chan:
			return
		case <-s.haltSimNet.ReqStop.Chan:
			return
		case simNetConfig := <-s.StartSimNet:
			vv("got simNetConfig request (orig serverAddr: '%v') to start a sim net: '%#v'", serverAddr, simNetConfig)
		}
		conn := s.cfg.newSimtime(simNetConfig)
		conn.netAddr = netAddr
		pair := s.newRWPair(conn)
		go pair.runSendLoop(conn)
		go pair.runReadLoop(conn)

	}
}

// simtime implements the same workspace/blabber interface
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
type simtime struct {
	cfg       *Config
	simNetCfg *SimNetConfig
	netAddr   *SimNetAddr
	netsim    *netsim
	//ckt    *Circuit // wants Fragments not Messages, of course.
}

func (cfg *Config) newSimtime(simNetConfig *SimNetConfig) *simtime {
	return &simtime{
		cfg:       cfg,
		simNetCfg: simNetConfig,
		netAddr:   &SimNetAddr{},
	}
}

// not actually used though
func (s *simtime) Write(p []byte) (n int, err error)   { return }
func (s *simtime) SetWriteDeadline(t time.Time) error  { return nil }
func (s *simtime) Read(data []byte) (n int, err error) { return }
func (s *simtime) SetReadDeadline(t time.Time) error   { return nil }
func (s *simtime) Close() error                        { return nil }
func (s *simtime) LocalAddr() net.Addr                 { return s.netAddr }
func (s *simtime) RemoteAddr() net.Addr {
	return s.netAddr
}
func (s *simtime) SetDeadline(t time.Time) error { return nil }

//func (s *simtime) SetReadDeadline(t time.Time) error  { return nil }
//func (s *simtime) SetWriteDeadline(t time.Time) error { return nil }

// receiveMessage reads a framed message from conn.
func (w *simtime) readMessage(conn uConn) (msg *Message, err error) {
	panic("TODO simtime.readMessage")
	return
}

func (w *simtime) sendMessage(conn uConn, msg *Message, timeout *time.Duration) error {

	return nil
}
