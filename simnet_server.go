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
		vv("exiting Server.runSimNetServer('%v')", serverAddr) // seen, yes, on shutdown test.
	}()

	netAddr := &SimNetAddr{network: "simnet@" + serverAddr}

	s.mut.Lock()
	AliasRegister(serverAddr, serverAddr+" (simnet_server: "+s.name+")")
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
		case simNetConfig := <-s.StartSimNet:
			vv("got simNetConfig request (orig serverAddr: '%v') to start a sim net: '%#v'", serverAddr, simNetConfig)
		}
		s.simnet = s.cfg.newSimnet(simNetConfig, nil, s, s.halt)
		s.simnet.netAddr = netAddr
		conn := s.simnet

		pair := s.newRWPair(conn)
		go pair.runSendLoop(conn)
		go pair.runReadLoop(conn)
	}
}

// not actually used though, much.
func (s *simnet) Write(p []byte) (n int, err error)   { return }
func (s *simnet) SetWriteDeadline(t time.Time) error  { return nil }
func (s *simnet) Read(data []byte) (n int, err error) { return }
func (s *simnet) SetReadDeadline(t time.Time) error   { return nil }
func (s *simnet) Close() error                        { return nil }
func (s *simnet) LocalAddr() net.Addr                 { return s.netAddr }
func (s *simnet) RemoteAddr() net.Addr {
	return s.netAddr
}
func (s *simnet) SetDeadline(t time.Time) error { return nil }

//func (s *simnet) SetReadDeadline(t time.Time) error  { return nil }
//func (s *simnet) SetWriteDeadline(t time.Time) error { return nil }
