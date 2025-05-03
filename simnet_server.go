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
		vv("exiting Server.runSimNetServer('%v')", serverAddr) // seen, yes, on shutdown test.
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
		s.simnet = s.cfg.newSimnet(simNetConfig, nil, s)
		s.simnet.netAddr = netAddr
		conn := s.simnet

		pair := s.newRWPair(conn)
		go pair.runSendLoop(conn)
		go pair.runReadLoop(conn)
	}
}

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
type simnet struct {
	cfg       *Config
	simNetCfg *SimNetConfig
	netAddr   *SimNetAddr
	netsim    *netsim
	srv       *Server
	cli       *Client
	isCli     bool
	//ckt    *Circuit // wants Fragments not Messages, of course.
	msgSendCh chan *fop
	msgReadCh chan *fop
}

func (cfg *Config) newSimnet(simNetConfig *SimNetConfig, cli *Client, srv *Server) *simnet {
	return &simnet{
		cfg:       cfg,
		simNetCfg: simNetConfig,
		msgSendCh: make(chan *fop),
		msgReadCh: make(chan *fop),
		cli:       cli,
		srv:       srv,
		isCli:     cli != nil,
	}
}

// not actually used though
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

// receiveMessage reads a framed message from conn.
func (s *simnet) readMessage(conn uConn) (msg *Message, err error) {
	panic("TODO simnet.readMessage")
	return
}

func (s *simnet) newSendMsg(msg *Message) (op *fop) {
	op = &fop{
		note:       note,
		ckt:        ckt,
		frag:       frag,
		sn:         netsimNextSn(),
		kind:       SEND,
		proceed:    make(chan struct{}),
		FromPeerID: senderPeerID,
	}
	switch senderPeerID {
	case ckt.LocalPeerID:
		op.ToPeerID = ckt.RemotePeerID
	case ckt.RemotePeerID:
		op.ToPeerID = ckt.LocalPeerID
	default:
		panic("bad senderPeerID, not on ckt")
	}
	return
}

func (s *simnet) sendMessage(conn uConn, msg *Message, timeout *time.Duration) error {

	send := s.newSendMsg(msg)
	select {
	case s.netsim.msgSendCh <- send:
	case <-s.srv.halt.ReqStop.Chan:
		return ErrShutdown()
	}
	select {
	case <-send.proceed:
	case <-s.srv.halt.ReqStop.Chan:
		return ErrShutdown()
	}
	return nil
}
