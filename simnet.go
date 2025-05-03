package rpc25519

import (
	//"context"
	//"crypto/ed25519"
	//"crypto/tls"
	//"fmt"
	//"io"
	//"log"
	//"net"
	//"strings"
	"time"

	"github.com/glycerine/idem"
)

type SimNetConfig struct{}

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
	netAddr   *SimNetAddr // satisfy uConn
	netsim    *netsim
	srv       *Server
	cli       *Client
	isCli     bool
	halt      *idem.Halter
	msgSendCh chan *fop
	msgReadCh chan *fop
}

func (cfg *Config) newSimnet(simNetConfig *SimNetConfig, cli *Client, srv *Server, halt *idem.Halter) *simnet {
	s := &simnet{
		halt:      halt,
		cfg:       cfg,
		simNetCfg: simNetConfig,
		msgSendCh: make(chan *fop),
		msgReadCh: make(chan *fop),
		cli:       cli,
		srv:       srv,
		isCli:     cli != nil,
	}
	s.Start()
	return s
}
func (s *simnet) Start() {

}

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
