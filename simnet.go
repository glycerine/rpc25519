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
	"sync/atomic"
	"time"

	"github.com/glycerine/idem"
	rb "github.com/glycerine/rbtree"
)

type SimNetConfig struct{}

var simnetLastSn int64

func simnetNextSn() int64 {
	return atomic.AddInt64(&simnetLastSn, 1)
}

type simnet struct {
	cfg       *Config
	simNetCfg *SimNetConfig
	netAddr   *SimNetAddr // satisfy uConn
	//netsim    *netsim
	srv       *Server
	cli       *Client
	isCli     bool
	halt      *idem.Halter
	msgSendCh chan *mop
	msgReadCh chan *mop
}

func (cfg *Config) newSimnet(simNetConfig *SimNetConfig, srv *Server) *simnet {

	// server creates simnet; must start server first.
	s := &simnet{
		halt:      srv.halt,
		cfg:       cfg,
		simNetCfg: simNetConfig,
		msgSendCh: make(chan *mop),
		msgReadCh: make(chan *mop),
		srv:       srv,
	}
	cfg.simnetRendezvous.simnet = s // let client find the shared simnet in their cfg.
	s.Start()
	return s
}

func (s *simnet) Start() {
	for {
		select {
		case <-s.halt.ReqStop.Chan:
			return
		}
	}
}

// receiveMessage reads a framed message from conn.
func (s *simnet) readMessage(conn uConn) (msg *Message, err error) {
	vv("top simnet.readMessage")

	read := s.newReadMsg()
	select {
	case s.msgReadCh <- read:
	case <-s.halt.ReqStop.Chan:
		return nil, ErrShutdown()
	}
	select {
	case <-read.proceed:
		msg = read.msg
	case <-s.halt.ReqStop.Chan:
		return nil, ErrShutdown()
	}
	return
}

type mop struct {
	sn int64

	senderLC int64
	readerLC int64

	dur  time.Duration // timer duration
	when time.Time     // when read for READS, when sent for SENDS?

	sorter uint64

	kind simkind
	msg  *Message

	sendmop *mop // for reads, which send did we get?
	readmop *mop // for sends, which read did we go to?

	pqit rb.Iterator

	// clients of scheduler wait on proceed.
	// timer fires, send delivered, read accepted by kernel
	proceed chan struct{}
}

func (s *simnet) newReadMsg() (op *mop) {
	op = &mop{
		sn:      simnetNextSn(),
		kind:    READ,
		proceed: make(chan struct{}),
	}
	return
}

func (s *simnet) newSendMsg(msg *Message) (op *mop) {
	op = &mop{
		msg:     msg,
		sn:      simnetNextSn(),
		kind:    SEND,
		proceed: make(chan struct{}),
	}
	// switch senderPeerID {
	// case ckt.LocalPeerID:
	// 	op.ToPeerID = ckt.RemotePeerID
	// case ckt.RemotePeerID:
	// 	op.ToPeerID = ckt.LocalPeerID
	// default:
	// 	panic("bad senderPeerID, not on ckt")
	// }
	return
}

func (s *simnet) sendMessage(conn uConn, msg *Message, timeout *time.Duration) error {
	vv("top simnet.sendMessage")

	send := s.newSendMsg(msg)
	select {
	case s.msgSendCh <- send:
	case <-s.halt.ReqStop.Chan:
		return ErrShutdown()
	}
	select {
	case <-send.proceed:
	case <-s.halt.ReqStop.Chan:
		return ErrShutdown()
	}
	return nil
}
