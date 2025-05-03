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

func (cfg *Config) newSimnet(simNetConfig *SimNetConfig, cli *Client, srv *Server, halt *idem.Halter) *simnet {
	s := &simnet{
		halt:      halt,
		cfg:       cfg,
		simNetCfg: simNetConfig,
		msgSendCh: make(chan *mop),
		msgReadCh: make(chan *mop),
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

	send := s.newSendMsg(msg)
	select {
	case s.msgSendCh <- send:
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
