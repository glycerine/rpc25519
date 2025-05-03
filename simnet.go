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

	srv  *Server
	cli  *Client
	halt *idem.Halter // just srv.halt for now.

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
	// let client find the shared simnet in their cfg.
	cfg.simnetRendezvous.simnet = s
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

type pq struct {
	tree *rb.Tree
}

func (s *pq) peek() *mop {
	if s.tree.Len() == 0 {
		return nil
	}
	it := s.tree.Min()
	return it.Item().(*mop)
}

func (s *pq) pop() *mop {
	if s.tree.Len() == 0 {
		return nil
	}
	it := s.tree.Min()
	top := it.Item().(*mop)
	s.tree.DeleteWithIterator(it)
	return top
}

func (s *pq) add(op *mop) (added bool, it rb.Iterator) {
	added, it = s.tree.InsertGetIt(op)
	return
}

// order by when, then Frag(circuitID, fromID, sn...); try
// hard not to delete tickets with the same when,
// and even then we may have reason to keep
// the exact same ticket for a task at the same time;
// so use fop.sn too.
func newPQ() *pq {
	return &pq{
		tree: rb.NewTree(func(a, b rb.Item) int {
			av := a.(*mop)
			bv := b.(*mop)

			if av == bv {
				return 0 // points to same memory (or both nil)
			}
			if av == nil {
				// just a is nil; b is not. sort nils to the front
				// so they get popped and GC-ed sooner (and
				// don't become temporary memory leaks by sitting at the
				// back of the queue.x
				return -1
			}
			if bv == nil {
				return 1
			}
			// INVAR: neither av nor bv is nil
			if av.when.Before(bv.when) {
				return -1
			}
			if av.when.After(bv.when) {
				return 1
			}
			// Hopefully not, but just in case...
			// av.msg could be nil (so could bv.msg)
			if av.msg == nil && bv.msg == nil {
				return 0
			}
			if av.msg == nil {
				return -1
			}
			if bv.msg == nil {
				return 1
			}
			// INVAR: a.when == b.when

			if av.msg.HDR.CallID == bv.msg.HDR.CallID {
				if av.msg.HDR.Serial == bv.msg.HDR.Serial {
					return 0
				}
				if av.msg.HDR.Serial < bv.msg.HDR.Serial {
					return -1
				}
				return 1
			}
			if av.msg.HDR.CallID < bv.msg.HDR.CallID {
				return -1
			}
			return 1
		}),
	}
}
