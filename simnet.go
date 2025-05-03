package rpc25519

import (
	//"context"
	//"crypto/ed25519"
	//"crypto/tls"
	"fmt"
	//"io"
	//"log"
	//"net"
	//"strings"
	mathrand2 "math/rand/v2"
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
	pq     *pq
	nextPQ *time.Timer

	tick time.Duration
	hop  time.Duration

	seed [32]byte
	rng  *mathrand2.ChaCha8

	cfg       *Config
	simNetCfg *SimNetConfig
	netAddr   *SimNetAddr // satisfy uConn

	srv      *Server
	cli      *Client
	cliReady chan *Client
	halt     *idem.Halter // just srv.halt for now.

	msgSendCh chan *mop
	msgReadCh chan *mop
	addTimer  chan *mop

	sentFromCli []*mop
	sentFromSrv []*mop
}

func (cfg *Config) newSimnetOnServer(simNetConfig *SimNetConfig, srv *Server) *simnet {

	var seed [32]byte

	// server creates simnet; must start server first.
	s := &simnet{
		tick:      time.Second,
		hop:       time.Second,
		cfg:       cfg,
		srv:       srv,
		halt:      srv.halt,
		cliReady:  make(chan *Client),
		simNetCfg: simNetConfig,
		msgSendCh: make(chan *mop),
		msgReadCh: make(chan *mop),
		addTimer:  make(chan *mop),
		nextPQ:    time.NewTimer(0),
		pq:        newPQ(),
		seed:      seed,
		rng:       mathrand2.NewChaCha8(seed),
	}
	// let client find the shared simnet in their cfg.
	cfg.simnetRendezvous.simnet = s
	s.Start()
	return s
}

type mopkind int

const (
	TIMER mopkind = 1
	SEND  mopkind = 2
	READ  mopkind = 3
)

func (k mopkind) String() string {
	switch k {
	case TIMER:
		return "TIMER"
	case SEND:
		return "SEND"
	case READ:
		return "READ"
	default:
		return fmt.Sprintf("unknown mopkind %v", int(k))
	}
}

// leave the cli/srv setup in place to avoid the
// startup overhead for every time, and test
// at the peer/ckt/frag level a particular test scenario.
type scenario struct {
}

func (s *simnet) showQ() {
	i := 0
	tsPrintfMut.Lock()
	fmt.Printf("\n ------- PQ --------\n")
	for it := s.pq.tree.Min(); it != s.pq.tree.Limit(); it = it.Next() {
		op := it.Item().(*fop)
		fmt.Printf("pq[%2d] = %v\n", i, op)
		i++
	}
	if i == 0 {
		fmt.Printf("empty PQ\n")
	}
	tsPrintfMut.Unlock()
}

// Message operation
type mop struct {
	sn int64

	// number of times handleSend() has seen this mop.
	seen int

	originCli bool

	senderLC int64
	readerLC int64

	dur time.Duration // timer duration

	// when: when the operation completes and
	// control returns to user code.
	// READS: when the read returns to user who called readMessage()
	// SENDS: when the send returns to user who called sendMessage()
	when time.Time

	kind mopkind
	msg  *Message

	sendmop *mop // for reads, which send did we get?
	readmop *mop // for sends, which read did we go to?

	pqit rb.Iterator

	// clients of scheduler wait on proceed.
	// timer fires, send delivered, read accepted by kernel
	proceed chan struct{}
}

func (s *simnet) newReadMsg(isCli bool) (op *mop) {
	op = &mop{
		originCli: isCli,
		sn:        simnetNextSn(),
		kind:      READ,
		proceed:   make(chan struct{}),
	}
	return
}

func (s *simnet) newSendMsg(msg *Message, isCli bool) (op *mop) {
	op = &mop{
		originCli: isCli,
		msg:       msg,
		sn:        simnetNextSn(),
		kind:      SEND,
		proceed:   make(chan struct{}),
	}
	return
}

// readMessage reads a framed message from conn.
func (s *simnet) readMessage(conn uConn) (msg *Message, err error) {

	isCli := conn.(*simnetConn).isCli
	vv("top simnet.readMessage. iscli=%v", isCli)

	read := s.newReadMsg(isCli)
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

func (s *simnet) sendMessage(conn uConn, msg *Message, timeout *time.Duration) error {

	isCli := conn.(*simnetConn).isCli
	vv("top simnet.sendMessage. iscli=%v", isCli)

	send := s.newSendMsg(msg, isCli)
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

// order by when, then Message.HDR.CallID then HDR.Serial
// (try hard not to delete tickets with the same when,
// and even then we may have reason to keep
// the exact same mop for a task at the same time;
// so use Serial too).
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

func (s *simnet) handleSend(send *mop) {
	vv("top of handleSend, here is the Q prior to send: '%v'\n", send)
	s.showQ()

	send.seen++
	send.when = time.Now().Add(s.hop)

	if send.originCli {
		s.sentFromCli = append(s.sentFromCli, send)
	} else {
		s.sentFromSrv = append(s.sentFromSrv, send)
	}
}
func (s *simnet) handleRead(read *mop) {
	vv("top of handleRead, here is the Q prior to read='%v'\n", read)
	s.showQ()

	addedToPQ := false

	read.seen++
	read.when = time.Now().Add(s.hop)

	if read.originCli {
		if len(s.sentFromSrv) > 0 {
			// can service the read
			send := s.sentFromSrv[0]
			read.msg = send.msg // TODO clone()?
			// matchmaking
			vv("[1]matchmaking send '%v' -> read '%v'", send, read)
			read.sendmop = send
			send.readmop = read

			s.sentFromSrv = s.sentFromSrv[1:]

			close(read.proceed)
			close(send.proceed)
		} else {
			vv("no sends from server, stalling the client read")
			read.when = time.Now().Add(s.tick)
			_, read.pqit = s.pq.add(read)
			addedToPQ = true
		}
	} else {
		// read originated on server
		if len(s.sentFromCli) > 0 {
			// can service the read
			send := s.sentFromCli[0]
			read.msg = send.msg // TODO clone()?
			// matchmaking
			vv("[1]matchmaking send '%v' -> read '%v'", send, read)
			read.sendmop = send
			send.readmop = read

			s.sentFromCli = s.sentFromCli[1:]

			close(read.proceed)
			close(send.proceed)
		} else {
			vv("no sends from client, stalling the server read")
			read.when = time.Now().Add(s.tick)
			_, read.pqit = s.pq.add(read)
			addedToPQ = true
		}
	}
	if addedToPQ {
		s.queueNext()
	}
}

// INVAR: we remove nothing from pq.
// If pq has anything, set s.nextPQ to
// fire off its time.After timer when the
// earliest is due to be delivered.
func (s *simnet) queueNext() {
	vv("top of queueNext")
	s.showQ()
	next := s.pq.peek()
	if next != nil {
		wait := next.when.Sub(time.Now())
		s.nextPQ.Reset(wait)
	} else {
		vv("queueNext: empty PQ")
	}
}

func (s *simnet) Start() {

	for i := 0; ; i++ {
		if i > 0 {
			time.Sleep(s.tick)
		}
		select {
		case now := <-s.nextPQ.C: // the time for action has arrived
			vv("s.nextPQ -> now %v", now)

			for op := s.pq.peek(); op != nil && op.when.Equal(now); {
				s.pq.pop() // remove op from pq
				vv("got from <-nextPQ: op = %v. PQ without op is:", op)
				s.showQ()
				switch op.kind {
				case READ:
					vv("have READ from <-nextPQ: op='%v'", op)
					s.handleRead(op)
				case SEND:
					vv("have SEND from <-nextPQ: op=%v", op)
					s.handleSend(op)
				case TIMER:
					vv("have TIMER firing from <-nextPQ")
					close(op.proceed)
				}
			}
			vv("done with now events, going to queueNext()")
			s.queueNext()

		case timer := <-s.addTimer:
			_, timer.pqit = s.pq.add(timer)
			s.queueNext()

		case s.cli = <-s.cliReady:
		case send := <-s.msgSendCh:
			s.handleSend(send)
		case read := <-s.msgReadCh:
			s.handleRead(read)
		case <-s.halt.ReqStop.Chan:
			return
		}
	}
}
