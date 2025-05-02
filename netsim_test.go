package rpc25519

import (
	"context"
	"fmt"
	//"os"
	//"strings"
	mathrand2 "math/rand/v2"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	cv "github.com/glycerine/goconvey/convey"
	"github.com/glycerine/idem"
	rb "github.com/glycerine/rbtree"
)

var _ = fmt.Sprintf
var _ = time.Sleep

var _ UniversalCliSrv = &netsim{}

type testNetsimJunk4 struct {
	name string
	cfg  *Config
	srv  *Server
	cli  *Client

	cliServiceName string
	srvServiceName string

	clis *countService
	srvs *countService

	netsim *netsim
}

type lowestTimeFirst struct {
	tree *rb.Tree
}

func (s *lowestTimeFirst) peek() *fop {
	if s.tree.Len() == 0 {
		return nil
	}
	it := s.tree.Min()
	return it.Item().(*fop)
}

func (s *lowestTimeFirst) pop() *fop {
	if s.tree.Len() == 0 {
		return nil
	}
	it := s.tree.Min()
	top := it.Item().(*fop)
	s.tree.DeleteWithIterator(it)
	return top
}

func (s *lowestTimeFirst) add(op *fop) (added bool, it rb.Iterator) {
	added, it = s.tree.InsertGetIt(op)
	return
}

// order by when, then Frag(circuitID, fromID, sn...); try
// hard not to delete tickets with the same when,
// and even then we may have reason to keep
// the exact same ticket for a task at the same time;
// so use fop.sn too.
func newLowestTimeFirst() *lowestTimeFirst {
	return &lowestTimeFirst{
		tree: rb.NewTree(func(a, b rb.Item) int {
			av := a.(*fop)
			bv := b.(*fop) // .when

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
			// av.frag could be nil (so could bv.frag)
			if av.frag == nil && bv.frag == nil {
				return 0
			}
			if av.frag == nil {
				return -1
			}
			if bv.frag == nil {
				return 1
			}
			// INVAR: a.when == b.when
			return av.frag.Compare(bv.frag)
		}),
	}
}

func (j *testNetsimJunk4) cleanup() {
	j.cli.Close()
	j.srv.Close()
}

func newTestNetsimJunk4(name string) (j *testNetsimJunk4) {

	var seed [32]byte
	j = &testNetsimJunk4{
		name:           name,
		cliServiceName: "cli_" + name,
		srvServiceName: "srv_" + name,
		netsim:         newNetsim(seed),
	}

	cfg := NewConfig()
	cfg.TCPonly_no_TLS = true

	cfg.ServerAddr = "127.0.0.1:0"
	srv := NewServer("srv_"+name, cfg)

	serverAddr, err := srv.Start()
	panicOn(err)

	cfg.ClientDialToHostPort = serverAddr.String()
	cli, err := NewClient("cli_"+name, cfg)
	panicOn(err)
	err = cli.Start()
	panicOn(err)

	j.clis = newcountService()
	j.srvs = newcountService()

	err = cli.PeerAPI.RegisterPeerServiceFunc(j.cliServiceName, j.clis.start)
	panicOn(err)

	err = srv.PeerAPI.RegisterPeerServiceFunc(j.srvServiceName, j.srvs.start)
	panicOn(err)

	j.cli = cli
	j.srv = srv
	j.cfg = cfg

	return j
}

// netsim uses channels instead of os network.
// It is used for testing, for simulating
// patitions (isolated peers), and message
// dropped, duplicated, and reordered Fragments;
// even though the circuit may be robust
// to duplication, the peer may re-send a
// message thinking it did not arrive; or
// the network may re-deliver an already
// delivered fragment (unlikely with TCP,
// but that is what Raft/the fault-tolerance
// protocols guard against).
func Test509_netsim_lots_of_send_and_read(t *testing.T) {

	return
	cv.Convey("On a netsim, many sends and reads between peers", t, func() {

		j := newTestNetsimJunk4("manysend_509")
		defer j.cleanup()

		ctx := context.Background()
		cli_lpb, err := j.cli.PeerAPI.StartLocalPeer(ctx, j.cliServiceName, nil)
		panicOn(err)
		defer cli_lpb.Close()

		srv_lpb, err := j.srv.PeerAPI.StartLocalPeer(ctx, j.srvServiceName, nil)
		panicOn(err)
		defer srv_lpb.Close()

		// establish a circuit
		// since cli starts, they are the active initiator and server is passive.
		j.clis.startCircuitWith <- srv_lpb.URL()

		// prevent race with client starting circuit
		<-j.srvs.dropcopy_reads
		// we know the server has read a frag now.

		if got, want := cli_lpb.OpenCircuitCount(), 1; got != want {
			t.Fatalf("error: expected 1 open circuit on cli, got: '%v'", got)
		}
		if got, want := srv_lpb.OpenCircuitCount(), 1; got != want {
			t.Fatalf("error: expected 1 open circuit on srv, got: '%v'", got)
		}
		////vv("OK!")
		// send and read.

		// establish the baseline.

		// starting: client has read zero.
		if got, want := j.clis.getAllReads(), 0; got != want {
			t.Fatalf("error: expected %v reads to start, client got: %v", want, got)
		}
		// setting up the circuit means the server got a CallPeerStartCircuit frag.
		// to start with
		if got, want := j.srvs.getAllReads(), 1; got != want {
			t.Fatalf("error: expected %v reads to start, server got: %v", want, got)
		}
		j.srvs.reset() // set the server count to zero to start with.
		if got, want := j.srvs.getAllReads(), 0; got != want {
			t.Fatalf("error: expected %v reads after reset(), server got: %v", want, got)
		}

		// and the send side. verify at 1 then reset to zero
		if got, want := j.clis.getAllSends(), 1; got != want {
			t.Fatalf("error: expected %v sends to start, client got: %v", want, got)
		}
		j.clis.reset() // set the server count to zero to start with.
		if got, want := j.clis.getAllSends(), 0; got != want {
			t.Fatalf("error: expected %v sends after reset, cli got: %v", want, got)
		}
		if got, want := j.srvs.getAllSends(), 0; got != want {
			t.Fatalf("error: expected %v sends to start, server got: %v", want, got)
		}

		// and we can simply count the size of the readch, since it is buffered 1000
		if got, want := len(j.clis.readch), 0; got != want {
			t.Fatalf("error: expected %v in readch to start, clis got: %v", want, got)
		}
		if got, want := len(j.srvs.readch), 1; got != want {
			t.Fatalf("error: expected %v in readch to start, srvs got: %v", want, got)
		}
		if got, want := len(j.clis.sendch), 1; got != want {
			t.Fatalf("error: expected cli sendch to have %v, got: %v", want, got)
		}
		if got, want := len(j.srvs.sendch), 0; got != want {
			t.Fatalf("error: expected srv sendch to have %v, got: %v", want, got)
		}

		// have server send 1 to client.
		frag := NewFragment()
		frag.FragPart = 0
		j.srvs.requestToSend <- frag

		// wait for it to get the client
		select {
		case <-j.clis.dropcopy_reads:
		case <-time.After(2 * time.Second):
			t.Fatalf("client did not get their dropcopy after 2 sec")
		}
		if got, want := len(j.clis.readch), 1; got != want {
			t.Fatalf("error: expected cli readch to have %v, got: %v", want, got)
		}
		if got, want := len(j.srvs.readch), 1; got != want {
			t.Fatalf("error: expected srv readch to have %v, got: %v", want, got)
		}
		if got, want := len(j.clis.sendch), 1; got != want {
			t.Fatalf("error: expected cli sendch to have %v, got: %v", want, got)
		}
		if got, want := len(j.srvs.sendch), 1; got != want {
			t.Fatalf("error: expected srv sendch to have %v, got: %v", want, got)
		}

		//vv("okay up to here.")

		// have client send 1 to server.
		frag = NewFragment()
		frag.FragPart = 1
		drain(j.srvs.dropcopy_reads)
		j.clis.requestToSend <- frag

		// wait for it to get the server
		select {
		case <-j.srvs.dropcopy_reads:
		case <-time.After(2 * time.Second):
			t.Fatalf("server did not get their dropcopy after 2 sec")
		}

		// assert readch/sendch state
		if got, want := len(j.clis.readch), 1; got != want {
			t.Fatalf("error: expected cli readch to have %v, got: %v", want, got)
		}
		if got, want := len(j.srvs.readch), 2; got != want {
			t.Fatalf("error: expected srv readch to have %v, got: %v", want, got)
		}
		if got, want := len(j.clis.sendch), 2; got != want {
			t.Fatalf("error: expected cli sendch to have %v, got: %v", want, got)
		}
		if got, want := len(j.srvs.sendch), 1; got != want {
			t.Fatalf("error: expected srv sendch to have %v, got: %v", want, got)
		}

		//vv("ask the client to start another circuit to the same remote.")
		drain(j.srvs.dropcopy_reads)

		frag = NewFragment()
		ckt2name := "client-started-2nd-ckt-to-same"
		frag.FragSubject = ckt2name
		j.clis.activeSideStartAnotherCkt <- frag

		// prevent race with client starting circuit
		<-j.srvs.dropcopy_reads
		// we know the server has read another frag now.

		// verify open circuit count
		if got, want := cli_lpb.OpenCircuitCount(), 2; got != want {
			t.Fatalf("error: expected %v open circuit on cli, got: '%v'", want, got)
		}
		if got, want := srv_lpb.OpenCircuitCount(), 2; got != want {
			t.Fatalf("error: expected %v open circuit on srv, got: '%v'", want, got)
		}

		// assert readch state on sever has incremented.
		if got, want := len(j.clis.readch), 1; got != want {
			t.Fatalf("error: expected cli readch to have %v, got: %v", want, got)
		}
		if got, want := len(j.srvs.readch), 3; got != want {
			t.Fatalf("error: expected srv readch to have %v, got: %v", want, got)
		}
		if got, want := len(j.clis.sendch), 2; got != want {
			t.Fatalf("error: expected cli sendch to have %v, got: %v", want, got)
		}
		if got, want := len(j.srvs.sendch), 1; got != want {
			t.Fatalf("error: expected srv sendch to have %v, got: %v", want, got)
		}

		//vv("ask the server to start (a third) circuit to the same remote.")

		drain(j.clis.dropcopy_reads)

		frag = NewFragment()
		ckt3name := "server-started-3rd-ckt-to-same"
		frag.FragSubject = ckt3name
		j.srvs.passiveSideStartAnotherCkt <- frag

		// prevent race with client starting circuit
		<-j.clis.dropcopy_reads
		// we know the server has read another frag now.

		// verify open circuit count went to 3.
		if got, want := cli_lpb.OpenCircuitCount(), 3; got != want {
			t.Fatalf("error: expected %v open circuit on cli, got: '%v'", want, got)
		}
		if got, want := srv_lpb.OpenCircuitCount(), 3; got != want {
			t.Fatalf("error: expected %v open circuit on srv, got: '%v'", want, got)
		}

		// assert readch state on client has incremented.
		if got, want := len(j.clis.readch), 2; got != want {
			t.Fatalf("error: expected cli readch to have %v, got: %v", want, got)
		}
		if got, want := len(j.srvs.readch), 3; got != want {
			t.Fatalf("error: expected srv readch to have %v, got: %v", want, got)
		}
		if got, want := len(j.clis.sendch), 2; got != want {
			t.Fatalf("error: expected cli sendch to have %v, got: %v", want, got)
		}
		if got, want := len(j.srvs.sendch), 1; got != want {
			t.Fatalf("error: expected srv sendch to have %v, got: %v", want, got)
		}

		//vv("we have 3 open circuits between the same two peers. do some sends on each")

		// server to client, do N sends

		N := 10
		drain(j.clis.dropcopy_reads)
		j.srvs.passiveSideSendN <- N

		for range N {
			<-j.clis.dropcopy_reads
		}
		// verify client readch incremented by N
		if got, want := len(j.clis.readch), 2+N; got != want {
			t.Fatalf("error: expected cli readch to have %v, got: %v", want, got)
		}
		if got, want := len(j.srvs.readch), 3; got != want {
			t.Fatalf("error: expected srv readch to have %v, got: %v", want, got)
		}
		if got, want := len(j.clis.sendch), 2; got != want {
			t.Fatalf("error: expected cli sendch to have %v, got: %v", want, got)
		}
		if got, want := len(j.srvs.sendch), 1+N; got != want {
			t.Fatalf("error: expected srv sendch to have %v, got: %v", want, got)
		}

		// client to server, do N sends
		drain(j.srvs.dropcopy_reads)
		j.clis.activeSideSendN <- N

		for range N {
			<-j.srvs.dropcopy_reads
		}

		// verify server readch incremented by N
		if got, want := len(j.clis.readch), 2+N; got != want {
			t.Fatalf("error: expected cli readch to have %v, got: %v", want, got)
		}
		if got, want := len(j.srvs.readch), 3+N; got != want {
			t.Fatalf("error: expected srv readch to have %v, got: %v", want, got)
		}
		if got, want := len(j.clis.sendch), 2+N; got != want {
			t.Fatalf("error: expected cli sendch to have %v, got: %v", want, got)
		}
		if got, want := len(j.srvs.sendch), 1+N; got != want {
			t.Fatalf("error: expected srv sendch to have %v, got: %v", want, got)
		}

		// verify open circuit count stayed at 3
		if got, want := cli_lpb.OpenCircuitCount(), 3; got != want {
			t.Fatalf("error: expected %v open circuit on cli, got: '%v'", want, got)
		}
		if got, want := srv_lpb.OpenCircuitCount(), 3; got != want {
			t.Fatalf("error: expected %v open circuit on srv, got: '%v'", want, got)
		}

		// CallPeerError should get returned on the ckt.Errors not ctk.Reads.

		//vv("client to server, send one error")

		// we let whichever ckt goro gets it send it (for now).
		drain(j.srvs.read_dropcopy_errors)
		errReqActiveSend := "send this error from cli to srv"
		j.clis.activeSideSendCktError <- errReqActiveSend

		<-j.srvs.read_dropcopy_errors

		nSrvErr := len(j.srvs.read_errorch)
		//vv("got past server reading from ckt.Errors: nSrvErr = %v", nSrvErr)
		if got, want := nSrvErr, 1; got != want {
			t.Fatalf("error: expected nSrvErr: %v , got: '%v'", want, got)
		}

		//vv("server to client, send one error")

		// we let whichever ckt goro gets it send it (for now).
		drain(j.clis.read_dropcopy_errors)
		errReqPassiveSend := "send this error from srv(passive) to cli(active)"
		j.srvs.passiveSideSendCktError <- errReqPassiveSend

		<-j.clis.read_dropcopy_errors

		nCliErr := len(j.clis.read_errorch)
		//vv("got past server reading from ckt.Errors: nCliErr = %v", nCliErr)
		if got, want := nCliErr, 1; got != want {
			t.Fatalf("error: expected nCliErr: %v , got: '%v'", want, got)
		}

		//vv("====  ckt shutdown on one side should get propagated to the other side.")

		// verify we have 3 open channels now
		if got, want := cli_lpb.OpenCircuitCount(), 3; got != want {
			t.Fatalf("error: expected %v open circuit on cli, got: '%v'", want, got)
		}
		if got, want := srv_lpb.OpenCircuitCount(), 3; got != want {
			t.Fatalf("error: expected %v open circuit on srv, got: '%v'", want, got)
		}

		// (passive) server side shuts down first
		// we let whichever ckt goro gets it shut down
		drain(j.srvs.passive_side_ckt_saw_remote_shutdown)
		j.clis.activeSideShutdownCkt <- nil

		<-j.srvs.passive_side_ckt_saw_remote_shutdown

		// verify open circuit count only went down to 2, not 0.
		//vv(" cli_lpb.OpenCircuitCount() = %v ; srv_lpb.OpenCircuitCount() = %v", cli_lpb.OpenCircuitCount(), srv_lpb.OpenCircuitCount())

		if got, want := cli_lpb.OpenCircuitCount(), 2; got != want {
			t.Fatalf("error: expected %v open circuit on cli, got: '%v'", want, got)
		}
		if got, want := srv_lpb.OpenCircuitCount(), 2; got != want {
			t.Fatalf("error: expected %v open circuit on srv, got: '%v'", want, got)
		}

		// (active) client side shuts down first for the next ckt.
		// we let whichever ckt goro gets it shut down
		drain(j.clis.active_side_ckt_saw_remote_shutdown)
		j.srvs.passiveSideShutdownCkt <- nil

		<-j.clis.active_side_ckt_saw_remote_shutdown

		if got, want := cli_lpb.OpenCircuitCount(), 1; got != want {
			t.Fatalf("error: expected %v open circuit on cli, got: '%v'", want, got)
		}
		if got, want := srv_lpb.OpenCircuitCount(), 1; got != want {
			t.Fatalf("error: expected %v open circuit on srv, got: '%v'", want, got)
		}

		//ckts := []*Circuit{}
		//for useCkt := range 0; useCkt < 3; useCkt++
		// but which ckt is doing the sends? can we specify it from here?
		// request it by name?

		//select {}

		//vv("#####   end of test. let the defer cleanups run now:")
	})

}

func (s *netsim) SendOneWayMessage(ctx context.Context, msg *Message, errWriteDur time.Duration) (error, chan *Message) {
	panic("TODO")
	return nil, nil
}

func (s *netsim) GetConfig() *Config {
	panic("TODO")
	return nil
}
func (s *netsim) RegisterPeerServiceFunc(peerServiceName string, psf PeerServiceFunc) error {
	panic("TODO")
	return nil
}

func (s *netsim) StartLocalPeer(ctx context.Context, peerServiceName string, requestedCircuit *Message) (lpb *LocalPeer, err error) {
	panic("TODO")
	return
}

func (s *netsim) StartRemotePeer(ctx context.Context, peerServiceName, remoteAddr string, waitUpTo time.Duration) (remotePeerURL, RemotePeerID string, err error) {
	panic("TODO")
	return
}

func (s *netsim) StartRemotePeerAndGetCircuit(lpb *LocalPeer, circuitName string, frag *Fragment, peerServiceName, remoteAddr string, waitUpTo time.Duration) (ckt *Circuit, err error) {
	panic("TODO")
	return nil, nil
}

func (s *netsim) GetReadsForCallID(ch chan *Message, callID string) {
	panic("TODO")
	return
}
func (s *netsim) GetErrorsForCallID(ch chan *Message, callID string) {
	panic("TODO")
	return
}

// for Peer/Object systems; ToPeerID gets priority over CallID
// to allow such systems to implement custom message
// types. An example is the Fragment/Peer/Circuit system.
// (This priority is implemented in notifies.handleReply_to_CallID_ToPeerID).
func (s *netsim) GetReadsForToPeerID(ch chan *Message, objID string) {
	panic("TODO")
	return
}
func (s *netsim) GetErrorsForToPeerID(ch chan *Message, objID string) {
	panic("TODO")
	return
}

func (s *netsim) UnregisterChannel(ID string, whichmap int) {
	panic("TODO")
	return
}
func (s *netsim) LocalAddr() string {
	panic("TODO")
	return ""
}
func (s *netsim) RemoteAddr() string { // client provides, server gives ""
	panic("TODO")
	return ""
}

// allow peers to find out that the host Client/Server is stopping.
func (s *netsim) GetHostHalter() *idem.Halter {
	panic("TODO")
	return nil
}

// fragment memory recycling, to avoid heap pressure.
func (s *netsim) NewFragment() *Fragment {
	panic("TODO")
	return nil
}
func (s *netsim) FreeFragment(frag *Fragment) {
	panic("TODO")
	return
}
func (s *netsim) RecycleFragLen() int {
	panic("TODO")
	return 0
}
func (s *netsim) PingStats(remote string) *PingStat {
	panic("TODO")
	return nil
}
func (s *netsim) AutoClients() (list []*Client, isServer bool) {
	panic("TODO")
	return
}

var netsimLastSn int64

func netsimNextSn() int64 {
	return atomic.AddInt64(&netsimLastSn, 1)
}

type netsim struct {
	//ckts map[string]*Circuit
	seed     [32]byte
	rng      *mathrand2.ChaCha8
	send     chan *fop
	read     chan *fop
	addTimer chan *fop
	//waitQ    *waitQ
}

/*
type netSend struct {
	sn     int64
	when   time.Time
	ckt    *Circuit
	frag   *Fragment
	inside chan struct{}
}

type netRead struct {
	sn      int64
	when    time.Time
	ckt     *Circuit
	frag    *Fragment // maybe redundant
	readgot chan *Fragment
}
*/

// fragment operation
type fop struct {
	sn int64

	dur  time.Duration // timer duration
	when time.Time     // when read for READS, when sent for SENDS?

	sorter uint64

	kind simkind
	frag *Fragment
	ckt  *Circuit

	sendfop *fop // for reads, which send did we get?

	pqit rb.Iterator

	FromPeerID string
	ToPeerID   string
	note       string

	// clients of scheduler wait on proceed.
	// timer fires, send delivered, read accepted by kernel
	proceed chan struct{}
}

func (op *fop) String() string {
	// subj := ""
	// if op.frag != nil {
	// 	subj = op.frag.FragSubject
	// } else {
	// 	subj = "(nil frag)"
	// }
	return fmt.Sprintf("fop{kind:%v, from:%v, to:%v, sn:%v, when:%v, note:'%v'}", op.kind, op.FromPeerID, op.ToPeerID, op.sn, op.when, op.note)
}

func newSend(ckt *Circuit, frag *Fragment, senderPeerID, note string) (op *fop) {
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
func newRead(ckt *Circuit, readerPeerID, note string) (op *fop) {
	op = &fop{
		note:     note,
		when:     time.Now(),
		ckt:      ckt,
		sn:       netsimNextSn(),
		kind:     READ,
		proceed:  make(chan struct{}),
		ToPeerID: readerPeerID,
	}
	switch readerPeerID {
	case ckt.LocalPeerID:
		op.FromPeerID = ckt.RemotePeerID
	case ckt.RemotePeerID:
		op.FromPeerID = ckt.LocalPeerID
	default:
		panic("bad readerPeerID, not on ckt")
	}
	return
}
func newTimer(dur time.Duration) *fop {
	return &fop{
		sn:      netsimNextSn(),
		kind:    TIMER,
		dur:     dur,
		when:    time.Now().Add(dur),
		proceed: make(chan struct{}),
	}
}

func newNetsim(seed [32]byte) (s *netsim) {
	s = &netsim{
		//ckts:     make(map[string]*Circuit),
		seed:     seed,
		rng:      mathrand2.NewChaCha8(seed),
		send:     make(chan *fop),
		read:     make(chan *fop),
		addTimer: make(chan *fop),
		//waitQ:    newWaitQ(),
	}
	return
}

// type waitQ struct {
// 	reads  map[int64]*fop
// 	sends  map[int64]*fop
// 	timers map[int64]*fop

// 	pq pqTime // priority queue, op.when ordered
// }

//	func newWaitQ() *waitQ {
//		return &waitQ{
//			reads:  make(map[int64]*fop),
//			sends:  make(map[int64]*fop),
//			timers: make(map[int64]*fop),
//		}
//	}
func (s *netsim) AddPeer(peerID string, ckt *Circuit) (err error) {
	//s.ckts[peerID] = ckt
	return nil
}

// type netTimer struct {
// 	sn   int64
// 	when time.Time
// 	dur  time.Duration
// 	//isTicker bool // auto-reloading deferred
// 	fires chan struct{}
// }

type simkind int

const (
	TIMER simkind = 1
	SEND  simkind = 2
	READ  simkind = 3
)

func (k simkind) String() string {
	switch k {
	case TIMER:
		return "TIMER"
	case SEND:
		return "SEND"
	case READ:
		return "READ"
	default:
		return fmt.Sprintf("unknown simkind %v", int(k))
	}
}

/*
func (s *netsim) start() {

	go func() {

		var now time.Time
		// somewhat deterministic scheduling loop
		for {

			//chrono := s.timeorder()
			//log := s.logicalClockOrder()

			// causality dictates:
			// reads can only read on a ckt if a
			//   send on the ckt happened before.

			op0 := s.waitQ.pq.peek()
			_ = op0
			if op0 == nil {
				vv("empty pq")
				// get a timer or network op
				select {
				case netSend := <-s.send:
					vv("simnet.in -> netSend = '%#v'", netSend)
				case ti := <-s.addTimer:
					vv("addTimer: '%#v'", ti)
					op0 = &fop{sn: ti.sn, when: ti.when, kind: TIMER}
					op0.pqit = s.waitQ.pq.add(op0)
					s.waitQ.timers[op0.sn] = ti
				//case op := <-s.opReady:
				//	_ = op
				default:

					op := s.waitQ.pq.peek()
					if op == nil {
						continue // panic("weird: no op")
					}
					if !op.when.Before(now) {
						// preserve time
						vv("not yet time for op '%#v'", op)
						go func() {
							time.Sleep(now.Sub(op.when))
							vv("time for op '%#v'", op)
							//s.opReady <- op
						}()
					}

					// let other goro run
					synctest.Wait()
					// all timers and goro are durably blocked, time to schedule
					now = time.Now()
					vv("scheduling at %v", now)
				}
			}
			//for _, op := range *log {
			s.waitQ.pq.pop()
			switch op0.kind {
			case TIMER:
				vv("firing timer '%#v'", op0)
				ti := s.waitQ.timers[op0.sn]
				delete(s.waitQ.timers, op0.sn)
				close(ti.proceed)
			}

				// case READ:
				// 	re := s.reads[op.sn]
				// 	delete(s.reads, op.sn)
				// 	re.frag = frag
				// 	re.readgot <- frag

				// case SEND:
				// 	se := s.sends[op.sn]
				// 	delete(s.sends, op.sn)
				// 	close(se.inside)
				// }

			//}
		}
	}()
}

type permutation []op

func (p permutation) Len() int { return len(p) }
func (p permutation) Less(i, j int) bool {
	return p[i].sorter < p[j].sorter
}
func (p permutation) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

type chronological []op

func (c chronological) Len() int { return len(c) }
func (c chronological) Less(i, j int) bool {
	return c[i].when.Before(c[j].when)
}
func (c chronological) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

type logicalclock []op

func (c logicalclock) Len() int { return len(c) }
func (c logicalclock) Less(i, j int) bool {
	return c[i].sn < c[j].sn
}
func (c logicalclock) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

func (s *netsim) permute() *permutation {
	var perm permutation
	for _, ti := range s.waitQ.timers {
		perm = append(perm, op{sn: ti.sn, when: ti.when, sorter: rng.Unint64(), kind: TIMER})
	}
	for _, re := range s.waitQ.reads {
		perm = append(perm, op{sn: re.sn, when: ti.when, sorter: rng.Unint64(), kind: READ})
	}
	for _, se := range sends {
		perm = append(perm, op{sn: se.sn, when: ti.when, sorter: rng.Unint64(), kind: SEND})
	}
	sort.Sort(perm)
	return &perm
}

func (s *netsim) timeorder() *chronological {
	var chrono chronological
	for _, ti := range s.timers {
		chrono = append(chrono, op{sn: ti.sn, when: ti.when, kind: TIMER})
	}
	for _, re := range s.reads {
		chrono = append(chrono, op{sn: re.sn, when: re.when, kind: READ})
	}
	for _, se := range s.sends {
		chrono = append(chrono, op{sn: se.sn, when: se.when, kind: SEND})
	}
	sort.Sort(chrono)
	return &chrono
}

func (s *netsim) logicalClockOrder() *logicalclock {
	var logical logicalclock
	for _, ti := range s.timers {
		logical = append(logical, op{sn: ti.sn, when: ti.when, kind: TIMER})
	}
	for _, re := range s.reads {
		logical = append(logical, op{sn: re.sn, when: re.when, kind: READ})
	}
	for _, se := range s.sends {
		logical = append(logical, op{sn: se.sn, when: se.when, kind: SEND})
	}
	sort.Sort(logical)
	return &logical
}
*/

func Test500_synctest_basic(t *testing.T) {
	synctest.Run(func() {

		//var seed [32]byte
		//netsim := newNetsim(seed)
		//netsim.start()

		dur := time.Second * 10
		timer := newTimer(dur)

		addTimer := make(chan *fop)
		schedDone := make(chan struct{})
		defer close(schedDone)

		ckts := make(map[string]*Circuit)
		newCktCh := make(chan *Circuit)

		hop := time.Second * 2 // duration of network single hop/delay
		tick := time.Second

		// use ckt now
		cktReadCh := make(chan *fop)
		cktSendCh := make(chan *fop)

		readOn := func(ckt *Circuit, readerPeerID, note string) *fop {
			read := newRead(ckt, readerPeerID, note)
			cktReadCh <- read
			return read
		}
		sendOn := func(ckt *Circuit, frag *Fragment, fromPeerID, note string) *fop {
			if frag.FromPeerID != fromPeerID {
				panic("bad caller: sendOn with frag.FromPeerID != fromPeerID")
			}
			send := newSend(ckt, frag, frag.FromPeerID, note)
			cktSendCh <- send
			return send
		}

		// play the "scheduler" part
		go func() {
			pq := newLowestTimeFirst()
			var nextPQ <-chan time.Time

			showQ := func() {
				i := 0
				for it := pq.tree.Min(); it != pq.tree.Limit(); it = it.Next() {
					op := it.Item().(*fop)
					fmt.Printf("pq[%2d] = %v\n", i, op)
					i++
				}
			}
			queueNext := func() {
				vv("top of queueNext")
				showQ()
				next := pq.peek()
				if next != nil {
					wait := next.when.Sub(time.Now())
					nextPQ = time.After(wait)
				} else {
					vv("queueNext: empty PQ")
				}
			}
			for i := 0; ; i++ {
				if i > 0 {
					time.Sleep(tick)
				}
				select {
				case ckt := <-newCktCh:
					ckts[ckt.CircuitID] = ckt
					vv("registered ckt '%v'", ckt.CircuitID)

				case <-nextPQ:
					op := pq.peek()
					if op != nil {
						pq.pop()
						vv("got from <-nextPQ: op = %v", op)
						switch op.kind {
						case READ:
							// read from ckt.sentFromLocal or ckt.sentFromRemote
							ckt, ok := ckts[op.ckt.CircuitID]
							if !ok {
								panic("bad READ op.frag.CircuitID; not in ckts")
							}
							switch op.ToPeerID {
							case ckt.LocalPeerID:
								if len(ckt.sentFromRemote) > 0 {
									// can service the read
									read := ckt.sentFromRemote[0]
									op.frag = read.frag // TODO clone()?
									ckt.sentFromRemote = ckt.sentFromRemote[1:]

									// why the asymmetry? this is ok, but not below?
									// think b/c we are randomly deleting?
									//pq.delOneItem(read.pqit)

									close(op.proceed)
								} else {
									//panic("stall the read?")
									vv("1st no sends for reader, stalling the read: len(ckt.sentFromLocal)=%v; and len(ckt.sentFromRemote)='%v'", len(ckt.sentFromLocal), len(ckt.sentFromRemote))
									op.when = time.Now().Add(tick)
									_, op.pqit = pq.add(op)

									//below: queueNext()

								}
							case ckt.RemotePeerID:
								if len(ckt.sentFromLocal) > 0 {
									// can service the read
									read := ckt.sentFromLocal[0]
									op.frag = read.frag // TODO clone()?
									ckt.sentFromLocal = ckt.sentFromLocal[1:]
									//pq.delOneItem(read.pqit)
									close(op.proceed)
								} else {
									//panic("stall the read?")
									vv("2nd no sends for reader, stalling the read: len(ckt.sentFromLocal)=%v; and len(ckt.sentFromRemote)='%v'", len(ckt.sentFromLocal), len(ckt.sentFromRemote))
									op.when = time.Now().Add(tick)
									_, op.pqit = pq.add(op)
									//below: queueNext()

								}
							default:
								panic("bad READ op on ckt, not for local or remote")
							}

						case SEND:
							ckt, ok := ckts[op.frag.CircuitID]
							if !ok {
								panic(fmt.Sprintf("bad SEND, unregistered ckt op.frag.CircuitID='%v'", op.frag.CircuitID))
							}
							// buffer on ckt.sentFromLocal or ckt.sentFromRemote
							switch op.ToPeerID {
							case ckt.LocalPeerID:
								ckt.sentFromRemote = append(ckt.sentFromRemote, op)
							case ckt.RemotePeerID:
								ckt.sentFromLocal = append(ckt.sentFromLocal, op)
							default:
								panic(fmt.Sprintf("bad SEND op on ckt, not for local or remote. op.ToPeerID = '%v'; ckt.LocalPeerID='%v'; ckt.RemotePeerID='%v'", op.ToPeerID, ckt.LocalPeerID, ckt.RemotePeerID))
							}

						case TIMER:
							vv("TIMER firing")
							close(op.proceed)
						}
					} // end if op != nil
					queueNext()

				case timer := <-addTimer:
					_, timer.pqit = pq.add(timer)
					queueNext()

				case send := <-cktSendCh:
					vv("scheduler cktSendCh")
					//vv("scheduler cktSendCh from:%v to %v", send.FromPeerID, send.ToPeerID)
					//send := newSend()
					send.when = time.Now().Add(hop)
					//send.frag = frag
					_, send.pqit = pq.add(send)
					queueNext()
					close(send.proceed)

				case read := <-cktReadCh:
					vv("scheduler cktReadCh")
					//read := newRead()
					_, read.pqit = pq.add(read)
					queueNext()

				case <-schedDone:
					return
				}
			}
		}()

		cktID := "ckt1"
		reader1 := "reader1"
		sender1 := "sender1"
		ckt1 := &Circuit{
			LocalPeerID:  sender1,
			RemotePeerID: reader1,
			CircuitID:    cktID,
		}
		newCktCh <- ckt1

		// sender 1
		go func() {
			frag := NewFragment()
			frag.FromPeerID = sender1
			frag.ToPeerID = reader1
			frag.CircuitID = cktID
			frag.Serial = 1
			note := "from sender1 to reader1 on ckt1"
			send := sendOn(ckt1, frag, sender1, note)
			<-send.proceed
			vv("sender1 sent '%v'", note)

		}()

		/*
			// sender 2
			go func() {
				sendCh <- &Fragment{
					FromPeerID:  "sender2",
					ToPeerID:    "reader1",
					CircuitID:   "ckt1",
					FragSubject: "from sender2 to reader1 on ckt2"}
				vv("sender2 sent")
			}()
		*/

		// reader 1
		go func() {
			read := readOn(ckt1, reader1, "read1, reader1") // get a read op dest "reader1"
			<-read.proceed
			vv("reader 1 got 1st frag from %v; Serial=%v; op='%v'", read.FromPeerID, read.frag.Serial)

			read2 := readOn(ckt1, reader1, "read2, reader1") // get a read op dest "reader1"
			<-read2.proceed
			vv("reader 1 got 2nd frag from %v; Serial=%v", read2.FromPeerID, read2.frag.Serial)

		}()

		vv("dur=%v, about to wait on timer at %v", dur, time.Now())
		//netsim.addTimer <- timer
		addTimer <- timer
		<-timer.proceed
		vv("timer fired at %v", time.Now())

		// sender 1, sends again.
		go func() {
			frag := NewFragment()
			frag.FromPeerID = sender1
			frag.ToPeerID = reader1
			frag.CircuitID = cktID
			frag.Serial = 2
			note := "Serial 2 message from sender1 to reader1 on ckt1"
			send := sendOn(ckt1, frag, sender1, note)
			<-send.proceed
			vv("sender1 sent Serial 2")

		}()

		timer2 := newTimer(dur)
		vv("dur=%v, about to wait on 2nd timer at %v", dur, time.Now())
		//netsim.addTimer <- timer
		addTimer <- timer2
		<-timer2.proceed
		vv("timer2 fired at %v", time.Now())

		/*
			// Create a context.Context which is canceled after a timeout.
			const timeout = 5 * time.Second
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			// Wait just less than the timeout.
			time.Sleep(timeout - time.Nanosecond)
			synctest.Wait()
			fmt.Printf("before timeout: ctx.Err() = %v\n", ctx.Err())

			// Wait the rest of the way until the timeout.
			time.Sleep(time.Nanosecond)
			synctest.Wait()
			fmt.Printf("after timeout:  ctx.Err() = %v\n", ctx.Err())
		*/
	})
}
