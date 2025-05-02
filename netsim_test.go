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
	send     chan *netSend
	read     chan *netRead
	addTimer chan *netTimer
	waitq    *waitQ
}

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

func newNetSend() *netSend {
	return &netSend{
		sn:     netsimNextSn(),
		inside: make(chan struct{}),
	}
}
func newNetRead() *netRead {
	return &netRead{
		sn:      netsimNextSn(),
		readgot: make(chan *Fragment),
	}
}
func newNetTimer(dur time.Duration) *netTimer {
	return &netTimer{
		sn:    netsimNextSn(),
		when:  time.Now().Add(dur),
		fires: make(chan struct{}), // wakeup
	}
}

func newNetsim(seed [32]byte) (s *netsim) {
	s = &netsim{
		//ckts:     make(map[string]*Circuit),
		seed:     seed,
		rng:      mathrand2.NewChaCha8(seed),
		send:     make(chan *netSend),
		read:     make(chan *netRead),
		addTimer: make(chan *netTimer),
		waitq:    newWaitQ(),
	}
	return
}

type waitQ struct {
	reads  map[int]*netRead
	sends  map[int]*netSend
	timers map[int]*netTimer

	pq pq // priority queue, op.when ordered
}

func newWaitQ() *waitQ {
	return &waitQ{
		reads:  make(map[int]*netRead),
		sends:  make(map[int]*netSend),
		timers: make(map[int]*netTimer),
	}
}
func (s *netsim) AddPeer(peerID string, ckt *Circuit) (err error) {
	//s.ckts[peerID] = ckt
	return nil
}

type netTimer struct {
	sn   int64
	when time.Time
	//isTicker bool // auto-reloading deferred
	fires chan struct{}
}

// reproducibly sort opsn using pseudo rng,
// producing a permutation of the available
// netsim elements (timers, reads, sends).
type opsn struct {
	sn    int64
	when  time.Time
	order uint64
	kind  simkind
}
type simkind int

const (
	TIMER simkind = 1
	SEND  simkind = 2
	READ  simkind = 3
)

func (s *netsim) start() {

	go func() {

		// deterministic scheduling loop
		for {
			synctest.Wait()
			// all timers and goro are durably blocked, time to schedule
			now := time.Now()
			vv("scheduling at %v", now)

			//chrono := s.timeorder()
			//log := s.logicalClockOrder()

			// causality dictates:
			// reads can only read on a ckt if a
			//   send on the ckt happened before.

			op := s.waitQ.peek()
			if op != nil {
				//for _, op := range *log {
				if op.when.After(now) {
					// preserve time
					continue
				}
				switch op.kind {
				case TIMER:
					ti := s.q.timers[op.sn]
					delete(s.q.timers, op.sn)
					close(ti.fires)
				}
				/*
					case READ:
						re := s.reads[op.sn]
						delete(s.reads, op.sn)
						re.frag = frag
						re.readgot <- frag

					case SEND:
						se := s.sends[op.sn]
						delete(s.sends, op.sn)
						close(se.inside)
					}
				*/
			}

			select {
			case netSend := <-s.send:
				vv("simnet.in -> netSend = '%v'", netSend)
			case timer := <-s.newTimer:
				vv("newTimer: '%#v'", timer)
			}
		}
	}()
}

type permutation []opsn

func (p permutation) Len() int { return len(lo) }
func (p permutation) Less(i, j int) bool {
	return p[i].order < p[j].order
}
func (p permutation) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

type chronological []opsn

func (c chronological) Len() int { return len(c) }
func (c chronological) Less(i, j int) bool {
	return c[i].when.Before(c[j].when)
}
func (c chronological) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

type logicalclock []opsn

func (c logicalclock) Len() int { return len(c) }
func (c logicalclock) Less(i, j int) bool {
	return c[i].sn < c[j].sn
}
func (c logicalclock) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

func (s *netsim) permute() *permutation {
	var perm permutation
	for _, ti := range timers {
		perm = append(perm, opsn{sn: ti.sn, when: ti.when, order: rng.Unint64(), kind: TIMER})
	}
	for _, re := range reads {
		perm = append(perm, opsn{sn: re.sn, when: ti.when, order: rng.Unint64(), kind: READ})
	}
	for _, se := range sends {
		perm = append(perm, opsn{sn: se.sn, when: ti.when, order: rng.Unint64(), kind: SEND})
	}
	sort.Sort(perm)
	return &perm
}

func (s *netsim) timeorder() *chronological {
	var chrono chronological
	for _, ti := range s.timers {
		chrono = append(chrono, opsn{sn: ti.sn, when: ti.when, kind: TIMER})
	}
	for _, re := range s.reads {
		chrono = append(chrono, opsn{sn: re.sn, when: re.when, kind: READ})
	}
	for _, se := range s.sends {
		chrono = append(chrono, opsn{sn: se.sn, when: se.when, kind: SEND})
	}
	sort.Sort(chrono)
	return &chrono
}

func (s *netsim) logicalClockOrder() *logicalclock {
	var logical logicalclock
	for _, ti := range s.timers {
		logical = append(logical, opsn{sn: ti.sn, when: ti.when, kind: TIMER})
	}
	for _, re := range s.reads {
		logical = append(logical, opsn{sn: re.sn, when: re.when, kind: READ})
	}
	for _, se := range s.sends {
		logical = append(logical, opsn{sn: se.sn, when: se.when, kind: SEND})
	}
	sort.Sort(logical)
	return &logical
}

func Test500_synctest_basic(t *testing.T) {
	synctest.Run(func() {

		var seed [32]byte
		netsim := newNetsim(seed)
		netsim.start()

		dur := time.Second()
		timer := newNetTimer(dur)

		vv("dur=%v, about to wait on timer at %v", dur, time.Now())
		<-timer.fires
		vv("timer fired at %v", time.Now())

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
