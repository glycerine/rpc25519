package rpc25519

import (
	"context"
	"fmt"
	//"os"
	//"strings"
	"testing"
	"testing/synctest"
	"time"

	cv "github.com/glycerine/goconvey/convey"
	"github.com/glycerine/idem"
)

var _ = fmt.Sprintf
var _ = time.Sleep

type testSimNetJunk4 struct {
	name string
	cfg  *Config
	srv  *Server
	cli  *Client

	cliServiceName string
	srvServiceName string

	clis *countService
	srvs *countService

	simnet *SimNet
}

func (j *testSimNetJunk4) cleanup() {
	j.cli.Close()
	j.srv.Close()
}

func newTestSimNetJunk4(name string) (j *testSimNetJunk4) {

	j = &testSimNetJunk4{
		name:           name,
		cliServiceName: "cli_" + name,
		srvServiceName: "srv_" + name,
		simnet:         NewSimNet(),
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

// SimNet uses channels instead of os network.
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
func Test509_SimNet_lots_of_send_and_read(t *testing.T) {

	return
	cv.Convey("On a SimNet, many sends and reads between peers", t, func() {

		j := newTestSimNetJunk4("manysend_509")
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

var _ UniversalCliSrv = &SimNet{}

type SimNet struct {
	ckts map[string]*Circuit
}

func NewSimNet() *SimNet {
	return &SimNet{
		ckts: make(map[string]*Circuit),
	}
}

func (s *SimNet) AddPeer(peerID string, ckt *Circuit) (err error) {
	s.ckts[peerID] = ckt
	return nil
}

func (s *SimNet) SendOneWayMessage(ctx context.Context, msg *Message, errWriteDur time.Duration) (error, chan *Message) {
	panic("TODO")
	return nil, nil
}

func (s *SimNet) GetConfig() *Config {
	panic("TODO")
	return nil
}
func (s *SimNet) RegisterPeerServiceFunc(peerServiceName string, psf PeerServiceFunc) error {
	panic("TODO")
	return nil
}

func (s *SimNet) StartLocalPeer(ctx context.Context, peerServiceName string, requestedCircuit *Message) (lpb *LocalPeer, err error) {
	panic("TODO")
	return
}

func (s *SimNet) StartRemotePeer(ctx context.Context, peerServiceName, remoteAddr string, waitUpTo time.Duration) (remotePeerURL, RemotePeerID string, err error) {
	panic("TODO")
	return
}

func (s *SimNet) StartRemotePeerAndGetCircuit(lpb *LocalPeer, circuitName string, frag *Fragment, peerServiceName, remoteAddr string, waitUpTo time.Duration) (ckt *Circuit, err error) {
	panic("TODO")
	return nil, nil
}

func (s *SimNet) GetReadsForCallID(ch chan *Message, callID string) {
	panic("TODO")
	return
}
func (s *SimNet) GetErrorsForCallID(ch chan *Message, callID string) {
	panic("TODO")
	return
}

// for Peer/Object systems; ToPeerID gets priority over CallID
// to allow such systems to implement custom message
// types. An example is the Fragment/Peer/Circuit system.
// (This priority is implemented in notifies.handleReply_to_CallID_ToPeerID).
func (s *SimNet) GetReadsForToPeerID(ch chan *Message, objID string) {
	panic("TODO")
	return
}
func (s *SimNet) GetErrorsForToPeerID(ch chan *Message, objID string) {
	panic("TODO")
	return
}

func (s *SimNet) UnregisterChannel(ID string, whichmap int) {
	panic("TODO")
	return
}
func (s *SimNet) LocalAddr() string {
	panic("TODO")
	return ""
}
func (s *SimNet) RemoteAddr() string { // client provides, server gives ""
	panic("TODO")
	return ""
}

// allow peers to find out that the host Client/Server is stopping.
func (s *SimNet) GetHostHalter() *idem.Halter {
	panic("TODO")
	return nil
}

// fragment memory recycling, to avoid heap pressure.
func (s *SimNet) NewFragment() *Fragment {
	panic("TODO")
	return nil
}
func (s *SimNet) FreeFragment(frag *Fragment) {
	panic("TODO")
	return
}
func (s *SimNet) RecycleFragLen() int {
	panic("TODO")
	return 0
}
func (s *SimNet) PingStats(remote string) *PingStat {
	panic("TODO")
	return nil
}
func (s *SimNet) AutoClients() (list []*Client, isServer bool) {
	panic("TODO")
	return
}

func Test500_synctest_basic(t *testing.T) {
	synctest.Run(func() {
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

	})
}
