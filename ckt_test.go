package rpc25519

import (
	"context"
	"fmt"
	//"os"
	//"strings"
	"testing"
	"time"

	cv "github.com/glycerine/goconvey/convey"
)

var _ = fmt.Sprintf
var _ = time.Sleep

type testJunk3 struct {
	name string
	cfg  *Config
	srv  *Server
	cli  *Client

	cliServiceName string
	srvServiceName string

	clis *countService
	srvs *countService
}

func (j *testJunk3) cleanup() {
	j.cli.Close()
	j.srv.Close()
}

func newTestJunk3(name string) (j *testJunk3) {

	j = &testJunk3{
		name:           name,
		cliServiceName: "cli_" + name,
		srvServiceName: "srv_" + name,
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

type counts struct {
	sends int
	reads int
}

type countService struct {
	// key is circuit name
	stats *Mutexmap[string, *counts]

	// keep a record of all reads, so test can assert on history at the end.
	readch chan *Fragment
	// and copy all reads to here so test avoid races.
	dropcopy_reads chan *Fragment

	// ask the start() func to send to remote.
	sendch chan *Fragment

	requestToSend chan *Fragment

	// have the start ack each send request here.
	dropcopy_sends chan *Fragment
	//dropcopy_sent     chan *Fragment

	startAnotherCkt            chan *Fragment
	passiveSideStartAnotherCkt chan *Fragment

	startCircuitWith chan string // remote URL to contact.
	nextCktNo        int
}

func newcountService() *countService {
	return &countService{
		stats:                      NewMutexmap[string, *counts](),
		readch:                     make(chan *Fragment, 1000),
		sendch:                     make(chan *Fragment, 1000),
		requestToSend:              make(chan *Fragment),
		dropcopy_sends:             make(chan *Fragment, 1000),
		dropcopy_reads:             make(chan *Fragment, 1000),
		startCircuitWith:           make(chan string),
		startAnotherCkt:            make(chan *Fragment),
		passiveSideStartAnotherCkt: make(chan *Fragment),
	}
}

func (s *countService) reset() {
	s.stats.Clear()
}
func (s *countService) getAllReads() (n int) {
	countsSlice := s.stats.GetValSlice()
	for _, count := range countsSlice {
		n += count.reads
	}
	return
}
func (s *countService) getAllSends() (n int) {
	countsSlice := s.stats.GetValSlice()
	for _, count := range countsSlice {
		n += count.sends
	}
	return
}

func (s *countService) incrementReads(cktName string) (tot int) {
	s.stats.Update(func(stats map[string]*counts) {
		c, ok := stats[cktName]
		if !ok {
			c = &counts{}
			stats[cktName] = c
		}
		c.reads++
		tot = c.reads
	})
	return
}
func (s *countService) incrementSends(cktName string) (tot int) {
	s.stats.Update(func(stats map[string]*counts) {
		c, ok := stats[cktName]
		if !ok {
			c = &counts{}
			stats[cktName] = c
		}
		c.sends++
		tot = c.sends
	})
	return
}

func (s *countService) start(myPeer *LocalPeer, ctx0 context.Context, newCircuitCh <-chan *Circuit) error {

	name := myPeer.PeerServiceName
	_ = name // used when logging is on.

	defer func() {
		//vv("%v: end of start() inside defer, about the return/finish", name)
		//s.halt.Done.Close()
	}()

	//vv("%v: start() top.", name)
	//zz("%v: ourID = '%v'; peerServiceName='%v';", name, myPeer.ID(), myPeer.ServiceName())

	aliasRegister(myPeer.PeerID, myPeer.PeerID+" ("+myPeer.ServiceName()+")")

	done0 := ctx0.Done()

	for {
		//vv("%v: top of select", name)
		select {
		case <-done0:
			//vv("%v: done0! cause: '%v'", name, context.Cause(ctx0))
			return ErrContextCancelled
			//case <-s.halt.ReqStop.Chan:
			//	//zz("%v: halt.ReqStop seen", name)
			//	return ErrHaltRequested

			// new Circuit connection arrives => we are the passive side for it.
		case rckt := <-newCircuitCh:
			vv("%v: newCircuitCh got rckt! service sees new peerURL: '%v'", name, rckt.RemoteCircuitURL())

			// talk to this peer on a separate goro if you wish; or just a func
			var passiveSide func(ckt *Circuit)
			passiveSide = func(ckt *Circuit) {
				//vv("%v: (ckt '%v') got incoming ckt", name, ckt.Name)
				//s.gotIncomingCkt <- ckt
				//zz("%v: (ckt '%v') got past <-ckt for incoming ckt", name, ckt.Name)
				defer func() {
					//vv("%v: (ckt '%v') defer running! finishing new Circuit func.", name, ckt.Name) // seen on server
					ckt.Close()
					//s.gotCktHaltReq.Close()
				}()

				//zz("%v: (ckt '%v') <- got new incoming ckt", name, ckt.Name)
				////zz("incoming ckt has RemoteCircuitURL = '%v'", ckt.RemoteCircuitURL())
				////zz("incoming ckt has LocalCircuitURL = '%v'", ckt.LocalCircuitURL())
				//done := ctx.Done()

				// this is the passive side, as we <-newCircuitCh
				for {
					select {
					case frag := <-s.passiveSideStartAnotherCkt:

						vv("%v: (ckt '%v') (passive) passiveSideStartAnotherCkt requsted!: '%v'", name, ckt.Name, frag.FragSubject)
						ckt2, _, err := ckt.NewCircuit(frag.FragSubject, frag)
						panicOn(err)
						vv("%v: (ckt '%v') (passive) created ckt2 to: '%v'", name, ckt.Name, ckt2.RemoteCircuitURL())
						go passiveSide(ckt2)

					case frag := <-s.requestToSend:
						// external test code requests that we send.
						vv("%v: (ckt '%v') (passive) got requestToSend, sending to '%v'; from '%v'", name, ckt.Name, ckt.RemoteCircuitURL(), ckt.LocalCircuitURL())

						err := ckt.SendOneWay(frag, 0)
						panicOn(err)
						s.incrementSends(ckt.Name)
						s.sendch <- frag
						s.dropcopy_sends <- frag

					case frag := <-ckt.Reads:
						_ = frag
						tot := s.incrementReads(ckt.Name)
						vv("%v: (ckt %v) (passive) ckt.Reads (total %v) sees frag:'%s'", name, ckt.Name, tot, frag)
						s.readch <- frag // buffered 1000 so will not block til then.
						s.dropcopy_reads <- frag

					case fragerr := <-ckt.Errors:
						//zz("%v: (ckt '%v') fragerr = '%v'", name, ckt.Name, fragerr)
						_ = fragerr

					case <-ckt.Halt.ReqStop.Chan:
						//vv("%v: (ckt '%v') ckt halt requested.", name, ckt.Name)
						//s.gotCktHaltReq.Close()
						return

					case <-ckt.Context.Done():
						//vv("%v: (ckt '%v') done! cause: '%v'", name, ckt.Name, context.Cause(ctx))
						return
					case <-done0:
						//vv("%v: (ckt '%v') done0! reason: '%v'", name, ckt.Name, context.Cause(ctx0))
						return
						//case <-s.halt.ReqStop.Chan:
						//zz("%v: (ckt '%v') top func halt.ReqStop seen", name, ckt.Name)
						//	return
					}
				}

			}
			go passiveSide(rckt)

		case remoteURL := <-s.startCircuitWith:
			vv("%v: requested startCircuitWith: '%v'", name, remoteURL)
			ckt, _, err := myPeer.NewCircuitToPeerURL(fmt.Sprintf("cicuit-init-by:%v:%v", name, s.nextCktNo), remoteURL, nil, 0)
			s.nextCktNo++
			panicOn(err)
			s.incrementSends(ckt.Name)
			s.sendch <- nil
			s.dropcopy_sends <- nil

			var activeSide func(ckt *Circuit)
			activeSide = func(ckt *Circuit) {
				// this is the active side, as we called NewCircuitToPeerURL()
				ctx := ckt.Context
				for {
					select {
					case frag := <-s.startAnotherCkt:
						vv("%v: (ckt '%v') (active) startAnotherCkt requsted!: '%v'", name, ckt.Name, frag.FragSubject)
						ckt2, _, err := ckt.NewCircuit(frag.FragSubject, frag)
						panicOn(err)
						vv("%v: (ckt '%v') (active) created ckt2 to: '%v'", name, ckt.Name, ckt2.RemoteCircuitURL())
						go activeSide(ckt2)

					case <-ctx.Done():
						//vv("%v: (ckt '%v') (active) ctx.Done seen. cause: '%v'", name, ckt.Name, context.Cause(ctx))
						return
					case frag := <-ckt.Reads:
						seen := s.incrementReads(ckt.Name)
						vv("%v: (ckt '%v') (active) saw read! total= %v", name, ckt.Name, seen)
						s.readch <- frag
						s.dropcopy_reads <- frag

					case fragerr := <-ckt.Errors:
						_ = fragerr
						panic("not expecting errors yet!")
					case <-ckt.Halt.ReqStop.Chan:
						vv("%v: (ckt '%v') (active) ckt halt requested.", name, ckt.Name)
						return

					case frag := <-s.requestToSend:
						// external test code requests that we send.
						vv("%v: (ckt '%v') (active) got on requestToSend, sending to '%v'; from '%v'", name, ckt.Name, ckt.RemoteCircuitURL(), ckt.LocalCircuitURL())

						err := myPeer.SendOneWay(ckt, frag, 0)
						panicOn(err)
						s.incrementSends(ckt.Name)
						s.sendch <- frag
						s.dropcopy_sends <- frag

					}
				}
			}
			go activeSide(ckt)
		}
	}
	return nil
}

func Test409_lots_of_send_and_read(t *testing.T) {

	cv.Convey("many sends and reads between peers", t, func() {

		j := newTestJunk3("manysend_409")
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
			t.Fatalf("expected 1 open circuit on cli, got: '%v'", got)
		}
		if got, want := srv_lpb.OpenCircuitCount(), 1; got != want {
			t.Fatalf("expected 1 open circuit on srv, got: '%v'", got)
		}
		//vv("OK!")
		// send and read.

		// establish the baseline.

		// starting: client has read zero.
		if got, want := j.clis.getAllReads(), 0; got != want {
			t.Fatalf("expected %v reads to start, client got: %v", want, got)
		}
		// setting up the circuit means the server got a CallPeerStartCircuit frag.
		// to start with
		if got, want := j.srvs.getAllReads(), 1; got != want {
			t.Fatalf("expected %v reads to start, server got: %v", want, got)
		}
		j.srvs.reset() // set the server count to zero to start with.
		if got, want := j.srvs.getAllReads(), 0; got != want {
			t.Fatalf("expected %v reads after reset(), server got: %v", want, got)
		}

		// and the send side. verify at 1 then reset to zero
		if got, want := j.clis.getAllSends(), 1; got != want {
			t.Fatalf("expected %v sends to start, client got: %v", want, got)
		}
		j.clis.reset() // set the server count to zero to start with.
		if got, want := j.clis.getAllSends(), 0; got != want {
			t.Fatalf("expected %v sends after reset, cli got: %v", want, got)
		}
		if got, want := j.srvs.getAllSends(), 0; got != want {
			t.Fatalf("expected %v sends to start, server got: %v", want, got)
		}

		// and we can simply count the size of the readch, since it is buffered 1000
		if got, want := len(j.clis.readch), 0; got != want {
			t.Fatalf("expected %v in readch to start, clis got: %v", want, got)
		}
		if got, want := len(j.srvs.readch), 1; got != want {
			t.Fatalf("expected %v in readch to start, srvs got: %v", want, got)
		}
		if got, want := len(j.clis.sendch), 1; got != want {
			t.Fatalf("expected cli sendch to have %v, got: %v", want, got)
		}
		if got, want := len(j.srvs.sendch), 0; got != want {
			t.Fatalf("expected srv sendch to have %v, got: %v", want, got)
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
			t.Fatalf("expected cli readch to have %v, got: %v", want, got)
		}
		if got, want := len(j.srvs.readch), 1; got != want {
			t.Fatalf("expected srv readch to have %v, got: %v", want, got)
		}
		if got, want := len(j.clis.sendch), 1; got != want {
			t.Fatalf("expected cli sendch to have %v, got: %v", want, got)
		}
		if got, want := len(j.srvs.sendch), 1; got != want {
			t.Fatalf("expected srv sendch to have %v, got: %v", want, got)
		}

		vv("okay up to here.")

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
			t.Fatalf("expected cli readch to have %v, got: %v", want, got)
		}
		if got, want := len(j.srvs.readch), 2; got != want {
			t.Fatalf("expected srv readch to have %v, got: %v", want, got)
		}
		if got, want := len(j.clis.sendch), 2; got != want {
			t.Fatalf("expected cli sendch to have %v, got: %v", want, got)
		}
		if got, want := len(j.srvs.sendch), 1; got != want {
			t.Fatalf("expected srv sendch to have %v, got: %v", want, got)
		}

		vv("ask the client to start another circuit to the same remote.")
		drain(j.srvs.dropcopy_reads)

		frag = NewFragment()
		ckt2name := "client-started-2nd-ckt-to-same"
		frag.FragSubject = ckt2name
		j.clis.startAnotherCkt <- frag

		// prevent race with client starting circuit
		<-j.srvs.dropcopy_reads
		// we know the server has read another frag now.

		// verify open circuit count
		if got, want := cli_lpb.OpenCircuitCount(), 2; got != want {
			t.Fatalf("expected %v open circuit on cli, got: '%v'", want, got)
		}
		if got, want := srv_lpb.OpenCircuitCount(), 2; got != want {
			t.Fatalf("expected %v open circuit on srv, got: '%v'", want, got)
		}

		// assert readch state on sever has incremented.
		if got, want := len(j.clis.readch), 1; got != want {
			t.Fatalf("expected cli readch to have %v, got: %v", want, got)
		}
		if got, want := len(j.srvs.readch), 3; got != want {
			t.Fatalf("expected srv readch to have %v, got: %v", want, got)
		}
		if got, want := len(j.clis.sendch), 2; got != want {
			t.Fatalf("expected cli sendch to have %v, got: %v", want, got)
		}
		if got, want := len(j.srvs.sendch), 1; got != want {
			t.Fatalf("expected srv sendch to have %v, got: %v", want, got)
		}

		vv("ask the server to start (a third) circuit to the same remote.")

		drain(j.clis.dropcopy_reads)

		frag = NewFragment()
		ckt3name := "server-started-3rd-ckt-to-same"
		frag.FragSubject = ckt3name
		j.srvs.passiveSideStartAnotherCkt <- frag // passive specific? needed?

		// prevent race with client starting circuit
		<-j.clis.dropcopy_reads
		// we know the server has read another frag now.

		// verify open circuit count
		if got, want := cli_lpb.OpenCircuitCount(), 3; got != want {
			t.Fatalf("expected %v open circuit on cli, got: '%v'", want, got)
		}
		if got, want := srv_lpb.OpenCircuitCount(), 3; got != want {
			t.Fatalf("expected %v open circuit on srv, got: '%v'", want, got)
		}

		// assert readch state on client has incremented.
		if got, want := len(j.clis.readch), 2; got != want {
			t.Fatalf("expected cli readch to have %v, got: %v", want, got)
		}
		if got, want := len(j.srvs.readch), 3; got != want {
			t.Fatalf("expected srv readch to have %v, got: %v", want, got)
		}
		if got, want := len(j.clis.sendch), 2; got != want {
			t.Fatalf("expected cli sendch to have %v, got: %v", want, got)
		}
		if got, want := len(j.srvs.sendch), 1; got != want {
			t.Fatalf("expected srv sendch to have %v, got: %v", want, got)
		}

		//select {}

	})

}
func drain(ch chan *Fragment) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}
