package rpc25519

import (
	"context"
	"fmt"
	"os"
	//"strings"
	"testing"
	"time"

	cv "github.com/glycerine/goconvey/convey"
	"github.com/glycerine/idem"
)

var _ = fmt.Sprintf

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
	cfg.QuietTestMode = true

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

	readErrors int
	sendErrors int
}

type countService struct {
	halt *idem.Halter

	// key is circuit name
	stats *Mutexmap[string, *counts]

	// keep a record of all reads, so test can assert on history at the end.
	readch chan *Fragment
	// and copy all reads to here so test avoid races.
	dropcopy_reads chan *Fragment

	// same for errors
	read_errorch         chan *Fragment
	read_dropcopy_errors chan *Fragment
	send_errorch         chan *Fragment
	send_dropcopy_errors chan *Fragment

	// ask the start() func to send to remote.
	sendch chan *Fragment

	requestToSend chan *Fragment

	// have the start ack each send request here.
	dropcopy_sends chan *Fragment
	//dropcopy_sent     chan *Fragment

	activeSideStartAnotherCkt  chan *Fragment
	passiveSideStartAnotherCkt chan *Fragment

	passiveSideSendN chan int
	activeSideSendN  chan int

	startCircuitWith chan string // remote URL to contact.
	nextCktNo        int

	activeSideSendCktError  chan string
	passiveSideSendCktError chan string

	activeSideShutdownCkt                chan *Fragment
	activeSideShutdownCktAckReq          chan *Fragment
	passive_side_ckt_saw_remote_shutdown chan *Fragment

	passiveSideShutdownCkt              chan *Fragment
	passiveSideShutdownCktAckReq        chan *Fragment
	active_side_ckt_saw_remote_shutdown chan *Fragment
}

func newcountService() *countService {
	return &countService{
		halt:   idem.NewHalterNamed("countService"),
		stats:  NewMutexmap[string, *counts](),
		readch: make(chan *Fragment, 1000),
		sendch: make(chan *Fragment, 1000),

		read_errorch:         make(chan *Fragment, 1000),
		read_dropcopy_errors: make(chan *Fragment, 1000),
		send_errorch:         make(chan *Fragment, 1000),
		send_dropcopy_errors: make(chan *Fragment, 1000),

		requestToSend:    make(chan *Fragment),
		dropcopy_sends:   make(chan *Fragment, 1000),
		dropcopy_reads:   make(chan *Fragment, 1000),
		startCircuitWith: make(chan string),

		activeSideStartAnotherCkt:  make(chan *Fragment),
		passiveSideStartAnotherCkt: make(chan *Fragment),

		passiveSideSendN: make(chan int),
		activeSideSendN:  make(chan int),

		activeSideSendCktError:  make(chan string),
		passiveSideSendCktError: make(chan string),

		activeSideShutdownCkt:                make(chan *Fragment),
		activeSideShutdownCktAckReq:          make(chan *Fragment, 1000),
		passive_side_ckt_saw_remote_shutdown: make(chan *Fragment, 1000),

		passiveSideShutdownCkt:              make(chan *Fragment),
		passiveSideShutdownCktAckReq:        make(chan *Fragment, 1000),
		active_side_ckt_saw_remote_shutdown: make(chan *Fragment, 1000),
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
func (s *countService) getAllReadErrors() (n int) {
	countsSlice := s.stats.GetValSlice()
	for _, count := range countsSlice {
		n += count.readErrors
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
func (s *countService) getAllSendErrors() (n int) {
	countsSlice := s.stats.GetValSlice()
	for _, count := range countsSlice {
		n += count.sendErrors
	}
	return
}

func (s *countService) incrementReadErrors(cktName string) (tot int) {
	s.stats.Update(func(stats map[string]*counts) {
		c, ok := stats[cktName]
		if !ok {
			c = &counts{}
			stats[cktName] = c
		}
		c.sendErrors++
		tot = c.sendErrors
	})
	return
}
func (s *countService) incrementSendErrors(cktName string) (tot int) {
	s.stats.Update(func(stats map[string]*counts) {
		c, ok := stats[cktName]
		if !ok {
			c = &counts{}
			stats[cktName] = c
		}
		c.readErrors++
		tot = c.readErrors
	})
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

func (s *countService) start(myPeer *LocalPeer, ctx0 context.Context, newCircuitCh <-chan *Circuit) (err0 error) {

	name := fmt.Sprintf("%v-%v", myPeer.PeerServiceName, cryptoRandPositiveInt64())
	_ = name // used when logging is on.

	defer func() {
		//vv("%v: end of start() inside defer, about the return/finish", name)
		s.halt.ReqStop.Close()
		s.halt.Done.Close()
		myPeer.Close()
	}()

	////vv("%v: start() top.", name)
	//zz("%v: ourID = '%v'; peerServiceName='%v';", name, myPeer.ID(), myPeer.ServiceName())

	AliasRegister(myPeer.PeerID, myPeer.PeerID+" ("+myPeer.ServiceName()+")")

	done0 := ctx0.Done()

	for {
		//vv("%v: top of select, myPeer.ServiceName='%v'", name, myPeer.ServiceName())
		select {
		case <-done0:
			//vv("%v: done0! cause: '%v'", name, context.Cause(ctx0))
			return ErrContextCancelled
			//case <-s.halt.ReqStop.Chan:
			//	//zz("%v: halt.ReqStop seen", name)
			//	return ErrHaltRequested

			// new Circuit connection arrives => we are the passive side for it.
		case rckt := <-newCircuitCh:
			//vv("%v: newCircuitCh got rckt! service sees new peerURL: '%v'", name, rckt.RemoteCircuitURL()) // not seen 410

			// talk to this peer on a separate goro if you wish; or just a func
			var passiveSide func(ckt *Circuit) error
			passiveSide = func(ckt *Circuit) (err0 error) {
				////vv("%v: (ckt '%v') got incoming ckt", name, ckt.Name)
				//s.gotIncomingCkt <- ckt
				//zz("%v: (ckt '%v') got past <-ckt for incoming ckt", name, ckt.Name)
				defer func() {
					////vv("%v: (ckt '%v') defer running! finishing new Circuit func. stack=\n'%v'", name, ckt.Name, stack()) // seen on server
					ckt.Close(err0)
					s.passive_side_ckt_saw_remote_shutdown <- nil
				}()

				//zz("%v: (ckt '%v') <- got new incoming ckt", name, ckt.Name)
				////zz("incoming ckt has RemoteCircuitURL = '%v'", ckt.RemoteCircuitURL())
				////zz("incoming ckt has LocalCircuitURL = '%v'", ckt.LocalCircuitURL())
				//done := ctx.Done()

				// this is the passive side, as we <-newCircuitCh
				for {
					select {
					case <-s.passiveSideShutdownCkt:

						return
					case errReq := <-s.passiveSideSendCktError:
						frag := NewFragment()
						frag.Err = errReq
						err := ckt.SendOneWay(frag, 0, 0)
						panicOn(err)
						s.incrementSends(ckt.Name)
						s.sendch <- frag
						s.dropcopy_sends <- frag

					case n := <-s.passiveSideSendN:
						//vv("%v: (ckt '%v') (passive) passiveSideSendN = %v requsted!: '%v'", name, ckt.Name, n)
						for i := range n {
							frag := NewFragment()
							frag.FragPart = int64(i)
							err := ckt.SendOneWay(frag, 0, 0)
							panicOn(err)
							s.incrementSends(ckt.Name)
							s.sendch <- frag
							s.dropcopy_sends <- frag
						}

					case frag := <-s.passiveSideStartAnotherCkt:

						//vv("%v: (ckt '%v') (passive) passiveSideStartAnotherCkt requsted!: '%v'", name, ckt.Name, frag.FragSubject)
						ckt2, _, err := ckt.NewCircuit(frag.FragSubject, frag)
						panicOn(err)
						//vv("%v: (ckt '%v') (passive) created ckt2 to: '%v'", name, ckt.Name, ckt2.RemoteCircuitURL())
						go passiveSide(ckt2)

					case frag := <-s.requestToSend:
						// external test code requests that we send.
						//vv("%v: (ckt '%v') (passive) got requestToSend, sending to '%v'; from '%v'", name, ckt.Name, ckt.RemoteCircuitURL(), ckt.LocalCircuitURL())

						err := ckt.SendOneWay(frag, 0, 0)
						//panicOn(err)
						if err != nil {
							// shutdown
							return
						}
						s.incrementSends(ckt.Name)
						s.sendch <- frag
						s.dropcopy_sends <- frag

					case frag := <-ckt.Reads:
						_ = frag
						tot := s.incrementReads(ckt.Name)
						_ = tot
						//vv("%v: (ckt %v) (passive) ckt.Reads (total %v) sees frag:'%s'", name, ckt.Name, tot, frag)
						s.readch <- frag // buffered 1000 so will not block til then.
						s.dropcopy_reads <- frag

					case fragerr := <-ckt.Errors:
						//vv("%v: (ckt '%v') fragerr = '%v'", name, ckt.Name, fragerr) // not seen 410
						_ = fragerr

						tot := s.incrementReadErrors(ckt.Name)
						_ = tot
						//vv("%v: (ckt %v) (passive) ckt.Errors (total %v) sees fragerr:'%s'", name, ckt.Name, tot, fragerr)

						s.read_errorch <- fragerr
						s.read_dropcopy_errors <- fragerr

					case <-ckt.Halt.ReqStop.Chan:
						////vv("%v: (ckt '%v') ckt halt requested.", name, ckt.Name)
						//s.gotCktHaltReq.Close()
						return

					case <-ckt.Context.Done():
						//vv("%v: (ckt '%v') done! cause: '%v'", name, ckt.Name, context.Cause(ckt.Context))
						return
					case <-done0:
						////vv("%v: (ckt '%v') done0! reason: '%v'", name, ckt.Name, context.Cause(ctx0))
						return
						//case <-s.halt.ReqStop.Chan:
						//zz("%v: (ckt '%v') top func halt.ReqStop seen", name, ckt.Name)
						//	return
					}
				}

			}
			go passiveSide(rckt)

		case remoteURL := <-s.startCircuitWith:
			//vv("%v: requested startCircuitWith: '%v'", name, remoteURL) // not seen 410
			ckt, _, err := myPeer.NewCircuitToPeerURL(fmt.Sprintf("cicuit-init-by:%v:%v", name, s.nextCktNo), remoteURL, nil, 0)
			s.nextCktNo++
			//panicOn(err)
			if err != nil {
				// shutdown
				return
			}
			s.incrementSends(ckt.Name)
			s.sendch <- nil
			s.dropcopy_sends <- nil

			var activeSide func(ckt *Circuit) error
			activeSide = func(ckt *Circuit) (err0 error) {
				// this is the active side, as we called NewCircuitToPeerURL()
				defer func() {
					////vv("%v: active side ckt '%v' shutting down", name, ckt.Name)
					ckt.Close(err0)
					s.activeSideShutdownCktAckReq <- nil
					s.active_side_ckt_saw_remote_shutdown <- nil
				}()
				ctx := ckt.Context
				for {
					//vv("top of active side select") // not seen 410
					select {
					case <-s.activeSideShutdownCkt:
						return nil

					case errReq := <-s.activeSideSendCktError:
						frag := NewFragment()
						frag.Err = errReq
						err := ckt.SendOneWay(frag, 0, 0)
						//panicOn(err)
						if err != nil {
							// shutdown
							return
						}
						s.incrementSends(ckt.Name)
						s.sendch <- frag
						s.dropcopy_sends <- frag

					case n := <-s.activeSideSendN:
						//vv("%v: (ckt '%v') (active) activeSideSendN = %v requsted!: '%v'", name, ckt.Name, n)
						for i := range n {
							frag := NewFragment()
							frag.FragPart = int64(i)
							err := ckt.SendOneWay(frag, 0, 0)
							//panicOn(err)
							if err != nil {
								// shutdown
								return
							}
							s.incrementSends(ckt.Name)
							s.sendch <- frag
							s.dropcopy_sends <- frag
						}

					case frag := <-s.activeSideStartAnotherCkt:
						//vv("%v: (ckt '%v') (active) activeSideStartAnotherCkt requsted!: '%v'", name, ckt.Name, frag.FragSubject)
						ckt2, _, err := ckt.NewCircuit(frag.FragSubject, frag)
						panicOn(err)
						//vv("%v: (ckt '%v') (active) created ckt2 to: '%v'", name, ckt.Name, ckt2.RemoteCircuitURL())
						go activeSide(ckt2)

					case <-ctx.Done():
						////vv("%v: (ckt '%v') (active) ctx.Done seen. cause: '%v'", name, ckt.Name, context.Cause(ctx))
						return
					case frag := <-ckt.Reads:
						seen := s.incrementReads(ckt.Name)
						_ = seen
						//vv("%v: (ckt '%v') (active) saw read! total= %v", name, ckt.Name, seen)
						s.readch <- frag
						s.dropcopy_reads <- frag

					case fragerr := <-ckt.Errors:

						tot := s.incrementReadErrors(ckt.Name)
						_ = tot
						//vv("%v: (ckt %v) (active) ckt.Errors (total %v) sees fragerr:'%s'", name, ckt.Name, tot, fragerr)
						s.read_errorch <- fragerr
						s.read_dropcopy_errors <- fragerr

					case <-ckt.Halt.ReqStop.Chan:
						//vv("%v: (ckt '%v') (active) ckt halt requested.", name, ckt.Name)
						return

					case frag := <-s.requestToSend:
						// external test code requests that we send.
						//vv("%v: (ckt '%v') (active) got on requestToSend, sending to '%v'; from '%v'", name, ckt.Name, ckt.RemoteCircuitURL(), ckt.LocalCircuitURL())

						err := myPeer.SendOneWay(ckt, frag, 0, 0)
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

	if faketime {
		t.Skip("skip under synctest, net calls will never settle.")
		return
	}
	if _, rr := os.LookupEnv("RUNNING_UNDER_RR"); rr {
		t.Skip("flaky under rr chaos")
	}

	cv.Convey("many sends and reads between peers", t, func() {

		j := newTestJunk3("manysend_409")
		defer j.cleanup()

		ctx := context.Background()
		cli_lpb, err := j.cli.PeerAPI.StartLocalPeer(ctx, j.cliServiceName, nil, "")
		panicOn(err)
		defer cli_lpb.Close()

		srv_lpb, err := j.srv.PeerAPI.StartLocalPeer(ctx, j.srvServiceName, nil, "")
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

		// 5 sec still flaky under rr chaos, turn off above.
		timeout := j.cli.NewTimer(2 * time.Second)
		select {
		case <-j.clis.dropcopy_reads:
		case <-timeout.C:
			// don't care, crashing: timeout.Discard()
			t.Fatalf("client did not get their dropcopy after 2 sec")
		}
		timeout.Discard()
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
			t.Fatalf("error: expected srv sendch to have %v, got: %v", want, got) // ckt_test.go:595: error: expected srv sendch to have 1, got: 0
		}

		//vv("okay up to here.")

		// have client send 1 to server.
		frag = NewFragment()
		frag.FragPart = 1
		drain(j.srvs.dropcopy_reads)
		j.clis.requestToSend <- frag

		// wait for it to get the server
		timeout = j.srv.NewTimer(2 * time.Second)
		select {
		case <-j.srvs.dropcopy_reads:
		case <-timeout.C:
			t.Fatalf("server did not get their dropcopy after 2 sec")
		}
		timeout.Discard()

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

// drain is a helper to prep for the next part of the test.
// drain helps prevents races by emptying a channel's buffer.
func drain(ch chan *Fragment) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}
