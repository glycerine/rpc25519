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

	// have the start ack each send request here.
	dropcopy_send_req chan *Fragment
	//dropcopy_sent     chan *Fragment

	startCircuitWith chan string // remote URL to contact.
	nextCktNo        int
}

func newcountService() *countService {
	return &countService{
		stats:             NewMutexmap[string, *counts](),
		readch:            make(chan *Fragment, 1000),
		sendch:            make(chan *Fragment),
		dropcopy_send_req: make(chan *Fragment, 1000),
		//dropcopy_sent:     make(chan *Fragment, 1000),
		dropcopy_reads:   make(chan *Fragment, 1000),
		startCircuitWith: make(chan string),
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

func (s *countService) start(myPeer *LocalPeer, ctx0 context.Context, newCircuitCh <-chan *RemotePeer) error {

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

		// new Circuit connection arrives
		case rpeer := <-newCircuitCh:
			vv("%v: got from newCircuitCh! service sees new peerURL: '%v'", name, rpeer.PeerURL)

			// talk to this peer on a separate goro if you wish; or just a func
			go func(rpeer *RemotePeer) {

				ckt, ctx, err := rpeer.IncomingCircuit()
				if err != nil {
					//zz("%v: RemotePeer err from IncomingCircuit: '%v'; stopping", name, err)
					return
				}
				//vv("%v: (ckt '%v') got incoming ckt", name, ckt.Name)
				//s.gotIncomingCkt <- ckt
				//zz("%v: (ckt '%v') got past <-ckt for incoming ckt", name, ckt.Name)
				defer func() {
					//vv("%v: (ckt '%v') defer running! finishing new Circuit func.", name, ckt.Name) // seen on server
					ckt.Close()
					//s.gotCktHaltReq.Close()
				}()

				//zz("%v: (ckt '%v') <- got new IncomingCircuit", name, ckt.Name)
				////zz("IncomingCircuit got RemoteCircuitURL = '%v'", ckt.RemoteCircuitURL())
				////zz("IncomingCircuit got LocalCircuitURL = '%v'", ckt.LocalCircuitURL())
				//done := ctx.Done()

				// this is the passive side, as we <-newCircuitCh
				for {
					select {
					case frag := <-s.sendch:
						// external test code requests that we send.
						vv("%v: (ckt '%v') (passive) got on sendch, sending to '%v'; from '%v'", name, ckt.Name, ckt.RemoteCircuitURL(), ckt.LocalCircuitURL())

						err := rpeer.SendOneWay(ckt, frag, 0)
						panicOn(err)
						s.incrementSends(ckt.Name)
						s.dropcopy_send_req <- frag

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

					case <-ctx.Done():
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

			}(rpeer)

		case remoteURL := <-s.startCircuitWith:
			vv("%v: requested startCircuitWith: '%v'", name, remoteURL)
			ckt, ctx, err := myPeer.NewCircuitToPeerURL(fmt.Sprintf("cicuit-init-by:%v:%v", name, s.nextCktNo), remoteURL, nil, 0)
			s.nextCktNo++
			panicOn(err)
			go func() {
				// this is the active side, as we called NewCircuitToPeerURL()
				for {
					select {
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
					}
				}
			}()
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

		// and we can simply count the size of the readch, since it is buffered 1000
		if got, want := len(j.clis.readch), 0; got != want {
			t.Fatalf("expected %v in readch to start, clis got: %v", want, got)
		}
		if got, want := len(j.srvs.readch), 1; got != want {
			t.Fatalf("expected %v in readch to start, srvs got: %v", want, got)
		}

		// have server send 1 to client.
		frag := NewFragment()
		frag.FragPart = 0
		j.srvs.sendch <- frag

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

	})

}
