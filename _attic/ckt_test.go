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
	stats  *Mutexmap[string, *counts]
	readch chan *Fragment
}

func newcountService() *countService {
	return &countService{
		stats:  NewMutexmap[string, *counts](),
		readch: make(chan *Fragment, 1000),
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
			//zz("%v: done0! cause: '%v'", name, ctx0.Cause())
			return ErrContextCancelled
			//case <-s.halt.ReqStop.Chan:
			//	//zz("%v: halt.ReqStop seen", name)
			//	return ErrHaltRequested

		// new Circuit connection arrives
		case peer := <-newCircuitCh:
			//vv("%v: got from newCircuitCh! service sees new peerURL: '%v'", name, peer.PeerURL)

			// talk to this peer on a separate goro if you wish; or just a func
			go func(peer *RemotePeer) {

				ckt, ctx, err := peer.IncomingCircuit()
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

				for {
					select {
					case frag := <-ckt.Reads:
						//vv("%v: (ckt %v) ckt.Reads sees frag:'%s'", name, ckt.Name, frag)
						_ = frag
						s.stats.Update(func(stats map[string]*counts) {
							c, ok := stats[ckt.Name]
							if !ok {
								c = &counts{}
								stats[ckt.Name] = c
							}
							c.reads++
						})
						s.readch <- frag // buffered 1000 so will not block til then.

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

			}(peer)
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
		cktname := "409ckt_first_ckt"

		ckt, _, err := cli_lpb.NewCircuitToPeerURL(cktname, srv_lpb.URL(), nil, 0)
		panicOn(err)
		defer ckt.Close()

		if got, want := cli_lpb.OpenCircuitCount(), 1; got != want {
			t.Fatalf("expected 1 open circuit on cli, got: '%v'", got)
		}
		// we can race with the server getting the read, so wait for a read.
		<-j.srvs.readch
		if got, want := srv_lpb.OpenCircuitCount(), 1; got != want {
			t.Fatalf("expected 1 open circuit on srv, got: '%v'", got)
		}

		// send and read.

		// starting: client has read zero.
		if got, want := j.clis.getAllReads(), 0; got != want {
			t.Fatalf("expected 0 reads to start, client got: %v", got)
		}
		// setting up the circuit means the server got a CallPeerStartCircuit frag.
		// to start with
		if got, want := j.srvs.getAllReads(), 1; got != want {
			t.Fatalf("expected 1 reads to start, server got: %v", got)
		}
		j.srvs.reset() // set the server count to zero to start with.
		if got, want := j.srvs.getAllReads(), 0; got != want {
			t.Fatalf("expected 0 reads to start, server got: %v", got)
		}

		// and we can simply count the size of the readch, since it is buffered 1000
		if got, want := len(j.clis.readch), 0; got != want {
			t.Fatalf("expected 0 in readch to start, clis got: %v", got)
		}
		if got, want := len(j.srvs.readch), 0; got != want {
			t.Fatalf("expected 0 in readch to start, srvs got: %v", got)
		}
	})

}
