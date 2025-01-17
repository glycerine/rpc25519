package rpc25519

import (
	"context"
	"fmt"
	//"os"
	//"strings"
	"testing"
	//"time"

	cv "github.com/glycerine/goconvey/convey"
)

var _ = fmt.Sprintf

type testJunk3 struct {
	name string
	cfg  *Config
	srv  *Server
	cli  *Client

	cliServiceName string
	srvServiceName string

	f PeerServiceFunc
}

func (j *testJunk3) cleanup() {
	j.cli.Close()
	j.srv.Close()
}

func newTestJunk3(name string, f PeerServiceFunc) (j *testJunk3) {

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

	//srvSync := newSyncer2(j.srvServiceName)

	err = srv.PeerAPI.RegisterPeerServiceFunc(j.srvServiceName, f)
	panicOn(err)

	err = cli.PeerAPI.RegisterPeerServiceFunc(j.cliServiceName, f)
	panicOn(err)

	j.cli = cli
	j.srv = srv
	j.cfg = cfg

	j.f = f

	return j
}

func Test409_lots_of_send_and_read(t *testing.T) {

	cv.Convey("testing peer2.go: many sends and reads", t, func() {

		start := func(myPeer *LocalPeer, ctx0 context.Context, newCircuitCh <-chan *RemotePeer) error {
			name := myPeer.PeerServiceName

			defer func() {
				vv("%v: end of start() inside defer, about the return/finish", name)
				//s.halt.Done.Close()
			}()

			vv("%v: start() top.", name)
			//zz("%v: ourID = '%v'; peerServiceName='%v';", name, myPeer.ID(), myPeer.ServiceName())

			aliasRegister(myPeer.PeerID, myPeer.PeerID+" ("+myPeer.ServiceName()+")")

			done0 := ctx0.Done()

			for {
				vv("%v: top of select", name)
				select {
				case <-done0:
					//zz("%v: done0! cause: '%v'", name, ctx0.Cause())
					return ErrContextCancelled
					//case <-s.halt.ReqStop.Chan:
					//	//zz("%v: halt.ReqStop seen", name)
					//	return ErrHaltRequested

				// new Circuit connection arrives
				case peer := <-newCircuitCh:
					vv("%v: got from newCircuitCh! service sees new peerURL: '%v'", name, peer.PeerURL)

					// talk to this peer on a separate goro if you wish; or just sep func
					func(peer *RemotePeer) {

						ckt, ctx, err := peer.IncomingCircuit()
						if err != nil {
							//zz("%v: RemotePeer err from IncomingCircuit: '%v'; stopping", name, err)
							return
						}
						vv("%v: (ckt '%v') got incoming ckt", name, ckt.Name)
						//s.gotIncomingCkt <- ckt
						//zz("%v: (ckt '%v') got past <-ckt for incoming ckt", name, ckt.Name)
						defer func() {
							vv("%v: (ckt '%v') defer running! finishing new Circuit func.", name, ckt.Name) // seen on server
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
								//zz("%v: (ckt %v) ckt.Reads sees frag:'%s'", name, ckt.Name, frag)

								//s.gotIncomingCktReadFrag <- frag
								//zz("%v: (ckt %v) past s.gotIncomingCktReadFrag <- frag. frag:'%s'", name, ckt.Name, frag)

								outFrag := NewFragment()
								outFrag.Payload = frag.Payload
								outFrag.FragSubject = "echo reply"
								outFrag.ServiceName = myPeer.ServiceName()
								//zz("%v: (ckt '%v') sending 'echo reply'='%v'", name, ckt.Name, frag)
								err := peer.SendOneWay(ckt, outFrag, 0)
								panicOn(err)

							case fragerr := <-ckt.Errors:
								//zz("%v: (ckt '%v') fragerr = '%v'", name, ckt.Name, fragerr)
								_ = fragerr

							case <-ckt.Halt.ReqStop.Chan:
								vv("%v: (ckt '%v') ckt halt requested.", name, ckt.Name)
								//s.gotCktHaltReq.Close()
								return

							case <-ctx.Done():
								vv("%v: (ckt '%v') done! cause: '%v'", name, ckt.Name, context.Cause(ctx))
								return
							case <-done0:
								vv("%v: (ckt '%v') done0! reason: '%v'", name, ckt.Name, context.Cause(ctx0))
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
		} // end of start()

		j := newTestJunk3("manysend_409", start)
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

		cliNumCkt := cli_lpb.OpenCircuitCount()
		if cliNumCkt != 1 {
			t.Fatalf("expected 1 open circuits, got cliNumCkt: '%v'", cliNumCkt)
		}
		srvNumCkt := srv_lpb.OpenCircuitCount()
		if srvNumCkt != 0 {
			t.Fatalf("expected 1 open circuits, got srvNumCkt: '%v'", srvNumCkt)
		}

		ckt.Close()
	})

}
