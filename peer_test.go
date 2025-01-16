package rpc25519

import (
	"context"
	//"fmt"
	//"os"
	//"strings"
	"testing"
	//"time"

	cv "github.com/glycerine/goconvey/convey"
)

func Test404_verify_peer_operations(t *testing.T) {

	cv.Convey("verify all the basic peer operations are functional", t, func() {

		cfg := NewConfig()
		cfg.TCPonly_no_TLS = true

		cfg.ServerAddr = "127.0.0.1:0"
		srv := NewServer("srv_test403", cfg)

		serverAddr, err := srv.Start()
		panicOn(err)
		defer srv.Close()

		cfg.ClientDialToHostPort = serverAddr.String()
		cli, err := NewClient("client_test403", cfg)
		panicOn(err)
		err = cli.Start()
		panicOn(err)
		defer cli.Close()

		//preventRaceByDoingPriorClientToServerRoundTrip(cli, srv)
		ctx, canc := context.WithCancel(context.Background())
		_ = canc

		srvSync := newSyncer("srvSync")

		err = srv.PeerAPI.RegisterPeerServiceFunc("srvSync", srvSync.Start)
		panicOn(err)

		cliSync := newSyncer("cliSync")

		cliAddr := cli.LocalAddr()
		_ = cliAddr

		cli.PeerAPI.RegisterPeerServiceFunc("cliSync", cliSync.Start)

		peerURL_cli, peerID_cli, err := cli.PeerAPI.StartLocalPeer(ctx, "cliSync")
		panicOn(err)
		vv("cli.PeerAPI.StartLocalPeer() on client peerURL_client = '%v'; peerID_client = '%v'", peerURL_cli, peerID_cli)

		srvURL := cli.RemoteAddr() + "/srvSync"
		cliSync.PushToPeerURL <- srvURL

		vv("past cliSync.PushToPeerURL <- srvURL")
		vv("test main about to select{}")
		select {}
	})
}

func newSyncer(name string) *syncer {
	return &syncer{
		name:          name,
		PushToPeerURL: make(chan string),
	}
}

type syncer struct {
	name          string
	PushToPeerURL chan string
}

func (me *syncer) Start(
	myPeer LocalPeer,
	ctx0 context.Context,
	newPeerCh <-chan RemotePeer,

) error {

	vv("syncer.Start() top. name '%v'", me.name)
	vv("ourID = '%v'; peerServiceName='%v';", myPeer.ID(), myPeer.ServiceName())

	done0 := ctx0.Done()

	for {
		select {
		// URL format: tcp://x.x.x.x:port/peerServiceName
		case pushToURL := <-me.PushToPeerURL:
			vv("%v sees pushToURL '%v'", me.name, pushToURL)

			//go func(pushToURL string) {
			/*
				defer func() {
					vv("echo starter shutting down.")
				}()

				outFrag := NewFragment()
				outFrag.Payload = []byte(fmt.Sprintf("echo request! "+
					"myPeer.ID='%v' (myPeer.PeerURL='%v') requested"+
					" to echo to peerURL '%v' on 'echo circuit'",
					myPeer.ID(), myPeer.URL(), pushToURL))

				outFrag.FragSubject = "echo request"

				//vv("about to send echo from myPeer.URL() = '%v'", myPeer.URL())
				//vv("... and about to send echo to pushToURL  = '%v'", pushToURL)

				aliasRegister(myPeer.ID(), myPeer.ID()+
					" (echo originator on server)")
				_, _, remotePeerID, _, err := parsePeerURL(pushToURL)
				panicOn(err)
				aliasRegister(remotePeerID, remotePeerID+" (echo replier on client)")
			*/

			ckt, ctx, err := myPeer.NewCircuitToPeerURL(pushToURL, nil, nil)
			panicOn(err)
			defer ckt.Close() // close when echo heard.
			done := ctx.Done()
			_ = done

			/*
				for {
					select {
					case frag := <-ckt.Reads:
						vv("echo circuit got read frag back: '%v'", frag.String())
						if frag.FragSubject == "echo reply" {
							vv("seen echo reply, ack and shutdown")
							me.ReportEchoTestCanSee <- string(frag.Payload)
							return
						}
					case fragerr := <-ckt.Errors:
						vv("echo circuit got error fragerr back: '%#v'", fragerr)

					case <-done:
						return
					case <-done0:
						return
					}
				}
			*/
			//}(pushToURL)
		// new Circuit connection arrives
		case peer := <-newPeerCh:

			vv("got from newPeerCh! '%v' sees new peerURL: '%v' ...\n   we want to be the echo-answer!",
				peer.PeerServiceName(), peer.PeerURL())

			// talk to this peer on a separate goro if you wish:
			go func(peer RemotePeer) {
				defer func() {
					vv("echo answerer shutting down.")
				}()

				myurl := myPeer.URL()

				ckt, ctx := peer.IncomingCircuit()
				vv("IncomingCircuit got RemoteCircuitURL = '%v'", ckt.RemoteCircuitURL())
				vv("IncomingCircuit got LocalCircuitURL = '%v'", ckt.LocalCircuitURL())

				// good: myPeer.URL() matches the LocalCircuitURL,
				// but of course without the Call/CircuitID.
				vv("compare myPeer.PeerURL = '%v'", myurl)
				done := ctx.Done()

				// this is racing (just in our test, not data race)
				// with our echo?, so skip it for now. Simpler.
				/*
					outFrag := NewFragment()
					// set Payload, other details...
					outFrag.Payload = []byte(fmt.Sprintf("hello from '%v'", myPeer.URL()))

					err := peer.SendOneWay(ckt, outFrag, nil)
					panicOn(err)
				*/
				for {
					select {
					case frag := <-ckt.Reads:
						vv("ckt.Reads sees frag:'%s';  I am myurl= '%v'", frag, myurl)
						if frag.FragSubject == "echo request" {
							outFrag := NewFragment()
							outFrag.Payload = frag.Payload
							outFrag.FragSubject = "echo reply"
							outFrag.ServiceName = myPeer.ServiceName()
							vv("ckt.Reads sees frag with echo request! sending reply='%v'", frag)
							err := peer.SendOneWay(ckt, outFrag, nil)
							panicOn(err)
						}

					case fragerr := <-ckt.Errors:
						vv("fragerr = '%v'", fragerr)

					case <-done:
						return
					case <-done0:
						return
					}
				}

			}(peer)

		case <-done0:
			return ErrContextCancelled
		}
	}
	return nil
}
