package rpc25519

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// PeerImpl is used in testing and also demonstrates
// how a user can implement a Peer.
// Users write a PeerServiceFunc,
// here called Start(), which can be a method
// to keep state in a struct, as PeerImpl
// does.
//
//msgp:ignore PeerImpl
type PeerImpl struct {
	KnownPeers           []string
	StartCount           atomic.Int64 `msg:"-"`
	DoEchoToThisPeerURL  chan string
	ReportEchoTestCanSee chan string
}

// Start is an example of PeerServiceFunc in action.
func (me *PeerImpl) Start(

	// how we were registered/invoked.
	// our local Peer interface, can do
	// NewCircuitToPeerURL() to send to URL.
	myPeer *LocalPeer,

	// overall context of Client/Server host, if
	// it starts shutdown, this will cancel, and
	// we should cleanup and return; as should any
	// child goroutines.
	ctx0 context.Context,

	newCircuitCh <-chan *Circuit,

) error {

	nStart := me.StartCount.Add(1)
	_ = nStart
	//vv("PeerImpl.Start() top. ourID = '%v'; peerServiceName='%v'; StartCount = %v", myPeer.ID(), myPeer.ServiceName(), nStart)

	var wg sync.WaitGroup
	defer wg.Wait() // wait for everyone to shutdown/catch stragglers that don't by hanging.

	done0 := ctx0.Done()

	for {
		select {
		// URL format: tcp://x.x.x.x:port/peerServiceName/peerID/circuitID
		case echoToURL := <-me.DoEchoToThisPeerURL:

			go func(echoToURL string) {

				defer func() {
					vv("echo starter shutting down.")
				}()

				outFrag := NewFragment()
				outFrag.Payload = []byte(fmt.Sprintf("echo request! "+
					"myPeer.ID='%v' (myPeer.PeerURL='%v') requested"+
					" to echo to peerURL '%v' on 'echo circuit'",
					myPeer.PeerID, myPeer.URL(), echoToURL))

				outFrag.FragSubject = "echo request"

				//vv("about to send echo from myPeer.URL() = '%v'", myPeer.URL())
				//vv("... and about to send echo to echoToURL  = '%v'", echoToURL)

				aliasRegister(myPeer.PeerID, myPeer.PeerID+
					" (echo originator on server)")
				_, _, remotePeerID, _, err := ParsePeerURL(echoToURL)
				panicOn(err)
				aliasRegister(remotePeerID, remotePeerID+" (echo replier on client)")

				circuitName := "echo-circuit"
				ckt, ctx, err := myPeer.NewCircuitToPeerURL(circuitName, echoToURL, outFrag, 0)
				panicOn(err)
				defer ckt.Close() // close when echo heard.
				done := ctx.Done()

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

			}(echoToURL)
		// new Circuit connection arrives
		case ckt := <-newCircuitCh:
			wg.Add(1)

			vv("got from newCircuitCh! '%v' sees new peerURL: '%v' ...\n   we want to be the echo-answer!",
				ckt.RemoteServiceName, ckt.RemoteCircuitURL())

			// talk to this peer on a separate goro if you wish:
			go func(ckt *Circuit) {
				defer wg.Done()
				defer func() {
					vv("echo answerer shutting down.")
				}()

				myurl := myPeer.URL()

				ctx := ckt.Context
				vv("incoming ckt has RemoteCircuitURL = '%v'", ckt.RemoteCircuitURL())
				vv("incoming ckt has LocalCircuitURL = '%v'", ckt.LocalCircuitURL())

				// good: myPeer.URL() matches the LocalCircuitURL,
				// but of course without the Call/CircuitID.
				//vv("compare myPeer.PeerURL = '%v'", myurl)
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
							err := ckt.SendOneWay(outFrag, 0)
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

			}(ckt)

		case <-done0:
			return ErrContextCancelled
		}
	}
	return nil
}
