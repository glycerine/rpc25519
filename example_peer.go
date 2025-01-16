package rpc25519

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

/*
~~~

Peer/Circuit/Fragment Design overview

Motivated by filesystem syncing, we envision a system that can
both stream efficiently and utilize the same code
on the client as on the server.

Syncing a filesystem needs efficient stream transmission.
The total data far exceeds what will fit in any single
message, and updates may be continuous or lumpy.
We don't want to wait for one "call"
to finish its round trip. We just want to
send data when we have it. Hence the
API is based on one-way messages and is asynchronous
in that the methods and channels involved do
not wait for network round trips to complete.

Once established, a circuit between peers
is designed to persist until deliberately closed.
A circuit can then handle any number of Fragments
of data during its lifetime.

To organize communications, a peer can maintain
multiple circuits, either with the same peer
or with any number of other peers. We can then
easily handle any arbitrary network topology.

Even between just two peers, multiple persistent
channels facilities code organization. One
could use a channel per file being synced,
for instance. Multiple large files being
scanned and their diffs streamed at once,
in parallel, becomes practical.

By using lightweight goroutines and channels,
circuit persistence is inexpensive and can be
facilities data streams with markedly
different lifetimes and update rates,
over long periods.

Symmetry of code deployment is also a natural
requirement. This is the git model. When syncing
two repositories, the operations needed are
the same on both sides, no matter who
initiated or whether a push or pull was
requested. Hence we want a way to register
the same functionality on the client as on the server.

Peer/Circuit/Fragment API essentials (utility methods omitted for compactness)

A) To establish circuits with new peers, use

   1) NewCircuitToPeerURL() for initiating a new circuit to a new peer.
   2) <-newPeerCh to recieve new initiations;
      then use the IncomingCircuit() method to get the Circuit.

B) To create additional circuits with an already connected peer:
   1) NewCircuit adds a new circuit with an existing RemotePeer, no URL needed.
   2) They get notified on <-newPeerCh too. (verify)

C) To communicate over a Circuit:
   1) get regular messages (called Fragments) from <-Circuit.Reads
   2) get error messages from <-Circuit.Errors
   3) send messages with SendOneWay(). It never blocks.
   4) Close() the circuit and the peer's ctx will be cancelled. (verify)

type Circuit struct {
	Reads  <-chan *Fragment
	Errors <-chan *Fragment
    Close() // when done
}
type LocalPeer interface {
	NewCircuitToPeerURL(peerURL string, frag *Fragment,
         errWriteDur *time.Duration) (ckt *Circuit, ctx context.Context, err error)
}
type RemotePeer interface {
	IncomingCircuit() (ckt *Circuit, ctx context.Context) // gets the first.
	NewCircuit()      (ckt *Circuit, ctx context.Context) // make 2nd, 3rd...
	SendOneWay(ckt *Circuit, frag *Fragment, errWriteDur *time.Duration) error
}
type PeerServiceFunc func(myPeer LocalPeer, ctx0 context.Context, newPeerCh <-chan RemotePeer) error

type Fragment struct {
           // system metadata
	  FromPeerID string
	    ToPeerID string
	   CircuitID string
	      Serial int64
	         Typ CallType
	 ServiceName string

           // user supplied data
          FragOp int
	 FragSubject string
	    FragPart int64
	        Args map[string]string
	     Payload []byte
	         Err string
}

D) boostrapping: registering your Peer implemenation and starting
    them up (from outside the PeerServiceFunc callback). The PeerAPI
    is available via Client.PeerAPI or Server.PeerAPI.
    The same facilities are available to peers running on either.

   1. register:

      PeerAPI.RegisterPeerServiceFunc(peerServiceName string, peer PeerServiceFunc) error

   2. start a previously registered PeerServiceFunc locally or remotely:

          PeerAPI.StartLocalPeer(
                   ctx context.Context,
          peerServiceName string) (localPeerURL, localPeerID string, err error)

      Starting a remote peer must also specify the host:port remoteAddr
      of the remote client/server. The user can call the RemoteAddr() and
      LocalAddr() methods on the Client/Server to obtain these.

          PeerAPI.StartRemotePeer(
                       ctx context.Context,
           peerServiceName string,
                remoteAddr string, // host:port
                  waitUpTo time.Duration,
                              ) (remotePeerURL, remotePeerID string, err error)

       The returned URLs can be used in myPeer.NewCircuitToPeerURL() calls
       inside the PeerServiceFunc.
~~~
*/

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
	myPeer LocalPeer,

	// overall context of Client/Server host, if
	// it starts shutdown, this will cancel, and
	// we should cleanup and return; as should any
	// child goroutines.
	ctx0 context.Context,

	// first on newPeerCh might be the client or server who invoked us,
	// if a remote wanted to start a circuit.
	newPeerCh <-chan RemotePeer,

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
					myPeer.ID(), myPeer.URL(), echoToURL))

				outFrag.FragSubject = "echo request"

				//vv("about to send echo from myPeer.URL() = '%v'", myPeer.URL())
				//vv("... and about to send echo to echoToURL  = '%v'", echoToURL)

				aliasRegister(myPeer.ID(), myPeer.ID()+
					" (echo originator on server)")
				_, _, remotePeerID, _, err := parsePeerURL(echoToURL)
				panicOn(err)
				aliasRegister(remotePeerID, remotePeerID+" (echo replier on client)")

				circuitName := "echo-circuit"
				ckt, ctx, err := myPeer.NewCircuitToPeerURL(circuitName, echoToURL, outFrag, nil)
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
		case peer := <-newPeerCh:
			wg.Add(1)

			vv("got from newPeerCh! '%v' sees new peerURL: '%v' ...\n   we want to be the echo-answer!",
				peer.PeerServiceName(), peer.PeerURL())

			// talk to this peer on a separate goro if you wish:
			go func(peer RemotePeer) {
				defer wg.Done()
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
