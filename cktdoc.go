/*
Peer/Circuit/Fragment Design overview

This is an alternative API to the Message and net/rpc APIs
provided by the rpc25519 package.

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
circuit persistence is inexpensive and
supports any number of data streams with markedly
different lifetimes and update rates,
over long periods.

Symmetry of code deployment is also a natural
requirement. This is the git model. When syncing
two repositories, the operations needed are
the same on both sides, no matter who
initiated or whether a push or pull was
requested. Hence we want a way to register
the same functionality on the client as on the server.
This is not available in a typical RPC package.

Peer/Circuit/Fragment API essentials (utility methods omitted for compactness)

A) To establish circuits with new peers, use

 1. NewCircuitToPeerURL() for initiating a new circuit to a new peer.
 2. <-newPeerCh to recieve new initiations;
    then use the IncomingCircuit() method to get the Circuit.

B) To create additional circuits with an already connected peer:
 1. NewCircuit adds a new circuit with an existing RemotePeer, no URL needed.
 2. They get notified on <-newPeerCh too.

C) To communicate over a Circuit:
 1. get regular messages (called Fragments) from <-Circuit.Reads
 2. get error messages from <-Circuit.Errors
 3. send messages with SendOneWay(). It never blocks.
 4. Close() the circuit and the peer's ctx will be cancelled.

// Circuit has other fields, but this is the essential interface:

	type Circuit struct {
		Reads  <-chan *Fragment
		Errors <-chan *Fragment
	    Close() // when done
	}

// LocalPeer is actually a struct, but you can think of it as this interface:

	type LocalPeer interface {
		NewCircuitToPeerURL(peerURL string, frag *Fragment,
	         errWriteDur *time.Duration) (ckt *Circuit, ctx context.Context, err error)
	}

// RemotePeer is actually a struct, but you can think of it as this interface:

	type RemotePeer interface {
		IncomingCircuit() (ckt *Circuit, ctx context.Context, err error) // gets the first.
		NewCircuit()      (ckt *Circuit, ctx context.Context, err error) // make 2nd, 3rd...
		SendOneWay(ckt *Circuit, frag *Fragment, errWriteDur *time.Duration) error
	}

// Users write PeerServiceFunc callbacks to create peers.

	type PeerServiceFunc func(myPeer *LocalPeer, ctx0 context.Context, newPeerCh <-chan *RemotePeer) error

// Fragment is the data packet transmitted over Circuits between Peers.

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
	       peerServiceName string) (lp *LocalPeer, err error)

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
*/
package rpc25519
