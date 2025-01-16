package rpc25519

import (
	"context"
	//"fmt"
	//"os"
	//"strings"
	"testing"
	"time"

	cv "github.com/glycerine/goconvey/convey"
)

func Test400_Fragments_riding_Circuits_API(t *testing.T) {

	cv.Convey("our peer-to-peer Fragment/Circuit API "+
		"generalizes peer-to-multiple-peers and dealing with infinite "+
		"streams of Fragments, both arriving and being sent.", t, func() {

		cfg := NewConfig()
		cfg.TCPonly_no_TLS = true

		cfg.ServerAddr = "127.0.0.1:0"
		srv := NewServer("srv_test400", cfg)

		serverAddr, err := srv.Start()
		panicOn(err)
		defer srv.Close()

		cfg.ClientDialToHostPort = serverAddr.String()
		cli, err := NewClient("client_test400", cfg)
		panicOn(err)
		err = cli.Start()
		panicOn(err)
		defer cli.Close()

		// An unexpected race we have to prevent:
		// the client TCP connection would get made,
		// but the subsequent server side Go code that
		// gets the listener accept and sets up the rwPair
		// was racing with us coming from the other direction
		// and telling the server to contact the client.
		// We saw this race in 6% of 1000 runs, but
		// by running this next round trip, we see the
		// race 0% of 1000 runs. This call makes us
		// wait on the client who waits on the server
		// to be ready. So when it returns we know
		// the server knows about the client.

		// simplify echo test by leaving this out. the auto retry should suffice.
		// Arg! Auto-retry does not always suffice! back in!
		preventRaceByDoingPriorClientToServerRoundTrip(cli, srv)
		ctx := context.Background()
		srvServiceName := "speer1_on_server"

		// Fragment/Circuit Peer API on Client/Server
		cliServiceName := "cpeer0_on_client"
		cpeer0 := &PeerImpl{}
		cli.PeerAPI.RegisterPeerServiceFunc(cliServiceName, cpeer0.Start)

		speer1 := &PeerImpl{
			DoEchoToThisPeerURL:  make(chan string),
			ReportEchoTestCanSee: make(chan string),
		}
		srv.PeerAPI.RegisterPeerServiceFunc(srvServiceName, speer1.Start)

		cliAddr := cli.LocalAddr()

		// This call starts the PeerImpl on the remote Client, from the server.

		// Arg. This was racing with the server actually getting
		// the rwPair spun up, crashing 6% of the time.
		// Thus we added a prior round-trip cli->srv->cli above.
		peerURL_client, peerID_client, err := srv.PeerAPI.StartRemotePeer(
			ctx, cliServiceName, cliAddr, 50*time.Microsecond)
		panicOn(err) // cliAddr not found?!?
		vv("started remote with peerURL_client = '%v'; cliServiceName = '%v'; peerID_client = '%v'", peerURL_client, cliServiceName, peerID_client)

		// any number of known peers can be supplied, or none, to bootstrap.
		peerURL_server, peerID_server, err := srv.PeerAPI.StartLocalPeer(ctx, srvServiceName)
		//peerURL_server, peerID_server, err := srv.PeerAPI.StartLocalPeer(ctx, srvServiceName, peerURL_client)
		panicOn(err)
		_ = peerURL_server
		_ = peerID_server
		vv("StartLocalPeer: on server peerURL_server = '%v'; peerID_server = '%v'", peerURL_server, peerID_server)

		// lets ask the client to ask the server to start one, to
		// test the symmetry of the CallPeerStartCircuit handling.

		// get the tcp:// or udp:// in front like the client expects.
		with_network_saddr := serverAddr.Network() + "://" + serverAddr.String()
		vv("with_network_saddr = '%v'", with_network_saddr)
		_ = with_network_saddr

		/* simplify the echo test by leaving out 2nd server for now
		peerURL_server2_from_remote_req, peerID_server2, err := cli.PeerAPI.StartRemotePeer(
			ctx, srvServiceName, with_network_saddr, 0)
		panicOn(err)
		vv("started second instance of speer1_on_server, this time remote with PeerURL = '%v'; for service name = '%v'; peerID_server2 = '%v'",
			peerURL_server2_from_remote_req, srvServiceName, peerID_server2)

		// we should have seen the client peer start 1x, and the server 2x.
		cv.So(cpeer0.StartCount.Load(), cv.ShouldEqual, 1)
		cv.So(speer1.StartCount.Load(), cv.ShouldEqual, 2)
		*/

		vv(" ============= about to begin speer1.DoEchoToThisPeerURL <- peerURL_client = '%v'", peerURL_client)

		// simple echo test from srv -> cli -> srv, over a circuit between peers.
		//		url :=
		speer1.DoEchoToThisPeerURL <- peerURL_client

		//vv("now have the peers talk to each other. what to do? keep a directory in sync between two nodes? want to handle files/data bigger than will fit in one Message in an rsync protocol.")

		//vv("start with something that won't take up tons of disk to test: create a chacha8 random stream, chunk it, send it one direction to the other with blake3 cumulative checksums so we know they were both getting everything... simple, but will stress the concurrency.")
		// 3 way sync?

		select {
		case report := <-speer1.ReportEchoTestCanSee:
			vv("speer1 echo got back: '%v'", report)
		}

	})
}

func preventRaceByDoingPriorClientToServerRoundTrip(cli *Client, srv *Server) {
	serviceName := "customEcho"
	srv.Register2Func(serviceName, customEcho)

	req := NewMessage()
	req.HDR.ServiceName = serviceName
	req.JobSerz = []byte("Hello from client!")

	reply, err := cli.SendAndGetReply(req, nil)
	panicOn(err)
	_ = reply
	//vv("reply = '%v'", reply)
	//vv("good, response from Server means it definitely has client registered now.")
}

func Test401_PeerURL_parsing(t *testing.T) {

	cv.Convey("parsePeerURL should extract network address, peerID, and optionally circuitID", t, func() {

		peerURL := "tcp://x.x.x.x:5023/serviceName/peerID/circuitID"
		netAddr, serviceName, peerID, circuitID, err := parsePeerURL(peerURL)
		panicOn(err)

		vv("%v -> netAddr = '%v'", peerURL, netAddr)
		vv("%v -> serviceName = '%v'", peerURL, serviceName)
		vv("%v -> peerID = '%v'", peerURL, peerID)
		vv("%v -> circuitID = '%v'", peerURL, circuitID)

		cv.So(netAddr, cv.ShouldEqual, "tcp://x.x.x.x:5023")
		cv.So(serviceName, cv.ShouldEqual, "serviceName")
		cv.So(peerID, cv.ShouldEqual, "peerID")
		cv.So(circuitID, cv.ShouldEqual, "circuitID")
	})

}

func Test402_simpler_startup_peer_service_test(t *testing.T) {

	cv.Convey("402: clone 400 and shrink it to focus just on not getting port 0 back", t, func() {

		cfg := NewConfig()
		cfg.TCPonly_no_TLS = true
		cfg.UseQUIC = false // true

		cfg.ServerAddr = "127.0.0.1:0"
		srv := NewServer("srv_test402", cfg)

		serverAddr, err := srv.Start()
		panicOn(err)
		defer srv.Close()

		cfg.ClientDialToHostPort = serverAddr.String()
		cli, err := NewClient("client_test402", cfg)
		panicOn(err)
		err = cli.Start()
		panicOn(err)
		defer cli.Close()

		preventRaceByDoingPriorClientToServerRoundTrip(cli, srv)
		ctx := context.Background()
		srvServiceName := "speer1_on_server"

		// Fragment/Circuit Peer API on Client/Server
		cliServiceName := "cpeer0_on_client"
		cpeer0 := &PeerImpl{}
		cli.PeerAPI.RegisterPeerServiceFunc(cliServiceName, cpeer0.Start)

		speer1 := &PeerImpl{
			DoEchoToThisPeerURL: make(chan string),
		}
		srv.PeerAPI.RegisterPeerServiceFunc(srvServiceName, speer1.Start)

		cliAddr := cli.LocalAddr()
		vv("cliAddr = '%v'", cliAddr)

		peerURL_server, peerID_server, err := srv.PeerAPI.StartLocalPeer(ctx, srvServiceName)
		panicOn(err)
		vv("StartLocalPeer: on server peerURL_server = '%v'; peerID_server = '%v'", peerURL_server, peerID_server)
		if cfg.UseQUIC {
			cv.So(peerURL_server, cv.ShouldStartWith, "udp://")
		} else {
			cv.So(peerURL_server, cv.ShouldStartWith, "tcp://")
		}
	})
}

// 403 just bringing up new Circuit from existing peer

func Test403_new_circuit_from_existing_peer(t *testing.T) {

	cv.Convey("new circuit bootstrapped, from existing registered and started peers.", t, func() {

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

		preventRaceByDoingPriorClientToServerRoundTrip(cli, srv)
		ctx := context.Background()
		srvServiceName := "speer1_on_server"

		// Fragment/Circuit Peer API on Client/Server
		cliServiceName := "cpeer0_on_client"
		cpeer0 := &PeerImpl{}
		cli.PeerAPI.RegisterPeerServiceFunc(cliServiceName, cpeer0.Start)

		speer1 := &PeerImpl{
			DoEchoToThisPeerURL:  make(chan string),
			ReportEchoTestCanSee: make(chan string),
		}
		srv.PeerAPI.RegisterPeerServiceFunc(srvServiceName, speer1.Start)

		cliAddr := cli.LocalAddr()

		// This call starts the PeerImpl on the remote Client, from the server.

		peerURL_client, peerID_client, err := srv.PeerAPI.StartRemotePeer(
			ctx, cliServiceName, cliAddr, 50*time.Microsecond)
		panicOn(err)
		vv("started remote with peerURL_client = '%v'; cliServiceName = '%v'; peerID_client = '%v'", peerURL_client, cliServiceName, peerID_client)

		peerURL_server, peerID_server, err := srv.PeerAPI.StartLocalPeer(ctx, srvServiceName)
		panicOn(err)
		vv("srv.PeerAPI.StartLocalPeer() on server peerURL_server = '%v'; peerID_server = '%v'", peerURL_server, peerID_server)

		vv(" ============= about to begin speer1.DoEchoToThisPeerURL <- peerURL_client = '%v'", peerURL_client)

		// simple echo test from srv -> cli -> srv, over a circuit between peers.
		//		url :=
		speer1.DoEchoToThisPeerURL <- peerURL_client

		//vv("now have the peers talk to each other. what to do? keep a directory in sync between two nodes? want to handle files/data bigger than will fit in one Message in an rsync protocol.")

		//vv("start with something that won't take up tons of disk to test: create a chacha8 random stream, chunk it, send it one direction to the other with blake3 cumulative checksums so we know they were both getting everything... simple, but will stress the concurrency.")
		// 3 way sync?

		select {
		case report := <-speer1.ReportEchoTestCanSee:
			vv("speer1 echo got back: '%v'", report)
		}
	})
}
