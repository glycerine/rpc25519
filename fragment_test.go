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
		srv := NewServer("srv_test001", cfg)

		serverAddr, err := srv.Start()
		panicOn(err)
		defer srv.Close()

		cfg.ClientDialToHostPort = serverAddr.String()
		cli, err := NewClient("test400", cfg)
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
		preventRaceByDoingPriorClientToServerRoundTrip(cli, srv)

		// Fragment/Circuit Peer API on Client/Server
		cliServiceName := "cpeer0_on_client"
		cpeer0 := &PeerImpl{}
		cli.PeerAPI.RegisterPeerServiceFunc(cliServiceName, cpeer0.Start)

		srvServiceName := "speer1_on_server"
		speer1 := &PeerImpl{}
		srv.PeerAPI.RegisterPeerServiceFunc(srvServiceName, speer1.Start)

		cliAddr := cli.LocalAddr()
		ctx := context.Background()

		// This call starts the PeerImpl on the remote Client, from the server.

		// Arg. This was racing with the server actually getting
		// the rwPair spun up, crashing 6% of the time.
		// Thus we added a prior round-trip cli->srv->cli above.
		peerID_client, err := srv.PeerAPI.StartRemotePeer(
			ctx, cliServiceName, cliAddr, 50*time.Microsecond)
		panicOn(err) // cliAddr not found?!?
		vv("started remote with PeerID = '%v'; cliServiceName = '%v'", peerID_client, cliServiceName)

		// any number of known peers can be supplied, or none, to bootstrap.
		peerID_server, err := srv.PeerAPI.StartLocalPeer(srvServiceName, peerID_client)
		panicOn(err)

		vv("started on server peerID_server = '%v'", peerID_server)

		// lets ask the client to ask the server to start one, to
		// test the symmetry of the CallStartPeerCircuit handling.

		// get the tcp:// or udp:// in front like the client expects.
		with_network_saddr := serverAddr.Network() + "://" + serverAddr.String()
		vv("with_network_saddr = '%v'", with_network_saddr)
		_ = with_network_saddr

		peerID_server2_from_remote_req, err := cli.PeerAPI.StartRemotePeer(
			ctx, srvServiceName, with_network_saddr, 0)
		panicOn(err)
		vv("started second instance of speer1_on_server, this time remote with PeerID = '%v'; for service name = '%v'",
			peerID_server2_from_remote_req, srvServiceName)

		// we should have seen the client peer start 1x, and the server 2x.
		cv.So(cpeer0.StartCount.Load(), cv.ShouldEqual, 1)
		cv.So(speer1.StartCount.Load(), cv.ShouldEqual, 2)

		vv("now have the peers talk to each other. what to do? keep a directory in sync between two nodes? want to handle files/data bigger than will fit in one Message in an rsync protocol.")

		vv("start with something that won't take up tons of disk to test: create a chacha8 random stream, chunk it, send it one direction to the other with blake3 cumulative checksums so we know they were both getting everything... simple, but will stress the concurrency.")
		// 3 way sync?
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
