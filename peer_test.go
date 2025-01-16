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
