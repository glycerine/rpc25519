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

		// Fragment/Circuit Peer API on Client/Server
		cliServiceName := "peer0_on_client"
		peer0 := &PeerImpl{}
		cli.PeerAPI.RegisterPeerServiceFunc(cliServiceName, peer0.Start)

		srvServiceName := "peer1_on_server"
		peer1 := &PeerImpl{}
		srv.PeerAPI.RegisterPeerServiceFunc(srvServiceName, peer1.Start)

		cliAddr := cli.LocalAddr()
		ctx := context.Background()

		// This call starts the PeerImpl on the remote Client, from the server.

		peerID_client, err := srv.PeerAPI.StartRemotePeer(
			ctx, cliServiceName, cliAddr)
		panicOn(err)
		vv("started remote with PeerID = '%v'; cliServiceName = '%v'", peerID_client, cliServiceName)

		// Symmetrically, this should also work, but we are going to do
		// test the server as our "local" for the moment, since it is
		// backwards with respect to the usual RPC.
		// cli.PeerAPI.StartRemotePeer("peer1_on_server", serverAddr.String())

		// any number of known peers can be supplied, or none, to bootstrap.
		peerID_server, err := srv.PeerAPI.StartLocalPeer(srvServiceName, peerID_client)
		panicOn(err)

		vv("started on server peerID_server = '%v'", peerID_server)

		// lets ask the client to ask the server to start another one, to
		// test the symmetry of the CallStartPeerCircuit handling.

		// get the tcp:// or udp:// in front like the client expects.
		with_network_saddr := serverAddr.Network() + "://" + serverAddr.String()
		vv("with_network_saddr = '%v'", with_network_saddr)
		_ = with_network_saddr

		peerID_server2_from_remote_req, err := cli.PeerAPI.StartRemotePeer(
			ctx, srvServiceName, with_network_saddr)
		panicOn(err)
		vv("started second instance of peer1_on_server, this time remote with PeerID = '%v'; for service name = '%v'",
			peerID_server2_from_remote_req, srvServiceName)

		select {}
	})
}
