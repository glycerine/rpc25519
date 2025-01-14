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
		peer0 := &PeerImpl{}
		cli.PeerAPI.RegisterPeerServiceFunc("peer0_on_client", peer0.Start)

		peer1 := &PeerImpl{}
		srv.PeerAPI.RegisterPeerServiceFunc("peer1_on_server", peer1.Start)

		cliAddr := cli.LocalAddr()
		ctx := context.Background()

		// This call starts the PeerImpl on the remote Client, from the server.

		peerID0, err := srv.PeerAPI.StartRemotePeer(ctx, "peer0_on_client", cliAddr)
		panicOn(err)
		vv("started remote with PeerID = '%v'", peerID0)

		// Symmetrically, this should also work, but we are going to do
		// test the server as our "local" for the moment, since it is
		// backwards with respect to the usual RPC.
		// cli.PeerAPI.StartRemotePeer("peer1_on_server", serverAddr.String())

		// any number of known peers can be supplied, or none, to bootstrap.
		localPeerID, err := srv.PeerAPI.StartLocalPeer("peer1_on_server", peerID0)
		panicOn(err)

		vv("started localPeerID = '%v'", localPeerID)
		select {}
	})
}
