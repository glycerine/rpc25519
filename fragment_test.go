package rpc25519

import (
	//"context"
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

		// This call starts the PeerImpl on the remote Client.
		// Deliberately not the way an RPC usually works;
		// we go server -> client instead of client -> server.
		// Both ways are supported; this could have equally
		// have been cli.PeerAPI.StartRemotePeer("peer1_on_server")
		// followed by cli.PeerAPI.StartLocalPeer("peer0_on_client").
		peerID0, err := srv.PeerAPI.StartRemotePeer("peer0_on_client")
		panicOn(err)

		// any number of known peers can be supplied, or none.
		err = srv.PeerAPI.StartLocalPeer("peer1_on_server", peerID0)
		panicOn(err)

		select {}
	})
}
