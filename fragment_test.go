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

		// Fragment/Circuit Peer API on server
		peer := &PeerImpl{}
		cli.PeerAPI.RegisterPeerStreamFunc("my peer", peer.PeerStream)

	})
}
