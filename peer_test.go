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

		//preventRaceByDoingPriorClientToServerRoundTrip(cli, srv)
		ctx, canc := context.WithCancel(context.Background())
		_ = canc

		srvSync := newSyncer("srvSync")

		err = srv.PeerAPI.RegisterPeerServiceFunc("srvSync", srvSync.Start)
		panicOn(err)

		cliSync := newSyncer("cliSync")

		cliAddr := cli.LocalAddr()
		_ = cliAddr

		cli.PeerAPI.RegisterPeerServiceFunc("cliSync", cliSync.Start)

		lpb, err := cli.PeerAPI.StartLocalPeer(ctx, "cliSync", nil)
		panicOn(err)
		peerURL_cli, peerID_cli := lpb.URL(), lpb.ID()

		vv("cli.PeerAPI.StartLocalPeer() on client peerURL_client = '%v'; peerID_client = '%v'", peerURL_cli, peerID_cli)

		srvURL := cli.RemoteAddr() + "/srvSync"
		vv("srvURL = '%v'", srvURL)
		cliSync.PushToPeerURL <- srvURL

		//vv("past cliSync.PushToPeerURL <- srvURL")
		//vv("test main about to select{}")
		select {}
	})
}
