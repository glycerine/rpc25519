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

type testJunk struct {
	name string
	cfg  *Config
	srv  *Server
	cli  *Client

	srvSync *syncer
	cliSync *syncer
}

func (j *testJunk) cleanup() {
	j.cli.Close()
	j.srv.Close()
}

func newTestJunk(name string) (j *testJunk) {

	j = &testJunk{
		name: name,
	}

	cfg := NewConfig()
	cfg.TCPonly_no_TLS = true

	cfg.ServerAddr = "127.0.0.1:0"
	srv := NewServer("srv_"+name, cfg)

	serverAddr, err := srv.Start()
	panicOn(err)
	//defer srv.Close()

	cfg.ClientDialToHostPort = serverAddr.String()
	cli, err := NewClient("cli_"+name, cfg)
	panicOn(err)
	err = cli.Start()
	panicOn(err)
	//defer cli.Close()

	srvSync := newSyncer("srvSync_" + name)

	err = srv.PeerAPI.RegisterPeerServiceFunc("srvSync_"+name, srvSync.Start)
	panicOn(err)

	cliSync := newSyncer("cliSync_" + name)
	err = cli.PeerAPI.RegisterPeerServiceFunc("cliSync_"+name, cliSync.Start)
	panicOn(err)

	j.cli = cli
	j.srv = srv
	j.cfg = cfg

	j.cliSync = cliSync
	j.srvSync = srvSync

	return j
}

func Test404_verify_peer_operations(t *testing.T) {

	cv.Convey("verify all the basic peer operations are functional", t, func() {

		cfg := NewConfig()
		cfg.TCPonly_no_TLS = true

		cfg.ServerAddr = "127.0.0.1:0"
		srv := NewServer("srv_test404", cfg)

		serverAddr, err := srv.Start()
		panicOn(err)
		defer srv.Close()

		cfg.ClientDialToHostPort = serverAddr.String()
		cli, err := NewClient("client_test404", cfg)
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

		// minimal base setup done!

		// does cancel of local on server work?
		// does cancel of local on client work?

		cliSync := newSyncer("cliSync")

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
		time.Sleep(5 * time.Second)

		vv("slept 5 sec. now cancel ctx.")
		canc()
		select {}

		// systematic cancel tests:
		//does local peer cancel -> local service cancel
		//local service -> all local ckt cancel
		// start here [ ] local ckt -> remote ckt
	})
}

func Test405_user_can_cancel_local_service_with_context(t *testing.T) {

	cv.Convey("user code calling cancel on a running local peer service should stop it", t, func() {
		j := newTestJunk("local_ctx_cancel_test405")
		defer j.cleanup()

	})

}
