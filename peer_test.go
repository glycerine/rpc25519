package rpc25519

import (
	"context"
	"fmt"
	//"os"
	//"strings"
	"testing"
	"time"

	cv "github.com/glycerine/goconvey/convey"
)

var _ = fmt.Sprintf

type testJunk struct {
	name string
	cfg  *Config
	srv  *Server
	cli  *Client

	srvSync *syncer
	cliSync *syncer

	cliServiceName string
	srvServiceName string
}

func (j *testJunk) cleanup() {
	j.cli.Close()
	j.srv.Close()
}

func newTestJunk(name string) (j *testJunk) {

	j = &testJunk{
		name:           name,
		cliServiceName: "cliSync_" + name,
		srvServiceName: "srvSync_" + name,
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

	srvSync := newSyncer(j.srvServiceName)

	err = srv.PeerAPI.RegisterPeerServiceFunc(j.srvServiceName, srvSync.Start)
	panicOn(err)

	cliSync := newSyncer(j.cliServiceName)
	err = cli.PeerAPI.RegisterPeerServiceFunc(j.cliServiceName, cliSync.Start)
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

func Test405_user_can_close_Client_and_Server(t *testing.T) {

	cv.Convey("user code calling Close on Client and Server should shut down. make sure j.cleanup works!", t, func() {
		j := newTestJunk("close_cli_srv405")

		j.cleanup()

		<-j.cli.halt.Done.Chan
		vv("cli has halted")
		<-j.srv.halt.Done.Chan
		vv("srv has halted")

		// verify children
		j = newTestJunk("close_cli_srv405")
		j.cleanup()

	})

}

func Test406_user_can_cancel_local_service_with_context(t *testing.T) {

	cv.Convey("user code calling halt on a running local peer service should stop it", t, func() {
		j := newTestJunk("local_ctx_cancel_test406")
		defer j.cleanup()

		ctx := context.Background()
		lpb, err := j.cli.PeerAPI.StartLocalPeer(ctx, j.cliServiceName, nil)
		panicOn(err)
		lpb.Close()

		<-j.cliSync.halt.Done.Chan
		vv("good: cliSync has halted")
	})

	cv.Convey("user code calling cancel on a running local peer service should stop it", t, func() {
		j := newTestJunk("local_ctx_cancel_test406")
		defer j.cleanup()

		ctx, canc := context.WithCancel(context.Background())
		lpb, err := j.cli.PeerAPI.StartLocalPeer(ctx, j.cliServiceName, nil)
		panicOn(err)
		defer lpb.Close()

		canc() // should be equivalent to lpb.Close()

		<-j.cliSync.halt.Done.Chan
		vv("good: cliSync has halted")
	})

}

func Test407_single_circuits_can_cancel_and_propagate_to_remote(t *testing.T) {

	cv.Convey("a circuit can close down, telling the remote but not closing the peer", t, func() {

		j := newTestJunk("circuit_close_test407")
		defer j.cleanup()

		ctx := context.Background()
		cli_lpb, err := j.cli.PeerAPI.StartLocalPeer(ctx, j.cliServiceName, nil)
		panicOn(err)
		defer cli_lpb.Close()

		// later test:
		//_, _, err := j.cli.PeerAPI.StartRemotePeer(ctx, j.srvServiceName, cli.RemoteAddr(), 0)
		//panicOn(err)

		server_lpb, err := j.srv.PeerAPI.StartLocalPeer(ctx, j.srvServiceName, nil)
		panicOn(err)
		defer server_lpb.Close()

		// establish a circuit, then close it
		cktname := "407ckt"

		// optional first frag
		frag0 := NewFragment()
		frag0.FragSubject = "initial setup frag0"

		ckt, ctxCkt, err := cli_lpb.NewCircuitToPeerURL(cktname, server_lpb.URL(), frag0, nil)
		panicOn(err)
		_ = ctxCkt
		defer ckt.Close()

		// verify it is up

		serverCkt := <-j.srvSync.gotIncomingCkt
		vv("server got circuit '%v'", serverCkt.Name)

		fragSrvInRead0 := <-j.srvSync.gotIncomingCktReadFrag
		cv.So(fragSrvInRead0.FragSubject, cv.ShouldEqual, "initial setup frag0")

		// verify server gets Reads
		frag := NewFragment()
		frag.FragSubject = "are we live?"
		cli_lpb.SendOneWay(ckt, frag, nil)
		vv("cli_lpb.SendOneWay() are we live back.")

		fragSrvInRead1 := <-j.srvSync.gotIncomingCktReadFrag
		vv("good: past 2nd read from server. fragSrvInRead1 = '%v'", fragSrvInRead1)

		_ = fragSrvInRead1
		if fragSrvInRead1.FragSubject != "are we live?" {
			t.Fatalf("error: not expected subject 'are we live?' but: '%v'", fragSrvInRead1.FragSubject)
		}

		vv("good: past the are we live check.")

		if ckt.IsClosed() {
			t.Fatalf("error: client side circuit '%v' should NOT be closed.", ckt.Name)
		}
		if serverCkt.IsClosed() {
			t.Fatalf("error: server circuit '%v' should NOT be closed.", serverCkt.Name)
		}

		vv("about to ckt.Close() from the client side ckt")
		ckt.Close()

		vv("good: past the ckt.Close()")
		if !ckt.IsClosed() {
			t.Fatalf("error: circuit '%v' should be closed.", ckt.Name)
		}

		// verify that the server side also closed the circuit.

		// might be racing against the close ckt going to the server.
		select {
		case <-serverCkt.Halt.Done.Chan:
		case <-time.After(2 * time.Second):
			t.Fatalf("error: server circuit '%v' did not close after 2 sec", serverCkt.Name)
		}
		if !serverCkt.IsClosed() {
			t.Fatalf("error: server circuit '%v' should be closed.", serverCkt.Name)
		}
		vv("good: past the serverCkt.IsClosed()")

		// did the peer code recognize the closed ckt?

		// non-deterministic which gets here first, have to do twice.
		select {
		case <-j.srvSync.gotCktHaltReq:
			vv("good: server saw the ckt channels were closed.")
		case <-j.cliSync.gotCktHaltReq:
			vv("good: client saw the ckt channels were closed.")
		}

		select {
		case <-j.srvSync.gotCktHaltReq:
			vv("good: server saw the ckt channels were closed.")
		case <-j.cliSync.gotCktHaltReq:
			vv("good: client saw the ckt channels were closed.")
		}

		// sends and reads on the closed ckt should give errors / nil channel hangs

		select {}

		// printing a random "x" ? but not here... async?
		//cv.So(fragSrvInRead1.FragSubject, cv.ShouldEqual, "initial setup frag0")
		//server_lpb
		//Reads <- chan *Fragment
		//Errors <- chan *Fragment

		//select {}
	})

}
