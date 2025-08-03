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

func Test405_user_can_close_Client_and_Server(t *testing.T) {

	if faketime {
		t.Skip("skip under synctest, net calls will never settle.")
		return
	}

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

	if faketime {
		t.Skip("skip under synctest, net calls will never settle.")
		return
	}

	cv.Convey("user code calling halt on a running local peer service should stop it", t, func() {
		j := newTestJunk("local_ctx_cancel_test406")
		defer j.cleanup()

		ctx := context.Background()
		lpb, err := j.cli.PeerAPI.StartLocalPeer(ctx, j.cliServiceName, nil, "")
		panicOn(err)
		lpb.Close()

		<-j.cliSync.halt.Done.Chan
		vv("good: cliSync has halted")
	})

	cv.Convey("user code calling cancel on a running local peer service should stop it", t, func() {
		j := newTestJunk("local_ctx_cancel_test406")
		defer j.cleanup()

		ctx, canc := context.WithCancel(context.Background())
		lpb, err := j.cli.PeerAPI.StartLocalPeer(ctx, j.cliServiceName, nil, "")
		panicOn(err)
		defer lpb.Close()

		canc() // should be equivalent to lpb.Close()

		<-j.cliSync.halt.Done.Chan
		vv("good: cliSync has halted")
	})

}

func Test408_single_circuits_can_cancel_and_propagate_to_remote(t *testing.T) {

	if faketime {
		t.Skip("skip under synctest, net calls will never settle.")
		return
	}

	cv.Convey("a circuit can close down, telling the remote but not closing the peer", t, func() {

		j := newTestJunk("circuit_close_test407")
		defer j.cleanup()

		ctx := context.Background()
		cli_lpb, err := j.cli.PeerAPI.StartLocalPeer(ctx, j.cliServiceName, nil, "")
		panicOn(err)
		defer cli_lpb.Close()

		// later test:
		//_, _, err := j.cli.PeerAPI.StartRemotePeer(ctx, j.srvServiceName, cli.RemoteAddr(), 0)
		//panicOn(err)

		server_lpb, err := j.srv.PeerAPI.StartLocalPeer(ctx, j.srvServiceName, nil, "")
		panicOn(err)
		defer server_lpb.Close()

		// establish a circuit, then close it
		cktname := "407ckt"

		// optional first frag
		frag0 := NewFragment()
		frag0.FragSubject = "initial setup frag0"

		ckt, ctxCkt, err := cli_lpb.NewCircuitToPeerURL(cktname, server_lpb.URL(), frag0, 0)
		panicOn(err)
		_ = ctxCkt
		defer ckt.Close(nil)

		// verify it is up

		serverCkt := <-j.srvSync.gotIncomingCkt
		//vv("server got circuit '%v'", serverCkt.Name)

		fragSrvInRead0 := <-j.srvSync.gotIncomingCktReadFrag
		cv.So(fragSrvInRead0.FragSubject, cv.ShouldEqual, "initial setup frag0")

		// verify server gets Reads
		frag := NewFragment()
		frag.FragSubject = "are we live?"
		cli_lpb.SendOneWay(ckt, frag, 0, 0)
		//vv("cli_lpb.SendOneWay() are we live back.")

		fragSrvInRead1 := <-j.srvSync.gotIncomingCktReadFrag
		//vv("good: past 2nd read from server. fragSrvInRead1 = '%v'", fragSrvInRead1)

		_ = fragSrvInRead1
		if fragSrvInRead1.FragSubject != "are we live?" {
			t.Fatalf("error: not expected subject 'are we live?' but: '%v'", fragSrvInRead1.FragSubject)
		}

		//vv("good: past the are we live check.")

		if ckt.IsClosed() {
			t.Fatalf("error: client side circuit '%v' should NOT be closed.", ckt.Name)
		}
		if serverCkt.IsClosed() {
			t.Fatalf("error: server circuit '%v' should NOT be closed.", serverCkt.Name)
		}

		//vv("about to ckt.Close() from the client side ckt")
		ckt.Close(nil)

		//vv("good: past the ckt.Close()")
		if !ckt.IsClosed() {
			t.Fatalf("error: circuit '%v' should be closed.", ckt.Name)
		}

		// verify that the server side also closed the circuit.

		// IsClosed() wil race against the close ckt going to the server,
		// so wait on serverCkt.Halt.Done.Chan first.
		timeout := j.srv.NewTimer(time.Second * 2)
		select {
		case <-serverCkt.Halt.Done.Chan:
		case <-timeout.C:
			t.Fatalf("error: server circuit '%v' did not close after 2 sec", serverCkt.Name)
		}
		timeout.Discard()
		if !serverCkt.IsClosed() {
			t.Fatalf("error: server circuit '%v' should be closed.", serverCkt.Name)
		}
		//vv("good: past the serverCkt.IsClosed()")

		// did the server peer code recognize the closed ckt?

		// we have been acting as the client through lbp, so the
		// client peer code has not been active. And that's super
		// useful to keep this test deterministic and not having
		// two competing reads on response channels.

		<-j.srvSync.gotCktHaltReq.Chan
		//vv("good: server saw the ckt peer code stopped reading ckt.")

		// sends and reads on the closed ckt should give errors / nil channel hangs

		// server side is responding well when this test proxies the client.

		//vv("   ========   now proxy the server and have ckt to client... separate test?")

		// Let's try it the other way: proxy the server and set up
		// a circuit with the client

		// optional first frag
		frag2 := NewFragment()
		frag2.FragSubject = "initial setup frag2"

		cktname2 := "proxy_the_server407"
		ckt2, ctxCkt2, err := server_lpb.NewCircuitToPeerURL(cktname2, cli_lpb.URL(), frag2, 0)
		panicOn(err)
		_ = ctxCkt2
		defer ckt2.Close(nil)

		cliCkt := <-j.cliSync.gotIncomingCkt
		//vv("client got circuit '%v'", cliCkt.Name)

		if cliCkt.Name != "proxy_the_server407" {
			t.Fatalf("error: cliCktName should be 'proxy_the_server407' but we got '%v'", cliCkt.Name)
		}
		//vv("good: client got the named circuit we expected.")

		fragCliInRead2 := <-j.cliSync.gotIncomingCktReadFrag
		cv.So(fragCliInRead2.FragSubject, cv.ShouldEqual, "initial setup frag2")

		// verify client gets Reads
		frag3 := NewFragment()
		frag3.FragSubject = "frag3 to the client"
		server_lpb.SendOneWay(ckt2, frag3, 0, 0)

		fragCliInRead3 := <-j.cliSync.gotIncomingCktReadFrag
		//vv("good: past frag3 read in the client. fragCliInRead3 = '%v'", fragCliInRead3)

		_ = fragCliInRead3
		if fragCliInRead3.FragSubject != "frag3 to the client" {
			t.Fatalf("error: not expected subject 'are we live?' but: '%v'", fragCliInRead3.FragSubject)
		}

		//vv("good: past the client frag3 read check.")

		// shut down the peer service on one side. does the other side
		// stay up, but clean up all the circuits associated with that service?

		//select {}
	})

}
