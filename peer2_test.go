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

type testJunk2 struct {
	name string
	cfg  *Config
	srv  *Server
	cli  *Client

	srvSync *syncer2
	cliSync *syncer2

	cliServiceName string
	srvServiceName string
}

func (j *testJunk2) cleanup() {
	j.cli.Close()
	j.srv.Close()
}

func newTestJunk2(name string) (j *testJunk2) {

	j = &testJunk2{
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

	srvSync := newSyncer2(j.srvServiceName)

	err = srv.PeerAPI.RegisterPeerServiceFunc(j.srvServiceName, srvSync.Start)
	panicOn(err)

	cliSync := newSyncer2(j.cliServiceName)
	err = cli.PeerAPI.RegisterPeerServiceFunc(j.cliServiceName, cliSync.Start)
	panicOn(err)

	j.cli = cli
	j.srv = srv
	j.cfg = cfg

	j.cliSync = cliSync
	j.srvSync = srvSync

	return j
}

func Test408_multiple_circuits_stay_independent_syncer2(t *testing.T) {

	cv.Convey("testing peer2.go: no cross talk between many circuits open at once. Use syncer2", t, func() {

		j := newTestJunk("no_crosstalk_test408")
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
		cktname := "408ckt"

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

		// IsClosed() wil race against the close ckt going to the server,
		// so wait on serverCkt.Halt.Done.Chan first.
		select {
		case <-serverCkt.Halt.Done.Chan:
		case <-time.After(2 * time.Second):
			t.Fatalf("error: server circuit '%v' did not close after 2 sec", serverCkt.Name)
		}
		if !serverCkt.IsClosed() {
			t.Fatalf("error: server circuit '%v' should be closed.", serverCkt.Name)
		}
		vv("good: past the serverCkt.IsClosed()")

		// did the server peer code recognize the closed ckt?

		// we have been acting as the client through lbp, so the
		// client peer code has not been active. And that's super
		// useful to keep this test deterministic and not having
		// two competing reads on response channels.

		<-j.srvSync.gotCktHaltReq.Chan
		vv("good: server saw the ckt peer code stopped reading ckt.")

		// sends and reads on the closed ckt should give errors / nil channel hangs

		// server side is responding well when this test proxies the client.

		vv("   ========   now proxy the server and have ckt to client... separate test?")

		// Let's try it the other way: proxy the server and set up
		// a circuit with the client

		// optional first frag
		frag2 := NewFragment()
		frag2.FragSubject = "initial setup frag2"

		cktname2 := "proxy_the_server408"
		ckt2, ctxCkt2, err := server_lpb.NewCircuitToPeerURL(cktname2, cli_lpb.URL(), frag2, nil)
		panicOn(err)
		_ = ctxCkt2
		defer ckt2.Close()

		cliCkt := <-j.cliSync.gotIncomingCkt
		vv("client got circuit '%v'", cliCkt.Name)

		if cliCkt.Name != "proxy_the_server408" {
			t.Fatalf("error: cliCktName should be 'proxy_the_server408' but we got '%v'", cliCkt.Name)
		}
		vv("good: client got the named circuit we expected.")

		fragCliInRead2 := <-j.cliSync.gotIncomingCktReadFrag
		cv.So(fragCliInRead2.FragSubject, cv.ShouldEqual, "initial setup frag2")

		// verify client gets Reads
		frag3 := NewFragment()
		frag3.FragSubject = "frag3 to the client"
		server_lpb.SendOneWay(ckt2, frag3, nil)

		fragCliInRead3 := <-j.cliSync.gotIncomingCktReadFrag
		vv("good: past frag3 read in the client. fragCliInRead3 = '%v'", fragCliInRead3)

		_ = fragCliInRead3
		if fragCliInRead3.FragSubject != "frag3 to the client" {
			t.Fatalf("error: not expected subject 'are we live?' but: '%v'", fragCliInRead3.FragSubject)
		}

		vv("good: past the client frag3 read check.")

		// shut down the peer service on one side. does the other side
		// stay up, but clean up all the circuits associated with that service?

		//select {}
	})

}
