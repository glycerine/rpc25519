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

		j := newTestJunk2("no_crosstalk_test408")
		defer j.cleanup()

		ctx := context.Background()
		cli_lpb, err := j.cli.PeerAPI.StartLocalPeer(ctx, j.cliServiceName, nil)
		panicOn(err)
		defer cli_lpb.Close()

		// later test:
		//_, _, err := j.cli.PeerAPI.StartRemotePeer(ctx, j.srvServiceName, cli.RemoteAddr(), 0)
		//panicOn(err)

		srv_lpb, err := j.srv.PeerAPI.StartLocalPeer(ctx, j.srvServiceName, nil)
		panicOn(err)
		defer srv_lpb.Close()

		// establish a circuit, then close it
		cktname := "408ckt_first_ckt"

		// optional first frag
		frag0 := NewFragment()
		frag0.FragSubject = "initial setup frag0"

		ckt, ctxCkt, err := cli_lpb.NewCircuitToPeerURL(cktname, srv_lpb.URL(), frag0, 0)
		panicOn(err)
		_ = ctxCkt
		defer ckt.Close()

		// verify it is up

		serverCkt := <-j.srvSync.gotIncomingCkt
		//zz("server got circuit '%v'", serverCkt.Name)

		fragSrvInRead0 := <-j.srvSync.gotIncomingCktReadFrag
		cv.So(fragSrvInRead0.FragSubject, cv.ShouldEqual, "initial setup frag0")

		// verify server gets Reads
		frag := NewFragment()
		frag.FragSubject = "are we live?"
		cli_lpb.SendOneWay(ckt, frag, 0)
		//zz("cli_lpb.SendOneWay() are we live back.")

		fragSrvInRead1 := <-j.srvSync.gotIncomingCktReadFrag
		//zz("good: past 2nd read from server. fragSrvInRead1 = '%v'", fragSrvInRead1)

		_ = fragSrvInRead1
		if fragSrvInRead1.FragSubject != "are we live?" {
			t.Fatalf("error: not expected subject 'are we live?' but: '%v'", fragSrvInRead1.FragSubject)
		}

		//zz("good: past the are we live check.")

		if ckt.IsClosed() {
			t.Fatalf("error: client side circuit '%v' should NOT be closed.", ckt.Name)
		}
		if serverCkt.IsClosed() {
			t.Fatalf("error: server circuit '%v' should NOT be closed.", serverCkt.Name)
		}

		//zz("about to ckt.Close() from the client side ckt")
		ckt.Close()

		//zz("good: past the ckt.Close()")
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
		//zz("good: past the serverCkt.IsClosed()")

		// are our lbp shutdown? they should be up! they should
		// survive any of their circuits shutting down!!

		if srv_lpb.IsClosed() {
			vv("bad! srv_lpb should be still open now!")
			t.Fatalf("error: server local '%v' should be open.", srv_lpb.PeerServiceName)
		}
		if cli_lpb.IsClosed() {
			vv("bad! cli_lpb should be still open now!")
			t.Fatalf("error: client local '%v' should be open.", cli_lpb.PeerServiceName)
		}

		// did the server peer code recognize the closed ckt?

		// we have been acting as the client through lbp, so the
		// client peer code has not been active. And that's super
		// useful to keep this test deterministic and not having
		// two competing reads on response channels.

		<-j.srvSync.gotCktHaltReq.Chan
		//zz("good: server saw the ckt peer code stopped reading ckt.")

		// sends and reads on the closed ckt should give errors / nil channel hangs

		cliNumCkt := cli_lpb.OpenCircuitCount()
		if cliNumCkt != 0 {
			t.Fatalf("expected zero open circuits, got cliNumCkt: '%v'", cliNumCkt)
		}
		srvNumCkt := srv_lpb.OpenCircuitCount()
		if srvNumCkt != 0 {
			t.Fatalf("expected zero open circuits, got srvNumCkt: '%v'", srvNumCkt)
		}

		// server side is responding well when this test proxies the client.

		//zz("   ========   now proxy the server and have ckt to client... separate test?")

		/*
			if false {
				srv_lpb, err = j.srv.PeerAPI.StartLocalPeer(ctx, j.srvServiceName, nil)
				panicOn(err)
				defer srv_lpb.Close()

				cli_lpb, err = j.cli.PeerAPI.StartLocalPeer(ctx, j.cliServiceName, nil)
				panicOn(err)
				defer cli_lpb.Close()
			}
		*/

		// future test idea, not below:
		// shut down the peer service on one side. does the other side
		// stay up, but clean up all the circuits associated with that service?

		// make a bunch of circuits to check, initated by cli and server
		var cli_ckts []*Circuit
		var srv_ckts []*Circuit

		var cli_send_frag []*Fragment
		var srv_send_frag []*Fragment

		for i := range 10 {
			cktname9 := fmt.Sprintf("cli_ckt9_%02d", i)
			// leak the ctx, we don't care here(!)
			ckt9, _, err := cli_lpb.NewCircuitToPeerURL(cktname9, srv_lpb.URL(), nil, 0)
			panicOn(err)
			//defer ckt.Close()
			cli_ckts = append(cli_ckts, ckt9)

			frag := NewFragment()
			frag.FragSubject = cktname9
			cli_send_frag = append(cli_send_frag, frag)

		}

		for i := range 10 {
			////zz("server makes new ckt, i = %v", i)
			cktname9 := fmt.Sprintf("srv_ckt9_%02d", i)
			// leak the ctx, we don't care here(!)
			ckt9, _, err := srv_lpb.NewCircuitToPeerURL(cktname9, cli_lpb.URL(), nil, 0)
			panicOn(err)
			////zz("server back from making new ckt, i = %v", i)
			//defer ckt.Close()
			srv_ckts = append(srv_ckts, ckt9)
			frag := NewFragment()
			frag.FragSubject = cktname9
			srv_send_frag = append(srv_send_frag, frag)
		}

		// cancellation from one side gets handled.

		// simplest send and recieve

		// start FragPart at 0. increment part when you echo it.
		// discard parts with numbers > 1.

		/* TODO:
		// launch indep goro to send/read on each of the 20 circuits from both sides.
		punisher := func(k int, ckt *Circuit, role string) {

		}
		k := 0
		var wg sync.WaitGroup
		for i := range 10 {
			ckt := cli_ckts[i]
			srvCkt := what //? what here
			punisher(k, ckt, "cli_read")
			punisher(k, srvCkt, "cli_send")
			k++
		}
		for i := range 10 {
			ckt := srv_ckts[i]
			k++
		}
		*/

		// close all but one from each
		for i := range 9 {
			cli_ckts[i].Close()
		}
		for i := range 9 {
			srv_ckts[i].Close()
		}
		if cli_ckts[9].IsClosed() {
			t.Fatalf("error: client circuit '%v' should NOT be closed.", cli_ckts[9].Name)
		}
		if srv_ckts[9].IsClosed() {
			t.Fatalf("error: server circuit '%v' should NOT be closed.", srv_ckts[9].Name)
		}
		for i := range 9 {
			if !cli_ckts[i].IsClosed() {
				t.Fatalf("error: client circuit '%v' should be closed.", cli_ckts[i].Name)
			}
			if !srv_ckts[i].IsClosed() {
				t.Fatalf("error: server circuit '%v' should be closed.", srv_ckts[i].Name)
			}
		}

		cli_ckts[9].Close()
		srv_ckts[9].Close()

		//select {}
	})

}
