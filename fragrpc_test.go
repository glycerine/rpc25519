package rpc25519

import (
	"context"
	"fmt"
	//"os"
	"strings"
	"testing"
	"time"
	//cv "github.com/glycerine/goconvey/convey"
	//"github.com/glycerine/idem"
)

func Test410_FragRPC_NewCircuitToPeerURL_with_empty_PeerID_in_URL(t *testing.T) {

	// empty ToPeerID in URL means we use
	//
	// CallPeerStartCircuitAtMostOne       CallType = 116
	// and all ckt are now acked with
	// CallPeerCircuitEstablishedAck       CallType = 114
	//

	// and we need to verify that we eventually
	// get back a proper ckt with RemotePeerID set.
	// The "make up a CallID and ask the new remote to
	// adopt it" does not work when the remote is
	// already up and we only want a new circuit
	// from it.

	suffix := "emptyPeerID_410"
	cfg := NewConfig()
	cfg.ServiceLimit = 1

	j := newTestJunk3(suffix, cfg)
	defer j.cleanup()

	// srv is started. cli is started.
	// "cli_" + suffix peerServiceFunc has been registered at client (j.cliServiceName)
	// (cli_emptyPeerID_410)
	// "srv_" + suffix peerServiceFunc has been registered at server (j.srvServiceName)
	// (srv_emptyPeerID_410)
	// No peers have been started yet.

	ctx := context.Background()

	cliPeerName := "cliPeerName_410"
	//vv("starting client peer service '%v'", j.cliServiceName)
	cli_lpb, err := j.cli.PeerAPI.StartLocalPeer(ctx, j.cliServiceName, nil, cliPeerName)
	panicOn(err)
	defer cli_lpb.Close()

	// 2,3 only: part2 on makes part3 hang. fixed by omitting the CallPeerError error.
	//part1, part2, part3, part4 := false, true, true, false
	part1, part2, part3, part4 := true, true, true, true

	netAddr := "tcp://" + j.cfg.ClientDialToHostPort
	if part1 { // focus on server -> client for a moment

		//serviceName := "srv_" + suffix
		//url := netAddr + sep + serviceName
		url := netAddr + sep + j.srvServiceName
		_ = url
		//vv("url constructed = '%v'", url)

		cktName := "ckt-410" // what to call our new circuit
		_ = cktName
		//ckt, ctx, err := cli_lpb.NewCircuitToPeerURL(cktName, url, nil, 0)

		wait := true
		//	ckt, err := cli_lpb.U.StartRemotePeerAndGetCircuit(cli_lpb, cktName, nil, serviceName, netAddr, 0, wait)

		ckt, err := j.cli.PeerAPI.StartRemotePeerAndGetCircuit(cli_lpb, cktName, nil, j.srvServiceName, netAddr, 0, wait)

		panicOn(err)
		_ = ctx

		//vv("ckt = '%v'", ckt)

		if ckt.RemotePeerID == "" {
			//vv("ckt='%v'", ckt)
			panic("ckt.RemotePeerID should not be empty")
		}

		// now ask for the same peer as before to respond,
		// supposing its a freshly booted remote node whose
		// address we have, but we do not have its PeerID yet.x

		cktName2 := "ckt-410-2nd" // what to call our new circuit
		ckt2, err := j.cli.PeerAPI.PreferExtantRemotePeerGetCircuit(cli_lpb, cktName2, nil, j.srvServiceName, netAddr, 0)
		panicOn(err)
		vv("ckt2 = '%v'", ckt2)

		// we want that the remote PeerID is the same as the one
		// we started originally/first time.
		if ckt2.RemotePeerID != ckt.RemotePeerID {
			panic(fmt.Sprintf("wanted ckt.RemotePeerID='%v', got ckt2.RemotePeerID='%v'", ckt.RemotePeerID, ckt2.RemotePeerID))
		}
	}

	if part2 {
		// we should get error back from server if
		// no such peer service name available.

		// we should get an error if there is no such service name available!
		wrongServiceNameSrv := "service_name_not_avail_on_server"
		cktName4 := "cli_cktname_will_never_be_used"
		var ckt4 *Circuit
		ckt4, err = j.cli.PeerAPI.PreferExtantRemotePeerGetCircuit(cli_lpb, cktName4, nil, wrongServiceNameSrv, netAddr, time.Second*2)
		if err == nil {
			panic("should get no name found!")
		}
		if err == ErrTimeout {
			panic("should get no such service name found! not ErrTimeout")
		}
		//vv("server no-such-service checked: good, got err = '%v'", err)
		// cli_emptyPeerID_410 ckt.Name is not handling Errors!
		if ckt4 != nil {
			alwaysPrintf("ckt4 = %#v", ckt4)
			panic("ckt4 should be nil")
		}
	}

	// ============================
	// good, client to server works.
	// Now check the other way around: server -to-> client
	//
	// This also verifies that our last error
	// did not accidentally take down the whole client!

	//vv("and check from server to client, the same test of PreferExtantRemotePeerGetCircuit. to cli_lpb.URL() = '%v'", cli_lpb.URL())

	// already started above, will hit limit of 1 if we do it again.
	// verify that we can an error if we try:
	srv_lpb, err := j.srv.PeerAPI.StartLocalPeer(ctx, j.srvServiceName, nil, "srv_lpb_peer_name")
	if err == nil {
		panic("wanted limit 1 error!")
	}
	if !strings.Contains(err.Error(), "already at cfg.ServiceLimit") {
		panic(fmt.Sprintf("did not see 'already at cfg.ServiceLimit' in error: '%v'", err))
	}

	// get the currently up peer instead:
	srv_lpbs := j.srv.PeerAPI.GetLocalPeers(j.srvServiceName)
	if len(srv_lpbs) != 1 {
		panic(fmt.Sprintf("should only be 1, not %v", len(srv_lpbs)))
	}
	srv_lpb = srv_lpbs[0]

	// truncate the client's URL to get TCP netAddr and service name for client

	cliNetAddr, cliServiceName, cliPeerID, cliCktID, err := ParsePeerURL(cli_lpb.URL())
	panicOn(err)
	if cliServiceName != j.cliServiceName {
		panic("huh?")
	}
	_ = cliCktID
	//vv("client serviceName = '%v'", cliServiceName)
	//vv("client netAddr = '%v'", cliNetAddr)
	//vv("client peerID = '%v'", cliPeerID)
	//vv("client cktID = '%v'", cliCktID)

	cliurl := cliNetAddr + sep + cliServiceName
	_ = cliurl
	//vv("client url in use: '%v'", cliurl)

	cktName3 := "ckt-410-3rd" // what to call our new circuit
	if part3 {
		// works. but now hung? works on its own; crosstalk between test parts...
		// maybe the error test above is taking down the service?
		ckt3, err := j.srv.PeerAPI.PreferExtantRemotePeerGetCircuit(srv_lpb, cktName3, nil, cliServiceName, cliNetAddr, 0)
		panicOn(err)

		//vv("ckt3 = '%v'", ckt3)

		// we want that the remote cli_lpb.PeerID is the same as the one
		// we started originally/first time.
		if ckt3.RemotePeerID != cliPeerID {
			panic(fmt.Sprintf("wanted cli_lpb.PeerID='%v', got ckt3.RemotePeerID='%v'", cliPeerID, ckt3.RemotePeerID))
		}
	}

	if part4 {
		// works
		// we should get an error if there is no such service name available!
		wrongServiceName := "service_name_not_avail_on_client"
		_, err = j.srv.PeerAPI.PreferExtantRemotePeerGetCircuit(srv_lpb, cktName3, nil, wrongServiceName, cliNetAddr, time.Second*2)
		if err == nil {
			panic("should get no name found!")
		}
		if err == ErrTimeout {
			panic("should get no such service name found! not ErrTimeout")
		}
		//vv("good, got err = '%v'", err)
	}

	// verify ServiceLimit rejects more than 1 instance of a peer service name

	// server -> client
	ckt5, err := j.srv.PeerAPI.StartRemotePeerAndGetCircuit(srv_lpb, cktName3+"_over_1_limit", nil, cliServiceName, cliNetAddr, 0, true)

	if ckt5 != nil {
		panic("wanted nil ckt5")
	}
	if err == nil {
		panic("wanted rejection over ServiceLimit")
	}
	if !strings.Contains(err.Error(), "already at cfg.ServiceLimit") {
		panic(fmt.Sprintf("did not see 'already at cfg.ServiceLimit' in error: '%v'", err))
	}
	vv("good 5, got err = '%v'", err)

	if true {
		// client -> server
		vv("ServiceLimit enforced client to server too ==========")
		ckt6, err := j.cli.PeerAPI.StartRemotePeerAndGetCircuit(cli_lpb, cktName3+"_over_1_limit", nil, j.srvServiceName, netAddr, 0, true)

		if ckt6 != nil {
			panic(fmt.Sprintf("wanted nil ckt6; got '%v'", ckt6))
		}
		if err == nil {
			panic("wanted rejection over ServiceLimit")
		}
		if !strings.Contains(err.Error(), "already at cfg.ServiceLimit") {
			panic(fmt.Sprintf("did not see 'already at cfg.ServiceLimit' in error: '%v'", err))
		}
		vv("good 6, got err = '%v'", err)
	}
}
