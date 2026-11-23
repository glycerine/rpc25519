package rpc25519

// Regression tests for the rpc25519-level bugs found by
// Specular in tube (see bug-report.md).

import (
	"strings"
	"testing"
)

// Bug 9 (Medium): when a peer restarts, frames still sent on
// circuits of its old incarnation are routed by address onto
// the connection to its new incarnation. The read loop did
// not find the (old) ToPeerID/circuit and returned, tearing
// down that healthy connection and every circuit on it. Such
// a frame must just be dropped.
func Test1009_stale_circuit_frame_does_not_tear_down_connection(t *testing.T) {
	onlyBubbled(t, func(t *testing.T) {

		cfg := NewConfig()
		cfg.UseSimNet = true
		cfg.ServerAddr = "127.0.0.1:0"
		srv := NewServer("srv_test1009", cfg)
		serverAddr, err := srv.Start()
		panicOn(err)
		defer srv.Close()
		simnet := cfg.GetSimnet()
		defer simnet.Close()

		srv.Register2Func("customEcho", customEcho)

		cfg.ClientDialToHostPort = serverAddr.String()
		cli, err := NewClient("cli_test1009", cfg)
		panicOn(err)
		panicOn(cli.Start())
		defer cli.Close()

		echo := func(what string) error {
			req := NewMessage()
			req.HDR.ToServiceName = "customEcho"
			req.HDR.FromServiceName = "customEcho"
			req.JobSerz = []byte(what)
			reply, err := cli.SendAndGetReply(req, nil, 0)
			if err != nil {
				return err
			}
			if !strings.HasPrefix(string(reply.JobSerz), what) {
				t.Fatalf("bad echo reply '%v'", string(reply.JobSerz))
			}
			return nil
		}
		if err := echo("before"); err != nil {
			t.Fatalf("setup: first call failed: %v", err)
		}
		_, connsBefore := srv.ListClients()

		// a frame for the old incarnation's circuit: the
		// server has no such local peer or circuit.
		stale := NewMessage()
		stale.HDR.Typ = CallPeerTraffic
		stale.HDR.ToPeerID = "peer-id-of-the-dead-incarnation"
		stale.HDR.CallID = "circuit-id-of-the-dead-incarnation"
		stale.HDR.ToServiceName = "tube-replica"
		stale.JobSerz = []byte("RequestPreVote for incarnation 0")
		panicOn(cli.OneWaySend(stale, nil, 0))

		if err := echo("after"); err != nil {
			t.Fatalf("Bug 9: the connection was torn down by one stale-circuit frame: %v", err)
		}
		if _, n := srv.ListClients(); n != connsBefore {
			t.Fatalf("Bug 9: server connections went %v -> %v after a stale-circuit frame", connsBefore, n)
		}
	})
}
