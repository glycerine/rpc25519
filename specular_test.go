package rpc25519

// Regression tests for the rpc25519-level bugs found by
// Specular in tube (see bug-report.md).

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"
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

// stalledPeer accepts TCP connections and never reads
// from them: a SIGSTOPped or wedged peer whose kernel still
// holds the connection open, so its receive window fills.
func stalledPeer(t *testing.T) (addr string, stop func()) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	panicOn(err)
	var conns []net.Conn
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			conns = append(conns, c) // never read.
		}
	}()
	return ln.Addr().String(), func() {
		ln.Close()
		<-done
		for _, c := range conns {
			c.Close()
		}
	}
}

func bigPeerMessage(to string) *Message {
	msg := NewMessage()
	msg.HDR.Typ = CallPeerTraffic
	msg.HDR.To = to
	msg.JobSerz = make([]byte, 1<<20)
	return msg
}

// Bug 8 (High, tube): every tube event-loop send used
// errWriteDur -1, documented as "do not block", but
// sendOneWayMessage's -1 hand-off to the connection's send
// loop blocked with no exit once a peer stopped reading:
// one stalled peer froze the single event loop of every
// replica that sent to it. With SendChannelBufferSlots > 0
// a -1 or positive errWriteDur send must never block, and a
// positive errWriteDur must abandon the stalled write by
// its deadline (HDR.Deadline).
func Test1008_send_to_stalled_peer_does_not_block(t *testing.T) {
	if faketime {
		t.Skip("needs a real TCP peer whose receive window fills; simnet does not model that")
	}
	for _, mode := range []time.Duration{-1, 200 * time.Millisecond} {
		addr, stop := stalledPeer(t)

		cfg := NewConfig()
		cfg.TCPonly_no_TLS = true
		cfg.SendChannelBufferSlots = 4
		cfg.ClientDialToHostPort = addr
		cfg.QuietTestMode = true
		cli, err := NewClient("cli_test1008", cfg)
		panicOn(err)
		panicOn(cli.Start())

		var last *Message
		var queueFull int
		done := make(chan struct{})
		go func() {
			defer close(done)
			for i := 0; i < 64; i++ {
				msg := bigPeerMessage(addr)
				_, _, err := cli.SendOneWayMessage(context.Background(), msg, mode, true)
				if err == ErrSendQueueFull {
					queueFull++
				}
				last = msg
			}
		}()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatalf("Bug 8: errWriteDur=%v send to a stalled peer blocked the caller", mode)
		}
		if queueFull == 0 {
			t.Fatalf("errWriteDur=%v: expected ErrSendQueueFull once the stalled connection's queue filled", mode)
		}
		if mode > 0 {
			// a queued message past its deadline is abandoned
			// (not written), and reported via DoneCh/LocalErr.
			_ = last
			select {
			case <-cli.halt.ReqStop.Chan:
				// stalled mid-frame: the connection was closed.
			case <-time.After(5 * time.Second):
				t.Fatalf("errWriteDur=%v: the writer never gave up on the stalled connection", mode)
			}
		}
		cli.Close()
		stop()
	}
}

// CR-D-83: an unknown compression byte, or a corrupt
// compressed body, from a peer panicked the server's read
// loop and aborted the whole process.
func TestDecompressRejectsBadPeerInput(t *testing.T) {
	d := newDecomp(1000)
	p := newPressor(1000)
	for _, tc := range []struct {
		name   string
		magic7 magic7b
		body   []byte
	}{
		{"unimplemented lz4 byte", 2, []byte("payload")},
		{"unknown byte", 9, []byte("payload")},
		{"corrupt s2 body", 1, []byte("this is not s2 data at all")},
	} {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("%v: handleDecompress panicked: %v", tc.name, r)
				}
			}()
			if _, err := d.handleDecompress(tc.magic7, tc.body); err == nil {
				t.Errorf("%v: handleDecompress accepted bad input", tc.name)
			}
		}()
	}
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("handleCompress panicked on an unknown byte: %v", r)
			}
		}()
		if _, err := p.handleCompress(9, []byte("payload")); err == nil {
			t.Error("handleCompress accepted an unknown compression byte")
		}
	}()
}
