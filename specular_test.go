package rpc25519

// Regression tests for the rpc25519-level bugs found by
// Specular in tube (see bug-report.md).

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/glycerine/idem"
	"github.com/quic-go/quic-go"
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

// CR-D-93: an empty, non-nil frag.Args became a nil map in
// ToMessage, and newCircuit then panicked writing into it.
func TestEmptyFragArgsGiveWritableMessageArgs(t *testing.T) {
	frag := NewFragment()
	frag.Args = map[string]string{}
	msg := frag.ToMessage()
	if msg.HDR.Args == nil {
		t.Fatal("ToMessage turned empty frag.Args into a nil map")
	}
	msg.HDR.Args["#circuitName"] = "ok" // newCircuit writes here
}

// stallConn accepts limit bytes, then times out every write.
type stallConn struct {
	limit, wrote int
}

func (c *stallConn) Write(p []byte) (int, error) {
	room := c.limit - c.wrote
	if room <= 0 {
		return 0, os.ErrDeadlineExceeded
	}
	if len(p) > room {
		c.wrote += room
		return room, os.ErrDeadlineExceeded
	}
	c.wrote += len(p)
	return len(p), nil
}
func (c *stallConn) Read(p []byte) (int, error)         { return 0, io.EOF }
func (c *stallConn) SetWriteDeadline(t time.Time) error { return nil }
func (c *stallConn) SetReadDeadline(t time.Time) error  { return nil }

// CR-D-85: a keepalive written under its timeout could be
// cut short mid-frame without the caller learning that the
// stream was now unusable.
func TestKeepAliveTimeoutReportsPartialFrame(t *testing.T) {
	w := newWorkspace("crd85", 4096, false, &Config{}, nil, &cliPairState{})
	msg := NewMessage()
	msg.HDR.Typ = CallKeepAlive
	timeout := 50 * time.Millisecond
	conn := &stallConn{limit: 12} // past the magic, inside the length
	err := w.sendMessage(conn, msg, &timeout)
	if !errors.Is(err, ErrPartialFrameWritten) {
		t.Fatalf("partial keepalive frame returned %v, want ErrPartialFrameWritten", err)
	}
	conn = &stallConn{limit: 0}
	if err := w.sendMessage(conn, msg, &timeout); errors.Is(err, ErrPartialFrameWritten) || err == nil {
		t.Fatalf("an unwritten keepalive should time out cleanly, got %v", err)
	}
}

// CR-D-85: the QUIC client never set c.cpair, so the first
// keepalive (and PingStats) dereferenced nil.
func TestQUICClientKeepAliveAndPingStats(t *testing.T) {
	if faketime {
		t.Skip("skip under synctest, net calls will never settle.")
	}
	cfg := NewConfig()
	cfg.UseQUIC = true
	cfg.ServerAddr = "127.0.0.1:0"
	srv := NewServer("srv_crd85", cfg)
	serverAddr, err := srv.Start()
	panicOn(err)
	defer srv.Close()
	srv.Register2Func("customEcho", customEcho)

	ccfg := NewConfig()
	ccfg.UseQUIC = true
	ccfg.ClientDialToHostPort = serverAddr.String()
	ccfg.ClientSendKeepAlive = 100 * time.Millisecond
	cli, err := NewClient("cli_crd85", ccfg)
	panicOn(err)
	panicOn(cli.Start())
	defer cli.Close()

	if ps := cli.PingStats(serverAddr.String()); ps == nil {
		t.Fatal("no PingStats")
	}
	time.Sleep(600 * time.Millisecond)
	req := NewMessage()
	req.HDR.ToServiceName, req.HDR.FromServiceName = "customEcho", "customEcho"
	req.JobSerz = []byte("after keepalives")
	if _, err := cli.SendAndGetReply(req, nil, 0); err != nil {
		t.Fatalf("QUIC client failed after sending keepalives: %v", err)
	}
	if cli.PingStats(serverAddr.String()).LastPingSentTmu == 0 {
		t.Fatal("no keepalive recorded as sent")
	}
}

// CR-D-86: Close lowered the shared QUIC socket's count on
// every call, so a repeated Close (or one by a client that
// never took a reference) closed the socket under other users,
// and a negative count panicked with the mutex held.
func TestQUICCloseReleasesSharedSocketOncePerHolder(t *testing.T) {
	udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	panicOn(err)
	defer udp.Close()
	cfg := NewConfig()
	cfg.UseQUIC = true
	cfg.shared.quicTransport = &quic.Transport{Conn: udp}
	cfg.shared.shareCount = 1 // held by a running server

	cli, err := NewClient("crd86", cfg)
	panicOn(err)
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Close of a never-started QUIC client panicked: %v", r)
			}
		}()
		cli.Close()
		cli.Close()
	}()
	if cfg.shared.isClosed || cfg.shared.shareCount != 1 {
		t.Fatalf("closing a client that held no reference released the shared socket: count %v closed %v",
			cfg.shared.shareCount, cfg.shared.isClosed)
	}
}

// recordingU records the auto-dial policy of each send.
type recordingU struct {
	UniversalCliSrv
	forbidAutoCli []bool
}

func (u *recordingU) SendOneWayMessage(ctx context.Context, msg *Message, errWriteDur time.Duration, forbidAutoCli bool) (bool, *LoopComm, error) {
	u.forbidAutoCli = append(u.forbidAutoCli, forbidAutoCli)
	return false, nil, ErrNetConnectionNotFound
}

// CR-D-29: a send on a circuit whose connection had died
// dialed a new auto-client synchronously, with no connect
// timeout, on the caller's goroutine (Tube's event loop).
// A circuit only exists on its connection: it must not dial.
func TestCircuitSendNeverAutoDials(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	u := &recordingU{}
	lpb := &LocalPeer{Halt: idem.NewHalter(), Ctx: ctx, U: u, PeerID: "me"}
	ckt := &Circuit{Context: ctx, CircuitID: "c1", LocalPeerID: "me", RemotePeerID: "them",
		LpbFrom: lpb, RpbTo: &RemotePeer{NetAddr: "tcp://127.0.0.1:9", PeerID: "them"}}
	_, err := lpb.SendOneWay(ckt, NewFragment(), -1, 1)
	if !errors.Is(err, ErrNetConnectionNotFound) {
		t.Fatalf("send on a circuit without a connection returned %v", err)
	}
	if len(u.forbidAutoCli) != 1 || !u.forbidAutoCli[0] {
		t.Fatalf("circuit send allowed an auto-client dial: %v", u.forbidAutoCli)
	}
}

// CR-D-41: when the hand-off of a new circuit lost its race
// (full channel, then the call's deadline), PreferExtant still
// returned err == nil with a live circuit nobody would read.
func TestNewCircuitHandOffFailureClosesCircuit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ckt := &Circuit{CircuitID: "orphan", Context: context.Background(), Halt: idem.NewHalter(),
		LpbFrom: &LocalPeer{Halt: idem.NewHalter(), HandleCircuitClose: make(chan *Circuit, 1)},
		RpbTo:   &RemotePeer{}}
	full := make(chan *Circuit) // nobody receiving
	cancel()                    // the call's deadline has passed
	err := handOffNewCircuit(ckt, full, ctx, nil, nil)
	if err == nil {
		t.Fatal("failed hand-off reported success")
	}
	if !ckt.IsClosed() {
		t.Fatal("undelivered circuit left open")
	}
}

// CR-D-42: an auto-client whose send loop died before its
// cleanup hook was installed stayed in remote2pair as a
// tombstone, so its address was never dialed again.
func TestDeadAutoClientLeavesNoTombstone(t *testing.T) {
	cfg := NewConfig()
	srv := NewServer("crd42", cfg)
	// never started; nothing to close
	cli := &Client{halt: idem.NewHalter(), garbageCollectThisRwPairCh: make(chan *autoCliInRwPair)}
	cli.halt.ReqStop.Close() // send loop already gone
	p := &rwPair{halt: idem.NewHalter()}
	key := "tcp://127.0.0.1:9"
	srv.remote2pair.Set(key, p)
	srv.pair2remote.Set(p, key)
	srv.autoClients = append(srv.autoClients, cli)
	if srv.registerAutoCliCleanup(cli, key, p) {
		t.Fatal("registered cleanup on a dead auto-client")
	}
	if _, ok := srv.remote2pair.Get(key); ok {
		t.Fatal("dead auto-client left a tombstone route")
	}
	if len(srv.autoClients) != 0 {
		t.Fatal("dead auto-client still listed")
	}
}

// CR-D-42: when a duplicate connection to the same address
// died, deletePair removed the live connection's route too.
func TestDeletePairOnlyRemovesItsOwnRoute(t *testing.T) {
	srv := NewServer("crd42b", NewConfig())
	// never started; nothing to close
	key := "tcp://127.0.0.1:9"
	live, dup := &rwPair{}, &rwPair{}
	srv.pair2remote.Set(dup, key)
	srv.remote2pair.Set(key, live) // the route belongs to live
	srv.pair2remote.Set(live, key)
	srv.deletePair(dup)
	if got, ok := srv.remote2pair.Get(key); !ok || got != live {
		t.Fatal("deleting a duplicate removed the live connection's route")
	}
}

// CR-D-37: Circuit.Close(reason) sent its end-of-circuit
// notice retyped as an ordinary CallPeerError, so the remote
// never closed its end (or, unroutable, dropped the whole
// connection).
func TestCloseWithReasonStaysEndCircuit(t *testing.T) {
	frag := NewFragment()
	frag.Typ = CallPeerEndCircuit
	frag.Err = "pruned redundant circuit"
	msg := frag.ToMessage()
	if msg.HDR.Typ != CallPeerEndCircuit {
		t.Fatalf("close notice went out as %v, want CallPeerEndCircuit", msg.HDR.Typ)
	}
	if msg.JobErrs != frag.Err {
		t.Fatalf("close reason lost: %q", msg.JobErrs)
	}
	user := NewFragment()
	user.Err = "application error"
	if user.ToMessage().HDR.Typ != CallPeerError {
		t.Fatal("an ordinary error fragment must still be CallPeerError")
	}
}

// lockProbeU reports whether a lock token was free when the
// circuit-established ack was sent.
type lockProbeU struct {
	UniversalCliSrv
	mut          chan bool
	sawLockHeld  bool
	sendsChecked int
}

func (u *lockProbeU) SendOneWayMessage(ctx context.Context, msg *Message, errWriteDur time.Duration, forbidAutoCli bool) (bool, *LoopComm, error) {
	u.sendsChecked++
	select {
	case <-u.mut: // free: put it back
		u.mut <- true
	default:
		u.sawLockHeld = true
	}
	return false, nil, ErrNetConnectionNotFound
}

// CR-D-33: bootstrapCircuit held the node-wide chanmut lock
// while sending CircuitEstablishedAck, so one peer with a stuck
// send loop blocked every other circuit setup on the node.
func TestCircuitAckSentWithoutSetupLockHeld(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	chanmut := make(chan bool, 1) // held: no token inside
	u := &lockProbeU{mut: chanmut}
	lpb := &LocalPeer{Halt: idem.NewHalter(), Ctx: ctx, U: u, PeerID: "me", PeerServiceName: "svc",
		TellPumpNewCircuit: make(chan *Circuit, 1), Remotes: NewMutexmap[string, *RemotePeer]()}
	msg := NewMessage()
	msg.HDR.Typ = CallPeerStartCircuit
	msg.HDR.FromPeerID, msg.HDR.From = "them", "tcp://127.0.0.1:9"
	msg.HDR.CallID = "ckt1"
	var skipUnlock bool
	lpb.provideRemoteOnNewCircuitCh(false, msg, ctx, &LoopComm{}, false, onRemote2ndSide, true, chanmut, &skipUnlock)
	if u.sendsChecked == 0 {
		t.Fatal("setup: the ack was never sent")
	}
	if u.sawLockHeld {
		t.Fatal("circuit-established ack sent while holding the node-wide setup lock")
	}
}

// CR-D-92: Circuit.Close blocked forever on the connection's
// cktServedDel channel once its send loop was gone, and each
// repeated Close posted another removal.
func TestCircuitCloseDoesNotBlockOnGoneSendLoop(t *testing.T) {
	gone := make(chan struct{})
	close(gone)
	mk := func(del chan *Circuit, loopGone <-chan struct{}) *Circuit {
		return &Circuit{CircuitID: "c", Halt: idem.NewHalter(),
			loopy:   &LoopComm{cktServedDel: del, sendLoopGone: loopGone},
			LpbFrom: &LocalPeer{Halt: idem.NewHalter(), HandleCircuitClose: make(chan *Circuit, 10)}}
	}
	ckt := mk(make(chan *Circuit), gone) // nobody will ever receive
	done := make(chan struct{})
	go func() { ckt.Close(nil); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Circuit.Close blocked on a send loop that is gone")
	}

	del := make(chan *Circuit, 10)
	ckt2 := mk(del, make(chan struct{}))
	ckt2.Close(nil)
	ckt2.Close(nil)
	if len(del) != 1 {
		t.Fatalf("repeated Close posted %v removals, want 1", len(del))
	}
}
