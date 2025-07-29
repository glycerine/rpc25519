package rpc25519

// srv.go: simple TCP server, with TLS encryption.

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"errors"
	"fmt"
	"go/token"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/glycerine/greenpack/msgp"
	"github.com/glycerine/idem"
	"github.com/glycerine/ipaddr"
	"github.com/glycerine/rpc25519/selfcert"
	"github.com/quic-go/quic-go"
)

var _ = os.MkdirAll
var _ = fmt.Printf

const notClient = false
const yesIsClient = true

//var serverAddress = "0.0.0.0:8443"

//var serverAddress = "192.168.254.151:8443"

var ErrContextCancelled = fmt.Errorf("rpc25519 error: context cancelled")

var ErrHaltRequested = fmt.Errorf("rpc25519 error: halt requested")
var ErrSendTimeout = fmt.Errorf("rpc25519 error: send timeout")

// break deadlocks on cli/srv both blocked on sending.
var ErrAntiDeadlockMustQueue = fmt.Errorf("rpc25519 error: must queue send to avoid deadlock. Could not send immediately.")

// boundCh should be buffered, at least 1, if it is not nil. If not nil, we
// will send the bound net.Addr back on it after we have started listening.
func (s *Server) runServerMain(
	serverAddress string, tcp_only bool, certPath string, boundCh chan net.Addr) {

	srvNotInit := s.srvStarted.CompareAndSwap(false, true)
	if !srvNotInit {
		panic("can only start Server once")
	}
	//vv("runServerMain running")

	defer func() {
		s.halt.ReqStop.Close()
		s.halt.Done.Close()
	}()
	s.tmStart = time.Now()

	//vv("s.cfg.UseSimNet=%v", s.cfg.UseSimNet)
	if s.cfg.UseSimNet {
		simNetConfig := &s.cfg.SimNetConfig

		// note this blocks until the server exits.
		s.runSimNetServer(serverAddress, boundCh, simNetConfig)
		if !s.cfg.QuietTestMode {
			alwaysPrintf("runSimNetServer exited: %v", s.name)
		}
		return
	}

	log.SetFlags(log.LstdFlags | log.Lshortfile) // Add Lshortfile for short file names

	s.cfg.checkPreSharedKey("server")
	//vv("server: s.cfg.encryptPSK = %v", s.cfg.encryptPSK)

	dirCerts := GetCertsDir()

	sslCA := fixSlash(dirCerts + "/ca.crt") // path to CA cert

	keyName := "node"
	if s.cfg.ServerKeyPairName != "" {
		keyName = s.cfg.ServerKeyPairName
	}

	// since was redundant always,
	// selfcert.LoadNodeTLSConfigProtected() below does not use.
	// So commenting out:
	// path to CA cert to verify client certs, can be same as sslCA
	// sslClientCA := sslCA

	sslCert := fixSlash(fmt.Sprintf(dirCerts+"/%v.crt", keyName))    // path to server cert
	sslCertKey := fixSlash(fmt.Sprintf(dirCerts+"/%v.key", keyName)) // path to server key

	if certPath != "" {
		sslCA = fixSlash(fmt.Sprintf("%v/ca.crt", certPath)) // path to CA cert
		//sslClientCA = sslCA
		sslCert = fixSlash(fmt.Sprintf("%v/%v.crt", certPath, keyName))    // path to server cert
		sslCertKey = fixSlash(fmt.Sprintf("%v/%v.key", certPath, keyName)) // path to server key
	}

	var err error
	var config *tls.Config

	if !tcp_only {
		if s.cfg.UseSimNet {
			panic("cannot have both TLS and UseSimNet true")
		}

		// handle pass-phrase protected certs/node.key
		config, s.creds, err = selfcert.LoadNodeTLSConfigProtected(true, sslCA, sslCert, sslCertKey)
		if err != nil {
			panic(fmt.Sprintf("error on LoadServerTLSConfig(): '%v'", err))
		}
	}

	// Not needed now that we have proper CA cert from gen.sh; or
	// perhaps this is the default anyway(?)
	// In any event, "localhost" is what we see during handshake; but
	// maybe that is because localhost is what we put in the ca.cnf and openssl-san.cnf
	// as the CN and DNS.1 names too(!)
	//config.ServerName = "localhost" // this would be the name of the remote client.

	// start of as http, the get CONNECT and hijack to TCP.

	if tcp_only {
		if s.cfg.UseSimNet {
			panic("cannot have both TCPonly_no_TLS and UseSimNet true")
		}
		// actually just run TCP and not TLS, since we might not have cert authority (e.g. under test)
		s.runTCP(serverAddress, boundCh)
		return
	}

	if s.cfg.SkipVerifyKeys {
		// turns off client cert checking, allowing any
		// random person on the internet to connect... not a good idea!
		config.ClientAuth = tls.NoClientCert
	} else {
		config.ClientAuth = tls.RequireAndVerifyClientCert
	}

	if s.cfg.UseQUIC {
		if s.cfg.TCPonly_no_TLS {
			panic("cannot have both UseQUIC and TCPonly_no_TLS true")
		}
		if s.cfg.UseSimNet {
			panic("cannot have both UseQUIC and UseSimNet true")
		}
		s.runQUICServer(serverAddress, config, boundCh)
		return
	}

	// Listen on the specified serverAddress
	listener, err := tls.Listen("tcp", serverAddress, config)
	if err != nil {
		log.Fatalf("Failed to listen on %v: %v", serverAddress, err)
	}
	defer listener.Close()

	addr := listener.Addr()
	//vv("Server listening on %v://%v   ... addr='%#v'/%T", addr.Network(), addr.String(), addr, addr) // net.TCPAddr
	//vv("Server listening on %v://%v", addr.Network(), addr.String())

	switch a := addr.(type) {
	case *net.TCPAddr:
		// a.IP is a net.IP
		if a.IP.IsUnspecified() {
			externalIP, extNetIP := ipaddr.GetExternalIP2() // e.g. 100.x.x.x
			vv("have unspecified IP, trying to report a specific external: '%v'", externalIP)
			a.IP = extNetIP
		}
	default:
		panic(fmt.Sprintf("arg! handle this type: %T", addr))
	}

	// net.ParseIP() to go from string -> net.IP if need be.
	//scheme, ip, port, isUnspecified, isIPv6, err := ipaddr.ParseURLAddress(hostIP)
	//panicOn(err)
	//vv("server defaults to binding: scheme='%v', ip='%v', port=%v, isUnspecified='%v', isIPv6='%v'", scheme, ip, port, isUnspecified, isIPv6)

	if boundCh != nil {
		timeout_100msec := s.NewTimer(100 * time.Millisecond)
		if timeout_100msec == nil {
			// happens on system shutdown
			return
		}
		defer timeout_100msec.Discard()

		select {
		case boundCh <- addr:
		case <-timeout_100msec.C:
		}
	}

	s.mut.Lock() // avoid data races
	addrs := addr.Network() + "://" + addr.String()
	s.boundAddressString = addrs
	s.netAddr = addr
	AliasRegister(addrs, addrs+" (server: "+s.name+")")
	s.lsn = listener // allow shutdown
	s.mut.Unlock()

	for {
		select {
		case <-s.halt.ReqStop.Chan:
			return
		default:
		}

		// Accept a new connection
		conn, err := listener.Accept()
		if err != nil {
			if strings.Contains(err.Error(), "use of closed network connection") {
				// check for shutdown
				select {
				case <-s.halt.ReqStop.Chan:
					return
				default:
				}
			}
			alwaysPrintf("Failed to accept connection: %v", err)
			continue
		}
		//vv("Accepted connection from %v", conn.RemoteAddr())

		// Handle the connection in a new goroutine
		tlsConn := conn.(*tls.Conn)

		go s.handleTLSConnection(tlsConn)
	}
}

func (s *Server) runTCP(serverAddress string, boundCh chan net.Addr) {

	// Listen on the specified serverAddress
	listener, err := net.Listen("tcp", serverAddress)
	if err != nil {
		log.Fatalf("Failed to listen on %v: %v", serverAddress, err)
	}
	defer listener.Close()

	addr := listener.Addr()

	s.mut.Lock()
	addrs := addr.Network() + "://" + addr.String()
	s.boundAddressString = addrs
	AliasRegister(addrs, addrs+" (tcp_server: "+s.name+")")
	// set s.lsn under mut to prevent data race.
	s.lsn = listener // allow shutdown
	s.mut.Unlock()

	//vv("Server listening on %v://%v", addr.Network(), addr.String())

	if boundCh != nil {
		timeout_100msec := s.NewTimer(100 * time.Millisecond)
		if timeout_100msec == nil {
			// happens on system shutdown
			return
		}
		defer timeout_100msec.Discard()

		select {
		case boundCh <- addr:
		case <-timeout_100msec.C:
		}
	}

	if s.cfg.HTTPConnectRequired {
		mux := http.NewServeMux()
		mux.Handle(DefaultRPCPath, s) // calls back to Server.ServeHTTP(),
		httpsrv := &http.Server{Handler: mux}
		httpsrv.Serve(listener) // calls Server.serveConn(conn) with each new connection.
		return
	}

acceptAgain:
	for {
		select {
		case <-s.halt.ReqStop.Chan:
			return
		default:
		}

		// Accept a new connection
		conn, err := listener.Accept()
		if err != nil {
			// If it is a shutdown request, the the s.halt.ReqStop.Chan will return for us.
			if strings.Contains(err.Error(), "use of closed network connection") {
				continue acceptAgain // fullRestart
			}
			alwaysPrintf("Failed to accept connection: %v", err)
			continue acceptAgain
		}
		//vv("server accepted connection from %v", conn.RemoteAddr())

		if false {
			// another rpc system did this:
			if tc, ok := conn.(*net.TCPConn); ok {
				theTCPKeepAlivePeriod := time.Minute * 3
				if theTCPKeepAlivePeriod > 0 {
					tc.SetKeepAlive(true)
					tc.SetKeepAlivePeriod(theTCPKeepAlivePeriod)
					// For *only* 10 seconds, the OS will try to send
					// data even after we close. The default is longer, wethinks.
					tc.SetLinger(10)
				}
			}
		}

		s.serveConn(conn)
	}
}

// can be called after HTTP CONNECT hijack too; see Server.ServeHTTP().
func (s *Server) serveConn(conn net.Conn) {

	//vv("tcp only server: s.cfg.encryptPSK = %v", s.cfg.encryptPSK)
	var randomSymmetricSessKey [32]byte
	var cliEphemPub []byte
	var srvEphemPub []byte
	var cliStaticPub ed25519.PublicKey

	if !s.cfg.TCPonly_no_TLS && s.cfg.encryptPSK {
		var err error
		switch {
		case useVerifiedHandshake:
			randomSymmetricSessKey, cliEphemPub, srvEphemPub, cliStaticPub, err =
				symmetricServerVerifiedHandshake(conn, s.cfg.preSharedKey, s.creds)

		case wantForwardSecrecy:
			randomSymmetricSessKey, cliEphemPub, srvEphemPub, cliStaticPub, err =
				symmetricServerHandshake(conn, s.cfg.preSharedKey, s.creds)

		case mixRandomnessWithPSK:
			randomSymmetricSessKey, err = simpleSymmetricServerHandshake(conn, s.cfg.preSharedKey, s.creds)

		default:
			randomSymmetricSessKey = s.cfg.preSharedKey
		}

		if err != nil {
			alwaysPrintf("tcp/tls failed to athenticate: '%v'", err)
			//continue acceptAgain
			return
		}
	}

	pair := s.newRWPair(conn)
	pair.randomSymmetricSessKeyFromPreSharedKey = randomSymmetricSessKey
	pair.cliEphemPub = cliEphemPub
	pair.srvEphemPub = srvEphemPub
	pair.cliStaticPub = cliStaticPub

	go pair.runSendLoop(conn)
	go pair.runReadLoop(conn)
}

func (s *Server) handleTLSConnection(conn *tls.Conn) {
	//vv("top of handleTLSConnection()")

	// Perform the handshake; it is lazy on first Read/Write, and
	// we want to check the certifcates from the client; we
	// won't get them until the handshake happens. From the docs:
	//
	// Handshake runs the client or server handshake protocol if it has not yet been run.
	//
	// Most uses of this package need not call Handshake explicitly:
	// the first Conn.Read or Conn.Write will call it automatically.
	//
	// For control over canceling or setting a timeout on a handshake,
	// use Conn.HandshakeContext or the Dialer's DialContext method instead.

	// Create a context with a timeout for the handshake, since
	// it can hang.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	_ = ctx
	defer cancel()

	// ctx gives us a timeout. Otherwise, one must set a deadline
	// on the conn to avoid an infinite hang during handshake.
	if err := conn.HandshakeContext(ctx); err != nil {
		alwaysPrintf("tlsConn.Handshake() failed: '%v'", err)
		return
	}

	knownHostsPath := "known_client_keys"
	connState := conn.ConnectionState()
	raddr := conn.RemoteAddr()
	remoteAddr := strings.TrimSpace(raddr.String())

	if !s.cfg.SkipVerifyKeys {
		// NB only ed25519 keys are permitted, any others will result
		// in an immediate error
		good, bad, wasNew, err := hostKeyVerifies(knownHostsPath, &connState, remoteAddr)
		_ = wasNew
		_ = bad
		if err != nil && len(good) == 0 {
			//fmt.Fprintf(os.Stderr, "key failed list:'%#v': '%v'\n", bad, err)
			return
		}
		if err != nil {
			//vv("hostKeyVerifies returned error '%v' for remote addr '%v'", err, remoteAddr)
			return
		}
		const showAcceptedIdentities = false
		if showAcceptedIdentities {
			for i := range good {
				alwaysPrintf("accepted identity for client: '%v' (was new: %v)\n", good[i], wasNew)
			}
		}
	}

	// end of handleTLSConnection()
	s.serveConn(conn)
}

func (s *rwPair) runSendLoop(conn net.Conn) {
	defer func() {
		s.Server.deletePair(s)
		s.halt.ReqStop.Close()
		s.halt.Done.Close()
	}()

	sendLoopGoroNum := GoroNumber()
	_ = sendLoopGoroNum
	//vv("srv.go rwPair.runSendLoop(cli '%v' -> '%v' srv) sendLoopGoroNum = [%v] for pairID = '%v'", remote(conn), local(conn), sendLoopGoroNum, s.pairID)

	symkey := s.cfg.preSharedKey
	if s.cfg.encryptPSK {
		s.mut.Lock()
		symkey = s.randomSymmetricSessKeyFromPreSharedKey
		s.mut.Unlock()
	}

	//vv("about to make a newBlabber for server send loop; s.Server.cfg = %p", s.Server.cfg)
	w := newBlabber("server send loop", symkey, conn, s.Server.cfg.encryptPSK, maxMessage, true, s.Server.cfg, s, nil)

	// implement ServerSendKeepAlive
	var lastPing time.Time
	var doPing bool
	var pingEvery time.Duration
	var pingWakeTimer *SimTimer
	var pingWakeCh <-chan time.Time
	var keepAliveWriteTimeout time.Duration // := s.cfg.WriteTimeout

	if s.cfg.ServerSendKeepAlive > 0 {
		doPing = true
		pingEvery = s.cfg.ServerSendKeepAlive
		lastPing = time.Now()
		pingWakeTimer = s.Server.NewTimer(pingEvery)
		if pingWakeTimer == nil {
			// shutdown in progress
			return
		}
		//pingWakeTimer.Discard()
		pingWakeCh = pingWakeTimer.C
		// keep the ping attempts to a minimum to keep this loop lively.
		if keepAliveWriteTimeout == 0 || keepAliveWriteTimeout > 10*time.Second {
			keepAliveWriteTimeout = 2 * time.Second
		}
	}

	for {
		if doPing {
			now := time.Now()
			var nextPingDur time.Duration
			if time.Since(lastPing) > pingEvery {
				// transmit the EpochID as the StreamPart in keepalives.
				s.keepAliveMsg.HDR.StreamPart = atomic.LoadInt64(&s.epochV.EpochID)
				err := w.sendMessage(conn, &s.keepAliveMsg, &keepAliveWriteTimeout)
				//vv("srv sent rpc25519 keep alive. err='%v'; keepAliveWriteTimeout='%v'", err, keepAliveWriteTimeout)
				if err != nil {
					alwaysPrintf("server had problem sending keep alive: '%v'", err)
				} else {
					s.lastPingSentTmu.Store(now.UnixNano())
				}
				lastPing = now
				nextPingDur = pingEvery
			} else {
				nextPingDur = lastPing.Add(pingEvery).Sub(now)
			}
			pingWakeTimer.Discard()
			pingWakeTimer = s.Server.NewTimer(nextPingDur)
			if pingWakeTimer == nil {
				// shutdown in progress
				return
			}
			pingWakeCh = pingWakeTimer.C
		}

		select {
		case <-pingWakeCh:
			// check and send above.
			continue

		case msg := <-s.SendCh:
			//vv("srv %v (%v) sendLoop got from s.SendCh, sending msg.HDR = '%v'", s.Server.name, s.from, msg.HDR.String())

			real, ok := s.Server.unNAT.Get(msg.HDR.To)
			if ok && real != msg.HDR.To {
				//vv("unNAT replacing msg.HDR.To '%v' -> '%v'", msg.HDR.To, real)
				msg.HDR.To = real
			}

			//err := w.sendMessage(conn, msg, &s.cfg.WriteTimeout)
			err := w.sendMessage(conn, msg, nil)
			if err != nil {
				// notify any short-time-waiting server push user.
				// This is super useful to let goq retry jobs quickly.
				msg.LocalErr = err
				if msg.DoneCh != nil {
					msg.DoneCh.Close()
				}
				// can become very noisy on shutdown, comment out.
				//alwaysPrintf("sendMessage got err = '%v'; on trying to send Seqno=%v", err, msg.HDR.Seqno)
				// just let user try again?
			} else {
				// tell caller there was no error.
				if msg.DoneCh != nil {
					msg.DoneCh.Close()
				}
				lastPing = time.Now() // no need for ping
			}
		case <-s.halt.ReqStop.Chan:
			return
		}
	}
}

func (s *rwPair) runReadLoop(conn net.Conn) {

	if s.Server.cfg.UseSimNet {

	}

	ctx, canc := context.WithCancel(context.Background())
	defer func() {
		//vv("rpc25519.Server: runReadLoop shutting down for local conn = '%v'", conn.LocalAddr())
		canc()
		s.halt.ReqStop.Close()
		s.halt.Done.Close()
		conn.Close() // just the one, let other clients continue.
	}()

	localAddr := local(conn)
	remoteAddr := remote(conn)

	symkey := s.cfg.preSharedKey
	if s.cfg.encryptPSK {
		s.mut.Lock()
		symkey = s.randomSymmetricSessKeyFromPreSharedKey
		s.mut.Unlock()
	}

	//vv("about to make a newBlabber for server read loop; s.Server.cfg = %p", s.Server.cfg)
	w := newBlabber("server read loop", symkey, conn, s.Server.cfg.encryptPSK, maxMessage, true, s.Server.cfg, s, nil)

	for {
		select {
		case <-s.halt.ReqStop.Chan:
			//vv("s.halt.ReqStop.Chan requested!")
			return
		default:
		}

		// It does not work to use a timeout with readMessage.
		// Under load, we will get
		// partial reads which are then difficult to
		// recover from, because we have not tracked
		// how much of the rest of the incoming
		// stream needs to be discarded!
		// So: always read without a timeout. Update:
		// we've eliminated the readTimeout parameter all together
		// to disallow it.
		req, err := w.readMessage(conn)
		if err == io.EOF {
			if !s.Server.cfg.QuietTestMode {
				alwaysPrintf("server sees io.EOF from receiveMessage")
			}
			// close of socket before read of full message.
			// shutdown this connection or we'll just
			// spin here at 500% cpu.
			return
		}
		if err != nil {
			//vv("srv read loop err = '%v'", err)
			r := err.Error()
			if strings.Contains(r, "remote error: tls: bad certificate") {
				//vv("ignoring client connection with bad TLS cert.")
				continue
			}
			if strings.Contains(r, "i/o timeout") || strings.Contains(r, "deadline exceeded") {
				//if strings.Contains(r, "deadline exceeded") {
				// just our readTimeout happening, so we can poll on shutting down, above.
				continue
			}
			if strings.Contains(r, "use of closed network connection") {
				return // shutting down
			}

			// "timeout: no recent network activity" should only
			// be seen on disconnection of a client because we
			// have a 10 second heartbeat going.
			if strings.Contains(r, "timeout: no recent network activity") {
				// We should never see this because of our app level keep-alives.
				// If we do, then it means the client really went down.
				//vv("quic server read loop exiting on '%v'", err)
				return
			}
			if strings.Contains(r, "Application error 0x0 (remote)") {
				// normal message from active client who knew they were
				// closing down and politely let us know too. Otherwise
				// we just have to time out.
				return
			}

			if !s.Server.cfg.QuietTestMode {
				alwaysPrintf("ugh. error from remote %v: %v", conn.RemoteAddr(), err)
			}
			return
		}
		if req == nil {
			// simnet shutdown can cause this
			continue
		}
		//vv("srv read loop sees req = '%v'", req.String()) // not seen 040

		if req.HDR.From != "" {
			s.Server.unNAT.Set(req.HDR.From, remoteAddr)
		}
		if req.HDR.To != "" {
			s.Server.unNAT.Set(req.HDR.To, localAddr)
		}

		switch req.HDR.Typ {

		case CallKeepAlive:
			//vv("srv read loop got an rpc25519 keep alive.")
			s.lastPingReceivedTmu.Store(time.Now().UnixNano())
			continue

		case CallPeerStart, CallPeerStartCircuit, CallPeerStartCircuitTakeToID:

			// Why this is not in its own goroutine? Backpressure.
			// It is important to provide back pressure
			// and let the sends catch up with the reads... we
			// just need to make sure that the sends don't also
			// depend on a read happening, since then we can deadlock.
			// To wit: peerbackPump in pump.go now detects a
			// busy send loop and queues closes to avoid such
			// deadlocks. See also the comments on
			// SendOneWayMessage in srv.go.

			err := s.Server.PeerAPI.bootstrapCircuit(notClient, req, ctx, s.SendCh)
			if err != nil {
				// only error is on shutdown request received.
				return
			}
			continue
		}
		if s.Server.notifies.handleReply_to_CallID_ToPeerID(notClient, ctx, req) {
			//vv("server side (%v) notifies says we are done after "+
			//	"req = '%v'", s.from, req.HDR.String())
			continue
		} else {
			//vv("server side (%v) notifies says we are NOT done after "+
			//	"req = '%v'", s.from, req.HDR.String())
		}

		if req.HDR.Typ == CallPeerTraffic ||
			req.HDR.Typ == CallPeerError {
			// on shutdown we can get here. Don't freak.
			return
		}

		// Idea: send the job to the central work queue, so
		// we service jobs fairly in FIFO order.
		// Update: turns out this didn't really matter.
		// So we got rid of the work queue.
		job := &job{req: req, conn: conn, pair: s, w: w}

		s.handleIncomingMessage(ctx, req, job)
	}
}

func (pair *rwPair) handleIncomingMessage(ctx context.Context, req *Message, job *job) {

	switch req.HDR.Typ {

	// We destroy the FIFO of a stream if we don't
	// queue up the stream messages here, exactly in
	// the order we received them.
	case CallUploadBegin, CallRequestBistreaming:
		// early but sequential setup, we'll revist
		// this again for CallUploadBegin to add ctx and
		// cancel func. The important thing is that
		// we queue up req in the stream channel now.
		pair.Server.registerInFlightCallToCancel(req, nil, nil)

		// fallthrough and processWork to handle the rest;
		// we just needed to do this beforehand to make sure
		// we got FIFO order of incoming Messages.
		// Once we start a goroutine for processWork,
		// FIFO order will be lost.

	case CallUploadMore, CallUploadEnd:
		pair.Server.handleUploadParts(req)
		return
	}

	// Workers requesting jobs can keep calls open for
	// minutes or hours or days; so we cannot just have
	// just use this readLoop goroutine to process
	// call sequentially; we cannot block here: this
	// has to be in a new goroutine.
	go pair.Server.processWork(job)
}

// Client and Server both use (their own copies) to keep code in sync.
// In fact they might even share one, if in the same process.
type notifies struct {
	// use mapIDtoChan instead of mutex in notifies, to avoid
	// deadlocks with the cli readLoop and the peer pump both accessing,
	// while the cli readLoop wants the pump to service the
	// channel send it got from the notifies map in the first place;
	// while the pump wants to unregister a shutdown peerID chan.

	// isCli must only be for debug logging, b/c might not be accurate!
	//isCli bool

	notifyOnReadCallIDMap  *mapIDtoChan
	notifyOnErrorCallIDMap *mapIDtoChan

	notifyOnReadToPeerIDMap  *mapIDtoChan
	notifyOnErrorToPeerIDMap *mapIDtoChan

	u UniversalCliSrv
}

// for notifies to avoid long holds of a mutex, we use this instead.
// See also mutmap.go, a later generic version.
type mapIDtoChan struct {
	mut sync.RWMutex
	m   map[string]chan *Message
}

func newMapIDtoChan() *mapIDtoChan {
	return &mapIDtoChan{
		m: make(map[string]chan *Message),
	}
}
func (m *mapIDtoChan) get(id string) (ch chan *Message, ok bool) {
	m.mut.RLock()
	ch, ok = m.m[id]
	m.mut.RUnlock()
	return
}
func (m *mapIDtoChan) set(id string, ch chan *Message) {
	m.mut.Lock()
	m.m[id] = ch
	m.mut.Unlock()
}
func (m *mapIDtoChan) del(id string) {
	m.mut.Lock()
	delete(m.m, id)
	m.mut.Unlock()
}
func (m *mapIDtoChan) clear() {
	m.mut.Lock()
	clear(m.m)
	m.mut.Unlock()
}
func (m *mapIDtoChan) keys() (ks []string) {
	m.mut.Lock()
	for k := range m.m {
		ks = append(ks, k)
	}
	m.mut.Unlock()
	return
}

func newNotifies(isCli bool, u UniversalCliSrv) *notifies {
	return &notifies{
		u:                      u,
		notifyOnReadCallIDMap:  newMapIDtoChan(),
		notifyOnErrorCallIDMap: newMapIDtoChan(),

		notifyOnReadToPeerIDMap:  newMapIDtoChan(),
		notifyOnErrorToPeerIDMap: newMapIDtoChan(),
		//isCli:                    isCli,
	}
}

// For Peer/Object systems, ToPeerID get priority over CallID
// to allow such systems to implement custom message
// types. An example is the Fragment/Peer/Circuit system.
func (c *notifies) handleReply_to_CallID_ToPeerID(isCli bool, ctx context.Context, msg *Message) (done bool) {

	switch msg.HDR.Typ {
	case CallError, CallPeerError:
		// not CallPeerFromIsShutdown per pump handling it on ReadsIn

		//alwaysPrintf("error type seen!: '%v'", msg.HDR.Typ.String())

		// give ToPeerID priority
		if msg.HDR.ToPeerID != "" {

			wantsErrObj, ok := c.notifyOnErrorToPeerIDMap.get(msg.HDR.ToPeerID)
			if ok {
				select {
				case wantsErrObj <- msg:
					//vv("notified a channel! %p for CallID '%v'", wantsErr, msg.HDR.ToPeerID)

				case <-ctx.Done():
					// think we want backpressure and to make sure the peer goro keep up.
				case <-c.u.GetHostHalter().ReqStop.Chan:
					// ctx not enough for clean shutdown, need this too.

				}
				return true // only send to ToPeerID, not CallID too.
			}
		}

		wantsErr, ok := c.notifyOnErrorCallIDMap.get(msg.HDR.CallID)
		if ok {
			select {
			case wantsErr <- msg:
			//vv("notified a channel! %p for CallID '%v'", wantsErr, msg.HDR.CallID)
			case <-ctx.Done():
			case <-c.u.GetHostHalter().ReqStop.Chan:
				// ctx not enough for clean shutdown, need this too.
			}
			return true
		}
	} // end CallError

	if msg.HDR.ToPeerID != "" {

		wantsToPeerID, ok := c.notifyOnReadToPeerIDMap.get(msg.HDR.ToPeerID)
		//vv("have ToPeerID msg = '%v'; ok='%v'; for '%v'", msg.HDR.String(), ok, msg.HDR.ToPeerID)
		if ok {
			// allow back pressure. Don't time out here.
			select {
			case wantsToPeerID <- msg:
				//vv("sent msg to wantsToPeerID chan! %p", wantsToPeerID)
			case <-ctx.Done():
				return
			case <-c.u.GetHostHalter().ReqStop.Chan: // ctx not enough
				return
			}
			return true // only send to ToPeerID, priority over CallID.
		}
	}

	wantsCallID, ok := c.notifyOnReadCallIDMap.get(msg.HDR.CallID)
	//vv("isCli=%v, c.notifyOnReadCallIDMap[callID='%v'] -> %p, ok=%v", c.isCli, msg.HDR.CallID, wantsCallID, ok)
	if ok {
		select {
		case wantsCallID <- msg:
			//vv("isCli = %v; notifies.handleReply notified registered channel for callID = '%v'", isCli, msg.HDR.CallID)
		case <-ctx.Done():
		case <-c.u.GetHostHalter().ReqStop.Chan: // ctx not enough
		}
		return true
	}
	return false
}

type job struct {
	req  *Message
	conn net.Conn
	pair *rwPair
	w    *blabber
}

// PRE: only req.HDR.Typ in {CallUploadMore, CallUploadEnd}
// should be calling here.
func (s *Server) handleUploadParts(req *Message) {

	callID := req.HDR.CallID

	s.inflight.mut.Lock()
	cc, ok := s.inflight.activeCalls[callID]
	var part int64
	_ = part
	if ok {
		// track progress
		part = req.HDR.StreamPart
		cc.lastStreamPart = part
	}
	s.inflight.mut.Unlock()

	if !ok {
		alwaysPrintf("Warning: dropping a StreamPart: '%s' because no handler "+
			"registered/inflight. This is highly un-expected. "+
			"However, it might mean the server func exited with "+
			"an error but the client has not caught up with "+
			"that yet. hdr='%v'", req.HDR.Typ, req.HDR.String())
		return
	}

	// be aware that we are on the read-loop goroutine stack here.
	// If we are stalled, we will stall all reads on the server.
	// If we hang, the server becomes unresponsive.
	//
	// BUT! This is probably good, because we *want* back-pressure
	// when under load. If the streamer cannot take any more
	// messages, then we don't want to read any more off the wire either.
	// Just wait until a slot opens up.

	select {
	case cc.streamCh <- req:
		//vv("handleUploadParts: cc.StreamCh: sent req with part %v", part)
	case <-s.halt.ReqStop.Chan:
		return

		// don't time out here! allow for back-pressure on
		// the network read, which can be transmitted across
		// TCP to slow the sending side down.
	}
}

func (s *Server) processWork(job *job) {

	var callme1 OneWayFunc
	var callme2 TwoWayFunc
	var callmeServerSendsDownloadFunc ServerSendsDownloadFunc
	var callmeUploadReaderFunc UploadReaderFunc

	var callmeBi BistreamFunc

	foundCallback1 := false
	foundCallback2 := false
	foundServerSendsDownload := false
	foundBistream := false
	foundUploader := false

	req := job.req
	//vv("processWork got job: req.HDR='%v'", req.HDR.String()) // not see 040 hang

	if req.HDR.Typ == CallCancelPrevious {
		s.cancelCallID(req.HDR.CallID)
		return
	}

	//vv("processWork() sees req.HDR = %v", req.HDR.String())

	conn := job.conn
	pair := job.pair
	w := job.w

	req.HDR.Nc = conn

	if req.HDR.Typ == CallNetRPC {
		//vv("have IsNetRPC call: '%v'", req.HDR.ServiceName)
		err := pair.callBridgeNetRpc(req, job)
		if err != nil {
			alwaysPrintf("callBridgeNetRpc errored out: '%v'", err)
		}
		return // continue
	}

	switch req.HDR.Typ {
	case CallRPC:
		back, ok := s.callme2map.Get(req.HDR.ServiceName)
		if ok {
			callme2 = back
			foundCallback2 = true
		} else {
			s.respondToReqWithError(req, job, fmt.Sprintf("error! CallRPC begin received but no server side upcall registered for req.HDR.ServiceName='%v'; req.HDR.CallID='%v'", req.HDR.ServiceName, req.HDR.CallID))
			return
		}
	case CallRequestBistreaming:
		back, ok := s.callmeBistreamMap.Get(req.HDR.ServiceName)
		if ok {
			callmeBi = back
			foundBistream = true
			//vv("foundBistream true!")
		} else {
			s.respondToReqWithError(req, job, fmt.Sprintf("error! CallRequestBistreaming received but no server side upcall registered for req.HDR.ServiceName='%v'; req.HDR.CallID='%v'", req.HDR.ServiceName, req.HDR.CallID))
			return
		}
	case CallRequestDownload:
		back, ok := s.callmeServerSendsDownloadMap.Get(req.HDR.ServiceName)
		if ok {
			callmeServerSendsDownloadFunc = back
			foundServerSendsDownload = true
		} else {
			s.respondToReqWithError(req, job, fmt.Sprintf("error! CallRequestDownload received but no server side upcall registered for req.HDR.ServiceName='%v'; req.HDR.CallID='%v'", req.HDR.ServiceName, req.HDR.CallID))
			return
		}
	case CallUploadBegin:
		uploader, ok := s.callmeUploadReaderMap.Get(req.HDR.ServiceName)
		if !ok {
			// nothing to do
			// send back a CallError
			s.respondToReqWithError(req, job, fmt.Sprintf("error! CallUploadBegin stream begin received but no registered stream upload reader available on the server. req.HDR.ServiceName='%v'; req.HDR.CallID='%v'", req.HDR.ServiceName, req.HDR.CallID))
			return
		}
		foundUploader = true
		callmeUploadReaderFunc = uploader

	case CallUploadMore, CallUploadEnd:
		panic("cannot see these here, must for FIFO of the stream be called from the readloop")

	case CallOneWay:
		back, ok := s.callme1map.Get(req.HDR.ServiceName)
		if ok {
			callme1 = back
			foundCallback1 = true
		} else {
			s.respondToReqWithError(req, job, fmt.Sprintf("error! CallOneWay received but no server side callme1map upcall registered for ServiceName='%v'; from req.HDR='%v'", req.HDR.ServiceName, req.HDR.String()))
			return
		}
	}

	if !foundCallback1 &&
		!foundCallback2 &&
		!foundServerSendsDownload &&
		!foundBistream &&
		!foundUploader {
		//vv("warning! no callbacks found for req = '%v'", req)
		return
	}

	if foundCallback1 {
		callme1(req)
		return
	}

	// handle:
	//
	// foundCallback2
	// foundServerSendsDownload
	// foundBistream
	// foundUploader

	//vv("foundCallback2 true, req.HDR = '%v'", req.HDR)

	//vv("req.Nc local = '%v', remote = '%v'", local(req.Nc), remote(req.Nc))
	//vv("stream local = '%v', remote = '%v'", local(stream), remote(stream))
	//vv("conn   local = '%v', remote = '%v'", local(conn), remote(conn))

	reply := s.GetEmptyMessage()

	// enforce these are the same.
	replySeqno := req.HDR.Seqno
	reqCallID := req.HDR.CallID
	serviceName := req.HDR.ServiceName

	// allow user to change Subject
	reply.HDR.Subject = req.HDR.Subject

	// allow cancellation of inflight calls.
	ctx0 := context.Background()
	var cancelFunc context.CancelFunc
	var deadline time.Time
	if !req.HDR.Deadline.IsZero() {
		//vv("server side sees deadline set on request HDR: '%v'", req.HDR.Deadline)
		deadline = req.HDR.Deadline
		ctx0, cancelFunc = context.WithDeadline(ctx0, deadline)
	} else {
		//vv("server side sees NO deadline")
		ctx0, cancelFunc = context.WithCancel(ctx0)
	}
	defer cancelFunc()
	ctx := ContextWithHDR(ctx0, &req.HDR)
	s.registerInFlightCallToCancel(req, cancelFunc, ctx)

	defer s.noLongerInFlight(reqCallID)
	req.HDR.Ctx = ctx

	var err error
	switch {
	case foundCallback2:
		err = callme2(req, reply)
		//err = callme2(ctx, req, reply) // add ctx?
	case foundServerSendsDownload:
		help := s.newServerSendDownloadHelper(ctx, job)
		err = callmeServerSendsDownloadFunc(s, ctx, req, help.sendDownloadPart, reply)
	case foundBistream:
		help := s.newServerSendDownloadHelper(ctx, job)
		err = callmeBi(s, ctx, req, req.HDR.streamCh, help.sendDownloadPart, reply)
	case foundUploader:
		err = pair.beginReadStream(ctx, callmeUploadReaderFunc, req, reply)
	}
	if err != nil {
		reply.JobErrs = err.Error()
	}
	// don't read from req now, just in case callme2 messed with it.

	reply.HDR.Created = time.Now()
	reply.HDR.Serial = issueSerial()
	reply.HDR.From = pair.from
	reply.HDR.To = pair.to

	// We are able to match call and response rigorously on the CallID, or Seqno, alone.
	reply.HDR.CallID = reqCallID
	reply.HDR.Seqno = replySeqno
	reply.HDR.Typ = CallRPCReply
	reply.HDR.Deadline = deadline
	reply.HDR.ServiceName = serviceName

	//vv("2way about to send its reply: '%#v'", reply)

	// We write ourselves rather than switch
	// goroutines. We've added a mutex
	// inside sendMessage so our writes won't conflict
	// with keep-alive pings.
	//err = w.sendMessage(conn, reply, &s.cfg.WriteTimeout)
	err = w.sendMessage(conn, reply, nil)
	if err != nil {
		alwaysPrintf("sendMessage got err = '%v'; on trying to send Seqno=%v", err, reply.HDR.Seqno)

		// should we be tearing down the pair?
	}

	// NB: server side reply.DoneCh is not used.
	s.FreeMessage(reply)
}

// Servers read and respond to requests. Two APIs are available.
//
// Using the rpc25519.Message based API:
//
//	Register1Func() and Register2Func() register callbacks.
//
// Using the net/rpc API:
//
//	Server.Register() registers structs with callback methods on them.
//
// The net/rpc API is implemented as a layer on top of the rpc25519.Message
// based API. Both can be used concurrently if desired.
type Server struct {
	mut sync.Mutex

	//StartSimNet   chan *SimNetConfig
	simnet     *Simnet
	simnode    *simnode
	simNetAddr *SimNetAddr
	srvStarted atomic.Bool

	boundAddressString string
	netAddr            net.Addr

	cfg        *Config
	quicConfig *quic.Config
	tmStart    time.Time

	lastPairID atomic.Int64

	jobcount int64

	name  string // which server, for debugging.
	creds *selfcert.Creds

	callme2map *Mutexmap[string, TwoWayFunc]
	callme1map *Mutexmap[string, OneWayFunc]

	// client -> server streams
	callmeUploadReaderMap *Mutexmap[string, UploadReaderFunc]

	// server -> client streams
	callmeServerSendsDownloadMap *Mutexmap[string, ServerSendsDownloadFunc]
	callmeBistreamMap            *Mutexmap[string, BistreamFunc]

	notifies *notifies
	PeerAPI  *peerAPI // must be Exported to users!

	fragLock sync.Mutex

	lsn  io.Closer // net.Listener
	halt *idem.Halter

	remote2pair *Mutexmap[string, *rwPair]
	pair2remote *Mutexmap[*rwPair, string]

	// also need to be able to un-nat things!
	// to translate from the msg.HDR.From to what
	// the net.Conn actually thinks--when not on
	// a hole-punching network like Tailscale.
	unNAT *Mutexmap[string, string]

	// RemoteConnectedCh sends the remote host:port address
	// when the server gets a new client,
	// See srv_test.go Test004_server_push for example,
	// where it is used to avoid a race/panic.
	RemoteConnectedCh chan *ServerClient

	// net/rpc implementation details
	serviceMap sync.Map   // map[string]*service
	reqLock    sync.Mutex // protects freeReq
	freeReq    *Request
	respLock   sync.Mutex // protects freeResp
	freeResp   *Response

	msgLock sync.Mutex // protects freeMsg
	freeMsg *Message

	// try to avoid expensive sync.Map.Load call
	// for the common case of just one service.
	svcCount atomic.Int64
	svc0     *service
	svc0name string

	// We put cancellation support on the
	// server rather than the rwPair in case
	// the pair gets torn down during a
	// reconnect. We still want to support
	// cancellation after a reconnect.
	inflight inflight

	// protect with mut to avoid data races.
	autoClients []*Client
}

type ServerClient struct {
	Remote string
	GoneCh chan struct{}
}

func newServerClient(remote string) *ServerClient {
	return &ServerClient{
		Remote: remote,
		GoneCh: make(chan struct{}),
	}
}

// layer2 will provide a 2nd, symmetric encryption
// layer for post-quantum resistance like Wireguard.
type layer2 struct {
	preSharedKey  []byte // must be 32 bytes, else we will panic.
	sessionSecret bytes.Buffer
	// symKey will be our 2nd layer of symmetric encryption key.
	// It is the SHA-256 HMAC of sessionSecret using key preSharedKey.
	symkey []byte
}

func (l *layer2) setup() {
	if len(l.preSharedKey) != 32 {
		panic("preSharedKey must be 32 bytes long")
	}
	sec := l.sessionSecret.Bytes()
	if len(sec) < 100 {
		panic("session key too short")
	}
	l.symkey = computeHMAC(sec, l.preSharedKey)

	// clear out the sessionSecret
	for i := range sec {
		sec[i] = 0
	}
	l.sessionSecret = bytes.Buffer{}
}

func (server *Server) getRequest() *Request {
	server.reqLock.Lock()
	req := server.freeReq
	if req == nil {
		req = new(Request)
	} else {
		server.freeReq = req.next
		*req = Request{}
	}
	server.reqLock.Unlock()
	return req
}

func (server *Server) freeRequest(req *Request) {
	server.reqLock.Lock()
	req.next = server.freeReq
	server.freeReq = req
	server.reqLock.Unlock()
}

func (server *Server) getResponse() *Response {
	server.respLock.Lock()
	resp := server.freeResp
	if resp == nil {
		resp = new(Response)
	} else {
		server.freeResp = resp.next
		*resp = Response{}
	}
	server.respLock.Unlock()
	return resp
}

func (server *Server) freeResponse(resp *Response) {
	server.respLock.Lock()
	resp.next = server.freeResp
	server.freeResp = resp
	server.respLock.Unlock()
}

func (s *Server) GetEmptyMessage() *Message {
	s.msgLock.Lock()
	msg := s.freeMsg
	if msg == nil {
		msg = new(Message)
	} else {
		s.freeMsg = msg.nextOrReply
		*msg = Message{}
	}
	s.msgLock.Unlock()
	return msg
}

func (s *Server) FreeMessage(msg *Message) {
	s.msgLock.Lock()
	msg.nextOrReply = s.freeMsg
	s.freeMsg = msg
	s.msgLock.Unlock()
}

// like net_server.go NetServer.ServeCodec
func (p *rwPair) callBridgeNetRpc(reqMsg *Message, job *job) error {
	//vv("bridge called! subject: '%v'", reqMsg.HDR.ServiceName)

	p.encBuf.Reset()
	p.encBufWriter.Reset(&p.encBuf)
	p.greenCodec.enc.Reset(p.encBufWriter)

	p.decBuf.Reset()
	p.decBuf.Write(reqMsg.JobSerz)
	p.greenCodec.dec.Reset(&p.decBuf)

	service, mtype, req, argv, replyv, keepReading, wantsCtx, err := p.readRequest(p.greenCodec)
	//vv("p.readRequest() back with err = '%v'; req='%#v'", err, req)
	if err != nil {
		if debugLog && err != io.EOF {
			log.Println("rpc:", err)
		}
		if !keepReading {
			return err
		}
		// send a response if we actually managed to read a header.
		if req != nil {
			p.sendResponse(reqMsg, req, invalidRequest, p.greenCodec, err.Error(), job)
			p.Server.freeRequest(req)
		}
		return err
	}
	//vv("about to callMethodByReflection")
	service.callMethodByReflection(p, reqMsg, mtype, req, argv, replyv, p.greenCodec, wantsCtx, job)

	return nil
}

func (s *service) callMethodByReflection(pair *rwPair, reqMsg *Message, mtype *methodType, req *Request, argv, replyv reflect.Value, codec ServerCodec, wantsCtx bool, job *job) {

	mtype.Lock()
	mtype.numCalls++
	mtype.Unlock()
	function := mtype.method.Func

	// Invoke the method, providing a new value for the reply.
	var returnValues []reflect.Value
	if wantsCtx {
		//vv("wantsCtx so setting up to cancel...")
		ctx0 := context.Background()
		var cancelFunc context.CancelFunc
		if !reqMsg.HDR.Deadline.IsZero() {
			ctx0, cancelFunc = context.WithDeadline(ctx0, reqMsg.HDR.Deadline)
		} else {
			ctx0, cancelFunc = context.WithCancel(ctx0)
		}
		defer cancelFunc()
		ctx := ContextWithHDR(ctx0, &reqMsg.HDR)

		pair.Server.registerInFlightCallToCancel(reqMsg, cancelFunc, ctx)
		defer pair.Server.noLongerInFlight(reqMsg.HDR.CallID)

		reqMsg.HDR.Ctx = ctx

		rctx := reflect.ValueOf(ctx)
		returnValues = function.Call([]reflect.Value{s.rcvr, rctx, argv, replyv})

	} else {
		returnValues = function.Call([]reflect.Value{s.rcvr, argv, replyv})
	}
	// The return value for the method is an error.
	errInter := returnValues[0].Interface()
	errmsg := ""
	if errInter != nil {
		errmsg = errInter.(error).Error()
	}

	greenReplyv, ok := replyv.Interface().(Green)
	if !ok {
		panic(fmt.Sprintf("reply must be Green. type '%T' was not.", replyv.Interface()))
	}
	pair.sendResponse(reqMsg, req, greenReplyv, codec, errmsg, job)
	pair.Server.freeRequest(req)
}

// called by callMethodByReflection
func (p *rwPair) sendResponse(reqMsg *Message, req *Request, reply Green, codec ServerCodec, errmsg string, job *job) {

	//vv("pair sendResponse() top, reply: '%#v'", reply)

	resp := p.Server.getResponse()
	// Encode the response header
	resp.ServiceMethod = req.ServiceMethod
	if errmsg != "" {
		resp.Error = errmsg
		reply = invalidRequest
	}
	resp.Seq = req.Seq
	//vv("srv sendResonse() for req.Seq = %v; resp='%#v'", req.Seq, resp)
	p.sending.Lock()
	err := codec.WriteResponse(resp, reply)
	if err != nil {
		alwaysPrintf("error writing resp was '%v'; resp='%#v'", err, resp)
	}
	if debugLog && err != nil {
		log.Println("rpc: writing response:", err)
	}
	p.sending.Unlock()
	p.Server.freeResponse(resp)

	msg := p.Server.GetEmptyMessage()
	msg.HDR.Created = time.Now()
	msg.HDR.Serial = issueSerial()
	msg.HDR.From = job.pair.from
	msg.HDR.To = job.pair.to
	msg.HDR.ServiceName = reqMsg.HDR.ServiceName // echo back
	msg.HDR.Subject = reqMsg.HDR.Subject         // echo back
	msg.HDR.Typ = CallRPCReply
	msg.HDR.Seqno = reqMsg.HDR.Seqno       // echo back
	msg.HDR.CallID = reqMsg.HDR.CallID     // echo back
	msg.HDR.Deadline = reqMsg.HDR.Deadline // echo back
	// We are able to match call and response rigourously on the CallID alone.

	by := p.encBuf.Bytes()
	msg.JobSerz = make([]byte, len(by))
	copy(msg.JobSerz, by)
	//vv("response JobSerz is len %v; blake3= '%v'", len(by), blake3OfBytesString(by))
	//err = job.w.sendMessage(p.Conn, msg, &p.cfg.WriteTimeout)
	err = job.w.sendMessage(p.Conn, msg, nil)
	if err != nil {
		alwaysPrintf("sendMessage got err = '%v'; on trying to send Seqno=%v", err, msg.HDR.Seqno)
		// just let user try again?
	} else {
		//vv("srv sendMessage went ok")
	}
	p.Server.FreeMessage(msg)
}

// from net/rpc Server.readRequest
func (p *rwPair) readRequest(codec ServerCodec) (service *service, mtype *methodType, req *Request, argv, replyv reflect.Value, keepReading bool, wantsCtx bool, err error) {
	//vv("pair readRequest() top")

	service, mtype, req, keepReading, wantsCtx, err = p.readRequestHeader(codec)
	// err can legit be: rpc: can't find method Arith.BadOperation
	// if a method is not found, so do not panic on err here.
	if err != nil {
		if !keepReading {
			return
		}
		// discard body
		codec.ReadRequestBody(nil)
		//vv("srv readRequest got err='%v' back: req='%#v'", err, req)
		return
	}
	//vv("srv readRequest got back: req='%#v'", req)

	// Decode the argument value.
	argIsValue := false // if true, need to indirect before calling.
	if mtype.ArgType.Kind() == reflect.Pointer {
		argv = reflect.New(mtype.ArgType.Elem())
	} else {
		argv = reflect.New(mtype.ArgType)
		argIsValue = true
	}
	// argv guaranteed to be a pointer now.

	//vv("argv is '%#v'", argv)
	greenArgv, ok := argv.Interface().(Green)
	if !ok {
		panic(fmt.Sprintf("argv must be Green. type '%T' was not.", argv.Interface()))
	}

	if err = codec.ReadRequestBody(greenArgv); err != nil {
		return
	}
	if argIsValue {
		argv = argv.Elem()
	}

	replyv = reflect.New(mtype.ReplyType.Elem())

	switch mtype.ReplyType.Elem().Kind() {
	case reflect.Map:
		replyv.Elem().Set(reflect.MakeMap(mtype.ReplyType.Elem()))
	case reflect.Slice:
		replyv.Elem().Set(reflect.MakeSlice(mtype.ReplyType.Elem(), 0, 0))
	}
	return
}

func (p *rwPair) readRequestHeader(codec ServerCodec) (svc *service, mtype *methodType, req *Request, keepReading bool, wantsCtx bool, err error) {
	// Grab the request header.
	req = p.Server.getRequest()
	err = codec.ReadRequestHeader(req)
	if err != nil {
		req = nil
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return
		}
		err = errors.New("rpc: server cannot decode request: " + err.Error())
		return
	}

	// We read the header successfully. If we see an error now,
	// we can still recover and move on to the next request.
	keepReading = true

	dot := strings.LastIndex(req.ServiceMethod, ".")
	if dot < 0 {
		err = errors.New("rpc: service/method request ill-formed: " + req.ServiceMethod)
		return
	}
	serviceName := req.ServiceMethod[:dot]
	methodName := req.ServiceMethod[dot+1:]

	// Look up the request.
	var svci any
	var ok bool

	// sync.Map serviceMap.Load() is costly, try to
	// optimize it away when we only have one service.
	if p.Server.svcCount.Load() == 1 {
		if serviceName != p.Server.svc0name {
			err = errors.New("rpc: can't find service " + req.ServiceMethod)
			return
		} else {
			svc = p.Server.svc0
		}
	} else {
		svci, ok = p.Server.serviceMap.Load(serviceName)
		if !ok {
			err = errors.New("rpc: can't find service " + req.ServiceMethod)
			return
		}
		svc = svci.(*service)
	}
	mtype = svc.method[methodName]
	if mtype == nil {
		mtype = svc.ctxMethod[methodName]
		if mtype == nil {
			err = errors.New("rpc: can't find method " + req.ServiceMethod)
		} else {
			wantsCtx = true
		}
	}
	return
}

// Register implements the net/rpc Server.Register() API. Its docs:
//
// Register publishes in the server the set of methods of the
// receiver value that satisfy the following conditions:
//   - exported method of exported type
//   - two arguments, both of exported type
//   - the second argument is a pointer
//   - one return value, of type error
//
// It returns an error if the receiver is not an exported type or has
// no suitable methods. It also logs the error using package log.
// The client accesses each method using a string of the form "Type.Method",
// where Type is the receiver's concrete type.
//
// rpc25519 addendum:
//
// Callback methods in the `net/rpc` style traditionally look like this first
// `NoContext` example below. We now allow a context.Context as an additional first
// parameter. The ctx will a pointer to the `rpc25519.HDR` header from the
// incoming Message set on it. Call rpc25519.HDRFromContext()
// to retreive it.
//
//	func (s *Service) NoContext(args *Args, reply *Reply) error
//
// * new:
//
//	func (s *Service) GetsContext(ctx context.Context, args *Args, reply *Reply) error {
//	  if hdr, ok := rpc25519.HDRFromContext(ctx); ok {
//	        fmt.Printf("GetsContext called with HDR = '%v'; "+
//	             "HDR.Nc.RemoteAddr() gives '%v'; HDR.Nc.LocalAddr() gives '%v'\n",
//	             h.String(), h.Nc.RemoteAddr(), h.Nc.LocalAddr())
//	   } else {
//	        fmt.Println("HDR not found")
//	   }
//	}
func (s *Server) Register(rcvr msgp.Encodable) error {
	return s.register(rcvr, "", false)
}

// RegisterName is like [Register] but uses the provided name for the type
// instead of the receiver's concrete type.
func (s *Server) RegisterName(name string, rcvr msgp.Encodable) error {
	return s.register(rcvr, name, true)
}
func (s *Server) register(rcvr msgp.Encodable, name string, useName bool) error {

	svc := new(service)
	svc.typ = reflect.TypeOf(rcvr)
	svc.rcvr = reflect.ValueOf(rcvr)
	sname := name
	if !useName {
		sname = reflect.Indirect(svc.rcvr).Type().Name()
	}
	if sname == "" {
		s := "rpc.Register: no service name for type " + svc.typ.String()
		log.Print(s)
		return errors.New(s)
	}
	if !useName && !token.IsExported(sname) {
		s := "rpc.Register: type " + sname + " is not exported"
		log.Print(s)
		return errors.New(s)
	}
	svc.name = sname

	// Install the methods
	svc.method = suitableMethods(svc.typ, logRegisterError)

	svc.ctxMethod = contextFirstSuitableMethods(svc.typ, logRegisterError)

	if len(svc.method) == 0 && len(svc.ctxMethod) == 0 {
		str := ""

		// To help the user, see if a pointer receiver would work.
		method := suitableMethods(reflect.PointerTo(svc.typ), false)
		ctxMethod := contextFirstSuitableMethods(reflect.PointerTo(svc.typ), false)

		if len(method) != 0 || len(ctxMethod) != 0 {
			str = "rpc.Register: type " + sname + " has no exported methods of suitable type (hint: pass a pointer to value of that type)"
		} else {
			str = "rpc.Register: type " + sname + " has no exported methods of suitable type"
		}
		log.Print(str)
		return errors.New(str)
	}

	if _, dup := s.serviceMap.LoadOrStore(sname, svc); dup {
		return errors.New("rpc: service already defined: " + sname)
	}

	count := s.svcCount.Add(1)
	if count == 1 {
		s.svc0 = svc
		s.svc0name = sname
	}

	return nil
}

// keep the pair of goroutines running
// the read loop and the write loop
// for a given connection together so
// we can figure out who to SendCh to
// and how to halt each other.
type rwPair struct {
	pairID int64 // for metrics, looking at fairness/starvation

	// our parent Server
	Server *Server

	// copy of Server.cfg for convenience
	cfg *Config

	Conn   net.Conn
	SendCh chan *Message

	halt *idem.Halter

	allDone chan struct{}

	// net/rpc api
	greenCodec   *greenpackServerCodec
	sending      sync.Mutex
	encBuf       bytes.Buffer // target for codec writes: encode into here first
	encBufWriter *bufio.Writer
	decBuf       bytes.Buffer // target for code reads.

	// creds:

	// mut protects the following
	mut sync.Mutex

	// the ephemeral keys from the ephemeral ECDH handshake
	// to estblish randomSymmetricSessKeyFromPreSharedKey
	randomSymmetricSessKeyFromPreSharedKey [32]byte
	cliEphemPub                            []byte
	srvEphemPub                            []byte
	// only one of these two will be filled here, depending on if we are client or server.
	srvStaticPub ed25519.PublicKey
	cliStaticPub ed25519.PublicKey

	// cache instead of compute on every call
	from string
	to   string

	// better here than on cfg, more likely to work.
	lastReadMagic7 atomic.Int64

	// grid based server's auto-client?
	isAutoCli bool

	lastPingReceivedTmu atomic.Int64
	lastPingSentTmu     atomic.Int64

	// rwPair gets its own keep alive message
	// to avoid data races with any client(s) also here,
	// and will transmit epochV.EpochID in any
	// keep-alive ping.
	epochV       EpochVers
	keepAliveMsg Message
}

func (s *Server) newRWPair(conn net.Conn) *rwPair {

	p := &rwPair{
		pairID: s.lastPairID.Add(1),
		cfg:    s.cfg.Clone(),
		Server: s,
		Conn:   conn,
		SendCh: make(chan *Message),
		from:   local(conn),
		to:     remote(conn),
		epochV: EpochVers{EpochTieBreaker: NewCallID("")},
	}
	p.halt = idem.NewHalterNamed(fmt.Sprintf("Server.rwPair(%p)", p))
	p.keepAliveMsg.HDR.Typ = CallKeepAlive
	p.keepAliveMsg.HDR.Subject = p.epochV.EpochTieBreaker

	p.encBufWriter = bufio.NewWriter(&p.encBuf)
	p.greenCodec = &greenpackServerCodec{
		pair:         p,
		rwc:          nil,
		dec:          msgp.NewReader(&p.decBuf),
		enc:          msgp.NewWriter(p.encBufWriter),
		encBufWriter: p.encBufWriter,
		encBufItself: &p.encBuf,
	}

	key := remote(conn)

	s.mut.Lock() // want these two set together atomically.
	s.remote2pair.Set(key, p)
	s.pair2remote.Set(p, key)
	s.mut.Unlock()

	sc := newServerClient(key)
	p.allDone = sc.GoneCh
	select {
	case s.RemoteConnectedCh <- sc:
	default:
	}
	return p
}

func (s *Server) deletePair(p *rwPair) {
	s.mut.Lock()
	defer s.mut.Unlock()

	key, ok := s.pair2remote.Get(p)
	if !ok {
		return
	}
	s.pair2remote.Del(p)
	s.remote2pair.Del(key)

	// see srv_test 015 for example use.
	close(p.allDone)
	//vv("Server.deletePair() has closed allDone for pair '%v'", p)
}

var ErrNetConnectionNotFound = fmt.Errorf("error in SendMessage: net.Conn not found")
var ErrWrongCallTypeForSendMessage = fmt.Errorf("error in SendMessage: msg.HDR.Typ must be CallOneWay or CallUploadBegin; or greater in number")

// SendMessage can be used on the server to push data to
// one of the connected clients.
//
// The Message msg should have msg.JobSerz set, as well as
// the HDR fields Subject, CallID, and Seqno.
// The NewCallID() can be used to generate a random (without
// conflicts with prior CallID if needed/not matching
// a previous call.
//
// If the HDR.To destination address is not already connected to the server, the
// ErrNetConnectionNotFound error will be returned.
//
// errWriteDur is how long we pause waiting for the
// writing goroutine to send the message or give us a fast
// error reply. Early discovery of client disconnect
// can allow us to try other (worker) clients, rather
// than wait for pings or other slow error paths.
//
// The errWriteDur can be set to a few seconds if this would
// save the caller a minute of two of waiting to discover
// the send is unlikely to suceed; or to time.Duration(0) if
// they want no pause after writing Message to the connection.
// The default if errWriteDur < 0 is 30 msec. It is a guess and aims at balance:
// allowing enough time to get an error back from quic-go
// if we are going to discover "Application error 0x0 (remote)"
// right away, and not wanting to stall the caller too much.
//
// goq uses this srv.SendMessage(), cli.OneWaySend(),
// cli.SendAndGetReply(), cli.GetReadIncomingCh(),
// cli.LocalAddr(), and cli.Close(). It calls here
// with errWriteDur = 5 seconds.
func (s *Server) SendMessage(callID, subject, destAddr string, data []byte, seqno uint64,
	errWriteDur time.Duration) error {

	pair, ok := s.remote2pair.Get(destAddr)

	if !ok {
		alwaysPrintf("could not find destAddr='%v' in our map: '%#v'", destAddr, s.remote2pair)
		return ErrNetConnectionNotFound
	}
	//vv("found remote2pair for destAddr = '%v': '%#v'", destAddr, pair)

	msg := NewMessage()
	msg.JobSerz = data

	from := local(pair.Conn)
	to := remote(pair.Conn)
	subject = fmt.Sprintf("srv.SendMessage('%v')", subject)

	mid := NewHDR(from, to, subject, CallOneWay, 0)
	if callID != "" {
		mid.CallID = callID
	}
	mid.Seqno = seqno
	msg.HDR = *mid

	//vv("send message attempting to send %v bytes to '%v'", len(data), destAddr)
	select {
	case pair.SendCh <- msg:
		//vv("sent to pair.SendCh, msg='%v'", msg.HDR.String())

		//    case <-s.TimeAfter(time.Second):
		//vv("warning: time out trying to send on pair.SendCh")
	case <-s.halt.ReqStop.Chan:
		// shutting down
		return ErrShutdown()
	}

	var dur time.Duration

	if errWriteDur < 0 {
		dur = 30 * time.Millisecond
	} else if errWriteDur > 0 {
		dur = errWriteDur
	}

	if dur > 0 {
		//vv("srv SendMessage about to wait %v to check on connection.", dur)

		sendTimeout := s.NewTimer(dur)
		if sendTimeout == nil {
			// shutdown in progress
			return ErrShutdown()
		}
		defer sendTimeout.Discard()
		sendTimeoutCh := sendTimeout.C

		select {
		case <-msg.DoneCh.WhenClosed():
			//vv("srv SendMessage got back msg.LocalErr = '%v'", msg.LocalErr)
			return msg.LocalErr
		case <-sendTimeoutCh:
			//vv("srv SendMessage timeout after waiting %v", dur)
			return ErrSendTimeout
		case <-s.halt.ReqStop.Chan:
			// shutting down
			return ErrShutdown()
		}
	}
	return nil
}

// destAddrToSendCh needs to unNAT to make destAddr
// when behind Network-Address-Translation boxes.
// Callers should re-write the msg.HDR.To if the
// returned to is different from destAddr, to avoid
// having to have the sendLoop do it again.
func (s *Server) destAddrToSendCh(destAddr string) (sendCh chan *Message, haltCh chan struct{}, to, from string, ok bool) {

	pair, ok := s.remote2pair.Get(destAddr)
	if !ok {
		real, ok2 := s.unNAT.Get(destAddr)
		if ok2 && real != destAddr {
			//vv("unNAT replacing destAddr '%v' -> '%v'", destAddr, real)
			destAddr = real
			// and try again
			pair, ok = s.remote2pair.Get(destAddr)
		} else {
			//vv("tried to unNAT but !(ok2 == %v) || (real(%v) == destAddr(%v)",
			//   ok2, real, destAddr)
		}
	} else {
		//vv("no unNAT needed, found destAddr = '%v' in remote2pair", destAddr)
	}

	if !ok {
		if !s.cfg.ServerAutoCreateClientsToDialOtherServers {
			//alwaysPrintf("yikes! Server did not find destAddr '%v' in remote2pair: '%v'", destAddr, remote2pair)
		}
		alwaysPrintf("yikes! Server did not find destAddr '%v' in remote2pair: '%v'", destAddr, s.remote2pair.GetKeySlice())

		return nil, nil, "", "", false
	}
	vv("ok true in Server.destAddrToSendCh(destAddr='%v')", destAddr)
	// INVAR: ok is true
	haltCh = s.halt.ReqStop.Chan
	from = local(pair.Conn)
	to = remote(pair.Conn)
	sendCh = pair.SendCh
	return
}

type oneWaySender interface {
	destAddrToSendCh(destAddr string) (sendCh chan *Message, haltCh chan struct{}, to, from string, ok bool)
	NewTimer(dur time.Duration) (ti *SimTimer)
}

// allow simnet to properly classify LoneCli vs autocli
// associted with server peers.
const auto_cli_recognition_prefix = "auto-cli-from-"

// SendOneWayMessage is the same as SendMessage above except that it
// takes a fully prepared msg to avoid API churn when new HDR fields are
// added/needed. msg.HDR.Type must be >= CallOneWay (100), to try and
// catch mis-use of this when the user actually wants a round-trip call.
//
// SendOneWayMessage only sets msg.HDR.From to its correct value.
//
// As a facility to prevent deadlock when both cli and srv are
// trying to send and both have full OS read buffers, the
// pump.go peerbackPump loop will call here with errWriteDur == -2.
// In this case, if the send cannot pass msg to the send loop
// within 1 millisecond, we return ErrAntiDeadlockMustQueue
// along with the sendCh to send on in the background when
// the send loop becomes available. See pump.go and how
// it calls closeCktInBackgroundToAvoidDeadlock() to avoid
// deadlocks on circuit shutdown.
func (s *Server) SendOneWayMessage(ctx context.Context, msg *Message, errWriteDur time.Duration) (err error, ch chan *Message) {

	err, ch = sendOneWayMessage(s, ctx, msg, errWriteDur)
	if err == ErrNetConnectionNotFound {
		if !s.cfg.ServerAutoCreateClientsToDialOtherServers {
			return
		}
		if !s.cfg.QuietTestMode {
			s.mut.Lock()
			lenAutoCli := len(s.autoClients) + 1
			s.mut.Unlock()
			alwaysPrintf("%v server did not find destAddr (msg.HDR.To='%v') in "+
				"remote2pair, but cfg.ServerAutoCreateClientsToDialOtherServers"+
				" is true so spinning up new client... for a total of %v autoClients", s.name, msg.HDR.To, lenAutoCli)
			//" is true so spinning up new client... full msg='%v'", msg.HDR.To, msg)
		}
		dest, err1 := ipaddr.StripNanomsgAddressPrefix(msg.HDR.To)
		panicOn(err1)
		//vv("dest = '%v'", dest)

		// note: simnet depends on auto_cli_recognition_prefix
		// to properly report auto-clients vs lone-cli in the
		// reporting/diagnostics calls. Hence leave this prefix in place.
		cliName := auto_cli_recognition_prefix + s.name + "-to-" + dest
		ccfg := *s.cfg
		ccfg.ClientDialToHostPort = dest
		// uses same serverBaseID so simnet can group same host simnodes.
		cli, err2 := NewClient(cliName, &ccfg)
		panicOn(err2)
		if err2 != nil {
			return
		}
		s.halt.AddChild(cli.halt)

		// does it matter than the net.Conn / rwPair is
		// running on a client instead of from the server?
		// as long as the registry for PeerServiceFunc is
		// shared between all local clients and servers,
		// it should not matter. So we force the client
		// to use the registrations of the server.
		// The localServiceNameMap must be shared; not
		// sure we can also share the u though.
		cli.PeerAPI = s.PeerAPI
		cli.notifies = s.notifies

		// have to Start() first to get the cli.conn setup.
		err2 = cli.Start()
		if err2 != nil {
			return
		}
		if cli == nil || cli.conn == nil {
			if !s.cfg.QuietTestMode {
				alwaysPrintf("%v no cli.conn, assuming shutdown in progress...", s.name)
			}
			return
		}

		s.mut.Lock()
		s.autoClients = append(s.autoClients, cli)
		s.mut.Unlock()

		key := remote(cli.conn)
		p := &rwPair{
			isAutoCli: true,
			pairID:    s.lastPairID.Add(1),
			Conn:      cli.conn.(net.Conn),
			SendCh:    cli.oneWayCh,
			from:      local(cli.conn),
			to:        key,
		}
		s.mut.Lock() // want these two set together atomically.
		s.remote2pair.Set(key, p)
		s.pair2remote.Set(p, key)
		s.mut.Unlock()

		//vv("started auto-client ok. trying again... from:'%v'; to:'%v'", p.from, p.to)
		err, ch = sendOneWayMessage(s, ctx, msg, errWriteDur)
	}
	return
}

// one difference from Client.oneWaySendHelper is that
// is that the client side applies backpressure in that
// it waits for the send loop to actually send.
//
// Update: The old default for errWriteDur of 30 msec (if nil) no
// longer applies. Server.SendMessage() retains it for
// goq use, but for keeping our memory low, we want
// back-pressure. Also to not have to allocate a DoneCh
// when we don't need/want one, we avoid the
// 30 msec default.
//
// So the new errWriteDur parameter is a value
// of type Duration (which is int64) rather than a pointer *Duration.
// An errWriteDur of 0 will mean block until sent. We will
// allocate a local-only DoneCh to wait on if you don't have one.
// A negative errWriteDur
// will mean no waiting (to balance/avoid blocking other service
// loops). A positive errWriteDur will wait for that long
// before returning, and will supply the a DoneCh if it
// is nil on msg.
func sendOneWayMessage(s oneWaySender, ctx context.Context, msg *Message, errWriteDur time.Duration) (error, chan *Message) {

	//if msg.HDR.Serial == 0 {
	// nice to do this, but test 004, 014, 015 crash then and need attention.
	//panic("Serial 0 not allowed! must issueSerial() numbers!")
	//}

	if msg.HDR.Typ < 100 {
		return ErrWrongCallTypeForSendMessage, nil
	}

	destAddr := msg.HDR.To
	// abstract this for Client/Server symmetry
	sendCh, haltCh, to, from, ok := s.destAddrToSendCh(destAddr)
	if !ok {
		// srv_test.go:651
		vv("could not find destAddr='%v' in from our s.destAddrToSendCh() call.", destAddr)
		return ErrNetConnectionNotFound, nil
	}
	if to != destAddr {
		// re-write the unNAT-ed (or re-NAT-ed!) addresses
		// so that sendLoop does not have to.
		msg.HDR.To = to
	}

	msg.HDR.From = from

	//vv("send message attempting to send %v bytes to '%v'", len(data), destAddr)

	// An errWriteDur == -2 is a special case: it
	// means that pump.go is trying to
	// close a circuit, which solves this problem:
	// if instead we block, this can result in
	// a distributed deadlock where two senders both block
	// their read loops trying to close circuits.
	// pump.go and any caller using -1 is/must be prepared
	// to queue a background close instead; we
	// will give up after a millisecond to avoid the
	// deadlock.
	if errWriteDur == -2 {

		timeout := s.NewTimer(time.Millisecond)
		if timeout == nil {
			// happens on system shutdown
			return ErrShutdown(), nil
		}
		defer timeout.Discard()

		select {
		case sendCh <- msg:
			return nil, nil
		case <-haltCh:
			//vv("errWriteDur == -2: shutting down on haltCh = %p", haltCh)
			return ErrShutdown(), nil
		case <-ctx.Done():
			return ErrContextCancelled, nil

		case <-timeout.C:
			return ErrAntiDeadlockMustQueue, sendCh
		}
	} else {
		select {
		case sendCh <- msg:

		case <-haltCh:
			//vv("shutting down on haltCh = %p", haltCh)
			return ErrShutdown(), nil
		case <-ctx.Done():
			return ErrContextCancelled, nil
		}
	}

	if errWriteDur < 0 {
		return nil, nil
	}

	// so we need channel
	var doneCh <-chan struct{}
	var timeoutCh <-chan time.Time
	if msg.DoneCh == nil {
		// user does not want notifying
		doneCh = make(<-chan struct{})
	} else {
		// use wants notifying: use the chan they gave us.
		doneCh = msg.DoneCh.WhenClosed()
	}
	if errWriteDur > 0 {
		timeout := s.NewTimer(errWriteDur)
		if timeout == nil {
			// happens on system shutdown
			return ErrShutdown(), nil
		}
		defer timeout.Discard()
		timeoutCh = timeout.C
	}

	//vv("srv SendMessage about to wait %v to check on connection.", errWriteDur)
	select {
	case <-doneCh:
		//vv("srv SendMessage got back msg.LocalErr = '%v'", msg.LocalErr)
		return msg.LocalErr, nil
	case <-timeoutCh:
		//vv("srv SendMessage timeout after waiting %v", errWriteDur)
		return ErrSendTimeout, nil
	case <-ctx.Done():
		return ErrContextCancelled, nil
	case <-haltCh:
		return ErrShutdown(), nil
	}
	return nil, nil
}

// NewServer will keep its own copy of
// config. If config is nil, the
// server will make its own upon Start().
func NewServer(name string, config *Config) *Server {

	var cfg *Config
	if config != nil {
		clone := *config // cfg.shared is a pointer to enable this shallow copy.
		cfg = &clone
	} else {
		// why defer this to Start()? needed for serverBaseID now. previously was lazy
		cfg = NewConfig()
	}
	cfg.serverBaseID = NewCallID("")

	s := &Server{
		name:              name,
		cfg:               cfg,
		remote2pair:       NewMutexmap[string, *rwPair](),
		pair2remote:       NewMutexmap[*rwPair, string](),
		RemoteConnectedCh: make(chan *ServerClient, 20),

		callme2map:                   NewMutexmap[string, TwoWayFunc](),
		callme1map:                   NewMutexmap[string, OneWayFunc](),
		callmeServerSendsDownloadMap: NewMutexmap[string, ServerSendsDownloadFunc](),
		callmeUploadReaderMap:        NewMutexmap[string, UploadReaderFunc](),
		callmeBistreamMap:            NewMutexmap[string, BistreamFunc](),

		unNAT: NewMutexmap[string, string](),
	}
	s.halt = idem.NewHalterNamed(fmt.Sprintf("Server(%v %p)", name, s))

	s.notifies = newNotifies(notClient, s)
	// allow nil config still, since the above does.
	useSimNet := false
	if cfg != nil {
		useSimNet = cfg.UseSimNet
		// always have cfg above, to assign serverBaseID
		//} else {
		//	alwaysPrintf("warning: nil cfg, so useSimNet off.")
	}
	s.PeerAPI = newPeerAPI(s, notClient, useSimNet)
	return s
}

func (s *Server) RegisterServerSendsDownloadFunc(name string, callme ServerSendsDownloadFunc) {
	s.callmeServerSendsDownloadMap.Set(name, callme)
}

func (s *Server) RegisterBistreamFunc(name string, callme BistreamFunc) {
	s.callmeBistreamMap.Set(name, callme)
}

// Register2Func tells the server about a func or method
// that will have a returned Message value. See the
// [TwoWayFunc] definition.
func (s *Server) Register2Func(serviceName string, callme2 TwoWayFunc) {
	s.callme2map.Set(serviceName, callme2)
}

// Register1Func tells the server about a func or method
// that will not reply. See the [OneWayFunc] definition.
func (s *Server) Register1Func(serviceName string, callme1 OneWayFunc) {
	//vv("Register1Func called with callme1 = %p", callme1)
	s.callme1map.Set(serviceName, callme1)
}

// RegisterUploadReaderFunc tells the server about a func or method
// to handle uploads. See the
// [UploadReaderFunc] definition.
func (s *Server) RegisterUploadReaderFunc(name string, callmeUploadReader UploadReaderFunc) {
	s.callmeUploadReaderMap.Set(name, callmeUploadReader)
}

// Start has the Server begin receiving and processing RPC calls.
// The Config.ServerAddr tells us what host:port to bind and listen on.
func (s *Server) Start() (serverAddr net.Addr, err error) {
	//vv("Server.Start() called")
	if s.cfg == nil {
		// we do this in NewServer now, but leave this here
		// in case we revert/realize why we didn't eagerly instantiate
		// it there before... might have to do with cli and srv
		// sharing a config.
		s.cfg = NewConfig()
	}

	if s.cfg.UseSimNet {
		// turn off TLS for sure under simnet.
		s.cfg.TCPonly_no_TLS = true
	}

	s.cfg.srvStartingDir, err = os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("rpc25519.Server.Start() could not Getwd(): '%v'", err)
	}
	dataDir, err := GetServerDataDir()
	if err != nil {
		return nil, fmt.Errorf("rpc25519.Server.Start() could "+
			"not GetDataDir(): '%v'", err)
	}
	s.cfg.srvDataDir = dataDir
	if !dirExists(s.cfg.srvDataDir) {
		err = os.MkdirAll(s.cfg.srvDataDir, 0700)
		if err != nil {
			return nil, fmt.Errorf("rpc25519.Server.Start() could "+
				"not make the server data directory '%v': '%v'",
				s.cfg.srvDataDir, err)
		}
	}

	if s.cfg.ServerAddr == "" {
		panic(fmt.Errorf("no ServerAddr specified in Server.cfg"))
	}
	boundCh := make(chan net.Addr, 1)
	//vv("about to call runServerMain")
	go s.runServerMain(s.cfg.ServerAddr, s.cfg.TCPonly_no_TLS, s.cfg.CertPath, boundCh)

	select {
	case serverAddr = <-boundCh:

		s.mut.Lock()
		s.netAddr = serverAddr
		s.mut.Unlock()

		// Important about the following, case <-time.After(10 * time.Second).
		// The simnet not necessarily inititiazed yet,
		// since that happens in runServerMain() just launched above.
		// So this next time.After() needs to be an
		// exception to using s.TimeAfter() which would
		// put it through the simnet. This is fine.
		// This is just a bootstrapping thing. We are not
		// involved in network communication because
		// the caller won't do net comms until this
		// select responds that server has bound its port.
	case <-time.After(10 * time.Second):

		err = fmt.Errorf("server could not bind '%v' after 10 seconds", s.cfg.ServerAddr)
	}
	//vv("Server.Start() returning. serverAddr='%v'; err='%v'", serverAddr, err)
	return
}

// Close asks the Server to shut down.
func (s *Server) Close() error {
	//vv("Server.Close() '%v' called.", s.name)

	// simnet_server.go:36 already does this, if started.
	//if s.simnet != nil && s.simnode != nil {
	//	const wholeHost = true
	//	s.simnet.alterCircuit(s.simnode.name, SHUTDOWN, wholeHost)
	//}

	// ask any sub components (peer pump loops) to stop;
	// give them all up to 500 msec.
	//vv("%v about to s.halt.StopTreeAndWaitTilDone()", s.name)
	s.halt.StopTreeAndWaitTilDone(500*time.Millisecond, nil, nil)
	//vv("%v back from s.halt.StopTreeAndWaitTilDone()", s.name)

	if s.cfg.UseQUIC {
		s.cfg.shared.mut.Lock()
		if !s.cfg.shared.isClosed { // since Server.Close() might be called more than once.
			s.cfg.shared.shareCount--
			if s.cfg.shared.shareCount < 0 {
				panic("server count should never be < 0")
			}
			//vv("s.cfg.shared.shareCount = '%v' for '%v'", s.cfg.shared.shareCount, s.name)
			if s.cfg.shared.shareCount == 0 {
				s.cfg.shared.quicTransport.Conn.Close()
				s.cfg.shared.isClosed = true
				//vv("s.cfg.shared.quicTransport.Conn.Close() called for '%v'.", s.name)
			}
		}
		s.cfg.shared.mut.Unlock()
	}
	s.halt.ReqStop.Close()
	s.mut.Lock() // avoid data race
	if s.cfg.UseSimNet {
		// no lsn in use.
	} else {
		s.lsn.Close() // cause runServerMain listening loop to exit.
	}
	// close down any automatically started cluster/grid clients
	for _, cli := range s.autoClients {
		cli.Close()
	}
	s.mut.Unlock()
	<-s.halt.Done.Chan

	return nil
}

// http CONNECT -> hijack to TCP stuff, from net/rpc

const (
	// Defaults used by HandleHTTP
	DefaultRPCPath = "/_goRPC_"
)

// Can connect to RPC service using HTTP CONNECT to rpcPath.
var connectedToGoRPC = "200 Connected to Go RPC"

// ServeHTTP implements an [http.Handler] that answers RPC requests.
func (server *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		io.WriteString(w, "405 must CONNECT\n")
		return
	}
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Print("rpc hijacking ", req.RemoteAddr, ": ", err.Error())
		return
	}
	io.WriteString(conn, "HTTP/1.0 "+connectedToGoRPC+"\n\n")
	server.serveConn(conn)
}

// for cancelling via context; now
// also for streaming from cli to server.
type inflight struct {
	mut sync.Mutex

	// map callID to *callctx
	activeCalls map[string]*callctx
}

type callctx struct {
	callID     string
	added      time.Time
	callSubj   string
	cancelFunc context.CancelFunc
	deadline   time.Time
	ctx        context.Context

	lastStreamPart int64
	streamCh       chan *Message
}

func (s *Server) registerInFlightCallToCancel(msg *Message, cancelFunc context.CancelFunc, ctx context.Context) {

	//vv("top registerInFlightCallToCancel(msg='%v')", msg.HDR.String())

	s.inflight.mut.Lock()
	defer s.inflight.mut.Unlock()

	if s.inflight.activeCalls == nil {
		s.inflight.activeCalls = make(map[string]*callctx)
	}

	// this might be the 2nd time here for the CallUploadBegin,
	// now that we have the cancelFunc made.
	cc, ok := s.inflight.activeCalls[msg.HDR.CallID]

	if !ok {
		// first time here: cancelFunc and ctx will be nil
		// for CallUploadBegin, but not for others.
		cc = &callctx{
			callID:         msg.HDR.CallID,
			added:          time.Now(),
			callSubj:       msg.HDR.Subject,
			ctx:            ctx,
			cancelFunc:     cancelFunc,
			lastStreamPart: msg.HDR.StreamPart,
		}

		switch msg.HDR.Typ {
		case CallUploadBegin, CallRequestBistreaming:
			// give it lots of room because to keep FIFO
			// we need the read loop to post here directly,
			// without starting goroutines to service the stream.
			streamChan := make(chan *Message, 10_000)
			// must queue ourselves to be sure we are
			// process first.
			streamChan <- msg // large new buffer above, cannot block.
			cc.streamCh = streamChan
			msg.HDR.streamCh = streamChan
		}
		if ctx != nil {
			dl, ok := ctx.Deadline()
			if ok {
				cc.deadline = dl
			}
		}
	} else {
		// second time here, this is a CallUploadBegin,
		// and now we have ctx and cancelFunc
		if ctx != nil {
			dl, ok := ctx.Deadline()
			if ok {
				cc.deadline = dl
			}
			cc.ctx = ctx
		}
		cc.cancelFunc = cancelFunc
	}

	s.inflight.activeCalls[msg.HDR.CallID] = cc
	return
}

func (s *Server) noLongerInFlight(callID string) {
	s.cancelCallID(callID)
}

// PRE: s.inflight.mut is NOT held. Inside this func, we update
// inflight after cancelling the call, all while holding
// the inflight mut so this is atomic. Even if callID has
// no cancelFunc it will be deleted from the inflight.activeCalls
// map.
func (s *Server) cancelCallID(callID string) {

	s.inflight.mut.Lock()
	defer s.inflight.mut.Unlock()

	if s.inflight.activeCalls == nil {
		s.inflight.activeCalls = make(map[string]*callctx)
		return
	}

	cc, ok := s.inflight.activeCalls[callID]
	if !ok {
		return
	}
	if cc.cancelFunc != nil {
		cc.cancelFunc()
	}
	delete(s.inflight.activeCalls, callID)
}

func (s *rwPair) beginReadStream(
	ctx context.Context,
	callmeUploadReaderFunc UploadReaderFunc,
	req *Message,
	reply *Message,
) (err error) {

	//vv("beginReadStream internal TwoFunc top.")

	// Now the StreamBegain call itself is in the queue,
	// to ensure FIFO order of the stream messages.
	// So we don't need a separate call to s.callmeUploadReader before
	// the loop; everything can be handled uniformly.

	hdr0 := &req.HDR
	//ctx := req.HDR.Ctx
	var msgN *Message
	bytesRead := 0
	var last *Message
	callID := req.HDR.CallID

	// when we exit, tell the user func to cleanup after this callID
	defer callmeUploadReaderFunc(nil, nil, nil, callID)

	for i := int64(0); last == nil; i++ {

		// get streaming messages from the processWork() goroutine,
		// when it calls handleUploadParts().
		select {
		case msgN = <-hdr0.streamCh:
			if i == 0 {
				ctx = msgN.HDR.Ctx
				if msgN != req {
					panic("req should be first in the queue!")
				}
			} else {
				msgN.HDR.Ctx = ctx
			}
		case <-ctx.Done():
			// allow call cancellation.
			return fmt.Errorf("context cancelled")
		}
		bytesRead += len(msgN.JobSerz)

		hdrN := &msgN.HDR

		if hdrN.StreamPart != i {
			panic(fmt.Errorf("MessageAPI_ReceiveFile: stream "+
				"out of sync, expected next StreamPart to be %v; "+
				"but we see %v.", i, hdrN.StreamPart))
		}
		// INVAR: we only get matching CallID messages here. Assert this.
		if i > 0 && hdrN.CallID != hdr0.CallID {
			panic(fmt.Errorf("hdr0.CallID = '%v', but "+
				"hdrN.CallID = '%v', at part %v", hdr0.CallID, hdrN.CallID, i))
		}

		switch hdrN.Typ {
		case CallUploadEnd:
			last = reply
			// since s.GetEmptyMessage() does not allocate the Args map.
			last.HDR.Args = map[string]string{}
		} // else leave last nil

		err = callmeUploadReaderFunc(ctx, msgN, last, "")
		if err != nil {
			return
		}
		if last != nil {
			//vv("on last, client set replyLast = '%#v'", reply)
			return
		}
	}
	return nil
}

type serverSendDownloadHelper struct {
	srv *Server
	job *job
	req *Message
	ctx context.Context

	template *Message
	mut      sync.Mutex
	nextPart int64
}

func (s *Server) newServerSendDownloadHelper(ctx context.Context, job *job) *serverSendDownloadHelper {
	msg := &Message{}
	// no DoneCh, as we send ourselves.

	req := job.req

	msg.HDR.Subject = req.HDR.Subject
	msg.HDR.ServiceName = req.HDR.ServiceName
	msg.HDR.To = req.HDR.From
	msg.HDR.From = req.HDR.To
	msg.HDR.Seqno = req.HDR.Seqno
	msg.HDR.CallID = req.HDR.CallID
	msg.HDR.Deadline, _ = ctx.Deadline()

	return &serverSendDownloadHelper{
		ctx:      ctx,
		job:      job,
		srv:      s,
		req:      req,
		template: msg,
	}
}

func (s *serverSendDownloadHelper) sendDownloadPart(ctx context.Context, msg *Message, last bool) error {
	//vv("serverSendDownloadHelper.sendDownloadPart called! last = %v", last)

	// return early if we are cancelled, rather
	// than wasting time or bandwidth.
	select {
	case <-ctx.Done():
		return ErrContextCancelled
	default:
	}

	s.mut.Lock()
	defer s.mut.Unlock()

	i := s.nextPart
	s.nextPart++

	// need to preserve Args and JobSerz, to allow
	// Echo Bistreamer test to work, for example.

	msg.HDR.Created = time.Now()
	msg.HDR.From = s.template.HDR.To
	msg.HDR.To = s.template.HDR.From
	msg.HDR.ServiceName = s.template.HDR.ServiceName
	// Args kept
	msg.HDR.Subject = s.template.HDR.Subject
	msg.HDR.Seqno = s.template.HDR.Seqno

	switch {
	case i == 0:
		msg.HDR.Typ = CallDownloadBegin
	case last:
		msg.HDR.Typ = CallDownloadEnd
	default:
		msg.HDR.Typ = CallDownloadMore
	}
	msg.HDR.CallID = s.template.HDR.CallID
	msg.HDR.Serial = issueSerial()

	msg.HDR.Deadline, _ = ctx.Deadline()
	msg.HDR.StreamPart = i

	//err := s.job.w.sendMessage(s.job.conn, msg, &s.srv.cfg.WriteTimeout)
	err := s.job.w.sendMessage(s.job.conn, msg, nil)
	if err != nil {
		alwaysPrintf("serverSendDownloadHelper.sendDownloadPart error(): sendMessage got err = '%v'", err)
	} else {
		//vv("serverSendDownloadHelper.sendDownloadPart end: sendMessage went OK! no error.")
	}
	return err
}

func (s *Server) respondToReqWithError(req *Message, job *job, description string) {
	req.HDR.From, req.HDR.To = req.HDR.To, req.HDR.From
	req.JobSerz = nil
	req.HDR.Typ = CallError
	req.HDR.Subject = description
	//err := job.w.sendMessage(job.conn, req, &s.cfg.WriteTimeout)
	err := job.w.sendMessage(job.conn, req, nil)
	_ = err
	// called by srv.go:893, for example.
	alwaysPrintf("CallError being sent back: '%v'", req.HDR.String())

	// sendfile will just keep coming, so kill the connection.
	// after a short pause to try and let the CallError through.
	// Hopefully client will get it first and do the disconnect
	// themselves; since then they will have TIME_WAIT and not
	// the server.
	ti := s.NewTimer(time.Second * 3)
	if ti == nil {
		// happens on system shutdown
		return
	}

	select {
	case <-ti.C:
	case <-job.pair.halt.ReqStop.Chan:
	}
	ti.Discard()
	job.pair.halt.ReqStop.Close()
}

// GetReads registers to get any received messages on ch.
// It is similar to GetReadIncomingCh but for when ch
// already exists and you do not want a new one.
// It filters for CallID
func (s *Server) GetReadsForCallID(ch chan *Message, callID string) {
	//vv("GetReads called! stack='\n\n%v\n'", stack())
	if cap(ch) == 0 {
		panic("ch must be bufferred")
	}
	s.notifies.notifyOnReadCallIDMap.set(callID, ch)
	//vv("Server s.notifies.notifyOnReadCallIDMap[%v] = %p", callID, ch)
}

func (s *Server) GetErrorsForCallID(ch chan *Message, callID string) {
	//vv("GetReads called! stack='\n\n%v\n'", stack())
	if cap(ch) == 0 {
		panic("ch must be bufferred")
	}
	s.notifies.notifyOnErrorCallIDMap.set(callID, ch)
}

func (s *Server) GetReadsForToPeerID(ch chan *Message, objID string) {
	//vv("GetReads called! stack='\n\n%v\n'", stack())
	if cap(ch) == 0 {
		panic("ch must be bufferred")
	}
	s.notifies.notifyOnReadToPeerIDMap.set(objID, ch)
}

func (s *Server) GetErrorsForToPeerID(ch chan *Message, objID string) {
	//vv("GetReads called! stack='\n\n%v\n'", stack())
	if cap(ch) == 0 {
		panic("ch must be bufferred")
	}
	s.notifies.notifyOnErrorToPeerIDMap.set(objID, ch)
}

func (s *Server) UnregisterChannel(ID string, whichmap int) {

	switch whichmap {
	case CallIDReadMap:
		s.notifies.notifyOnReadCallIDMap.del(ID)
		//vv("Server Unregister s.notifies.notifyOnReadCallIDMap deleted %v", ID)
	case CallIDErrorMap:
		s.notifies.notifyOnErrorCallIDMap.del(ID)
	case ToPeerIDReadMap:
		s.notifies.notifyOnReadToPeerIDMap.del(ID)
	case ToPeerIDErrorMap:
		s.notifies.notifyOnErrorToPeerIDMap.del(ID)
	}
}

// LocalAddr retreives the local host/port that the
// Client is calling from.
// It is a part of the UniversalCliSrv interface.
func (s *Server) LocalAddr() string {
	s.mut.Lock()
	defer s.mut.Unlock()
	//vv("Server.LocalAddr returning '%v'", s.boundAddressString)
	return s.boundAddressString
}

func (s *Server) LocalNetAddr() net.Addr {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.netAddr
}

// RemoteAddr returns "" on the Server, because
// it is not well defined (which client? there can
// be many!) We still want to
// satisfy the UniveralCliSrv interface and
// let the Client help Peer users. Hence this stub.
// It is a part of the UniversalCliSrv interface.
func (s *Server) RemoteAddr() string {
	return "" // fragment.go StartRemotePeer() logic depends on this too.
}

// GetHostHalter returns the Servers's Halter
// control mechanism.
// It is a part of the UniversalCliSrv interface.
func (s *Server) GetHostHalter() *idem.Halter {
	return s.halt
}

// GetHostHalter returns the Client's Halter
// control mechanism.
// It is a part of the UniversalCliSrv interface.
func (s *Client) GetHostHalter() *idem.Halter {
	return s.halt
}

// RegisterPeerServiceFunc registers your
// PeerServiceFunc under the given name.
// Afterwards, it can be named in a call
// to start a local or remote peer.
// It is a part of the UniversalCliSrv interface.
func (s *Client) RegisterPeerServiceFunc(
	peerServiceName string,
	psf PeerServiceFunc,
) error {
	return s.PeerAPI.RegisterPeerServiceFunc(peerServiceName, psf)
}

// RegisterPeerServiceFunc registers your
// PeerServiceFunc under the given name.
// Afterwards, it can be named in a call
// to start a local or remote peer.
// It is a part of the UniversalCliSrv interface.
func (s *Server) RegisterPeerServiceFunc(
	peerServiceName string,
	psf PeerServiceFunc,
) error {

	return s.PeerAPI.RegisterPeerServiceFunc(peerServiceName, psf)
}

// StartLoalPeer creates and returns a local Peer,
// optionally establishing the Circuit in accordance
// with the requestedCircuit Message. Primarily
// requestedCircuit is used internally by the
// the system. It can be nil; no Circuit will be built.
//
// StartLoalPeer is a part of the Peer/Circuit/Fragment API.
// It is a part of the UniversalCliSrv interface.
func (s *Client) StartLocalPeer(
	ctx context.Context,
	peerServiceName string,
	requestedCircuit *Message,
) (lpb *LocalPeer, err error) {

	return s.PeerAPI.StartLocalPeer(ctx, peerServiceName, requestedCircuit)
}

// StartLoalPeer creates and returns a local Peer,
// optionally establishing the Circuit in accordance
// with the requestedCircuit Message. Primarily
// requestedCircuit is used internally by the
// the system. It can be nil; no Circuit will be built.
//
// StartLoalPeer is a part of the Peer/Circuit/Fragment API.
// It is a part of the UniversalCliSrv interface.
func (s *Server) StartLocalPeer(
	ctx context.Context,
	peerServiceName string,
	requestedCircuit *Message,
) (lpb *LocalPeer, err error) {

	return s.PeerAPI.StartLocalPeer(ctx, peerServiceName, requestedCircuit)
}

// StartRemotePeer bootstraps a remote Peer without
// establishing a Circuit with it. You should prefer
// StartRemotePeerAndGetCircuit to avoid a 2nd network
// roundtrip, if you can.
//
// This creates a remote Peer in the Peer/Circuit/Fragment API.
// It is a part of the UniversalCliSrv interface.
func (s *Client) StartRemotePeer(
	ctx context.Context,
	peerServiceName,
	remoteAddr string,
	waitUpTo time.Duration,
) (remotePeerURL, RemotePeerID string, err error) {

	return s.PeerAPI.StartRemotePeer(ctx, peerServiceName, remoteAddr, waitUpTo)
}

// StartRemotePeer bootstraps a remote Peer without
// establishing a Circuit with it. You should prefer
// StartRemotePeerAndGetCircuit to avoid a 2nd network
// roundtrip, if you can.
//
// This creates a remote Peer in the Peer/Circuit/Fragment API.
// It is a part of the UniversalCliSrv interface.
func (s *Server) StartRemotePeer(
	ctx context.Context,
	peerServiceName,
	remoteAddr string,
	waitUpTo time.Duration,
) (remotePeerURL, RemotePeerID string, err error) {

	return s.PeerAPI.StartRemotePeer(ctx, peerServiceName, remoteAddr, waitUpTo)
}

// StartRemotePeerAndGetCircuit is the main way to bootstrap
// a remote Peer in the Peer/Circuit/Fragment API. It
// returns a ready-to-use Circuit with circuitName as
// its name. It is a part of the UniversalCliSrv interface.
func (s *Client) StartRemotePeerAndGetCircuit(
	lpb *LocalPeer,
	circuitName string,
	frag *Fragment,
	peerServiceName,
	remoteAddr string,
	waitUpTo time.Duration,
) (ckt *Circuit, err error) {

	return s.PeerAPI.StartRemotePeerAndGetCircuit(
		lpb, circuitName, frag, peerServiceName, remoteAddr, waitUpTo)
}

// StartRemotePeerAndGetCircuit is the main way to bootstrap
// a remote Peer in the Peer/Circuit/Fragment API. It
// returns a ready-to-use Circuit with circuitName as
// its name. It is a part of the UniversalCliSrv interface.
func (s *Server) StartRemotePeerAndGetCircuit(
	lpb *LocalPeer,
	circuitName string,
	frag *Fragment,
	peerServiceName,
	remoteAddr string,
	waitUpTo time.Duration,
) (ckt *Circuit, err error) {

	return s.PeerAPI.StartRemotePeerAndGetCircuit(
		lpb, circuitName, frag, peerServiceName, remoteAddr, waitUpTo)
}

// GetConfig returns the Server's Config.
// It is a part of the UniversalCliSrv interface.
func (s *Server) GetConfig() *Config {
	return s.cfg
}

// GetConfig returns the Clients's Config.
// It is a part of the UniversalCliSrv interface.
func (c *Client) GetConfig() *Config {
	return c.cfg
}

// Used to be public, but data races forced
// a re-design to use the NewFragment on the LocalPeer.
// Just means doing peer.NewFragment() instead of peer.U.NewFragment().
func (s *Server) newFragment() (f *Fragment) {
	s.fragLock.Lock()
	defer s.fragLock.Unlock()
	return s.PeerAPI.newFragment()
}
func (s *Client) newFragment() (f *Fragment) {
	s.fragLock.Lock()
	defer s.fragLock.Unlock()
	return s.PeerAPI.newFragment()
}

func (s *Server) freeFragment(frag *Fragment) {
	s.fragLock.Lock()
	defer s.fragLock.Unlock()
	s.PeerAPI.freeFragment(frag)
}
func (s *Client) freeFragment(frag *Fragment) {
	s.fragLock.Lock()
	defer s.fragLock.Unlock()
	s.PeerAPI.freeFragment(frag)
}

func (s *Client) recycleFragLen() int {
	s.fragLock.Lock()
	defer s.fragLock.Unlock()
	return s.PeerAPI.recycleFragLen()
}
func (s *Server) recycleFragLen() int {
	s.fragLock.Lock()
	defer s.fragLock.Unlock()
	return s.PeerAPI.recycleFragLen()
}

func (s *Server) PingStats(remote string) *PingStat {
	pair, ok := s.remote2pair.Get(remote)
	if !ok {
		return nil
	}
	return &PingStat{
		LastPingSentTmu:     pair.lastPingSentTmu.Load(),
		LastPingReceivedTmu: pair.lastPingReceivedTmu.Load(),
	}
}

func (s *Server) AutoClients() (list []*Client, isServer bool) {
	isServer = true
	s.mut.Lock()
	defer s.mut.Unlock()

	for _, c := range s.autoClients {
		list = append(list, c)
	}
	return
}

func (c *Client) AutoClients() (list []*Client, isServer bool) {
	return
}
