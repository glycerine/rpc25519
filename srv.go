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
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/glycerine/greenpack/msgp"
	"github.com/glycerine/idem"
	"github.com/glycerine/rpc25519/selfcert"
	"github.com/quic-go/quic-go"
)

var _ = os.MkdirAll
var _ = fmt.Printf

//var serverAddress = "0.0.0.0:8443"

//var serverAddress = "192.168.254.151:8443"

// boundCh should be buffered, at least 1, if it is not nil. If not nil, we
// will send the bound net.Addr back on it after we have started listening.
func (s *Server) runServerMain(serverAddress string, tcp_only bool, certPath string, boundCh chan net.Addr) {
	defer func() {
		s.halt.Done.Close()
	}()
	log.SetFlags(log.LstdFlags | log.Lshortfile) // Add Lshortfile for short file names

	s.tmStart = time.Now()

	s.cfg.checkPreSharedKey("server")
	//vv("server: s.cfg.encryptPSK = %v", s.cfg.encryptPSK)

	sslCA := fixSlash("certs/ca.crt") // path to CA cert

	keyName := "node"
	if s.cfg.ServerKeyPairName != "" {
		keyName = s.cfg.ServerKeyPairName
	}

	// since was redundant always,
	// selfcert.LoadNodeTLSConfigProtected() below does not use.
	// So commenting out:
	// path to CA cert to verify client certs, can be same as sslCA
	// sslClientCA := sslCA

	sslCert := fixSlash(fmt.Sprintf("certs/%v.crt", keyName))    // path to server cert
	sslCertKey := fixSlash(fmt.Sprintf("certs/%v.key", keyName)) // path to server key

	if certPath != "" {
		sslCA = fixSlash(fmt.Sprintf("%v/ca.crt", certPath)) // path to CA cert
		//sslClientCA = sslCA
		sslCert = fixSlash(fmt.Sprintf("%v/%v.crt", certPath, keyName))    // path to server cert
		sslCertKey = fixSlash(fmt.Sprintf("%v/%v.key", certPath, keyName)) // path to server key
	}

	var err error
	var config *tls.Config

	// handle pass-phrase protected certs/node.key
	config, s.creds, err = selfcert.LoadNodeTLSConfigProtected(true, sslCA, sslCert, sslCertKey)
	if err != nil {
		panic(fmt.Sprintf("error on LoadServerTLSConfig(): '%v'", err))
	}

	// Not needed now that we have proper CA cert from gen.sh; or
	// perhaps this is the default anyway(?)
	// In any event, "localhost" is what we see during handshake; but
	// maybe that is because localhost is what we put in the ca.cnf and openssl-san.cnf
	// as the CN and DNS.1 names too(!)
	//config.ServerName = "localhost" // this would be the name of the remote client.

	// start of as http, the get CONNECT and hijack to TCP.

	if tcp_only {
		// actually just run TCP and not TLS, since we might not have cert authority (e.g. under test)
		s.runTCP(serverAddress, boundCh)
		return
	}

	if s.cfg.SkipVerifyKeys {
		//turn off client cert checking, allowing any random person on the internet to connect...
		config.ClientAuth = tls.NoClientCert
	} else {
		config.ClientAuth = tls.RequireAndVerifyClientCert
	}

	if s.cfg.UseQUIC {
		if s.cfg.TCPonly_no_TLS {
			panic("cannot have both UseQUIC and TCPonly_no_TLS true")
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
	//vv("Server listening on %v:%v", addr.Network(), addr.String())
	if boundCh != nil {
		select {
		case boundCh <- addr:
		case <-time.After(100 * time.Millisecond):
		}
	}

	s.mut.Lock()     // avoid data race
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
	//vv("Server listening on %v:%v", addr.Network(), addr.String())
	if boundCh != nil {
		select {
		case boundCh <- addr:
		case <-time.After(100 * time.Millisecond):
		}
	}

	s.lsn = listener // allow shutdown

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

	if s.cfg.encryptPSK {
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
		//for i := range good {
		//	vv("accepted identity for client: '%v' (was new: %v)\n", good[i], wasNew)
		//}
	}

	if s.cfg.HTTPConnectRequired {
		// this might be too late to try and do CONNECT stuff here.
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
	vv("sendLoopGoroNum = [%v] for pairID = '%v'", sendLoopGoroNum, s.pairID)

	symkey := s.cfg.preSharedKey
	if s.cfg.encryptPSK {
		s.mut.Lock()
		symkey = s.randomSymmetricSessKeyFromPreSharedKey
		s.mut.Unlock()
	}

	w := newBlabber("server send loop", symkey, conn, s.Server.cfg.encryptPSK, maxMessage, true)

	// implement ServerSendKeepAlive
	var lastPing time.Time
	var doPing bool
	var pingEvery time.Duration
	var pingWakeCh <-chan time.Time
	keepAliveWriteTimeout := s.cfg.WriteTimeout

	if s.cfg.ServerSendKeepAlive > 0 {
		doPing = true
		pingEvery = s.cfg.ServerSendKeepAlive
		lastPing = time.Now()
		pingWakeCh = time.After(pingEvery)
		// keep the ping attempts to a minimum to keep this loop lively.
		if keepAliveWriteTimeout == 0 || keepAliveWriteTimeout > 10*time.Second {
			keepAliveWriteTimeout = 2 * time.Second
		}
	}

	for {
		if doPing {
			now := time.Now()
			if time.Since(lastPing) > pingEvery {
				err := w.sendMessage(conn, keepAliveMsg, &keepAliveWriteTimeout)
				//vv("srv sent rpc25519 keep alive. err='%v'; keepAliveWriteTimeout='%v'", err, keepAliveWriteTimeout)
				if err != nil {
					alwaysPrintf("server had problem sending keep alive: '%v'", err)
				}
				lastPing = now
				pingWakeCh = time.After(pingEvery)
			} else {
				pingWakeCh = time.After(lastPing.Add(pingEvery).Sub(now))
			}
		}

		select {
		case <-pingWakeCh:
			// check and send above.
			continue

		case msg := <-s.SendCh:
			//vv("srv got from s.SendCh, sending msg.HDR = '%v'", msg.HDR)
			err := w.sendMessage(conn, msg, &s.cfg.WriteTimeout)
			if err != nil {
				// notify any short-time-waiting server push user.
				// This is super useful to let goq retry jobs quickly.
				msg.LocalErr = err
				select {
				case msg.DoneCh <- msg:
				default:
				}
				alwaysPrintf("sendMessage got err = '%v'; on trying to send Seqno=%v", err, msg.HDR.Seqno)
				// just let user try again?
			} else {
				// tell caller there was no error.
				select {
				case msg.DoneCh <- msg:
				default:
				}
				lastPing = time.Now() // no need for ping
			}
		case <-s.halt.ReqStop.Chan:
			return
		}
	}
}

func (s *rwPair) runReadLoop(conn net.Conn) {
	defer func() {
		//vv("rpc25519.Server: runReadLoop shutting down for local conn = '%v'", conn.LocalAddr())

		s.halt.ReqStop.Close()
		s.halt.Done.Close()
		conn.Close() // just the one, let other clients continue.
		s.statsPairIDdone()
	}()

	readLoopGoroNum := GoroNumber()

	symkey := s.cfg.preSharedKey
	if s.cfg.encryptPSK {
		s.mut.Lock()
		symkey = s.randomSymmetricSessKeyFromPreSharedKey
		s.mut.Unlock()
	}
	w := newBlabber("server read loop", symkey, conn, s.Server.cfg.encryptPSK, maxMessage, true)

	for {

		select {
		case <-s.halt.ReqStop.Chan:
			return
		default:
		}

		req, err := w.readMessage(conn, &s.cfg.ReadTimeout)
		if err == io.EOF {
			//vv("server sees io.EOF from receiveMessage")
			// close of socket before read of full message.
			// shutdown this connection or we'll just
			// spin here at 500% cpu.
			return
		}
		if err != nil {
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

			alwaysPrintf("ugh. error from remote %v: %v", conn.RemoteAddr(), err)
			return
		}
		if req.HDR.Typ == CallKeepAlive {
			//vv("srv read loop got an rpc25519 keep alive.")
			continue
		}

		//vv("server received message with seqno=%v: %v", req.HDR.Seqno, req)

		// show diagnostics for fairness/starvation
		if s.cfg.ReportStats > 0 {
			s.statsPairIDAddCountAndReportOnJobs(readLoopGoroNum)
		}

		// Idea: send the job to the central work queue, so
		// we service jobs fairly in FIFO order.
		// Update: turns out this didn't really matter.
		// So we got rid of the work queue.
		job := &job{req: req, conn: conn, pair: s, w: w}

		// Workers requesting jobs can keep calls open for
		// minutes or hours or days; so we cannot just have
		// a single readLoop worker (say for 1 cpu) that
		// blocks waiting to finish: this has to be in
		// a new goroutine.
		go s.Server.processWorkQ(job)
	}
}

func (s *rwPair) statsPairIDdone() {
	if s.cfg.ReportStats > 0 {
		s.Server.mut.Lock()
		stats := s.Server.pair2jobs[s.pairID]
		// negative count means pair (connection) is finished.
		stats.count = -stats.count
		s.Server.mut.Unlock()
	}
}

type pairstat struct {
	count   int
	goronum int
	pairID  int64
	pair    *rwPair
}

func (s *rwPair) statsPairIDAddCountAndReportOnJobs(goronum int) {
	if s.cfg.ReportStats > 0 {
		s.Server.mut.Lock()
		stats, ok := s.Server.pair2jobs[s.pairID]
		if !ok {
			stats = &pairstat{goronum: goronum, pairID: s.pairID, pair: s}
			s.Server.pair2jobs[s.pairID] = stats
		}
		stats.count++
		s.Server.jobcount++
		if s.Server.jobcount%s.cfg.ReportStats == 1 {
			s.Server.reportOnJobs()
		}
		s.Server.mut.Unlock()
	}
}

type pairstatSlice []*pairstat

func (p pairstatSlice) Len() int { return len(p) }
func (p pairstatSlice) Less(i, j int) bool {
	return p[i].count < p[j].count
}
func (p pairstatSlice) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

// PRE: s.mut must be locked already
func (s *Server) reportOnJobs() {
	var stats pairstatSlice
	for _, stat := range s.pair2jobs {
		stats = append(stats, stat)
	}
	sort.Sort(stats)

	fmt.Printf("(%v since start). count of jobs done by each client:\n a=c(", time.Since(s.tmStart))
	for i, stat := range stats {
		if i == 0 {
			fmt.Printf("%v[%v]", stat.count, stat.goronum)
		} else {
			fmt.Printf(", %v[%v]", stat.count, stat.goronum)
		}
	}
	fmt.Printf(")\n\n")

	const dumpStacksWhenStuck = false // off for now
	if dumpStacksWhenStuck {
		// any stuck but still alive?
		if len(s.prevStats) > 0 {
			for _, stat := range stats {
				if stat.count <= 0 {
					continue // ignore dead goroutine, cannot dump their stacks.
				}
				// INVAR: stat is live, compare current count to prev count
				prev, ok := s.prevPair2jobs[stat.pairID]
				if !ok {
					continue // ignore, probably just a new client.
				}
				if prev.count == stat.count {
					alwaysPrintf("have same count %v as last time. goro = [%v]; pairID = %v ; last call goro = [%v]", prev.count, stat.goronum, stat.pairID, stat.pair.lastCallGoro)
					// found stuck goroutine, still live, note the pairID
					clone := *stat
					s.stuckPair = &clone
					vv("allstacks = \n\n%v\n\n", allstacks())
					break
				}
			}
		}

		// copy so we can detect stuck goro.
		s.prevPair2jobs = clonePair2jobs(s.pair2jobs)
		s.prevStats = append([]*pairstat{}, stats...)
	}
}

func clonePair2jobs(m map[int64]*pairstat) (r map[int64]*pairstat) {
	r = make(map[int64]*pairstat)
	for k, v := range m {
		// have to copy previous state of paristat
		cp := *v
		r[k] = &cp
	}
	return
}

type job struct {
	req  *Message
	conn net.Conn
	pair *rwPair
	w    *blabber
}

func (s *Server) processWorkQ(job *job) {

	var callme1 OneWayFunc
	var callme2 TwoWayFunc
	foundCallback1 := false
	foundCallback2 := false

	// qjob := job
	// select {
	// case qjob = <-s.workQ:
	// case <-s.halt.ReqStop.Chan:
	// 	return
	// }
	//if qjob != job {
	//vv("processWorkQ was not fifo?") // seen alot
	//	job = qjob // didn't help starvation on macOS.
	//}

	req := job.req
	//vv("processWorkQ got job: req.HDR='%v'", req.HDR.String())
	conn := job.conn
	pair := job.pair
	w := job.w

	pair.mut.Lock()
	pair.lastCallGoro = GoroNumber() // to locate the stuck call goroutine.
	pair.mut.Unlock()

	req.HDR.Nc = conn

	if req.HDR.Typ == CallNetRPC {
		//vv("have IsNetRPC call: '%v'", req.HDR.Subject)
		err := pair.callBridgeNetRpc(req)
		if err != nil {
			alwaysPrintf("callBridgeNetRpc errored out: '%v'", err)
		}
		return // continue
	}

	foundCallback1 = false
	foundCallback2 = false
	callme1 = nil
	callme2 = nil

	s.mut.Lock()
	if req.HDR.Typ == CallRPC {
		if s.callme2 != nil {
			callme2 = s.callme2
			foundCallback2 = true
		}
	} else {
		if s.callme1 != nil {
			callme1 = s.callme1
			foundCallback1 = true
		}
	}
	s.mut.Unlock()

	if !foundCallback1 && !foundCallback2 {
		//vv("warning! no callbacks found for req = '%v'", req)
	}

	if foundCallback1 {
		// run the callback in a goro, so we can keep doing reads.
		//go callme1(req)
		callme1(req)
		return
	}

	if foundCallback2 {
		//vv("foundCallback2 true, req.HDR = '%v'", req.HDR) // not seen

		//vv("req.Nc local = '%v', remote = '%v'", local(req.Nc), remote(req.Nc))
		//vv("stream local = '%v', remote = '%v'", local(stream), remote(stream))
		//vv("conn   local = '%v', remote = '%v'", local(conn), remote(conn))

		if cap(req.DoneCh) < 1 || len(req.DoneCh) >= cap(req.DoneCh) {
			panic("req.DoneCh too small; fails the sanity check to be received on.")
		}

		reply := NewMessage()

		replySeqno := req.HDR.Seqno // just echo back same.
		// allow user to change Subject
		reply.HDR.Subject = req.HDR.Subject
		reqCallID := req.HDR.CallID

		err := callme2(req, reply)
		if err != nil {
			reply.JobErrs = err.Error()
		}
		// don't read from req now, just in case callme2 messed with it.

		from := local(conn)
		to := remote(conn)
		hdr := NewHDR(from, to, reply.HDR.Subject, CallRPC)

		// We are able to match call and response rigourously on the CallID alone.
		hdr.CallID = reqCallID
		hdr.Seqno = replySeqno
		reply.HDR = *hdr

		// what if we just do the send ourself? why
		// wait for a channel send on s.SendCh?
		// We've added a mutex
		// inside sendMessage so we for sure won't conflict
		// with keep-alive pings. Will we get less
		// scheduling starvation and clients timing out
		// if we just do the w.sendMessage() ourselves?
		if false {
			select {
			case pair.SendCh <- reply:
				//vv("reply went over pair.SendCh to the send goro write loop: '%v'", reply)
			case <-s.halt.ReqStop.Chan:
				return
			}
		} else {
			err := w.sendMessage(conn, reply, &s.cfg.WriteTimeout)
			if err != nil {
				// notify any short-time-waiting server push user.
				// This is super useful to let goq retry jobs quickly.
				reply.LocalErr = err
				select {
				case reply.DoneCh <- reply:
				default:
				}
				alwaysPrintf("sendMessage got err = '%v'; on trying to send Seqno=%v", err, reply.HDR.Seqno)
				// just let user try again?
			} else {
				// tell caller there was no error.
				select {
				case reply.DoneCh <- reply:
				default:
				}
			}
		}
	}
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
	mut        sync.Mutex
	cfg        *Config
	quicConfig *quic.Config
	tmStart    time.Time

	lastPairID atomic.Int64

	// pair2jobs is: pairID(i.e. client) -> number of jobs requested.
	// update: use pairstat to track with goroutine is stalled to
	// help analyze stack dump.
	pair2jobs     map[int64]*pairstat
	prevPair2jobs map[int64]*pairstat
	prevStats     []*pairstat
	stuckPair     *pairstat // if set, have stuck read loop => dump its stack.

	jobcount int64
	workQ    chan *job

	name  string // which server, for debugging.
	creds *selfcert.Creds

	callme2 TwoWayFunc
	callme1 OneWayFunc

	lsn  io.Closer // net.Listener
	halt *idem.Halter

	remote2pair map[string]*rwPair
	pair2remote map[*rwPair]string

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

// like net_server.go NetServer.ServeCodec
func (p *rwPair) callBridgeNetRpc(reqMsg *Message) error {
	//vv("bridge called! subject: '%v'", reqMsg.HDR.Subject)

	p.encBuf.Reset()
	p.encBufW.Reset(&p.encBuf)
	p.greenCodec.enc.Reset(p.encBufW)

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
			p.sendResponse(reqMsg, req, invalidRequest, p.greenCodec, err.Error())
			p.Server.freeRequest(req)
		}
		return err
	}
	//wg.Add(1)
	//vv("about to callMethodByReflection")
	service.callMethodByReflection(p, reqMsg, mtype, req, argv, replyv, p.greenCodec, wantsCtx)

	return nil
}

func (s *service) callMethodByReflection(pair *rwPair, reqMsg *Message, mtype *methodType, req *Request, argv, replyv reflect.Value, codec ServerCodec, wantsCtx bool) {

	mtype.Lock()
	mtype.numCalls++
	mtype.Unlock()
	function := mtype.method.Func

	// Invoke the method, providing a new value for the reply.
	var returnValues []reflect.Value
	if wantsCtx {
		ctx := context.WithValue(context.Background(), "HDR", &reqMsg.HDR)
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
	pair.sendResponse(reqMsg, req, greenReplyv, codec, errmsg)
	pair.Server.freeRequest(req)
}

func (p *rwPair) sendResponse(reqMsg *Message, req *Request, reply Green, codec ServerCodec, errmsg string) {

	//vv("pair sendResponse() top, reply: '%#v'", reply)

	resp := p.Server.getResponse()
	// Encode the response header
	resp.ServiceMethod = req.ServiceMethod
	if errmsg != "" {
		resp.Error = errmsg
		reply = invalidRequest
	}
	resp.Seq = req.Seq
	//vv("srv sendResonse() for req.Seq = %v", req.Seq)
	//p.sending.Lock()
	err := codec.WriteResponse(resp, reply)
	if debugLog && err != nil {
		log.Println("rpc: writing response:", err)
	}
	//p.sending.Unlock()
	p.Server.freeResponse(resp)

	msg := NewMessage()
	replySeqno := reqMsg.HDR.Seqno // just echo back same.
	subject := reqMsg.HDR.Subject
	reqCallID := reqMsg.HDR.CallID

	from := local(p.Conn)
	to := remote(p.Conn)
	mid := NewHDR(from, to, subject, CallNetRPC)

	// We are able to match call and response rigourously on the CallID alone.
	mid.CallID = reqCallID
	mid.Seqno = replySeqno
	msg.HDR = *mid

	by := p.encBuf.Bytes()
	msg.JobSerz = make([]byte, len(by))
	copy(msg.JobSerz, by)
	//vv("response JobSerz is len %v", len(by))

	select {
	case p.SendCh <- msg:
		//vv("reply msg went over pair.SendCh to the send goro write loop: '%v'", msg)
	case <-p.halt.ReqStop.Chan:
		return
	}

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

	//nextDecoderType, err := p.greenCodec.dec.NextType()
	//if err == nil {
	//vv("srv: readRequestHeader(): header was read successfully, req = '%#v', left in reader ='%v' of type '%v'", req, p.greenCodec.dec.Buffered(), nextDecoderType)
	//}

	dot := strings.LastIndex(req.ServiceMethod, ".")
	if dot < 0 {
		err = errors.New("rpc: service/method request ill-formed: " + req.ServiceMethod)
		return
	}
	serviceName := req.ServiceMethod[:dot]
	methodName := req.ServiceMethod[dot+1:]

	// Look up the request.
	svci, ok := p.Server.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("rpc: can't find service " + req.ServiceMethod)
		return
	}
	svc = svci.(*service)
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
// parameter. The ctx will have an "HDR" value set on it giving a pointer to
// the `rpc25519.HDR` header from the incoming Message.
//
//	func (s *Service) NoContext(args *Args, reply *Reply) error
//
// * new:
//
//	func (s *Service) GetsContext(ctx context.Context, args *Args, reply *Reply) error {
//	   if hdr := ctx.Value("HDR"); hdr != nil {
//	      h, ok := hdr.(*rpc25519.HDR)
//	      if ok {
//	        fmt.Printf("GetsContext called with HDR = '%v'; "+
//	           "HDR.Nc.RemoteAddr() gives '%v'; HDR.Nc.LocalAddr() gives '%v'\n",
//	           h.String(), h.Nc.RemoteAddr(), h.Nc.LocalAddr())
//	      }
//	   } else {
//	      fmt.Println("HDR not found")
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
	greenCodec *greenpackServerCodec
	//sending  sync.Mutex
	encBuf  bytes.Buffer // target for codec writes: encode into here first
	encBufW *bufio.Writer
	decBuf  bytes.Buffer // target for code reads.

	// creds:

	// mut protects the following
	mut sync.Mutex

	lastCallGoro int // to locate the stuck call goroutine.

	// the ephemeral keys from the ephemeral ECDH handshake
	// to estblish randomSymmetricSessKeyFromPreSharedKey
	randomSymmetricSessKeyFromPreSharedKey [32]byte
	cliEphemPub                            []byte
	srvEphemPub                            []byte
	// only one of these two will be filled here, depending on if we are client or server.
	srvStaticPub ed25519.PublicKey
	cliStaticPub ed25519.PublicKey
}

func (s *Server) newRWPair(conn net.Conn) *rwPair {

	p := &rwPair{
		pairID: s.lastPairID.Add(1),
		cfg:    s.cfg.Clone(),
		Server: s,
		Conn:   conn,
		SendCh: make(chan *Message),
		halt:   idem.NewHalter(),
	}

	p.encBufW = bufio.NewWriter(&p.encBuf)
	p.greenCodec = &greenpackServerCodec{
		pair:   p,
		rwc:    nil,
		dec:    msgp.NewReader(&p.decBuf),
		enc:    msgp.NewWriter(p.encBufW),
		encBuf: p.encBufW,
	}

	key := remote(conn)

	s.mut.Lock()
	defer s.mut.Unlock()

	s.remote2pair[key] = p
	s.pair2remote[p] = key

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

	key, ok := s.pair2remote[p]
	if !ok {
		return
	}
	delete(s.pair2remote, p)
	delete(s.remote2pair, key)

	// see srv_test 015 for example use.
	close(p.allDone)
	//vv("Server.deletePair() has closed allDone for pair '%v'", p)
}

var ErrNetConnectionNotFound = fmt.Errorf("error: net.Conn not found")

// SendMessage can be used on the server to push data to
// one of the connected clients; that found at destAdddr.
//
// A NewMessage() Message will be created and JobSerz will contain the data.
// The HDR fields Subject, CallID, and Seqno will also be set from the arguments.
// If callID argument is the empty string, we will use a crypto/rand
// randomly generated one.
//
// If the destAddr is not already connected to the server, the
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
// The default is 30 msec. It is a guess and aims at balance:
// allowing enough time to get an error back from quic-go
// if we are going to discover "Application error 0x0 (remote)"
// right away, and not wanting to stall the caller too much.
func (s *Server) SendMessage(callID, subject, destAddr string, data []byte, seqno uint64,
	errWriteDur *time.Duration) error {

	s.mut.Lock()
	pair, ok := s.remote2pair[destAddr]
	// if we hold this too long then our pair cannot shutdown asap.
	s.mut.Unlock()

	if !ok {
		//vv("could not find destAddr='%v' in our map: '%#v'", destAddr, s.remote2pair)
		return ErrNetConnectionNotFound
	}
	//vv("found remote2pair for destAddr = '%v': '%#v'", destAddr, pair)

	msg := NewMessage()
	msg.JobSerz = data

	from := local(pair.Conn)
	to := remote(pair.Conn)
	subject = fmt.Sprintf("srv.SendMessage('%v')", subject)

	mid := NewHDR(from, to, subject, CallOneWay)
	if callID != "" {
		mid.CallID = callID
	}
	mid.Seqno = seqno
	msg.HDR = *mid

	//vv("send message attempting to send %v bytes to '%v'", len(data), destAddr)
	select {
	case pair.SendCh <- msg:
		//vv("sent to pair.SendCh, msg='%v'", msg.HDR.String())

		//	case <-time.After(time.Second):
		//vv("warning: time out trying to send on pair.SendCh")
	case <-s.halt.ReqStop.Chan:
		// shutting down
		return ErrShutdown
	}

	dur := 30 * time.Millisecond
	if errWriteDur != nil {
		dur = *errWriteDur
	}
	if dur > 0 {
		//vv("srv SendMessage about to wait %v to check on connection.", dur)
		select {
		case <-msg.DoneCh:
			//vv("srv SendMessage got back msg.LocalErr = '%v'", msg.LocalErr)
			return msg.LocalErr
		case <-time.After(dur):
			//vv("srv SendMessage timeout after waiting %v", dur)
		}
	}
	return nil
}

// NewServer will keep its own copy of
// config. If config is nil, the
// server will make its own upon Start().
func NewServer(name string, config *Config) *Server {

	var cfg *Config
	if config != nil {
		clone := *config // cfg.shared is a pointer to enable this shallow copy.
		cfg = &clone
	}
	return &Server{
		name:              name,
		cfg:               cfg,
		remote2pair:       make(map[string]*rwPair),
		pair2remote:       make(map[*rwPair]string),
		halt:              idem.NewHalter(),
		RemoteConnectedCh: make(chan *ServerClient, 20),
		pair2jobs:         make(map[int64]*pairstat),
		workQ:             make(chan *job, 1000),
	}
}

// Register2Func tells the server about a func or method
// that will have a returned Message value. See the
// [TwoWayFunc] definition.
func (s *Server) Register2Func(callme2 TwoWayFunc) {
	s.mut.Lock()
	defer s.mut.Unlock()
	s.callme2 = callme2
}

// Register1Func tells the server about a func or method
// that will not reply. See the [OneWayFunc] definition.
func (s *Server) Register1Func(callme1 OneWayFunc) {
	//vv("Register1Func called with callme1 = %p", callme1)
	s.mut.Lock()
	defer s.mut.Unlock()
	s.callme1 = callme1
}

// Start has the Server begin receiving and processing RPC calls.
// The Config.ServerAddr tells us what host:port to bind and listen on.
func (s *Server) Start() (serverAddr net.Addr, err error) {
	//vv("Server.Start() called")
	if s.cfg == nil {
		s.cfg = NewConfig()
	}
	if s.cfg.ServerAddr == "" {
		panic(fmt.Errorf("no ServerAddr specified in Server.cfg"))
	}
	boundCh := make(chan net.Addr, 1)
	go s.runServerMain(s.cfg.ServerAddr, s.cfg.TCPonly_no_TLS, s.cfg.CertPath, boundCh)

	select {
	case serverAddr = <-boundCh:
	case <-time.After(10 * time.Second):
		err = fmt.Errorf("server could not bind '%v' after 10 seconds", s.cfg.ServerAddr)
	}
	//vv("Server.Start() returning. serverAddr='%v'; err='%v'", serverAddr, err)
	return
}

// Close asks the Server to shut down.
func (s *Server) Close() error {
	//vv("Server.Close() '%v' called.", s.name)
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
	s.mut.Lock()  // avoid data race
	s.lsn.Close() // cause runServerMain listening loop to exit.
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
