package rpc25519

// srv.go: simple TCP server, with TLS encryption.

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/glycerine/idem"
)

var _ = os.MkdirAll
var _ = fmt.Printf

//var serverAddress = "0.0.0.0:8443"

//var serverAddress = "192.168.254.151:8443"

// boundCh should be buffered, at least 1, if it is not nil. If not nil, we
// will send the bound net.Addr back on it after we have started listening.
func (s *Server) RunServerMain(serverAddress string, tcp_only bool, certPath string, boundCh chan net.Addr) {
	defer func() {
		s.halt.Done.Close()
	}()
	log.SetFlags(log.LstdFlags | log.Lshortfile) // Add Lshortfile for short file names

	embedded := false                 // always false now
	sslCA := fixSlash("certs/ca.crt") // path to CA cert

	keyName := "node"
	if s.cfg.KeyPairName != "" {
		keyName = s.cfg.KeyPairName
	}

	// path to CA cert to verify client certs, can be same as sslCA
	sslClientCA := sslCA
	sslCert := fixSlash(fmt.Sprintf("certs/%v.crt", keyName))    // path to server cert
	sslCertKey := fixSlash(fmt.Sprintf("certs/%v.key", keyName)) // path to server key

	if certPath != "" {
		embedded = false
		sslCA = fixSlash(fmt.Sprintf("%v/ca.crt", certPath)) // path to CA cert
		sslClientCA = sslCA
		sslCert = fixSlash(fmt.Sprintf("%v/%v.crt", certPath, keyName))    // path to server cert
		sslCertKey = fixSlash(fmt.Sprintf("%v/%v.key", certPath, keyName)) // path to server key
	}

	var err error
	var config *tls.Config
	if tcp_only {
		// actually just run TCP and not TLS, since we might not have cert authority (e.g. under test)
		s.RunTCP(serverAddress, boundCh)
		return
	} else {
		config, err = LoadServerTLSConfig(embedded, sslCA, sslClientCA, sslCert, sslCertKey)
		if err != nil {
			panic(fmt.Sprintf("error on LoadServerTLSConfig() (using embedded=%v): '%v'", embedded, err))
		}
		// Not needed now that we have proper CA cert from gen.sh; or
		// perhaps this is the default anyway(?)
		// In any event, "localhost" is what we see during handshake; but
		// maybe that is because localhost is what we put in the ca.cnf and openssl-san.cnf
		// as the CN and DNS.1 names too(!)
		//config.ServerName = "localhost" // this would be the name of the remote client.
	}

	if s.cfg.SkipVerifyKeys {
		//if s.cfg.SkipClientCerts {
		//turn off client cert checking, allowing any random person on the internet to connect...
		config.ClientAuth = tls.NoClientCert
	} else {
		config.ClientAuth = tls.RequireAndVerifyClientCert
	}

	if s.cfg.UseQUIC {
		if s.cfg.TCPonly_no_TLS {
			panic("cannot have both UseQUIC and TCPonly_no_TLS true")
		}
		s.RunQUICServer(serverAddress, config, boundCh)
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

	s.lsn = listener // allow shutdown

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
			vv("Failed to accept connection: %v", err)
			continue
		}
		//vv("Accepted connection from %v", conn.RemoteAddr())

		// Handle the connection in a new goroutine
		tlsConn := conn.(*tls.Conn)

		go s.handleTLSConnection(tlsConn)
	}
}

func (s *Server) RunTCP(serverAddress string, boundCh chan net.Addr) {

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
			vv("Failed to accept connection: %v", err)
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

		pair := s.NewRWPair(conn)
		go pair.runSendLoop(conn)
		go pair.runRecvLoop(conn)
	}
}

func (s *Server) handleTLSConnection(conn *tls.Conn) {
	//vv("top of handleConnection()")

	defer func() {
		//vv("Closing connection from %v", conn.RemoteAddr())
		conn.Close()
	}()

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
		vv("tlsConn.Handshake() failed: '%v'", err)
		return
	}

	knownHostsPath := "known_client_keys"
	connState := conn.ConnectionState()
	raddr := conn.RemoteAddr()
	remoteAddr := strings.TrimSpace(raddr.String())

	if !s.cfg.SkipVerifyKeys {
		// NB only ed25519 keys are permitted, any others will result
		// in an immediate error
		good, bad, wasNew, err := HostKeyVerifies(knownHostsPath, &connState, remoteAddr)
		_ = wasNew
		_ = bad
		if err != nil && len(good) == 0 {
			//fmt.Fprintf(os.Stderr, "key failed list:'%#v': '%v'\n", bad, err)
			return
		}
		if err != nil {
			//vv("HostKeyVerifies returned error '%v' for remote addr '%v'", err, remoteAddr)
			return
		}
		//for i := range good {
		//	vv("accepted identity for client: '%v' (was new: %v)\n", good[i], wasNew)
		//}
	}

	pair := s.NewRWPair(conn)
	go pair.runSendLoop(conn)
	pair.runRecvLoop(conn)
}

func (s *RWPair) runSendLoop(conn net.Conn) {
	defer func() {
		s.halt.ReqStop.Close()
		s.halt.Done.Close()
	}()

	w := newWorkspace()

	for {
		select {
		case msg := <-s.SendCh:
			err := w.sendMessage(msg.Seqno, conn, msg, &s.cfg.WriteTimeout)
			if err != nil {
				vv("sendMessage got err = '%v'; on trying to send Seqno=%v", err, msg.Seqno)
			}
		case <-s.halt.ReqStop.Chan:
			return
		}
	}
}

func (s *RWPair) runRecvLoop(conn net.Conn) {
	defer func() {
		//vv("rpc25519.Server: runRecvLoop shutting down for local conn = '%v'", conn.LocalAddr())

		s.halt.ReqStop.Close()
		s.halt.Done.Close()
		conn.Close() // just the one, let other clients continue.

	}()

	w := newWorkspace()

	for {

		select {
		case <-s.halt.ReqStop.Chan:
			return
		default:
		}

		seqno, req, err := w.receiveMessage(conn, &s.cfg.ReadTimeout)
		if err == io.EOF {
			//vv("server sees io.EOF from receiveMessage")
			continue // close of socket before read of full message.
		}
		if err != nil {
			r := err.Error()
			if strings.Contains(r, "remote error: tls: bad certificate") {
				//vv("ignoring client connection with bad TLS cert.")
				continue
			}
			if strings.Contains(r, "use of closed network connection") {
				return // shutting down
			}

			vv("ugh. error from remote %v: %v", conn.RemoteAddr(), err)
			return
		}

		//vv("server received message with seqno=%v: %v", seqno, req)

		s.Server.mut.Lock()
		var callme CallbackFunc
		foundCallback := false
		if s.Server.callme != nil {
			callme = s.Server.callme
			foundCallback = true
		}
		s.Server.mut.Unlock()

		if foundCallback {
			// run the callback in a goto, so we can keep doing reads.
			go func(seqno uint64, req *Message, callme CallbackFunc) {

				req.Nc = conn
				req.Seqno = seqno
				req.MID.Seqno = seqno

				if cap(req.DoneCh) < 1 || len(req.DoneCh) >= cap(req.DoneCh) {
					panic("req.DoneCh too small; fails the sanity check to be received on.")
				}

				reply := callme(req)
				// <-req.DoneCh

				if reply != nil && reply.Err != nil {
					vv("note: callback on seqno %v from '%v' got Err='%v", seqno, conn.RemoteAddr(), err)
				}
				// if reply is nil, then we return nothing; it was probably a OneWaySend() target.

				if reply == nil && seqno > 0 {
					//vv("back from Server.callme() callback: nil reply but calling seqno=%v. huh", seqno)
				}
				// Since seqno was >0, we know that
				// a reply is eventually, expected, even though this callme gave none.
				// That's okay, server might just respond to it later with a sendMessage().

				if reply != nil {
					// Seqno: increment by one; so request 3 return response 4.
					reply.Seqno = req.Seqno + 1

					from := local(conn)
					to := remote(conn)
					isRPC := true
					isLeg2 := true
					subject := req.Subject

					mid := NewMID(from, to, subject, isRPC, isLeg2)

					// We are able to match call and response rigourously on the CallID alone.
					mid.CallID = req.MID.CallID
					mid.Seqno = reply.Seqno
					reply.MID = *mid

					select {
					case s.SendCh <- reply:
						//vv("reply went over pair.SendCh to the send goro write loop")
					case <-s.halt.ReqStop.Chan:
						return
					}
				}
			}(seqno, req, callme)
		}
	}
}

// Servers read and respond to requests.
// Server.Register() says which CallbackFunc to call.
// Only one call back func is supported at the moment.
type Server struct {
	mut sync.Mutex
	cfg *Config

	callme CallbackFunc

	lsn  io.Closer // net.Listener
	halt *idem.Halter

	remote2pair map[string]*RWPair

	// remote when server gets a new client,
	// So test 004 can avoid a race/panic.
	RemoteConnectedCh chan string
}

// keep the pair of goroutines running
// the read loop and the write loop
// for a given connection together so
// we can figure out who to SendCh to
// and how to halt each other.
type RWPair struct {
	// our parent Server
	Server *Server

	// copy of Server.cfg for convenience
	cfg *Config

	Conn   net.Conn
	SendCh chan *Message

	halt *idem.Halter
}

func (s *Server) NewRWPair(conn net.Conn) *RWPair {
	p := &RWPair{
		cfg:    s.cfg,
		Server: s,
		Conn:   conn,
		SendCh: make(chan *Message, 10),
		halt:   idem.NewHalter(),
	}
	key := remote(conn)

	s.mut.Lock()
	defer s.mut.Unlock()

	s.remote2pair[key] = p

	select {
	case s.RemoteConnectedCh <- key:
	default:
	}
	return p
}

var ErrNetConnectionNotFound = fmt.Errorf("error: net.Conn not found")

func (s *Server) SendMessage(callID, subject, destAddr string, by []byte, seqno uint64) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	pair, ok := s.remote2pair[destAddr]
	if !ok {
		//vv("could not find destAddr='%v' in our map: '%#v'", destAddr, s.remote2pair)

		return ErrNetConnectionNotFound
	}
	msg := NewMessage()
	msg.JobSerz = by
	msg.Seqno = seqno
	msg.MID.Seqno = seqno

	from := local(pair.Conn)
	to := remote(pair.Conn)
	isRPC := false
	isLeg2 := false
	subject = fmt.Sprintf("srv.SendMessage('%v')", subject)

	mid := NewMID(from, to, subject, isRPC, isLeg2)
	mid.CallID = callID
	msg.MID = *mid

	//vv("send message attempting to send %v bytes to '%v'", len(by), destAddr)
	select {
	case pair.SendCh <- msg:
		//	case <-time.After(time.Second):
		//vv("warning: time out trying to send on pair.SendCh")
	case <-s.halt.ReqStop.Chan:
		// shutting down
	}
	return nil
}

// NewServer will keep its own copy of
// config. If config is nil, the
// server will make its own upon Start().
func NewServer(config *Config) *Server {

	var cfg *Config
	if config != nil {
		clone := *config // cfg.shared is a pointer to enable this shallow copy.
		cfg = &clone
	}
	return &Server{
		cfg:               cfg,
		remote2pair:       make(map[string]*RWPair),
		halt:              idem.NewHalter(),
		RemoteConnectedCh: make(chan string, 20),
	}
}

func (s *Server) RegisterFunc(callme CallbackFunc) {
	s.mut.Lock()
	defer s.mut.Unlock()
	s.callme = callme
}

func (s *Server) Start() (serverAddr net.Addr, err error) {
	//vv("Server.Start() called")
	if s.cfg == nil {
		s.cfg = NewConfig()
	}
	if s.cfg.ServerAddr == "" {
		hostport := "127.0.0.1:0" // default to safe loopback
		AlwaysPrintf("Server.Start(): warning: nil config or no ServerAddr specified, binding to '%v'", hostport)
		s.cfg.ServerAddr = hostport
	}
	boundCh := make(chan net.Addr, 1)
	go s.RunServerMain(s.cfg.ServerAddr, s.cfg.TCPonly_no_TLS, s.cfg.CertPath, boundCh)

	select {
	case serverAddr = <-boundCh:
	case <-time.After(10 * time.Second):
		err = fmt.Errorf("server could not bind '%v' after 10 seconds", s.cfg.ServerAddr)
	}
	//vv("Server.Start() returning. serverAddr='%v'; err='%v'", serverAddr, err)
	return
}

func (s *Server) Close() error {
	//vv("Server.Close() called.")
	s.halt.ReqStop.Close()
	s.lsn.Close() // cause RunServerMain listening loop to exit.
	<-s.halt.Done.Chan
	return nil
}
