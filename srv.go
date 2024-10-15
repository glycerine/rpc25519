package rpc25519

// srv.go: simple TCP server, with TLS encryption.

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/gob"
	"errors"
	"fmt"
	"go/token"
	"io"
	"log"
	"net"
	"os"
	"reflect"
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
	if s.cfg.ServerKeyPairName != "" {
		keyName = s.cfg.ServerKeyPairName
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
			AlwaysPrintf("Failed to accept connection: %v", err)
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
			AlwaysPrintf("Failed to accept connection: %v", err)
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

		pair := s.newRWPair(conn)
		go pair.runSendLoop(conn)
		go pair.runReadLoop(conn)
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
		AlwaysPrintf("tlsConn.Handshake() failed: '%v'", err)
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

	pair := s.newRWPair(conn)
	go pair.runSendLoop(conn)
	pair.runReadLoop(conn)
}

func (s *rwPair) runSendLoop(conn net.Conn) {
	defer func() {
		s.halt.ReqStop.Close()
		s.halt.Done.Close()
	}()

	w := newWorkspace()

	for {
		select {
		case msg := <-s.SendCh:
			err := w.sendMessage(conn, msg, &s.cfg.WriteTimeout)
			if err != nil {
				r := err.Error()
				if strings.Contains(r, "broken pipe") {
					msg.Err = err
					// how can we restart the connection? problem is, submitters reach out to us.
					// Maybe with quic if they run a server too, since we'll know the port
					// to find them on, if they are still up.
				}
				AlwaysPrintf("sendMessage got err = '%v'; on trying to send Seqno=%v", err, msg.HDR.Seqno)
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

	}()

	w := newWorkspace()

	var callme1 OneWayFunc
	var callme2 TwoWayFunc
	foundCallback1 := false
	foundCallback2 := false

	for {

		select {
		case <-s.halt.ReqStop.Chan:
			return
		default:
		}

		req, err := w.receiveMessage(conn, &s.cfg.ReadTimeout)
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

			AlwaysPrintf("ugh. error from remote %v: %v", conn.RemoteAddr(), err)
			return
		}

		vv("server received message with seqno=%v: %v", req.HDR.Seqno, req)

		req.HDR.Nc = conn

		if req.HDR.IsNetRPC {
			//vv("have IsNetRPC call: '%v'", req.HDR.Subject)
			s.callBridgeNetRpc(req)
			continue
		}

		foundCallback1 = false
		foundCallback2 = false
		callme1 = nil
		callme2 = nil

		s.Server.mut.Lock()
		if req.HDR.IsRPC {
			if s.Server.callme2 != nil {
				callme2 = s.Server.callme2
				foundCallback2 = true
			}
		} else {
			if s.Server.callme1 != nil {
				callme1 = s.Server.callme1
				foundCallback1 = true
			}
		}
		s.Server.mut.Unlock()

		if !foundCallback1 && !foundCallback2 {
			vv("warning! no callbacks found for req = '%v'", req)
		}

		if foundCallback1 {
			// run the callback in a goro, so we can keep doing reads.
			go callme1(req)
		}

		if foundCallback2 {
			// run the callback in a goro, so we can keep doing reads.
			go func(req *Message, callme2 TwoWayFunc) {

				//vv("req.Nc local = '%v', remote = '%v'", local(req.Nc), remote(req.Nc))
				//vv("stream local = '%v', remote = '%v'", local(stream), remote(stream))
				//vv("conn   local = '%v', remote = '%v'", local(conn), remote(conn))

				if cap(req.DoneCh) < 1 || len(req.DoneCh) >= cap(req.DoneCh) {
					panic("req.DoneCh too small; fails the sanity check to be received on.")
				}

				reply := NewMessage()

				replySeqno := req.HDR.Seqno // just echo back same.
				subject := req.HDR.Subject
				reqCallID := req.HDR.CallID

				callme2(req, reply)
				// don't read from req now, just in case callme2 messed with it.

				from := local(conn)
				to := remote(conn)
				isRPC := true
				isLeg2 := true

				mid := NewHDR(from, to, subject, isRPC, isLeg2)

				// We are able to match call and response rigourously on the CallID alone.
				mid.CallID = reqCallID
				mid.Seqno = replySeqno
				reply.HDR = *mid

				select {
				case s.SendCh <- reply:
					//vv("reply went over pair.SendCh to the send goro write loop: '%v'", reply)
				case <-s.halt.ReqStop.Chan:
					return
				}
			}(req, callme2)
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
	mut sync.Mutex
	cfg *Config

	name string // which server, for debugging.

	callme2 TwoWayFunc
	callme1 OneWayFunc

	lsn  io.Closer // net.Listener
	halt *idem.Halter

	remote2pair map[string]*rwPair

	// remote when server gets a new client,
	// So test 004 can avoid a race/panic.
	RemoteConnectedCh chan string

	// net/rpc implementation details
	serviceMap sync.Map   // map[string]*service
	reqLock    sync.Mutex // protects freeReq
	freeReq    *Request
	respLock   sync.Mutex // protects freeResp
	freeResp   *Response
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

	p.decBuf.Reset()
	p.decBuf.Write(reqMsg.JobSerz)

	service, mtype, req, argv, replyv, keepReading, err := p.readRequest(p.gobCodec)
	//vv("p.readRequest() back with err = '%v'", err)
	if err != nil {
		if debugLog && err != io.EOF {
			log.Println("rpc:", err)
		}
		if !keepReading {
			return err
		}
		// send a response if we actually managed to read a header.
		if req != nil {
			p.sendResponse(reqMsg, req, invalidRequest, p.gobCodec, err.Error())
			p.Server.freeRequest(req)
		}
		return err
	}
	//wg.Add(1)
	//vv("about to call25519")
	service.call25519(p, reqMsg, mtype, req, argv, replyv, p.gobCodec)

	return nil
}

func (s *service) call25519(pair *rwPair, reqMsg *Message, mtype *methodType, req *Request, argv, replyv reflect.Value, codec ServerCodec) {

	mtype.Lock()
	mtype.numCalls++
	mtype.Unlock()
	function := mtype.method.Func
	// Invoke the method, providing a new value for the reply.
	returnValues := function.Call([]reflect.Value{s.rcvr, argv, replyv})
	// The return value for the method is an error.
	errInter := returnValues[0].Interface()
	errmsg := ""
	if errInter != nil {
		errmsg = errInter.(error).Error()
	}
	pair.sendResponse(reqMsg, req, replyv.Interface(), codec, errmsg)
	pair.Server.freeRequest(req)
}

func (p *rwPair) sendResponse(reqMsg *Message, req *Request, reply any, codec ServerCodec, errmsg string) {

	//vv("pair sendResponse() top")

	resp := p.Server.getResponse()
	// Encode the response header
	resp.ServiceMethod = req.ServiceMethod
	if errmsg != "" {
		resp.Error = errmsg
		reply = invalidRequest
	}
	resp.Seq = req.Seq
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
	isRPC := true
	isLeg2 := true

	mid := NewHDR(from, to, subject, isRPC, isLeg2)

	// We are able to match call and response rigourously on the CallID alone.
	mid.CallID = reqCallID
	mid.Seqno = replySeqno
	mid.IsNetRPC = true
	msg.HDR = *mid

	by := p.encBuf.Bytes()
	msg.JobSerz = make([]byte, len(by))
	copy(msg.JobSerz, by)

	select {
	case p.SendCh <- msg:
		//vv("reply msg went over pair.SendCh to the send goro write loop: '%v'", msg)
	case <-p.halt.ReqStop.Chan:
		return
	}

}

// from net/rpc Server.readRequest
func (p *rwPair) readRequest(codec ServerCodec) (service *service, mtype *methodType, req *Request, argv, replyv reflect.Value, keepReading bool, err error) {
	//vv("pair readRequest() top")

	service, mtype, req, keepReading, err = p.readRequestHeader(codec)
	// err can legit be: rpc: can't find method Arith.BadOperation
	// if a method is not found, so do not panic on err here.
	if err != nil {
		if !keepReading {
			return
		}
		// discard body
		codec.ReadRequestBody(nil)
		return
	}

	// Decode the argument value.
	argIsValue := false // if true, need to indirect before calling.
	if mtype.ArgType.Kind() == reflect.Pointer {
		argv = reflect.New(mtype.ArgType.Elem())
	} else {
		argv = reflect.New(mtype.ArgType)
		argIsValue = true
	}
	// argv guaranteed to be a pointer now.
	if err = codec.ReadRequestBody(argv.Interface()); err != nil {
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

func (p *rwPair) readRequestHeader(codec ServerCodec) (svc *service, mtype *methodType, req *Request, keepReading bool, err error) {
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
		// re error: rpc: server cannot decode request: gob: duplicate type received.
		// fixed by clearing client encBuf before each new gob encoding.
		//
		// per https://www.reddit.com/r/golang/comments/ucmbmu/gob_duplicate_types_received/
		//
		// "You have to use one Encoder for one stream!
		//  The error suggests that you write to the file with
		//  several Encoders, so the Decoder meets the same type
		//  two times (gob encodes the type information once per
		//  stream, so the Decoder wants to meet a type description only one time)
		//
		// "You have to use gob.Encoder and gob.Decoder in pair, Decode all the stream that
		//  has been Encoded with one Encoder, with one Decoder.
		//	Either Encode/Decode each and every object (struct) separately
		//  (use a separate Encoder/Decoder for each value), or use one
		//  Encoder/Decoder for one stream (file)."
		//
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
	svci, ok := p.Server.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("rpc: can't find service " + req.ServiceMethod)
		return
	}
	svc = svci.(*service)
	mtype = svc.method[methodName]
	if mtype == nil {
		err = errors.New("rpc: can't find method " + req.ServiceMethod)
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
func (s *Server) Register(rcvr any) error {
	return s.register(rcvr, "", false)
}

// RegisterName is like [Register] but uses the provided name for the type
// instead of the receiver's concrete type.
func (s *Server) RegisterName(name string, rcvr any) error {
	return s.register(rcvr, name, true)
}
func (s *Server) register(rcvr any, name string, useName bool) error {

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

	if len(svc.method) == 0 {
		str := ""

		// To help the user, see if a pointer receiver would work.
		method := suitableMethods(reflect.PointerTo(svc.typ), false)
		if len(method) != 0 {
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
	// our parent Server
	Server *Server

	// copy of Server.cfg for convenience
	cfg *Config

	Conn   net.Conn
	SendCh chan *Message

	halt *idem.Halter

	// net/rpc api
	gobCodec *gobServerCodec
	//sending  sync.Mutex
	encBuf  bytes.Buffer // target for codec writes: encode gobs into here first
	encBufW *bufio.Writer
	decBuf  bytes.Buffer // target for code reads.

}

func (s *Server) newRWPair(conn net.Conn) *rwPair {

	p := &rwPair{
		cfg:    s.cfg,
		Server: s,
		Conn:   conn,
		SendCh: make(chan *Message, 10),
		halt:   idem.NewHalter(),
	}
	p.encBufW = bufio.NewWriter(&p.encBuf)
	p.gobCodec = &gobServerCodec{
		rwc:    nil,
		dec:    gob.NewDecoder(&p.decBuf),
		enc:    gob.NewEncoder(p.encBufW),
		encBuf: p.encBufW,
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

	from := local(pair.Conn)
	to := remote(pair.Conn)
	isRPC := false
	isLeg2 := false
	subject = fmt.Sprintf("srv.SendMessage('%v')", subject)

	mid := NewHDR(from, to, subject, isRPC, isLeg2)
	mid.CallID = callID
	mid.Seqno = seqno
	msg.HDR = *mid

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
		halt:              idem.NewHalter(),
		RemoteConnectedCh: make(chan string, 20),
	}
}

func (s *Server) Register2Func(callme2 TwoWayFunc) {
	s.mut.Lock()
	defer s.mut.Unlock()
	s.callme2 = callme2
}

func (s *Server) Register1Func(callme1 OneWayFunc) {
	//vv("Register1Func called with callme1 = %p", callme1)
	s.mut.Lock()
	defer s.mut.Unlock()
	s.callme1 = callme1
}

func (s *Server) Start() (serverAddr net.Addr, err error) {
	//vv("Server.Start() called")
	if s.cfg == nil {
		s.cfg = NewConfig()
	}
	if s.cfg.ServerAddr == "" {
		panic(fmt.Errorf("no ServerAddr specified in Server.cfg"))
		//hostport := "127.0.0.1:0" // default to safe loopback
		//AlwaysPrintf("Server.Start(): warning: nil config or no ServerAddr specified, binding to '%v'", hostport)
		//s.cfg.ServerAddr = hostport
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
	s.lsn.Close() // cause RunServerMain listening loop to exit.
	s.mut.Unlock()
	<-s.halt.Done.Chan
	return nil
}
