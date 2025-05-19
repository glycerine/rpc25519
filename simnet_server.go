package rpc25519

import (
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	//"sync/atomic"
	"time"

	"github.com/glycerine/idem"
)

func (s *Server) runSimNetServer(serverAddr string, boundCh chan net.Addr, simNetConfig *SimNetConfig) {
	//vv("top of runSimnetServer, serverAddr = '%v'; name='%v'", serverAddr, s.name)
	defer func() {
		r := recover()
		//vv("defer running, end of runSimNetServer() for '%v' r='%v'", s.name, r)
		s.halt.ReqStop.Close()
		s.halt.Done.Close()
		//vv("exiting Server.runSimNetServer('%v')", serverAddr) // seen, yes, on shutdown test.
		if r != nil {
			panic(r)
		}
	}()

	simnet, serverNewConnCh, netAddr, err := s.bootAndRegisterSimNetServer(serverAddr, simNetConfig)
	if err != nil {
		return
	}

	defer func() {
		if simnet != nil && s.simnode != nil {
			const wholeHost = true
			simnet.alterNode(s.simnode, SHUTDOWN, wholeHost)
			//vv("simnet.alterNode(s.simnode, SHUTDOWN) done for %v", s.name)
		}
	}()

	if boundCh != nil {
		// avoid client/server races by giving userland test
		// a copy of the address rather than the same.
		cp := *netAddr
		externalizedNetAddr := &cp

		select {
		case boundCh <- externalizedNetAddr: // not  netAddr
			// like the srv comment, this exception to
			// using the simnet TimeAfter is fine,
			// as obvious it gets created just below and
			// the server is not up yet.
		case <-time.After(100 * time.Millisecond):
		}
	}

	for {
		select { // wait for a new client to connect
		case conn := <-serverNewConnCh:
			//s.simnode = conn.local

			//vv("%v simnet server got new conn '%#v', about to start read/send loops", netAddr, conn) // not seen
			pair := s.newRWPair(conn)
			go pair.runSendLoop(conn)
			go pair.runReadLoop(conn)

		case <-s.halt.ReqStop.Chan:
			return
		}
	}
}

// bootAndRegisterSimNetServer is the
// the core helper for getting a simnet server
// started. It returns normally after
// booting the simnet and registering s.
//
// This is used by runSimNetServer() above
// and by Listen() below. Listen is a super thin
// wrapper around it.
func (s *Server) bootAndRegisterSimNetServer(serverAddr string, simNetConfig *SimNetConfig) (simnet *simnet, serverNewConnCh chan *simnetConn, netAddr *SimNetAddr, err error) {
	//vv("top of runSimnetServer, serverAddr = '%v'; name='%v'", serverAddr, s.name)

	// satisfy uConn interface; don't crash cli/tests that check
	netAddr = &SimNetAddr{network: "simnet", addr: serverAddr, name: s.name, isCli: false}

	s.mut.Lock()
	AliasRegister(serverAddr, serverAddr+" (simnet_server: "+s.name+")")
	s.mut.Unlock()

	// idempotent, so all new servers can try;
	// only the first will boot it up (still pass s for s.halt);
	// second and subsequent will get back
	// the cfg.simnetRendezvous.singleSimnet
	// per config shared simnet.
	simnet = s.cfg.bootSimNetOnServer(simNetConfig, s)

	// sets s.simnode, s.simnet, s.netAddr
	serverNewConnCh, err = simnet.registerServer(s, netAddr)
	if err != nil {
		if err == ErrShutdown2 {
			//vv("simnet_server sees shutdown in progress")
			return
		}
		panicOn(err)
	}
	if serverNewConnCh == nil {
		panic(fmt.Sprintf("%v got a nil serverNewConnCh, should not be allowed!", s.name))
	}

	s.mut.Lock() // avoid data races
	addrs := netAddr.Network() + "://" + netAddr.String()
	s.boundAddressString = addrs
	s.simNetAddr = netAddr
	AliasRegister(addrs, addrs+" (server: "+s.name+")")
	s.mut.Unlock()
	return
}

// a connection between two nodes.
// implements uConn, see simnet_server.go
// the readMessage/sendMessage are well tested;
// the other net.Conn generic Read/Write less so, at the moment.
type simnetConn struct {
	mut sync.Mutex

	// distinguish cli from srv
	isCli   bool
	net     *simnet
	netAddr *SimNetAddr // local address

	local  *simnode
	remote *simnode

	readDeadlineTimer *mop
	sendDeadlineTimer *mop

	nextRead []byte

	// no more reads, but serve the rest of nextRead.
	localClosed *idem.IdemCloseChan
	// like EOF or TPC RST has been sent.
	remoteClosed *idem.IdemCloseChan

	// probability of local read fault, in [0,1].
	// 1 means no reads will be obtained.
	// 0 means normal operation, not deaf to reads (default).
	// The other end can still be dropping their sends; see dropSend.
	deafRead float64

	// probability of local send fault, in [0,1].
	// 1 means all sends dropped. No sends go out.
	// 0 means normal operation, not dropping sends (default).
	// The other end can still be deaf to the send; see deafRead.
	dropSend float64
}

func newSimnetConn() *simnetConn {
	return &simnetConn{
		localClosed:  idem.NewIdemCloseChan(),
		remoteClosed: idem.NewIdemCloseChan(),
	}
}

// Write implements io.Writer.
func (s *simnetConn) Write(p []byte) (n int, err error) {
	s.mut.Lock()
	defer s.mut.Unlock()

	if s.localClosed.IsClosed() {
		err = &simconnError{
			desc: "use of closed network connection",
		}
		return
	}

	if len(p) == 0 {
		return
	}

	msg := NewMessage()
	n = len(p)
	if n > UserMaxPayload {
		n = UserMaxPayload
		// copy into the "kernel buffer"
		msg.JobSerz = append([]byte{}, p[:n]...)
	} else {
		msg.JobSerz = append([]byte{}, p...)
	}

	var sendDead chan time.Time
	if s.sendDeadlineTimer != nil {
		sendDead = s.sendDeadlineTimer.timerC
	}

	return s.msgWrite(msg, sendDead, n)
}

// helper for Write. s.mut must be held locked during.
func (s *simnetConn) msgWrite(msg *Message, sendDead chan time.Time, n0 int) (n int, err error) {

	n = n0
	isCli := s.isCli

	send := newSendMop(msg, isCli)
	send.origin = s.local
	send.sendFileLine = fileLine(2)
	send.target = s.remote
	send.initTm = time.Now()

	isEOF := msg.EOF
	if isEOF {
		send.isEOF_RST = true
	}

	//vv("top simnet.Write(%v) (isEOF_RST: %v) from %v at %v to %v", string(msg.JobSerz), send.isEOF_RST, send.origin.name, send.sendFileLine, send.target.name) // RACEY! comment out before go test -race

	select {
	case s.net.msgSendCh <- send:
	case <-s.net.halt.ReqStop.Chan:
		n = 0
		err = ErrShutdown()
		return
	case timeout := <-sendDead:
		_ = timeout
		n = 0
		err = &simconnError{isTimeout: true, desc: "i/o timeout"}
		return
	case <-s.localClosed.Chan:
		n = 0
		err = io.EOF
		return
		//case <-s.remoteClosed.Chan:
		//	n = 0
		//	err = io.EOF
		//	return
	}

	//vv("net has it (isEOF:%v), about to wait for proceed... simnetConn.Write('%v') isCli=%v, origin=%v ; target=%v;", isEOF, string(send.msg.JobSerz), s.isCli, send.origin.name, send.target.name)  // RACEY! comment out before go test -race

	if isEOF {
		return 0, nil // don't expect a reply from EOF/RST
	}

	select {
	case <-send.proceed:
		return
	case <-s.net.halt.ReqStop.Chan:
		n = 0
		err = ErrShutdown()
		return
	case timeout := <-sendDead:
		_ = timeout
		n = 0
		err = &simconnError{isTimeout: true, desc: "i/o timeout"}
		return
	case <-s.localClosed.Chan:
		n = 0
		err = io.EOF
		return
		//case <-s.remoteClosed.Chan:
		//	n = 0
		//	err = io.EOF
		//	return
	}
	return
}

// Read implements io.Reader.
func (s *simnetConn) Read(data []byte) (n int, err error) {

	if len(data) == 0 {
		return
	}

	s.mut.Lock()
	defer s.mut.Unlock()

	// leftovers?
	if len(s.nextRead) > 0 {
		n = copy(data, s.nextRead)
		s.nextRead = s.nextRead[n:]
		return
	}

	if s.localClosed.IsClosed() {
		//vv("Read at %v: local is already closed", s.local.name)
		err = io.EOF
		return
	}

	isCli := s.isCli

	var readDead chan time.Time
	if s.readDeadlineTimer != nil {
		readDead = s.readDeadlineTimer.timerC
	}

	//vv("top simnet.readMessage() %v READ", read.origin)

	read := newReadMop(isCli)
	read.initTm = time.Now()
	read.origin = s.local
	read.readFileLine = fileLine(2)
	read.target = s.remote

	//vv("in simnetConn.Read() isCli=%v, origin=%v at %v; target=%v", s.isCli, read.origin.name, read.readFileLine, read.target.name)

	select {
	case s.net.msgReadCh <- read:
	case <-s.net.halt.ReqStop.Chan:
		err = ErrShutdown()
		return
	case timeout := <-readDead:
		_ = timeout
		err = os.ErrDeadlineExceeded
		//err = &simconnError{isTimeout: true, desc: "i/o timeout"}
		return
	case <-s.localClosed.Chan:
		//vv("local side was closed before Read submitted")
		err = io.EOF
		return

		// comment out so we don't shutdown before getting our read
		//case <-s.remoteClosed.Chan:
		//	err = io.EOF
		//	return
	}
	select {
	case <-read.proceed:
		msg := read.msg
		n = copy(data, msg.JobSerz)
		if n < len(msg.JobSerz) {
			// buffer the leftover
			s.nextRead = append(s.nextRead, msg.JobSerz[n:]...)
		}
		//vv("Read on '%v' got '%v'; eof: %v", s.local.name, string(data[:n]), read.isEOF_RST)
		if read.isEOF_RST {
			//vv("read has EOF mark! on read at %v from %v", s.local.name, s.remote.name) // seen
			err = io.EOF
			s.remoteClosed.Close() // for sure?
			s.localClosed.Close()  // this too, maybe?
		}

	case <-s.net.halt.ReqStop.Chan:
		err = ErrShutdown()
		return
	case timeout := <-readDead:
		_ = timeout
		err = os.ErrDeadlineExceeded
		//err = &simconnError{isTimeout: true, desc: "i/o timeout"}
		return
	case <-s.localClosed.Chan:
		//vv("local side was closed waiting for proceed")
		err = io.EOF
		return

		// as above, get our read even if other side has shutdown.
		//case <-s.remoteClosed.Chan:
		//	err = io.EOF
		//	return
	}
	return
}

func (s *simnetConn) Close() error {
	// only close local, might still be bytes to read on other end.

	// send the EOF message
	m := NewMessage()
	m.EOF = true
	//vv("Close sending EOF in msgWrite, on %v", s.local.name)
	s.msgWrite(m, nil, 0) // nil send-deadline channel for now. TODO improve?

	s.localClosed.Close()
	return nil
}

func (s *simnetConn) LocalAddr() net.Addr {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.local.netAddr
}

func (s *simnetConn) RemoteAddr() net.Addr {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.remote.netAddr
}

func (s *simnetConn) SetDeadline(t time.Time) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	if t.IsZero() {
		if s.readDeadlineTimer != nil {
			s.net.discardTimer(s.local, s.readDeadlineTimer, time.Now())
		}
		s.readDeadlineTimer = nil
		if s.sendDeadlineTimer != nil {
			s.net.discardTimer(s.local, s.sendDeadlineTimer, time.Now())
		}
		s.sendDeadlineTimer = nil
		return nil
	}
	now := time.Now()
	dur := t.Sub(now)
	s.readDeadlineTimer = s.net.createNewTimer(s.local, dur, now, s.isCli)
	s.sendDeadlineTimer = s.readDeadlineTimer
	return nil
}

func (s *simnetConn) SetWriteDeadline(t time.Time) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	if t.IsZero() {
		if s.sendDeadlineTimer != nil {
			s.net.discardTimer(s.local, s.sendDeadlineTimer, time.Now())
		}
		s.sendDeadlineTimer = nil
		return nil
	}
	now := time.Now()
	dur := t.Sub(now)
	s.sendDeadlineTimer = s.net.createNewTimer(s.local, dur, now, s.isCli)
	return nil
}
func (s *simnetConn) SetReadDeadline(t time.Time) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	if t.IsZero() {
		if s.readDeadlineTimer != nil {
			s.net.discardTimer(s.local, s.sendDeadlineTimer, time.Now())
		}
		s.readDeadlineTimer = nil
		return nil
	}
	now := time.Now()
	dur := t.Sub(now)
	s.readDeadlineTimer = s.net.createNewTimer(s.local, dur, now, s.isCli)
	return nil
}

// implements net.Error interface, which
// net.Conn operations return; for Timeout() especially.
type simconnError struct {
	isTimeout bool
	desc      string
}

func (s *simconnError) Error() string {
	return s.desc
}
func (s *simconnError) Timeout() bool {
	return s.isTimeout
}
func (s *simconnError) Temporary() bool {
	return s.isTimeout
}

// =============================================
// By Implementing the net.Listener interface,
// as gosimnet does, we provide a
// net.Conn oriented altenative to srv.Start();
// namely srv.Listen() and srv.Accept() --
// warning! they ONLY WORK ON simnet! These
// are NOT for regular actual network connections.
// =============================================

var _ net.Listener = &Server{}

// Listen currently ignores the network and addr strings,
// which are there to match the net.Listen method.
// The addr will be the name set on NewServer(name).
func (s *Server) Listen(network, addr string) (lsn net.Listener, err error) {
	// start the server, first server boots the network,
	// but it can continue even if the server is shutdown.

	simnet, serverNewConnCh, netAddr, err :=
		s.bootAndRegisterSimNetServer(s.name, &s.cfg.SimNetConfig)

	_, _, _ = simnet, serverNewConnCh, netAddr
	if err != nil {
		return nil, err
	}
	lsn = s
	return
}

// Accept is part of the net.Listener interface.
// Accept waits for and returns the next connection
// from a Client.
// You should call Server.Listen() to start the Server
// before calling Accept() on the Listener
// interface returned. Currently the Server is also
// the Listener, but following the net
// package's use convention allows us flexibility
// to change this in the future if need be.
func (s *Server) Accept() (nc net.Conn, err error) {
	select {
	case nc = <-s.simnode.tellServerNewConnCh:
		if isNil(nc) {
			err = ErrShutdown()
			return
		}
		//vv("Server.Accept returning nc = '%#v'", nc.(*simnetConn))
	case <-s.halt.ReqStop.Chan:
		err = ErrShutdown()
	}
	return
}

// Addr is a method on the net.Listener interface
// for obtaining the Server's locally bound
// address.
func (s *Server) Addr() (a net.Addr) {
	s.mut.Lock()
	defer s.mut.Unlock()
	// avoid data race
	cp := *s.simNetAddr
	return &cp
}

// Dial connects a Client to a Server. This is ONLY for simnet!
// We will panic if not c.cfg.UseSimNet. Regular
// RPC and peer/circuit must use Client.Start() as usual.
func (c *Client) Dial(network, address string) (nc net.Conn, err error) {

	if !c.cfg.UseSimNet {
		panic("Client.Dial is only for simnet.")
	}
	//vv("Client.DialSimnet called with local='%v', server='%v'", c.name, address)

	// false => no read/send loops
	err = c.runSimNetClient(c.name, address, false)

	select {
	case <-c.connected:
		nc = c.simconn
		return
	case <-c.halt.ReqStop.Chan:
		err = ErrShutdown()
		return
	}
	return
}
