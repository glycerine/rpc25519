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

	s.mut.Lock()
	AliasRegister(serverAddr, serverAddr+" (simnet_server: "+s.name+")")
	s.mut.Unlock()

	// satisfy uConn interface; don't crash cli/tests that check
	netAddr := &SimNetAddr{network: "simnet", addr: serverAddr, name: s.name, isCli: false}
	// avoid client/server races by giving userland test
	// a copy of the address rather than the same.
	cp := *netAddr
	externalizedNetAddr := &cp

	// idempotent, so all new servers can try;
	// only the first will boot it up (still pass s for s.halt);
	// second and subsequent will get back the cfg.simnetRendezvous.singleSimnet
	// per config shared simnet.
	simnet := s.cfg.bootSimNetOnServer(simNetConfig, s)

	// sets s.simnode, s.simnet, s.netAddr
	serverNewConnCh, err := simnet.registerServer(s, netAddr)
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

	defer func() {
		simnet.alterNode(s.simnode, SHUTDOWN)
		//vv("simnet.alterNode(s.simnode, SHUTDOWN) done for %v", s.name)
	}()

	s.mut.Lock() // avoid data races
	addrs := netAddr.Network() + "://" + netAddr.String()
	s.boundAddressString = addrs
	s.simNetAddr = netAddr
	AliasRegister(addrs, addrs+" (server: "+s.name+")")
	s.mut.Unlock()

	if boundCh != nil {
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
	// no more reads, but serve the rest of nextRead.
	localClosed  *idem.IdemCloseChan
	remoteClosed *idem.IdemCloseChan
}

func newSimnetConn() *simnetConn {
	return &simnetConn{
		localClosed:  idem.NewIdemCloseChan(),
		remoteClosed: idem.NewIdemCloseChan(),
	}
}

// originally not actually used much by simnet. We'll
// fill them out to try and allow testing of net.Conn code.
// doc:
// "Write writes len(p) bytes from p to the
// underlying data stream. It returns the number
// of bytes written from p (0 <= n <= len(p)) and
// any error encountered that caused the write to
// stop early. Write must return a non-nil error
// if it returns n < len(p). Write must not modify
// the slice data, even temporarily.
// Implementations must not retain p."
//
// Implementation note: we will only send
// UserMaxPayload bytes at a time.
func (s *simnetConn) Write(p []byte) (n int, err error) {
	s.mut.Lock()
	defer s.mut.Unlock()

	isCli := s.isCli

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

	//vv("top simnet.sendMessage() %v SEND  msg.Serial=%v", send.origin, msg.HDR.Serial)
	//vv("sendMessage\n conn.local = %v (isCli:%v)\n conn.remote = %v (isCli:%v)\n", s.local.name, s.local.isCli, s.remote.name, s.remote.isCli)
	send := newSendMop(msg, isCli)
	send.origin = s.local
	send.sendFileLine = fileLine(3)
	send.target = s.remote
	send.initTm = time.Now()

	//vv("top simnet.Write(%v) from %v at %v to %v", string(msg.JobSerz), send.origin.name, send.sendFileLine, send.target.name)

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
	case <-s.remoteClosed.Chan:
		n = 0
		err = io.EOF
		return
	}

	//vv("net has it, about to wait for proceed... simnetConn.Write('%v') isCli=%v, origin=%v ; target=%v;", string(send.msg.JobSerz), s.isCli, send.origin.name, send.target.name)

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
	case <-s.remoteClosed.Chan:
		n = 0
		err = io.EOF
		return
	}
	return
}

// doc:
// "When Read encounters an error or end-of-file
// condition after successfully reading n > 0 bytes,
// it returns the number of bytes read. It may
// return the (non-nil) error from the same call
// or return the error (and n == 0) from a subsequent call.
// An instance of this general case is that a
// Reader returning a non-zero number of bytes
// at the end of the input stream may return
// either err == EOF or err == nil. The next
// Read should return 0, EOF.
// ...
// "If len(p) == 0, Read should always
// return n == 0. It may return a non-nil
// error if some error condition is known,
// such as EOF.
//
// "Implementations of Read are discouraged
// from returning a zero byte count with a nil
// error, except when len(p) == 0. Callers should
// treat a return of 0 and nil as indicating that
// nothing happened; in particular it
// does not indicate EOF.
//
// "Implementations must not retain p."
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

	//vv("in simnetConn.Read() isCli=%v, origin=%v at %v; target=%v", s.isCli, read.origin.name, read.fileLine, read.target.name)

	select {
	case s.net.msgReadCh <- read:
	case <-s.net.halt.ReqStop.Chan:
		err = ErrShutdown()
		return
	case timeout := <-s.readDeadlineTimer.timerC:
		_ = timeout
		err = &simconnError{isTimeout: true, desc: "i/o timeout"}
		return
	case timeout := <-readDead:
		_ = timeout
		err = os.ErrDeadlineExceeded
		//err = &simconnError{isTimeout: true, desc: "i/o timeout"}
		return
	case <-s.localClosed.Chan:
		err = io.EOF
		return

		// TODO: implement an EOF "message" sent
		// after the last send...instead of assuming omniscience
		// I think we want reads to finish first?
	case <-s.remoteClosed.Chan:
		err = io.EOF
		return
	}
	select {
	case <-read.proceed:
		msg := read.msg
		n = copy(data, msg.JobSerz)
		if n < len(msg.JobSerz) {
			// buffer the leftover
			s.nextRead = append(s.nextRead, msg.JobSerz[n:]...)
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
		err = io.EOF
		return
	// TODO: implement an EOF message instead of assuming omniscience
	case <-s.remoteClosed.Chan:
		err = io.EOF
		return
	}
	return
}

func (s *simnetConn) Close() error {
	// only close local, might still be bytes to read on other end.
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
		s.readDeadlineTimer = nil
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

// ============ from gosimnet version

var _ net.Listener = &Server{}

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

// Listen currently ignores the network and addr strings,
// which are there to match the net.Listen method.
// The addr will be the name set on NewServer(name).
func (s *Server) Listen(network, addr string) (lsn net.Listener, err error) {
	// start the server, first server boots the network,
	// but it can continue even if the server is shutdown.
	addrCh := make(chan net.Addr, 1)
	s.runSimNetServer(s.name, addrCh, nil)
	lsn = s
	var netAddr *SimNetAddr
	select {
	case netAddrI := <-addrCh:
		netAddr = netAddrI.(*SimNetAddr)
	case <-s.halt.ReqStop.Chan:
		err = ErrShutdown()
	}
	_ = netAddr
	return
}
