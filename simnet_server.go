package rpc25519

import (
	"fmt"
	"io"
	"net"
	"sync"
	//"sync/atomic"
	"time"

	"github.com/glycerine/idem"
)

func (s *Server) runSimNetServer(serverAddr string, boundCh chan net.Addr, simNetConfig *SimNetConfig) {
	vv("top of runSimnetServer, serverAddr = '%v'; name='%v'", serverAddr, s.name)
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

	// idempotent, so all new servers can try;
	// only the first will boot it up (still pass s for s.halt);
	// second and subsequent will get back the cfg.simnetRendezvous.singleSimnet
	// per config shared simnet.
	simnet := s.cfg.bootSimNetOnServer(simNetConfig, s)

	// sets s.simnode, s.simnet
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
	AliasRegister(addrs, addrs+" (server: "+s.name+")")
	s.mut.Unlock()

	if boundCh != nil {
		select {
		case boundCh <- netAddr:
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
	isClosed bool
	closed   *idem.IdemCloseChan
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

	if s.isClosed {
		err = &simconnError{
			desc: "use of closed network connection",
		}
		return
	}

	if len(p) == 0 {
		return
	}

	isCli := s.isCli
	msg := NewMessage()
	n = len(p)
	if n > UserMaxPayload {
		n = UserMaxPayload
		// copy into the "kernel buffer"
		msg.JobSerz = append([]byte{}, p[:n]...)
	} else {
		msg.JobSerz = append([]byte{}, p...)
	}

	//vv("top simnet.sendMessage() %v SEND  msg.Serial=%v", send.origin, msg.HDR.Serial)
	//vv("sendMessage\n conn.local = %v (isCli:%v)\n conn.remote = %v (isCli:%v)\n", s.local.name, s.local.isCli, s.remote.name, s.remote.isCli)
	send := newSendMop(msg, isCli)
	send.origin = s.local
	send.target = s.remote
	send.initTm = time.Now()
	select {
	case s.net.msgSendCh <- send:
	case <-s.net.halt.ReqStop.Chan:
		n = 0
		err = ErrShutdown()
		return
	case timeout := <-s.sendDeadlineTimer.timerC:
		_ = timeout
		n = 0
		err = &simconnError{isTimeout: true, desc: "i/o timeout"}
		return
	case <-s.closed.Chan:
		err = io.EOF
		return
	}
	select {
	case <-send.proceed:
		return
	case <-s.net.halt.ReqStop.Chan:
		n = 0
		err = ErrShutdown()
		return
	case timeout := <-s.sendDeadlineTimer.timerC:
		_ = timeout
		n = 0
		err = &simconnError{isTimeout: true, desc: "i/o timeout"}
		return
	case <-s.closed.Chan:
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

	if s.isClosed {
		err = io.EOF
		return
	}

	isCli := s.isCli

	//vv("top simnet.readMessage() %v READ", read.origin)

	read := newReadMop(isCli)
	read.initTm = time.Now()
	read.origin = s.local
	read.target = s.remote
	select {
	case s.net.msgReadCh <- read:
	case <-s.net.halt.ReqStop.Chan:
		err = ErrShutdown()
		return
	case timeout := <-s.readDeadlineTimer.timerC:
		_ = timeout
		err = &simconnError{isTimeout: true, desc: "i/o timeout"}
		return
	case <-s.closed.Chan:
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
	case timeout := <-s.readDeadlineTimer.timerC:
		_ = timeout
		err = &simconnError{isTimeout: true, desc: "i/o timeout"}
		return
	case <-s.closed.Chan:
		err = io.EOF
		return
	}
	return
}

func (s *simnetConn) Close() error {
	s.mut.Lock()
	defer s.mut.Unlock()

	s.isClosed = true
	s.closed.Close() // unblock sends/reads
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

func (s *simnet) Dial(localHostPort, serverAddr string) *simnetConn {

	// so something like
	// runSimNetClient(localHostPort, serverAddr string)
	// in simnet_client.go

	//
	// currently the simnet.go:413 :433 methods
	//
	// addEdgeFromSrv(srvnode, clinode *simnode) *simnetConn
	// and
	// addEdgeFromCli(clinode, srvnode *simnode) *simnetConn
	//
	// are how we make new simnetConn; but they are
	// internal not external.
	// We'll wait until we have actual client code to figure
	// out what is actually required.
	panic("TODO Dial for clients")
	return nil
}

func (s *simnet) Listen() *simnetConn {
	// see comments in Dial above. wait for actual client code.

	// s.bootSimNetOnServer(simNetConfig *SimNetConfig, srv *Server) *simnet
	// will be needed.
	// then a more standalone version of the simnet.go:1540  code
	// s.registerServer(srv *Server, srvNetAddr *SimNetAddr) (newCliConnCh chan *simnetConn, err error)

	panic("TODO Listen for servers")
	return nil
}
