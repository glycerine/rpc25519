package rpc25519

// cli.go: simple TCP client, with TLS encryption.

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	cryrand "crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/glycerine/idem"
	"github.com/glycerine/rpc25519/selfcert"
	"github.com/quic-go/quic-go"
)

var _ = cryrand.Read

type localRemoteAddr interface {
	RemoteAddr() net.Addr
	LocalAddr() net.Addr
}

type uConnLR interface {
	uConn
	localRemoteAddr
}

var _ quic.Connection

var sep = string(os.PathSeparator)

// eg. serverAddr = "localhost:8443"
// serverAddr = "192.168.254.151:8443"
func (c *Client) runClientMain(serverAddr string, tcp_only bool, certPath string) {
	log.SetFlags(log.LstdFlags | log.Lshortfile) // Add Lshortfile for short file names

	defer func() {
		c.halt.Done.Close()

		if c.seenNetRPCCalls {
			c.netRpcShutdownCleanup(ErrShutdown)
		}
	}()

	c.cfg.checkPreSharedKey("client")

	if tcp_only {
		c.RunClientTCP(serverAddr)
		return
	}

	embedded := false                 // always false now
	sslCA := fixSlash("certs/ca.crt") // path to CA cert

	keyName := "client"
	if c.cfg.ClientKeyPairName != "" {
		keyName = c.cfg.ClientKeyPairName
	}

	sslCert := fixSlash(fmt.Sprintf("certs/%v.crt", keyName))    // path to server cert
	sslCertKey := fixSlash(fmt.Sprintf("certs/%v.key", keyName)) // path to server key

	if certPath != "" {
		embedded = false
		sslCA = fixSlash(fmt.Sprintf("%v/ca.crt", certPath))               // path to CA cert
		sslCert = fixSlash(fmt.Sprintf("%v/%v.crt", certPath, keyName))    // path to server cert
		sslCertKey = fixSlash(fmt.Sprintf("%v/%v.key", certPath, keyName)) // path to server key
	}

	config, err2 := LoadClientTLSConfig(embedded, sslCA, sslCert, sslCertKey)
	if err2 != nil {
		c.err = fmt.Errorf("error on LoadClientTLSConfig() (using embedded=%v): '%v'", embedded, err2)
		panic(c.err)
	}
	_ = err2 // skip panic: x509: certificate signed by unknown authority (possibly because of "crypto/rsa: verification error" while trying to verify candidate authority certificate "Cockroach CA")

	// without this ServerName assignment, we used to get (before gen.sh put in SANs using openssl-san.cnf)
	// 2019/01/04 09:36:18 failed to call: x509: cannot validate certificate for 127.0.0.1 because it doesn't contain any IP SANs
	//
	// update:
	// ServerName set to "localhost" is still needed in order to run the server on a different TCP host.
	// otherwise we get:
	// 2024/10/04 21:27:50 Failed to connect to server: tls: failed to verify certificate: x509: certificate is valid for 127.0.0.1, not 192.168.254.151
	//
	// docs:
	// "ServerName is the value of the Server Name Indication extension sent by
	// the client. It's available both on the server and on the client side."
	// See also: https://en.wikipedia.org/wiki/Server_Name_Indication
	//     and   https://www.ietf.org/archive/id/draft-ietf-tls-esni-18.html for ECH
	//
	// Basically this lets the client say which cert they want to talk to,
	// oblivious to whatever IP that the host is on, or the domain name of that
	// IP alone.
	config.ServerName = "localhost"

	if c.cfg.SkipVerifyKeys {
		config.InsecureSkipVerify = true // true would be insecure
	}

	if c.cfg.UseQUIC {
		if c.cfg.TCPonly_no_TLS {
			panic("cannot have both UseQUIC and TCPonly_no_TLS true")
		}
		localHostPort := c.cfg.ClientHostPort
		if localHostPort == "" {
			localHost, err := LocalAddrMatching(serverAddr)
			panicOn(err)
			//vv("localHost = '%v', matched to quicServerAddr = '%v'", localHost, quicServerAddr)
			localHostPort = localHost + ":0" // client can pick any port
		}
		c.RunQUIC(localHostPort, serverAddr, config)
		return
	}

	// Dial the server, with a timeout

	ctx := context.Background()
	if c.cfg.ConnectTimeout > 0 {
		// Create a context with a timeout
		ctx2, cancel := context.WithTimeout(ctx, c.cfg.ConnectTimeout)
		defer cancel() // Ensure cancel is called to release resources
		ctx = ctx2
	}

	// Use tls.Dialer to dial with the context
	d := &tls.Dialer{
		NetDialer: &net.Dialer{},
		Config:    config,
	}

	// Dial the server, with a timeout.
	nconn, err := d.DialContext(ctx, "tcp", serverAddr)
	if err != nil {
		c.err = err
		c.Connected <- err
		log.Printf("Failed to connect to server: %v", err)
		return
	}
	c.isTLS = true
	// do this before signaling on c.Connected, else tests will race and panic
	// not having a connection
	c.conn = nconn

	conn := nconn.(*tls.Conn) // docs say this is for sure.
	defer conn.Close()

	la := conn.LocalAddr()
	c.SetLocalAddr(la.Network() + "://" + la.String())

	// only signal ready once SetLocalAddr() is done, else submitter can crash.
	c.Connected <- nil

	//log.Printf("Connected to server %s", serverAddr)

	// possible to check host keys for TOFU like SSH does,
	// but be aware that if they have the contents of
	// certs/node.key that has the server key,
	// they can use that to impersonate the server and MITM the connection.
	// So protect both node.key and client.key from
	// distribution.
	knownHostsPath := "known_server_keys"
	// return error on host-key change.
	connState := conn.ConnectionState()
	raddr := conn.RemoteAddr()
	remoteAddr := strings.TrimSpace(raddr.String())

	if !c.cfg.SkipVerifyKeys {
		good, bad, wasNew, err := HostKeyVerifies(knownHostsPath, &connState, remoteAddr)
		_ = good
		_ = wasNew
		_ = bad
		if err != nil {
			fmt.Fprintf(os.Stderr, "HostKeyVerifies has failed: key failed list:'%#v': '%v'\n", bad, err)
			return
		}
		//for i := range good {
		//	vv("accepted identity for server: '%v' (was new: %v)\n", good[i], wasNew)
		//}
	}

	go c.RunSendLoop(conn)
	c.RunReadLoop(conn)
}

func (c *Client) RunClientTCP(serverAddr string) {

	// Dial the server
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		c.err = err
		c.Connected <- err
		log.Printf("Failed to connect to server: %v", err)
		return
	}

	la := conn.LocalAddr()
	c.SetLocalAddr(la.Network() + "://" + la.String())

	c.isTLS = false
	c.conn = conn

	c.Connected <- nil
	defer conn.Close()
	//log.Printf("Connected to server %s", serverAddr)

	go c.RunSendLoop(conn)
	c.RunReadLoop(conn)
}

func (c *Client) RunReadLoop(conn net.Conn) {
	defer func() {
		c.halt.ReqStop.Close()
		c.halt.Done.Close()
	}()

	//w := newWorkspace(maxMessage)
	w := newBlabber(c.cfg.preSharedKey, conn, c.cfg.encryptPSK, maxMessage)

	readTimeout := time.Millisecond * 100
	for {

		// shutting down?
		select {
		case <-c.halt.ReqStop.Chan:
			return
		default:
		}

		// Receive a message
		msg, err := w.readMessage(conn, &readTimeout)
		if err != nil {
			r := err.Error()
			if strings.Contains(r, "timeout") || strings.Contains(r, "deadline exceeded") {
				continue
			}
			// quic server specific
			if strings.Contains(r, "Application error 0x0 (remote)") {
				//vv("normal quic shutdown.")
				return
			}

			if strings.Contains(r, "server shutdown") {
				//vv("client sees quic server shutdown")
				return
			}
			if strings.Contains(r, "use of closed network connection") {
				return
			}
			if strings.Contains(r, "connection reset by peer") {
				return
			}
			if strings.Contains(r, "Application error 0x0 (local)") {
				return
			}
			if r == "EOF" && msg == nil {
				return
			}
			//vv("ignore err = '%v'; msg = '%v'", err, msg)
		}
		if msg == nil {
			continue
		}

		seqno := msg.HDR.Seqno
		//vv("client %v received message with seqno=%v, msg.HDR='%v'", c.name, seqno, msg.HDR.String())

		c.mut.Lock()
		whoCh, waiting := c.notifyOnce[seqno]
		//vv("notifyOnce waiting = %v", waiting)
		if waiting {
			delete(c.notifyOnce, seqno)

			select {
			case whoCh <- msg:
				//vv("client %v: yay. sent on notifyOnce channel! for seqno=%v", c.name, seqno)
			default:
				//vv("could not send to notifyOnce channel!")
			}
		} else {
			//vv("len c.notifyOnRead = %v", len(c.notifyOnRead))
			// assume the round-trip "calls" should be consumed,
			// and not repeated here to client listeners who want events???
			// trying to match what other RPC systems do.
			for _, ch := range c.notifyOnRead {
				select {
				case ch <- msg:
					//vv("client: %v: yay. sent on notifyOnRead channel: %p", c.name, ch)
				default:
					//vv("could not send to notifyOnRead channel!")
				}
			}
		}
		c.mut.Unlock()
	}
}

func (c *Client) RunSendLoop(conn net.Conn) {
	defer func() {
		c.halt.ReqStop.Close()
		c.halt.Done.Close()
	}()

	//w := newWorkspace(maxMessage)
	w := newBlabber(c.cfg.preSharedKey, conn, c.cfg.encryptPSK, maxMessage)

	// PRE: Message.DoneCh must be buffered at least 1, so our logic below does not have to deal with ever blocking.
	for {
		select {
		case <-c.halt.ReqStop.Chan:
			return
		case msg := <-c.oneWayCh:

			//vv("cli %v has had a one-way requested: '%v'", c.name, msg)

			// one-way always use seqno 0,
			// so we know that no follow up is expected.
			seqno := c.nextSeqno()
			msg.HDR.Seqno = seqno

			if msg.HDR.Nc == nil {
				// use default conn
				msg.HDR.Nc = conn
			}
			// Send the message
			if err := w.sendMessage(conn, msg, &c.cfg.WriteTimeout); err != nil {
				log.Printf("Failed to send message: %v", err)
				msg.Err = err
			} else {
				//vv("cli %v has sent a 1-way message: %v'", c.name, msg)
			}
			msg.DoneCh <- msg // convey the error or lack thereof.

		case msg := <-c.roundTripCh:

			seqno := c.nextSeqno()
			msg.HDR.Seqno = seqno

			//vv("cli %v has had a round trip requested: GetOneRead is registering for seqno=%v: '%v'", c.name, seqno, msg)
			c.GetOneRead(seqno, msg.DoneCh)

			if err := w.sendMessage(conn, msg, &c.cfg.WriteTimeout); err != nil {
				//vv("Failed to send message: %v", err)
				msg.Err = err
				msg.DoneCh <- msg
				continue
			} else {
				//vv("(client %v) Sent message: (seqno=%v): '%v'", c.name, msg.HDR.Seqno, msg)
			}

		}
	}
}

// interface for goq

func NewMessage() *Message {
	return &Message{
		// NOTE: buffer size must be at least 1, so our Client.RunSendLoop never blocks.
		// Thus we simplify the logic there, not requiring a ton of extra selects to
		// handle shutdown/timeout/etc.
		DoneCh: make(chan *Message, 1),
	}
}

func (msg *Message) String() string {
	return fmt.Sprintf("&Message{HDR:%v, Err:'%v'}", msg.HDR.String(), msg.Err)
}

func NewMessageFromBytes(by []byte) (msg *Message) {
	msg = NewMessage()
	msg.JobSerz = by
	return
}

// TwoWayFunc is the user's own function that they
// register with the server for remote procedure calls.
//
// The user's Func may not want to return anything.
// In that case they should register a OneWayFunc instead.
//
// req.JobSerz []byte contains the job payload.
//
// Implementers of TwoWayFunc should assign their
// return []byte to reply.JobSerz. reply.Jobserz can also
// be left nil, of course.
//
// Any errors can be returned on reply.JobErrs; this is optional.
// Not that JobErrs is a string value.
//
// The system will overwrite the reply.HDR field when sending the
// reply, so the user should not bother trying to alter it.
type TwoWayFunc func(req *Message, reply *Message) error

// OneWayFunc is the simpler sibling to the above.
// A OneWayFunc will not return anything to the sender.
//
// As above req.JobSerz [] byte contains the job payload.
type OneWayFunc func(req *Message)

// Config says who to contact (for a client), or
// where to listen (for a server); and sets how
// strong a security posture we adopt.
//
// Copying a Config is fine, but it should be a simple
// shallow copy to preserve the shared *SharedTransport struct.
//
// This shared pointer is the basis of port (and file handle) reuse where a single
// process can maintain a server and multiple clients
// in a "star" pattern. This only works with QUIC of course,
// and is one of the main reasons to use QUIC.
// The shared pointer is reference counted and the underlying
// net.UDPConn is only closed when the last instance
// in use is Close()-ed.
type Config struct {

	// ServerAddr host:port where the server should listen.
	ServerAddr string

	// optional. Can be used to suggest that the
	// client use a specific host:port. NB: For QUIC, by default, the client and
	// server will share the same port if they are in the same process.
	// In that case this setting will definitely be ignored.
	ClientHostPort string

	// Who the client should contact
	ClientDialToHostPort string

	// TCP false means TLS-1.3 secured. true here means do TCP only; with no encryption.
	TCPonly_no_TLS bool

	// UseQUIC cannot be true if TCPonly_no_TLS is true.
	UseQUIC bool

	// path to certs/ like certificate
	// directory on the live filesystem. If left
	// empty then the embedded certs/ from build-time, those
	// copied from the on-disk certs/ directory and baked
	// into the executable as a virtual file system with
	// the go:embed directive are used.
	CertPath string

	// SkipVerifyKeys true allows any incoming
	// key to be signed by
	// any CA; it does not have to be ours. Obviously
	// this discards almost all access control; it
	// should rarely be used unless communication
	// with the any random agent/hacker/public person
	// is desired.
	SkipVerifyKeys bool

	// This is not a Config option, but creating
	// the known key file on the client/server is
	// typically the last security measure in hardening.
	//
	// If known_client_keys exists on the server,
	// then we will read from it.
	// Likewise, if known_server_keys exists on
	// the client, then we will read from it.
	//
	// If the known keys file is read-only: Read-only
	// means we are in lockdown mode and no unknown
	// client certs will be accepted, even if they
	// have been properly signed by our CA.
	//
	// If the known keys file is writable then we are
	// Trust On First Use mode, and new remote parties
	// are recorded in the file if their certs are valid (signed
	// by us/our CA).
	//
	// Note if the known_client_keys is read-only, it
	// had better not be empty or nobody will be
	// able to contact us. The server will notice
	// this and crash since why bother being up.

	ClientKeyPairName string // default "client" means use certs/client.crt and certs/client.key
	ServerKeyPairName string // default "node" means use certs/node.crt and certs/node.key

	// PreSharedKeyPath locates an optional pre-shared
	// hex written in hex that must be 32 bytes (or more) long
	// (so 64 hex characters). Only the first 32 bytes will
	// be used to create a symmetric 2nd encryption layer.
	PreSharedKeyPath string

	preSharedKey [32]byte
	encryptPSK   bool

	// These are timeouts for connection and transport tuning.
	// The defaults of 0 mean wait forever.
	ConnectTimeout time.Duration
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration

	LocalAddress string

	// for port sharing between a server and 1 or more clients over QUIC
	shared *sharedTransport
}

func (cfg *Config) checkPreSharedKey(name string) {
	if cfg.PreSharedKeyPath != "" && fileExists(cfg.PreSharedKeyPath) {
		by, err := ioutil.ReadFile(cfg.PreSharedKeyPath)
		panicOn(err)
		if len(by) < 32 {
			panic(fmt.Sprintf("cfg.PreSharedKeyPath '%v' did not have 32 bytes of data in it.", cfg.PreSharedKeyPath))
		}
		copy(cfg.preSharedKey[:], by)
		cfg.encryptPSK = true
		AlwaysPrintf("activated pre-shared-key on '%v' from cfg.PreSharedKeyPath='%v'", name, cfg.PreSharedKeyPath)
	}
}

type sharedTransport struct {
	mut           sync.Mutex
	quicTransport *quic.Transport
	shareCount    int
	isClosed      bool
}

func NewConfig() *Config {
	return &Config{
		shared: &sharedTransport{},
	}
}

// Clients write requests, and maybe wait for responses.
type Client struct {
	cfg *Config
	mut sync.Mutex

	name string

	notifyOnRead []chan *Message
	notifyOnce   map[uint64]chan *Message

	conn     uConnLR
	quicConn quic.Connection

	isTLS  bool
	isQUIC bool

	oneWayCh    chan *Message
	roundTripCh chan *Message

	halt *idem.Halter

	// Connected lets the user wait for
	// handshake to complete. If connecting suceeds,
	// a nil will be sent on this chan, otherwise
	// the error will be provided.
	Connected chan error

	err error // detect inability to connect.

	lastSeqno uint64

	// net/rpc api implementation
	seenNetRPCCalls bool

	encBuf  bytes.Buffer // target for codec writes: encode gobs into here first
	encBufW *bufio.Writer
	decBuf  bytes.Buffer // target for code reads.

	codec ClientCodec

	reqMutex sync.Mutex // protects following
	request  Request

	mutex sync.Mutex // protects following

	pending  map[uint64]*Call
	closing  bool // user has called Close
	shutdown bool // server has told us to stop
}

// Compute HMAC using SHA-256
func computeHMAC(plaintext []byte, key []byte) (hash []byte) {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(plaintext))
	return h.Sum(nil)
}

// Go implements the net/rpc Client.Go() API; its docs:
//
// Go invokes the function asynchronously. It returns the [Call] structure representing
// the invocation. The done channel will signal when the call is complete by returning
// the same Call object. If done is nil, Go will allocate a new channel.
// If non-nil, done must be buffered or Go will deliberately crash.
func (c *Client) Go(serviceMethod string, args any, reply any, done chan *Call) *Call {
	c.mut.Lock()
	c.seenNetRPCCalls = true
	c.mut.Unlock()

	call := new(Call)
	call.ServiceMethod = serviceMethod
	call.Args = args
	call.Reply = reply
	if done == nil {
		done = make(chan *Call, 10) // buffered.
	} else {
		// If caller passes done != nil, it must arrange that
		// done has enough buffer for the number of simultaneous
		// RPCs that will be using that channel. If the channel
		// is totally unbuffered, it's best not to run at all.
		if cap(done) == 0 {
			log.Panic("rpc: done channel is unbuffered")
		}
	}
	call.Done = done
	//vv("Go() about to send()")
	c.send(call)
	//vv("Go() back from send()")
	return call
}

// Call implements the net/rpc Client.Call() API; its docs:
//
// Call invokes the named function, waits for it to complete, and returns its error status.
func (c *Client) Call(serviceMethod string, args any, reply any) error {
	c.mut.Lock()
	c.seenNetRPCCalls = true
	c.mut.Unlock()

	doneCh := make(chan *Call, 1)
	call := c.Go(serviceMethod, args, reply, doneCh)
	select {
	case call = <-doneCh:
		return call.Error
	case <-c.halt.ReqStop.Chan:
		return ErrShutdown
	}
}

func (c *Client) send(call *Call) {
	c.reqMutex.Lock()
	defer c.reqMutex.Unlock()

	// Register this call.
	c.mutex.Lock()
	if c.shutdown || c.closing {
		c.mutex.Unlock()
		call.Error = ErrIsShutdown
		call.done()
		return
	}

	seq := c.nextSeqno()
	c.pending[seq] = call
	c.mutex.Unlock()

	// Encode and send the request.

	c.encBuf.Reset()
	c.encBufW.Reset(&c.encBuf)

	c.request.Seq = seq
	c.request.ServiceMethod = call.ServiceMethod
	err := c.codec.WriteRequest(&c.request, call.Args)

	// should be in c.encBuf.Bytes() now
	//vv("Client.send(Call): c.encBuf.Bytes() is now len %v", len(c.encBuf.Bytes()))
	//vv("Client.send(Call): c.encBuf.Bytes() is now '%v'", string(c.encBuf.Bytes()))

	req := NewMessage()
	req.HDR.Subject = call.ServiceMethod
	req.HDR.IsNetRPC = true

	by := c.encBuf.Bytes()
	req.JobSerz = make([]byte, len(by))
	copy(req.JobSerz, by)

	reply, err := c.SendAndGetReply(req, nil)
	_ = reply
	//vv("got reply '%v'; err = '%v'", reply, err)

	if err == nil {
		err = c.gotNetRpcInput(reply)
	}

	if err != nil {
		c.mutex.Lock()
		call = c.pending[seq]
		delete(c.pending, seq)
		c.mutex.Unlock()
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

// like net/rpc Client.input()
func (c *Client) gotNetRpcInput(replyMsg *Message) (err error) {

	var response Response

	c.decBuf.Reset()
	c.decBuf.Write(replyMsg.JobSerz)

	err = c.codec.ReadResponseHeader(&response)
	panicOn(err)
	if err != nil {
		return err
	}
	seq := response.Seq
	c.mutex.Lock()
	call := c.pending[seq]
	delete(c.pending, seq)
	c.mutex.Unlock()

	switch {
	case call == nil:
		// We've got no pending call. That usually means that
		// WriteRequest partially failed, and call was already
		// removed; response is a server telling us about an
		// error reading request body. We should still attempt
		// to read error body, but there's no one to give it to.
		err = c.codec.ReadResponseBody(nil)
		if err != nil {
			err = errors.New("reading error body: " + err.Error())
		}
	case response.Error != "":
		// We've got an error response. Give this to the request;
		// any subsequent requests will get the ReadResponseBody
		// error if there is one.
		call.Error = ServerError(response.Error)
		err = c.codec.ReadResponseBody(nil)
		if err != nil {
			err = errors.New("reading error body: " + err.Error())
		}
		call.done()
	default:
		err = c.codec.ReadResponseBody(call.Reply)
		if err != nil {
			call.Error = errors.New("reading body " + err.Error())
		}
		call.done()
	}
	return nil
}

// any pending calls are unlocked with err set.
func (c *Client) netRpcShutdownCleanup(err error) {

	// Terminate pending calls.
	c.reqMutex.Lock()
	c.mutex.Lock()
	c.shutdown = true
	closing := c.closing
	if err == io.EOF {
		if closing {
			err = ErrIsShutdown
		} else {
			err = io.ErrUnexpectedEOF
		}
	}
	for _, call := range c.pending {
		call.Error = err
		call.done()
	}
	c.mutex.Unlock()
	c.reqMutex.Unlock()
	if debugLog && err != io.EOF && !closing {
		log.Println("rpc: client protocol error:", err)
	}
}

// original, not net/rpc derived below:

// Err returns any Client stored error.
func (c *Client) Err() error {
	return c.err
}

// GetReadIncomingCh creates and returns
// a buffered channel that reads incoming
// messages that are server-pushed (not associated
// with a round-trip rpc call request/response pair.
func (c *Client) GetReadIncomingCh() (ch chan *Message) {
	ch = make(chan *Message, 100)
	//vv("GetReadIncommingCh is %p on client '%v'", ch, c.name)
	c.GetReads(ch)
	return
}

// register to get any received messages on ch.
func (c *Client) GetReads(ch chan *Message) {
	//vv("GetReads called! stack='\n\n%v\n'", stack())
	if cap(ch) == 0 {
		panic("ch must be bufferred")
	}
	c.mut.Lock()
	defer c.mut.Unlock()
	c.notifyOnRead = append(c.notifyOnRead, ch)
}

// GetOneRead responds on ch with the first incoming message
// whose Seqno matches seqno, then auto unregisters itself
// after that single send on ch.
func (c *Client) GetOneRead(seqno uint64, ch chan *Message) {
	if cap(ch) == 0 {
		panic("ch must be bufferred")
	}
	c.mut.Lock()
	defer c.mut.Unlock()
	c.notifyOnce[seqno] = ch
}

// UngetReads reverses what GetReads does:
// un-register and have ch be deaf from now on.
// Idempotent: if ch is already gone, no foul is reported.
func (c *Client) UngetReads(ch chan *Message) {
	c.mut.Lock()
	defer c.mut.Unlock()
	for i := range c.notifyOnRead {
		if c.notifyOnRead[i] == ch {
			c.notifyOnRead = append(c.notifyOnRead[:i], c.notifyOnRead[i+1:]...)
			return
		}
	}
}

// NewClient attemps to connect to config.ClientDialToHostPort;
// err will come back with any problems encountered.
// The name setting allows users to track multiple instances
// of Clients, and the Client.Name() method will retreive it.
func NewClient(name string, config *Config) (c *Client, err error) {

	// make our own copy
	var cfg *Config
	if config != nil {
		clone := *config
		cfg = &clone
	} else {
		return nil, fmt.Errorf("missing config.ServerAddr to connect to")
	}
	c = &Client{
		cfg:         cfg,
		name:        name,
		oneWayCh:    make(chan *Message),
		roundTripCh: make(chan *Message),
		halt:        idem.NewHalter(),
		Connected:   make(chan error, 1),
		lastSeqno:   1,
		notifyOnce:  make(map[uint64]chan *Message),

		// net/rpc
		pending: make(map[uint64]*Call),
	}
	c.encBufW = bufio.NewWriter(&c.encBuf)
	c.codec = &gobClientCodec{
		rwc:    nil,
		dec:    gob.NewDecoder(&c.decBuf),
		enc:    gob.NewEncoder(c.encBufW),
		encBuf: c.encBufW,
	}

	go c.runClientMain(c.cfg.ClientDialToHostPort, c.cfg.TCPonly_no_TLS, c.cfg.CertPath)

	// wait for connection (or not).
	err = <-c.Connected
	return c, err
}

// Name reports the name the Client was created with.
func (c *Client) Name() string {
	return c.name
}

// Close shuts down the Client.
func (c *Client) Close() error {
	//vv("Client.Close() called.") // not seen in shutdown.
	if c.cfg.UseQUIC {
		c.cfg.shared.mut.Lock()
		if !c.cfg.shared.isClosed { // since Client.Close() might be called more than once.
			c.cfg.shared.shareCount--
			if c.cfg.shared.shareCount < 0 {
				panic("client count should never be < 0")
			}
			//vv("c.cfg.shared.shareCount = '%v' for '%v'", c.cfg.shared.shareCount, c.name)
			if c.cfg.shared.shareCount == 0 {
				c.cfg.shared.quicTransport.Conn.Close()
				c.cfg.shared.isClosed = true
			}
		}
		c.cfg.shared.mut.Unlock()
	}
	c.halt.ReqStop.Close()
	<-c.halt.Done.Chan
	//vv("Client.Close() finished.")
	return nil
}

var ErrShutdown = fmt.Errorf("shutting down")
var ErrDone = fmt.Errorf("done channel closed")

// SendAndGetReplyWithTimeout expires the call after
// timeout.
func (c *Client) SendAndGetReplyWithTimeout(timeout time.Duration, req *Message) (reply *Message, err error) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	go func() {
		time.Sleep(timeout)
		cancelFunc()
	}()
	return c.SendAndGetReply(req, ctx.Done())
}

// SendAndGetReply starts a round-trip RPC call.
// We will wait for a response before retuning.
// The doneCh is optional; it can be nil. A
// context.Done() like channel can be supplied there to
// stop waiting before a reply comes back.
func (c *Client) SendAndGetReply(req *Message, doneCh <-chan struct{}) (reply *Message, err error) {

	if len(req.DoneCh) > cap(req.DoneCh) || cap(req.DoneCh) < 1 {
		panic(fmt.Sprintf("req.DoneCh did not have capacity; cap = %v, len=%v", cap(req.DoneCh), len(req.DoneCh)))
	}
	var from, to string
	if c.isQUIC {
		from = local(c.quicConn)
		to = remote(c.quicConn)
	} else {
		from = local(c.conn)
		to = remote(c.conn)
	}
	isRPC := true
	isLeg2 := false
	isNetRPC := req.HDR.IsNetRPC

	mid := NewHDR(from, to, req.HDR.Subject, isRPC, isLeg2)
	mid.IsNetRPC = isNetRPC

	req.HDR = *mid

	//vv("Client '%v' SendAndGetReply(req='%v') (ignore req.Seqno:0 not yet assigned)", c.name, req)
	select {
	case c.roundTripCh <- req:
		// proceed
		//vv("Client '%v' SendAndGetReply(req='%v') delivered on roundTripCh", c.name, req)
	case <-doneCh:
		//vv("Client '%v' SendAndGetReply(req='%v'): doneCh files before roundTripCh", c.name, req)
		return nil, ErrDone
	case <-c.halt.ReqStop.Chan:
		c.halt.Done.Close()
		return nil, ErrShutdown
	}

	//vv("client '%v' to wait on req.DoneCh; after sending req='%v'", c.name, req)

	select { // shutdown test stuck here, even with calls in own goro. goq.go has exited.
	case reply = <-req.DoneCh:
		if reply != nil {
			err = reply.Err
		}
		//vv("client.SendAndGetReply() got on reply.Err = '%v'", err)
		return
	case <-doneCh:
		// usually a timeout
		return nil, ErrDone
	case <-c.halt.ReqStop.Chan:
		c.halt.Done.Close()
		return nil, ErrShutdown
	}
}

// OneWaySend sends a message without expecting or waiting for a response.
// The doneCh is optional, and can be nil.
func (c *Client) OneWaySend(msg *Message, doneCh <-chan struct{}) (err error) {

	var from, to string
	if c.isQUIC {
		from = local(c.quicConn)
		to = remote(c.quicConn)
	} else {
		from = local(c.conn)
		to = remote(c.conn)
	}

	isRPC := false
	isLeg2 := false

	mid := NewHDR(from, to, msg.HDR.Subject, isRPC, isLeg2)
	msg.HDR = *mid
	// allow msg.CallID to not be empty; in case we get a reply.
	// isRPC=false so this is 1-way, but it might in turn still
	// generate a response.

	select {
	case c.oneWayCh <- msg:
		return nil // not worth waiting?
	case <-doneCh:
		return ErrDone

	case <-c.halt.ReqStop.Chan:
		c.halt.Done.Close()
		return ErrShutdown
	}
}

func (c *Client) SetLocalAddr(local string) {
	c.mut.Lock()
	defer c.mut.Unlock()
	c.cfg.LocalAddress = local
}
func (c *Client) LocalAddr() string {
	c.mut.Lock()
	defer c.mut.Unlock()
	return c.cfg.LocalAddress
}

func remote(nc localRemoteAddr) string {
	ra := nc.RemoteAddr()
	return ra.Network() + "://" + ra.String()
}

func local(nc localRemoteAddr) string {
	la := nc.LocalAddr()
	return la.Network() + "://" + la.String()
}

func (c *Client) nextSeqno() (n uint64) {
	return atomic.AddUint64(&c.lastSeqno, 1)
}

// SelfyNewKey will generate a self-signed certificate
// authority, a new ed25519 key pair, sign the public
// key to create a cert, and write these four
// new files to disk. The directories
// odir/my-keep-private-dir and odir/certs will be created.
func SelfyNewKey(createKeyPairNamed, odir string) error {
	odirPrivateKey := odir + sep + "my-keep-private-dir"
	odirCerts := odir + sep + "certs"
	host, _ := os.Hostname()
	email := createKeyPairNamed + "@" + host

	const verbose = false
	const encryptWithPassphhrase = false

	if !dirExists(odirPrivateKey) || !fileExists(odirPrivateKey+sep+"ca.crt") {
		//vv("key-pair '%v' requested but CA does not exist in '%v', so auto-generating a self-signed CA for your first.", createKeyPairNamed, odirPrivateKey)
		selfcert.Step1_MakeCertificatAuthority(odirPrivateKey, verbose, encryptWithPassphhrase)
	}

	privKey, err := selfcert.Step2_MakeEd25519PrivateKey(createKeyPairNamed, odirCerts, verbose, encryptWithPassphhrase)
	panicOn(err)
	selfcert.Step3_MakeCertSigningRequest(privKey, createKeyPairNamed, email, odirCerts)
	selfcert.Step4_MakeCertificate(nil, odirPrivateKey, createKeyPairNamed, odirCerts, verbose)

	return nil
}

// chmod og-wrx path
func ownerOnly(path string) error {

	// Get the current file info
	fileInfo, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("Error getting file '%v' stat: '%v'", path, err)
	}

	// Get the current permissions
	currentPerm := fileInfo.Mode().Perm()

	// Remove read, write, and execute permissions for group and others
	newPerm := currentPerm &^ (os.FileMode(0o077))

	// Change the file permissions
	err = os.Chmod(path, newPerm)
	if err != nil {
		return fmt.Errorf("Error changing file permissions on '%v': '%v'", path, err)
	}
	return nil
}
