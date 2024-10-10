package rpc25519

// cli.go: simple TCP client, with TLS encryption.

import (
	"context"
	"crypto/tls"
	"fmt"
	//"io"
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

var sep = string(os.PathSeparator)

// eg. serverAddr = "localhost:8443"
// serverAddr = "192.168.254.151:8443"
func (c *Client) RunClientMain(serverAddr string, tcp_only bool, certPath string) {
	log.SetFlags(log.LstdFlags | log.Lshortfile) // Add Lshortfile for short file names

	defer func() {
		c.halt.Done.Close()
	}()

	if tcp_only {
		c.RunClientTCP(serverAddr)
		return
	}

	embedded := true
	sslCA := fixSlash("certs/ca.crt") // path to CA cert

	keyName := "client"
	if c.cfg.KeyPairName != "" {
		keyName = c.cfg.KeyPairName
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
	//panicOn(err2)
	// under test vs...?
	// without this ServerName assignment, we used to get (before gen.sh put in SANs using openssl-san.cnf)
	// 2019/01/04 09:36:18 failed to call: x509: cannot validate certificate for 127.0.0.1 because it doesn't contain any IP SANs
	//
	// update:
	// This is still needed in order to run the server on a different TCP host.
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
		c.RunQUIC(serverAddr, config)
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
	c.Conn = nconn
	c.Connected <- nil
	conn := nconn.(*tls.Conn) // docs say this is for sure.
	defer conn.Close()

	//log.Printf("Connected to server %s", serverAddr)

	la := conn.LocalAddr()
	c.cfg.LocalAddress = la.Network() + "://" + la.String()

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

	go c.RunReadLoop(conn)
	c.RunSendLoop(conn)
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
	c.cfg.LocalAddress = la.Network() + "://" + la.String()

	c.isTLS = false
	c.Conn = conn

	c.Connected <- nil
	defer conn.Close()
	//log.Printf("Connected to server %s", serverAddr)

	// TODO debug reverse this comment out. should we just be single threaded client??
	go c.RunReadLoop(conn)
	c.RunSendLoop(conn)
}

func (c *Client) RunReadLoop(conn net.Conn) {
	defer func() {
		c.halt.ReqStop.Close()
		c.halt.Done.Close()
	}()

	w := newWorkspace()
	readTimeout := time.Millisecond * 100
	for {

		// shutting down?
		select {
		case <-c.halt.ReqStop.Chan:
			return
		default:
		}

		// Receive a message
		seqno, msg, err := w.receiveMessage(conn, &readTimeout)
		if err != nil {
			r := err.Error()
			if strings.Contains(r, "timeout") {
				continue
			}
			if strings.Contains(r, "use of closed network connection") {
				return
			}
			if strings.Contains(r, "connection reset by peer") {
				return
			}
			if r == "EOF" && msg == nil {
				return
			}
			//vv("ignore err = '%v'; msg = '%v'", err, msg)
		}

		//vv("client %v received message with seqno=%v, msg='%v'", c.name, seqno, msg)

		// server's responsibility is to increment the responses +1, from odd to even.

		c.mut.Lock()
		whoCh, waiting := c.notifyOnce[seqno]
		if waiting {
			delete(c.notifyOnce, seqno)
			select {
			case whoCh <- msg:
				//vv("client %v: yay. sent on notifyOnce channel! for seqno=%v", c.name, seqno)
			default:
				//vv("could not send to notifyOnce channel!")
			}
		} else {
			// assume the round-trip "calls" should be consumed,
			// and not repeated here to client listeners who want events???
			// trying to match what other RPC systems do.
			for _, ch := range c.notifyOnRead {
				select {
				case ch <- msg:
					//vv("client: %v: yay. sent on notifyOnRead channel!", c.name)
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

	w := newWorkspace()

	// PRE: Message.DoneCh must be buffered at least 1, so our logic below does not have to deal with ever blocking.
	for {
		select {
		case <-c.halt.ReqStop.Chan:
			return
		case msg := <-c.oneWayCh:

			// one-way always use seqno 0,
			// so we know that no follow up is expected.
			msg.Seqno = 0

			if msg.Nc == nil {
				// use default conn
				msg.Nc = conn
			}
			// Send the message
			if err := w.sendMessage(msg.Seqno, conn, msg, &c.cfg.WriteTimeout); err != nil {
				log.Printf("Failed to send message: %v", err)
				msg.Err = err
			} else {
				//vv("cli %v has sent a 1-way message: %v'", c.name, msg)
			}
			close(msg.DoneCh) // convey the error or lack thereof.

		case msg := <-c.roundTripCh:

			// these will start at 3 and go up, in each client.
			seqno := c.nextOddSeqno()
			msg.Seqno = seqno

			//vv("cli %v has had a round trip requested: GetOneRead is registering for seqno=%v", c.name, seqno+1)
			c.GetOneRead(seqno+1, msg.DoneCh)

			if err := w.sendMessage(msg.Seqno, conn, msg, &c.cfg.WriteTimeout); err != nil {
				log.Printf("Failed to send message: %v", err)
				msg.Err = err
				close(msg.DoneCh)
				continue
			} else {
				//vv("(client %v) Sent message: (seqno=%v): '%v'", c.name, msg.Seqno, msg)
			}

		}
	}
}

// interface for goq

type Message struct {
	Nc    net.Conn
	Seqno uint64

	Subject string // intent. example: "rpc call to ThisFunc()"
	MID     MID

	JobSerz []byte

	// Err is not serialized on the wire by the server,
	// so communicates only local information. Callback
	// functions should convey errors in-band within
	// JobSerz.
	Err error

	DoneCh chan *Message
}

func NewMessage() *Message {
	return &Message{
		// NOTE: buffer size must be at least 1, so our Client.RunSendLoop never blocks.
		// Thus we simplify the logic there, not requiring a ton of extra selects to
		// handle shutdown/timeout/etc.
		DoneCh: make(chan *Message, 1),
	}
}

func (msg *Message) String() string {
	return fmt.Sprintf("&Message{Seqno:%v, MID:%v, Err:'%v'}", msg.Seqno, msg.MID.String(), msg.Err)
}

func NewMessageFromBytes(by []byte) (msg *Message) {
	msg = NewMessage()
	msg.JobSerz = by
	return
}

// CallbackFunc is the user's own function that they
// register with the server for remote procedure calls.
//
// The users's func may not want to return anything: be a one-way.
// In that case they should return nil in out.
//
// If they want to return anything, even an error, they
// must allocate with rpc25519.NewMessage() and return
// that (in out). The out.Err field can be assigned
// for an error to be returned. The
// JobSerz []byte are the main place to return structured
// information, but it can be nil if there is only an
// error. It is fine to set neither and still allocate out.
// The caller will get a response that no error was encountered.
//
// A one-way function is equivalent to returning nil. No
// reply will be sent to the caller, and so they hopefully
// sent using SendOneWay(). This may be desired though:
// a later asynchronous server push will unblock them.
type CallbackFunc func(in *Message) (out *Message)

// Config says who to contact (for a client), or
// where to listen (for a server); and sets how
// strong a security posture we adopt.
type Config struct {

	// ServerAddr host:port of the rpc25519.Server to contact.
	ServerAddr string

	// TCP false means TLS-1.3 secured. true here means do TCP only.
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

	KeyPairName string // default "client" means use certs/client.crt and certs/client.key

	// These are timeouts for connection and transport tuning.
	// The defaults of 0 mean wait forever.
	ConnectTimeout time.Duration
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration

	LocalAddress string
}

// Clients write requests, and maybe wait for responses.
type Client struct {
	cfg *Config
	mut sync.Mutex

	name string

	notifyOnRead []chan *Message
	notifyOnce   map[uint64]chan *Message

	Conn     net.Conn // the default.
	QuicConn quic.Connection

	isTLS  bool
	isQUIC bool

	oneWayCh    chan *Message
	roundTripCh chan *Message

	halt *idem.Halter

	// if connecting suceeds, a nil will be sent; else the error.
	Connected chan error

	err error // detect inability to connect.

	lastOddSeqno uint64
}

func (c *Client) Err() error {
	return c.err
}

func (c *Client) GetReadIncomingCh() (ch chan *Message) {
	ch = make(chan *Message, 100)
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

// auto unregister after a single send on ch.
func (c *Client) GetOneRead(seqno uint64, ch chan *Message) {
	if cap(ch) == 0 {
		panic("ch must be bufferred")
	}
	c.mut.Lock()
	defer c.mut.Unlock()
	c.notifyOnce[seqno] = ch
}

// un-register to get any received messages on ch.
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

func NewClient(name string, config *Config) (c *Client, err error) {

	// make our own copy
	var cfg *Config
	if config != nil {
		clone := *config
		cfg = &clone
	} else {
		cfg = &Config{} // defaults.
	}
	c = &Client{
		cfg:          cfg,
		name:         name,
		oneWayCh:     make(chan *Message),
		roundTripCh:  make(chan *Message),
		halt:         idem.NewHalter(),
		Connected:    make(chan error, 1),
		lastOddSeqno: 1,
		notifyOnce:   make(map[uint64]chan *Message),
	}
	go c.RunClientMain(c.cfg.ServerAddr, c.cfg.TCPonly_no_TLS, c.cfg.CertPath)

	// wait for connection (or not).
	err = <-c.Connected
	return c, err
}

func (c *Client) Close() error {
	//vv("Client.Close() called.") // not seen in shutdown.
	c.halt.ReqStop.Close()
	<-c.halt.Done.Chan
	//vv("Client.Close() finished.")
	return nil
}

var ErrShutdown = fmt.Errorf("shutting down")
var ErrDone = fmt.Errorf("done channel closed")

func (c *Client) SendAndGetReplyWithTimeout(timeout time.Duration, req *Message) (reply *Message, err error) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	go func() {
		time.Sleep(timeout)
		cancelFunc()
	}()
	return c.SendAndGetReply(req, ctx.Done())
}

// doneCh is optional; can be nil.
func (c *Client) SendAndGetReply(req *Message, doneCh <-chan struct{}) (reply *Message, err error) {

	from := local(c.Conn)
	to := remote(c.Conn)
	isRPC := true
	isLeg2 := false
	mid := NewMID(from, to, req.Subject, isRPC, isLeg2)
	req.MID = *mid

	select {
	case c.roundTripCh <- req:
		// proceed

	case <-doneCh:
		return nil, ErrDone
	case <-c.halt.ReqStop.Chan:
		c.halt.Done.Close()
		return nil, ErrShutdown
	}

	select { // shutdown test stuck here, even with calls in own goro. goq.go has exited.
	case reply = <-req.DoneCh:
		err = reply.Err
		return
	case <-doneCh:
		// usually a timeout
		return nil, ErrDone
	case <-c.halt.ReqStop.Chan:
		c.halt.Done.Close()
		return nil, ErrShutdown
	}
}

// doneCh is optional, can be nil.
func (c *Client) OneWaySend(msg *Message, doneCh <-chan struct{}) (err error) {

	from := local(c.Conn)
	to := remote(c.Conn)
	isRPC := false
	isLeg2 := false

	mid := NewMID(from, to, msg.Subject, isRPC, isLeg2)
	msg.MID = *mid
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

	select {
	case <-msg.DoneCh: // closed, but  Err set possibly on msg.
		err = msg.Err
		return

	case <-doneCh:
		return ErrDone

	case <-c.halt.ReqStop.Chan:
		c.halt.Done.Close()
		return ErrShutdown
	}
}

func (c *Client) LocalAddr() string {
	return c.cfg.LocalAddress
}

func remote(nc net.Conn) string {
	ra := nc.RemoteAddr()
	return ra.Network() + "://" + ra.String()
}

func local(nc net.Conn) string {
	la := nc.LocalAddr()
	return la.Network() + "://" + la.String()
}

// issue 3, 5, 7, 9, ...
func (c *Client) nextOddSeqno() (n uint64) {
	return atomic.AddUint64(&c.lastOddSeqno, 2)
}

// odir/my-keep-private-dir and odir/certs will be created.
func SelfyNewKey(createKeyPairNamed, odir string) error {
	odirPrivateKey := odir + sep + "my-keep-private-dir"
	odirCerts := odir + sep + "certs"
	host, _ := os.Hostname()
	email := createKeyPairNamed + "@" + host

	if !DirExists(odirPrivateKey) || !FileExists(odirPrivateKey+sep+"ca.crt") {
		log.Printf("key-pair '%v' requested but CA does not exist in '%v', so auto-generating a self-signed CA for your first.", createKeyPairNamed, odirPrivateKey)
		selfcert.Step1_MakeCertificatAuthority(odirPrivateKey)
	}

	selfcert.Step2_MakeEd25519PrivateKeys([]string{createKeyPairNamed}, odirCerts)
	selfcert.Step3_MakeCertSigningRequests([]string{createKeyPairNamed}, []string{email}, odirCerts)
	selfcert.Step4_MakeCertificates(odirPrivateKey, []string{createKeyPairNamed}, odirCerts)

	return nil
}
