package rpc25519

// cli.go: simple TCP client, with TLS encryption.

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	cryrand "crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"fmt"
	"golang.org/x/crypto/hkdf"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/glycerine/greenpack/msgp"
	"github.com/glycerine/idem"
	"github.com/glycerine/ipaddr"
	"github.com/glycerine/loquet"
	"github.com/glycerine/rpc25519/selfcert"
	"github.com/quic-go/quic-go"
)

var _ = cryrand.Read

const DefaultUseCompression = true
const DefaultUseCompressAlgo = "s2" // see magic7.go

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

	defer func() {
		vv("runClientMain defer: end for goro = %v", GoroNumber()) // deadlock on vprint mutex here?
		c.halt.ReqStop.Close()
		c.halt.Done.Close()

		c.mut.Lock()
		doClean := c.seenNetRPCCalls
		c.mut.Unlock()
		if doClean {
			c.netRpcShutdownCleanup(ErrShutdown())
		}
	}()

	dirCerts := GetCertsDir()

	c.cfg.checkPreSharedKey("client")

	sslCA := fixSlash(dirCerts + "/ca.crt") // path to CA cert

	keyName := "client"
	if c.cfg.ClientKeyPairName != "" {
		keyName = c.cfg.ClientKeyPairName
	}

	sslCert := fixSlash(fmt.Sprintf(dirCerts+"/%v.crt", keyName))    // path to server cert
	sslCertKey := fixSlash(fmt.Sprintf(dirCerts+"/%v.key", keyName)) // path to server key

	if certPath != "" {
		sslCA = fixSlash(fmt.Sprintf("%v/ca.crt", certPath))               // path to CA cert
		sslCert = fixSlash(fmt.Sprintf("%v/%v.crt", certPath, keyName))    // path to server cert
		sslCertKey = fixSlash(fmt.Sprintf("%v/%v.key", certPath, keyName)) // path to server key
	}

	var config *tls.Config
	var creds *selfcert.Creds
	if !tcp_only {
		// handle pass-phrase protected certs/client.key
		var err2 error
		config, creds, err2 = selfcert.LoadNodeTLSConfigProtected(false, sslCA, sslCert, sslCertKey)
		//config, err2 := loadClientTLSConfig(embedded, sslCA, sslCert, sslCertKey)
		if err2 != nil {
			c.err = fmt.Errorf("error on LoadClientTLSConfig()'%v'", err2)
			panic(c.err)
		}
		c.creds = creds
	}

	// since TCP may verify creds now too, only run TCP client *after* loading creds.
	if tcp_only {
		if c.cfg.UseSimNet {
			panic("cannot have both TCPonly_no_TLS and UseSimNet true")
		}
		c.runClientTCP(serverAddr)
		return
	}

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
		if c.cfg.UseSimNet {
			panic("cannot have both UseQUIC and UseSimNet true")
		}
		localHostPort := c.cfg.ClientHostPort
		if localHostPort == "" {
			localHost, err := ipaddr.LocalAddrMatching(serverAddr)
			panicOn(err)
			//vv("localHost = '%v', matched to quicServerAddr = '%v'", localHost, serverAddr)
			localHostPort = localHost + ":0" // client can pick any port
		}
		c.runQUIC(localHostPort, serverAddr, config)
		return
	}
	if c.cfg.UseSimNet {
		c.runSimNetClient(c.cfg.ClientHostPort, serverAddr)
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
		c.connected <- fmt.Errorf("error: client local: '%v' failed to "+
			"connect to server: '%v'", c.cfg.ClientHostPort, err)
		alwaysPrintf("error: client from '%v' failed to connect to server: %v", c.cfg.ClientHostPort, err)
		return
	}
	c.isTLS = true
	// do this before signaling on c.connected, else tests will race and panic
	// not having a connection
	c.conn = nconn

	conn := nconn.(*tls.Conn) // docs say this is for sure.
	defer conn.Close()        // in runClientMain() here.

	c.setLocalAddr(conn)

	// only signal ready once SetLocalAddr() is done, else submitter can crash.
	c.connected <- nil

	//alwaysPrintf("connected to server %s", serverAddr)

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
		good, bad, wasNew, err := hostKeyVerifies(knownHostsPath, &connState, remoteAddr)
		_ = good
		_ = wasNew
		_ = bad
		if err != nil {
			fmt.Fprintf(os.Stderr, "hostKeyVerifies has failed: key failed list:'%#v': '%v'\n", bad, err)
			return
		}
		//for i := range good {
		//	vv("accepted identity for server: '%v' (was new: %v)\n", good[i], wasNew)
		//}
	}

	err = c.setupPSK(conn)
	if err != nil {
		alwaysPrintf("setupPSK error: '%v'", err)
		return
	}

	cpair := &cliPairState{}
	go c.runSendLoop(conn, cpair)
	c.runReadLoop(conn, cpair)
}

func (c *Client) runClientTCP(serverAddr string) {

	// Dial the server
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		c.err = err
		c.connected <- err
		alwaysPrintf("Failed to connect to server: %v", err)
		return
	}

	c.setLocalAddr(conn)

	c.isTLS = false
	c.conn = conn

	c.connected <- nil
	defer conn.Close() // in runClientTCP() here.
	//alwaysPrintf("connected to server %s", serverAddr)

	if c.cfg.HTTPConnectRequired {
		io.WriteString(conn, "CONNECT "+DefaultRPCPath+" HTTP/1.0\n\n")

		// Require successful HTTP response
		// before switching to RPC protocol.
		resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
		if err != nil || resp.Status != connectedToGoRPC {
			alwaysPrintf("unexpected HTTP response: '%v'; err = '%v'", resp.Status, err)
			return // does the conn.Close() above in the defer.
		}
		//vv("resp.Status = '%v'", resp.Status)
	}

	err = c.setupPSK(conn)
	if err != nil {
		alwaysPrintf("setupPSK error: '%v'", err)
		return
	}

	cpair := &cliPairState{} // track the last compression used, so we can echo it.
	c.cpair = cpair
	go c.runSendLoop(conn, cpair)
	c.runReadLoop(conn, cpair)
}

type cliPairState struct {
	lastReadMagic7      atomic.Int64
	lastPingReceivedTmu atomic.Int64
	lastPingSentTmu     atomic.Int64
}

func (c *Client) runReadLoop(conn net.Conn, cpair *cliPairState) {
	var err error
	ctx, canc := context.WithCancel(context.Background())
	defer func() {
		r := recover()
		if r != nil {
			alwaysPrintf("cli runReadLoop defer/shutdown running. saw panic '%v'; stack=\n%v\n", r, stack())
		} else {
			//vv("cli runReadLoop defer/shutdown running.")
		}
		//vv("client runReadLoop exiting, last err = '%v'", err)
		canc()
		c.halt.ReqStop.Close()
		c.halt.Done.Close()
	}()

	symkey := c.cfg.preSharedKey
	if c.cfg.encryptPSK {
		c.mut.Lock()
		symkey = c.randomSymmetricSessKeyFromPreSharedKey
		c.mut.Unlock()
	}

	cliLocalAddr := c.LocalAddr()
	_ = cliLocalAddr

	//vv("about to make a newBlabber for client read loop; c.cfg = %p ", c.cfg)
	w := newBlabber("client read loop", symkey, conn, c.cfg.encryptPSK, maxMessage, false, c.cfg, nil, cpair)

	readTimeout := time.Millisecond * 100
	_ = readTimeout
	var msg *Message
	for {

		// poll for: shutting down?
		select {
		case <-c.halt.ReqStop.Chan:
			return
		default:
		}

		// Receive a message

		// Does not work to use a timeout: we will get
		// partial reads which are then difficult to
		// recover from, because we have not tracked
		// how much of the rest of the incoming
		// stream needs to be discarded!
		// So: always read without a timeout. In fact
		// we've eliminated the parameter.
		msg, err = w.readMessage(conn)
		if err != nil {
			if msg != nil {
				panic("should not have a message if error back!")
			}
			r := err.Error()

			// under TCP: its normal to see 'read tcp y.y.y.y:59705->x.x.x.x:9999: i/o timeout'
			if strings.Contains(r, "i/o timeout") || strings.Contains(r, "deadline exceeded") {
				//if strings.Contains(r, "deadline exceeded") {
				// just our readTimeout happening, so we can poll on shutting down, above.
				continue
			}
			//vv("err = '%v'", err)
			if strings.Contains(r, "timeout: no recent network activity") {
				// we will hard spin the CPU to 100% (after disconnect)
				// if we don't exit on this.
				//vv("cli read loop exiting on '%v'", err)
				return
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
				//vv("cli readLoop sees EOF, exiting.")
				return
			}
			if err == io.EOF && msg == nil {
				return
			}
			//vvv("ignore err = '%v'; msg = '%v'", err, msg)
		}
		if msg == nil {
			continue
		}
		now := time.Now()
		if msg.HDR.Typ == CallKeepAlive {
			//vv("client got an rpc25519 keep alive.")
			// nothing more to do, the keepalive just keeps the
			// middle boxes on the internet from dropping
			// our network connection.
			cpair.lastPingReceivedTmu.Store(now.UnixNano())
			continue
		}
		msg.HDR.LocalRecvTm = now

		seqno := msg.HDR.Seqno
		//vv("client %v (cliLocalAddr='%v') received message with seqno=%v, msg.HDR='%v'", c.name, cliLocalAddr, seqno, msg.HDR.String())

		// special case to bootstrap up a peer by remote
		// request, since no other way to register stuff
		// on the client, and this is a pretty unique call
		// anyway. This is like the Client acting like
		// a server and starting up a peer service.
		switch msg.HDR.Typ {
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

			err := c.PeerAPI.bootstrapCircuit(yesIsClient, msg, ctx, c.oneWayCh)
			if err != nil {
				// only error is on shutdown request received.
				//vv("cli c.PeerAPI.bootstrapCircuit returned err = '%v'", err)
				return
			}
			continue
		}

		if c.notifies.handleReply_to_CallID_ToPeerID(true, ctx, msg) {
			//vv("client side (%v) notifies says we are done after msg = '%v'", cliLocalAddr, msg.HDR.String())
			continue
		} else {
			//vv("client side (%v) notifies says we are NOT done after msg = '%v'", cliLocalAddr, msg.HDR.String())
		}
		if msg.HDR.Typ == CallPeerTraffic ||
			msg.HDR.Typ == CallPeerError {
			bad := fmt.Sprintf("cli readLoop: Peer traffic should never get here! msg.HDR='%v'", msg.HDR.String())
			alwaysPrintf(bad)
			panic(bad)
		}

		// protect map access. Be sure to Unlock if you "continue" below.
		c.mut.Lock()

		whoCh, waiting := c.notifyOnce[seqno]
		//vv("notifyOnce waiting = %v for seqno %v", waiting, seqno)
		if waiting {
			delete(c.notifyOnce, seqno)
			whoCh.CloseWith(msg)
		} else {
			//vv("notifyOnce: nobody was waiting for seqno = %v", seqno)

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

func (c *Client) runSendLoop(conn net.Conn, cpair *cliPairState) {
	defer func() {
		r := recover()
		if r != nil {
			alwaysPrintf("cli runSendLoop defer/shutdown running. saw panic '%v'; stack=\n%v\n", r, stack())
		} else {
			//vv("cli runSendLoop defer/shutdown running.")
		}
		c.halt.ReqStop.Close()
		c.halt.Done.Close()
	}()

	symkey := c.cfg.preSharedKey
	if c.cfg.encryptPSK {
		c.mut.Lock()
		symkey = c.randomSymmetricSessKeyFromPreSharedKey
		c.mut.Unlock()
	}

	//vv("about to make a newBlabber for client send loop; c.cfg = %p ", c.cfg)
	w := newBlabber("client send loop", symkey, conn, c.cfg.encryptPSK, maxMessage, false, c.cfg, nil, cpair)

	// PRE: Message.DoneCh must be buffered at least 1, so
	// our logic below does not have to deal with ever blocking.

	// implement ClientSendKeepAlive
	var lastPing time.Time
	var doPing bool
	var pingEvery time.Duration
	var pingTimer *RpcTimer
	var pingWakeCh <-chan time.Time
	var keepAliveWriteTimeout time.Duration // := c.cfg.WriteTimeout

	if c.cfg.ClientSendKeepAlive > 0 {
		//vv("client side pings are on")
		doPing = true
		pingEvery = c.cfg.ClientSendKeepAlive
		lastPing = time.Now()
		pingTimer = c.NewTimer(pingEvery)
		pingWakeCh = pingTimer.C
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
				c.keepAliveMsg.HDR.StreamPart = atomic.LoadInt64(&c.epochV.EpochID)
				err := w.sendMessage(conn, &c.keepAliveMsg, &keepAliveWriteTimeout)
				//vv("cli sent rpc25519 keep alive. err='%v'; keepAliveWriteTimeout='%v'", err, keepAliveWriteTimeout)
				if err != nil {
					alwaysPrintf("client had problem sending keep alive: '%v'", err)
				} else {
					c.cpair.lastPingSentTmu.Store(now.UnixNano())
				}
				lastPing = now
				nextPingDur = pingEvery
				//pingWakeCh = c.TimeAfter(pingEvery) // bad, hides it from simnet.

			} else {
				// Pre go1.23 this would have leaked timer memory, but not now.
				// https://pkg.go.dev/time#After says
				// Before Go 1.23, this documentation warned that the
				// underlying Timer would not be recovered by the garbage
				// collector until the timer fired, and that if efficiency
				// was a concern, code should use NewTimer instead and call
				// Timer.Stop if the timer is no longer needed. As of Go 1.23,
				// the garbage collector can recover unreferenced,
				// unstopped timers. There is no reason to prefer NewTimer
				// when After will do.
				// If using < go1.23, see
				// https://medium.com/@oboturov/golang-time-after-is-not-garbage-collected-4cbc94740082
				// for a memory leak story.
				// bad: hides the timer discard from simnet.
				//pingWakeCh = c.TimeAfter(lastPing.Add(pingEvery).Sub(now))
				// good: simnet hears about the timer discard.
				nextPingDur = lastPing.Add(pingEvery).Sub(now)
			}
			pingTimer.Discard() // good, let simnet discard it.
			pingTimer = c.NewTimer(nextPingDur)
			pingWakeCh = pingTimer.C
		}

		select {
		case <-pingWakeCh:
			// check and send above.
			continue
		case <-c.halt.ReqStop.Chan:
			return
		case msg := <-c.oneWayCh:

			// one-way now use seqno too, but may already be set.
			if msg.HDR.Seqno == 0 {
				seqno := c.nextSeqno()
				msg.HDR.Seqno = seqno
			}

			//vv("cli name:'%v' has had a one-way requested: '%v'", c.name, msg)

			if msg.HDR.Nc == nil {
				// use default conn
				msg.HDR.Nc = conn
			}
			// Send the message
			//if err := w.sendMessage(conn, msg, &c.cfg.WriteTimeout); err != nil {
			if err := w.sendMessage(conn, msg, nil); err != nil {
				alwaysPrintf("Failed to send message: %v\n", err)
				msg.LocalErr = err
				rr := err.Error()
				if strings.Contains(rr, "use of closed network connection") ||
					strings.Contains(rr, "connection reset by peer") {
					return
				}
			} else {
				//vv("cli %v has sent a 1-way message: %v'", c.name, msg)
				lastPing = time.Now() // no need for ping
			}
			if msg.HDR.Typ == CallCancelPrevious {
				if msg.LocalErr == nil {
					msg.LocalErr = ErrCancelReqSent
				}
			}
			// convey the error or lack thereof.
			if msg.DoneCh != nil {
				msg.DoneCh.Close()
			}

		case msg := <-c.roundTripCh:

			seqno := c.nextSeqno()
			msg.HDR.Seqno = seqno

			//vv("cli %v has had a round trip requested: GetOneRead is registering for seqno=%v: '%v'; part '%v'", c.name, seqno, msg, msg.HDR.StreamPart)
			c.GetOneRead(seqno, msg.DoneCh)

			//if err := w.sendMessage(conn, msg, &c.cfg.WriteTimeout); err != nil {
			if err := w.sendMessage(conn, msg, nil); err != nil {
				//vv("Failed to send message: %v", err)
				msg.LocalErr = err
				c.UngetOneRead(seqno, msg.DoneCh)
				if msg.DoneCh != nil {
					msg.DoneCh.Close()
				}
				continue
			} else {
				//vv("7777777 (client %v) Sent message: (seqno=%v): CallID= %v", c.name, msg.HDR.Seqno, msg.HDR.CallID)
				lastPing = time.Now() // no need for ping
			}

		}
	}
}

// interface for goq

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
// Note that JobErrs is a string value rather than an error.
//
// The system will overwrite the reply.HDR.{To,From} fields when sending the
// reply, so the user should not bother setting those.
// The one exception to this rule is the reply.HDR.Subject string, which
// can be set by the user to return user-defined information.
// The reply will still be matched to the request on the HDR.Seqno, so
// a change of HDR.Subject will not change which goroutine
// receives the reply.
type TwoWayFunc func(req *Message, reply *Message) error

// OneWayFunc is the simpler sibling to the above.
// A OneWayFunc will not return anything to the sender.
//
// As above req.JobSerz [] byte contains the job payload.
type OneWayFunc func(req *Message)

// ServerSendsDownloadFunc is used to send a stream to the
// client on the streamToClientChan.
// Use Server.RegisterServerSendsDownloadFunc() to register it.
type ServerSendsDownloadFunc func(
	srv *Server,
	ctx context.Context,
	req *Message,
	sendDownloadPartToClient func(ctx context.Context, msg *Message, last bool) error,
	lastReply *Message,
) (err error)

// BistreamFunc aims to allow the user to implement server
// operations with full generality; it provies for
// uploads and downloads to the originating client,
// and for communication with other clients.
// Use Server.RegisterBistreamFunc() to register your BistreamFunc
// under a name. The BistreamFunc and its siblings
// the ServerSendsDownloadFunc and the UploadReaderFunc
// are only available for the Message based API; not in
// the net/rpc API.
//
// On the client side, the Client.RequestBistreaming() call
// is used to create a Bistreamer that will call the
// BistreamFunc by its registered name (the name
// that Server.RegisterBistreamFunc() was called with).
//
// In a BistreamFunc on the server, the full
// generality of interleaving upload and download
// handling is available. The initial Message in req
// will also be the first Message in the req.HDR.UploadsCh
// which receives all upload messages from the client.
//
// To note, it may be more convenient for the user
// to use an UploadReaderFunc or
// ServerSendsDownloadFunc if the full generality
// of the BistreamFunc is not needed. For simplicity, the
// Server.RegisterServerSendsDownloadFunc() is used
// to register your ServerSendsDownloadFunc.
// Server.RegisterUploadReaderFunc() is used to
// register you UploadReaderFunc. Note in particular
// that the UploadReaderFunc is not persistent
// but rather receives a callback per Message
// received from the Client.Uploader. This may
// simplify the implementation of your server-side
// upload function. Note that persistent state between messages
// is still available by registering a method on
// your struct; see the ServerSideUploadFunc struct in
// example.go for example.
//
// BistreamFunc, in contrast, are not a callback-per-message, but rather
// persist and would typically only exit if ctx.Done()
// is received, or if it wishes to finish the operation (say on an
// error, or by noting that a CallUploadEnd type Message has
// been received) so as to save goroutine resources
// on the server. The BistreamFunc is started on the server
// when the CallRequestBistreaming Message.HDR.Typ
// is received with the ServiceName matching the registered
// name. Each live instance of the BistreamFunc is identified
// by the req.HDR.CallID set by the originating client. All
// download messages sent will have this same CallID
// on them (for the client to match).
//
// When the BistreamFunc finishes (returns), a
// final message will of type CallRPCReply will be
// sent back to the client. This is the lastReply
// *Message provided in the BistreamFunc. The
// BistreamFunc should fill in this lastReply
// with any final JobSerz payload it wishes to
// send; this is optional. On the client side, the
// Client.RequestBistreaming() is used to start
// bi-streaming. It returns a Bistreamer. This Bistreamer
// has a ReadCh that will receive this final message
// (as well as all other download messages). See the
// cli_test.go Test065_bidirectional_download_and_upload
// for example use.
//
// A BistreamFunc is run on its own goroutine. It can
// start new goroutines, if it wishes, but
// this is not required. An additional (new) goroutine
// may be useful to reduce the latency of message
// handling while simultaneously reading from
// req.HDR.UploadsCh for uploads and writing to
// downloads with sendDownloadPartToClient(),
// as both of these are blocking, synchronous, operations.
// If you do so, be sure to handle goroutine cancellation and
// cleanup if the ctx provided is cancelled.
//
// The sendDownloadPartToClient()
// helper function is used to write download
// Messages. It properly assigns the HDR.StreamPart
// sequence numbers and HDR.Typ as one of
// CallDownloadBegin, CallDownloadMore, and
// CallDownloadEnd). The BistreamFunc should
// call sendDownloadPartToClient() with last=true
// to signal the end of the download, in
// which case HDR.Typ CallDownloadEnd will
// be set on the sent Message.
//
// To provide back-pressure by default,
// the sendDownloadPartToClient() call is
// synchronous and will return only when
// the message is sent. If you wish to continue
// to process uploads while sending a download
// part, your BistreamFunc can call the
// provided sendDownloadPartToClient()
// in a goroutine that you start for this
// purpose. The sendDownloadPartToClient() call
// is goroutine safe, as it uses its own internal
// sync.Mutex to ensure only one send is in
// progress at a time.
//
// A BistreamFunc by default communicates download
// messages to its originating client. However
// other clients can also be sent messages.
// The Server.SendOneWayMessage() and
// Server.SendMessage() operations on the
// Server can be used for this purpose.
//
// Visit the example.go implementation of
// ServeBistreamState.ServeBistream() to see
// it in action.
type BistreamFunc func(
	srv *Server,
	ctx context.Context,
	req *Message,
	uploadsFromClientCh <-chan *Message,
	sendDownloadPartToClient func(ctx context.Context, msg *Message, last bool) error,
	lastReply *Message,
) (err error)

// A UploadReaderFunc receives messages from a Client's upload.
// It corresponds to the client-side Uploader, created
// by Client.UploadBegin().
//
// For a quick example, see the ReceiveFileInParts()
// implementation in the example.go file. It is a method on the
// ServerSideUploadFunc struct that holds state
// between the callbacks to ReceiveFileInParts().
//
// A UploadReaderFunc is like a OneWayFunc, but it
// generally should also be a method or closure to capture
// the state it needs, as it will receive multiple req *Message
// up-calls from the same client Stream. It should return a
// non-nil error to tell the client to stop sending.
// A nil return means we are fine and want to continue
// to receive more Messages from the same Stream.
// The req.HDR.CallID can be used to identify distinct Streams,
// and the req.HDR.StreamPart will convey their order
// which will start at 0 and count up.
//
// The lastReply argument will be nil until
// the Client calls Stream.More() with
// the last argument set to true. The user/client is
// telling the UploadReaderFunc not to expect any further
// messages. The UploadReaderFunc can then fill in the
// lastReply message with any finishing detail, and
// it will be sent back to the client.
//
// Note that even when lastReply is not nil,
// req may still have the tail content of
// the stream, and so generally req should be processed
// before considering if this is the last message and a final
// lastReply should also be filled out.
//
// For cleanup/avoiding memory leaks:
// If deadCallID is not the empty string, then the
// connection for this CallID has died and we should
// cleanup its resources. ctx, req, and lastReply
// will all be nil in this case.
type UploadReaderFunc func(ctx context.Context, req *Message, lastReply *Message, deadCallID string) error

// Config is the same struct type for both NewClient
// and NewServer setup.
//
// Config says who to contact (for a client), or
// where to listen (for a server and/or client); and sets how
// strong a security posture we adopt.
//
// Copying a Config is fine, but it should be a simple
// shallow copy to preserve the shared *sharedTransport struct.
// See/use the Config.Clone() method if in doubt.
//
// nitty gritty details/dev note: the `shared` pointer here is the
// basis of port (and file handle) reuse where a single
// process can maintain a server and multiple clients
// in a "star" pattern. This only works with QUIC of course,
// and is one of the main reasons to use QUIC.
//
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

	// NoSharePortQUIC defaults false so sharing is allowed.
	// If true, then we do not share same UDP port between a QUIC
	// client and server (in the same process). Used
	// for testing client shutdown paths too.
	NoSharePortQUIC bool

	// path to certs/ like certificate
	// directory on the live filesystem.
	// defaults to GetCertsDir(); see config.go.
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
	// key. It must be 32 bytes (or more). Ideally
	// it should be generated from crypto/rand.
	// The `selfy -gensym outpath` command will
	// do this, writing 32 cryptographically random
	// bytes to output.
	PreSharedKeyPath string

	preSharedKey [32]byte
	encryptPSK   bool

	// This is a timeout for dialing the initial socket connection.
	// The default of 0 mean wait forever.
	ConnectTimeout time.Duration

	//
	ServerSendKeepAlive time.Duration
	ClientSendKeepAlive time.Duration

	// CompressAlgo choices are in magic7.go;
	// The current choices are "" (default compression, "s2" at the moment),
	// or: "s2" (Klaus Post's faster SnappyV2, good for incompressibles);
	// "lz4", (a very fast compressor; see https://lz4.org/);
	// "zstd:01" (fastest setting for Zstandard, very little compression);
	// "zstd:03", (the Zstandard 'default' level; slower but more compression);
	// "zstd:07", (even more compression, even slower);
	// "zstd:11", (slowest version of Zstandard, the most compression).
	//
	// Note! empty string means we use DefaultUseCompressAlgo
	// (at the top of cli.go), which is currently "s2".
	// To turn off compression, you must use the
	// CompressionOff setting.
	CompressAlgo string

	// CompressionOff must be universal among peers, if true;
	// all clients and servers must have compression off, or none.
	// If this does not hold, we will panic because, to save memory,
	// if CompressionOff we do not allocate any
	// compression handling machinery. Normally the reader-
	// makes-right approach is safer -- allowing compression
	// as an option -- but that means that everyone must allocate
	// the decompressor to handle that eventuality.
	CompressionOff bool

	// Intially speak HTTP and only
	// accept CONNECT requests that
	// we turn into our protocol.
	// Only works with TCPonly_no_TLS true also,
	// at the moment. Also adds on another
	// round trip.
	HTTPConnectRequired bool

	localAddress string

	// for port sharing between a server and 1 or more clients over QUIC
	shared *sharedTransport

	// these starting directories
	// noted on {Client/Server}.Start and referenced
	// thereafter; to allow tests
	// to change starting directories.
	// Since these are set before starting goroutines,
	// and then immutable, they should not need mutex protection.
	cliStartingDir string
	srvStartingDir string

	// where the server stores its data. Server.DataDir() to view.
	srvDataDir string

	// ServerAutoCreateClientsToDialOtherServers:
	// if the server is asked to connect to peer that
	// it does not currently have a connection to, setting
	// this to true means the server will spin up a client
	// automatically to dial out to create the connection.
	// This facilitates creating a cluster/grid of
	// communicating servers.
	ServerAutoCreateClientsToDialOtherServers bool

	// UseSimNet uses channels all in one
	// process, rather than network calls.
	// Client and Server must be in the same process.
	UseSimNet bool
}

// ClientStartingDir returns the directory the Client was started in.
func (c *Client) ClientStartingDir() string {
	return c.cfg.cliStartingDir
}

// ServerStartingDir returns the directory the Server was started in.
func (s *Server) ServerStartingDir() string {
	return s.cfg.srvStartingDir
}

func (s *Server) DataDir() string {
	return s.cfg.srvDataDir
}

// Clone returns a copy of cfg. This is a shallow copy to
// enable shared transport between a QUIC client and a QUIC
// server on the same port.
func (cfg *Config) Clone() *Config {
	clone := *cfg
	return &clone
}

func (cfg *Config) checkPreSharedKey(name string) {
	if cfg.PreSharedKeyPath != "" && fileExists(cfg.PreSharedKeyPath) {
		by, err := ioutil.ReadFile(cfg.PreSharedKeyPath)
		panicOn(err)
		if len(by) < 32 {
			panic(fmt.Sprintf("cfg.PreSharedKeyPath '%v' did not have 32 bytes of data in it.", cfg.PreSharedKeyPath))
		}

		// We might have gotten a file of hex string text instead of binary.
		// Or, we might have gotten some other user-defined stuff. Either
		// way still use all of it, but run through an HKDF for safety first.
		salt := make([]byte, 32)
		salt[0] = 43 // make it apparent, not a great salt.
		hkdf := hkdf.New(sha256.New, by, salt, nil)
		var finalKey [32]byte
		_, err = io.ReadFull(hkdf, finalKey[:])
		panicOn(err)
		by = finalKey[:]

		copy(cfg.preSharedKey[:], by)
		cfg.encryptPSK = true
		//alwaysPrintf("activated pre-shared-key on '%v' from cfg.PreSharedKeyPath='%v'", name, cfg.PreSharedKeyPath)
	}
}

type sharedTransport struct {
	mut           sync.Mutex
	quicTransport *quic.Transport
	shareCount    int
	isClosed      bool
}

// NewConfig should be used to create Config
// for use in NewClient or NewServer setup.
func NewConfig() *Config {
	return &Config{
		shared: &sharedTransport{},
	}
}

// A Client starts requests, and (might) wait for responses.
type Client struct {
	cfg *Config
	mut sync.Mutex

	simnet  *simnet
	simnode *simnode
	simconn *simnetConn

	// these are client only. server keeps track
	// per connection in their rwPair.
	// These ephemeral keys are from the ephemeral ECDH handshake
	// to estblish randomSymmetricSessKeyFromPreSharedKey.
	randomSymmetricSessKeyFromPreSharedKey [32]byte
	cliEphemPub                            []byte
	srvEphemPub                            []byte
	srvStaticPub                           ed25519.PublicKey

	name  string
	creds *selfcert.Creds

	notifyOnRead []chan *Message
	notifyOnce   map[uint64]*loquet.Chan[Message]

	notifies *notifies
	PeerAPI  *peerAPI // must be Exported to users!

	conn       uConnLR
	quicConn   quic.Connection
	quicConfig *quic.Config

	isTLS  bool
	isQUIC bool

	oneWayCh    chan *Message
	roundTripCh chan *Message

	halt *idem.Halter

	// internal use: if connecting succeeds,
	// a nil will be sent on this chan, otherwise
	// the error will be provided.
	connected chan error

	err error // detect inability to connect.

	lastSeqno uint64

	// net/rpc api implementation
	seenNetRPCCalls bool

	encBuf       bytes.Buffer // target for codec writes: encode into here first
	encBufWriter *bufio.Writer

	decBuf bytes.Buffer // target for code reads.

	codec *greenpackClientCodec

	// allow PingStats to be maintained.
	// note blabber shares, but we use
	// atomics to update and avoid races.
	cpair *cliPairState

	reqMutex sync.Mutex // protects following
	request  Request

	mutex sync.Mutex // protects following

	pending  map[uint64]*Call
	closing  bool // user has called Close
	shutdown bool // server has told us to stop

	fragLock     sync.Mutex
	recycledFrag []*Fragment

	// client gets its own keep alive message
	// to avoid data races with any server also here,
	// and will transmit epochV.EpochID in any
	// keep-alive ping.
	epochV       EpochVers
	keepAliveMsg Message
}

// Compute HMAC using SHA-256, so 32 bytes long.
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
//
// octx is an optional context, for early cancelling of
// a job. It can be nil.
func (c *Client) Go(serviceMethod string, args Green, reply Green, done chan *Call, octx context.Context) *Call {
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
			panic("rpc: done channel is unbuffered")
		}
	}
	call.Done = done
	//vv("Go() about to send()")
	c.send(call, octx)
	//vv("Go() back from send()")
	return call
}

// Call implements the net/rpc Client.Call() API; its docs:
//
// Call invokes the named function, waits for it to complete, and returns its error status.
//
// Added: octx is an optional context for cancelling the job.
// It can be nil.
func (c *Client) Call(serviceMethod string, args, reply Green, octx context.Context) error {
	c.mut.Lock()
	c.seenNetRPCCalls = true
	c.mut.Unlock()

	doneCh := make(chan *Call, 1)
	call := c.Go(serviceMethod, args, reply, doneCh, octx)
	select {
	case call = <-doneCh:
		return call.Error
	case <-c.halt.ReqStop.Chan:
		return ErrShutdown()
	}
}

func (c *Client) send(call *Call, octx context.Context) {
	c.reqMutex.Lock()
	defer c.reqMutex.Unlock()

	// Register this call.
	c.mutex.Lock()
	if c.shutdown || c.closing {
		c.mutex.Unlock()
		call.Error = ErrNetRpcShutdown
		call.done() // do call.Done <- call
		return
	}

	seq := c.nextSeqno()
	c.pending[seq] = call
	c.mutex.Unlock()

	// Encode and send the request.

	c.encBuf.Reset()
	c.encBufWriter.Reset(&c.encBuf)
	c.codec.enc.Reset(c.encBufWriter)

	c.request.Seq = seq
	c.request.ServiceMethod = call.ServiceMethod
	err := c.codec.WriteRequest(&c.request, call.Args)

	// should be in c.encBuf.Bytes() now
	//vv("Client.send(Call): c.encBuf.Bytes() is now len %v", len(c.encBuf.Bytes()))
	//vv("Client.send(Call): c.encBuf.Bytes() is now '%v'", string(c.encBuf.Bytes()))

	//vv("cli c.request.Seq = %v; request='%#v'", c.request.Seq, c.request)

	req := NewMessage()
	req.HDR.ServiceName = call.ServiceMethod
	req.HDR.Typ = CallNetRPC

	by := c.encBuf.Bytes()
	req.JobSerz = make([]byte, len(by))
	copy(req.JobSerz, by)

	var requestStopCh <-chan struct{}
	if octx != nil && !IsNil(octx) {
		requestStopCh = octx.Done()
		deadline, ok := octx.Deadline()
		if ok {
			req.HDR.Deadline = deadline
		}
		//vv("requestStopCh was set from octx")
	} // else leave it nil.

	reply, err := c.SendAndGetReply(req, requestStopCh, 0)
	_ = reply
	//vv("cli got reply '%v'; err = '%v'", reply, err)

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
			call.done() // do call.Done <- call
			//vv("client called call.done()")
		}
	}
}

// like net/rpc Client.input()
func (c *Client) gotNetRpcInput(replyMsg *Message) (err error) {

	var response Response

	c.decBuf.Reset()
	var nw int
	nw, err = c.decBuf.Write(replyMsg.JobSerz)
	if err != nil || nw != len(replyMsg.JobSerz) {
		panic(fmt.Sprintf("short write! nw=%v but len(replyMsgJobSerz) = %v", nw, len(replyMsg.JobSerz)))
	}
	c.codec.dec.Reset(&c.decBuf) // means read from this.
	//vv("gotNetRpcInput replyMsg.JobSerz is len %v", len(replyMsg.JobSerz))

	//decBufBy := c.decBuf.Bytes()
	//blak3 := blake3OfBytesString(decBufBy)
	//vv("c.decBuf has %v; blake3sum='%v'", len(decBufBy), blak3)

	err = c.codec.ReadResponseHeader(&response) // r.DecodeMsg(c.dec)
	panicOn(err)
	if err != nil {
		return err
	}
	//vv("reading header got '%#v', c.decBuf has %v", response, len(c.decBuf.Bytes()))

	seq := response.Seq
	c.mutex.Lock()
	//vv("c.pending looking for seq=%v is: '%#v'; response was '%#v'", seq, c.pending, response)
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
		//vv("no one to give body to? pending = '%#v'", c.pending)
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
		//vv("response.Error was '%v'", response.Error)
	default:
		err = c.codec.ReadResponseBody(call.Reply)
		if err != nil {
			call.Error = errors.New("reading body " + err.Error())
		}
		call.done()
		//vv("default cli switch on call")
	}
	return nil
}

// any pending calls are unlocked with err set.
func (c *Client) netRpcShutdownCleanup(err error) {
	//vv("netRpcShutdownCleanup called.")

	// Terminate pending calls.
	c.reqMutex.Lock()
	c.mutex.Lock()
	c.shutdown = true
	closing := c.closing
	if err == io.EOF {
		if closing {
			err = ErrNetRpcShutdown
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
		alwaysPrintf("rpc: client protocol error: '%v'", err)
	}
}

// original, not net/rpc derived below:

func (c *Client) IsDown() (down bool) {
	c.mut.Lock()
	down = c.halt.ReqStop.IsClosed()
	c.mut.Unlock()
	if down {
		//vv("c.halt.ReqStop has been closed, IsDown returning true")
		return
	}
	c.mutex.Lock()
	down = c.shutdown
	c.mutex.Unlock()
	//vv("IsDown returning %v after checking on c.shutdown", down)
	return
}

// Err returns any Client stored error.
func (c *Client) Err() error {
	return c.err
}

// GetReadIncomingCh creates and returns
// a buffered channel that reads incoming
// messages that are server-pushed (not associated
// with a round-trip rpc call request/response pair).
func (c *Client) GetReadIncomingCh() (ch chan *Message) {
	ch = make(chan *Message, 100)
	//vv("GetReadIncommingCh is %p on client '%v'", ch, c.name)
	c.GetReads(ch)
	return
}

// GetReadIncomingChForCallID creates and returns
// a buffered channel that reads incoming
// messages that are server-pushed (not associated
// with a round-trip rpc call request/response pair).
// It filters for those with callID.
func (c *Client) GetReadIncomingChForCallID(callID string) (ch chan *Message) {
	ch = make(chan *Message, 100)
	//vv("GetReadIncommingCh is %p on client '%v'", ch, c.name)
	c.GetReadsForCallID(ch, callID)
	return
}

func (c *Client) GetErrorChForCallID(callID string) (ch chan *Message) {
	ch = make(chan *Message, 100)
	//vv("GetReadIncommingCh is %p on client '%v'", ch, c.name)
	c.GetErrorsForCallID(ch, callID)
	return
}

// GetReads registers to get any received messages on ch.
// It is similar to GetReadIncomingCh but for when ch
// already exists and you do not want a new one.
// It filters for CallID
func (c *Client) GetReadsForCallID(ch chan *Message, callID string) {
	//vv("GetReads called! stack='\n\n%v\n'", stack())
	if cap(ch) == 0 {
		panic("ch must be bufferred")
	}
	c.notifies.notifyOnReadCallIDMap.set(callID, ch)
}

func (c *Client) GetErrorsForCallID(ch chan *Message, callID string) {
	//vv("GetReads called! stack='\n\n%v\n'", stack())
	if cap(ch) == 0 {
		panic("ch must be bufferred")
	}
	c.notifies.notifyOnErrorCallIDMap.set(callID, ch)
}

// GetReads registers to get any received messages on ch.
// It is similar to GetReadIncomingCh but for when ch
// already exists and you do not want a new one.
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
func (c *Client) GetOneRead(seqno uint64, ch *loquet.Chan[Message]) {
	c.mut.Lock()
	defer c.mut.Unlock()
	c.notifyOnce[seqno] = ch
}

func (c *Client) UngetOneRead(seqno uint64, ch *loquet.Chan[Message]) {
	c.mut.Lock()
	defer c.mut.Unlock()
	prev, ok := c.notifyOnce[seqno]
	if ok && prev == ch {
		delete(c.notifyOnce, seqno)
	}
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

// NewClient creates a new client. Call Start() to begin a connection.
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
	c.setDefaults(config)
	c = &Client{
		cfg:         cfg,
		name:        name,
		oneWayCh:    make(chan *Message), // not buffered! synchronous so we get back-pressure.
		roundTripCh: make(chan *Message), // not buffered! synchronous so we get back-pressure.
		halt:        idem.NewHalter(),
		connected:   make(chan error, 1),
		lastSeqno:   1,
		notifyOnce:  make(map[uint64]*loquet.Chan[Message]),

		// share code with server for CallID and ToPeerID callbacks.
		notifies: newNotifies(yesIsClient),
		// net/rpc
		pending: make(map[uint64]*Call),
		epochV:  EpochVers{EpochTieBreaker: NewCallID("")},
	}
	c.keepAliveMsg.HDR.Typ = CallKeepAlive
	c.keepAliveMsg.HDR.Subject = c.epochV.EpochTieBreaker

	c.PeerAPI = newPeerAPI(c, yesIsClient, cfg.UseSimNet)
	c.encBufWriter = bufio.NewWriter(&c.encBuf)
	c.codec = &greenpackClientCodec{
		cli:          c,
		rwc:          nil,
		dec:          msgp.NewReader(&c.decBuf),
		enc:          msgp.NewWriter(c.encBufWriter),
		encBufWriter: c.encBufWriter,
		encBufItself: &c.encBuf,
	}
	return c, nil
}

func (c *Client) setDefaults(cfg *Config) {
	if !cfg.CompressionOff {
		if cfg.CompressAlgo == "" {
			cfg.CompressAlgo = DefaultUseCompressAlgo
		}
	}
}

// Start dials the server.
// That is, Start attemps to connect to config.ClientDialToHostPort.
// The err will come back with any problems encountered.
func (c *Client) Start() (err error) {

	c.cfg.cliStartingDir, err = os.Getwd()
	if err != nil {
		return fmt.Errorf("rpc25519.Client.Start() could not Getwd(): '%v'", err)
	}

	go c.runClientMain(c.cfg.ClientDialToHostPort, c.cfg.TCPonly_no_TLS, c.cfg.CertPath)

	// wait for connection (or not).
	err = <-c.connected // hung here in test702
	return err
}

// Name reports the name the Client was created with.
func (c *Client) Name() string {
	return c.name
}

// Close shuts down the Client.
func (c *Client) Close() error {
	//vv("Client.Close() called.") // not seen in shutdown.

	// ask any sub components (peer pump loops) to stop.
	c.halt.StopTreeAndWaitTilDone(500*time.Millisecond, nil, nil)

	if c.cfg.UseQUIC {
		if c.isQUIC && c.quicConn != nil {
			// try to tell server we are gone before
			// we tear down the communication framework.
			c.quicConn.CloseWithError(0, "")
			//vv("cli quicConn.CloseWithError(0) sent.")
		}
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

var ErrShutdown2 = fmt.Errorf("shutting down")

func ErrShutdown() error {
	return ErrShutdown2
}

var ErrCancelChanClosed = fmt.Errorf("rpc25519 error: request cancelled/done channel closed")
var ErrTimeout = fmt.Errorf("time-out waiting for call to complete")
var ErrCancelReqSent = fmt.Errorf("cancellation request sent")

// SendAndGetReplyWithTimeout expires the call after
// timeout.
func (c *Client) SendAndGetReplyWithTimeout(timeout time.Duration, req *Message) (reply *Message, err error) {
	t0 := time.Now()
	req.HDR.Deadline = t0.Add(timeout)
	defer func() {
		elap := time.Since(t0)
		if elap > 5*time.Second {
			alwaysPrintf("SendAndGetReplyWithTimeout(timeout='%v') is returning after %v", timeout, elap)
		}
	}()

	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
	defer cancelFunc()

	return c.SendAndGetReply(req, ctx.Done(), timeout)
}

// SendAndGetReplyWithCtx is like SendAndGetReply(), with
// the additional feature that it will send a remote cancellation request
// when the ctx is cancelled or if the ctx.Deadline() time
// (if set) is surpassed. Note that none of the Values inside
// ctx, if set, will be transmitted to the remote call, because the
// context.Context API provides no method to enumerate them.
// Such values are likely not serializable in any case.
//
// A similar deadline effect can be acheived just by
// setting the req.HDR.Deadline field in a SendAndGetReply()
// call. This may also be more efficient on the client,
// because the client need not wait for the remote
// cancellation response to be sent and received.
// However this is a little racey: the server could
// suceed and be in the process of replying when
// the client hits the deadline. In this case the
// client might retry a call that actually did
// finish, and end up doing the call twice. This
// is also always a hazard with servers crashing
// before they can finish responding. Ideally
// server APIs are idempotent to guard against this.
//
// If the req.HDR.Deadline is already set (not zero),
// then we do not touch it. If it is zero and
// ctx has a deadline, we set it as the req.HDR.Deadline.
// We leave it to the user to coordinate/update these two
// ways of setting a dealine, knowing that the
// req.HDR.Deadline will win, if set.
func (c *Client) SendAndGetReplyWithCtx(ctx context.Context, req *Message, errWriteDur time.Duration) (reply *Message, err error) {

	if req.HDR.Deadline.IsZero() {
		deadline, ok := ctx.Deadline()
		if ok {
			req.HDR.Deadline = deadline
		}
	}
	return c.SendAndGetReply(req, ctx.Done(), errWriteDur)
}

// SendAndGetReply starts a round-trip RPC call.
// We will wait for a response before returning.
// The requestStopCh is optional; it can be nil. A
// context.Done() like channel can be supplied there to
// cancel the job before a reply comes back.
//
// UPDATE: a DEFAULT timeout is in force now. Because
// server failure or blink (down then up) can
// leave us stalled forever, we put in a default
// timeout of 10 seconds, if not otherwise
// specified. If you expect your call to take
// more than a few seconds, you should set
// the timeout directly with
// SendAndGetReplyWithTimeout() or pass in
// a cancelJobCh here to manage it. Otherwise, to
// handle the common case when we expect very
// fast replies, if cancelJobCh is nil, we will
// cancel the job if it has not finished after 10 seconds.
func (c *Client) SendAndGetReply(req *Message, cancelJobCh <-chan struct{}, errWriteDur time.Duration) (reply *Message, err error) {

	var defaultTimeout <-chan time.Time
	// leave deafultTimeout nil if user supplied a cancelJobCh.
	if cancelJobCh == nil {
		// try hard not to get stuck when server goes away.
		//defaultTimeout = c.TimeAfter(20 * time.Second)
		timer := c.NewTimer(20 * time.Second)
		defer timer.Discard() // let simnet know the timer can be GC-ed.
		defaultTimeout = timer.C
	}
	var writeDurTimeoutChan <-chan time.Time
	if errWriteDur > 0 {
		ti := c.NewTimer(errWriteDur)
		defer ti.Discard()
		writeDurTimeoutChan = ti.C
	}

	var from, to string
	if c.isQUIC {
		from = local(c.quicConn)
		to = remote(c.quicConn)
	} else {
		from = local(c.conn)
		to = remote(c.conn)

		// diagnostics for simnet.
		//sc, ok := c.conn.(*simnetConn)
		//_ = sc
		//if ok {
		//	//vv("isCli=%v; c.conn.netAddr = '%v'; simnetConn.local='%v'; remote='%v'; sc.netAddr='%v'", sc.isCli, sc.netAddr, sc.local.name, sc.remote.name, sc.netAddr)
		//}
	}

	req.HDR.To = to
	req.HDR.From = from

	// preserve the deadline, but
	// don't lengthen deadline if it
	// is already shorter.
	var hdrCtxDone <-chan struct{}

	// Allow user to adjust deadline however they like.
	// But if not set, set it if context has one.
	if req.HDR.Ctx != nil && !IsNil(req.HDR.Ctx) {
		hdrCtxDone = req.HDR.Ctx.Done()
		if req.HDR.Deadline.IsZero() {
			req.HDR.Deadline, _ = req.HDR.Ctx.Deadline()
		}
	}

	// preserve created at time.
	if req.HDR.Created.IsZero() {
		req.HDR.Created = time.Now()
	}
	req.HDR.Serial = issueSerial()

	// don't override a CallNetRPC, or a streaming type.
	if req.HDR.Typ == CallNone {
		req.HDR.Typ = CallRPC
	}
	if req.HDR.CallID == "" {
		req.HDR.CallID = NewCallID(req.HDR.ServiceName)
		// let loop assign Seqno.
	}

	//vv("Client '%v' SendAndGetReply(req='%v') (ignore req.Seqno:0 not yet assigned)", c.name, req)
	select {
	case c.roundTripCh <- req:
		// proceed
		//vv("Client '%v' SendAndGetReply(req='%v') delivered on roundTripCh", c.name, req)
	case <-cancelJobCh:
		//vv("Client '%v' SendAndGetReply(req='%v'): cancelJobCh files before roundTripCh", c.name, req)
		return nil, ErrCancelChanClosed

	case <-hdrCtxDone:
		// support passing in a req with req.HDR.Ctx set.
		return nil, ErrCancelChanClosed

	case <-defaultTimeout:
		// definitely a timeout
		//vv("ErrTimeout being returned from SendAndGetReply()")
		return nil, ErrTimeout

	case <-writeDurTimeoutChan:
		return nil, ErrTimeout

	case <-c.halt.ReqStop.Chan:
		//vv("Client '%v' SendAndGetReply(req='%v'): sees halt.ReqStop before roundTripCh <- req", c.name, req)
		c.halt.Done.Close()
		return nil, ErrShutdown()
	}

	//vv("client '%v' to wait on req.DoneCh; after sending req='%v'", c.name, req) // seen 040

	select {
	case <-req.DoneCh.WhenClosed():
		//reply = req.nextOrReply // not set anywhere yet
		reply, _ = req.DoneCh.Read()
		if reply != nil {
			err = reply.LocalErr
		}
		//vv("client.SendAndGetReply() got on reply.Err = '%v'", err)
		return
	case <-cancelJobCh:
		// usually a timeout

		// tell remote to cancel the job.
		// We re-use same req message channel,
		// so our client caller gets a reponse on it,
		// and can unblock themselves.
		// Since this can race with the original reply,
		// we've enlarged the DoneCh channel buffer
		// to be size 2 now, in the NewMessage() implementation.
		cancelReq := &Message{DoneCh: req.DoneCh}
		cancelReq.HDR = req.HDR
		cancelReq.HDR.Typ = CallCancelPrevious
		c.oneWaySendHelper(cancelReq, nil, errWriteDur)
		return nil, ErrCancelReqSent

	case <-defaultTimeout:
		// definitely a timeout
		//vv("ErrTimeout being returned from SendAndGetReply(), 2nd part")
		return nil, ErrTimeout

	case <-writeDurTimeoutChan:
		return nil, ErrTimeout

	case <-c.halt.ReqStop.Chan:
		//vv("Client '%v' SendAndGetReply(req='%v'): sees halt.ReqStop", c.name, req) // here

		c.halt.Done.Close()
		return nil, ErrShutdown()
	}
}

// Uploader helps the client to make a series of non-blocking
// (one-way) calls to a remote server's UploadReaderFunc
// which must have been already registered on the server.
type Uploader struct {
	mut    sync.Mutex
	cli    *Client
	next   int64
	callID string
	done   bool
	name   string
	seqno  uint64
	ReadCh <-chan *Message

	ErrorCh <-chan *Message
}

func (s *Uploader) CallID() string {
	return s.callID
}

// UploadBegin sends the msg to the server
// to execute with the func that has registed
// with RegisterUploaderReadererFunc() -- at the
// moment there can only be one such func
// registered at a time. UploadBegin() will
// contact it, and Uploader.UploadMore() will,
// as it suggests, send another Message.
//
// We maintain FIFO arrival of Messages
// at the server as follows (despite having
// each server side func callback executing in a
// goroutine).
//
// 1. Since the client side uses the same
// channel into the send loop for both
// UploadBegin and UploadMore, these
// calls will be properly ordered into
// the send loop on the client/sending side.
//
// 2. The TCP/QUIC stream maintains
// FIFO order of its messages as it
// delivers them to the server.
//
// 3. On the server, in the TCP/QUIC
// read loop, we queue messages in
// FIFO order into a large buffered
// channel before we spin up a goroutine
// once at UploadBegin time to handle
// all the subsequent messages in the
// order they were queued.
//
// This also yeilds an efficient design.
// While normal OneWayFunc and TwoWayFunc
// messages each start their own new goroutine
// to avoid long-running functions starving
// the server's read loop, a UploadReadFunc
// only utilizes a single new goroutine to
// process all messages sent in a stream.
func (c *Client) UploadBegin(
	ctx context.Context,
	serviceName string,
	msg *Message,
	errWriteDur time.Duration,

) (strm *Uploader, err error) {

	msg.HDR.Typ = CallUploadBegin
	msg.HDR.ServiceName = serviceName
	msg.HDR.StreamPart = 0
	cancelJobCh := ctx.Done()
	seqno := c.nextSeqno()
	msg.HDR.Seqno = seqno

	err = c.OneWaySend(msg, cancelJobCh, errWriteDur)
	if err != nil {
		return nil, err
	}
	return &Uploader{
		cli:     c,
		next:    1,
		callID:  msg.HDR.CallID,
		ReadCh:  c.GetReadIncomingChForCallID(msg.HDR.CallID),
		ErrorCh: c.GetErrorChForCallID(msg.HDR.CallID),
		name:    serviceName,
		seqno:   seqno,
	}, nil
}

var ErrAlreadyDone = fmt.Errorf("Uploader has already been marked done. No more sending is allowed.")

func (s *Uploader) UploadMore(ctx context.Context, msg *Message, last bool, errWriteDur time.Duration) (err error) {
	s.mut.Lock()
	defer s.mut.Unlock()

	if s.done {
		return ErrAlreadyDone
	}

	if last {
		msg.HDR.Typ = CallUploadEnd
		s.done = true
	} else {
		msg.HDR.Typ = CallUploadMore
	}
	msg.HDR.ServiceName = s.name
	msg.HDR.StreamPart = s.next
	msg.HDR.CallID = s.callID
	msg.HDR.Seqno = s.seqno
	// set deadline too!
	msg.HDR.Ctx = ctx
	deadline, _ := ctx.Deadline()
	msg.HDR.Deadline = deadline

	s.next++
	return s.cli.OneWaySend(msg, ctx.Done(), errWriteDur)
}

// UniversalCliSrv allows protocol objects to exist
// and run on either Client or Server. They
// can be implemented to just use this interface.
type UniversalCliSrv interface {
	GetConfig() *Config
	RegisterPeerServiceFunc(peerServiceName string, psf PeerServiceFunc) error

	StartLocalPeer(ctx context.Context, peerServiceName string, requestedCircuit *Message) (lpb *LocalPeer, err error)

	StartRemotePeer(ctx context.Context, peerServiceName, remoteAddr string, waitUpTo time.Duration) (remotePeerURL, RemotePeerID string, err error)

	StartRemotePeerAndGetCircuit(lpb *LocalPeer, circuitName string, frag *Fragment, peerServiceName, remoteAddr string, waitUpTo time.Duration) (ckt *Circuit, err error)

	SendOneWayMessage(ctx context.Context, msg *Message, errWriteDur time.Duration) (error, chan *Message)

	GetReadsForCallID(ch chan *Message, callID string)
	GetErrorsForCallID(ch chan *Message, callID string)

	// for Peer/Object systems; ToPeerID gets priority over CallID
	// to allow such systems to implement custom message
	// types. An example is the Fragment/Peer/Circuit system.
	// (This priority is implemented in notifies.handleReply_to_CallID_ToPeerID).
	GetReadsForToPeerID(ch chan *Message, objID string)
	GetErrorsForToPeerID(ch chan *Message, objID string)

	UnregisterChannel(ID string, whichmap int)
	LocalAddr() string
	RemoteAddr() string // client provides, server gives ""

	// allow peers to find out that the host Client/Server is stopping.
	GetHostHalter() *idem.Halter

	// fragment memory recycling, to avoid heap pressure.
	NewFragment() *Fragment
	FreeFragment(frag *Fragment)
	RecycleFragLen() int
	PingStats(remote string) *PingStat
	AutoClients() (list []*Client, isServer bool)

	NewTimer(dur time.Duration) (ti *RpcTimer)
}

type PingStat struct {
	LastPingSentTmu     int64
	LastPingReceivedTmu int64
}

func (s *PingStat) String() string {
	return fmt.Sprintf(`PingStat{
    LastPingSentTmu: "%v", 
LastPingReceivedTmu: "%v",
}`, time.Unix(s.LastPingSentTmu/1e9, s.LastPingSentTmu%1e9), time.Unix(s.LastPingReceivedTmu/1e9, s.LastPingReceivedTmu%1e9))
}

func (c *Client) PingStats(remote string) *PingStat {
	//vv("Client.PingStats called.")
	return &PingStat{
		LastPingSentTmu:     c.cpair.lastPingSentTmu.Load(),
		LastPingReceivedTmu: c.cpair.lastPingReceivedTmu.Load(),
	}
}

// maintain the requirement that Client and Server both
// implement UniversalCliSrv.
var _ UniversalCliSrv = &Client{}
var _ UniversalCliSrv = &Server{}

// for symmetry: see srv.go for details, under the same func name.
//
// SendOneWayMessage only sets msg.HDR.From to its correct value.
func (cli *Client) SendOneWayMessage(ctx context.Context, msg *Message, errWriteDur time.Duration) (error, chan *Message) {
	return sendOneWayMessage(cli, ctx, msg, errWriteDur)
}

// implements the oneWaySender interface for symmetry with Server.
func (cli *Client) destAddrToSendCh(destAddr string) (sendCh chan *Message, haltCh chan struct{}, to, from string, ok bool) {
	// future: allow client to contact multple servers.
	// for now, only the original client->sever conn is supported.

	//defer func() {
	//	vv("Client.destAddrToSendCh() returning ok = '%v'", ok)
	//}()

	if cli.isQUIC {
		from = local(cli.quicConn)
		to = remote(cli.quicConn)
	} else {
		from = local(cli.conn)
		to = remote(cli.conn)
	}
	_ = from
	// fill default if empty, like fragment.go converting Fragment to Message does.
	if destAddr == "" {
		destAddr = to
	}

	// commenting out this check and error was essential to having
	// the client talk to a multi-interface bound server 0.0.0.0:8443.
	//
	// panic: ugh: cli wanted to send to destAddr='tcp://100.114.32.72:8443' but we only know to='tcp://127.0.0.1:8443' => b/c the server was bound to both... we should just let it go through.
	/*	const allowAnyway = true
		if destAddr != to && !allowAnyway {
			problem := fmt.Sprintf("ugh: cli wanted to send to destAddr='%v' "+
				"but we only know to='%v' / from ='%v'", destAddr, to, from)
			panic(problem)
			alwaysPrintf("%v\n", problem)
			return nil, nil, "", "", false
		}
	*/
	//vv("cli okay with destAddr '%v' == to", destAddr)
	haltCh = cli.halt.ReqStop.Chan
	sendCh = cli.oneWayCh
	ok = true
	return
}

// OneWaySend sends a message without expecting or waiting for a response.
// The cancelJobCh is optional, and can be nil.
// If msg.HDR.CallID is set, we will preserve it.
func (c *Client) OneWaySend(msg *Message, cancelJobCh <-chan struct{}, errWriteDur time.Duration) (err error) {

	var from, to string
	if c.isQUIC {
		from = local(c.quicConn)
		to = remote(c.quicConn)
	} else {
		from = local(c.conn)
		to = remote(c.conn)
	}

	msg.HDR.To = to
	msg.HDR.From = from

	// preserve created at too.
	if msg.HDR.Created.IsZero() {
		msg.HDR.Created = time.Now()
	}
	msg.HDR.Serial = issueSerial()

	//case CallUploadMore, CallUploadEnd, CallRequestBistreaming,
	// CallRequestDownload:
	//
	// Basically, always want to preserve it if allocated!
	// must preserve the CallID on streaming calls.

	switch msg.HDR.Typ {
	// preserve streaming call types.
	case CallNone:
		msg.HDR.Typ = CallOneWay
	}
	if msg.HDR.CallID == "" {
		msg.HDR.CallID = NewCallID(msg.HDR.ServiceName)
	}

	// allow msg.CallID to not be empty; in case we get a reply.
	// isRPC=false so this is 1-way, but it might in turn still
	// generate a response.

	//vv("one way send '%v'", msg.HDR.String())
	return c.oneWaySendHelper(msg, cancelJobCh, errWriteDur)
}

// cancel uses this too, so we don't change the CallID.
func (c *Client) oneWaySendHelper(msg *Message, cancelJobCh <-chan struct{}, errWriteDur time.Duration) (err error) {

	select {
	case c.oneWayCh <- msg:

		// Experiement: for backpressure/fast error: comment this out:
		// return nil // not worth waiting for anything more?
		// and wait for the sendMessage() in the sendLoop to complete:

		select {
		case <-msg.DoneCh.WhenClosed():
			return msg.LocalErr

		case <-c.halt.ReqStop.Chan:
			c.halt.Done.Close()
			return ErrShutdown()
		}

	case <-cancelJobCh:
		return ErrCancelChanClosed

	case <-c.halt.ReqStop.Chan:
		c.halt.Done.Close()
		return ErrShutdown()
	}
}

func (c *Client) setLocalAddr(conn localRemoteAddr) {
	c.mut.Lock()
	defer c.mut.Unlock()

	c.cfg.localAddress = local(conn)
	AliasRegister(c.cfg.localAddress, c.cfg.localAddress+" (client: "+c.name+")")
}

// LocalAddr retreives the local host/port that the
// Client is calling from.
func (c *Client) LocalAddr() string {
	c.mut.Lock()
	defer c.mut.Unlock()
	return c.cfg.localAddress
}

// RemoteAddr retreives the remote host/port for
// the Server that the Client is connected to.
func (c *Client) RemoteAddr() string {
	c.mut.Lock()
	defer c.mut.Unlock()

	if c.isQUIC {
		return remote(c.quicConn)
	} else {
		return remote(c.conn)
	}
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
	n = atomic.AddUint64(&c.lastSeqno, 1)
	return
}

// SelfyNewKey is only for testing, not production.
// It is used by the tests to check that certs
// are signed by the expected CA.
//
// SelfyNewKey will generate a self-signed certificate
// authority, a new ed25519 key pair, sign the public
// key to create a cert, and write these four
// new files to disk. The directories
// odir/my-keep-private-dir and odir/certs will be created,
// based on the odir argument.
// For a given createKeyPairNamed name, we will
// create odir/certs/name.crt and odir/certs/name.key files.
// The odir/certs/name.key and my-keep-private-dir/ca.key files
// contain private keys and should be kept confidential.
// The `selfy` command in this package can be used to produce the
// same keys but with password protection, which is
// recommended.
func SelfyNewKey(createKeyPairNamed, odir string) error {
	odirPrivateKey := odir + sep + "my-keep-private-dir"
	odirCerts := odir + sep + "certs"
	host, _ := os.Hostname()
	email := createKeyPairNamed + "@" + host

	const verbose = false
	const encryptWithPassphhrase = false

	caValidForDur := 36600 * 24 * time.Hour // 100 years validity
	if !dirExists(odirPrivateKey) || !fileExists(odirPrivateKey+sep+"ca.crt") {
		//vv("key-pair '%v' requested but CA does not exist in '%v', so auto-generating a self-signed CA for your first.", createKeyPairNamed, odirPrivateKey)
		selfcert.Step1_MakeCertificateAuthority(odirPrivateKey, verbose, encryptWithPassphhrase, caValidForDur)
	}

	privKey, err := selfcert.Step2_MakeEd25519PrivateKey(createKeyPairNamed, odirCerts, verbose, encryptWithPassphhrase)
	panicOn(err)
	selfcert.Step3_MakeCertSigningRequest(privKey, createKeyPairNamed, email, odirCerts)
	certGoodForDur := 36600 * 24 * time.Hour // 100 years validity
	selfcert.Step4_MakeCertificate(nil, odirPrivateKey, createKeyPairNamed, odirCerts, certGoodForDur, verbose)

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

func fixSlash(s string) string {
	if runtime.GOOS != "windows" {
		return s
	}
	return strings.Replace(s, "/", "\\", -1)
}

func (c *Client) setupPSK(conn uConn) error {
	if c.cfg.TCPonly_no_TLS || !c.cfg.encryptPSK {
		return nil
	}
	if useVerifiedHandshake {
		randomSymmetricSessKey, cliEphemPub, srvEphemPub, srvStaticPub, err :=
			symmetricClientVerifiedHandshake(conn, c.cfg.preSharedKey, c.creds)
		if err != nil {
			return err
		}
		c.randomSymmetricSessKeyFromPreSharedKey = randomSymmetricSessKey
		c.cliEphemPub = cliEphemPub
		c.srvEphemPub = srvEphemPub
		c.srvStaticPub = srvStaticPub
	} else {
		if wantForwardSecrecy {
			randomSymmetricSessKey, cliEphemPub, srvEphemPub, srvStaticPub, err :=
				symmetricClientHandshake(conn, c.cfg.preSharedKey, c.creds)
			if err != nil {
				return err
			}
			c.randomSymmetricSessKeyFromPreSharedKey = randomSymmetricSessKey
			c.cliEphemPub = cliEphemPub
			c.srvEphemPub = srvEphemPub
			c.srvStaticPub = srvStaticPub
		} else {
			if mixRandomnessWithPSK {
				randomSymmetricSessKey, err := simpleSymmetricClientHandshake(conn, c.cfg.preSharedKey, c.creds)
				if err != nil {
					return err
				}
				c.randomSymmetricSessKeyFromPreSharedKey = randomSymmetricSessKey
			} else {
				c.randomSymmetricSessKeyFromPreSharedKey = c.cfg.preSharedKey
			}

		}
	}
	return nil
}

// Downloader is used when the client receives stream from server.
// It is returned by RequestDownload() or NewDownloader().
type Downloader struct {
	mut   sync.Mutex
	seqno uint64

	ReadDownloadsCh <-chan *Message
	ErrorCh         <-chan *Message

	name   string
	cli    *Client
	callID string
}

func (s *Downloader) CallID() string {
	return s.callID
}
func (s *Downloader) Name() string {
	return s.name
}
func (s *Downloader) Seqno() uint64 {
	return s.seqno
}

// cancel any outstanding call
func (s *Downloader) Close() {
	req := NewMessage()
	req.HDR.ServiceName = s.name
	req.HDR.Typ = CallCancelPrevious
	req.HDR.CallID = s.callID
	req.HDR.Seqno = s.seqno
	s.cli.OneWaySend(req, nil, -1) // best effort
}

func (c *Client) NewDownloader(ctx context.Context, streamerName string) (downloader *Downloader, err error) {
	callID := NewCallID(streamerName)
	downloader = &Downloader{
		cli:             c,
		callID:          callID,
		ReadDownloadsCh: c.GetReadIncomingChForCallID(callID),
		ErrorCh:         c.GetErrorChForCallID(callID),
		name:            streamerName,
		seqno:           c.nextSeqno(),
	}
	return
}

func (c *Client) RequestDownload(ctx context.Context, streamerName, path string) (downloader *Downloader, err error) {

	downloader, err = c.NewDownloader(ctx, streamerName)
	if err != nil {
		return
	}
	return downloader, downloader.BeginDownload(ctx, path)
}

func (b *Downloader) BeginDownload(ctx context.Context, path string) (err error) {

	cli := b.cli
	//seqno := b.seqno
	//vv("downloader to use seqno %v", seqno)

	var from, to string
	if cli.isQUIC {
		from = local(cli.quicConn)
		to = remote(cli.quicConn)
	} else {
		from = local(cli.conn)
		to = remote(cli.conn)
	}
	// from bistream

	req := NewMessage()
	req.HDR.Created = time.Now()
	req.HDR.To = to
	req.HDR.From = from
	req.HDR.ServiceName = b.name
	req.HDR.StreamPart = 0
	req.HDR.Typ = CallRequestDownload
	req.HDR.Seqno = b.seqno

	req.HDR.Serial = issueSerial()
	req.HDR.CallID = b.callID
	req.HDR.Ctx = ctx
	req.HDR.Deadline, _ = ctx.Deadline()

	req.HDR.Args = map[string]string{"downloadRequestedPath": path}

	//vv("Downloader.Begin(): sending req = '%v'", req.String())

	// NOTE THE SEND IS HERE! ALL BE READY BY NOW!
	err = cli.OneWaySend(req, ctx.Done(), 0)
	if err != nil {
		return
	}

	// This waits for the req to actually be sent on the wire.
	// It does not wait for any replies though.
	select {
	case <-req.DoneCh.WhenClosed():
		return req.LocalErr
	case <-cli.halt.ReqStop.Chan:
		return ErrShutdown()
	case <-ctx.Done():
		return ErrContextCancelled
	}
}

// Bistreamer is the client side handle to talking
// with a server func that does bistreaming: the
// client can stream to the server func, and the
// server func can, symmetrically, stream to the client.
// The basics of TCP are finally available to users.
type Bistreamer struct {
	mut   sync.Mutex
	seqno uint64

	ReadDownloadsCh <-chan *Message
	WriteCh         chan<- *Message
	ErrorCh         <-chan *Message

	name string

	cli    *Client
	next   int64
	callID string
	done   bool
}

func (s *Bistreamer) CallID() string {
	return s.callID
}
func (s *Bistreamer) Name() string {
	return s.name
}
func (s *Bistreamer) Seqno() uint64 {
	return s.seqno
}

func (s *Bistreamer) UploadMore(ctx context.Context, msg *Message, last bool, errWriteDur time.Duration) (err error) {

	s.mut.Lock()
	defer s.mut.Unlock()

	if s.done {
		return ErrAlreadyDone
	}

	if last {
		msg.HDR.Typ = CallUploadEnd
		s.done = true
	} else {
		msg.HDR.Typ = CallUploadMore
	}
	msg.HDR.ServiceName = s.name
	msg.HDR.StreamPart = s.next
	msg.HDR.CallID = s.callID
	msg.HDR.Seqno = s.seqno
	msg.HDR.Ctx = ctx
	deadline, _ := ctx.Deadline()
	msg.HDR.Deadline = deadline

	s.next++
	return s.cli.OneWaySend(msg, ctx.Done(), errWriteDur)
}

// NewBistream creates a new Bistreamer but does no communication yet. Just for
// setting up reader/writer goroutines before everything starts.
func (c *Client) NewBistreamer(bistreamerName string) (b *Bistreamer, err error) {
	callID := NewCallID(bistreamerName)
	b = &Bistreamer{
		next:            1,
		cli:             c,
		callID:          callID,
		ReadDownloadsCh: c.GetReadIncomingChForCallID(callID),
		ErrorCh:         c.GetErrorChForCallID(callID),
		name:            bistreamerName,
		seqno:           c.nextSeqno(),
	}
	return b, nil
}

func (c *Client) RequestBistreaming(ctx context.Context, bistreamerName string, req *Message) (b *Bistreamer, err error) {

	b, err = c.NewBistreamer(bistreamerName)
	if err != nil {
		return nil, err
	}
	return b, b.Begin(ctx, req)
}

// cancel any outstanding call
func (b *Bistreamer) Close() {
	//vv("Bistreamer.Close() called.")
	req := NewMessage()
	req.HDR.ServiceName = b.name
	req.HDR.Typ = CallCancelPrevious
	req.HDR.CallID = b.callID
	req.HDR.Seqno = b.seqno
	b.cli.OneWaySend(req, nil, -1) // best effort
}

func (b *Bistreamer) Begin(ctx context.Context, req *Message) (err error) {

	if req == nil {
		req = NewMessage()
	}

	var from, to string
	if b.cli.isQUIC {
		from = local(b.cli.quicConn)
		to = remote(b.cli.quicConn)
	} else {
		from = local(b.cli.conn)
		to = remote(b.cli.conn)
	}

	req.HDR.To = to
	req.HDR.From = from
	req.HDR.ServiceName = b.name
	req.HDR.StreamPart = 0
	req.HDR.Typ = CallRequestBistreaming
	req.HDR.Seqno = b.seqno

	// preserve created at too.
	if req.HDR.Created.IsZero() {
		req.HDR.Created = time.Now()
	}
	req.HDR.Serial = issueSerial()
	req.HDR.CallID = b.callID

	//vv("RequestBistreaming, req.HDR = '%v'", req.HDR.String())

	if req.HDR.Deadline.IsZero() {
		req.HDR.Deadline, _ = ctx.Deadline()
	}

	//vv("Bistreamer.Begin(): sending req = '%v'", req.String())
	err = b.cli.OneWaySend(req, ctx.Done(), 0)
	if err != nil {
		return
	}

	return
}

func (s *Client) GetReadsForToPeerID(ch chan *Message, objID string) {
	//vv("GetReads called! stack='\n\n%v\n'", stack())
	if cap(ch) == 0 {
		panic("ch must be bufferred")
	}
	s.notifies.notifyOnReadToPeerIDMap.set(objID, ch)
}

func (s *Client) GetErrorsForToPeerID(ch chan *Message, objID string) {
	//vv("GetReads called! stack='\n\n%v\n'", stack())
	if cap(ch) == 0 {
		panic("ch must be bufferred")
	}
	s.notifies.notifyOnErrorToPeerIDMap.set(objID, ch)
}

// whichmap meanings for UnregisterChannel
const (
	CallIDReadMap    = 0
	CallIDErrorMap   = 1
	ToPeerIDReadMap  = 2
	ToPeerIDErrorMap = 3
)

func (s *Client) UnregisterChannel(ID string, whichmap int) {

	switch whichmap {
	case CallIDReadMap:
		s.notifies.notifyOnReadCallIDMap.del(ID)
	case CallIDErrorMap:
		s.notifies.notifyOnErrorCallIDMap.del(ID)
	case ToPeerIDReadMap:
		s.notifies.notifyOnReadToPeerIDMap.del(ID)
	case ToPeerIDErrorMap:
		s.notifies.notifyOnErrorToPeerIDMap.del(ID)
	}
}

/*
func (c *Client) TimeAfter(dur time.Duration) (timerC <-chan time.Time) {
	if !c.cfg.UseSimNet {
		return time.After(dur)
	}
	// ignore shutdown errors for now (these are the only
	// errors possible at the moment. a nil channel
	// must be handled fine by all client code also
	// selecting on a shutdown signal.
	timerC, _ = c.simnet.createNewTimer(dur, time.Now(), true) // isCli
	return
}

func (s *Server) TimeAfter(dur time.Duration) (timerC <-chan time.Time) {
	if !s.cfg.UseSimNet {
		return time.After(dur)
	}
	// ditto: on error a nil channel will be fine.
	timerC, _ = s.simnet.createNewTimer(dur, time.Now(), false) // isCli
	return
}
*/

type RpcTimer struct {
	gotimer  *time.Timer
	isCli    bool
	simnode  *simnode
	simnet   *simnet
	simtimer *mop
	C        <-chan time.Time
}

func (c *Client) NewTimer(dur time.Duration) (ti *RpcTimer) {
	ti = &RpcTimer{
		isCli: true,
	}
	if !c.cfg.UseSimNet {
		ti.gotimer = time.NewTimer(dur)
		return
	}
	ti.simnet = c.simnet
	ti.simnode = c.simnode
	ti.simtimer = c.simnet.createNewTimer(c.simnode, dur, time.Now(), true) // isCli
	ti.C = ti.simtimer.timerC
	return
}

func (s *Server) NewTimer(dur time.Duration) (ti *RpcTimer) {
	ti = &RpcTimer{
		isCli: false,
	}
	if !s.cfg.UseSimNet {
		ti.gotimer = time.NewTimer(dur)
		return
	}
	ti.simnet = s.simnet
	ti.simnode = s.simnode
	ti.simtimer = s.simnet.createNewTimer(s.simnode, dur, time.Now(), false) // isCli
	ti.C = ti.simtimer.timerC
	return
}

func (ti *RpcTimer) Discard() (wasArmed bool) {
	if ti.simnet == nil {
		ti.gotimer.Stop()
		ti.gotimer = nil // Go will GC.
		return
	}
	wasArmed = ti.simnet.discardTimer(ti.simnode, ti.simtimer, time.Now())
	return
}

/*
func (ti *RpcTimer) Reset(dur time.Duration) (wasArmed bool) {
	if ti.simnet == nil {
		return ti.gotimer.Reset(dur)
	}
	wasArmed = ti.simnet.resetTimer(ti, time.Now(), ti.onCli)
	return
}
func (ti *RpcTimer) Stop(dur time.Duration) (wasArmed bool) {
	if ti.simnet == nil {
		return ti.gotimer.Stop()
	}
	wasArmed = ti.simnet.stopTimer(ti, time.Now(), ti.onCli)
	return
}

// returns wasArmed (not expired or stopped)
func (c *Client) StopTimer(ti *RpcTimer) bool {
	return ti.Stop()
}
func (s *Server) StopTimer(ti *RpcTimer) bool {
	return ti.Stop()
}

// returns wasArmed (not expired or stopped)
func (c *Client) ResetTimer(ti *RpcTimer, dur time.Duration) bool {
	return ti.Reset(dur)
}
func (s *Server) ResetTimer(ti *RpcTimer, dur time.Duration) bool {
	return ti.Reset(dur)
}
*/
