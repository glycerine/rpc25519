package rpc25519

import (
	"context"
	"crypto/tls"
	//"crypto/x509"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"
	//"os"

	"github.com/quic-go/quic-go"
	//"github.com/quic-go/quic-go/qlog"
)

var _ = time.Time{}

var ErrHandshakeQUIC = fmt.Errorf("quic handshake failure")

func (c *Client) runQUIC(localHostPort, quicServerAddr string, tlsConfig *tls.Config) {

	//defer func() {
	//	vv("finishing '%v' Client.runQUIC(quicServerAddr='%v')", c.name, quicServerAddr)
	//}()

	ctx := context.Background()
	// Warning: do not WithTimeout this ctx, because that
	// will kill it while it is working... the ctx timeout
	// applies to the full lifecycle not just the dial/handshake.

	// this way we for sure get notified of when the connection has closed,
	// and our little goro <-ctxWithCancel will see the
	// Done() channel closed.
	ctxWithCancel, outerCancel := context.WithCancel(ctx)
	defer outerCancel()

	// could also try context.AfterFunc(ctxWithCancel, func() {
	// 		select {
	//      case <-ctxWithCancel.Done():
	//			//vv("quic_client '%v' sees ctxWithCancel is Done.", c.name) // yes! seen on shutdown.
	//			c.halt.ReqStop.Close()
	//		case <-c.halt.ReqStop.Chan:
	//		}
	// })  // to avoid a goroutine.
	go func() {
		select {
		case <-ctxWithCancel.Done():
			//vv("quic_client '%v' sees ctxWithCancel is Done.", c.name) // yes! seen on shutdown.
			c.halt.ReqStop.Close()
		case <-c.halt.ReqStop.Chan:
		}
	}()

	// Server address to connect to
	serverAddr, err := net.ResolveUDPAddr("udp", quicServerAddr)
	if err != nil {
		alwaysPrintf("Failed to resolve server address: %v\n", err)
		return
	}
	//vv("quicServerAddr '%v' -> '%v'", quicServerAddr, serverAddr)

	localAddr, err := net.ResolveUDPAddr("udp", localHostPort) // get net.UDPAddr
	panicOn(err)
	//vv("quic client using localAddr '%v' -> serverAddr '%v'", localHostPort, serverAddr)

	// We'll share the port with the same process server (if he's around).
	var transport *quic.Transport
	c.cfg.shared.mut.Lock()
	if !c.cfg.NoSharePortQUIC && c.cfg.shared.quicTransport != nil {
		transport = c.cfg.shared.quicTransport
		c.cfg.shared.shareCount++
		//vv("c.cfg.shared.shareCount is now %v on client '%v'", c.cfg.shared.shareCount, c.name)
		c.cfg.shared.mut.Unlock()
	} else {
		udpConn, err := net.ListenUDP("udp", localAddr)
		if err != nil {
			alwaysPrintf("Failed to bind UPD client to '%v'/'%v': '%v'\n", localAddr, localHostPort, err)
			c.cfg.shared.mut.Unlock()
			return
		}
		transport = &quic.Transport{
			Conn:               udpConn,
			ConnectionIDLength: 20,
		}
		c.cfg.shared.quicTransport = transport
		c.cfg.shared.shareCount++
		//vv("c.cfg.shared.shareCount is now %v on client '%v'", c.cfg.shared.shareCount, c.name)
		c.cfg.shared.mut.Unlock()
	}
	// note: we do not defer updConn.Close() because it may be shared with other clients/servers.
	// Instead: reference count in cfg.shareCount and call in Close()

	quicConfig := &quic.Config{
		Allow0RTT:            true,
		HandshakeIdleTimeout: c.cfg.ConnectTimeout,

		// 1200 is important, else we will have trouble with MTU 1280 networks like Tailscale.
		InitialPacketSize: 1200,

		// export QLOGDIR and set this get get qlog tracing.
		//Tracer: qlog.DefaultConnectionTracer,

		// quic-go keep alives are unreliable, so do them ourselves
		// in the quic server send loop. See
		// https://github.com/quic-go/quic-go/issues/4710
		//KeepAlivePeriod: 5 * time.Second,
	}

	// this conn is a quic.EarlyConnection
	conn, err := transport.DialEarly(ctx, serverAddr, tlsConfig, quicConfig)
	if err != nil {
		c.err = err
		c.connected <- fmt.Errorf("error: client local: '%v' failed to "+
			"connect to server: '%v'", localHostPort, err)
		alwaysPrintf("error: client from '%v' failed to connect to server: %v", localHostPort, err)
		return
	}
	// assigning QuicConn before signaling on c.connected, else tests will race and panic
	// not having a connection
	c.quicConn = conn
	c.isQUIC = true
	c.quicConfig = quicConfig

	defer func() {
		conn.CloseWithError(0, "") // in runQUIC() here. fired when read loop returns.
		//vv("quic_client has called quicConn.CloseWithError(0)")
	}()

	// wait for the handshake to complete so we are encrypted/can verify host keys.
	// see https://quic-go.net/docs/quic/server/
	select {
	case <-conn.HandshakeComplete():
		//vv("quic_client handshake completed")
	case <-conn.Context().Done():
		// connection closed before handshake completion, e.g. due to handshake failure
		c.connected <- ErrHandshakeQUIC
		alwaysPrintf("quic_client handshake failure on DialEarly")
		return
	}

	c.setLocalAddr(conn)

	c.connected <- nil

	//vv("QUIC client connected to server %v, with local addr='%v'", remote(conn), c.cfg.LocalAddress)

	// We check host keys like SSH does,
	// but be aware that if attackers have the contents of
	// an un-encrypted certs/node.key that has the server key,
	// they can use that to impersonate the server and MITM the connection.
	//
	// So: protect both node.key and client.key (and ca.key) from
	// distribution. selfy will password protect them
	// by default, but that makes it difficult to run
	// and restart un-attended.
	knownHostsPath := "known_server_keys"
	// return error on host-key change.
	connState := conn.ConnectionState()
	raddr := conn.RemoteAddr()
	remoteAddr := strings.TrimSpace(raddr.String())

	if !c.cfg.SkipVerifyKeys {
		good, bad, wasNew, err := hostKeyVerifies(knownHostsPath, &connState.TLS, remoteAddr)
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

	// Create a stream to send and receive data
	// There is no signaling to the peer about new streams:
	// The peer can only accept the stream after data has been sent on the stream.
	// jea: So this should avoid a round trip?
	stream, err := conn.OpenStream()

	// jea: OpenStreamSync blocks until a new stream can be opened. So might be slower?
	//stream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		log.Fatalf("Error opening stream: %v", err)
	}
	defer stream.Close()

	wrap := &NetConnWrapper{Stream: stream, Connection: conn}

	//vv("quic client stream: local = '%v'", local(wrap))
	//vv("quic client stream: remote = '%v'", remote(wrap))

	//vv("quic client: s.cfg.encryptPSK = %v", c.cfg.encryptPSK)
	err = c.setupPSK(wrap)
	if err != nil {
		alwaysPrintf("failed setupPSK/handshake with server: '%v'", err)
		return
	}

	cpair := &cliPairState{}
	go c.runSendLoop(wrap, cpair)
	c.runReadLoop(wrap, cpair)
}

// NetConnWrapper is exported so that clients
// like `goq` and others that want to inspect
// that context of their calls can do so.
type NetConnWrapper struct {
	quic.Stream
	quic.Connection
}

func (w *NetConnWrapper) Close() error {
	//vv("NetConnWrapper Close() called")
	w.Stream.Close()
	return w.Connection.CloseWithError(0, "server shutdown")
}
