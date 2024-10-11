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
)

var _ = time.Time{}

func (c *Client) RunQUIC(quicServerAddr string, tlsConfig *tls.Config) {

	//defer func() {
	//	vv("finishing '%v' Client.RunQUIC(quicServerAddr='%v')", c.name, quicServerAddr)
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

	// could also try context.AfterFunc(ctxWithCancelt, func() {
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

	localHost, err := LocalAddrMatching(quicServerAddr)
	panicOn(err)
	//vv("localHost = '%v', matched to quicServerAddr = '%v'", localHost, quicServerAddr)
	localHostPort := localHost + ":0" // client can pick any port

	// Server address to connect to
	serverAddr, err := net.ResolveUDPAddr("udp", quicServerAddr)
	if err != nil {
		AlwaysPrintf("Failed to resolve server address: %v\n", err)
		return
	}
	//vv("quicServerAddr '%v' -> '%v'", quicServerAddr, serverAddr)

	localAddr, err := net.ResolveUDPAddr("udp", localHostPort) // get net.UDPAddr
	panicOn(err)
	//vv("quic client using localAddr '%v' -> serverAddr '%v'", localHostPort, serverAddr)

	// prep for same-same port sharing, change Dial to DialEarly on a quic.Transport.
	var transport *quic.Transport
	c.cfg.shared.mut.Lock()
	if c.cfg.shared.quicTransport != nil {
		transport = c.cfg.shared.quicTransport
		c.cfg.shared.mut.Unlock()
	} else {
		udpConn, err := net.ListenUDP("udp", localAddr)
		if err != nil {
			AlwaysPrintf("Failed to bind UPD client to '%v'/'%v': '%v'\n", localAddr, localHostPort, err)
			c.cfg.shared.mut.Unlock()
			return
		}
		transport = &quic.Transport{
			Conn: udpConn,
		}
		c.cfg.shared.quicTransport = transport
		c.cfg.shared.mut.Unlock()
	}
	// note: we do not defer updConn.Close() because it may be shared with other clients/servers.
	/*
		// Create the UDP connection bound to the specified local address
		udpConn, err := net.ListenUDP("udp", localAddr)
		if err != nil {
			AlwaysPrintf("Failed to bind UPD client to '%v'/'%v': '%v'\n", localAddr, localHostPort, err)
			return
		}
		defer udpConn.Close()
	*/

	quicConfig := &quic.Config{
		//Allow0RTT:            true,
		KeepAlivePeriod:      5 * time.Second,
		HandshakeIdleTimeout: c.cfg.ConnectTimeout,
		InitialPacketSize:    1200,
	}

	// didn't allow us to bind a specific network interface,
	// and thus get an actual IP address for local/remote;
	// so we do the 3 steps above first; instead of:
	//conn, err := quic.DialAddr(ctx, quicServerAddr, tlsConfig, nil)

	// this conn is a quic.Connection
	//conn, err := quic.Dial(ctx, udpConn, serverAddr, tlsConfig, quicConfig)

	// this conn is a quic.EarlyConnection
	conn, err := transport.DialEarly(ctx, serverAddr, tlsConfig, quicConfig)
	if err != nil {
		c.err = err
		c.Connected <- err
		log.Printf("Failed to connect to server: %v", err)
		return
	}
	// do this before signaling on c.Connected, else tests will race and panic
	// not having a connection
	//c.Conn = conn
	c.QuicConn = conn
	c.isQUIC = true
	c.Connected <- nil

	defer conn.CloseWithError(0, "")

	// wait for the handshake to complete so we are encrypted/can verify host keys.
	select {
	case <-conn.HandshakeComplete():
		//vv("quic_client handshake completed")
	case <-conn.Context().Done():
		// connection closed before handshake completion, e.g. due to handshake failure
		vv("quic_client handshake failure on DialEarly")
		return
	}

	la := conn.LocalAddr()
	c.cfg.LocalAddress = la.Network() + "://" + la.String()

	//vv("QUIC connected to server %v, with local addr='%v'", remote(conn), c.cfg.LocalAddress)

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
		good, bad, wasNew, err := HostKeyVerifies(knownHostsPath, &connState.TLS, remoteAddr)
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

	//vv("client: local = '%v'", local(wrap))
	//vv("client: remote = '%v'", remote(wrap))

	go c.RunSendLoop(wrap)
	c.RunReadLoop(wrap)
}

type NetConnWrapper struct {
	quic.Stream
	quic.Connection
}

/*
func remoteQ(nc quic.Connection) string {
	ra := nc.RemoteAddr()
	return ra.Network() + "://" + ra.String()
}

func localQ(nc quic.Connection) string {
	la := nc.LocalAddr()
	return la.Network() + "://" + la.String()
}
*/
