package rpc25519

import (
	"context"
	"crypto/tls"
	//"crypto/x509"
	"fmt"
	"log"
	//"net"
	"os"
	"strings"
	//"os"

	"github.com/quic-go/quic-go"
)

func (c *Client) RunQUIC(quicServerAddr string, tlsConfig *tls.Config) {

	ctx := context.Background()
	if c.cfg.ConnectTimeout > 0 {
		// Create a context with a timeout
		ctx2, cancel := context.WithTimeout(ctx, c.cfg.ConnectTimeout)
		defer cancel()
		ctx = ctx2
	}

	// Establish a QUIC connection
	conn, err := quic.DialAddr(ctx, quicServerAddr, tlsConfig, nil)
	if err != nil {
		c.err = err
		c.Connected <- err
		log.Printf("Failed to connect to server: %v", err)
		return
	}
	// do this before signaling on c.Connected, else tests will race and panic
	// not having a connection
	c.QuicConn = conn
	c.isQUIC = true
	c.Connected <- nil

	defer conn.CloseWithError(0, "")

	log.Printf("QUIC connected to server %s", quicServerAddr)

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
	go c.RunReadLoop(wrap)
	c.RunSendLoop(wrap)
	/*
	   timeout := 20 * time.Second

	   // Send the message

	   	if err := sendMessage(1, stream, []byte(*message), &timeout); err != nil {
	   		log.Fatalf("Failed to send message: %v", err)
	   	}

	   log.Printf("Sent message: %s", *message)

	   // Receive the echoed message
	   response, err := receiveMessage(stream, &timeout)

	   	if err != nil {
	   		if strings.Contains(err.Error(), "Application error 0x0 (remote)") {
	   			// normal shutdown.
	   			return
	   		}
	   		log.Fatalf("Failed to receive response: %v", err)
	   	}

	   log.Printf("Received response: %s", string(response))
	*/
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
