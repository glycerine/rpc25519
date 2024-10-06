package edwardsrpc

// srv.go: simple TCP server, with TLS encryption.

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"time"
	//"net"
	"os"
	"strings"
)

const (
	maxMessage = 1024 * 1024 // 1MB max message size
)

//var serverAddress = "0.0.0.0:8443"

//var serverAddress = "192.168.254.151:8443"

func RunServerMain(serverAddress string) {
	log.SetFlags(log.LstdFlags | log.Lshortfile) // Add Lshortfile for short file names

	sslCA := fixSlash("static/certs/server/ca.crt") // path to CA cert

	// path to CA cert to verify client certs, can be same as sslCA
	sslClientCA := fixSlash("static/certs/server/ca.crt")
	sslCert := fixSlash("static/certs/server/node.crt")    // path to server cert
	sslCertKey := fixSlash("static/certs/server/node.key") // path to server key

	embedded := true
	config, err := LoadServerTLSConfig(embedded, sslCA, sslClientCA, sslCert, sslCertKey)
	panicOn(err)
	// Not needed now that we have proper CA cert from gen.sh; or
	// perhaps this is the default anyway(?)
	// In any event, "localhost" is what we see during handshake; but
	// maybe that is because localhost is what we put in the ca.cnf and openssl-san.cnf
	// as the CN and DNS.1 names too(!)
	//config.ServerName = "localhost"

	//	const insecure = false
	//	if insecure {
	//		//insecure to turn off client cert checking with:
	//		config.ClientAuth = tls.NoClientCert
	//	} else {
	config.ClientAuth = tls.RequireAndVerifyClientCert
	//	}

	// Listen on the specified serverAddress
	listener, err := tls.Listen("tcp", serverAddress, config)
	if err != nil {
		log.Fatalf("Failed to listen on %v: %v", serverAddress, err)
	}
	defer listener.Close()
	log.Printf("Server listening on %v", serverAddress)

	for {
		// Accept a new connection
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}
		log.Printf("Accepted connection from %v", conn.RemoteAddr())

		// Handle the connection in a new goroutine
		tlsConn := conn.(*tls.Conn)

		go handleConnection(tlsConn)
	}
}

func handleConnection(conn *tls.Conn) {
	//vv("top of handleConnection()")

	defer func() {
		//log.Printf("Closing connection from %v", conn.RemoteAddr())
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
		log.Printf("tlsConn.Handshake() failed: '%v'", err)
		return
	}

	knownHostsPath := "known_client_keys"
	connState := conn.ConnectionState()
	raddr := conn.RemoteAddr()
	remoteAddr := strings.TrimSpace(raddr.String())

	const verifyKeys = false
	if verifyKeys {
		good, bad, wasNew, err := HostKeyVerifies(knownHostsPath, &connState, remoteAddr)
		if err != nil && len(good) == 0 {
			fmt.Fprintf(os.Stderr, "key failed list:'%#v': '%v'\n", bad, err)
			return
		}
		if err != nil {
			vv("err = '%v' but we had some good pubkeys?", err)
			return
		}
		for i := range good {
			vv("accepted identity for client: '%v' (was new: %v)\n", good[i], wasNew)
		}
	}

	timeout := time.Minute
	for {

		msg, err := receiveMessage(conn, &timeout)
		if err == io.EOF {
			//log.Printf("server sees io.EOF from receiveMessage")
			return // close of socket before read of full message.
		}
		if err != nil && strings.Contains(err.Error(), "remote error: tls: bad certificate") {
			vv("ignoring client connection with bad TLS cert.")
			return
		}
		panicOn(err)

		log.Printf("Received message from %v: '%v'", conn.RemoteAddr(), string(msg))

		// Echo the message back to the client
		if err := sendMessage(1, conn, msg, &timeout); err != nil {
			log.Printf("Error sending message to %v: %v", conn.RemoteAddr(), err)
			return
		}
	}
}
