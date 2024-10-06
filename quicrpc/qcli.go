package quicrpc

import (
	"context"
	//"crypto/tls"
	//"crypto/x509"
	"fmt"
	"log"
	"os"
	"strings"
	//"os"
	"flag"
	"time"

	"github.com/quic-go/quic-go"
)

func RunQuicClientMain(quicServerAddr string) {

	log.SetFlags(log.LstdFlags | log.Lshortfile) // Add Lshortfile for short file names

	sslCA := fixSlash("static/certs/client/ca.crt")          // path to CA cert
	sslCert := fixSlash("static/certs/client/client.crt")    // path to client cert
	sslCertKey := fixSlash("static/certs/client/client.key") // path to client key

	//sslCert := fixSlash("static/certs/client/client2.crt")    // path to client cert
	//sslCertKey := fixSlash("static/certs/client/client2.key") // path to client key

	embedded := true
	tlsConfig, err2 := LoadClientTLSConfig(embedded, sslCA, sslCert, sslCertKey)
	panicOn(err2)

	// Parse command-line arguments
	message := flag.String("msg", "Hello, QUIC Server!", "Message to send to the server")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	_ = ctx
	defer cancel()

	// Establish a QUIC connection
	conn, err := quic.DialAddr(ctx, quicServerAddr, tlsConfig, nil)
	if err != nil {
		log.Fatalf("Error dialing QUIC server: %v", err)
	}
	defer conn.CloseWithError(0, "")

	// check for known and certified public key from server.
	connState := conn.ConnectionState().TLS
	raddr := conn.RemoteAddr()
	addr := strings.TrimSpace(raddr.String())
	knownHostsPath := "known_server_keys"

	good, bad, wasNew, err := HostKeyVerifies(knownHostsPath, &connState, addr)
	if err != nil && len(good) == 0 {
		fmt.Fprintf(os.Stderr, "key failed list:'%#v': '%v'\n", bad, err)
		os.Exit(1)
	}
	for i := range good {
		vv("accepted identity for server: '%v' (was new: %v)\n", good[i], wasNew)
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

}
