package edwardsrpc

// cli.go: simple TCP client, with TLS encryption.

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

// eg. serverAddr = "localhost:8443"
// serverAddr = "192.168.254.151:8443"
func RunClientMain(serverAddr string) {

	log.SetFlags(log.LstdFlags | log.Lshortfile) // Add Lshortfile for short file names

	sslCA := fixSlash("static/certs/client/ca.crt")          // path to CA cert
	sslCert := fixSlash("static/certs/client/client.crt")    // path to client cert
	sslCertKey := fixSlash("static/certs/client/client.key") // path to client key

	//sslCert := fixSlash("static/certs/client/client2.crt")    // path to client cert
	//sslCertKey := fixSlash("static/certs/client/client2.key") // path to client key

	// could we use just one cert for both client and server? Sure, if need be.
	//sslCert := fixSlash("static/certs/node.crt")    // path to server cert
	//sslCertKey := fixSlash("static/certs/node.key") // path to server key

	embedded := true
	config, err2 := LoadClientTLSConfig(embedded, sslCA, sslCert, sslCertKey)
	panicOn(err2)
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

	config.InsecureSkipVerify = false // true would be insecure

	// Parse command-line arguments
	message := flag.String("msg", "Hello, Server!", "Message to send to the server")
	flag.Parse()

	// Dial the server
	conn, err := tls.Dial("tcp", serverAddr, config)
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()
	log.Printf("Connected to server %s", serverAddr)

	// possible to check host keys for TOFU like SSH does,
	// but be aware that if they have the contents of
	// static/certs/node.key that has the server key,
	// they can use that to impersonate the server and MITM the connection.
	// So protect both node.key and client.key from
	// distribution.
	knownHostsPath := "known_server_keys"
	// return error on host-key change.
	connState := conn.ConnectionState()
	raddr := conn.RemoteAddr()
	remoteAddr := strings.TrimSpace(raddr.String())

	const verifyKeys = false
	if verifyKeys {
		good, bad, wasNew, err := HostKeyVerifies(knownHostsPath, &connState, remoteAddr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "key failed list:'%#v': '%v'\n", bad, err)
			os.Exit(1)
		}
		for i := range good {
			vv("accepted identity for server: '%v' (was new: %v)\n", good[i], wasNew)
		}
	}

	timeout := time.Minute

	// Send the message
	if err := sendMessage(1, conn, []byte(*message), &timeout); err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}
	log.Printf("Sent message: %s", *message)

	// Receive the echoed message
	response, err := receiveMessage(conn, &timeout)
	if err != nil {
		log.Fatalf("Failed to receive response: %v", err)
	}
	log.Printf("Received response: %s", string(response))
}
