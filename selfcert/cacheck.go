package selfcert

// verify against a CA stored in the ssh-agent.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/quic-go/quic-go"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

// RetrieveCAKeysFromAgent retrieves all keys from the ssh-agent that can be used as Certificate Authorities (CAs)
func RetrieveCAKeysFromAgent(a agent.Agent) ([]ssh.PublicKey, error) {
	// List all keys stored in the ssh-agent
	keys, err := a.List()
	if err != nil {
		return nil, fmt.Errorf("failed to list keys: %w", err)
	}

	var caKeys []ssh.PublicKey
	for _, key := range keys {
		// We assume here that the CA keys are stored with a recognizable comment or other heuristic
		// Modify this check according to your environment
		if key.Comment == "CA key" {
			parsedKey, _, _, _, err := ssh.ParseAuthorizedKey(key.Marshal())
			if err != nil {
				return nil, fmt.Errorf("failed to parse CA key: %w", err)
			}
			caKeys = append(caKeys, parsedKey)
		}
	}
	return caKeys, nil
}

// VerifyClientCertificate uses CA keys from the ssh-agent to verify incoming QUIC client certificates
func VerifyClientCertificate(caKeys []ssh.PublicKey) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
		// Parse the incoming certificate
		cert, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return fmt.Errorf("failed to parse certificate: %w", err)
		}

		// Check if the certificate is signed by one of the trusted CA keys
		for _, caKey := range caKeys {
			// Convert the ssh.PublicKey to a usable format (DER-encoded ASN.1 public key)
			derCAKey := caKey.Marshal()
			caCert, err := x509.ParseCertificate(derCAKey)
			if err != nil {
				return fmt.Errorf("failed to parse CA certificate: %w", err)
			}

			// Verify the client certificate against the CA key
			if err := cert.CheckSignatureFrom(caCert); err == nil {
				log.Printf("Client certificate is valid and signed by trusted CA: %s\n", caCert.Subject)
				return nil // Certificate is valid
			}
		}

		return fmt.Errorf("client certificate verification failed")
	}
}

// startQUICServer starts a QUIC server with TLS configuration to check client certificates against the CA keys from ssh-agent
func startQUICServer(sshAgent agent.Agent) error {
	// Retrieve the CA keys from ssh-agent
	caKeys, err := RetrieveCAKeysFromAgent(sshAgent)
	if err != nil {
		return fmt.Errorf("failed to retrieve CA keys from agent: %w", err)
	}

	// QUIC server TLS configuration
	tlsConfig := &tls.Config{
		ClientAuth: tls.RequireAnyClientCert, // Require a client certificate
		// Use the custom client certificate verification callback
		VerifyPeerCertificate: VerifyClientCertificate(caKeys),
	}

	// QUIC server configuration
	serverConfig := &quic.Config{}

	// Start the QUIC server
	listener, err := quic.ListenAddr("localhost:4242", tlsConfig, serverConfig)
	if err != nil {
		return fmt.Errorf("failed to start QUIC server: %w", err)
	}
	defer listener.Close()

	log.Println("QUIC server listening on localhost:4242")

	// Accept incoming connections
	for {
		session, err := listener.Accept(context.Background())
		if err != nil {
			return fmt.Errorf("failed to accept session: %w", err)
		}
		log.Printf("New session accepted: %v\n", session.RemoteAddr())
	}
}

func example() {
	// Connect to the ssh-agent using SSH_AUTH_SOCK
	sshAuthSock := os.Getenv("SSH_AUTH_SOCK")
	if sshAuthSock == "" {
		log.Fatal("SSH_AUTH_SOCK is not set")
	}

	conn, err := net.Dial("unix", sshAuthSock)
	if err != nil {
		log.Fatalf("Failed to connect to SSH agent: %v", err)
	}
	defer conn.Close()

	// Create an agent client
	sshAgent := agent.NewClient(conn)

	// Start the QUIC server
	if err := startQUICServer(sshAgent); err != nil {
		log.Fatalf("Failed to start QUIC server: %v", err)
	}
}
