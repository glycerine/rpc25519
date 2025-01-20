package selfcert

// plan? test and verify something like this might work. EXPERIMENTAL PROTOTYPE at the moment.
// prototype attempts (one in comments) to use ssh-agent
// during TLS handshake.
/*
import (
	"context"
	"crypto"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"github.com/quic-go/quic-go"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

// SSHAgentSigner represents a signer that uses the ssh-agent for signing operations
type SSHAgentSigner struct {
	agent agent.Agent
	key   ssh.PublicKey
}

// NewSSHAgentSigner initializes a new SSHAgentSigner
func NewSSHAgentSigner(a agent.Agent, key ssh.PublicKey) *SSHAgentSigner {
	return &SSHAgentSigner{
		agent: a,
		key:   key,
	}
}

// Sign signs the given data using the private key stored in the ssh-agent
func (s *SSHAgentSigner) Sign(rand io.Reader, data []byte, opts crypto.SignerOpts) (signature []byte, err error) {
	// Use the ssh-agent to sign the data
	sig, err := s.agent.Sign(s.key, data)
	if err != nil {
		return nil, fmt.Errorf("failed to sign data with ssh-agent: %w", err)
	}

	// Convert the signature to a byte slice
	return sig.Blob, nil
}

// Public returns the public key of the signer
func (s *SSHAgentSigner) Public() crypto.PublicKey {
	return s.key
}

//

// startQUICServer starts a QUIC server that uses ssh-agent for signing
func startQUICServer3(sshAgent agent.Agent) error {
	// Retrieve the server's public key from ssh-agent (replace with your logic)
	pubKey, err := ssh.ParsePublicKey([]byte("ssh-rsa AAAAB...")) // replace with your public key
	if err != nil {
		return err
	}

	// Create the custom SSHAgentSigner
	signer := NewSSHAgentSigner(sshAgent, pubKey)

	// Prepare TLS configuration using the signer
	tlsConfig := &tls.Config{
		GetCertificate: func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
			// Return a tls.Certificate that uses the custom signer for the handshake
			return &tls.Certificate{
				PrivateKey: signer, // Use the custom signer
			}, nil
		},
	}

	// Start the QUIC server
	listener, err := quic.ListenAddr("localhost:4242", tlsConfig, nil)
	if err != nil {
		return err
	}
	defer listener.Close()

	log.Println("QUIC server listening on localhost:4242")

	for {
		session, err := listener.Accept(context.Background())
		if err != nil {
			return err
		}
		log.Printf("New session accepted: %v\n", session.RemoteAddr())
	}
}

func example3() {
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
	if err := startQUICServer3(sshAgent); err != nil {
		log.Fatalf("Failed to start QUIC server: %v", err)
	}
}
*/
/*
package selfcert

// figuring out how to use ssh-agent in the middle of a TLS handshake.

import (
	"context"
	"crypto"
	"crypto/tls"
	//"crypto/x509"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/quic-go/quic-go"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

// Custom Signer Implementation

// SSHAgentSigner represents a signer that uses the ssh-agent for signing operations
type SSHAgentSigner struct {
	agent agent.Agent
	key   ssh.PublicKey
}

// NewSSHAgentSigner initializes a new SSHAgentSigner
func NewSSHAgentSigner(a agent.Agent, key ssh.PublicKey) *SSHAgentSigner {
	return &SSHAgentSigner{
		agent: a,
		key:   key,
	}
}

// Sign signs the given data using the private key stored in the ssh-agent
func (s *SSHAgentSigner) Sign(rand crypto.Reader, data []byte, opts crypto.SignerOpts) (signature []byte, err error) {
	// Use the ssh-agent to sign the data
	signature, err = s.agent.Sign(s.key, data)
	if err != nil {
		return nil, fmt.Errorf("failed to sign data with ssh-agent: %w", err)
	}
	return signature, nil
}

// Public returns the public key of the signer
func (s *SSHAgentSigner) Public() crypto.PublicKey {
	return s.key
}

// QUIC Server Using the Custom Signer

// startQUICServer starts a QUIC server that uses ssh-agent for signing
func startQUICServer2(sshAgent agent.Agent) error {
	// Retrieve the server's public key from ssh-agent (add your own logic to identify it)
	pubKey, err := ssh.ParsePublicKey([]byte("ssh-rsa AAAAB...")) // replace with your public key
	if err != nil {
		return err
	}

	// Create the custom SSHAgentSigner
	signer := NewSSHAgentSigner(sshAgent, pubKey)

	// Prepare TLS configuration using the signer
	tlsConfig := &tls.Config{
		GetCertificate: func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
			// Return a tls.Certificate that uses the custom signer for the handshake
			return &tls.Certificate{
				PrivateKey: signer, // Use the custom signer
			}, nil
		},
	}

	// Start the QUIC server
	listener, err := quic.ListenAddr("localhost:4242", tlsConfig, nil)
	if err != nil {
		return err
	}
	defer listener.Close()

	log.Println("QUIC server listening on localhost:4242")

	for {
		session, err := listener.Accept(context.Background())
		if err != nil {
			return err
		}
		log.Printf("New session accepted: %v\n", session.RemoteAddr())
	}
}

func example2() {
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
	if err := startQUICServer2(sshAgent); err != nil {
		log.Fatalf("Failed to start QUIC server: %v", err)
	}
}

*/
