package selfcert

// find the ssh-agent from the socket at env SSH_AUTH_SOCK
// then read and or store keys in the agent.

import (
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

// StoreEd25519PrivateKey stores the private key in the SSH agent
func StoreEd25519PrivateKey(a agent.Agent, privateKey ed25519.PrivateKey, comment string) error {
	// Convert the Ed25519 private key to an ssh.Signer type
	signer, err := ssh.NewSignerFromSigner(privateKey)
	_ = signer
	if err != nil {
		return fmt.Errorf("failed to create signer: %w", err)
	}

	// Create a new agent.AddedKey with some expiration (e.g., 1 hour)
	addedKey := agent.AddedKey{
		PrivateKey:   privateKey,
		Comment:      comment,
		LifetimeSecs: uint32(time.Hour.Seconds()), // 1 hour lifetime
	}

	// Add the key to the SSH agent
	if err := a.Add(addedKey); err != nil {
		return fmt.Errorf("failed to add key to agent: %w", err)
	}

	log.Printf("Ed25519 private key added to SSH agent with comment: %s\n", comment)
	return nil
}

// ListAgentKeys lists all the keys stored in the SSH agent
func ListAgentKeys(a agent.Agent) error {
	// List all keys in the agent
	keys, err := a.List()
	if err != nil {
		return fmt.Errorf("failed to list keys: %w", err)
	}

	// Display all stored keys
	log.Println("Keys in SSH agent:")
	for _, key := range keys {
		log.Printf("  Key: %s (Comment: %s)\n", key.Format, key.Comment)
	}

	return nil
}

// RetrieveKeyByComment retrieves a key based on its comment from the SSH agent
func RetrieveKeyByComment(a agent.Agent, comment string) (*agent.Key, error) {
	// List all keys in the agent
	keys, err := a.List()
	if err != nil {
		return nil, fmt.Errorf("failed to list keys: %w", err)
	}

	// Search for a key with the specified comment
	for _, key := range keys {
		if key.Comment == comment {
			log.Printf("Found key with comment: %s\n", comment)
			return key, nil
		}
	}

	return nil, fmt.Errorf("no key found with comment: %s", comment)
}

func exampleMain() {
	// Connect to the SSH agent using the SSH_AUTH_SOCK environment variable
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

	// Generate an Ed25519 private key
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		log.Fatalf("Failed to generate Ed25519 key: %v", err)
	}

	// Define a comment for the key
	keyComment := "ed25519 key added from Go"

	// Store the Ed25519 private key in the agent
	if err := StoreEd25519PrivateKey(sshAgent, privateKey, keyComment); err != nil {
		log.Fatalf("Failed to store Ed25519 key: %v", err)
	}

	// List all the keys in the agent to verify the key was added
	if err := ListAgentKeys(sshAgent); err != nil {
		log.Fatalf("Failed to list keys: %v", err)
	}

	// Retrieve the key based on its comment
	retrievedKey, err := RetrieveKeyByComment(sshAgent, keyComment)
	if err != nil {
		log.Fatalf("Failed to retrieve key: %v", err)
	}

	// Display information about the retrieved key
	log.Printf("Retrieved Key: %s (Comment: %s)\n", retrievedKey.Format, retrievedKey.Comment)
}
