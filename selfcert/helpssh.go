package selfcert

import (
	"crypto/ed25519"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/crypto/ssh"
)

// PrivateToSSHKeyPair writes an Ed25519 private key and its public key in
// OpenSSH-compatible formats under destinationDir.
func PrivateToSSHKeyPair(privKey ed25519.PrivateKey, keyname, destinationDir string) error {
	if len(privKey) != ed25519.PrivateKeySize {
		return fmt.Errorf("ed25519 private key has length %d; want %d", len(privKey), ed25519.PrivateKeySize)
	}
	if keyname == "" {
		return fmt.Errorf("keyname cannot be empty")
	}
	if strings.ContainsAny(keyname, `/\`) {
		return fmt.Errorf("keyname cannot contain path separators: '%v'", keyname)
	}
	if destinationDir == "" {
		return fmt.Errorf("destinationDir cannot be empty")
	}

	if err := os.MkdirAll(destinationDir, 0700); err != nil {
		return fmt.Errorf("could not MkdirAll for destinationDir=%q: %w", destinationDir, err)
	}
	if err := ownerOnly(destinationDir); err != nil {
		return err
	}

	privatePath := filepath.Join(destinationDir, fmt.Sprintf("id_ed25519_%s", keyname))
	publicPath := privatePath + ".pub"

	if fileExists(privatePath) {
		return fmt.Errorf("privatePath already exists for keyname '%v': pick a different keyname or delete this path: '%v'", keyname, privatePath)
	}
	if fileExists(publicPath) {
		return fmt.Errorf("publicPath already exists for keyname '%v': pick a different keyname or delete this path: '%v'", keyname, publicPath)
	}

	privateBlock, err := ssh.MarshalPrivateKey(privKey, keyname)
	if err != nil {
		return fmt.Errorf("could not marshal private key in OpenSSH format: %w", err)
	}
	privateBytes := pem.EncodeToMemory(privateBlock)
	if privateBytes == nil {
		return fmt.Errorf("could not encode private key PEM")
	}
	if err := os.WriteFile(privatePath, privateBytes, 0600); err != nil {
		return fmt.Errorf("could not write private key to %q: %w", privatePath, err)
	}
	if err := os.Chmod(privatePath, 0600); err != nil {
		return fmt.Errorf("could not chmod private key %q: %w", privatePath, err)
	}

	publicKey, err := ssh.NewPublicKey(privKey.Public())
	if err != nil {
		return fmt.Errorf("could not marshal public key in OpenSSH format: %w", err)
	}
	if err := os.WriteFile(publicPath, ssh.MarshalAuthorizedKey(publicKey), 0644); err != nil {
		return fmt.Errorf("could not write public key to %q: %w", publicPath, err)
	}
	if err := os.Chmod(publicPath, 0644); err != nil {
		return fmt.Errorf("could not chmod public key %q: %w", publicPath, err)
	}

	return nil
}
