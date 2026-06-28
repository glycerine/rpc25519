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
// OpenSSH-compatible formats under destinationDir. privateKeyFileName
// should give just the private keys file name; the public key will
// be this plus a ".pub" suffix. The keys are written into
// the destinationDir directory. Any existing files with the
// same names will be overwritten and lost. keyName gives
// the internet short 'nickname' for the key. It is used
// as the comment when constructing the ssh key for user
// convenience. It can be the empty string.
func PrivateToSSHKeyPair(privKey ed25519.PrivateKey, keyname, privateKeyFileName, destinationDir string) error {
	if len(privKey) != ed25519.PrivateKeySize {
		return fmt.Errorf("ed25519 private key has length %d; want %d", len(privKey), ed25519.PrivateKeySize)
	}
	if privateKeyFileName == "" {
		return fmt.Errorf("privateKeyFileName cannot be empty")
	}
	if strings.ContainsAny(privateKeyFileName, `/\`) {
		return fmt.Errorf("privateKeyFileName cannot contain path separators: '%v'", privateKeyFileName)
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

	privatePath := filepath.Join(destinationDir, privateKeyFileName)
	publicPath := privatePath + ".pub"

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
	if err := ownerOnly(privatePath); err != nil {
		return fmt.Errorf("could not secure private key %q: %w", privatePath, err)
	}

	publicKey, err := ssh.NewPublicKey(privKey.Public())
	if err != nil {
		return fmt.Errorf("could not marshal public key in OpenSSH format: %w", err)
	}
	if err := os.WriteFile(publicPath, ssh.MarshalAuthorizedKey(publicKey), 0644); err != nil {
		return fmt.Errorf("could not write public key to %q: %w", publicPath, err)
	}
	if err := sshPublicKeyPerm(publicPath); err != nil {
		return fmt.Errorf("could not secure public key %q: %w", publicPath, err)
	}

	return nil
}
