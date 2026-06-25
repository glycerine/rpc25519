package selfcert

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/crypto/ssh"
)

func TestPrivateToSSHKeyPair(t *testing.T) {
	_, privKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey failed: %v", err)
	}

	destinationDir := t.TempDir()
	if err := PrivateToSSHKeyPair(privKey, "test", destinationDir); err != nil {
		t.Fatalf("PrivateToSSHKeyPair failed: %v", err)
	}

	privatePath := filepath.Join(destinationDir, "id_ed25519_test")
	publicPath := privatePath + ".pub"

	privateBytes, err := os.ReadFile(privatePath)
	if err != nil {
		t.Fatalf("ReadFile(%q) failed: %v", privatePath, err)
	}
	signer, err := ssh.ParsePrivateKey(privateBytes)
	if err != nil {
		t.Fatalf("ParsePrivateKey failed: %v", err)
	}

	wantPublicKey, err := ssh.NewPublicKey(privKey.Public())
	if err != nil {
		t.Fatalf("NewPublicKey failed: %v", err)
	}
	if !bytes.Equal(signer.PublicKey().Marshal(), wantPublicKey.Marshal()) {
		t.Fatalf("private key public half did not match generated key")
	}

	publicBytes, err := os.ReadFile(publicPath)
	if err != nil {
		t.Fatalf("ReadFile(%q) failed: %v", publicPath, err)
	}
	gotPublicKey, _, _, rest, err := ssh.ParseAuthorizedKey(publicBytes)
	if err != nil {
		t.Fatalf("ParseAuthorizedKey failed: %v", err)
	}
	if len(rest) != 0 {
		t.Fatalf("unexpected trailing public key data: %q", rest)
	}
	if !bytes.Equal(gotPublicKey.Marshal(), wantPublicKey.Marshal()) {
		t.Fatalf("public key file did not match generated key")
	}

	privateInfo, err := os.Stat(privatePath)
	if err != nil {
		t.Fatalf("Stat(%q) failed: %v", privatePath, err)
	}
	if got := privateInfo.Mode().Perm(); got != 0600 {
		t.Fatalf("private key permissions = %o; want 0600", got)
	}

	publicInfo, err := os.Stat(publicPath)
	if err != nil {
		t.Fatalf("Stat(%q) failed: %v", publicPath, err)
	}
	if got := publicInfo.Mode().Perm(); got != 0644 {
		t.Fatalf("public key permissions = %o; want 0644", got)
	}
}

func TestPrivateToSSHKeyPairRejectsInvalidPrivateKey(t *testing.T) {
	if err := PrivateToSSHKeyPair(ed25519.PrivateKey("short"), "test", t.TempDir()); err == nil {
		t.Fatalf("expected invalid private key length error")
	}
}
