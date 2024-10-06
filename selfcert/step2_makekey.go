package selfcert

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

// name might be "client" or "node"; odirCert default might be "static/certs/client".
func Step2_MakeEd25519PrivateKeys(names []string, odirCert string) {

	os.MkdirAll(odirCert, 0777)

	for _, name := range names {
		keyPath := fmt.Sprintf("%v%v%v.key", odirCert, sep, name)

		// Call the function to generate the ED25519 private key and save it to the desired location
		err := GenerateED25519Key(keyPath)
		if err != nil {
			log.Fatalf("Error generating ED25519 key: %v", err)
		}
	}
}

// GenerateED25519Key generates an ED25519 key pair and saves the private key to a specified file.
func GenerateED25519Key(privateKeyPath string) error {
	// Step 1: Generate the ED25519 key pair
	_, privKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		return err
	}

	// Step 2: Marshal the private key into PKCS8 format
	privKeyBytes, err := x509.MarshalPKCS8PrivateKey(privKey)
	if err != nil {
		return err
	}

	// Step 3: Create the directory if it doesn't exist
	odir := filepath.Dir(privateKeyPath)
	err = os.MkdirAll(odir, os.ModePerm)
	if err != nil {
		return err
	}

	// Step 4: Write the private key to a file in PEM format
	privKeyFile, err := os.Create(privateKeyPath)
	if err != nil {
		return err
	}
	defer privKeyFile.Close()

	// Step 5: Encode the private key as PEM
	if err := pem.Encode(privKeyFile, &pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: privKeyBytes,
	}); err != nil {
		return err
	}

	log.Printf("Private key saved to %s\n", privateKeyPath)
	return nil
}
