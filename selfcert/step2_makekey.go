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
func Step2_MakeEd25519PrivateKeys(names []string, odirCert string, verbose, encrypt bool) {

	os.MkdirAll(odirCert, 0700)
	ownerOnly(odirCert)

	for _, name := range names {
		keyPath := fmt.Sprintf("%v%v%v.key", odirCert, sep, name)

		// Call the function to generate the ED25519 private key and save it to the desired location
		err := GenerateED25519Key(keyPath, verbose, encrypt, name)
		if err != nil {
			log.Fatalf("Error generating ED25519 key: %v", err)
		}
	}
}

// GenerateED25519Key generates an ED25519 key pair and saves the private key to a specified file.
func GenerateED25519Key(privateKeyPath string, verbose, encrypt bool, name string) error {
	// Step 1: Generate the ED25519 key pair
	_, privKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		return err
	}

	// Step 2: Create the directory if it doesn't exist
	odir := filepath.Dir(privateKeyPath)
	err = os.MkdirAll(odir, 0700)
	if err != nil {
		return fmt.Errorf("could not MkdirAll for odir='%v': '%v'", odir, err)
	}
	ownerOnly(odir)

	if encrypt {
		fmt.Printf("Setting pass phrase for the '%v' private key in '%v'.\n", name, privateKeyPath)
		err = SavePrivateKeyToPathUnderPassword(privKey, privateKeyPath)
		if err != nil {
			return fmt.Errorf("Failed to create encrypted key for '%v' at path '%v': %v", name, privateKeyPath, err)
		}
		ownerOnly(privateKeyPath)
	} else {

		// Step 3: Marshal the private key into PKCS8 format
		privKeyBytes, err := x509.MarshalPKCS8PrivateKey(privKey)
		if err != nil {
			return err
		}

		// Step 4: Write the private key to a file in PEM format
		privKeyFile, err := os.Create(privateKeyPath)
		if err != nil {
			return err
		}
		defer ownerOnly(privateKeyPath)
		defer privKeyFile.Close()

		// Step 5: Encode the private key as PEM
		if err := pem.Encode(privKeyFile, &pem.Block{
			Type:  "PRIVATE KEY",
			Bytes: privKeyBytes,
		}); err != nil {
			return err
		}
	}

	if verbose {
		log.Printf("Private key saved to %s\n", privateKeyPath)
	}
	return nil
}
