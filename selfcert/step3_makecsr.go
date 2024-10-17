package selfcert

import (
	"crypto/ed25519"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
)

func Step3_MakeCertSigningRequest(privKey ed25519.PrivateKey, name string, email string, odirCert string) {

	os.MkdirAll(odirCert, 0700)
	ownerOnly(odirCert)

	keyPath := fmt.Sprintf("%v%v%v.key", odirCert, sep, name)
	csrPath := fmt.Sprintf("%v%v%v.csr", odirCert, sep, name)

	err := createCertSigningRequest(privKey, email, keyPath, csrPath)
	if err != nil {
		log.Fatalf("could not createCertSigningRequest: '%v'", err)
	}

}

func createCertSigningRequest(privateKey ed25519.PrivateKey, emailAddress, inputPrivateKeyPath, outputCsrPath string) error {

	if privateKey == nil {
		// Step 1: Load the private key from static/certs/client.key
		var err error
		privateKey, err = loadPrivateKey(inputPrivateKeyPath)
		if err != nil {
			return fmt.Errorf("Failed to load private key: %v", err)
		}
	}

	// Step 2: Create the CSR template (with subject and SAN extensions)
	csrTemplate := x509.CertificateRequest{
		Subject: pkix.Name{
			Country:            []string{"US"},
			Province:           []string{"New York"},
			Locality:           []string{"New York"},
			Organization:       []string{"MyOrg"},
			OrganizationalUnit: []string{"MyOrgUnit"},
			CommonName:         "localhost",
		},
		DNSNames:       []string{"localhost"},
		IPAddresses:    []net.IP{net.ParseIP("127.0.0.1")},
		EmailAddresses: []string{emailAddress},
	}

	// Step 3: Create the CSR
	csrBytes, err := x509.CreateCertificateRequest(nil, &csrTemplate, privateKey)
	if err != nil {
		return fmt.Errorf("Failed to create CSR: %v", err)
	}

	// Step 4: Write the CSR to file
	err = saveCSR(outputCsrPath, csrBytes)
	if err != nil {
		return fmt.Errorf("Failed to save CSR: %v", err)
	}

	//log.Printf("CSR generated successfully at %v", outputCsrPath)
	return nil
}

// also exported
func LoadPrivateKey(path string) (ed25519.PrivateKey, error) {
	return loadPrivateKey(path)
}

// loadPrivateKey loads an ED25519 private key from the given file
func loadPrivateKey(path string) (ed25519.PrivateKey, error) {
	keyBytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("unable to read private key file: %w", err)
	}

	block, _ := pem.Decode(keyBytes)
	if block == nil || block.Type != "PRIVATE KEY" {
		return nil, fmt.Errorf("failed to decode PEM block containing private key")
	}

	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %w", err)
	}

	// Ensure the key is an ED25519 private key
	privKey, ok := key.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("not an ED25519 private key")
	}

	return privKey, nil
}

// saveCSR saves the CSR bytes to a file in PEM format
func saveCSR(path string, csrBytes []byte) error {
	csrFile, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("unable to create CSR file: %w", err)
	}
	defer ownerOnly(path)
	defer csrFile.Close()

	err = pem.Encode(csrFile, &pem.Block{
		Type:  "CERTIFICATE REQUEST",
		Bytes: csrBytes,
	})
	if err != nil {
		return fmt.Errorf("failed to write CSR to file: %w", err)
	}

	return nil
}
