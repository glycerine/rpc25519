package selfcert

import (
	"crypto/ed25519"
	"crypto/x509"
	//"crypto/x509/pkix"
	"bytes"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"log"
	"math/big"
	//"net"
	"io"
	"os"
	"path/filepath"
	"time"
)

// if caPrivKey is provided (to avoid asking for pw), then odirCA/ca.key
// is assummed to be encrypted and we will use caPrivKey instead.
func Step4_MakeCertificate(caPrivKey ed25519.PrivateKey, odirCA string,
	name string, odirCerts string, goodForDur time.Duration, verbose bool) {

	os.MkdirAll(odirCerts, 0700)
	ownerOnly(odirCerts)

	caPrivKeyPath := odirCA + sep + "ca.key"
	caCertPath := odirCA + sep + "ca.crt"

	//keyPath := fmt.Sprintf("%v%v%v.key", odirCerts, sep, name)
	csrInPath := fmt.Sprintf("%v%v%v.csr", odirCerts, sep, name)
	certOutPath := fmt.Sprintf("%v%v%v.crt", odirCerts, sep, name)

	makeCerts(caPrivKey, caPrivKeyPath, caCertPath, csrInPath, certOutPath, goodForDur, verbose)

	copyFileToDir(caCertPath, filepath.Dir(certOutPath))
	ownerOnly(certOutPath)

	// discard the Certificate signing requests; they are just confusing
	// and all the information is in the cert anyhow.
	os.Remove(csrInPath)
}

func makeCerts(caPrivKey ed25519.PrivateKey, caPrivKeyPath, caCertPath, csrInPath, certOutPath string,
	goodForDur time.Duration, verbose bool) {

	// Step 1: Load the CA certificate and CA private key
	if verbose {
		fmt.Printf("\nLoading Certificate Authority...\n")
	}

	caCert, caKey, err := loadCA(caPrivKey, caCertPath, caPrivKeyPath)
	if err != nil {
		log.Fatalf("Failed to load CA: %v", err)
	}

	// Step 2: Load the CSR from file
	csr, err := loadCSR(csrInPath)
	if err != nil {
		log.Fatalf("Failed to load CSR: %v", err)
	}

	// Step 3: Sign the certificate and save it
	err = signCertificate(csr, caCert, caKey, certOutPath, goodForDur)
	if err != nil {
		log.Fatalf("Failed to sign certificate: %v", err)
	}

	if verbose {
		log.Printf("Signed certificate saved to '%v'", certOutPath)
	}
}

// Load CA certificate
func caLoadCert(certPath string) (caCert *x509.Certificate, caCertBytes []byte, err error) {

	caCertBytes, err = ioutil.ReadFile(certPath)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to read CA certificate path '%v': %v", certPath, err)
	}
	caCertBlock, _ := pem.Decode(caCertBytes)
	if caCertBlock == nil || caCertBlock.Type != "CERTIFICATE" {
		return nil, nil, fmt.Errorf("failed to decode CA certificate PEM block from '%v'", certPath)
	}
	caCert, err = x509.ParseCertificate(caCertBlock.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse CA certificate '%v': %v", certPath, err)
	}
	return caCert, caCertBytes, nil
}

// Load the CA certificate and CA private key
func loadCA(caPrivKey ed25519.PrivateKey, certPath, keyPath string) (*x509.Certificate, ed25519.PrivateKey, error) {

	// Load CA certificate
	caCert, caCertPEMBytes, err := caLoadCert(certPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse CA certificate: %w", err)
	}
	_ = caCertPEMBytes

	if caPrivKey == nil {
		// Load CA private key

		caKeyBytes, err := ioutil.ReadFile(keyPath)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to read CA key file: %w", err)
		}

		if bytes.Contains(caKeyBytes, []byte("BEGIN ENCRYPTED PRIVATE KEY")) {
			caPrivKey, _, err = LoadEncryptedEd25519PrivateKey(keyPath)
			if err != nil {
				return nil, nil, fmt.Errorf("Failed to de-crypt with pass phrase "+
					"the private key for the CA '%v': error = '%v'", keyPath, err)
			}
		} else {

			caKeyBlock, _ := pem.Decode(caKeyBytes)
			if caKeyBlock == nil || caKeyBlock.Type != "PRIVATE KEY" {
				return nil, nil, fmt.Errorf("failed to decode CA private key PEM block")
			}
			caKey, err := x509.ParsePKCS8PrivateKey(caKeyBlock.Bytes)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to parse CA private key: %w", err)
			}

			// Ensure the key is an ED25519 private key
			ok := false
			caPrivKey, ok = caKey.(ed25519.PrivateKey)
			if !ok {
				return nil, nil, fmt.Errorf("not an ED25519 private key")
			}
		}
		return caCert, caPrivKey, nil
	}
	return caCert, caPrivKey, nil
}

// Load the CSR from a file
func loadCSR(csrPath string) (*x509.CertificateRequest, error) {
	csrBytes, err := ioutil.ReadFile(csrPath)
	if err != nil {
		return nil, fmt.Errorf("unable to read CSR file: %w", err)
	}
	csrBlock, _ := pem.Decode(csrBytes)
	if csrBlock == nil || csrBlock.Type != "CERTIFICATE REQUEST" {
		return nil, fmt.Errorf("failed to decode CSR PEM block")
	}
	csr, err := x509.ParseCertificateRequest(csrBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSR: %w", err)
	}
	return csr, nil
}

// Sign the CSR using the CA certificate and private key, and
// write the signed certificate to a file.
// Use goodForDur of 0 to get the maximum validity.
func signCertificate(
	csr *x509.CertificateRequest,
	caCert *x509.Certificate,
	caKey ed25519.PrivateKey,
	certPath string,
	goodForDur time.Duration,
) error {

	notAfter := time.Now().Add(goodForDur)
	if goodForDur == 0 {
		notAfter = maxValidityTime
	}

	// Create the certificate template
	certTemplate := x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      csr.Subject,
		NotBefore:    time.Now(),
		NotAfter:     notAfter,

		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},

		DNSNames:       csr.DNSNames,
		IPAddresses:    csr.IPAddresses,
		EmailAddresses: csr.EmailAddresses,
	}

	// Sign the certificate with the CA's private key
	certBytes, err := x509.CreateCertificate(nil, &certTemplate, caCert, csr.PublicKey, caKey)
	if err != nil {
		return fmt.Errorf("failed to create certificate: %w", err)
	}

	// Write the certificate to file
	certFile, err := os.Create(certPath)
	if err != nil {
		return fmt.Errorf("failed to create certificate file: %w", err)
	}
	defer ownerOnly(certPath)
	defer certFile.Close()

	err = pem.Encode(certFile, &pem.Block{Type: "CERTIFICATE", Bytes: certBytes})
	if err != nil {
		return fmt.Errorf("failed to write certificate to file: %w", err)
	}

	return nil
}

// copyFileToDir copies a file from copyMePath to the toDir directory
func copyFileToDir(copyMePath string, toDir string) error {
	// Open the source file
	sourceFile, err := os.Open(copyMePath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer sourceFile.Close()

	// Ensure the destination directory exists
	if err := os.MkdirAll(toDir, 0700); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}
	ownerOnly(toDir)

	// Construct the destination file path
	destFilePath := filepath.Join(toDir, filepath.Base(copyMePath))

	// Open the destination file
	destFile, err := os.Create(destFilePath)
	if err != nil {
		return fmt.Errorf("failed to create destination file: %w", err)
	}
	defer ownerOnly(destFilePath)
	defer destFile.Close()

	// Copy the contents of the source file to the destination file
	if _, err := io.Copy(destFile, sourceFile); err != nil {
		return fmt.Errorf("failed to copy file: %w", err)
	}

	return nil
}
