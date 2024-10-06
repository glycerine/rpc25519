package main

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"log"
	//"os"
)

// Load and parse the PEM-encoded Ed25519 private key
func loadEd25519PrivateKey(keyPath string) (ed25519.PrivateKey, error) {
	// Read the private key file
	keyPEM, err := ioutil.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("unable to read private key file: %w", err)
	}

	// Decode the PEM block
	block, _ := pem.Decode(keyPEM)
	if block == nil || block.Type != "PRIVATE KEY" {
		return nil, fmt.Errorf("failed to decode PEM block containing private key")
	}

	// Parse the private key (Ed25519)
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %w", err)
	}

	// Assert the key type to Ed25519
	edKey, ok := key.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("not an Ed25519 private key")
	}

	return edKey, nil
}

// Load and parse the PEM-encoded certificate
func loadCertificate(certPath string) (*x509.Certificate, error) {
	// Read the certificate file
	certPEM, err := ioutil.ReadFile(certPath)
	if err != nil {
		return nil, fmt.Errorf("unable to read certificate file: %w", err)
	}

	// Decode the PEM block
	block, _ := pem.Decode(certPEM)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, fmt.Errorf("failed to decode PEM block containing certificate")
	}

	// Parse the certificate
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse certificate: %w", err)
	}

	return cert, nil
}

func main() {
	// Load the private key from the PEM file
	privateKeyPath := "static/certs/server/node.key"
	privateKey, err := loadEd25519PrivateKey(privateKeyPath)
	if err != nil {
		log.Fatalf("Error loading private key: %v", err)
	}
	_ = privateKey
	fmt.Printf("Private Key Loaded Successfully: %v\n", privateKeyPath)

	// Load the certificate from the PEM file
	certificate, err := loadCertificate("static/certs/server/node.crt")
	if err != nil {
		log.Fatalf("Error loading certificate: %v", err)
	}
	fmt.Printf("Certificate Loaded Successfully: Subject: %v\n", certificate.Subject)
	fmt.Printf("Certificate Loaded Successfully: DNSNames: %v\n", certificate.DNSNames)
	fmt.Printf("Certificate Loaded Successfully: EmailAddress: %v\n", certificate.EmailAddresses)
}
