package selfcert

import (
	"bytes"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"log"
	//"os"
)

// LoadNodeTLSConfigProtected will prompt
// for the pass-phrase if the key is protected.
func LoadNodeTLSConfigProtected(caCertPath, clientCertPath, privateKeyPath string) (*tls.Config, error) {

	// Load the private key from the PEM file
	privateKey, keyPEM, err := LoadEd25519PrivateKey(privateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("error in LoadNodeTLSConfigProtected "+
			"loading client private key '%v': %v", privateKeyPath, err)
	}
	_ = privateKey

	// Load the certificate from the PEM file

	certPEM, err := ioutil.ReadFile(clientCertPath)
	if err != nil {
		return nil, fmt.Errorf("error in LoadNodeTLSConfigProtected: unable to read certificate file '%v': %v", clientCertPath, err)
	}

	// Decode the PEM block
	block, _ := pem.Decode(certPEM)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, fmt.Errorf("error in LoadNodeTLSConfigProtected: failed to decode PEM block containing certificate, from path '%v'", clientCertPath)
	}

	// Parse the certificate
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("error in LoadNodeTLSConfigProtected "+
			"loading client certificate path '%v': %v", clientCertPath, err)
	}
	_ = certificate

	caCert, caPEM, err := caLoadCert(caCertPath)
	if err != nil {
		return nil, fmt.Errorf("error in LoadNodeTLSConfigProtected "+
			"loading CA cert path '%v': '%v'", caCertPath, err)
	}
	_ = caCert

	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("error in LoadNodeTLSConfigProtected: "+
			"failed call to tls.X509Keypair(): '%v'", err)
	}

	certPool := x509.NewCertPool()

	if !certPool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("error in LoadNodeTLSConfigProtected: " +
			"failed to parse PEM data to certPool")
	}

	cfg := &tls.Config{
		RootCAs: certPool,
		CipherSuites: []uint16{
			tls.TLS_CHACHA20_POLY1305_SHA256,
		},
		MinVersion: tls.VersionTLS13,
		ClientAuth: tls.RequireAndVerifyClientCert,

		ClientCAs:                certPool,
		PreferServerCipherSuites: true,
	}

	cfg.Certificates = []tls.Certificate{pair}

	//vv("loaded fine: caCertPath = '%v', clientCertPath='%v', privateKeyPath='%v'", caCertPath, clientCertPath, privateKeyPath)
	return cfg, nil
}

// typcially:
//
//	privateKeyPath = "static/certs/server/node.key"
//	certKeyPath = "static/certs/server/node.crt"
func Step6_LoadKeyPair(privateKeyPath, certPath string) {

	// Load the private key from the PEM file
	privateKey, _, err := LoadEd25519PrivateKey(privateKeyPath)
	if err != nil {
		log.Fatalf("Error loading private key: %v", err)
	}

	_ = privateKey
	fmt.Printf("Private Key Loaded Successfully: %v\n", privateKeyPath)

	// Load the certificate from the PEM file
	certificate, err, wasPrivKey := loadCertificate(certPath)
	if err != nil {
		log.Fatalf("Error loading certificate: %v", err)
	}
	if wasPrivKey {
		log.Fatalf("Arg! path '%v' was a private key and not a cert", certPath)
	}
	fmt.Printf("Certificate Loaded Successfully: Subject: %v\n", certificate.Subject)
	fmt.Printf("Certificate Loaded Successfully: DNSNames: %v\n", certificate.DNSNames)
	fmt.Printf("Certificate Loaded Successfully: EmailAddress: %v\n", certificate.EmailAddresses)
}

// Load and parse the PEM-encoded Ed25519 private key
func LoadEd25519PrivateKey(keyPath string) (edKey ed25519.PrivateKey, keyPEM []byte, err error) {
	// Read the private key file
	keyPEM, err = ioutil.ReadFile(keyPath)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to read private key file: %w", err)
	}

	if bytes.Contains(keyPEM, []byte("BEGIN ENCRYPTED PRIVATE KEY")) {
		return LoadEncryptedEd25519PrivateKey(keyPath)
	}

	// Decode the PEM block
	block, _ := pem.Decode(keyPEM)
	if block == nil || block.Type != "PRIVATE KEY" {
		return nil, nil, fmt.Errorf("failed to decode PEM block containing private key")
	}

	// Parse the private key (Ed25519)
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse private key: %w", err)
	}

	// Assert the key type to Ed25519
	edKey, ok := key.(ed25519.PrivateKey)
	if !ok {
		return nil, nil, fmt.Errorf("not an Ed25519 private key")
	}

	return edKey, keyPEM, nil
}
