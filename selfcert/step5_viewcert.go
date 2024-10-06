package main

import (
	"crypto/ed25519"
	"crypto/x509"
	//"encoding/hex"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"log"
	//"math/big"
	//"net"
	"strings"
	"time"
)

// Load and parse the certificate from the PEM file
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

// Helper to format bytes into hex colon-separated format
func formatHexBytes(data []byte) string {
	formatted := make([]string, len(data))
	for i, b := range data {
		formatted[i] = fmt.Sprintf("%02x", b)
	}
	return strings.Join(formatted, ":")
}

// Print the certificate details
func printCertificateDetails(cert *x509.Certificate) {
	fmt.Printf("Certificate:\n")
	fmt.Printf("    Data:\n")
	fmt.Printf("        Version: %d (%#x)\n", cert.Version, cert.Version-1)
	fmt.Printf("        Serial Number: %d (%#x)\n", cert.SerialNumber, cert.SerialNumber)
	fmt.Printf("        Signature Algorithm: %s\n", cert.SignatureAlgorithm)
	fmt.Printf("        Issuer: %s\n", cert.Issuer)
	fmt.Printf("        Validity\n")
	fmt.Printf("            Not Before: %s\n", cert.NotBefore.Format(time.RFC1123Z))
	fmt.Printf("            Not After : %s\n", cert.NotAfter.Format(time.RFC1123Z))
	fmt.Printf("        Subject: %s\n", cert.Subject)
	fmt.Printf("        Subject Public Key Info:\n")
	fmt.Printf("            Public Key Algorithm: %s\n", cert.PublicKeyAlgorithm)

	// Display the public key if it is of type ed25519
	switch pub := cert.PublicKey.(type) {
	case ed25519.PublicKey:
		fmt.Printf("                ED25519 Public-Key:\n")
		fmt.Printf("                pub:\n")
		fmt.Printf("                    %s\n", formatHexBytes(pub))
	default:
		fmt.Printf("                Public Key not ED25519\n")
	}

	// Display the X509v3 extensions
	fmt.Printf("        X509v3 extensions:\n")
	for _, ext := range cert.Extensions {
		extName := ext.Id.String()

		// Handle known extensions
		switch extName {
		case "2.5.29.15": // Key Usage
			fmt.Printf("            X509v3 Key Usage: critical\n")
			if cert.KeyUsage&x509.KeyUsageCertSign != 0 {
				fmt.Printf("                Certificate Sign")
			}
			if cert.KeyUsage&x509.KeyUsageCRLSign != 0 {
				fmt.Printf(", CRL Sign\n")
			}
		case "2.5.29.37": // Extended Key Usage
			fmt.Printf("            X509v3 Extended Key Usage:\n")
			fmt.Printf("                Any Extended Key Usage\n")
		case "2.5.29.19": // Basic Constraints
			fmt.Printf("            X509v3 Basic Constraints: critical\n")
			fmt.Printf("                CA:TRUE\n")
		case "2.5.29.14": // Subject Key Identifier
			fmt.Printf("            X509v3 Subject Key Identifier:\n")
			fmt.Printf("                %s\n", formatHexBytes(ext.Value))
		default:
			fmt.Printf("            Unknown extension: %s\n", extName)
		}
	}

	// Signature
	fmt.Printf("    Signature Algorithm: %s\n", cert.SignatureAlgorithm)
	fmt.Printf("    Signature Value:\n")
	fmt.Printf("        %s\n", formatSignature(cert.Signature))
}

// Format the signature in hex
func formatSignature(signature []byte) string {
	lines := []string{}
	for i := 0; i < len(signature); i += 16 {
		end := i + 16
		if end > len(signature) {
			end = len(signature)
		}
		lines = append(lines, formatHexBytes(signature[i:end]))
	}
	return strings.Join(lines, "\n        ")
}

func main() {
	// Load the certificate from the PEM file
	cert, err := loadCertificate("my-keep-private-dir/ca.crt")
	if err != nil {
		log.Fatalf("Error loading certificate: %v", err)
	}

	// Print the certificate details
	printCertificateDetails(cert)
}
