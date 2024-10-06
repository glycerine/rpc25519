package selfcert

import (
	"crypto/ed25519"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"

	"encoding/hex"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	//"math/big"
	//"net"
	"strings"
	"time"
)

var _ = hex.EncodeToString

// optional
func Step5_ViewCertificate(path string) {
	// Load the certificate from the PEM file
	cert, err := loadCertificate(path)
	if err != nil {
		log.Fatalf("Error loading certificate: %v", err)
	}

	// Print the certificate details
	printCertificateDetails(cert)
}

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

	// Print X.509v3 extensions
	fmt.Printf("        X509v3 extensions:\n")

	// Print Basic Constraints
	printBasicConstraints(cert)

	// Print Key Usage
	printKeyUsage(cert)

	// Print Extended Key Usage
	printExtendedKeyUsage(cert)

	// Parse X.509v3 extensions to match OpenSSL output
	for _, ext := range cert.Extensions {

		// Key Usage
		//if ext.Id.Equal([]int{2, 5, 29, 15}) { // Key Usage OID
		//	printKeyUsage(ext)
		//}

		// Subject Key Identifier
		if ext.Id.Equal([]int{2, 5, 29, 14}) {
			printSubjectKeyIdentifier(ext)
		}

		// Authority Key Identifier
		if ext.Id.Equal([]int{2, 5, 29, 35}) {
			printAuthorityKeyIdentifier(ext)
		}

		// Subject Alternative Name
		if ext.Id.Equal([]int{2, 5, 29, 17}) {
			printSubjectAlternativeName(ext)
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

func printExtendedKeyUsage(cert *x509.Certificate) {
	if len(cert.ExtKeyUsage) > 0 {
		usages := []string{}

		for _, usage := range cert.ExtKeyUsage {
			switch usage {
			case x509.ExtKeyUsageAny:
				usages = append(usages, "Any Extended Key Usage")
				// If "Any Extended Key Usage" is set, no need to print others
				break
			case x509.ExtKeyUsageServerAuth:
				usages = append(usages, "TLS Web Server Authentication")
			case x509.ExtKeyUsageClientAuth:
				usages = append(usages, "TLS Web Client Authentication")
			case x509.ExtKeyUsageCodeSigning:
				usages = append(usages, "Code Signing")
			case x509.ExtKeyUsageEmailProtection:
				usages = append(usages, "E-mail Protection")
			case x509.ExtKeyUsageTimeStamping:
				usages = append(usages, "Time Stamping")
			case x509.ExtKeyUsageOCSPSigning:
				usages = append(usages, "OCSP Signing")
			case x509.ExtKeyUsageMicrosoftServerGatedCrypto:
				usages = append(usages, "Microsoft Server Gated Crypto")
			case x509.ExtKeyUsageNetscapeServerGatedCrypto:
				usages = append(usages, "Netscape Server Gated Crypto")
			case x509.ExtKeyUsageIPSECEndSystem:
				usages = append(usages, "IPSec End System")
			case x509.ExtKeyUsageIPSECTunnel:
				usages = append(usages, "IPSec Tunnel")
			case x509.ExtKeyUsageIPSECUser:
				usages = append(usages, "IPSec User")
			case x509.ExtKeyUsageMicrosoftCommercialCodeSigning:
				usages = append(usages, "Microsoft Commercial Code Signing")
			case x509.ExtKeyUsageMicrosoftKernelCodeSigning:
				usages = append(usages, "Microsoft Kernel Code Signing")
			}
		}

		fmt.Printf("            X509v3 Extended Key Usage:\n")
		fmt.Printf("                %s\n", strings.Join(usages, ", "))
	}
}

// Helper to print Authority Key Identifier
func printAuthorityKeyIdentifier(ext pkix.Extension) {
	fmt.Printf("            X509v3 Authority Key Identifier:\n")
	// The value is typically the key identifier in hex format
	var authorityKeyIdentifier struct {
		KeyIdentifier []byte `asn1:"optional,tag:0"`
	}
	_, err := asn1.Unmarshal(ext.Value, &authorityKeyIdentifier)
	if err == nil {
		//fmt.Printf("                %s\n", strings.ToUpper(hex.EncodeToString(authorityKeyIdentifier.KeyIdentifier)))
		fmt.Printf("                %s\n", strings.ToUpper(formatHexBytes(authorityKeyIdentifier.KeyIdentifier)))
	}
}

// Helper to print Subject Alternative Name (SAN)
func printSubjectAlternativeName(ext pkix.Extension) {
	fmt.Printf("            X509v3 Subject Alternative Name:\n")
	var altNames []asn1.RawValue
	_, err := asn1.Unmarshal(ext.Value, &altNames)
	if err == nil {
		for _, altName := range altNames {
			switch altName.Tag {
			case 2: // DNS name
				fmt.Printf("                DNS:%s\n", string(altName.Bytes))
			case 1: // Email address
				fmt.Printf("                email:%s\n", string(altName.Bytes))
			case 7: // IP address
				ip := net.IP(altName.Bytes)
				fmt.Printf("                IP Address:%s\n", ip)
			}
		}
	}
}

// Helper to print Basic Constraints
func printBasicConstraints(cert *x509.Certificate) {
	fmt.Printf("            X509v3 Basic Constraints: critical\n")
	if cert.IsCA {
		fmt.Printf("                CA:TRUE\n")
	} else {
		fmt.Printf("                CA:FALSE\n")
	}
}

// Helper to print Subject Key Identifier
func printSubjectKeyIdentifier(ext pkix.Extension) {
	fmt.Printf("            X509v3 Subject Key Identifier:\n")
	// The value is typically the key identifier in hex format
	var subjectKeyIdentifier []byte
	_, err := asn1.Unmarshal(ext.Value, &subjectKeyIdentifier)
	if err == nil {
		//fmt.Printf("                %s\n", strings.ToUpper(hex.EncodeToString(subjectKeyIdentifier)))
		fmt.Printf("                %s\n", strings.ToUpper(formatHexBytes(subjectKeyIdentifier)))
	}
}

// Key Usage bits mapping
var keyUsageNames = []string{
	"Digital Signature",
	"Non Repudiation",
	"Key Encipherment",
	"Data Encipherment",
	"Key Agreement",
	"Certificate Sign",
	"CRL Sign",
	"Encipher Only",
	"Decipher Only",
}

// Function to parse and display Key Usage using the x509.Certificate struct
func printKeyUsage(cert *x509.Certificate) {
	if cert.KeyUsage != 0 { // Check if Key Usage is set
		fmt.Printf("            X509v3 Key Usage: critical\n")
		usages := []string{}

		// Map the bitmask to human-readable usage descriptions
		if cert.KeyUsage&x509.KeyUsageDigitalSignature != 0 {
			usages = append(usages, "Digital Signature")
		}
		if cert.KeyUsage&x509.KeyUsageContentCommitment != 0 { // This is "Non Repudiation"
			usages = append(usages, "Non Repudiation (Content Commitment)")
		}
		if cert.KeyUsage&x509.KeyUsageKeyEncipherment != 0 {
			usages = append(usages, "Key Encipherment")
		}
		if cert.KeyUsage&x509.KeyUsageDataEncipherment != 0 {
			usages = append(usages, "Data Encipherment")
		}
		if cert.KeyUsage&x509.KeyUsageKeyAgreement != 0 {
			usages = append(usages, "Key Agreement")
		}
		if cert.KeyUsage&x509.KeyUsageCertSign != 0 {
			usages = append(usages, "Certificate Sign")
		}
		if cert.KeyUsage&x509.KeyUsageCRLSign != 0 {
			usages = append(usages, "CRL Sign")
		}
		if cert.KeyUsage&x509.KeyUsageEncipherOnly != 0 {
			usages = append(usages, "Encipher Only")
		}
		if cert.KeyUsage&x509.KeyUsageDecipherOnly != 0 {
			usages = append(usages, "Decipher Only")
		}

		// Print the joined usages in a comma-separated list
		fmt.Printf("                %s\n", strings.Join(usages, ", "))
	}
}
