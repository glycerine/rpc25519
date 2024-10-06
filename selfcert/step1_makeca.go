package selfcert

import (
	"crypto/ed25519"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log"
	"math/big"
	"os"
	"time"
)

const (
	country      = "US"
	state        = "State"
	locality     = "City"
	organization = "Organization"
	orgUnit      = "Org Unit"
	commonName   = "localhost"
	validFor     = 36600 * 24 * time.Hour // 100 years.
)

var sep = string(os.PathSeparator)

// pathCA "my-keep-private-dir" is the default.
func Step1_MakeCertificatAuthority(pathCA string) {
	// Step 1: Generate the ED25519 private key
	pubKey, privKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		log.Fatalf("Failed to generate ED25519 key: %v", err)
	}

	// Step 2: Create the certificate template
	caCertTemplate := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Country:            []string{country},
			Province:           []string{state},
			Locality:           []string{locality},
			Organization:       []string{organization},
			OrganizationalUnit: []string{orgUnit},
			CommonName:         commonName,
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(validFor),

		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	// Step 3: Add the required CA-specific extensions
	caCertTemplate.BasicConstraintsValid = true
	caCertTemplate.IsCA = true

	// Step 4: Self-sign the certificate using the private key
	certBytes, err := x509.CreateCertificate(nil, &caCertTemplate, &caCertTemplate, pubKey, privKey)
	if err != nil {
		log.Fatalf("Failed to create certificate: %v", err)
	}

	// Step 5: Write the private key to a file
	odir := pathCA + sep
	os.MkdirAll(odir, 0777)
	privfn := odir + "ca.key"
	privKeyFile, err := os.Create(privfn)
	if err != nil {
		log.Fatalf("Failed to create '%v': %v", privfn, err)
	}
	defer privKeyFile.Close()

	privKeyBytes, err := x509.MarshalPKCS8PrivateKey(privKey)
	if err != nil {
		log.Fatalf("Failed to marshal private key: %v", err)
	}

	if err := pem.Encode(privKeyFile, &pem.Block{Type: "PRIVATE KEY", Bytes: privKeyBytes}); err != nil {
		log.Fatalf("Failed to write private key to ca-key.pem: %v", err)
	}

	// Step 6: Write the self-signed certificate to a file
	certfn := odir + "ca.crt"
	certFile, err := os.Create(certfn)
	if err != nil {
		log.Fatalf("Failed to create %v: '%v'", certfn, err)
	}
	defer certFile.Close()

	if err := pem.Encode(certFile, &pem.Block{Type: "CERTIFICATE", Bytes: certBytes}); err != nil {
		log.Fatalf("Failed to write certificate to ca-cert.pem: %v", err)
	}

	log.Printf("CA private key and self-signed certificate generated successfully in '%v' and '%v'.", privfn, certfn)
}
