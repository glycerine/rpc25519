package selfcert

import (
	"crypto/ed25519"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"log"
	"math/big"
	"os"
	"time"
)

/*
Small rant: expiring certificates are the worst idea
in the security theater of "current practices" today.

Do you want your encrypted backups to suddenly be inaccessible
because your keys have expired? No.

Do you want your billions in digital currency saved safely
in a digital vault or block-chain to suddenly become worthless because
your key expired? Again, no.

Having an expiration date on public keys is an awful,
terrible foot-gun. Just say no.

Expiring certs are a game to keep the for-profit cert authorities
in business, so they can keep renting you their "authority".

We have no use for this charade.

Expiration is actively a bad idea. Revoke keys
when you must, but keep the good ones working as designed.

See https://www.rfc-editor.org/rfc/rfc5280#section-4.1.2.5
where this use case is discussed:

"To indicate that a certificate has no well-defined
expiration date, the notAfter SHOULD be assigned
the GeneralizedTime value of 99991231235959Z."

*/

const (
	// RFC 5280 max date: 99991231235959Z
	MaxValidityTimeConst = "9999-12-31T23:59:59Z"
)

var maxValidityTime time.Time

func init() {
	var err error
	maxValidityTime, err = time.Parse(time.RFC3339, MaxValidityTimeConst)
	if err != nil {
		panic(fmt.Errorf("failed to parse max validity time: %w", err))
	}
}

const (
	country      = "US"
	state        = "State"
	locality     = "City"
	organization = "Organization"
	orgUnit      = "Org Unit"
	commonName   = "localhost"
)

var sep = string(os.PathSeparator)

// pathCA "my-keep-private-dir" is the default.
// return the un-encrypted key to be used in subsequent signing steps without
// having to request the passphrase again.
// Use validFor == 0 to get the maximum validity defined by the spec;
// no expiration.
func Step1_MakeCertificateAuthority(pathCA string, verbose bool, encrypt bool, validFor time.Duration) (ed25519.PrivateKey, error) {
	// Step 1: Generate the ED25519 private key
	pubKey, privKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		log.Fatalf("Failed to generate ED25519 key: %v", err)
	}

	notAfter := time.Now().Add(validFor)
	if validFor == 0 {
		notAfter = maxValidityTime
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
		NotAfter:  notAfter,

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
	os.MkdirAll(odir, 0700)
	ownerOnly(odir)

	privfn := odir + "ca.key"

	if encrypt {

		privKeyBytes, err := x509.MarshalPKCS8PrivateKey(privKey)
		if err != nil {
			log.Fatalf("Failed to marshal private key: %v", err)
		}

		fmt.Printf("===============================================\n")
		fmt.Printf("=========  Making new self-signed CA  =========\n")
		fmt.Printf("=========\n")
		fmt.Printf("=========  Setting pass phrase for the Certificate Authority private key:\n")
		fmt.Printf("=========\n")
		err = SavePrivateKeyToPathUnderPassphrase(privKeyBytes, privfn)
		if err != nil {
			log.Fatalf("Failed to create encrypted key path '%v': %v", privfn, err)
		}
		ownerOnly(privfn)
	} else {

		privKeyFile, err := os.Create(privfn)
		if err != nil {
			log.Fatalf("Failed to create '%v': %v", privfn, err)
		}
		defer ownerOnly(privfn)
		defer privKeyFile.Close()

		privKeyBytes, err := x509.MarshalPKCS8PrivateKey(privKey)
		if err != nil {
			log.Fatalf("Failed to marshal private key: %v", err)
		}

		if err := pem.Encode(privKeyFile, &pem.Block{Type: "PRIVATE KEY", Bytes: privKeyBytes}); err != nil {
			log.Fatalf("Failed to write private key to ca-key.pem: %v", err)
		}
	}

	// Step 6: Write the self-signed certificate to a file
	certfn := odir + "ca.crt"
	certFile, err := os.Create(certfn)
	if err != nil {
		log.Fatalf("Failed to create %v: '%v'", certfn, err)
	}
	defer ownerOnly(certfn)
	defer certFile.Close()

	if err := pem.Encode(certFile, &pem.Block{Type: "CERTIFICATE", Bytes: certBytes}); err != nil {
		log.Fatalf("Failed to write certificate to ca-cert.pem: %v", err)
	}

	if verbose {
		log.Printf("CA private key and self-signed certificate generated successfully in '%v' and '%v'.", privfn, certfn)
	}
	return privKey, nil
}

// chmod og-wrx path
func ownerOnly(path string) error {

	// Get the current file info
	fileInfo, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("Error getting file '%v' stat: '%v'", path, err)
	}

	// Get the current permissions
	currentPerm := fileInfo.Mode().Perm()

	// Remove read, write, and execute permissions for group and others
	newPerm := currentPerm &^ (os.FileMode(0o077))

	// Change the file permissions
	err = os.Chmod(path, newPerm)
	if err != nil {
		return fmt.Errorf("Error changing file permissions on '%v': '%v'", path, err)
	}
	return nil
}
