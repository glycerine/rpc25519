package selfcert

import (
	//"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
)

func Step7_VerifyCertIsSignedByCertificatAuthority(verifyMeCertPath, caCertPath string, verbose bool) error {

	// Load CA certificate
	if !fileExists(caCertPath) {
		return fmt.Errorf("path not found '%v'", caCertPath)
	}
	caCertBytes, err := ioutil.ReadFile(caCertPath)
	if err != nil {
		return fmt.Errorf("unable to read CA certificate path '%v': %v", caCertPath, err)
	}
	caCertBlock, _ := pem.Decode(caCertBytes)
	if caCertBlock == nil || caCertBlock.Type != "CERTIFICATE" {
		return fmt.Errorf("failed to decode CA certificate PEM block")
	}
	caCert, err := x509.ParseCertificate(caCertBlock.Bytes)
	if err != nil {
		return fmt.Errorf("failed to parse CA certificate: %v", err)
	}

	// load verifyMeCertPath
	if !fileExists(verifyMeCertPath) {
		return fmt.Errorf("path not found '%v'", verifyMeCertPath)
	}
	verifyMeCertBytes, err := ioutil.ReadFile(verifyMeCertPath)
	if err != nil {
		return fmt.Errorf("unable to read verifyMeCertPath '%v' file: %v", verifyMeCertPath, err)
	}
	verifyMeCertBlock, _ := pem.Decode(verifyMeCertBytes)
	if verifyMeCertBlock == nil || verifyMeCertBlock.Type != "CERTIFICATE" {
		return fmt.Errorf("failed to decode CA certificate PEM block")
	}
	verifyMeCert, err := x509.ParseCertificate(verifyMeCertBlock.Bytes)
	if err != nil {
		return fmt.Errorf("failed to parse CA certificate: %v", err)
	}

	// Verify that verifyMeCert is signed by caCert
	roots := x509.NewCertPool()
	roots.AddCert(caCert)

	_, err = verifyMeCert.Verify(x509.VerifyOptions{
		Roots: roots,
	})

	if verbose {
		if err != nil {
			fmt.Println("The certificate failed verification.")
		} else {
			fmt.Println("The certificate is valid and signed by the CA.")
		}
	}

	return err
}
