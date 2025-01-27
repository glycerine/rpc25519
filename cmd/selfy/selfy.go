package main

import (
	"crypto/ed25519"
	cryrand "crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/glycerine/rpc25519"
	"github.com/glycerine/rpc25519/selfcert"

	// for the nice base58 (version-checked) encoding of public keys
	"github.com/glycerine/base58"
)

type SelfCertConfig struct {
	OdirCerts              string
	OdirCA_privateKey      string
	CreateCA               bool
	CreateKeyPairNamed     string
	Viewpath               string
	Email                  string
	Quiet                  bool
	SkipEncryptPrivateKeys bool
	GenSymmetricKey32bytes string

	// verify that cert was signed by the private key
	// corresponding to OdirCA_privateKey/ca.crt
	VerifySignatureOnCertPath string

	AuthorityValidForDur time.Duration
	CertValidForDur      time.Duration

	// initial flag values go here, so we can support d (day), and y (year)
	certValidForDurStr      string
	authorityValidForDurStr string
}

type EncryptedKeyFile struct {
}

var sep = string(os.PathSeparator)

func (c *SelfCertConfig) DefineFlags(fs *flag.FlagSet) {

	certdir := rpc25519.GetCertsDir()
	cadir := rpc25519.GetPrivateCertificateAuthDir()

	fs.StringVar(&c.OdirCA_privateKey, "p", cadir, "directory to find the CA in. If doing -ca, we will save the newly created Certificate Authority (CA) private key to this directory. If doing -k to create a new key, we'll look for the CA here.")

	fs.StringVar(&c.OdirCerts, "o", certdir, "directory to save newly created certs into.")

	fs.StringVar(&c.CreateKeyPairNamed, "k", "", "2nd of 2 steps: -k {key_name} ; create a new ed25519 key pair (key and cert) and save it to this name. The pair will be saved under the -o directory; we strongly suggest you also use the -e your_email@actually.org flag to describe the job and/or provide the owner's email to contact when needed. A CA will be auto-generated if none is found in the -p directory, which has a default name which warns the user to protect it.")

	fs.StringVar(&c.Viewpath, "v", "", "path to cert to view. Similar output as the openssl command: 'openssl x509 -in certs/client.crt  -text -noout', which you could use instead; just replace certs/client.crt with the path to your cert.")

	fs.StringVar(&c.Email, "e", "", "email to write into the certificate (who to contact about this job) (strongly encouraged when making new certs! defaults to name@host if not given)")

	fs.BoolVar(&c.CreateCA, "ca", false, "create a new self-signed certificate authority (1st of 2 steps in making new certs). Written to the -p directory (see selfy -p flag, where -p is for private). The CA uses ed25519 keys too.")

	fs.BoolVar(&c.Quiet, "quiet", false, "run quietly. don't print a log of actions taken as we go")
	fs.BoolVar(&c.SkipEncryptPrivateKeys, "nopass", false, "by default we request a password and use it with Argon2id to encrypt the private key file. Setting -nopass means we generate an un-encrypted private key; this is not recommended.")

	fs.StringVar(&c.GenSymmetricKey32bytes, "gensym", "", "generate a new 32-byte symmetric encryption key with crypto/rand, and save it under this filename in the -p directory.")

	fs.StringVar(&c.VerifySignatureOnCertPath, "verify", "", "verify this path is a certificate signed by the private key corresponding to the -p {my-keep-private-dir}/ca.crt public key")

	fs.StringVar(&c.certValidForDurStr, "cert-validfor", "", "set this duration string as the lifetime of the cert key-pair. Default empty means never expires, which is recommended. If you absolutely require a lifetime on your certs, this flag will set it on creation. Without a -k flag, this option is meaningless. Supports suffixes d and y for days and years. (note! the m suffix is for minutes!)")

	fs.StringVar(&c.authorityValidForDurStr, "ca-validfor", "", "set this duration string as the lifetime of the Certificate Authority key pair. Default empty means never expires, which is recommended. If you absolutely require a lifetime on your CA key-pairs, this flag will set it on creation. Supports suffixes d and y for days and years. (note! the m suffix is for minutes!)")

}

// Call c.ValidateConfig() just after fs.Parse() to finish
// checking the config/filling in the config from the flags.
func (c *SelfCertConfig) ValidateConfig(fs *flag.FlagSet) (err error) {
	host, _ := os.Hostname()
	if c.CreateKeyPairNamed != "" && c.Email == "" {
		fmt.Fprintf(os.Stderr, "\n*****\narg! selfy is flumoxed: the selfy -e email flag was missing. Please use selfy -e your@email to tell trouble-shooters/debuggers how to contact you in case of trouble; and to give a human readable label to these cert identities(!) We'll auto-fill from as %v@%v for now. Bah!\n*****\n\n", c.CreateKeyPairNamed, host)
		c.Email = fmt.Sprintf("%v@%v", c.CreateKeyPairNamed, host)
	}
	if c.VerifySignatureOnCertPath != "" && !FileExists(c.VerifySignatureOnCertPath) {
		return fmt.Errorf("selfy -verify path not found: '%v'", c.VerifySignatureOnCertPath)
	}

	c.CertValidForDur, err = ParseDurationDaysYearsToo(c.certValidForDurStr)
	if err != nil {
		return err
	}
	c.AuthorityValidForDur, err = ParseDurationDaysYearsToo(c.authorityValidForDurStr)
	if err != nil {
		return err
	}

	return
}

func main() {

	rpc25519.Exit1IfVersionReq()

	log.SetFlags(log.LstdFlags | log.Lshortfile) // Add Lshortfile for short file names

	myflags := flag.NewFlagSet("selfy", flag.ExitOnError)
	c := &SelfCertConfig{}
	c.DefineFlags(myflags)

	err := myflags.Parse(os.Args[1:])
	if err != nil {
		log.Fatalf("selfy command line flag parse error: '%v'", err)
	}
	err = c.ValidateConfig(myflags)
	if err != nil {
		log.Fatalf("selfy command line flag error: '%v'", err)
	}

	verbose := !c.Quiet

	if c.VerifySignatureOnCertPath != "" {
		caCertPath := c.OdirCA_privateKey + sep + "ca.crt"
		verifyMeCertPath := c.VerifySignatureOnCertPath
		err = selfcert.Step7_VerifyCertIsSignedByCertificateAuthority(verifyMeCertPath, caCertPath, verbose)
		if err != nil {
			log.Fatalf("error: cert '%v' was NOT signed by '%v': '%v'", verifyMeCertPath, caCertPath, err)
		} else {
			fmt.Printf("cert '%v' was indeed signed by '%v'.\n", verifyMeCertPath, caCertPath)
		}
		return
	}

	var caPrivKey ed25519.PrivateKey
	var caValidForDur time.Duration // 0 => max validity
	caValidForDur = c.AuthorityValidForDur
	if c.CreateCA {
		caPrivKey, err = selfcert.Step1_MakeCertificateAuthority(
			c.OdirCA_privateKey, verbose, !c.SkipEncryptPrivateKeys, caValidForDur)
		if err != nil {
			log.Fatalf("selfy could not make Certficate Authority in '%v': '%v'", c.OdirCA_privateKey, err)
		}
	}

	if c.CreateKeyPairNamed != "" {

		if !c.SkipEncryptPrivateKeys {
			fmt.Printf("\nincautious reminder: the selfy -nopass flag will omit password protection...\n\n")
		}
		if !DirExists(c.OdirCA_privateKey) || !FileExists(c.OdirCA_privateKey+sep+"ca.crt") {
			log.Printf("key-pair '%v' requested but CA does not exist in '%v'...\n  ... auto-generating a self-signed CA for your first...\n", c.CreateKeyPairNamed, c.OdirCA_privateKey)
			caPrivKey, err = selfcert.Step1_MakeCertificateAuthority(c.OdirCA_privateKey, verbose, !c.SkipEncryptPrivateKeys, caValidForDur)
			if err != nil {
				log.Fatalf("selfy could not make Certficate Authority in '%v': '%v'", c.OdirCA_privateKey, err)
			}
		}
		privKey, err := selfcert.Step2_MakeEd25519PrivateKey(c.CreateKeyPairNamed, c.OdirCerts, verbose, !c.SkipEncryptPrivateKeys)
		if err != nil {
			log.Fatalf("selfy could not make private key '%v' in path '%v': '%v'", c.CreateKeyPairNamed, c.OdirCerts, err)
		}
		selfcert.Step3_MakeCertSigningRequest(privKey, c.CreateKeyPairNamed, c.Email, c.OdirCerts)
		var goodForDur time.Duration // 0 => max validity
		goodForDur = c.CertValidForDur
		if caValidForDur > 0 {
			// keep cert lifetime inside CA lifetime.
			// This makes it easier to gen on cmd line.
			// You only have to specify one, either -ca-validfor or -cert-validfor
			if goodForDur == 0 {
				goodForDur = caValidForDur
			} else {
				// cert can be <= ca, but not more
				if goodForDur > caValidForDur {
					goodForDur = caValidForDur
				}
			}
		}
		selfcert.Step4_MakeCertificate(caPrivKey, c.OdirCA_privateKey, c.CreateKeyPairNamed, c.OdirCerts, goodForDur, verbose)
	}

	// useful utilities. Not needed to make keys and certificates.
	if c.Viewpath != "" {
		cert, err, wasPrivKey := selfcert.Step5_ViewCertificate(c.Viewpath)
		if err != nil {
			log.Fatalf("Error loading '%v': %v", c.Viewpath, err)
		}
		if wasPrivKey {
			return
		}

		ed, ok := cert.PublicKey.(ed25519.PublicKey)
		if ok {
			pubkey58 := toBase58Check(ed)
			fmt.Printf("\nthe raw public key is %v bytes, but to make it readable:\n", len(ed))
			fmt.Printf("\nhex encoded public-key:          %x (%v bytes)\n", ed, len(ed)*2)
			fmt.Printf("\nbase58-check encoded public-key: %v (%v bytes)\n", pubkey58, len(pubkey58))
		} else {
			panic(fmt.Sprintf("what? not a ed25519 key? type = '%T'", cert.PublicKey))
		}
	}

	if c.GenSymmetricKey32bytes != "" {

		odir := c.OdirCA_privateKey
		key := newXChaCha20CryptoRandKey()
		path := odir + string(os.PathSeparator) + c.GenSymmetricKey32bytes

		if FileExists(path) {
			fmt.Fprintf(os.Stderr, "ERROR! selfy -gensym '%v' already exists. "+
				"refusing to overwrite.\n", path)
			os.Exit(1)
		}

		os.MkdirAll(odir, 0700)
		ownerOnly(odir)

		fd, err := os.Create(path)
		panicOn(err)
		_, err = fd.Write(key)
		panicOn(err)
		panicOn(fd.Close())
		ownerOnly(path)
		fmt.Printf("\nwrote 32 cryptographically random bytes to %v\n", path)
	}
}

// we always use 255, which is -1 in 8-bit 2's compliment.
const VersionByteBase59Checked byte = 255

func toBase58Check(by []byte) string {
	return base58.CheckEncode(by, VersionByteBase59Checked)
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

func newXChaCha20CryptoRandKey() []byte {
	key := make([]byte, 32)
	if _, err := cryrand.Read(key); err != nil {
		log.Fatal(err)
	}
	return key
}

func panicOn(err error) {
	if err != nil {
		panic(err)
	}
}

func ParseDurationDaysYearsToo(s string) (dur time.Duration, err error) {

	if s == "" {
		return
	}

	// Handle year suffix
	if strings.HasSuffix(s, "y") {
		years, err := strconv.ParseFloat(s[:len(s)-1], 64)
		if err != nil {
			return 0, err
		}
		// Approximate a year as 365.25 days to account for leap years
		return time.Duration(years * 365.25 * 24 * float64(time.Hour)), nil
	}

	// Handle day suffix
	if strings.HasSuffix(s, "d") {
		days, err := strconv.ParseFloat(s[:len(s)-1], 64)
		if err != nil {
			return 0, err
		}
		return time.Duration(days * 24 * float64(time.Hour)), nil
	}

	// Use standard time.ParseDuration for other suffixes
	return time.ParseDuration(s)
}
