package main

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/glycerine/rpc25519/selfcert"

	// for the nice base58 (version-checked) encoding of public keys
	"github.com/btcsuite/btcd/btcutil/base58"
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
}

type EncryptedKeyFile struct {
}

var sep = string(os.PathSeparator)

func (c *SelfCertConfig) DefineFlags(fs *flag.FlagSet) {

	fs.StringVar(&c.OdirCA_privateKey, "p", "my-keep-private-dir", "directory to find the CA in. If doing -ca, we will save the newly created Certificate Authority (CA) private key to this directory. If doing -k to create a new key, we'll look for the CA here.")

	fs.StringVar(&c.OdirCerts, "o", "certs", "directory to save newly created certs into.")

	fs.StringVar(&c.CreateKeyPairNamed, "k", "", "2nd of 2 steps: -k {key_name} ; create a new ed25519 key pair (key and cert) and save it to this name. The pair will be saved under the -o directory; we strongly suggest you also use the -e your_email@actually.org flag to describe the job and/or provide the owner's email to contact when needed. A CA will be auto-generated if none is found in the -p directory, which has a default name which warns the user to protect it.")

	fs.StringVar(&c.Viewpath, "v", "", "path to cert to view. Similar output as the openssl command: 'openssl x509 -in certs/client.crt  -text -noout', which you could use instead; just replace certs/client.crt with the path to your cert.")

	fs.StringVar(&c.Email, "e", "", "email to write into the certificate (who to contact about this job) (strongly encouraged when making new certs! defaults to name@host if not given)")

	fs.BoolVar(&c.CreateCA, "ca", false, "create a new self-signed certificate authority (1st of 2 steps in making new certs). Written to the -p directory (see selfy -p flag, where -p is for private). The CA uses ed25519 keys too.")

	fs.BoolVar(&c.Quiet, "quiet", false, "run quietly. don't print a log of actions taken as we go")
	fs.BoolVar(&c.SkipEncryptPrivateKeys, "nopass", false, "by default we request a password and use it with Argon2id to encrypt the private key file. Setting -nopass means we generate an un-encrypted private key; this is not recommended.")
}

// Call c.ValidateConfig() just after fs.Parse() to finish
// checking the config/filling in the config from the flags.
func (c *SelfCertConfig) ValidateConfig(fs *flag.FlagSet) (err error) {
	host, _ := os.Hostname()
	if c.CreateKeyPairNamed != "" && c.Email == "" {
		fmt.Fprintf(os.Stderr, "\n*****\narg! selfy is flumoxed: the selfy -e email flag was missing. Please use selfy -e your@email to tell trouble-shooters/debuggers how to contact you in case of trouble; and to give a human readable label to these cert identities(!) We'll auto-fill from as %v@%v for now. Bah!\n*****\n\n", c.CreateKeyPairNamed, host)
		c.Email = fmt.Sprintf("%v@%v", c.CreateKeyPairNamed, host)
	}
	return
}

func main() {
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

	var caPrivKey ed25519.PrivateKey
	if c.CreateCA {
		caPrivKey, err = selfcert.Step1_MakeCertificatAuthority(c.OdirCA_privateKey, verbose, !c.SkipEncryptPrivateKeys)
		if err != nil {
			log.Fatalf("selfy could not make Certficate Authority in '%v': '%v'", c.OdirCA_privateKey, err)
		}
	}

	if c.CreateKeyPairNamed != "" {
		if !DirExists(c.OdirCA_privateKey) || !FileExists(c.OdirCA_privateKey+sep+"ca.crt") {
			log.Printf("key-pair '%v' requested but CA does not exist in '%v'...\n  ... auto-generating a self-signed CA for your first...\n", c.CreateKeyPairNamed, c.OdirCA_privateKey)
			caPrivKey, err = selfcert.Step1_MakeCertificatAuthority(c.OdirCA_privateKey, verbose, !c.SkipEncryptPrivateKeys)
			if err != nil {
				log.Fatalf("selfy could not make Certficate Authority in '%v': '%v'", c.OdirCA_privateKey, err)
			}
		}
		privKey, err := selfcert.Step2_MakeEd25519PrivateKey(c.CreateKeyPairNamed, c.OdirCerts, verbose, !c.SkipEncryptPrivateKeys)
		if err != nil {
			log.Fatalf("selfy could not make private key '%v' in path '%v': '%v'", c.CreateKeyPairNamed, c.OdirCerts, err)
		}
		selfcert.Step3_MakeCertSigningRequest(privKey, c.CreateKeyPairNamed, c.Email, c.OdirCerts)
		selfcert.Step4_MakeCertificate(caPrivKey, c.OdirCA_privateKey, c.CreateKeyPairNamed, c.OdirCerts, verbose)
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
