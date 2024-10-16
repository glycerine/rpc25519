package rpc25519

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"strings"

	// for the nice base58 (version-checked) encoding of public keys
	"github.com/btcsuite/btcd/btcutil/base58"
)

var ErrPubKeyMismath = fmt.Errorf("remote host pubkey does not match that on file!")
var ErrPubKeyUnknown = fmt.Errorf("remote host pubkey is not on file, and TOFU is off!")

// KnownKey saved to a file results in lines like
// 127.0.0.1 pubkey@edwardsRPC-ed25519-b58c-9ZrrEXxvoqmj9UkgiPjHNZP41N9wuLyQTEUCg5S7VjPuJbXXL8a:froggy@example.com
type KnownKey struct {
	Addr    string // 192.168.254.151:8443
	KeyType string // ed25519
	PubKey  string // 9aTjVYv1K7vj3WYX3EktjaGPycNwym5Rn5Vo1WuxLdF7bxpMDV6
	Emails  string
	Line    int
}

func (kh *KnownKey) String() string {
	return fmt.Sprintf("%v pubkey@edwardsRPC-%v-b58c-%v:%v", kh.Addr, kh.KeyType, kh.PubKey, kh.Emails)
}

// e.g. "pubkey@edwardsRPC-ed25519-b58c-9ZrrEXxvoqmj9UkgiPjHNZP41N9wuLyQTEUCg5S7VjPuJbXXL8a:froggy@example.com"
func (kh *KnownKey) IdentityString() string {
	return fmt.Sprintf("pubkey@edwardsRPC-%v-b58c-%v:%v", kh.KeyType, kh.PubKey, kh.Emails)
}

type Known struct {
	Path  string
	Hosts []*KnownKey

	// "6v0qwCgyE6SrR7DYsuvbjih67HppzHBLPfSxnE" -> *KnownKey
	PubKeyMap map[string]*KnownKey
}

func NewKnown(path string) *Known {
	return &Known{
		Path:      path,
		PubKeyMap: make(map[string]*KnownKey),
	}
}

func (k *Known) WriteOut() (err error) {

	// try to back up any corrupt/previous file for inspection
	if fileExists(k.Path) {
		old := k.Path + ".old"
		os.Remove(old)
		os.Rename(k.Path, old)
	}

	fd, err := os.Create(k.Path)
	panicOn(err)
	defer fd.Close()
	fmt.Fprintf(fd, "# known_pub_keys file: address pubkey-identity-string\n")
	for _, host := range k.Hosts {
		fmt.Fprintf(fd, "%v\n", host.String())
	}
	return
}

func (k *Known) Add(kh *KnownKey) {

	_, found := k.PubKeyMap[kh.PubKey]
	if !found {
		k.PubKeyMap[kh.PubKey] = kh
		k.Hosts = append(k.Hosts, kh)
	}
}

var ErrNotFound = fmt.Errorf("known_tls_hosts file not found")

func readKnownKeys(path string) (kn *Known, err error) {

	if !fileExists(path) {
		return nil, ErrNotFound
	}

	by, err := os.ReadFile(path)
	if err != nil {
		vv("Failed to read file: %v", err)
		return nil, err

	}
	s := string(by)

	kn = NewKnown(path)
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		//vv("line i=%v is '%v'", i, line)
		l := strings.TrimSpace(line)
		if len(l) == 0 {
			continue // skip empty lines
		}
		if l[0] == '#' {
			continue // skip comments
		}
		// addr      pubkey-identity-string (artificially shorted for readability)
		// 127.0.0.1 pubkey@edwardsRPC-ed25519-b58c-9aTjVYv1K:froggy@example.com
		splt := strings.Fields(l)
		//vv("splt = '%#v'", splt)
		if len(splt) >= 2 {
			addr := splt[0]
			identString := splt[1]

			//vv("addr='%v'; identString =  '%v'", addr, identString)
			const prefix string = "pubkey@edwardsRPC-ed25519-b58c-"
			if !strings.HasPrefix(identString, prefix) {
				return nil, fmt.Errorf("badly formatted known pubkey file '%v' at "+
					"line %v: identity string did not start with 'pubkey@edwardsRPC-ed25519-b58c-'; "+
					"line = '%v'", path, i, l)
			}
			rest := identString[len(prefix):]
			//vv("rest = '%v'", rest)

			splt2 := strings.Split(rest, ":")
			if len(splt2) < 2 {
				return nil, fmt.Errorf("badly formatted known pubkey file '%v'"+
					" at line %v: identity string did not end with a colon ':'; line = '%v'", path, i, l)
			}
			pubkey := splt2[0]
			emails := splt2[1]
			kh := &KnownKey{Addr: addr, KeyType: "ed25519", PubKey: pubkey, Emails: emails, Line: i}

			kn.Hosts = append(kn.Hosts, kh)
			kn.PubKeyMap[pubkey] = kh
		}
	}
	return kn, nil
}

// server will want stripPort true since client's port will change all the time.
// tofu true means we add any unknown cert to our knownKeysPath.
// We don't really care what the IP or hostname is, as
// long as we recognized a certified public key (in one of the identities), we accept.
// The IP or port could change, we don't care.
//
// NB only ed25519 keys are permitted, any others will result
// in an immediate error and no further keys will be evaluated.
func HostKeyVerifies(
	knownKeysPath string,
	connState *tls.ConnectionState,
	remoteAddr string) (good, bad []string, wasNew bool, err0 error) {

	defer func() {
		if len(good) > 0 {
			// file must have been writable,
			// and we added previously unknown identities.
			err0 = nil
		}
	}()

	// If the file is read-only, then we do no Trust-On-First-Use (tofu).
	// But if the file does not exist, then we do tofu.
	tofu := true
	haveKnown := false
	var known *Known
	var err error

	// we use the existance of the known keys file to decide
	// whether to use it or not. So touch the file to start using it.
	recordKeys := fileExists(knownKeysPath)
	if recordKeys {
		if !isWritable(knownKeysPath) {
			tofu = false // no writing/modification of the read-only file.
		}

		known, err = readKnownKeys(knownKeysPath)
		haveKnown = true // default, adjust below
		if err != nil && err == ErrNotFound {
			err = nil
			haveKnown = false
			if !tofu {
				panic(fmt.Sprintf("Empty host key file '%v' AND it is not writable!"+
					" We will never accept any connections.", knownKeysPath))
			}
		}
		if err != nil {
			return nil, nil, false, err
		}
	}

	// Retrieve the server's certificate chain
	if len(connState.PeerCertificates) == 0 {
		panic("No certificates found")
	}
	ncert := len(connState.PeerCertificates)
	_ = ncert
	//vv("connectionState.ServerName = '%v'", state.ServerName) // localhost, on both cli and srv.
	//vv("server has %v certs", ncert)

	for _, serverCert := range connState.PeerCertificates {

		// what type of key?
		// Extract the public key from the certificate
		pubKeyIface := serverCert.PublicKey

		//vv("serverCert = '%#v'", serverCert)
		// Identify the type of public key
		keyType := ""
		var ed25519pubkey []byte
		switch key := pubKeyIface.(type) {
		case ed25519.PublicKey:
			//fmt.Println("Public Key Type: Ed25519")
			keyType = "ed25519" // 32 bytes
			ed25519pubkey = key
			//vv("key = '%#v' (len %v) : '%x'", key, len(key), key)
		default:
			vv("Unknown Public Key Type '%T'", pubKeyIface)
			return nil, nil, false, fmt.Errorf("unknown public key-type, not ed25519 but: '%T'", pubKeyIface)
		}

		if len(ed25519pubkey) == 0 {
			return nil, nil, false, fmt.Errorf("empty public key bytes??? ed25519 but no key present.")
		}

		if !recordKeys {
			// this flags false means: we don't want to create or read known_{client,server}_keys
			continue
		}

		//pubkey := fmt.Sprintf("%x", ed25519pubkey)
		pubkey := toBase58Check(ed25519pubkey)
		// sanity check:
		insanity := fromBase58Check(pubkey)
		if !bytes.Equal(insanity, ed25519pubkey) {
			panic(fmt.Sprintf("could not decode base64 pubkey '%v' back into ed25519pubkey '%x'. Instead got '%x'", pubkey, ed25519pubkey, insanity))
		}
		host, port, err := net.SplitHostPort(remoteAddr)
		_ = port
		if err != nil {
			panic(fmt.Sprintf("Error parsing '%v' into host and port: '%v'\n", remoteAddr, err))
		}
		// clients will change port all the time, maybe servers too. Just save host.

		emails := ""
		if len(serverCert.EmailAddresses) > 0 {
			emails = strings.Join(serverCert.EmailAddresses, ",")
		}
		kh := &KnownKey{Addr: host, KeyType: keyType, PubKey: pubkey, Emails: emails}

		combo := kh.IdentityString()

		if !haveKnown {
			// INVAR: tofu is true. See above panic.
			// first time
			known = NewKnown(knownKeysPath)
			known.Add(kh)
			good = append(good, combo)
			known.WriteOut()
			haveKnown = true
			wasNew = true
			continue
		}
		// INVAR: haveKnown is true, we have at least one entry in the file.

		_, pubFound := known.PubKeyMap[pubkey]

		if pubFound {
			// ignoring IP, IP:port, or hostname, and just for this public key.
			// If the pubkey is present under any address, this is
			// a recognized identity, and we just accept.

			good = append(good, combo)
			wasNew = false
			err0 = nil // accept immediately. This identity suffices.
			return
		}
		// INVAR: this is a new identity for us.

		if tofu {
			// first time, add to list of known keys
			known.Add(kh)
			good = append(good, combo)
			known.WriteOut()
			wasNew = true
			err0 = nil
		} else {
			bad = append(bad, combo)
			// do we want to keep searching through other identities?
			// We have to, since the client may always be offering everything it has.
			err0 = ErrPubKeyUnknown
		}
	}

	return
}

// Compute the SHA-256 fingerprint of a certificate
func computeFingerprint(cert *x509.Certificate) string {
	// Compute the SHA-256 hash of the certificate's raw bytes
	//hash := sha256.Sum256(cert.Raw)
	hash := sha512.Sum512_224(cert.Raw)
	return hex.EncodeToString(hash[:])
}

// we always use 255, which is -1 in 8-bit 2's compliment.
const versionByteBase59Checked byte = 255

func toBase58Check(by []byte) string {
	return base58.CheckEncode(by, versionByteBase59Checked)
}
func fromBase58Check(encodedStr string) []byte {
	decoded, version, err := base58.CheckDecode(encodedStr)
	_ = version
	//vv("Version = %v", version) // 255
	panicOn(err) // panic if checksum verification fails.
	return decoded
}

func toBase58(by []byte) string {
	return base58.Encode(by)
}
func fromBase58(encodedStr string) []byte {
	return base58.Decode(encodedStr)
}
