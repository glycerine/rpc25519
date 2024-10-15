package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/glycerine/rpc25519"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile) // Add Lshortfile for short file names

	var addr = flag.String("s", "0.0.0.0:8443", "server address to bind and listen on")
	var tcp = flag.Bool("tcp", false, "use TCP instead of the default TLS")
	var skipVerify = flag.Bool("skip-verify", false, "do not require client certs be from our CA, nor remember client certs in a known_client_keys file for later lockdown")

	var useName = flag.String("k", "node", "specifies name of keypairs to use (certs/name.crt and certs/name.key); instead of the default certs/node.crt and certs/node.key for the server.")
	var certPath = flag.String("certs", "", "use this path on the lived filesystem for certs; instead of the embedded certs/ from build-time.")
	// too complicated, just use skipverify
	//var skipClientCerts = flag.Bool("skip-client-certs", false, "skip verify-ing that client certs are in-use and authorized by our CA; only possible with TLS.")

	var quic = flag.Bool("q", false, "use QUIC instead of TCP/TLS")

	flag.Parse()

	cfg := rpc25519.NewConfig()
	cfg.ServerAddr = *addr // "0.0.0.0:8443"
	cfg.TCPonly_no_TLS = *tcp
	cfg.UseQUIC = *quic
	cfg.SkipVerifyKeys = *skipVerify
	cfg.ServerKeyPairName = *useName
	cfg.CertPath = *certPath
	//cfg.SkipClientCerts= *skipClientCerts

	srv := rpc25519.NewServer("srv", cfg)
	defer srv.Close()

	srv.Register2Func(customEcho)

	serverAddr, err := srv.Start()
	if err != nil {
		panic(fmt.Sprintf("could not start rpc25519.Server with config = '%#v'; err='%v'", cfg, err))
	}

	log.Printf("rpc25519.server Start() returned serverAddr = '%v'", serverAddr)
	select {}
}

// echo implements rpc25519.CallbackFunc
func customEcho(req, reply *rpc25519.Message) error {
	log.Printf("server customEcho called, Seqno=%v, msg='%v'", req.HDR.Seqno, string(req.JobSerz))
	//vv("callback to echo: with msg='%#v'", in)
	reply.JobSerz = append(req.JobSerz, []byte(fmt.Sprintf("\n with time customEcho sees this: '%v'", time.Now()))...)
	return nil
}
