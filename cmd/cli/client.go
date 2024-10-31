package main

import (
	//"fmt"
	"flag"
	"log"
	"os"

	"github.com/glycerine/rpc25519"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile) // Add Lshortfile for short file names

	var dest = flag.String("s", "127.0.0.1:8443", "server address to send echo request to.")
	var remoteDefault = flag.Bool("r", false, "ping the default test remote at 192.168.254.151")
	var tcp = flag.Bool("tcp", false, "use TCP instead of the default TLS")
	var skipVerify = flag.Bool("skip-verify", false, "skip verify-ing that server certs are in-use and authorized by our CA; only possible with TLS.")
	var useName = flag.String("k", "", "specifies name of keypairs to use (certs/name.crt and certs/name.key); instead of the default certs/client.crt and certs/client.key")
	var certPath = flag.String("certs", "", "use this path on the lived filesystem for certs; instead of the embedded certs/ from build-time.")

	var quic = flag.Bool("q", false, "use QUIC instead of TCP/TLS")
	var hang = flag.Bool("hang", false, "hang at the end, to see if keep-alives are working.")
	var psk = flag.String("psk", "", "path to pre-shared key file")

	flag.Parse()

	if *remoteDefault {
		*dest = "192.168.254.151:8443"
	}

	cfg := rpc25519.NewConfig()
	cfg.ClientDialToHostPort = *dest // "127.0.0.1:8443",
	cfg.TCPonly_no_TLS = *tcp
	cfg.UseQUIC = *quic
	cfg.SkipVerifyKeys = *skipVerify
	cfg.ClientKeyPairName = *useName
	cfg.CertPath = *certPath
	cfg.PreSharedKeyPath = *psk

	cli, err := rpc25519.NewClient("cli", cfg)
	if err != nil {
		log.Printf("client could not connect: '%v'\n", err)
		os.Exit(1)
	}
	defer cli.Close()
	log.Printf("client connected from local addr='%v'\n", cli.LocalAddr())

	req := rpc25519.NewMessage()
	req.JobSerz = []byte("client says hello and requests this be echoed back with a timestamp!")

	reply, err := cli.SendAndGetReply(req, nil)
	if err != nil {
		panic(err)
	}

	log.Printf("client sees reply (Seqno=%v) = '%v'\n", reply.HDR.Seqno, string(reply.JobSerz))

	if *hang {
		log.Printf("client hanging to see if keep-alives happen...")
		select {}
	}
}
