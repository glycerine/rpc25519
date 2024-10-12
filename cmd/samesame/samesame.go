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

	var local = flag.String("c", "127.0.0.1:8445", "client address to send echo request from.")
	var dest = flag.String("s", "127.0.0.1:8443", "server address to send echo request to.")
	var reverse = flag.Bool("r", false, "swap the local and dest addresses to make a complementary client and server")

	var skipVerify = flag.Bool("skip-verify", false, "skip verify-ing that server certs are in-use and authorized by our CA; only possible with TLS.")

	var cliName = flag.String("k", "client", "specifies name of keypairs to use for client (certs/name.crt and certs/name.key); instead of the default certs/client.crt and certs/client.key")

	var serverName = flag.String("ks", "node", "specifies name of keypairs to use for server (certs/name.crt and certs/name.key); instead of the default certs/node.crt and certs/node.key for the server.")

	var certPath = flag.String("certs", "", "use this path on the lived filesystem for certs; instead of the embedded certs/ from build-time.")

	flag.Parse()

	cfg := rpc25519.NewConfig()

	cfg.ServerAddr = *local
	cfg.ClientHostPort = *local
	cfg.ClientDialToHostPort = *dest

	cfg.TCPonly_no_TLS = false
	cfg.UseQUIC = true
	cfg.SkipVerifyKeys = *skipVerify
	cfg.ClientKeyPairName = *cliName
	cfg.ServerKeyPairName = *serverName
	cfg.CertPath = *certPath

	if *reverse {
		cfg.ServerAddr = *dest
		cfg.ClientHostPort = *dest
		cfg.ClientDialToHostPort = *local

		cfg.ClientKeyPairName = *serverName
		cfg.ServerKeyPairName = *cliName
	}

	srv := rpc25519.NewServer(cfg.ServerKeyPairName, cfg)
	defer srv.Close()

	srv.RegisterFunc(customEcho)

	serverAddr, err := srv.Start()
	if err != nil {
		panic(fmt.Sprintf("could not start rpc25519.Server with config = '%#v'; err='%v'", cfg, err))
	}

	log.Printf("rpc25519.server Start() returned serverAddr = '%v'", serverAddr)

	for {

		cli, err := rpc25519.NewClient(cfg.ClientKeyPairName, cfg)
		if err != nil {
			log.Printf("client '%v' could not connect: '%v'. Wait 2 sec and try again...\n", cfg.ClientKeyPairName, err)
			time.Sleep(time.Second * 2)
			continue
		}
		//defer cli.Close()

		req := rpc25519.NewMessage()
		req.JobSerz = []byte("client says hello and requests this be echoed back with a timestamp!")

		reply, err := cli.SendAndGetReply(req, nil)
		if err != nil {
			panic(err)
		}

		log.Printf("client sees reply (Seqno=%v) = '%v'\n", reply.Seqno, string(reply.JobSerz))
		break
	}
	select {}
}

// echo implements rpc25519.CallbackFunc
func customEcho(in *rpc25519.Message) (out *rpc25519.Message) {
	log.Printf("server customEcho called, Seqno=%v, msg='%v'", in.Seqno, string(in.JobSerz))
	//vv("callback to echo: with msg='%#v'", in)
	in.JobSerz = append(in.JobSerz, []byte(fmt.Sprintf("\n with time customEcho sees this: '%v'", time.Now()))...)
	return in // echo
}
