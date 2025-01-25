package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	filepath "path/filepath"
	"strconv"
	"time"

	"github.com/glycerine/idem"
	"github.com/glycerine/ipaddr"
	rpc "github.com/glycerine/rpc25519"
	//myblake3 "github.com/glycerine/rpc25519/hash"
	rsync "github.com/glycerine/rpc25519/jsync"
	"github.com/glycerine/rpc25519/progress"
)

var _ = progress.TransferStats{}

func main() {
	rpc.Exit1IfVersionReq()

	fmt.Printf("%v", rpc.GetCodeVersion("jpull"))

	log.SetFlags(log.LstdFlags | log.Lshortfile) // Add Lshortfile for short file names

	//certdir := rpc.GetCertsDir()
	//cadir := rpc.GetPrivateCertificateAuthDir()

	//var profile = flag.String("prof", "", "host:port to start web profiler on. host can be empty for all localhost interfaces")

	hostIP := ipaddr.GetExternalIP() // e.g. 100.x.x.x
	_ = hostIP

	flag.Parse()

	cfg := rpc.NewConfig()
	//cfg.ClientDialToHostPort = *dest // "127.0.0.1:8443",

	cli, err := rpc.NewClient("jpush", cfg)
	if err != nil {
		log.Printf("bad client config: '%v'\n", err)
		os.Exit(1)
	}
	err = cli.Start()
	if err != nil {
		log.Printf("client could not connect: '%v'\n", err)
		os.Exit(1)
	}
	defer cli.Close()
	//vv("client connected from local addr='%v'", cli.LocalAddr())
	fmt.Println()

	t0 := time.Now()
	_ = t0
	path := *rsyncPath

	giverPath := path
	takerPath := path

	var req *rsync.RequestToSyncPath

	cwd, err := os.Getwd()
	panicOn(err)
	dir, err := filepath.Abs(cwd)
	panicOn(err)

	// pull new file we don't have at the moment.
	req = &rsync.RequestToSyncPath{
		GiverPath:               giverPath,
		TakerPath:               takerPath,
		WasEmptyStartingFile:    true,
		Done:                    idem.NewIdemCloseChan(),
		ToRemotePeerServiceName: "rsync_server",
		//NB cannot use cfg.ClientDialToHostPort b/c lacks {tcp,udp}://	protocol part
		ToRemoteNetAddr: cli.RemoteAddr(),

		SyncFromHostname: rpc.Hostname,
		SyncFromHostCID:  rpc.HostCID,
		AbsDir:           dir,
		//RemoteTakes:      false, // !(*pullRsync),
	}

	reqs := make(chan *rsync.RequestToSyncPath)
	lpb, ctx, canc, err := rsync.RunRsyncService(cfg, cli, "rsync_client", true, reqs)
	panicOn(err)
	defer lpb.Close()
	defer canc()
	_ = ctx

	//vv("jpull about to send on reqs chan")
	reqs <- req
	//vv("jpull sent on reqs: requested to rsync path '%v'", path)
	<-req.Done.Chan

	if req.Errs != "" {
		alwaysPrintf("req.Err: '%v'", req.Errs)
		os.Exit(1)
	}
	//vv("req (%p) had no req.Errs; empty string ('%v'): '%#v'", req, req.Errs, req)
	vv("all good. elapsed time: %v", time.Since(t0))
	switch {
	case req.SizeModTimeMatch:
		vv("jpull rsync done: good size and mod time match for '%v'", path)
	case req.FullFileInitSideBlake3 == req.FullFileRespondSideBlake3:
		vv("jpull rsync done. Checksums agree for path '%v': %v", path, req.FullFileInitSideBlake3)
		tot := req.BytesRead + req.BytesSent
		_ = tot
		vv("total bytes (read or sent): %v", formatUnder(int(tot)))
		vv("bytes read = %v ; bytes sent = %v (out of %v). (%0.1f%%) ratio: %0.1f speedup", formatUnder(int(req.BytesRead)), formatUnder(int(req.BytesSent)), formatUnder(int(req.FileSize)), float64(tot)/float64(req.FileSize)*100, float64(req.FileSize)/float64(tot))
	default:
		vv("ARG! jpull rsync done but jpull/jsrv Checksums disagree!! for path %v': req = '%#v'", path, req)
	}
	return
}

func formatUnder(n int) string {
	// Convert to string first
	str := strconv.FormatInt(int64(n), 10)

	// Handle numbers less than 1000
	if len(str) <= 3 {
		return str
	}

	// Work from right to left, adding underscores
	var result []byte
	for i := len(str) - 1; i >= 0; i-- {
		if (len(str)-1-i)%3 == 0 && i != len(str)-1 {
			result = append([]byte{'_'}, result...)
		}
		result = append([]byte{str[i]}, result...)
	}

	return string(result)
}
