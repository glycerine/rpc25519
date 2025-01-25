package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	filepath "path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	tdigest "github.com/caio/go-tdigest"
	"github.com/glycerine/idem"
	"github.com/glycerine/ipaddr"
	"github.com/glycerine/loquet"
	rpc "github.com/glycerine/rpc25519"
	myblake3 "github.com/glycerine/rpc25519/hash"
	rsync "github.com/glycerine/rpc25519/jsync"
	"github.com/glycerine/rpc25519/progress"
	_ "net/http/pprof" // for web based profiling while running
)

var td *tdigest.TDigest

func main() {
	rpc.Exit1IfVersionReq()

	fmt.Printf("%v", rpc.GetCodeVersion("jpush"))

	log.SetFlags(log.LstdFlags | log.Lshortfile) // Add Lshortfile for short file names

	certdir := rpc.GetCertsDir()
	//cadir := rpc.GetPrivateCertificateAuthDir()

	var profile = flag.String("prof", "", "host:port to start web profiler on. host can be empty for all localhost interfaces")

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
	isPull := *pullRsync

	if fileExists(path) {
		fi, err := os.Stat(path)
		if err != nil {
			panic(fmt.Sprintf("cli error on -rsync: no such path '%v': '%v'", path, err))
		}
		if fi.IsDir() {
			panic(fmt.Sprintf("rsync of directories not yet supported: '%v'", path))
		}
		fmt.Printf("rsync_client to send path: '%v'\n", path)

		var precis *rsync.FilePrecis
		var chunks *rsync.Chunks
		if false { // isPull
			// ugh. this can be super expensive compared to
			// just sending the size and timestamp at first!
			// be lazier!
			const keepData = false
			const wantChunks = true
			precis, chunks, err = rsync.GetHashesOneByOne(rpc.Hostname, path)
		}
		// lazily, we don't scan a file until we know
		// we have a mod time or size diff.

		absPath, err := filepath.Abs(path)
		panicOn(err)
		dir := filepath.Dir(absPath)

		req = &rsync.RequestToSyncPath{
			GiverPath:               giverPath,
			TakerPath:               takerPath,
			FileSize:                fi.Size(),
			ModTime:                 fi.ModTime(),
			FileMode:                uint32(fi.Mode()),
			Done:                    idem.NewIdemCloseChan(),
			ToRemotePeerServiceName: "rsync_server",

			//NB cannot use cfg.ClientDialToHostPort b/c lacks {tcp,udp}://	protocol part
			ToRemoteNetAddr: cli.RemoteAddr(),

			SyncFromHostname: rpc.Hostname,
			SyncFromHostCID:  rpc.HostCID,
			AbsDir:           dir,

			RemoteTakes: !isPull, // remote takes on push, not pull.

			// (pull): for taking an update locally. Tell remote what we have now.
			Precis: precis,
			Chunks: chunks,
		}
		//vv("req using ToRemoteNetAddr: '%v'. push (remote takes) = %v", req.ToRemoteNetAddr, req.RemoteTakes)
	} else {
		// no such path at the moment

		if !*pullRsync {
			alwaysPrintf("cli error on -rsync: no such path '%v'. (did you want to -pull it from remote?)", path)
			os.Exit(1)
		}

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
			RemoteTakes:      !(*pullRsync),
		}
	}
	reqs := make(chan *rsync.RequestToSyncPath)
	lpb, ctx, canc, err := rsync.RunRsyncService(cfg, cli, "rsync_client", true, reqs)
	panicOn(err)
	defer lpb.Close()
	defer canc()
	_ = ctx

	//vv("cli about to send on reqs chan")
	reqs <- req
	//vv("cli sent on reqs: requested to rsync path '%v'", path)
	<-req.Done.Chan

	if req.Errs != "" {
		alwaysPrintf("req.Err: '%v'", req.Errs)
		os.Exit(1)
	}
	//vv("req (%p) had no req.Errs; empty string ('%v'): '%#v'", req, req.Errs, req)
	vv("all good. elapsed time: %v", time.Since(t0))
	switch {
	case req.SizeModTimeMatch:
		vv("cli rsync done: good size and mod time match for '%v'", path)
	case req.FullFileInitSideBlake3 == req.FullFileRespondSideBlake3:
		vv("cli rsync done. Checksums agree for path '%v': %v", path, req.FullFileInitSideBlake3)
		tot := req.BytesRead + req.BytesSent
		_ = tot
		vv("total bytes (read or sent): %v", formatUnder(int(tot)))
		vv("bytes read = %v ; bytes sent = %v (out of %v). (%0.1f%%) ratio: %0.1f speedup", formatUnder(int(req.BytesRead)), formatUnder(int(req.BytesSent)), formatUnder(int(req.FileSize)), float64(tot)/float64(req.FileSize)*100, float64(req.FileSize)/float64(tot))
	default:
		vv("ARG! cli rsync done but cli/srv Checksums disagree!! for path %v': req = '%#v'", path, req)
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
