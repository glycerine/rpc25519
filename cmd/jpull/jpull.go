package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	filepath "path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/glycerine/idem"
	"github.com/glycerine/ipaddr"
	rpc "github.com/glycerine/rpc25519"
	//myblake3 "github.com/glycerine/rpc25519/hash"
	rsync "github.com/glycerine/rpc25519/jsync"
	"github.com/glycerine/rpc25519/progress"
)

var _ = progress.TransferStats{}

type JpullConfig struct {
	Port int
}

func (c *JpullConfig) SetFlags(fs *flag.FlagSet) {
	fs.IntVar(&c.Port, "p", 8443, "port on server to connect to")
}

func (c *JpullConfig) FinishConfig(fs *flag.FlagSet) (err error) {
	return
}
func (c *JpullConfig) SetDefaults() {

}

func main() {
	rpc.Exit1IfVersionReq()

	//fmt.Printf("%v", rpc.GetCodeVersion("jpush"))

	//certdir := rpc.GetCertsDir()
	//cadir := rpc.GetPrivateCertificateAuthDir()

	hostIP := ipaddr.GetExternalIP() // e.g. 100.x.x.x
	_ = hostIP

	jcfg := &JpullConfig{}

	fs := flag.NewFlagSet("jpull", flag.ExitOnError)
	jcfg.SetFlags(fs)
	fs.Parse(os.Args[1:])
	jcfg.SetDefaults()
	err := jcfg.FinishConfig(fs)
	panicOn(err)

	args := fs.Args()
	//vv("args = '%#v'", args)
	if len(args) < 2 {
		fmt.Fprintf(os.Stderr, "jpull error: must supply source and target. ex: jpull copy-from-source-here host:to-target-there\n")
		os.Exit(1)
	}

	takerPath := args[0]
	giverPath := args[1]

	splt := strings.Split(giverPath, ":")
	if len(splt) < 2 {
		fmt.Fprintf(os.Stderr, "jpull error: target did not have ':' in it. ex: jpull copy-from-source-here host:to-target-there\n")
		os.Exit(1)
	}
	// use the last : to allow dest with IPV6
	n := len(splt)
	giverPath = splt[n-1]
	//dest := "tcp://" + strings.Join(splt[:n-1], ":")
	dest := strings.Join(splt[:n-1], ":") + fmt.Sprintf(":%v", jcfg.Port)
	vv("dest = '%v'", dest)
	vv("takerPath = '%v'", takerPath)
	vv("giverPath = '%v'", giverPath)

	cfg := rpc.NewConfig()
	cfg.ClientDialToHostPort = dest

	cli, err := rpc.NewClient("jpull", cfg)
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

	var req *rsync.RequestToSyncPath

	cwd, err := os.Getwd()
	panicOn(err)
	dir, err := filepath.Abs(cwd)
	panicOn(err)

	haveTaker := true
	fi, err := os.Stat(takerPath)
	if err != nil {
		haveTaker = false
	}

	// pull new file we don't have at the moment.
	req = &rsync.RequestToSyncPath{
		GiverPath:               giverPath,
		TakerPath:               takerPath,
		WasEmptyStartingFile:    true,
		Done:                    idem.NewIdemCloseChan(),
		ToRemotePeerServiceName: "rsync_server",
		//NB cannot use cfg.ClientDialToHostPort b/c lacks {tcp,udp}://	protocol part
		ToRemoteNetAddr: cli.RemoteAddr(),

		SyncFromHostname:      rpc.Hostname,
		SyncFromHostCID:       rpc.HostCID,
		AbsDir:                dir,
		RemoteTakes:           false,
		HaveExistingTakerPath: haveTaker,
	}
	if haveTaker {
		req.ModTime = fi.ModTime()
		req.FileSize = fi.Size()
		req.FileMode = uint32(fi.Mode())
	}

	reqs := make(chan *rsync.RequestToSyncPath)
	lpb, ctx, canc, err := rsync.RunRsyncService(cfg, cli, "rsync_client", true, reqs)
	panicOn(err)
	defer lpb.Close()
	defer canc()
	_ = ctx

	vv("jpull about to send on reqs chan")
	reqs <- req
	vv("jpull sent on reqs: requested to rsync to '%v' from %v:%v", takerPath, dest, giverPath)
	<-req.Done.Chan

	if req.Errs != "" {
		alwaysPrintf("req.Err: '%v'", req.Errs)
		os.Exit(1)
	}
	//vv("req (%p) had no req.Errs; empty string ('%v'): '%#v'", req, req.Errs, req)
	vv("all good. elapsed time: %v", time.Since(t0))
	switch {
	case req.SizeModTimeMatch:
		vv("jpull rsync done: good size and mod time match for '%v'", takerPath)
	case req.FullFileInitSideBlake3 == req.FullFileRespondSideBlake3:
		vv("jpull rsync done. Checksums agree for path '%v': %v", takerPath, req.FullFileInitSideBlake3)
		tot := req.BytesRead + req.BytesSent
		_ = tot
		vv("total bytes (read or sent): %v", formatUnder(int(tot)))
		vv("bytes read = %v ; bytes sent = %v (out of %v). (%0.1f%%) ratio: %0.1f speedup", formatUnder(int(req.BytesRead)), formatUnder(int(req.BytesSent)), formatUnder(int(req.FileSize)), float64(tot)/float64(req.FileSize)*100, float64(req.FileSize)/float64(tot))
	default:
		vv("ARG! jpull rsync done but jpull/jsrv Checksums disagree!! for path %v': req = '%#v'", takerPath, req)
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
