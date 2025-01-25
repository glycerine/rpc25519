package main

import (
	"flag"
	"fmt"
	"os"
	filepath "path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/glycerine/idem"
	"github.com/glycerine/ipaddr"
	rpc "github.com/glycerine/rpc25519"
	rsync "github.com/glycerine/rpc25519/jsync"
	"github.com/glycerine/rpc25519/progress"
)

var _ = progress.TransferStats{}

type JpushConfig struct {
	Src    string `json:"src" zid:"0"`
	Target string `json:"target" zid:"1"`
}

func (c *JpushConfig) SetFlags(fs *flag.FlagSet) {

}

func (c *JpushConfig) FinishConfig(fs *flag.FlagSet) (err error) {
	return
}
func (c *JpushConfig) SetDefaults() {

}

func main() {
	rpc.Exit1IfVersionReq()

	fmt.Printf("%v", rpc.GetCodeVersion("jpush"))

	//certdir := rpc.GetCertsDir()
	//cadir := rpc.GetPrivateCertificateAuthDir()

	hostIP := ipaddr.GetExternalIP() // e.g. 100.x.x.x
	_ = hostIP

	jcfg := &JpushConfig{}

	fs := flag.NewFlagSet("jpush", flag.ExitOnError)
	jcfg.SetFlags(fs)
	fs.Parse(os.Args[1:])
	jcfg.SetDefaults()
	err := jcfg.FinishConfig(fs)
	panicOn(err)

	args := fs.Args()
	vv("args = '%#v'", args)
	if len(args) < 2 {
		fmt.Fprintf(os.Stderr, "jpush error: must supply source and target. ex: jpush copy-from-source-here host:to-target-there\n")
		os.Exit(1)
	}

	giverPath := args[0]
	takerPath := args[1]
	splt := strings.Split(takerPath, ":")
	if len(splt) < 2 {
		fmt.Fprintf(os.Stderr, "jpush error: target did not have ':' in it. ex: jpush copy-from-source-here host:to-target-there\n")
		os.Exit(1)
	}
	// use the last : to allow dest like "127.0.0.1:8443:path" or IPV6
	n := len(splt)
	takerPath = splt[n-1]
	//dest := "tcp://" + strings.Join(splt[:n-1], ":")
	dest := strings.Join(splt[:n-1], ":")
	vv("dest = '%v'", dest)

	cfg := rpc.NewConfig()
	cfg.ClientDialToHostPort = dest

	cli, err := rpc.NewClient("jpush", cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "jpush error: bad client config: '%v'\n", err)
		os.Exit(1)
	}
	err = cli.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "jpush error: client could not connect: '%v'\n", err)
		os.Exit(1)
	}
	defer cli.Close()
	//vv("client connected from local addr='%v'", cli.LocalAddr())
	fmt.Println()

	t0 := time.Now()
	_ = t0

	var req *rsync.RequestToSyncPath

	if fileExists(giverPath) {
		fi, err := os.Stat(giverPath)
		if err != nil {
			panic(fmt.Sprintf("cli error on -rsync: no such path '%v': '%v'", giverPath, err))
		}
		if fi.IsDir() {
			panic(fmt.Sprintf("rsync of directories not yet supported: '%v'", giverPath))
		}
		fmt.Printf("rsync_client to send path: '%v'\n", giverPath)

		// lazily, we don't scan a file until we know
		// we have a mod time or size diff.

		absPath, err := filepath.Abs(giverPath)
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

			RemoteTakes: true, // !isPull, // remote takes on push, not pull.
		}
		//vv("req using ToRemoteNetAddr: '%v'. push (remote takes) = %v", req.ToRemoteNetAddr, req.RemoteTakes)
	} else {
		// no such path at the moment

		alwaysPrintf("cli error on -rsync: no such path '%v'. (did you want to -pull it from remote?)", giverPath)
		os.Exit(1)

		// no such path but go on...
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
			RemoteTakes:      true, // !(*pullRsync),
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
		vv("cli rsync done: good size and mod time match for '%v'", giverPath)
	case req.FullFileInitSideBlake3 == req.FullFileRespondSideBlake3:
		vv("cli rsync done. Checksums agree for path '%v': %v", giverPath, req.FullFileInitSideBlake3)
		tot := req.BytesRead + req.BytesSent
		_ = tot
		vv("total bytes (read or sent): %v", formatUnder(int(tot)))
		vv("bytes read = %v ; bytes sent = %v (out of %v). (%0.1f%%) ratio: %0.1f speedup", formatUnder(int(req.BytesRead)), formatUnder(int(req.BytesSent)), formatUnder(int(req.FileSize)), float64(tot)/float64(req.FileSize)*100, float64(req.FileSize)/float64(tot))
	default:
		vv("ARG! cli rsync done but cli/srv Checksums disagree!! for path %v': req = '%#v'", giverPath, req)
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
