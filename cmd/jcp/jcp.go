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

type JcopyConfig struct {
	Port int
}

func (c *JcopyConfig) SetFlags(fs *flag.FlagSet) {
	fs.IntVar(&c.Port, "p", 8443, "port on server to connect to")
}

func (c *JcopyConfig) FinishConfig(fs *flag.FlagSet) (err error) {
	return
}
func (c *JcopyConfig) SetDefaults() {

}

func main() {
	rpc.Exit1IfVersionReq()

	//fmt.Printf("%v", rpc.GetCodeVersion("jcp"))

	//certdir := rpc.GetCertsDir()
	//cadir := rpc.GetPrivateCertificateAuthDir()

	hostIP := ipaddr.GetExternalIP() // e.g. 100.x.x.x
	_ = hostIP

	jcfg := &JcopyConfig{}

	fs := flag.NewFlagSet("jcp", flag.ExitOnError)
	jcfg.SetFlags(fs)
	fs.Parse(os.Args[1:])
	jcfg.SetDefaults()
	err := jcfg.FinishConfig(fs)
	panicOn(err)

	args := fs.Args()
	//vv("args = '%#v'", args)
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "jcp error: must supply at least a source ex: jcp host:source-file-path {destination path optional}\n")
		os.Exit(1)
	}

	// jcp rog:giverPath      => pull from rog; derive takerPath from Base(giverPath)
	// jcp rog:giverPath takerPath  => pull from rog
	// jcp giverPath rog:takerPath => push to rog

	// so push/pull depends on where the ':' is in the command line.
	// but giver is always first. takerPath is always second.
	// These are cp semantics (also rsync, scp, ...)

	giverPath := args[0]
	takerPath := ""
	if len(args) > 1 {
		takerPath = args[1]
	}

	var dest string
	isPush := false
	giverIsDir := false
	takerIsDir := false
	takerExistsLocal := false
	giverExistsLocal := false

	// extract remote; the server to contact.
	splt := strings.Split(giverPath, ":")
	if len(splt) <= 1 {
		isPush = true
		// no ':' in giver, so this is the scenario
		// jcp giverPath rog:takerPath => push to rog
		// jcp giverPath rog:  => infer takerPath from Base(giverPath)

		if !fileExists(giverPath) {
			if dirExists(giverPath) {
				giverIsDir = true
			} else {
				fmt.Fprintf(os.Stderr, "jcp error: source path not found: '%v'\n", giverPath)
				os.Exit(1)
			}
		} else {
			giverExistsLocal = true
		}
		if takerPath == "" {
			fmt.Fprintf(os.Stderr, "jcp error: destination path not given\n")
			os.Exit(1)
		}

		splt2 := strings.Split(takerPath, ":")
		if len(splt2) <= 1 {
			fmt.Fprintf(os.Stderr, "jcp error: neither source nor destination had ':' in it. Which is the remote?\n")
			os.Exit(1)
		}
		n := len(splt2)
		dest = strings.Join(splt2[:n-1], ":") + fmt.Sprintf(":%v", jcfg.Port)
		takerPath = splt2[n-1]
		if takerPath == "" {
			takerPath = filepath.Base(giverPath)
		} else {
			if dirExists(takerPath) {
				takerExistsLocal = true
				takerIsDir = true
			} else if fileExists(takerPath) {
				takerExistsLocal = true
			} else {
				takerExistsLocal = false
			}
		}
	} else {
		// jcp rog:giverPath      => pull from rog; use giverPath for takerPath
		// jcp rog:giverPath takerPath  => pull from rog

		n := len(splt)
		dest = strings.Join(splt[:n-1], ":") + fmt.Sprintf(":%v", jcfg.Port)
		giverPath = splt[n-1]
		// (use the last : to allow dest with IPV6)

		if takerPath == "" {
			takerPath = giverPath
		}
	}
	if dirExists(takerPath) {
		takerExistsLocal = true
		takerIsDir = true
	} else if fileExists(takerPath) {
		takerExistsLocal = true
	} else {
		takerExistsLocal = false
	}

	vv("dest = '%v'", dest)
	vv("takerPath = '%v' exists=%v; isDir=%v", takerPath, takerExistsLocal, takerIsDir)
	vv("giverPath = '%v' exists=%v; isDir=%v", giverPath, giverExistsLocal, giverIsDir)

	var giverStartsEmpty bool
	var takerStartsEmpty bool

	if giverExistsLocal && !giverIsDir {
		sz, err := fileSize(giverPath)
		panicOn(err)
		if sz == 0 {
			giverStartsEmpty = true
		}
	}
	if takerExistsLocal && !takerIsDir {
		sz, err := fileSize(takerPath)
		panicOn(err)
		if sz == 0 {
			takerStartsEmpty = true
		}
	}

	cfg := rpc.NewConfig()
	cfg.ClientDialToHostPort = dest

	cli, err := rpc.NewClient("jcp", cfg)
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

	// haveTaker := true
	// var fi os.FileInfo
	// if takerPath == "" {
	// 	haveTaker = false
	// } else {
	// 	fi, err = os.Stat(takerPath)
	// 	if err != nil {
	// 		haveTaker = false
	// 	}
	// }

	// pull new file we don't have at the moment.
	req = &rsync.RequestToSyncPath{
		GiverPath: giverPath,
		TakerPath: takerPath,

		GiverIsDir:       giverIsDir,
		TakerIsDir:       takerIsDir,
		TakerExistsLocal: takerExistsLocal,
		GiverExistsLocal: giverExistsLocal,

		GiverStartsEmpty: giverStartsEmpty,
		TakerStartsEmpty: takerStartsEmpty,
		//WasEmptyStartingFile:    !takerExistsLocal,
		//HaveExistingTakerPath: takerExistsLocal,

		Done:                    idem.NewIdemCloseChan(),
		ToRemotePeerServiceName: "rsync_server",
		//NB cannot use cfg.ClientDialToHostPort b/c lacks {tcp,udp}://	protocol part
		ToRemoteNetAddr: cli.RemoteAddr(),

		SyncFromHostname: rpc.Hostname,
		SyncFromHostCID:  rpc.HostCID,
		AbsDir:           dir,
		RemoteTakes:      isPush,
	}
	if takerExistsLocal {

		var fi os.FileInfo
		fi, err = os.Stat(takerPath)
		panicOn(err)

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

	vv("jcp about to send on reqs chan")
	reqs <- req
	vv("jcp sent on reqs: requested to rsync to '%v' from %v:%v", takerPath, dest, giverPath)
	<-req.Done.Chan

	if req.Errs != "" {
		alwaysPrintf("req.Err: '%v'", req.Errs)
		os.Exit(1)
	}
	//vv("req (%p) had no req.Errs; empty string ('%v'): '%#v'", req, req.Errs, req)
	vv("all good. elapsed time: %v", time.Since(t0))
	switch {
	case req.SizeModTimeMatch:
		vv("jcp rsync done: good size and mod time match for '%v'", takerPath)
	case req.FullFileInitSideBlake3 == req.FullFileRespondSideBlake3:
		vv("jcp rsync done. Checksums agree for path '%v': %v", takerPath, req.FullFileInitSideBlake3)
		tot := req.BytesRead + req.BytesSent
		_ = tot
		vv("total bytes (read or sent): %v", formatUnder(int(tot)))
		vv("bytes read = %v ; bytes sent = %v (out of %v). (%0.1f%%) ratio: %0.1f speedup", formatUnder(int(req.BytesRead)), formatUnder(int(req.BytesSent)), formatUnder(int(req.FileSize)), float64(tot)/float64(req.FileSize)*100, float64(req.FileSize)/float64(tot))
	default:
		vv("ARG! jcp rsync done but jcp Checksums disagree!! for path %v': req = '%#v'", takerPath, req)
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
