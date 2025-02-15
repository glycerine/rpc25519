package main

import (
	"flag"
	"fmt"
	"iter"
	"log"
	"net/http"
	"os"
	filepath "path/filepath"
	"strconv"
	"strings"
	"time"

	_ "net/http/pprof" // for web based profiling while running
	"runtime/pprof"

	"github.com/glycerine/idem"
	"github.com/glycerine/ipaddr"
	rpc "github.com/glycerine/rpc25519"
	//myblake3 "github.com/glycerine/rpc25519/hash"
	rsync "github.com/glycerine/rpc25519/jsync"
	"github.com/glycerine/rpc25519/progress"
)

var _ = progress.TransferStats{}

var sep = string(os.PathSeparator)

type JcopyConfig struct {
	Port    int
	Quiet   bool
	Walk    bool
	Verbose bool

	WebProfile bool
	Memprofile string

	SerialNotParallel bool
}

// backup plan if :7070 was not available...
func startOnlineWebProfiling() (port int) {

	// To dump goroutine stack from a running program for debugging:
	// Start an HTTP listener if you do not have one already:
	// Then point a browser to http://127.0.0.1:9999/debug/pprof for a menu, or
	// curl http://127.0.0.1:9999/debug/pprof/goroutine?debug=2
	// for a full dump.
	port = ipaddr.GetAvailPort()
	go func() {
		err := http.ListenAndServe(fmt.Sprintf("127.0.0.1:%v", port), nil)
		if err != nil {
			panic(err)
		}
	}()
	fmt.Fprintf(os.Stderr, "\n for stack dump:\n\ncurl http://127.0.0.1:%v/debug/pprof/goroutine?debug=2\n\n for general debugging:\n\nhttp://127.0.0.1:%v/debug/pprof\n\n", port, port)
	return
}

func (c *JcopyConfig) SetFlags(fs *flag.FlagSet) {
	fs.IntVar(&c.Port, "p", 8443, "port on server to connect to")
	fs.BoolVar(&c.Quiet, "q", false, "quiet, no progress report")
	fs.BoolVar(&c.Walk, "w", false, "walk dir, to test walk.go")
	fs.BoolVar(&c.Verbose, "v", false, "verbosely walk dir, showing paths")

	fs.BoolVar(&c.WebProfile, "webprofile", false, "start web pprof profiling on localhost:7070")
	fs.StringVar(&c.Memprofile, "memprof", "", "file to write memory profile 30 sec worth to")
	fs.BoolVar(&c.SerialNotParallel, "serial", false, "serial single threaded file chunking, rather than parallel. Mostly for benchmarking")
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

	if jcfg.WebProfile {
		fmt.Printf("jcp -webprofile given, about to try and bind 127.0.0.1:7070\n")
		go func() {
			http.ListenAndServe("127.0.0.1:7070", nil)
			// hmm if we get here we couldn't bind 7070.
			startOnlineWebProfiling()
		}()
	}

	if jcfg.Walk {
		t0 := time.Now()
		nFile, modTime := jcfg.walktest()
		elap := time.Since(t0)
		vv("most recent modTime in %v files = '%v'; elap = '%v'", nFile, modTime, elap)
		return
	}

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
	takerIsLocal := true
	isPush := false
	giverIsDir := false
	takerIsDir := false
	takerExistsLocal := false
	giverExistsLocal := false
	cfg := rpc.NewConfig()

	serverOn := false
	_ = serverOn

	// extract remote; the server to contact.
	splt := strings.Split(giverPath, ":")
	if len(splt) <= 1 {
		isPush = true
		takerIsLocal = false
		// no ':' in giver, so this is the scenario
		// jcp giverPath rog:takerPath => push to rog
		// jcp giverPath rog:  => infer takerPath from giverPath

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
			//fmt.Fprintf(os.Stderr, "jcp error: neither source nor destination had ':' in it. Which is the remote?\n")
			//os.Exit(1)
			//fmt.Printf("no ':' in src/target: starting local rsync server to receive files...\n")
			cfg.ServerAddr = "127.0.0.1:0"
			srv := rpc.NewServer("srv_rsync_jcp", cfg)
			serverAddr, err := srv.Start()
			panicOn(err)
			defer srv.Close()
			serverOn = true

			cfg.ClientDialToHostPort = serverAddr.String()
			dest = cfg.ClientDialToHostPort

			reqs := make(chan *rsync.RequestToSyncPath)
			//fmt.Printf("starting rsync_server\n")
			lpb, ctx, canc, err := rsync.RunRsyncService(cfg, srv, "rsync_server", false, reqs)
			panicOn(err)
			defer lpb.Close()
			defer canc()
			_ = ctx

		} else {
			n := len(splt2)
			dest = strings.Join(splt2[:n-1], ":") + fmt.Sprintf(":%v", jcfg.Port)
			takerPath = splt2[n-1]
			if takerPath == "" {
				takerPath = giverPath
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
	} else {
		takerExistsLocal = fileExists(takerPath)
	}

	if takerIsLocal && !takerExistsLocal {
		if strings.HasSuffix(takerPath, sep) {
			// jcp rog:binarydiff ~/trash/tmp/deeper/does_not_exists/
			takerIsDir = true
		}
	}

	/*
			if takerIsDir && takerIsLocal && !giverIsDir {
				// jcp rog:binarydiff ~/trash/tmp/deeper/
				vv("using giverPath '%v': changing taker '%v' -> '%v'", giverPath, takerPath, takerPath+giverPath)
				takerPath = takerPath + giverPath
				takerIsDir = false
				takerExistsLocal = fileExists(takerPath)
			}

		if takerIsLocal && takerIsDir && takerExistsLocal {
			panic(fmt.Sprintf("problem: takerPath cannot be an existing dir: '%v'", takerPath))
		}
	*/
	//vv("dest = '%v'", dest)
	//vv("takerPath = '%v' exists=%v; isDir=%v", takerPath, takerExistsLocal, takerIsDir)
	//vv("giverPath = '%v' exists=%v; isDir=%v", giverPath, giverExistsLocal, giverIsDir)

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

	cfg.ClientDialToHostPort = dest
	cfg.CompressionOff = true

	if jcfg.Memprofile != "" {
		f, err := os.Create(jcfg.Memprofile)
		panicOn(err)
		wait := time.Second * 30
		go func() {
			time.Sleep(wait)
			pprof.WriteHeapProfile(f)
			f.Close()
		}()
	}

	rsync.SetParallelChunking(!jcfg.SerialNotParallel)

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
		GiverDirAbs:      dir,
		RemoteTakes:      isPush,

		UpdateProgress: make(chan *rsync.ProgressUpdate, 100),
	}
	if takerExistsLocal {

		var fi os.FileInfo
		fi, err = os.Stat(takerPath)
		panicOn(err)

		req.TakerModTime = fi.ModTime()
		req.TakerFileSize = fi.Size()
		req.TakerFileMode = uint32(fi.Mode())

	} else if isPush {

		var fi os.FileInfo
		fi, err = os.Stat(giverPath)
		panicOn(err)

		req.GiverModTime = fi.ModTime()
		req.GiverFileSize = fi.Size()
		req.GiverFileMode = uint32(fi.Mode())
	}

	reqs := make(chan *rsync.RequestToSyncPath)
	lpb, ctx, canc, err := rsync.RunRsyncService(cfg, cli, "rsync_client", true, reqs)
	panicOn(err)
	defer lpb.Close()
	defer canc()
	_ = ctx

	//vv("jcp about to send on reqs chan")
	reqs <- req
	//vv("jcp sent on reqs: requested to rsync to '%v' from %v:%v", takerPath, dest, giverPath)
	var curFile string
	var curTransfer *progress.TransferStats
	var part int64
	hadReport := false
jobDone:
	for {
		select {
		case prog := <-req.UpdateProgress:

			if !jcfg.Quiet {
				if prog.Path != curFile {
					if hadReport {
						fmt.Println()
					}
					curFile = prog.Path
					part = 0
					curTransfer = progress.NewTransferStats(prog.Total, prog.Path)
				}
				part++
				str := curTransfer.ProgressString(prog.Latest, part)
				if str != "" {
					fmt.Println(str) // debug! why truncation?
					fmt.Printf("prog = '%#v'\n", prog)
					//fmt.Print(str) // avoid having % interpretted.
					hadReport = true
				}
			}
			continue
		case <-req.Done.Chan:
			break jobDone
		}
	}

	if req.Errs != "" {
		if req.Errs == rsync.ErrNeedDirTaker.Error() {
			alwaysPrintf("jcp error: local taker has file '%v', where giver has directory of the same name. We must delete the file first if we want to do this; '%v'", req.TakerPath, req.Errs)
		} else {
			alwaysPrintf("req.Err: '%v'", req.Errs)
		}
		os.Exit(1)
	}
	//vv("req (%p) had no req.Errs; empty string ('%v'): '%#v'", req, req.Errs, req)
	//vv("all good. elapsed time: %v", time.Since(t0))
	switch {
	case req.SizeModTimeMatch:
		//vv("jcp rsync done: good size and mod time match for '%v'", takerPath)
	case req.GiverFullFileBlake3 == req.TakerFullFileBlake3:
		//vv("jcp rsync done. Checksums agree for path '%v': %v", takerPath, req.GiverFullFileBlake3)

		// put the biger of {read bytes,sent bytes} in tot.
		tot := req.BytesRead
		if req.BytesSent > tot {
			tot = req.BytesSent
		}
		//vv("total bytes (read or sent): %v", formatUnder(int(tot)))
		vv("giver total file sizes: %v", formatUnder(int(req.GiverFileSize)))
		vv("bytes read = %v ; bytes sent = %v (out of %v). (%0.1f%%) ratio: %s speedup", formatUnder(int(req.BytesRead)), formatUnder(int(req.BytesSent)), formatUnder(int(req.GiverFileSize)), float64(tot)/float64(req.GiverFileSize)*100, formatUnderFloat64(float64(req.GiverFileSize)/float64(tot)))
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

func formatUnderFloat64(f float64) string {

	n := int(f)
	s := formatUnder(n)

	var decimal string
	dec := f - float64(n)
	if dec > 0 {
		decimal = fmt.Sprintf("%.1f", dec)
		decimal = decimal[1:] // trim off the leading 0 in 0.1
	}
	return s + decimal
}

func (jcfg *JcopyConfig) walktest() (
	nFile int,
	mostRecentFileModTime time.Time,

) {
	di := rsync.NewDirIter()
	root := "."
	nextF, stopF := iter.Pull2(di.FilesOnly(root))
	defer stopF()

	for {
		regfile, ok, valid := nextF()
		if !valid {
			//vv("not valid, breaking, ok = %v", ok)
			break
		}
		if !ok {
			break
		}
		nFile++
		if jcfg.Verbose {
			fmt.Printf("%v\n", regfile.Path)
		}
		if regfile.ModTime.After(mostRecentFileModTime) {
			mostRecentFileModTime = regfile.ModTime
		}
	}
	stopF()

	return
}
