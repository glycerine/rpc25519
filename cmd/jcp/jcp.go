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

	// alternative progress reporting, might work outside emacs.
	"github.com/apoorvam/goterminal"
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

	CompressAlgo string
	Dry          bool
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
	fs.BoolVar(&c.Dry, "dry", false, "dry run, do not change anything really")

	fs.BoolVar(&c.WebProfile, "webprofile", false, "start web pprof profiling on localhost:7070")
	fs.StringVar(&c.Memprofile, "memprof", "", "file to write memory profile 30 sec worth to")
	fs.BoolVar(&c.SerialNotParallel, "serial", false, "serial single threaded file chunking, rather than parallel. Mostly for benchmarking")
	fs.StringVar(&c.CompressAlgo, "compress", "s2", "compression algo. other choices: none, s2, lz4, zstd:01, zstd:03, zstd:07, zstd:11")

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
			fmt.Printf("no ':' in src/target: starting local rsync server to receive files...\n")
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
			lazyStartPeer := true
			lpb, ctx, canc, err := rsync.RunRsyncService(cfg, srv, "rsync_server", false, reqs, lazyStartPeer)
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

	if jcfg.CompressAlgo != "" {
		if jcfg.CompressAlgo == "none" {
			jcfg.CompressAlgo = ""
		}
		if !compressionAlgoOK(jcfg.CompressAlgo) {
			msg := fmt.Sprintf("unrecognized magicCompressAlgo: '%v' ; "+
				"valid choices: (default/empty string for none); s2, lz4, zstd:01, zstd:03, zstd:07, zstd:11\n", jcfg.CompressAlgo)
			fmt.Fprintf(os.Stderr, msg)
			os.Exit(1)
		}
		cfg.CompressionOff = (jcfg.CompressAlgo == "")
		cfg.CompressAlgo = jcfg.CompressAlgo
	}

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
		DryRun:    jcfg.Dry,
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
	lazyStartPeer := false
	lpb, ctx, canc, err := rsync.RunRsyncService(cfg, cli, "rsync_client", true, reqs, lazyStartPeer)
	panicOn(err)
	defer lpb.Close()
	defer canc()
	_ = ctx

	//vv("jcp about to send on reqs chan")
	reqs <- req
	//vv("jcp sent on reqs: requested to rsync to '%v' from %v:%v", takerPath, dest, giverPath)
	var part int64

	// This is a terminal escape code to
	// erase the rest of the line, and then do a carriage return.
	// ref: https://www.baeldung.com/linux/echo-printf-overwrite-terminal-line
	eraseAndCR := append([]byte{0x1b}, []byte("[0K\r")...) // "\033[0K\r"
	meters := make(map[string]*progress.TransferStats)
	goTermWriter := goterminal.New(os.Stdout)
	lastUpdate := time.Now()

jobDone:
	for {
		select {
		case prog := <-req.UpdateProgress:
			if jcfg.Quiet {
				continue
			}
			meter, ok := meters[prog.Path]
			if !ok {
				meter = progress.NewTransferStats(prog.Total, filepath.Base(prog.Path))
				meters[prog.Path] = meter
			}
			// can we gc from map, on last report?
			if prog.Latest == meter.FileSize {
				delete(meters, prog.Path)
			}

			if time.Since(lastUpdate) < time.Millisecond*100 {
				continue
			}
			lastUpdate = time.Now()

			part++
			str := meter.ProgressString(prog.Latest, part)

			const useGoTermLib = true
			if useGoTermLib {
				goTermWriter.Clear()
				goTermWriter.Write(append([]byte(str), eraseAndCR...))
				goTermWriter.Print()
			} else {
				os.Stdout.Write(append([]byte(str), eraseAndCR...))
			}

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
		vv("byte transfer bandwidth: %v bytes/sec", formatUnderFloat64(float64(tot)/(float64(time.Since(t0))/float64(1e9))))
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

func compressionAlgoOK(compressAlgo string) bool {
	switch compressAlgo {
	case "", "compression-none":
		// no compression
		return true
	case "s2":
		return true
	case "lz4":
		return true
	case "zstd:01":
		return true
	case "zstd:03":
		return true
	case "zstd:07":
		return true
	case "zstd:11":
		return true
	}
	return false
}
