package main

// tubecli.go is a bit of a misnomer. This
// is a peer, and commonly (and by default)
// acts as a replica server in the tube/raft
// cluster (TUBE_REPLICA as the PeerServiceName.
//
// Also at times this code can be only a client.
// Then it will use the TUBE_CLIENT as the PeerServiceName.
//
// See also the -cli (ClientOnly) config flag below.
//
// Either way, most of this code will be runninig
// tube.go routines to do stuff as a peer.

import (
	//"context"
	"flag"
	"fmt"
	"os"
	"strings"
	//"path/filepath"
	"runtime/debug"
	//"sort"
	cryrand "crypto/rand"
	"time"

	"net/http"
	_ "net/http/pprof" // for web based profiling while running
	"runtime/pprof"

	cristalbase64 "github.com/cristalhq/base64"
	//rpc "github.com/glycerine/rpc25519"
	"github.com/glycerine/ipaddr"
	"github.com/glycerine/rpc25519/tube"
)

var sep = string(os.PathSeparator)

func main() {
	showBinaryVersion("tube")
	fmt.Printf("pid = %v\n", os.Getpid())
	fmt.Printf("started at %v\n", nice(time.Now()))

	cmdCfg := &tube.ConfigTubeCmd{}

	fs := flag.NewFlagSet("tube", flag.ExitOnError)
	cmdCfg.SetFlags(fs)
	fs.Parse(os.Args[1:])
	cmdCfg.SetDefaults()
	err := cmdCfg.FinishConfig(fs)
	panicOn(err)

	fmt.Printf("web profiling to 7070 on by default for now.\n")
	cmdCfg.WebProfile = true

	if cmdCfg.Help {
		fmt.Fprintf(os.Stderr, "tube {options} <a tube.cfg path>\n")
		fs.PrintDefaults()
		return
	}
	if cmdCfg.Verbose {
		verboseVerbose = true
		tube.VerboseVerbose.Store(true)
	}

	if cmdCfg.WebProfile {
		alwaysPrintf("tube -webprofile given, about to try and bind 127.0.0.1:7070")
		go func() {
			http.ListenAndServe("127.0.0.1:7070", nil)
			// hmm if we get here we couldn't bind 7070.
			startOnlineWebProfiling()
		}()
	}

	if cmdCfg.Cpuprofile != "" {
		startProfilingCPU(cmdCfg.Cpuprofile)
		defer pprof.StopCPUProfile() // backup plan if we exit early.
	}

	if cmdCfg.Memprofile != "" {
		startProfilingMemory(cmdCfg.Memprofile, 0)
	}

	if cmdCfg.ShowStateArchive != "" {
		os.Exit(showArchive(cmdCfg.ShowStateArchive))
	}

	args := fs.Args()
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "tube error: must supply config file as a non-flag argument\n")
		os.Exit(1)

	}
	pathCfg := args[0]
	by, err := os.ReadFile(pathCfg)
	panicOn(err)

	vv("pathCfg='%v' has:\n%v", pathCfg, string(by))

	cfg, err := tube.NewTubeConfigFromSexpString(string(by), nil)
	panicOn(err)
	if cfg.PeerServiceName == "" {
		// default to being client. Don't over-write
		cfg.PeerServiceName = tube.TUBE_CLIENT
	}
	if cmdCfg.ClientOnly {
		cfg.PeerServiceName = tube.TUBE_CLIENT
	}
	if cmdCfg.Zap {
		cfg.ZapMC = true
	}

	// see tube/tubecmd.go
	cfg.RunTubeServiceMain(cmdCfg)
}

func startOnlineWebProfiling() (port int) {

	// To dump goroutine stack from a running program for debugging:
	// Start an HTTP listener if you do not have one already:
	// Then point a browser to http://127.0.0.1:9999/debug/pprof for a menu, or
	// curl http://127.0.0.1:9999/debug/pprof/goroutine?debug=2
	// for a full dump.
	port = ipaddr.GetAvailPort()
	go func() {
		alwaysPrintf("actually... web profiling at 127.0.0.1:%v now", port)
		err := http.ListenAndServe(fmt.Sprintf("127.0.0.1:%v", port), nil)
		if err != nil {
			panic(err)
		}
	}()
	fmt.Fprintf(os.Stderr, "\n for stack dump:\n\ncurl http://127.0.0.1:%v/debug/pprof/goroutine?debug=2\n\n for general debugging:\n\nhttp://127.0.0.1:%v/debug/pprof\n\n", port, port)
	return
}

func startProfilingMemory(path string, wait time.Duration) {
	// add randomness so two tests run at once don't overwrite each other.
	fn := path + ".memprof." + cryRand15B()
	if wait == 0 {
		wait = time.Minute // default
	}
	alwaysPrintf("will write mem profile to '%v'; after wait of '%v'", fn, wait)
	go func() {
		time.Sleep(wait)
		WriteMemProfiles(fn)
	}()
}

func cryRand15B() string {
	var by [15]byte // 16 and 17 gets = signs. yuck.
	_, err := cryrand.Read(by[:])
	panicOn(err)
	return cristalbase64.URLEncoding.EncodeToString(by[:])
}

func startProfilingCPU(path string) {
	// add randomness so two tests run at once don't overwrite each other.
	fn := path + ".cpuprof." + cryRand15B()
	f, err := os.Create(fn)
	stopOn(err)
	alwaysPrintf("will write cpu profile to '%v'", fn)
	go func() {
		pprof.StartCPUProfile(f)
		time.Sleep(time.Minute)
		pprof.StopCPUProfile()
		alwaysPrintf("stopped and wrote cpu profile to '%v'", fn)
	}()
}

func WriteMemProfiles(fn string) {
	if !strings.HasSuffix(fn, ".") {
		fn += "."
	}
	h, err := os.Create(fn + "heap")
	panicOn(err)
	defer h.Close()
	a, err := os.Create(fn + "allocs")
	panicOn(err)
	defer a.Close()
	g, err := os.Create(fn + "goroutine")
	panicOn(err)
	defer g.Close()

	hp := pprof.Lookup("heap")
	ap := pprof.Lookup("allocs")
	gp := pprof.Lookup("goroutine")

	panicOn(hp.WriteTo(h, 1))
	panicOn(ap.WriteTo(a, 1))
	panicOn(gp.WriteTo(g, 2))
}

func showArchive(path string) (exitCode int) {
	node := &tube.TubeNode{}
	cfg := &tube.TubeConfig{}
	if !fileExists(path) {
		fmt.Printf("error: tube -a show archive path='%v'; path does not exist.\n", path)
		return 1
	}
	//vv("using path = '%v'", path)
	_, state, err := cfg.NewRaftStatePersistor(path, node, true)
	if err != nil {
		fmt.Printf("error: tube -a show archive path='%v'; load error: %v\n", path, err)
		return 1
	}
	if state == nil {
		fmt.Printf("(none) empty RaftState from path '%v'\n", path)
		return 0
	}
	// Gstring prints ALL the session entries (too many); String just one at random.
	fmt.Printf("\nRaftState from path '%v':\n%v\n", path, state.String())
	if state.KVstore != nil {
		fmt.Printf("KVstore: (len %v)\n", state.KVstore.Len())
		for table, tab := range state.KVstore.All() {
			fmt.Printf("    table '%v' (len %v):\n", table, tab.Len())
			for key, leaf := range tab.All() {
				fmt.Printf("       key: '%v': %v\n", key, tube.StringFromVtype(leaf.Value, leaf.Vtype))
			}
		}
	} else {
		fmt.Printf("(nil KVstore)\n")
	}
	return 0
}

func showBinaryVersion(program string) {
	// nb always going to have +dirty
	// in the version unless we bother
	// to get
	// git status --porcelain -unormal
	// to give an empty response.

	info, ok := debug.ReadBuildInfo()
	if !ok {
		fmt.Println("warning: build information not available.")
		return
	}

	//fmt.Printf("tube module path: %v\n", info.Main.Path)
	fmt.Printf("%v version: %v\n", program, info.Main.Version)

	// fmt.Println("---")
	// // You can also iterate through all dependencies.
	// fmt.Println("Dependencies:")
	// for _, dep := range info.Deps {
	// 	fmt.Printf("- %s: %s\n", dep.Path, dep.Version)
	// }
}
