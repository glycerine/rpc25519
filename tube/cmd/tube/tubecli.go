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
	"context"
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

type ConfigTubeCli struct {
	ContactName string // -c name of node to contact
	ClientOnly  bool

	FullConfig bool
	ShowLog    bool

	NonVotingShadowFollower bool   // -shadow add as non-voting-follower ("shadow replica")
	ShowStateArchive        string // -a path

	Help bool // -h for help, false, show this help

	// profiling
	Cpuprofile string `json:"cpuProfile" zid:"37"` // -cpuprofile, write cpu profile to file
	Memprofile string `json:"memProfile" zid:"38"` // -memprofile, write memory profile to this file
	WebProfile bool
	Verbose    bool // -v verbose: show config/connection attempts.
	Zap        bool
}

func (c *ConfigTubeCli) SetFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.ContactName, "c", "", "name of node to contact (defaults to leader)")
	fs.BoolVar(&c.ClientOnly, "cli", false, "act as client only, not replica")
	fs.BoolVar(&c.ShowLog, "log", false, "print  my raft log and exit.")
	fs.StringVar(&c.ShowStateArchive, "a", "", "display this state archive path")
	fs.BoolVar(&c.FullConfig, "full", false, "show full config -- all fields even defaults")
	fs.BoolVar(&c.Help, "h", false, "show this help")
	fs.BoolVar(&c.Zap, "zap", false, "zap the MC and ShadowReplicas on startup, clearing and zero-ing out the current membership configuration set.")

	fs.BoolVar(&c.WebProfile, "webprofile", false, "start web pprof profiling on localhost:7070")

	// profiling
	fs.StringVar(&c.Cpuprofile, "cpuprofile", "", "write cpu profile to file")
	fs.StringVar(&c.Memprofile, "memprofile", "", "write memory profile to this file")
	fs.BoolVar(&c.NonVotingShadowFollower, "shadow", false, "add node as non-voting shadow follower replica")
	fs.BoolVar(&c.Verbose, "v", false, "verbose diagnostics logging to stdout")
}

func (c *ConfigTubeCli) FinishConfig(fs *flag.FlagSet) (err error) {
	return
}
func (c *ConfigTubeCli) SetDefaults() {}

func main() {
	showBinaryVersion("tube")

	cmdCfg := &ConfigTubeCli{}

	fs := flag.NewFlagSet("tube", flag.ExitOnError)
	cmdCfg.SetFlags(fs)
	fs.Parse(os.Args[1:])
	cmdCfg.SetDefaults()
	err := cmdCfg.FinishConfig(fs)
	panicOn(err)

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
		alwaysPrintf("samp -webprofile given, about to try and bind 127.0.0.1:7070")
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

	// from :7000 -> 100.x.y.z:7000 for example.
	cfg.ConvertToExternalAddr()

	var myAddr string
	var ok bool

	myAddr, ok = cfg.Node2Addr[cfg.MyName]
	if !ok {
		panic(fmt.Sprintf("could not find MyName:'%v' in Node2Addr map: '%#v'", cfg.MyName, cfg.Node2Addr))
	}

	cfg.RpcCfg.ServerAddr = myAddr
	vv("will start my server at '%v'", cfg.RpcCfg.ServerAddr)

	// ======== ok
	if !cmdCfg.ShowLog {
		if cmdCfg.FullConfig {
			// long version, all fields
			fmt.Printf("tube starting with cfg '%v'!\n cfg = %v\n", cfg.ConfigName, cfg.SexpString(nil))
		} else {
			// only fields not the default zero values.
			fmt.Printf("tube starting with cfg '%v'!\n cfg = %v\n", cfg.ConfigName, cfg.ShortSexpString(nil))
		}
	}

	// be sure we are limited to just a canonical tube-replica
	// peer service func instance; only one (at a time, per process).
	limit := cfg.RpcCfg.GetLimitMax(string(tube.TUBE_REPLICA))
	if limit == 0 {
		// name not found => no limit.
		cfg.RpcCfg.LimitedServiceNames = append(cfg.RpcCfg.LimitedServiceNames, string(tube.TUBE_REPLICA))
		cfg.RpcCfg.LimitedServiceMax = append(cfg.RpcCfg.LimitedServiceMax, 1)
	}
	// set up our config
	const quiet = false
	const isTest = false
	cfg.Init(quiet, isTest)

	numName := len(cfg.Node2Addr)
	if numName < 1 {
		panic("must have Name2Addr len at least 1 in TubeConfig")
	}

	if cfg.ClusterSize == 0 {
		cfg.ClusterSize = numName
	}

	cfg.UseSimNet = false
	if cfg.HeartbeatDur <= 0 {
		cfg.HeartbeatDur = time.Millisecond * 200
	}
	if cfg.MinElectionDur <= 0 {
		cfg.MinElectionDur = time.Millisecond * 1000
	}
	if cfg.ClockDriftBound <= 0 {
		cfg.ClockDriftBound = time.Millisecond * 20
	}

	cfg.RpcCfg.TCPonly_no_TLS = cfg.TCPonly_no_TLS
	cfg.RpcCfg.ServerAutoCreateClientsToDialOtherServers = true

	if cmdCfg.ClientOnly {
		cfg.ClientProdConfigSaneOrPanic()
	} else {
		cfg.ReplicaProdConfigSaneOrPanic()
	}

	//nodeID := rpc.NewCallID("")

	node := tube.NewTubeNode(cfg.MyName, cfg)

	path := node.GetPersistorPath()
	_, state, err := cfg.NewRaftStatePersistor(path, node, true)
	if err != nil {
		alwaysPrintf("ignoring state restore error, probably none avail: '%v'", err)
		state = nil
	}

	if cmdCfg.ShowLog {
		if err != nil {
			alwaysPrintf("tube -log cannot proceed: no state file available.")
			os.Exit(1)
		}
		// dump on-disk state
		if state == nil {
			fmt.Printf("\n(none) empty RaftState from path '%v'.\n", path)
		} else {
			fmt.Printf("\nRaftState from path '%v':\n%v\n", path, state.Gstring())
			if state.KVstore != nil {
				fmt.Printf("KVstore: (len %v)\n", state.KVstore.Len())
				for table, tab := range state.KVstore.All() {
					fmt.Printf("    table '%v' (len %v):\n", table, tab.Len())
					var extra string
					for key, leaf := range tab.All() {
						if leaf.Leasor == "" {
							extra = ""
						} else {
							extra = fmt.Sprintf("[Leasor:'%v'; until '%v' (in %v)] ", leaf.Leasor, leaf.LeaseUntilTm.Format(rfc3339NanoNumericTZ0pad), leaf.LeaseUntilTm.Sub(time.Now()))
						}
						fmt.Printf("       key: '%v' (version %v): %v%v\n", key, leaf.Version, extra, stringFromVtype(leaf.Value, leaf.Vtype))
					}
				}
			} else {
				fmt.Printf("(nil KVstore)\n")
			}
		}
		// dump log
		fmt.Printf("\ncfg.InitialLeaderName = '%v'; cfg.MyName = '%v'\n", cfg.InitialLeaderName, cfg.MyName)
		node.SetState(state)
		err = node.DumpRaftWAL()
		panicOn(err)

		os.Exit(0)
	}

	ctx := node.Ctx // peer's Ctx

	vv("starting node.name = '%v'; cfg.MyName = '%v'", node.Name(), cfg.MyName)
	err = node.InitAndStart()
	panicOn(err)
	defer node.Close()

	const requireOnlyContact = false
	leaderURL, leaderName, _, reallyLeader, contacted, err := node.HelperFindLeader(cfg, cmdCfg.ContactName, requireOnlyContact)
	_ = reallyLeader // leaderName will be empty so maybe not needed?
	if err != nil {
		// this is fine... expected under example/remote
		// "error: no leaders found and no cfg.InitialLeaderName; use -c to contact a specific node"
		if strings.Contains(err.Error(), "no leaders found") {
			// ignore it.
			err = nil
		} else {
			panic(err)
		}
	}

	fmt.Printf("contacted:\n")
	for _, insp := range contacted {
		fmt.Printf(`%v %v  (lead: '%v')
   MC: %v   ShadowReplicas: %v   URL: %v
`, insp.ResponderName, insp.Role, insp.CurrentLeaderName,
			insp.MC,
			insp.ShadowReplicas,
			insp.ResponderPeerURL)
	}

	switch {
	case leaderName == cfg.MyName && reallyLeader:
		vv("%v wow: we are local peer and leader. don't make a socket/circuit to talk to ourselves... reallyLeader='%v'", cfg.MyName, reallyLeader)
	case leaderName == "" || !reallyLeader:
		vv("%v: empty leaderName('%v') OR !reallyLeader(%v), just let Start() loop run", cfg.MyName, leaderName, reallyLeader)

	default:
		vv("%v non-empty leaderName='%v' reallyLeader='%v'", cfg.MyName, leaderName, reallyLeader)

		baseServerHostPort := node.BaseServerHostPort()
		errWriteDur := time.Second * 10
	retryLoop:
		for retry := 0; retry < 2; retry++ {
			// note that this can result in dupliate
			// entries in the log for this operation if
			// we have to timeout and try again. That is
			// fine. We don't use the session logic since
			// this is for replicas not clients.
			// See cmd/tup/tup.go for client and session
			// examples.
			actualLeaderURL := leaderURL

			const forceAdd = false
			ctx5sec, canc5 := context.WithTimeout(ctx, 5*time.Second)
			memlistAfterAdd, stateSnapshot, err := node.AddPeerIDToCluster(ctx5sec, forceAdd, cmdCfg.NonVotingShadowFollower, cfg.MyName, node.PeerID, node.PeerServiceName, baseServerHostPort, actualLeaderURL, errWriteDur)
			canc5()
			// can have network unavail at first. Yes freak since otherwise we won't be up!
			//panicOn(err)
			if err == nil {
				pp("good: no error on AddPeerIDToCluster('%v'); shadow/nonVoting='%v'; contacting leader '%v'", cfg.MyName, cmdCfg.NonVotingShadowFollower, actualLeaderURL)
				if memlistAfterAdd.CurrentLeaderName != cfg.MyName {
					if stateSnapshot != nil {
						select {
						case node.ApplyNewStateSnapshotCh <- stateSnapshot:
							vv("%v tubecli sent node.ApplyNewStateSnapshotCh <- stateSnapshot", cfg.MyName)
						case <-node.Halt.Done.Chan:
							return
						}
					}
				}
				_ = memlistAfterAdd
				vv("%v: memlistAfterAdd = '%v'", cfg.MyName, memlistAfterAdd)
				break retryLoop
				//break tryNextOne
			} else {
				alwaysPrintf("initial add myself to cluster problem: '%v' ... wait 2 sec and try again", err) // 'error timeout' much better than 'connect: connection refused'; or JobErrs: 'no local peerServiceName 'tube-replica' available'.
				time.Sleep(time.Second * 2)
				//continue tryNextOne
				continue retryLoop
			}
		}
	}
	select {
	case <-node.Halt.Done.Chan:
	}
}

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
	fmt.Printf("\nRaftState from path '%v':\n%v\n", path, state.Gstring())
	if state.KVstore != nil {
		fmt.Printf("KVstore: (len %v)\n", state.KVstore.Len())
		for table, tab := range state.KVstore.All() {
			fmt.Printf("    table '%v' (len %v):\n", table, tab.Len())
			for key, leaf := range tab.All() {
				fmt.Printf("       key: '%v': %v\n", key, stringFromVtype(leaf.Value, leaf.Vtype))
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

func stringFromVtype(val tube.Val, vtyp string) string {
	switch vtyp {
	case tube.ReliableMembershipListType:
		rm := &tube.ReliableMembershipList{}
		rm.UnmarshalMsg(val)
		return rm.String()
	}
	return string(val)
}
