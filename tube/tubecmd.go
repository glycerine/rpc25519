package tube

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	//"path/filepath"
	//"runtime/debug"
	//"sort"
	//cryrand "crypto/rand"
	"time"
	//"net/http"
	//_ "net/http/pprof" // for web based profiling while running
	//"runtime/pprof"
	//cristalbase64 "github.com/cristalhq/base64"
	//rpc "github.com/glycerine/rpc25519"
	//"github.com/glycerine/ipaddr"
)

type ConfigTubeCmd struct {
	ContactName string // -c name of node to contact
	ClientOnly  bool

	FullConfig bool
	ShowLog    bool

	NonVotingShadowFollower bool   // -shadow add as non-voting-follower ("shadow replica")
	AddRegularFollower      bool   // -addmc add as regular member
	ShowStateArchive        string // -a path

	Help bool // -h for help, false, show this help

	// profiling
	Cpuprofile string `json:"cpuProfile" zid:"37"` // -cpuprofile, write cpu profile to file
	Memprofile string `json:"memProfile" zid:"38"` // -memprofile, write memory profile to this file
	WebProfile bool
	Verbose    bool // -v verbose: show config/connection attempts.
	Zap        bool
}

func (c *ConfigTubeCmd) SetFlags(fs *flag.FlagSet) {
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
	fs.BoolVar(&c.NonVotingShadowFollower, "shadow", false, "add node as non-voting shadow follower replica (conflicts with -add)")
	fs.BoolVar(&c.AddRegularFollower, "add", false, "auto-add this node to regular member config after start up (conflicts with -shadow)")
	fs.BoolVar(&c.Verbose, "v", false, "verbose diagnostics logging to stdout")
}

func (c *ConfigTubeCmd) FinishConfig(fs *flag.FlagSet) (err error) {
	if c.NonVotingShadowFollower &&
		c.AddRegularFollower {
		return fmt.Errorf("cannot have both -shadow and -add at once")
	}
	return
}
func (c *ConfigTubeCmd) SetDefaults() {}

// RunTubeServiceMain is the main routine for the
// primary command line program "tube",
// called from cmd/tube/tubecli.go
func (cfg *TubeConfig) RunTubeServiceMain(cmdCfg *ConfigTubeCmd) {

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
	limit := cfg.RpcCfg.GetLimitMax(string(TUBE_REPLICA))
	if limit == 0 {
		// name not found => no limit.
		cfg.RpcCfg.LimitedServiceNames = append(cfg.RpcCfg.LimitedServiceNames, string(TUBE_REPLICA))
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
		cfg.MinElectionDur = time.Millisecond * 1200
	}
	if cfg.ClockDriftBound <= 0 {
		cfg.ClockDriftBound = time.Millisecond * 500
	}

	cfg.RpcCfg.TCPonly_no_TLS = cfg.TCPonly_no_TLS
	cfg.RpcCfg.ServerAutoCreateClientsToDialOtherServers = true

	if cmdCfg.ClientOnly {
		cfg.ClientProdConfigSaneOrPanic()
	} else {
		cfg.ReplicaProdConfigSaneOrPanic()
	}

	//nodeID := rpc.NewCallID("")

	node := NewTubeNode(cfg.MyName, cfg)

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
		// dump on-disk state (moved to tube/kv.go)
		state.DumpStdoutAnnotatePath(path)

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

	ctx5, canc := context.WithTimeout(ctx, time.Second*50)
	leaderURL, leaderName, _, reallyLeader, contacted, err := node.HelperFindLeader(ctx5, cfg, cmdCfg.ContactName, requireOnlyContact, KEEP_CKT_UP)
	canc()
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
   LastLog:{Term: '%v'; Index: '%v'; LeaderName: '%v'; TicketOp: %v}
   LogIndexBaseC: %v      PID: %v     Hostname: %v
   MC: %v   ShadowReplicas: %v   URL: %v
`, insp.ResponderName, insp.Role, insp.CurrentLeaderName,

			insp.LastLogTerm,
			insp.LastLogIndex,
			insp.LastLogLeaderName,
			insp.LastLogTicketOp,
			insp.LogIndexBaseC,

			insp.PID,
			insp.Hostname,

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
		if cmdCfg.AddRegularFollower || cmdCfg.NonVotingShadowFollower {
			// we want to add ourselves to the cluster

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
	}
	select {
	case <-node.Halt.Done.Chan:
	}
}
