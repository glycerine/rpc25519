package main

// tubeadd adds the target replica named on
// the command line to the cluster membership.
//
// At the moment it will use tup's config to
// find the cluster.

import (
	"context"
	"flag"
	"fmt"
	//"net/url"
	"os"
	//"strings"
	//"path/filepath"
	//"sort"
	"time"

	"github.com/glycerine/ipaddr"
	rpc "github.com/glycerine/rpc25519"
	"github.com/glycerine/rpc25519/tube"
)

var sep = string(os.PathSeparator)

type set struct {
	nodes []string
}

type TubeRemoveConfig struct {
	ContactName string // -c name of node to contact
	Help        bool   // -h for help, false, show this help
	//ForceName   string // -f force node to remove MC change
	NonVotingShadowFollower bool // -shadow add as non-voting-follower ("shadow replica")
	Verbose                 bool // -v verbose: show config/connection attempts.
}

func (c *TubeRemoveConfig) SetFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.ContactName, "c", "", "name of node to contact (defaults to leader)")
	fs.BoolVar(&c.Help, "h", false, "show this help")
	fs.BoolVar(&c.NonVotingShadowFollower, "shadow", false, "add node as non-voting shadow follower replica")
	//fs.StringVar(&c.ForceName, "f", "", "name of node to force add MC change")
	fs.BoolVar(&c.Verbose, "v", false, "verbose diagnostics logging to stdout")
}

func (c *TubeRemoveConfig) FinishConfig(fs *flag.FlagSet) (err error) {
	return
}
func (c *TubeRemoveConfig) SetDefaults() {}

func main() {
	cmdCfg := &TubeRemoveConfig{}

	fs := flag.NewFlagSet("tubeadd", flag.ExitOnError)
	cmdCfg.SetFlags(fs)
	fs.Parse(os.Args[1:])
	cmdCfg.SetDefaults()
	err := cmdCfg.FinishConfig(fs)
	panicOn(err)

	if cmdCfg.Help {
		fmt.Fprintf(os.Stderr, "tubeadd help:\n")
		fs.PrintDefaults()
		return
	}
	if cmdCfg.Verbose {
		verboseVerbose = true
	}

	var target string
	args := fs.Args()
	if len(args) > 0 {
		target = args[0]
	}

	// first connect, then run repl
	dir := tube.GetConfigDir()
	pathCfg := dir + "/" + "tup.default.config"
	if fileExists(pathCfg) {
		//vv("using config file: '%v'", pathCfg)
	} else {
		fd, err := os.Create(pathCfg)
		panicOn(err)
		cfg := &tube.TubeConfig{
			MyName:          "tubeadd",
			PeerServiceName: tube.TUBE_CLIENT,
		}
		fmt.Fprintf(fd, "%v\n", cfg.SexpString(nil))
		fd.Close()
		fmt.Fprintf(os.Stderr, "tubeadd error: no config file. Created one from template in '%v'. Please complete it.\n", pathCfg)
		os.Exit(1)
	}
	by, err := os.ReadFile(pathCfg)
	panicOn(err)
	//vv("got by = '%v'", string(by))

	cfg, err := tube.NewTubeConfigFromSexpString(string(by), nil)
	panicOn(err)
	cfg.MyName = "tubeadd_" + tube.CryRand15B()
	cfg.PeerServiceName = tube.TUBE_CLIENT

	myHost := ipaddr.GetExternalIP()
	myPort := ipaddr.GetAvailPort()
	cfg.RpcCfg.ServerAddr = fmt.Sprintf("%v:%v", myHost, myPort)
	pp("will start server at '%v'", cfg.RpcCfg.ServerAddr)

	// set up our config
	const quiet = false
	const isTest = false
	cfg.Init(quiet, isTest)

	cfg.UseSimNet = false

	cfg.RpcCfg.TCPonly_no_TLS = cfg.TCPonly_no_TLS
	cfg.RpcCfg.ServerAutoCreateClientsToDialOtherServers = true
	cfg.RpcCfg.QuietTestMode = true

	cfg.ClientProdConfigSaneOrPanic()

	pp("cfg = '%v'", cfg.ShortSexpString(nil))

	//nodeID := rpc.NewCallID("")
	name := cfg.MyName
	node := tube.NewTubeNode(name, cfg)
	//defer node.Close()
	ctx := context.Background()

	var leaderURL string
	greet := cmdCfg.ContactName
	if greet == "" {
		greet = cfg.InitialLeaderName
	}
	addr, ok := cfg.Node2Addr[greet]
	if !ok {
		fmt.Fprintf(os.Stderr, "error: giving up, as no address! gotta have cfg.InitialLeaderName or -c name of node to contact (we use the names listed in the config file '%v' under Node2Addr).\n", pathCfg)
		os.Exit(1)
	} else {
		leaderURL = tube.FixAddrPrefix(addr)
		if cmdCfg.ContactName == "" {
			//vv("by default we contact cfg.InitialLeaderName='%v'; addr='%v' -> leaderURL = '%v'", cfg.InitialLeaderName, addr, leaderURL)
		} else {
			//vv("requested cmdCfg.ContactName='%v' maps to addr='%v' -> URL = '%v'", cmdCfg.ContactName, addr, leaderURL)
		}
	}

	//var cli *rpc.Client
	//cli, err := node.StartClientOnly(ctx, addr)
	err = node.InitAndStart()
	panicOn(err)
	defer node.Close()

	leaderURL, leaderName := findLeader(cfg, cmdCfg, node)
	pp("tubeadd is doing AddPeerIDToCluster using leaderName = '%v'; leaderURL='%v'", leaderName, leaderURL)

	targetPeerID := "" // empty string allowed now
	errWriteDur := time.Second * 20
	peerServiceName := tube.TUBE_REPLICA
	baseServerHostPort := ""
	memlistAfter, stateSnapshot, err := node.AddPeerIDToCluster(ctx, cmdCfg.NonVotingShadowFollower, target, targetPeerID, peerServiceName, baseServerHostPort, leaderURL, errWriteDur)
	panicOn(err)

	pp("good: no error on AddPeerIDToCluster('%v'); shadow/nonVoting='%v'; contacting leader '%v'", target, cmdCfg.NonVotingShadowFollower, leaderURL)

	if target == cfg.MyName {
		// we could apply the state snapshot like peercli does,
		// but usually we are just a client and not the peer
		// itself, and we don't really want to compete with/
		// overwrite the state if the user/admin is not expecting it.x
		if stateSnapshot != nil {
			// select {
			// case node.ApplyNewStateSnapshotCh <- stateSnapshot:
			// 	vv("%v we sent node.ApplyNewStateSnapshotCh <- stateSnapshot", cfg.MyName)
			// case <-node.Halt.Done.Chan:
			// 	return
			// }
		}
	}

	if memlistAfter == nil ||
		memlistAfter.MC == nil ||
		memlistAfter.MC.PeerNames == nil ||
		memlistAfter.MC.PeerNames.Len() == 0 {

		fmt.Printf("empty or nil membership from '%v'\n", leaderName)
	} else {
		fmt.Printf("membership after adding '%v': (%v leader)\n", target, leaderName)
		for name, det := range memlistAfter.MC.PeerNames.All() {
			fmt.Printf("  %v:   %v\n", name, det.URL)
		}
	}
	if memlistAfter.ShadowReplicas == nil ||
		memlistAfter.ShadowReplicas.PeerNames == nil ||
		memlistAfter.ShadowReplicas.PeerNames.Len() == 0 {

	} else {
		fmt.Printf("\nshadow replicas:\n")
		for name, det := range memlistAfter.ShadowReplicas.PeerNames.All() {
			url, ok := memlistAfter.CktAllByName[name]
			if !ok {
				url = det.Addr
			}
			fmt.Printf("  %v:   %v\n", name, tube.URLTrimCktID(url))
		}
	}
}

func findLeader(cfg *tube.TubeConfig, cmdCfg *TubeRemoveConfig, node *tube.TubeNode) (lastLeaderURL, lastLeaderName string) {
	// contact everyone, get their idea of who is leader
	leaders := make(map[string]*set)

	ctx := context.Background()

	var insps []*tube.Inspection
	for name, addr := range cfg.Node2Addr {
		url := tube.FixAddrPrefix(addr)
		_, insp, leaderURL, leaderName, _, err := node.GetPeerListFrom(ctx, url)
		//mc, insp, leaderURL, leaderName, _, err := node.GetPeerListFrom(ctx, url)

		if err != nil {
			//pp("skip '%v' b/c err = '%v'", leaderName, err)
			continue
		}
		if leaderName != "" {
			pp("candidate leader = '%v', url = '%v", leaderName, leaderURL)
			insps = append(insps, insp)
			lastLeaderName = leaderName
			lastLeaderURL = leaderURL
			s := leaders[leaderName]
			if s == nil {
				leaders[leaderName] = &set{nodes: []string{name}}
			} else {
				s.nodes = append(s.nodes, name)
			}
		}
	}
	// put together a transition set of known/connected nodes...
	xtra := make(map[string]string)
	for _, ins := range insps {
		for name, url := range ins.CktAll {
			_, skip := cfg.Node2Addr[name]
			if skip {
				// already contacted
				continue
			}
			surl, ok := xtra[name]
			if ok {
				if surl == "pending" {
					xtra[name] = url
				}
			} else {
				// avoid adding other clients/ourselves
				_, serviceName, _, _, err1 := rpc.ParsePeerURL(url)
				if err1 == nil && serviceName == tube.TUBE_REPLICA {
					xtra[name] = url
				}
			}
		}
	}

	for name, url := range xtra {
		if url == "pending" {
			continue
		}
		//url = tube.FixAddrPrefix(url)
		_, _, leaderURL, leaderName, _, err := node.GetPeerListFrom(ctx, url)
		//mc, insp, leaderURL, leaderName, _, err := node.GetPeerListFrom(ctx, url)
		if err != nil {
			continue
		}
		if leaderName != "" {
			lastLeaderName = leaderName
			lastLeaderURL = leaderURL
			pp("extra candidate leader = '%v', url = '%v", leaderName, leaderURL)
			s := leaders[leaderName]
			if s == nil {
				leaders[leaderName] = &set{nodes: []string{name}}
			} else {
				s.nodes = append(s.nodes, name)
			}
		}
	}

	if len(leaders) > 1 {
		if cmdCfg.ContactName == "" {
			fmt.Printf("ugh. we see multiple leaders in our nodes\n")
			fmt.Printf("     --not sure which one to talk to...\n")
			for lead, s := range leaders {
				for _, n := range s.nodes {
					fmt.Printf("  '%v' sees leader '%v'\n", n, lead)
				}
			}
			os.Exit(1)
		}
	}
	if len(leaders) == 1 {
		if cmdCfg.ContactName == "" {
			if cfg.InitialLeaderName != "" &&
				cfg.InitialLeaderName != lastLeaderName {

				fmt.Printf("warning: ignoring default '%v' "+
					"because we see leader '%v'\n",
					cfg.InitialLeaderName, lastLeaderName)
			}
		} else {
			fmt.Printf("abort: we see existing leader '%v'; conflicts with request -c %v\n", lastLeaderName, cmdCfg.ContactName)
			os.Exit(1)
		}
	} else {
		// INVAR: len(leaders) == 0
		if cmdCfg.ContactName == "" {
			if cfg.InitialLeaderName == "" {
				fmt.Printf("no leaders found and no cfg.InitialLeaderName; use -c to contact a specific node.\n")
				os.Exit(1)
			} else {
				pp("based on cfg.InitialLeaderName we will try to contact '%v'", cfg.InitialLeaderName)
				lastLeaderName = cfg.InitialLeaderName
				addr := cfg.Node2Addr[lastLeaderName]
				lastLeaderURL = tube.FixAddrPrefix(addr)
			}
		} else {
			lastLeaderName = cmdCfg.ContactName
			pp("based on -c we will try to contact '%v'", cmdCfg.ContactName)
			addr := cfg.Node2Addr[lastLeaderName]
			lastLeaderURL = tube.FixAddrPrefix(addr)
		}
	}
	return
}
