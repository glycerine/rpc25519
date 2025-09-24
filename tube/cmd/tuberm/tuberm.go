package main

// tuberm removes the target replica named on
// the command line from the cluster membership.
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

type TubeRemoveConfig struct {
	ContactName             string // -c name of node to contact
	Help                    bool   // -h for help, false, show this help
	ForceName               string // -f node to forcefully remove from MC
	WipeName                string // -e wipe to empty MC
	NonVotingShadowFollower bool   // -shadow remove shadow replica
	Verbose                 bool   // -v verbose: show config/connection attempts.
}

func (c *TubeRemoveConfig) SetFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.ContactName, "c", "", "name of node to contact (defaults to leader)")
	fs.BoolVar(&c.Help, "h", false, "show this help")
	fs.StringVar(&c.ForceName, "f", "", "forcefully remove node from MC")
	fs.StringVar(&c.WipeName, "e", "", "name of node to force install empty MC")
	fs.BoolVar(&c.NonVotingShadowFollower, "shadow", false, "remove this non-voting shadow follower replica")
	fs.BoolVar(&c.Verbose, "v", false, "verbose diagnostics logging to stdout")
}

func (c *TubeRemoveConfig) FinishConfig(fs *flag.FlagSet) (err error) {
	return
}
func (c *TubeRemoveConfig) SetDefaults() {}

func main() {
	cmdCfg := &TubeRemoveConfig{}

	fs := flag.NewFlagSet("tuberm", flag.ExitOnError)
	cmdCfg.SetFlags(fs)
	fs.Parse(os.Args[1:])
	cmdCfg.SetDefaults()
	err := cmdCfg.FinishConfig(fs)
	panicOn(err)

	if cmdCfg.Help {
		fmt.Fprintf(os.Stderr, "tuberm help:\n")
		fs.PrintDefaults()
		return
	}
	if cmdCfg.Verbose {
		verboseVerbose = true
		tube.VerboseVerbose.Store(true)
	}

	var target string
	args := fs.Args()
	if len(args) > 0 {
		target = args[0]
	}
	var force bool
	if cmdCfg.ForceName != "" {
		force = true
		target = cmdCfg.ForceName
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
			MyName:          "tuberm",
			PeerServiceName: tube.TUBE_CLIENT,
		}
		fmt.Fprintf(fd, "%v\n", cfg.SexpString(nil))
		fd.Close()
		fmt.Fprintf(os.Stderr, "tuberm error: no config file. Created one from template in '%v'. Please complete it.\n", pathCfg)
		os.Exit(1)
	}
	by, err := os.ReadFile(pathCfg)
	panicOn(err)
	//vv("got by = '%v'", string(by))

	cfg, err := tube.NewTubeConfigFromSexpString(string(by), nil)
	panicOn(err)
	cfg.MyName = "tuberm_" + tube.CryRand15B()
	cfg.PeerServiceName = tube.TUBE_CLIENT

	cfg.ConvertToExternalAddr()

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

	//vv("cfg = '%v'", cfg.ShortSexpString(nil))

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
	if cmdCfg.WipeName != "" {
		greet = cmdCfg.WipeName
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

	err = node.InitAndStart()
	panicOn(err)
	defer node.Close()

	leaderURL, leaderName, insp, reallyLeader, contacted, err := node.HelperFindLeader(cfg, cmdCfg.ContactName, true)
	_ = reallyLeader
	panicOn(err)

	if true {
		fmt.Printf("tubeadd contacted:\n")
		for _, insp2 := range contacted {
			fmt.Printf(`%v %v  (lead: '%v')
   MC: %v   ShadowReplicas: %v   URL: %v
`, insp2.ResponderName, insp2.Role, insp2.CurrentLeaderName,
				insp2.MC,
				insp2.ShadowReplicas,
				insp2.ResponderPeerURL)
		}
	}

	pp("tuberm is doing RemovePeerIDFromCluster using leaderName = '%v'; leaderURL='%v'", leaderName, leaderURL)

	if cmdCfg.WipeName != "" {
		target := cmdCfg.WipeName
		vv("target = '%v'", target)
		var url string
		ok = false
		if insp != nil {
			url, ok = insp.CktAllByName[target]
			vv("ok=%v, url = '%v'", ok, url)
		} else {
			vv("no insp available")
		}
		if !ok {
			addr2, ok2 := cfg.Node2Addr[target]
			if ok2 && addr2 != "" {
				url = tube.FixAddrPrefix(addr2)
			} else {
				url = leaderURL // less likely to work.
			}
		}
		vv("for target '%v', using url = '%v'", target, url)
		err = node.InjectEmptyMC(ctx, url, target)
		if err != nil {
			alwaysPrintf("error when installing empty MC on '%v': %v", target, url, err)
			os.Exit(1)
		}
		alwaysPrintf("installed empty MC on '%v'/'%v'.", target, url)
		os.Exit(0)
	}

	if insp == nil {
		// no leader at the moment.
		if len(contacted) == 0 {
			fmt.Printf("could not contact any nodes.\n")
			os.Exit(1)
		}
		insp = contacted[0]
		fmt.Printf("no leader found, using MC from '%v': %v\n", insp.ResponderName, insp.MC.Short())
	}
	newestMembership := insp.MC
	var det *tube.PeerDetail

	if cmdCfg.NonVotingShadowFollower {
		det, ok = insp.ShadowReplicas.PeerNames.Get2(target)
		_ = det
		if !ok {
			fmt.Printf("error: target not currently a shadow replica. target='%v'\n", target)
			fmt.Printf("existing shadows: (%v leader)\n", leaderName)
			for name, det := range insp.ShadowReplicas.PeerNames.All() {
				fmt.Printf("  %v:   %v\n", name, det.URL)
			}
			os.Exit(1)
		}
	} else {
		det, ok = newestMembership.PeerNames.Get2(target)
		_ = det
		if !ok {
			fmt.Printf("error: target not in current membership. target='%v'\n", target)
			fmt.Printf("existing membership: (%v leader)\n", leaderName)
			for name, det := range newestMembership.PeerNames.All() {
				fmt.Printf("  %v:   %v\n", name, det.URL)
			}
			os.Exit(1)
		}
	}
	if false { // noisy, keep it simple.
		fmt.Printf("before remove: existing membership: (%v leader)\n", leaderName)
		for name, det := range newestMembership.PeerNames.All() {
			fmt.Printf("  %v:   %v\n", name, det.URL)
		}
	}

	var targetPeerID string
	if det.URL == "pending" {

	} else {
		_, _, targetPeerID, _, err = rpc.ParsePeerURL(det.URL)
		panicOn(err)
	}

	//errWriteDur := time.Second * 20
	var errWriteDur time.Duration
	insp2, _, err := node.RemovePeerIDFromCluster(ctx, force, cmdCfg.NonVotingShadowFollower, target, targetPeerID, tube.TUBE_REPLICA, "", leaderURL, errWriteDur)
	panicOn(err)

	if insp2 == nil ||
		insp2.MC == nil ||
		insp2.MC.PeerNames == nil {
		fmt.Printf("empty or nil membership from '%v'\n", leaderName)
	} else {
		// tuberm -shadow
		if cmdCfg.NonVotingShadowFollower {
			if insp2.ShadowReplicas.PeerNames.Len() == 0 {
				fmt.Printf("shadow replicas after removing '%v': empty.\n", target)
			} else {
				fmt.Printf("remaining shadow replicas:\n")
				for name, det := range insp2.ShadowReplicas.PeerNames.All() {
					fmt.Printf("  %v:   %v\n", name, det.URL)
				}
			}
		} else {
			// tuberm
			if insp2.MC.PeerNames.Len() == 0 {
				fmt.Printf("\nmembership after removing '%v': empty.\n", target)
			} else {
				fmt.Printf("\nmembership after removing '%v': (%v leader)\n", target, leaderName)
				for name, det := range insp2.MC.PeerNames.All() {
					fmt.Printf("  %v:   %v\n", name, det.URL)
				}
			}
			if insp2.ShadowReplicas != nil &&
				insp2.ShadowReplicas.PeerNames.Len() > 0 {
				fmt.Printf("shadow replicas:\n")
				for name, det := range insp2.ShadowReplicas.PeerNames.All() {
					fmt.Printf("  %v:   %v\n", name, det.URL)
				}
			}
		}
	}
}
