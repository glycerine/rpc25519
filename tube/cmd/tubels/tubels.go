package main

// tubels lists the current cluster membership.
//
// At the moment it will use tup's config to
// find the cluster.

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	//"path/filepath"
	//"sort"
	"time"

	"github.com/glycerine/ipaddr"
	//rpc "github.com/glycerine/rpc25519"
	"github.com/glycerine/zdb/tube"
	//"github.com/glycerine/zygomys/zygo"
)

var sep = string(os.PathSeparator)

type TubeListConfig struct {
	ContactName string // -c name of node to contact
	Help        bool   // -h for help, false, show this help
	Verbose     bool   // -v for verbose connection logging
}

func (c *TubeListConfig) SetFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.ContactName, "c", "", "name of node to contact (defaults to leader)")
	fs.BoolVar(&c.Help, "h", false, "show this help")
	fs.BoolVar(&c.Verbose, "v", false, "verbose connection logging")
}

func (c *TubeListConfig) FinishConfig(fs *flag.FlagSet) (err error) {
	return
}
func (c *TubeListConfig) SetDefaults() {}

func main() {
	cmdCfg := &TubeListConfig{}

	fs := flag.NewFlagSet("tubels", flag.ExitOnError)
	cmdCfg.SetFlags(fs)
	fs.Parse(os.Args[1:])
	cmdCfg.SetDefaults()
	err := cmdCfg.FinishConfig(fs)
	panicOn(err)

	if cmdCfg.Help {
		fmt.Fprintf(os.Stderr, "tubels help:\n")
		fs.PrintDefaults()
		return
	}
	verboseVerbose = cmdCfg.Verbose

	// first connect, then run repl
	dir := tube.GetConfigDir()
	pathCfg := dir + "/" + "tup.default.config"
	if fileExists(pathCfg) {
		//vv("using config file: '%v'", pathCfg)
	} else {
		fd, err := os.Create(pathCfg)
		panicOn(err)
		cfg := &tube.TubeConfig{
			MyName:          "tubels",
			PeerServiceName: tube.TUBE_CLIENT,
		}
		fmt.Fprintf(fd, "%v\n", cfg.SexpString(nil))
		fd.Close()
		fmt.Fprintf(os.Stderr, "tup error: no config file. Created one from template in '%v'. Please complete it.\n", pathCfg)
		os.Exit(1)
	}
	by, err := os.ReadFile(pathCfg)
	panicOn(err)
	//vv("got by = '%v'", string(by))

	cfg, err := tube.NewTubeConfigFromSexpString(string(by), nil)
	panicOn(err)
	cfg.PeerServiceName = tube.TUBE_CLIENT
	cfg.MyName = "tubels_" + tube.CryRand15B()

	myHost := ipaddr.GetExternalIP()
	myPort := ipaddr.GetAvailPort()
	cfg.RpcCfg.ServerAddr = fmt.Sprintf("%v:%v", myHost, myPort)

	// set up our config
	const quiet = false
	const isTest = false
	cfg.Init(quiet, isTest)

	cfg.UseSimNet = false

	cfg.RpcCfg.TCPonly_no_TLS = cfg.TCPonly_no_TLS
	cfg.RpcCfg.ServerAutoCreateClientsToDialOtherServers = true
	cfg.RpcCfg.QuietTestMode = true

	cfg.ClientProdConfigSaneOrPanic()

	//ps := zygo.NewPrintStateWithIndent(4)
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
			pp("by default we contact cfg.InitialLeaderName='%v'; addr='%v' -> leaderURL = '%v'", cfg.InitialLeaderName, addr, leaderURL)
		} else {
			pp("requested cmdCfg.ContactName='%v' maps to addr='%v' -> URL = '%v'", cmdCfg.ContactName, addr, leaderURL)
		}
	}

	pp("leaderURL='%v'; addr='%v'", leaderURL, addr)
	cli, err := node.StartClientOnly(ctx, addr)
	if err != nil {
		pp("error on client attempt to connect to addr='%v': %v", addr, err)
		if cli != nil {
			cli.Close()
		}
		node.Halt.ReqStop.Close()
		node = tube.NewTubeNode(name, cfg)

		// try others
		var name, addr string
		for name, addr = range cfg.Node2Addr {
			if name == greet || name == cfg.MyName {
				continue
			}
			pp("instead trying addr='%v'", addr)
			cli, err = node.StartClientOnly(ctx, addr)
			if err == nil {
				leaderURL = tube.FixAddrPrefix(addr)
				break
			}
		}
		if err != nil {
			alwaysPrintf("could not contact anyone. last attempt to '%v' (addr: '%v') => err='%v'\n", name, addr, err)
		}
	}
	defer cli.Close()

	//err = node.UseLeaderURL(ctx, leaderURL)
	//panicOn(err)
	//pp("back from cli.UseLeaderURL(leaderURL='%v')", leaderURL)

	ctx5sec, canc5 := context.WithTimeout(ctx, 5*time.Second)
	newestMembership, insp, actualLeaderURL, leaderName, onlyPossibleAddr, err := node.GetPeerListFrom(ctx5sec, leaderURL)
	canc5()
	if insp != nil {
		//alwaysPrintf("     insp.ResponderName='%v'", insp.ResponderName)
	}
	if err != nil {
		alwaysPrintf("error from node.GetPeerListFrom(leaderURL='%v'): %v", leaderURL, err)
		os.Exit(1)
	}
	_ = insp
	_ = actualLeaderURL
	_ = onlyPossibleAddr
	if leaderName == "" {
		leaderName = "no known"
	}
	seen := make(map[string]string)
	for k, v := range insp.CktAllByName {
		if k != cfg.MyName && !strings.HasPrefix(k, "tup_") {
			seen[k] = v
		}
	}
	fmt.Printf("existing membership: (%v leader)\n", leaderName)
	if newestMembership != nil && newestMembership.PeerNames != nil {
		for name, det := range newestMembership.PeerNames.All() {
			fmt.Printf("  %v:   %v\n", name, det.URL)
			delete(seen, name)
		}
	} else {
		fmt.Printf("   -- newestMembership is nil.\n")
	}
	if false { // usually dead, kind of irrelevant
		if len(seen) > 0 {
			fmt.Printf("   -- others in insp.CktAllByName:\n")
			for name, url := range seen {
				fmt.Printf("     %v:   %v\n", name, url)
			}
		}
	}
	return
	/*
		if memlistAfter == nil ||
			memlistAfter.MC == nil ||
			memlistAfter.MC.PeerNames == nil {
			fmt.Printf("empty or nil membership from '%v'\n", leaderName)
		} else {
			for name, det := range memlistAfter.MC.PeerNames.All() {
				fmt.Printf("  %v:   %v\n", name, det.URL)
			}
		}
	*/
	/*
	   //vv("GetPeerListFrom(leaderURL='%v') -> actualLeaderURL = '%v'", leaderURL, actualLeaderURL)

	   	if actualLeaderURL != "" && actualLeaderURL != leaderURL {
	   		vv("use actual='%v' rather than orig='%v'", actualLeaderURL, leaderURL)
	   		leaderURL = actualLeaderURL
	   	}

	   url, ok := newestMembership[target]

	   	if !ok {
	   		fmt.Printf("error: target not in current membership. target='%v'\n", target)
	   		fmt.Printf("\nexisting membership:\n")
	   		for name, url := range sorted(newestMembership) {
	   			fmt.Printf("  %v:   %v\n", name, url)
	   		}
	   		os.Exit(1)
	   	}

	   _, _, targetPeerID, _, err := rpc.ParsePeerURL(url)
	   panicOn(err)

	   errWriteDur := time.Second * 20
	   memlistAfter, err := node.RemovePeerIDFromCluster(ctx, target, targetPeerID, tube.TUBE_REPLICA, "", leaderURL, errWriteDur)
	   panicOn(err)

	   fmt.Printf("\nmembership after removing '%v':\n", target)

	   	for name, url := range memlistAfter.MC.PeerNames.All() {
	   		fmt.Printf("  %v:   %v\n", name, url)
	   	}
	*/
}
