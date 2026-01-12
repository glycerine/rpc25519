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
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/glycerine/ipaddr"
	//rpc "github.com/glycerine/rpc25519"
	"github.com/glycerine/rpc25519/tube"
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
	if cmdCfg.Verbose {
		verboseVerbose = true
		tube.VerboseVerbose.Store(true)
	}

	// first connect
	dir := tube.GetConfigDir()
	pathCfg := dir + "/" + "tup.default.config"
	if fileExists(pathCfg) {
		//vv("using config file: '%v'", pathCfg)
	} else {
		dir := filepath.Dir(pathCfg)
		if dir != "" {
			os.MkdirAll(dir, 0777)
		}
		fd, err := os.Create(pathCfg)
		panicOn(err)
		cfg := &tube.TubeConfig{
			MyName:          "tubels",
			PeerServiceName: tube.TUBE_CLIENT,
		}
		fmt.Fprintf(fd, "%v\n", cfg.SexpString(nil))
		fd.Close()
		fmt.Fprintf(os.Stderr, `
 -- tup error: no config file found at '%v'.
 -- I have just created a starter config file and wrote it to
%v
 -- for your convenience. Please fill in at least the :port of your nodes.
 -- An example can be found at
https://github.com/glycerine/rpc25519/blob/41cdfa8b5f81a35e0b7e59f44785b61d7ad850c1/tube/example/local/tup.default.config

`, pathCfg, pathCfg)
		os.Exit(1)
	}
	by, err := os.ReadFile(pathCfg)
	panicOn(err)
	//vv("got by = '%v'", string(by))

	cfg, err := tube.NewTubeConfigFromSexpString(string(by), nil)
	panicOn(err)
	cfg.PeerServiceName = tube.TUBE_CLIENT
	cfg.MyName = "tubels_" + tube.CryRand15B()

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

	//ps := zygo.NewPrintStateWithIndent(4)
	pp("cfg = '%v'", cfg.ShortSexpString(nil))

	name := cfg.MyName
	node := tube.NewTubeNode(name, cfg)

	err = node.InitAndStart()
	panicOn(err)
	defer node.Close()
	const keepCktUp = false

	ctx5, canc := context.WithTimeout(context.Background(), time.Second*5)
	leaderURL, leaderName, insp, reallyLeader, contacted, err := node.HelperFindLeader(ctx5, cfg, cmdCfg.ContactName, false, keepCktUp)
	canc()
	panicOn(err)
	fmt.Printf("contacted:\n")
	for _, insp := range sortByName(contacted) {
		fmt.Printf(`%v %v  (lead: '%v')
   LastLog:{Term: '%v'; Index: '%v'; LeaderName: '%v'; TicketOp: %v}
   LogIndexBaseC: %v
   MC: %v   ShadowReplicas: %v   URL: %v
`, insp.ResponderName, insp.Role, insp.CurrentLeaderName,

			insp.LastLogTerm,
			insp.LastLogIndex,
			insp.LastLogLeaderName,
			insp.LastLogTicketOp,
			insp.LogIndexBaseC,

			insp.MC,
			insp.ShadowReplicas,
			insp.ResponderPeerURL)
	}
	pp("tubels using leaderName = '%v'; leaderURL='%v'; err='%v'", leaderName, leaderURL, err)

	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}

	if leaderName == "" {
		leaderName = "no known"
	}
	seen := make(map[string]string)

	var newestMembership *tube.MemberConfig
	if insp != nil {
		newestMembership = insp.MC

		for k, v := range insp.CktAllByName {
			if k != cfg.MyName && !strings.HasPrefix(k, "tup_") {
				seen[k] = v
			}
		}
	}
	var who string
	if reallyLeader {
		who = fmt.Sprintf("(%v leader)", leaderName)
	} else {
		// only bother to report from leader
		return
	}
	//else {
	//	who = fmt.Sprintf("(contacted %v)", leaderName)
	//}
	fmt.Println()
	fmt.Printf("existing membership: %v\n", who)
	if newestMembership != nil && newestMembership.PeerNames != nil {
		for name, det := range newestMembership.PeerNames.All() {
			fmt.Printf("  %v:   %v\n", name, det.URL)
			delete(seen, name)
		}
	} else {
		fmt.Printf("   -- newestMembership is nil.\n")
	}
	if insp != nil &&
		insp.ShadowReplicas != nil &&
		insp.ShadowReplicas.PeerNames != nil &&
		insp.ShadowReplicas.PeerNames.Len() > 0 {

		fmt.Printf("\nshadow replicas:\n")
		for name, det := range insp.ShadowReplicas.PeerNames.All() {
			url, ok := insp.CktAllByName[name]
			if !ok {
				url = det.Addr
			}
			fmt.Printf("  %v:   %v\n", name, tube.URLTrimCktID(url))
			delete(seen, name)
		}
	}
	return
}

type sortByResponderName []*tube.Inspection

func (lo sortByResponderName) Len() int { return len(lo) }
func (lo sortByResponderName) Less(i, j int) bool {
	return lo[i].ResponderName < lo[j].ResponderName
}
func (lo sortByResponderName) Swap(i, j int) {
	lo[i], lo[j] = lo[j], lo[i]
}

func sortByName(s []*tube.Inspection) (r []*tube.Inspection) {
	sort.Sort(sortByResponderName(s))
	return s
}
