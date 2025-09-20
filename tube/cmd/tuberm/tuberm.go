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
	"net/url"
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
	ContactName string // -c name of node to contact
	Help        bool   // -h for help, false, show this help
	//ForceName   string // -f force node to remove MC change
	WipeName                string // -e wipe to empty MC
	NonVotingShadowFollower bool   // -shadow remove shadow replica
}

func (c *TubeRemoveConfig) SetFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.ContactName, "c", "", "name of node to contact (defaults to leader)")
	fs.BoolVar(&c.Help, "h", false, "show this help")
	//fs.StringVar(&c.ForceName, "f", "", "name of node to force remove MC change")
	fs.StringVar(&c.WipeName, "e", "", "name of node to force install empty MC")
	fs.BoolVar(&c.NonVotingShadowFollower, "shadow", false, "remove this non-voting shadow follower replica")
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

	cli, err := node.StartClientOnly(ctx, addr)
	if err != nil {
		if cli != nil {
			cli.Close()
		}
		node.Halt.ReqStop.Close()
		if cmdCfg.WipeName != "" {
			panic(fmt.Sprintf("error: could not connect to server '%v': err='%v'", cmdCfg.WipeName, err))
		}
		node = tube.NewTubeNode(name, cfg)

		// try others
		for name, addr := range cfg.Node2Addr {
			if name == greet {
				continue
			}
			cli, err = node.StartClientOnly(ctx, addr)
			if err == nil {
				leaderURL = tube.FixAddrPrefix(addr)
				break
			}
		}
		panicOn(err)
	}
	defer cli.Close()

	if cmdCfg.WipeName != "" {
		err = node.InjectEmptyMC(ctx, leaderURL, cmdCfg.WipeName)
		if err != nil {
			alwaysPrintf("error when installing empty MC on '%v': %v", leaderURL, err)
			os.Exit(1)
		}
		alwaysPrintf("installed empty MC on '%v'.", leaderURL)
		os.Exit(0)
	}

	//err = node.UseLeaderURL(ctx, leaderURL)
	//panicOn(err)
	//vv("back from cli.UseLeaderURL(leaderURL='%v')", leaderURL)

	newestMembership, insp, actualLeaderURL, leaderName, onlyPossibleAddr, err := node.GetPeerListFrom(ctx, leaderURL)
	_ = onlyPossibleAddr
	_ = insp
	panicOn(err)
	if target == "" {
		fmt.Printf("existing membership: (%v leader)\n", leaderName)
		for name, det := range newestMembership.PeerNames.All() {
			fmt.Printf("  %v:   %v\n", name, det)
		}
		os.Exit(0)
	}
	//vv("GetPeerListFrom(leaderURL='%v') -> actualLeaderURL = '%v'", leaderURL, actualLeaderURL)
	if actualLeaderURL != "" && actualLeaderURL != leaderURL {
		//vv("use actual as leaderURL='%v' rather than orig='%v'", actualLeaderURL, leaderURL)
		leaderURL = actualLeaderURL

		// so I guess we must start another client.
		cli.Close()
		node.Close()

		node = tube.NewTubeNode(name, cfg)

		addr2, _, _, _, err := rpc.ParsePeerURL(leaderURL)
		panicOn(err)
		u, err := url.Parse(addr2)
		panicOn(err)
		host := u.Host
		// port := u.Port()
		// if port != "" {
		// 	host = net.JoinHostPort(host, port)
		// }

		//vv("make cli2 to host='%v'", host)
		cli2, err := node.StartClientOnly(ctx, host)
		//cli, err := node.StartClientOnly(ctx, leaderURL)
		panicOn(err)
		defer cli2.Close()
		// this makes it work, but we were not getting ack
		// b/c the new leader didn't know how to contact us.
		// It hit the dead letter.

		// try again
		newestMembership, insp, actualLeaderURL, leaderName, onlyPossibleAddr, err = node.GetPeerListFrom(ctx, host)
		_ = insp
		_ = onlyPossibleAddr
		panicOn(err)
	}

	if newestMembership == nil {
		panic("why is newestMembership nil?")
	}
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
	insp2, _, err := node.RemovePeerIDFromCluster(ctx, cmdCfg.NonVotingShadowFollower, target, targetPeerID, tube.TUBE_REPLICA, "", leaderURL, errWriteDur)
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
				fmt.Printf("membership after removing '%v': empty.\n", target)
			} else {
				fmt.Printf("membership after removing '%v': (%v leader)\n", target, leaderName)
				for name, det := range insp2.MC.PeerNames.All() {
					fmt.Printf("  %v:   %v\n", name, det.URL)
				}
			}
		}
	}
}
