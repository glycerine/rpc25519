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
	//rpc "github.com/glycerine/rpc25519"
	"github.com/glycerine/rpc25519/tube"
)

var sep = string(os.PathSeparator)

type TubeAddConfig struct {
	ContactName             string // -c name of node to contact
	Help                    bool   // -h for help, false, show this help
	ForceName               string // -f forcely add node to MC
	NonVotingShadowFollower bool   // -shadow add as non-voting-follower ("shadow replica")
	Verbose                 bool   // -v verbose: show config/connection attempts.
}

func (c *TubeAddConfig) SetFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.ContactName, "c", "", "name of node to contact (defaults to leader)")
	fs.BoolVar(&c.Help, "h", false, "show this help")
	fs.BoolVar(&c.NonVotingShadowFollower, "shadow", false, "add node as non-voting shadow follower replica")
	fs.StringVar(&c.ForceName, "f", "", "node to forcefully add to MC")
	fs.BoolVar(&c.Verbose, "v", false, "verbose diagnostics logging to stdout")
}

func (c *TubeAddConfig) FinishConfig(fs *flag.FlagSet) (err error) {
	return
}
func (c *TubeAddConfig) SetDefaults() {}

func main() {
	cmdCfg := &TubeAddConfig{}

	fs := flag.NewFlagSet("tubeadd", flag.ExitOnError)
	cmdCfg.SetFlags(fs)
	fs.Parse(os.Args[1:])
	cmdCfg.SetDefaults()
	err := cmdCfg.FinishConfig(fs)
	panicOn(err)

	if cmdCfg.Help {
		fmt.Fprintf(os.Stderr, "tubeadd help:\n")
		fs.PrintDefaults()
		fmt.Printf(`
On getting wedged
=================

In the wedged scenario created by losing quorum:

1. bring up 3 nodes (member: 0,1,2)

2. if we tuberm node_0, to remove node 0
  into shadow (preparing for take it down...);
  then say node 1 becomes leader. (members: 1,2)

3. if node 2 accidentally dies (or is otherwise
unavailable for a long while) at this point,
 we currently have leader 1 with a quorum of both
(members: 1,2) but no way to meet it. Without
a way to meet quorum on any new membership,
it cannot be installed.

=> we are wedged. We have lost quorum.

We cannot bring back in node 0 to cover for
the node 2 failure, despite it being still available/
viable because quorum (2 of 2) needs
node 2, which has died.

Solution: tubeadd -f node_0 -c node_1
     AND: tubeadd -f node_0 -c node_0

which says: force 1 and 0 to add node_0 to the membership;
ignoring the qourum rules that usually forbid this.
This typically will enable an election to start.

It is a little risky because the version of the
membership could possible get desynced and
then differ across the cluster, but
usually it will get the cluster back
online quickly, and the leader will
quickly bring everyone up to speed with
its version of membership.

Another (rather nuclear) fallback is to use tuberm -e on
each node in turn to nuke (erase, that's -e for erase)
the membership on every node in turn,
and then tubeadd them back one by one.
Similarly, rebooting tube with tube -zap
will clear out the node's membership
and even the shadow replica set,
if it comes to that.
`)
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

	pp("cfg = '%v'", cfg.ShortSexpString(nil))

	//nodeID := rpc.NewCallID("")
	name := cfg.MyName
	node := tube.NewTubeNode(name, cfg)
	//defer node.Close()
	ctx := context.Background()

	var leaderURL string
	if false {
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
	}

	//var cli *rpc.Client
	//cli, err := node.StartClientOnly(ctx, addr)
	err = node.InitAndStart()
	panicOn(err)
	defer node.Close()
	const requireOnlyContact = true
	const keepCktUp = true

	ctx5, canc := context.WithTimeout(ctx, time.Second*5)
	leaderURL, leaderName, _, reallyLeader, contacted, err := node.HelperFindLeader(ctx5, cfg, cmdCfg.ContactName, requireOnlyContact, keepCktUp)
	canc()
	_ = reallyLeader
	panicOn(err)
	var contactURL string
	if cmdCfg.ContactName != "" {
		for _, insp := range contacted {
			if insp.ResponderName == cmdCfg.ContactName {
				contactURL = insp.ResponderPeerURL
			}
		}
	}
	if true {
		fmt.Printf("tubeadd contacted:\n")
		for _, insp := range contacted {
			fmt.Printf(`%v %v  (lead: '%v')
   LastLog:{Term: '%v'; Index: '%v'; LeaderName: '%v'; TicketOp: %v}
   LogIndexBaseC: %v      Hostname: %v    PID: %v
   MC: %v   ShadowReplicas: %v   URL: %v
`, insp.ResponderName, insp.Role, insp.CurrentLeaderName,

				insp.LastLogTerm,
				insp.LastLogIndex,
				insp.LastLogLeaderName,
				insp.LastLogTicketOp,
				insp.LogIndexBaseC,

				insp.Hostname,
				insp.PID,

				insp.MC,
				insp.ShadowReplicas,
				insp.ResponderPeerURL)
		}
	}
	if len(contacted) == 1 {
		// might be bootstrapping from 1 node, contact them
		// since they are all we have. Without this logic,
		// we don't even try unless that also happen to
		// be the pre-configured guess at who is leader
		// in our config file(!)
		leaderName = contacted[0].ResponderName
		leaderURL = contacted[0].ResponderPeerURL
	} else {
		if cmdCfg.ContactName != "" && contactURL != "" {
			// enforce -c contactName request. We actually
			// contacted them, so target them with our Add request.
			leaderName = cmdCfg.ContactName
			leaderURL = contactURL
		}
	}
	vv("tubeadd is doing AddPeerIDToCluster using leaderName = '%v'; leaderURL='%v'", leaderName, leaderURL)

	targetPeerID := "" // empty string allowed now
	var errWriteDur time.Duration
	//errWriteDur := time.Second * 20
	peerServiceName := tube.TUBE_REPLICA
	baseServerHostPort := ""
	ctx5, canc = context.WithTimeout(ctx, time.Second*5)
	memlistAfter, stateSnapshot, err := node.AddPeerIDToCluster(ctx5, force, cmdCfg.NonVotingShadowFollower, target, targetPeerID, peerServiceName, baseServerHostPort, leaderURL, errWriteDur)
	canc()
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

		fmt.Printf("\nempty or nil membership from '%v'\n", leaderName)
	} else {
		fmt.Printf("\nmembership after adding '%v': (%v leader)\n", target, leaderName)
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
