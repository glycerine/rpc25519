package main

import (
	//"bufio"
	"context"
	"flag"
	"fmt"
	//"io"
	"os"
	"strings"
	//"path/filepath"
	//"sort"
	//"time"

	//rpc "github.com/glycerine/rpc25519"
	"github.com/glycerine/ipaddr"
	"github.com/glycerine/rpc25519/tube"
	//"github.com/glycerine/rpc25519/tube/art"
)

var sep = string(os.PathSeparator)

type ConfigMember struct {
	ContactName string // -c name of node to contact
	Help        bool   // -h for help, false, show this help
	Verbose     bool   // -v verbose: show config/connection attempts.
}

func (c *ConfigMember) SetFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.ContactName, "c", "", "name of node to contact (defaults to leader)")
	fs.BoolVar(&c.Help, "h", false, "show this help")
	fs.BoolVar(&c.Verbose, "v", false, "verbose diagnostics logging to stdout")
}

func (c *ConfigMember) FinishConfig(fs *flag.FlagSet) (err error) {
	return
}

func (c *ConfigMember) SetDefaults() {}

func main() {
	cmdCfg := &ConfigMember{}

	fs := flag.NewFlagSet("member", flag.ExitOnError)
	cmdCfg.SetFlags(fs)
	fs.Parse(os.Args[1:])
	cmdCfg.SetDefaults()
	err := cmdCfg.FinishConfig(fs)
	panicOn(err)

	if cmdCfg.Verbose {
		verboseVerbose = true
		tube.VerboseVerbose.Store(true)
	}
	if cmdCfg.Help {
		fmt.Fprintf(os.Stderr, "member help:\n")
		fs.PrintDefaults()
		return
	}

	// first connect, then run repl
	dir := tube.GetConfigDir()
	pathCfg := dir + "/" + "member.default.config"
	if fileExists(pathCfg) {
		pp("using config file: '%v'", pathCfg)
	} else {
		fd, err := os.Create(pathCfg)
		panicOn(err)
		cfg := &tube.TubeConfig{
			// distinguish multiple member clients
			MyName:          "member_" + tube.CryRand15B(),
			PeerServiceName: tube.TUBE_CLIENT,
		}
		fmt.Fprintf(fd, "%v\n", cfg.SexpString(nil))
		fd.Close()
		fmt.Fprintf(os.Stderr, "member error: no config file. Created one from template in '%v'. Please complete it.\n", pathCfg)
		os.Exit(1)
	}
	by, err := os.ReadFile(pathCfg)
	panicOn(err)
	pp("got by = '%v'", string(by))

	cfg, err := tube.NewTubeConfigFromSexpString(string(by), nil)
	panicOn(err)
	// distinguish multiple member clients
	cfg.MyName = "member_" + tube.CryRand15B()
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

	cfg.ConvertToExternalAddr()

	pp("cfg = '%v'", cfg.ShortSexpString(nil))

	//nodeID := rpc.NewCallID("")
	name := cfg.MyName
	node := tube.NewTubeNode(name, cfg)

	err = node.InitAndStart()
	panicOn(err)
	defer node.Close()

	// Use HelperFindLeader for better chance of locating a leader

	ctx := context.Background()

	// If requireOnlyContact is true,
	// then HelperFindLeader will
	// immediately exit(1) if the contactName is
	// not also the current leader.
	const requireOnlyContact = false

	leaderURL, leaderName, _, reallyLeader, _, err := node.HelperFindLeader(cfg, cmdCfg.ContactName, requireOnlyContact)
	panicOn(err)
	if !reallyLeader {
		panic("could not find leader")
	}

	// when no leader, we hang, our tkt in awaitingLeader.
	pp("%v: calling node.CreateNewSession(leaderURL = '%v')", cfg.MyName, leaderURL)
	sess, err := node.CreateNewSession(ctx, leaderURL)
	panicOn(err)
	defer sess.Close()
	//pp("back from node.CreateNewSession(leaderURL='%v')", leaderURL)

	needNewSess := func(sess *tube.Session, err error) (s2 *tube.Session) {
		if err == nil {
			return sess
		}
		errs := err.Error()
		if strings.Contains(errs, "call CreateNewSession first") {
			sess.Close()
			s2, err := node.CreateNewSession(ctx, leaderURL)
			panicOn(err)
			return s2
		}
		return sess
	}
	_ = needNewSess
	// repl loop

	table := "hermes"
	fmt.Printf(`member: reliable membership demonstrator.
`)

	for {
		fmt.Printf("[%v connected](table '%v') > ", leaderName, table)
	}
}
