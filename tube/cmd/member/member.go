package main

import (
	//"bufio"
	"context"
	"flag"
	"fmt"
	//"io"
	"os"
	//"strings"
	//"path/filepath"
	//"sort"
	//"time"

	//rpc "github.com/glycerine/rpc25519"
	//"github.com/glycerine/ipaddr"
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

	cliName := "client710"
	const quiet = false
	const isTest = false
	const useSimNet = false
	cliCfg, err := tube.LoadFromDiskTubeConfig("member", quiet, useSimNet, isTest)
	panicOn(err)
	vv("cliCfg = '%v'", cliCfg)

	cli := tube.NewTubeNode(cliName, cliCfg)
	err = cli.InitAndStart()
	panicOn(err)
	defer cli.Close()

	bkg := context.Background()

	//leaderURL0 :=

	const requireOnlyContact = false

	leaderURL, leaderName, _, reallyLeader, _, err := cli.HelperFindLeader(cliCfg, "", requireOnlyContact)
	panicOn(err)
	vv("got leaderName = '%v'; leaderURL = '%v'; reallyLeader='%v'", leaderName, leaderURL, reallyLeader)

	sess, err := cli.CreateNewSession(bkg, leaderURL)
	panicOn(err)
	vv("got sess = '%v'", sess)

}
