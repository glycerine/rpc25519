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

	memb, err := tube.NewRMember()
	panicOn(err)
	memb.InitAndStart()
}
