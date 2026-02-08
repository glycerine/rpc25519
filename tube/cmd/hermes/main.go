package main

import (
	//"bufio"
	//"context"
	//"flag"
	"fmt"
	//"io"
	"os"
	//"strings"
	//"path/filepath"
	//"sort"
	"time"

	//rpc "github.com/glycerine/rpc25519"
	//"github.com/glycerine/ipaddr"
	"github.com/glycerine/rpc25519/hermes"
	"github.com/glycerine/rpc25519/tube"
	//"github.com/glycerine/rpc25519/tube/art"
)

var _ = &hermes.HermesTicket{}

//var sep = string(os.PathSeparator)

func main() {
	verbose := false
	tube.VerboseVerbose.Store(verbose)
	fmt.Printf("pid = %v\n", os.Getpid())

	//startOnlineWebProfiling()

	const quiet = false
	const isTest = false
	const useSimNet = false
	tubeCfg, err := tube.LoadFromDiskTubeConfig("member", quiet, useSimNet, isTest)
	panicOn(err)
	////vv("tubeCfg = '%v'", tubeCfg)
	tubeCfg.RpcCfg.QuietTestMode = false
	name := tubeCfg.MyName

	tubeCfg.ClockDriftBound = 500 * time.Millisecond
	tableSpace := "hermes"
	mem := tube.NewRMember(tableSpace, tubeCfg)
	mem.Start()
	<-mem.Ready.Chan

	hcfg := &hermes.HermesConfig{
		ReplicationDegree:  3,
		MessageLossTimeout: 3 * time.Second,
		TCPonly_no_TLS:     tubeCfg.RpcCfg.TCPonly_no_TLS,
	}
	hnode := hermes.NewHermesNode(name, hcfg)
	hnode.UpcallMembershipChangeCh = mem.UpcallMembershipChangeCh
	err = hnode.Init()
	panicOn(err)
	select {}

	for {
		select {
		case reply := <-mem.UpcallMembershipChangeCh:
			vv("%v member sees membership upcall: '%v'", name, reply)
		}
	}
}
