package main

import (
	"fmt"
	"os"
	"time"

	"github.com/glycerine/rpc25519/tube"
)

func main() {
	verbose := false
	tube.VerboseVerbose.Store(verbose)
	fmt.Printf("pid = %v\n", os.Getpid())

	startOnlineWebProfiling()

	const quiet = false
	const isTest = false
	const useSimNet = false
	cliCfg, err := tube.LoadFromDiskTubeConfig("member", quiet, useSimNet, isTest)
	panicOn(err)
	////vv("cliCfg = '%v'", cliCfg)
	cliCfg.RpcCfg.QuietTestMode = false
	name := cliCfg.MyName

	cliCfg.ClockDriftBound = 500 * time.Millisecond
	tableSpace := "hermes"
	mem := tube.NewRMember(tableSpace, cliCfg)
	mem.Start()
	<-mem.Ready.Chan
	for {
		select {
		case reply := <-mem.UpcallMembershipChangeCh:
			vv("%v member sees membership upcall: '%v'", name, reply)
		case untilTm := <-mem.OperatingLeaseRenewCh:
			_ = untilTm
			// have to service this, has a finite buffered channel.
			//vv("%v member sees operLeaseUntilTm: '%v'", name, nice(untilTm))
		}
	}
}
