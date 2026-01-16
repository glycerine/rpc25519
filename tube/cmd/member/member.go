package main

import (
	"fmt"
	"os"
	"time"

	"github.com/glycerine/rpc25519/tube"
)

func main() {
	tube.VerboseVerbose.Store(false)
	fmt.Printf("pid = %v\n", os.Getpid())

	startOnlineWebProfiling()

	clockDriftBound := 500 * time.Millisecond
	tableSpace := "hermes"
	mem := tube.NewRMember(tableSpace, clockDriftBound)
	mem.Start()
	<-mem.Ready.Chan
	select {}
}
