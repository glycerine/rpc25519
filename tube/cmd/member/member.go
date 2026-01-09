package main

import (
	"time"

	"github.com/glycerine/rpc25519/tube"
)

func main() {
	tube.VerboseVerbose.Store(true)

	clockDriftBound := 20 * time.Millisecond
	tableSpace := "hermes"
	mem := tube.NewRMember(tableSpace, clockDriftBound)
	mem.Start()
	<-mem.Ready
	select {}
}
