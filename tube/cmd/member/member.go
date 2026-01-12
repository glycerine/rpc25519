package main

import (
	"fmt"
	"os"
	"time"

	"github.com/glycerine/rpc25519/tube"
)

func main() {
	tube.VerboseVerbose.Store(true)
	fmt.Printf("pid = %v\n", os.Getpid())

	clockDriftBound := 500 * time.Millisecond
	tableSpace := "hermes"
	mem := tube.NewRMember(tableSpace, clockDriftBound)
	mem.Start()
	<-mem.Ready.Chan
	select {}
}
