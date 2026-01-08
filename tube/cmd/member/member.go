package main

import (
	"time"

	"github.com/glycerine/rpc25519/tube"
)

func main() {
	clockDriftBound := 20 * time.Millisecond
	mem := tube.NewRMember("hermes", clockDriftBound)
	mem.Start()
	<-mem.Ready
	select {}
}
