package main

import (
	"github.com/glycerine/rpc25519/tube"
)

func main() {
	mem := tube.NewRMember("hermes")
	mem.Start()
	<-mem.Ready
	select {}
}
