package main

import (
	"context"

	"github.com/glycerine/rpc25519/tube/hermes"
)

func main() {
	panicOn(hermes.RunFromDiskConfig(context.Background(), "hermes"))
}
