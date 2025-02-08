package jcdc

import (
	//"bytes"
	//"crypto/sha256"
	//"fmt"
	//"io"
	//"math/rand"

	chunker "github.com/glycerine/restic-chunker-mod"
)

func evaluateDistribution() {
	b := chunker.NewBase(chunker.Pol(0x3DA3358B4DC173))
	_ = b
}
