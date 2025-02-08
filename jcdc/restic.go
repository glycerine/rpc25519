package jcdc

import (
	resticChunker "github.com/glycerine/restic-chunker-mod" // chunker
)

func evaluateDistribution() {
	b := resticChunker.NewBase(resticChunker.Pol(0x3DA3358B4DC173))
	_ = b
}
