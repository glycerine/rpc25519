//go:build !synctest || !pont

package rpc25519

import (
// "testing/synctest"
)

var bubbleRootGoroNum uint64

func bgid() uint64 {
	// a poor and probably non-deterministic approximation when not under Pont.
	return uint64(GoroNumber()) - bubbleRootGoroNum

	// just to note...
	// we cannot use the deterministic synctest.Bgid() without Pont.
}
