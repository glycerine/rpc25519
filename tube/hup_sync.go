//go:build synctest

package tube

// stubs for hup.go under synctest

import (
	rpc "github.com/glycerine/rpc25519"
)

var debugGlobalCkt = rpc.NewMutexmap[string, *cktPlus]()

// for single node per process, HUP debug below; not tests.
var hupDebugGlobalLastNode *TubeNode
