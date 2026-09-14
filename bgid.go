//go:build synctest

package rpc25519

import (
	"testing/synctest"
)

func bgid() uint64 {
	return synctest.Bgid()
}
