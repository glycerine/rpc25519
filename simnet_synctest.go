//go:build goexperiment.synctest

package rpc25519

import (
	"testing/synctest"
)

func synctestWait() {
	synctest.Wait()
}
