//go:build goexperiment.synctest

package rpc25519

import (
	"testing"
	"testing/synctest"
)

func bubbleOrNot(f func()) {
	synctest.Run(f)
}

func onlyBubbled(t *testing.T, f func()) {
	synctest.Run(f)
}

const globalUseSynctest bool = true

func synctestWait_LetAllOtherGoroFinish() {
	synctest.Wait()
}
