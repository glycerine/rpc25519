//go:build !goexperiment.synctest

package rpc25519

import (
	"testing"
)

const globalUseSynctest bool = false

func synctestWait_LetAllOtherGoroFinish() {}

func bubbleOrNot(f func()) {
	f()
}

func onlyBubbled(t *testing.T, f func()) {
	t.Skip("onlyBubbled: skipping test")
}
