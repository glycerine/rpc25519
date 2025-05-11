//go:build goexperiment.synctest

package rpc25519

import (
	"fmt"
	"testing"
	"testing/synctest"
)

const globalUseSynctest bool = true

func init() {
	fmt.Printf("globalUseSynctest = %v\n", globalUseSynctest)
}

func bubbleOrNot(f func()) {
	synctest.Run(f)
}

func onlyBubbled(t *testing.T, f func()) {
	synctest.Run(f)
}

func synctestWait_LetAllOtherGoroFinish() {
	synctest.Wait()
}
