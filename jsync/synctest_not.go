//go:build !goexperiment.synctest

package jsync

import (
	"fmt"
	"testing"
)

const faketime bool = false

func init() {
	fmt.Printf("faketime = %v\n", faketime)
}

func synctestWait_LetAllOtherGoroFinish() {}

func bubbleOrNot(f func()) {
	f()
}

func onlyBubbled(t *testing.T, f func()) {
	t.Skip("onlyBubbled: skipping test")
}
