//go:build !goexperiment.synctest

package rpc25519

import (
	//"fmt"
	"testing"
)

const faketime bool = false

//func init() {
//fmt.Printf("faketime = %v\n", faketime)
//}

func synctestWait_LetAllOtherGoroFinish() {}

func bubbleOrNot(t *testing.T, f func(t *testing.T)) {
	f(t)
}

func onlyBubbled(t *testing.T, f func()) {
	t.Skip("onlyBubbled: skipping test")
}

func (s *Simnet) assertGoroAlone() {}

func goID() int {
	return GoroNumber()
}
