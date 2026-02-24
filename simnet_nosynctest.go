//go:build !synctest

package rpc25519

import (
	"fmt"
	"testing"
)

const faketime bool = false

var _ = fmt.Printf

//func init() {
//	fmt.Printf("rpc faketime = %v\n", faketime)
//}

func synctestWait_LetAllOtherGoroFinish() {}

func bubbleOrNot(t *testing.T, f func(t *testing.T)) {
	f(t)
}

func onlyBubbled(t *testing.T, f func(t *testing.T)) {
	if raceDetectorOn {
		return // the print below is racey inside the testing package.
	}
	t.Skip("onlyBubbled: skipping test")
}

func (s *Simnet) assertGoroAlone() {}

func goID() int {
	return GoroNumber()
}
