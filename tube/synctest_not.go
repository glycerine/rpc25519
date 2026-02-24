//go:build !synctest

package tube

import (
	"fmt"
	"testing"
)

var _ = fmt.Printf

const faketime bool = false

//func init() {
//	fmt.Printf("tube faketime = %v\n", faketime)
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
