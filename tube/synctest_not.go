//go:build !goexperiment.synctest

package tube

import (
	//"fmt"
	"testing"
)

const faketime bool = false

//func init() {
//	fmt.Printf("faketime = %v\n", faketime)
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
