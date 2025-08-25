//go:build goexperiment.synctest

package jsync

import (
	//"fmt"
	"testing"
	"testing/synctest"
)

const faketime bool = true

//func init() {
//	fmt.Printf("faketime = %v\n", faketime)
//}

func bubbleOrNot(f func()) {
	synctest.Run(f)
}

func onlyBubbled(t *testing.T, f func()) {
	synctest.Run(f)
}

func synctestWait_LetAllOtherGoroFinish() {
	synctest.Wait()
}
