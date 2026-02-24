//go:build synctest

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

func bubbleOrNot(t *testing.T, f func(t *testing.T)) {
	synctest.Test(t, f)
}

func onlyBubbled(t *testing.T, f func(t *testing.T)) {
	synctest.Test(t, f)
}

func synctestWait_LetAllOtherGoroFinish() {
	synctest.Wait()
}
