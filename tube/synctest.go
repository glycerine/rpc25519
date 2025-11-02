//go:build goexperiment.synctest

package tube

import (
	//"fmt"
	"testing"
	"testing/synctest"
)

// GOTRACEBACK=all GOEXPERIMENT=synctest go test -v

const faketime bool = true

//func init() {
//	fmt.Printf("faketime = %v\n", faketime)
//}

func synctestWait_LetAllOtherGoroFinish() {
	synctest.Wait()
}

func bubbleOrNot(t *testing.T, f func(t *testing.T)) {
	synctest.Test(t, f)
}

func onlyBubbled(t *testing.T, f func(t *testing.T)) {
	synctest.Test(t, f)
}
