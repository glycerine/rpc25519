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

func bubbleOrNot(f func()) {
	synctest.Run(f)
}

func onlyBubbled(t *testing.T, f func()) {
	synctest.Run(f)
}
