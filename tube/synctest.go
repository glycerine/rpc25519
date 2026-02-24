//go:build synctest

package tube

import (
	"fmt"
	"testing"
	"testing/synctest"
)

var _ = fmt.Printf

// for go1.24:
// GOTRACEBACK=all GOEXPERIMENT=synctest go test -v
//
// for go1.25:
// GOEXPERIMENT=synctest go test -v
//
// for go1.26 (since the GOEXPERIMENT=synctest tag went away),
// we use our own "synctest" build tag.
// go test -v -tags=synctest

const faketime bool = true

//func init() {
//	fmt.Printf("tube faketime = %v\n", faketime)
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
