//go:build goexperiment.synctest

package rpc25519

import (
	"fmt"
	//"runtime/debug"
	//"sync"
	"testing"
	"testing/synctest"
	"time"
)

const faketime bool = true

func init() {
	fmt.Printf("faketime = %v\n", faketime)
}

func bubbleOrNot(f func()) {
	synctest.Run(f)
}

func onlyBubbled(t *testing.T, f func()) {
	synctest.Run(f)
}

func synctestWait_LetAllOtherGoroFinish() {
	synctest.Wait()
}

// ideally we want every client goro to run
// a single step after scheduling. Assert to
// see how well we've acheived this goal.
func (s *simnet) assertGoroAlone() {
	s.singleGoroMut.Lock()
	defer s.singleGoroMut.Unlock()

	now := time.Now()
	me := GoroNumber()

	if now.Equal(s.singleGoroTm) {
		if s.singleGoroID != me {
			panic(fmt.Sprintf(
				"%v Wish I were alone, but I'm(%v) "+
					"with stupid: %v",
				now, me, s.singleGoroID))
		}
	} else {
		s.singleGoroTm = now
		s.singleGoroID = me
	}
}
