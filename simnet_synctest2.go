//go:build goexperiment.synctest && goj

package rpc25519

import (
	"fmt"
	//"runtime/debug"
	//"sync"
	"runtime"
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
func (s *Simnet) assertGoroAlone() {
	vv("top assertGoroAlone()")
	s.singleGoroMut.Lock()
	defer s.singleGoroMut.Unlock()

	total, running, active := synctest.RunningCount()
	vv("total=%v  running=%v  active = %v", total, running, active)

	now := time.Now()
	me := GoroNumber()

	if running > 1 {
		vv("who else is running with me (%v)? allstacks=\n%v", me, allstacks())
	}

	if now.Equal(s.singleGoroTm) {
		if s.singleGoroID != me {
			//vv("arg! assertGoroAlone: s.singleGoroID(%v) != me(%v)... panic-ing...", s.singleGoroID, me)
			//panic(fmt.Sprintf(
			vv("%v Wish I were alone, but I'm(%v) "+
				"with stupid: %v",
				now, me, s.singleGoroID)
		}
	} else {
		s.singleGoroTm = now
		s.singleGoroID = me
	}
	vv("end assertGoroAlone()")
}

func goID() int {
	return runtime.GoID()
}
