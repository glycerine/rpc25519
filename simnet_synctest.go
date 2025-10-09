//go:build goexperiment.synctest && !goj

package rpc25519

import (
	//"fmt"
	//"runtime/debug"
	//"sync"
	"testing"
	"testing/synctest"
	"time"
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

// will not durably block, b/c not created within bubble!
//var barrierExclusiveCh chan bool = make(chan bool, 1)

var barrierExclusiveCh *chan bool

func synctestWait_LetAllOtherGoroFinish() {
	// have to use channel as a mutex, because
	// actual sync.Mutex are not durably blocking in synctest.
	//vv("about to ask for exclusive access to synctest barrier synctestWait_LetAllOtherGoroFinish")

	select {
	case *barrierExclusiveCh <- true:
	}
	time.Sleep(0)
	synctest.Wait()

	select {
	case <-*barrierExclusiveCh:
	}

	//vv("released exclusive access to synctest barrier synctestWait_LetAllOtherGoroFinish")
}

// ideally we want every client goro to run
// a single step after scheduling. Assert to
// see how well we've acheived this goal.
func (s *Simnet) assertGoroAlone() {
	s.singleGoroMut.Lock()
	defer s.singleGoroMut.Unlock()

	now := time.Now()
	me := GoroNumber()

	if now.Equal(s.singleGoroTm) {
		if s.singleGoroID != me {
			//panic(fmt.Sprintf(
			vv("%v Wish I were alone, but I'm(%v) "+
				"with stupid: %v",
				now, me, s.singleGoroID)
		}
	} else {
		s.singleGoroTm = now
		s.singleGoroID = me
	}
}

func goID() int {
	return GoroNumber()
}
