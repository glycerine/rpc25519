//go:build goexperiment.synctest

package rpc25519

import (
	"sync"
	"testing/synctest"
	"time"
)

func bubblesOrNot(f func()) {
	synctest.Run(f)
}

const globalUseSynctest bool = true

var waitMut sync.Mutex

var waitCond *sync.Cond = sync.NewCond(&waitMut)
var waitBegan time.Time
var waitEnded time.Time

func waitInBubble() {
	synctest.Wait()
}

func waitInBubble2() {

	waitMut.Lock()
	if waitBegan.After(waitEnded) && waitBegan == time.Now() {
		waitCond.Wait() // unlock waitMut and suspend until Broadcast
	} else {
		// we are first
		waitBegan = time.Now()
		vv("first to arrive set waitBegan = '%v' and about to synctest.Wait()", waitBegan)
		synctest.Wait()
		waitEnded = time.Now()
		vv("after synctest.Wait, first to arrive set waitEnded = '%v' and about to synctest.Wait()", waitEnded)

		waitCond.Broadcast()
		waitMut.Unlock()
	}
}

//func synctestRun(f func()) {
//	synctest.Run(f)
//}
