//go:build !goexperiment.synctest

package rpc25519

const globalUseSynctest bool = false

func synctestWait_LetAllOtherGoroFinish() {}

//func synctestRun(f func()) {
//	f()
//}

func bubbleOrNot(f func()) {
	f()
}
