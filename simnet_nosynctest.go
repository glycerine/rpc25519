//go:build !goexperiment.synctest

package rpc25519

const globalUseSynctest bool = false

func synctestWait() {}

//func synctestRun(f func()) {
//	f()
//}

func bubblesOrNot(f func()) {
	f()
}
