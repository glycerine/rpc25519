package jsync

import (
	//"bytes"
	//cryrand "crypto/rand"
	//"fmt"
	//"io"
	//mathrand2 "math/rand/v2"
	//"os"
	"testing"
	//"time"
	//cv "github.com/glycerine/goconvey/convey"
	//rpc "github.com/glycerine/rpc25519"
	//"github.com/glycerine/rpc25519/hash"
)

func Test_Benchmark_parallel_blake3(t *testing.T) {

	path := "../Ubuntu_24.04_VB_LinuxVMImages.COM.vdi"

	/*	k := 10
		var err error
		res := make([]string, k)

		for i := range k {
			t0 := time.Now()
			res[i], err = SlurpBlake(path, i)
			panicOn(err)
			vv("parallel %v took '%v' : %v", i, time.Since(t0), res[i])
		}
	*/

	sum, err := SlurpBlake(path, 8)
	panicOn(err)
	_ = sum
}
