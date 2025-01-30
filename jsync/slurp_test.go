package jsync

import (
	//"bytes"
	//cryrand "crypto/rand"
	"fmt"
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

	for i := 20; i < 49; i++ {
		sum, elap, err := SlurpBlake(path, i)
		panicOn(err)

		fmt.Printf("with parallelism %v  elap = %v\n", i, elap)
		_ = sum
	}
}

/*
with parallelism 1  elap = 3.36196772s
with parallelism 2  elap = 1.820411611s
with parallelism 3  elap = 1.295132893s
with parallelism 4  elap = 995.889634ms
with parallelism 5  elap = 790.343426ms
with parallelism 6  elap = 662.980814ms
with parallelism 7  elap = 571.291072ms
with parallelism 8  elap = 488.19399ms
with parallelism 9  elap = 434.396548ms
with parallelism 10  elap = 383.822437ms
with parallelism 11  elap = 350.941816ms
with parallelism 12  elap = 324.111351ms
with parallelism 13  elap = 295.793873ms
with parallelism 14  elap = 277.148784ms
with parallelism 15  elap = 264.995577ms
with parallelism 16  elap = 243.857875ms
with parallelism 17  elap = 227.028452ms
with parallelism 18  elap = 217.816647ms
with parallelism 19  elap = 206.177476ms
with parallelism 20  elap = 219.855418ms
with parallelism 21  elap = 196.451325ms
with parallelism 22  elap = 193.754728ms
with parallelism 23  elap = 189.802725ms
with parallelism 24  elap = 189.571638ms
with parallelism 25  elap = 189.59453ms
with parallelism 26  elap = 184.796805ms
with parallelism 27  elap = 181.522148ms
with parallelism 28  elap = 177.600565ms
with parallelism 29  elap = 176.89132ms
with parallelism 30  elap = 174.520318ms
with parallelism 31  elap = 171.302762ms
with parallelism 32  elap = 173.791357ms
with parallelism 33  elap = 176.197592ms
with parallelism 34  elap = 173.827144ms
with parallelism 35  elap = 169.918114ms
with parallelism 36  elap = 169.302686ms
with parallelism 37  elap = 168.265908ms
with parallelism 38  elap = 166.106699ms
with parallelism 39  elap = 170.492584ms
with parallelism 40  elap = 169.429297ms
with parallelism 41  elap = 166.717038ms
with parallelism 42  elap = 164.211222ms <<< minimum
with parallelism 43  elap = 165.209907ms
with parallelism 44  elap = 170.713475ms
with parallelism 45  elap = 173.594672ms
with parallelism 46  elap = 162.950339ms
with parallelism 47  elap = 171.122229ms

*/
