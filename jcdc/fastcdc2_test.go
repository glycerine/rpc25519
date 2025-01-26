package jcdc

import (
	"fmt"
	mathrand2 "math/rand/v2"
	"os"
	"testing"
	"time"

	cv "github.com/glycerine/goconvey/convey"
)

func Test301_fastcdc2_should_not_infinite_loop(t *testing.T) {

	cv.Convey("saw an infinite loop in fastcdc2 on all 0s. must fix before it is usable. Update: was just slow under -race. It does actually finish.", t, func() {

		//host := ""
		path := "test300.dat"

		// create a test file
		// N := 1000  28sec. does finish, just slow.
		N := 10 // 2.8 sec for 100; 14sec for 500; its working just slower than expected, even with race detector off.
		testfd, err := os.Create(path)
		panicOn(err)
		slc := make([]byte, 1<<20) // 1 MB slice

		// deterministic pseudo-random numbers as data.
		var seed [32]byte
		seed[1] = 2
		generator := mathrand2.NewChaCha8(seed)

		// random or zeros?
		allZeros := true
		if allZeros {
			// slc is already ready with all 0.
		} else {
			generator.Read(slc)
		}
		for range N {
			_, err = testfd.Write(slc)
			panicOn(err)
		}
		testfd.Close()
		//vv("created N = %v MB test file in remotePath='%v'.", N, path)

		data, err := os.ReadFile(path)
		panicOn(err)

		// my take on the Stadia improved version of FastCDC
		opts := &CDC_Config{
			MinSize:    4 * 1024,
			TargetSize: 60 * 1024,
			MaxSize:    80 * 1024,
		}
		cdc := NewFastCDC_Stadia(opts)

		fin := make(chan bool)
		go func() {
			const dur = time.Second * 2
			select {
			case <-fin:
				return
			case <-time.After(dur):
				panic(fmt.Sprintf("Cutpoints has not finished after %v", dur))
			}
		}()

		// this inf looping on all zeros
		cuts := cdc.Cutpoints(data, 0)
		fmt.Printf("len(cuts) = %v\n", len(cuts))
		close(fin)
	})
}
