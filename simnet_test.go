package rpc25519

import (
	//"context"
	//"encoding/base64"
	//"fmt"
	//"os"
	//"path/filepath"
	//"strings"
	"testing"
	"testing/synctest"
	"time"

	cv "github.com/glycerine/goconvey/convey"
)

func Test800_SimNet_all_timers_dur_0_fire_now(t *testing.T) {

	cv.Convey("SimNet depends on all the times set to duration 0/now firing before we quiese to durable blocking. verify this assumption.", t, func() {
		synctest.Run(func() {
			t0 := time.Now()
			//vv("start test800")
			var timers []*time.Timer
			N := 10
			for range N {
				timers = append(timers, time.NewTimer(0))
			}
			for _, ti := range timers {
				<-ti.C
			}
			if !t0.Equal(time.Now()) {
				t.Fatalf("we have a problem, Houston.")
			}
			//vv("end test800") // shows same time as start, good.
		})
	})
}
