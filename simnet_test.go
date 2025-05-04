package rpc25519

import (
	//"context"
	//"encoding/base64"
	//"fmt"
	//"os"
	//"path/filepath"
	"strings"
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

func Test801_RoundTrip_SendAndGetReply_SimNet(t *testing.T) {

	cv.Convey("basic SimNet channel based remote procedure call with rpc25519: register a callback on the server, and have the client call it.", t, func() {

		synctest.Run(func() {

			cfg := NewConfig()
			cfg.UseSimNet = true

			cfg.ServerAddr = "127.0.0.1:0"
			srv := NewServer("srv_test801", cfg)

			serverAddr, err := srv.Start()
			panicOn(err)
			defer srv.Close()

			//vv("(SimNet) server Start() returned serverAddr = '%v'", serverAddr)

			serviceName := "customEcho"
			srv.Register2Func(serviceName, customEcho)

			cfg.ClientDialToHostPort = serverAddr.String()
			cli, err := NewClient("test801", cfg)
			panicOn(err)
			err = cli.Start()
			panicOn(err)

			defer cli.Close()

			req := NewMessage()
			req.HDR.ServiceName = serviceName
			req.JobSerz = []byte("Hello from client!")

			reply, err := cli.SendAndGetReply(req, nil, 0)
			panicOn(err)

			//vv("reply = %p", reply)
			//vv("server sees reply (Seqno=%v) = '%v'", reply.HDR.Seqno, string(reply.JobSerz))
			want := "Hello from client!"
			gotit := strings.HasPrefix(string(reply.JobSerz), want)
			if !gotit {
				t.Fatalf("expected JobSerz to start with '%v' but got '%v'", want, string(reply.JobSerz))
			}

			// set a timer
			t0 := time.Now()
			goalWait := 3 * time.Second
			timerC, err := cli.TimeAfter(goalWait)
			panicOn(err)
			t1 := <-timerC
			elap := time.Since(t0)
			if elap < goalWait {
				t.Fatalf("timer went off too early! elap(%v) < goalWait(%v)", elap, goalWait)
			}
			vv("good: finished timer (fired at %v) after %v >= goal %v", t1, elap, goalWait)
		})
	})
}

func Test804_SimNet_rng_hops(t *testing.T) {
	// rng should respect minHop, maxHop,
	// and the tie breaker should return -1 or 1

	var tick, minHop, maxHop time.Duration
	var seed [32]byte

	minHop = time.Second
	maxHop = time.Second
	s := newScenario(tick, minHop, maxHop, seed)

	var yes, no float64
	N := float64(100_000)
	for range int(N) {
		hop := s.rngHop()
		if got, want := hop, time.Second; got != want {
			t.Fatalf("want %v, but got %v", want, got)
		}
		tie := s.rngTieBreaker()
		ok := tie == -1 || tie == 1
		if !ok {
			t.Fatalf("want +/-1, but got %v", tie)
		}
		if tie == 1 {
			yes++
		} else {
			no++
		}
	}
	// tie breaker should be fair
	if yes < 0.45*N || yes > 0.55*N {
		t.Fatalf("tie breaker not a fair coin. yes rate = '%v'", yes/N)
	}
	// implied, but verify our test too...
	if no < 0.45*N || no > 0.55*N {
		t.Fatalf("tie breaker not a fair coin. no rate = '%v'", no/N)
	}

	minHop = time.Second
	maxHop = 2 * time.Second
	s = newScenario(tick, minHop, maxHop, seed)

	for range 1000 {
		hop := s.rngHop()
		if hop < minHop || hop > maxHop {
			t.Fatalf("got %v out of bounds [%v, %v]", hop, minHop, maxHop)
		}
	}
}
