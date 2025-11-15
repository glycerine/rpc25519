//go:build goexperiment.synctest

package rpc25519

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func Test800_SimNet_all_timers_dur_0_fire_now(t *testing.T) {

	if !faketime {
		t.Skip("test only for synctest.") // see also build tag at top.
		return
	}

	bubbleOrNot(t, func(t *testing.T) {
		// "SimNet using synctest depends on all the times set to duration 0/now firing before we quiese to durable blocking. verify this assumption under synctest. yes: note the Go runtime implementation does a select with a default: so it will discard the timer alert rather than block. Update: arg. no, the runtime does a special thing where it does not execute that select until it has a goro ready to accept it, so it always suceeds, I think.

		t0 := time.Now()
		//vv("start test800")
		var timers []*time.Timer
		N := 10
		order := make(map[*time.Timer]int)
		for range N {
			ti := time.NewTimer(0)
			order[ti] = len(timers)
			timers = append(timers, ti)
		}

		var cases []reflect.SelectCase
		for _, ti := range timers {
			cases = append(cases,
				reflect.SelectCase{
					Dir:  reflect.SelectRecv,
					Chan: reflect.ValueOf(ti.C),
				})
		}
		got := 0
		for i, ti := range timers {
			_ = ti
			_ = i
			//  <-ti.C
			chosen, recvVal, recvOK := reflect.Select(cases)
			_, _ = chosen, recvVal
			if !recvOK {
				panic("why not recvOK ?")
			}
			//vv("on i=%v, chosen=%v, timer %v: %v", i, chosen, order[ti], recvVal)
			got++
		}
		now := time.Now()
		if faketime {
			if !t0.Equal(now) {
				t.Fatalf("we have a problem, Houston. t0=%v, but now=%v", t0, now)
			}
			//vv("got %v timers firing now", got)
			if got != N {
				t.Fatalf("expected all N=%v timers to fire, not %v", N, got)
			}
		}
		//vv("end test800") // shows same time as start, good.
	})
}

func Test801_synctestonly_time_advances_in_order(t *testing.T) {

	onlyBubbled(t, func(t *testing.T) {

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
		req.HDR.ToServiceName = serviceName
		req.HDR.FromServiceName = serviceName
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

		done := make([]chan bool, 3)
		for i := range done {
			done[i] = make(chan bool)
		}
		simnet := cfg.GetSimnet()
		defer simnet.Close()

		go func() {
			name := "100 usec"
			simnet.NewGoro(name)
			time.Sleep(100 * time.Microsecond)
			vv("%v done", name)
			close(done[0])
		}()

		go func() {
			name := "200 usec"
			simnet.NewGoro(name)
			time.Sleep(200 * time.Microsecond)
			vv("%v done", name)
			close(done[1])
		}()

		go func() {
			name := "300 usec"
			simnet.NewGoro(name)
			time.Sleep(300 * time.Microsecond)
			vv("%v done", name)
			close(done[2])
		}()

		for i := range done {
			<-done[i]
		}
	})
}
