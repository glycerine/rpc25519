package rpc25519

// build/run with:
// GOTRACEBACK=all GOEXPERIMENT=synctest go test -v

import (
	"fmt"
	"math"
	"testing"
	"time"
)

func Test1001_simnetonly_drop_prob(t *testing.T) {

	cliDrops := true
	for j := range 1 {
		if j > 0 {
			cliDrops = false
		}
		// see that probability of deaf read matches
		// our setting, but running 10K messages through
		onlyBubbled(t, func() {
			// simnet with probabilistic deaf fault on server or client experiences the set level of send and/or read flakiness

			nmsg := 10
			simt, cfg := newSimnetTest(t, "test1001")
			cli, srv, simnet, srvname, cliname := setupSimnetTest(simt, cfg)
			defer srv.Close()
			defer cli.Close()
			defer simnet.Close()
			_, _, _ = simnet, srvname, cliname

			serviceName := "customEcho"
			srv.Register2Func(serviceName, customEcho)

			dropPct := 0.5
			var undoIsolated func()
			_ = undoIsolated
			//vv("before clientDropsSends(%v): %v", dropPct, simnet.GetSimnetSnapshot())
			///undoIsolated := simt.clientDropsSends(dropPct)
			if cliDrops {
				undoIsolated = simt.clientDropsSends(dropPct)
			} else {
				undoIsolated = simt.serverDropsSends(dropPct)
			}
			//vv("after clientDropsSends(%v): %v", dropPct, simnet.GetSimnetSnapshot())
			//vv("after clientDropsSends(%v): %v", dropPct, simnet.GetSimnetSnapshot().ShortString())
			got, goterr := 0, 0
			waitFor := 200 * time.Millisecond
			for range nmsg {
				req := NewMessage()
				req.HDR.ServiceName = serviceName
				req.JobSerz = []byte("Hello from client!")
				_, err := cli.SendAndGetReply(req, nil, waitFor)
				if err == nil {
					got++
				} else {
					goterr++
					if goterr == 1 {
						//vv("first err = '%v'", err)
					}
				}
			}
			pctDropped := 1 - (float64(got))/float64(nmsg)
			vv("nmsg = %v; got=%v; pctDropped=%0.5f; goterr=%v", nmsg, got, pctDropped, goterr)
			//vv("cli attemptedSend = %v; droppedSendDueToProb = %v", conn.attemptedSend, conn.droppedSendDueToProb)
			diff := math.Abs(pctDropped - dropPct)
			if diff >= 0.05 {
				panic(fmt.Sprintf("diff = %0.5f >= 0.05", diff))
			}
			vv("good, diff = %0.5f < 0.05", diff)
		})
	}
}

func Test1002_simnetonly_deaf_prob_tests(t *testing.T) {
	return
	onlyBubbled(t, func() {
		// simnet with probabilistic deaf fault on server or client experiences the set level of send and/or read flakiness

		nmsg := 1000
		simt, cfg := newSimnetTest(t, "test1002")
		cli, srv, simnet, srvname, cliname := setupSimnetTest(simt, cfg)
		defer srv.Close()
		defer cli.Close()
		defer simnet.Close()
		_, _, _ = simnet, srvname, cliname

		serviceName := "customEcho"
		srv.Register2Func(serviceName, customEcho)

		deafPct := 0.5
		var undoIsolated func()
		_ = undoIsolated
		//vv("before clientDeaf(%v): %v", deafPct, simnet.GetSimnetSnapshot())
		//undoIsolated := simt.clientDeaf(deafPct)

		undoIsolated = simt.clientDeaf(deafPct)
		//vv("after clientDeaf(%v): %v", deafPct, simnet.GetSimnetSnapshot())

		//undoIsolated = simt.serverDeaf(deafPct)
		//vv("after serverDeaf(%v): %v", deafPct, simnet.GetSimnetSnapshot())
		//}
		//vv("after clientDeaf(%v): %v", deafPct, simnet.GetSimnetSnapshot().ShortString())
		got, goterr := 0, 0
		waitFor := 100 * time.Millisecond
		for range nmsg {
			req := NewMessage()
			req.HDR.ServiceName = serviceName
			req.JobSerz = []byte("Hello from client!")
			_, err := cli.SendAndGetReply(req, nil, waitFor)
			if err == nil {
				got++
			} else {
				goterr++
				//if goterr == 1 {
				//vv("goterr %v,  err = '%v': %v", goterr, err, simnet.GetSimnetSnapshot())
				//}
			}
		}
		obsPctDeaf := 1 - (float64(got))/float64(nmsg)
		vv("nmsg = %v; got=%v; obsPctDeaf=%0.5f; goterr=%v; requested/expected deafPct:%0.5f", nmsg, got, obsPctDeaf, goterr, deafPct)

		diff := math.Abs(obsPctDeaf - deafPct)
		if diff >= 0.05 {
			panic(fmt.Sprintf("diff = %0.5f >= 0.05", diff))
		}
		vv("good, diff = %0.5f < 0.05", diff)
	})
}
