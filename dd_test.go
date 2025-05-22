package rpc25519

// build/run with:
// GOTRACEBACK=all GOEXPERIMENT=synctest go test -v

import (
	"fmt"
	"math"
	"testing"
	"time"
)

func Test1001_simnetonly_drop_deaf_tests(t *testing.T) {

	cliDrops := true
	for j := range 2 {
		if j > 0 {
			cliDrops = false
		}
		// see that probability of deaf read matches
		// our setting, but running 10K messages through
		onlyBubbled(t, func() {
			// simnet with probabilistic deaf fault on server or client experiences the set level of send and/or read flakiness

			nmsg := 1000
			simt, cfg := newSimnetTest(t, "test1001")
			cli, srv, simnet, srvname, cliname := setupSimnetTest(simt, cfg)
			defer srv.Close()
			defer cli.Close()
			_, _, _ = simnet, srvname, cliname

			serviceName := "customEcho"
			srv.Register2Func(serviceName, customEcho)

			dropPct := 0.1
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
					if goterr == 1 {
						vv("first err = '%v'", err)
					}
				}
			}
			pctDropped := 1 - (float64(got))/float64(nmsg)
			vv("nmsg = %v; got=%v; pctDropped=%0.5f; goterr=%v", nmsg, got, pctDropped, goterr)
			diff := math.Abs(pctDropped - dropPct)
			if diff >= 0.05 {
				panic(fmt.Sprintf("diff = %0.5f >= 0.05", diff))
			}
			vv("good, diff = %0.5f < 0.05", diff)
		})
	}
}
