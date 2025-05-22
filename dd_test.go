package rpc25519

// build/run with:
// GOTRACEBACK=all GOEXPERIMENT=synctest go test -v

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	cv "github.com/glycerine/goconvey/convey"
)

var _ = strings.HasPrefix

func Test1001_simnetonly_drop_deaf_tests(t *testing.T) {

	// see that probability of deaf read matches
	// our setting, but running 10K messages through
	onlyBubbled(t, func() {
		cv.Convey("simnet with probabilistic deaf fault on server or client experiences the set level of send and/or read flakiness", t, func() {

			nmsg := 1000
			simt, cfg := newSimnetTest(t, "test1001")
			cli, srv, simnet, srvname, cliname := setupSimnetTest(simt, cfg)
			defer srv.Close()
			defer cli.Close()
			_, _, _ = simnet, srvname, cliname

			serviceName := "customEcho"
			srv.Register2Func(serviceName, customEcho)

			dropPct := 0.1
			cliDrops := true
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

			/*
				stat := simnet.GetSimnetSnapshot()

				sps := stat.Peermap[srvname]
				sconn := sps.Connmap[srvname]
				cconn := stat.LoneCli[cliname].Conn[0]

				//vv("stat.Peermap = '%v'; cconn = '%v", stat.Peermap, cconn)

				// verify client is got ~ dropPct through
				ndrop := cconn.DroppedSendQ.Len()
				x := nmsg - got
				if ndrop != x {
					panic(fmt.Sprintf("expected cli ndrop(%v)== x(%v) == nmsg(%v) - got(%v)", ndrop, x, nmsg, got))
				} else {
					vv("good, saw cli ndrop(%v) == x(%v)", ndrop, x)
				}

				if false {
					ndrop = sconn.DroppedSendQ.Len()
					if ndrop > 0 {
						panic(fmt.Sprintf("expected srv ndrop(%v) == 0", ndrop))
					} else {
						vv("good, saw srv ndrop(%v) == 0", ndrop)
					}
					//vv("err = '%v'; reply = %p", err, reply)

					vv("srv still isolated, network after cli tried to send echo request: %v", simnet.GetSimnetSnapshot())

					// repair the network
					undoIsolated()

					vv("after srv repaired, re-attempt cli call with: %v", simnet.GetSimnetSnapshot())

					req2 := NewMessage()
					req2.HDR.ServiceName = serviceName
					req2.JobSerz = []byte("Hello from client! 2nd time.")

					reply2, err := cli.SendAndGetReply(req2, nil, waitFor)
					panicOn(err)
					want := string(req2.JobSerz)
					gotit := strings.HasPrefix(string(reply2.JobSerz), want)
					if !gotit {
						t.Fatalf("expected JobSerz to start with '%v' but got '%v'", want, string(reply2.JobSerz))
					}
				}
			*/
		})
	})
}
