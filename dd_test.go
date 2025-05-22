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

func Test1001_simnetonly_drop_deaf_tests(t *testing.T) {

	// see that probability of deaf read matches
	// our setting, but running 10K messages through
	onlyBubbled(t, func() {
		cv.Convey("simnet with probabilistic deaf fault on server or client experiences the set level of send and/or read flakiness", t, func() {

			nmsg := 100
			simt, cfg := newSimnetTest(t, "test1001")
			cli, srv, simnet, srvname, cliname := setupSimnetTest(simt, cfg)
			defer srv.Close()
			defer cli.Close()

			serviceName := "customEcho"
			srv.Register2Func(serviceName, customEcho)

			dropPct := 0.5
			//vv("before clientDropsSends(0.5)", simnet.GetSimnetSnapshot())
			///undoIsolated := simt.clientDropsSends(0.5)
			undoIsolated := simt.clientDropsSends(dropPct)
			//vv("after clientDropsSends(0.5): %v", simnet.GetSimnetSnapshot())
			//vv("after clientDropsSends(0.5): %v", simnet.GetSimnetSnapshot().ShortString())
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
						vv("first err = '%v'", err)
					}
				}
			}
			pctGot := float64(got) / float64(nmsg)
			vv("nmsg = %v; got=%v; pctGot=%v; goterr=%v", nmsg, got, pctGot, goterr)
			diff := math.Abs(pctGot - dropPct)
			if diff >= 0.05 {
				panic(fmt.Sprintf("diff = %v >= 0.05", diff))
			}
			vv("good, diff = %v < 0.05", diff)

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
		})
	})
}
