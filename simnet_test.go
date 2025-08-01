package rpc25519

// build/run with:
// GOTRACEBACK=all GOEXPERIMENT=synctest go test -v

import (
	//"context"
	//"encoding/base64"
	"bufio"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"sync"
	//"path/filepath"
	"context"
	"strings"
	"testing"
	"time"

	cv "github.com/glycerine/goconvey/convey"
)

func Test701_simnetonly_RoundTrip_SendAndGetReply_SimNet(t *testing.T) {

	onlyBubbled(t, func() { // fast
		//bubbleOrNot(func() { // slow, also :85 assertion incorrect from realtime
		//cv.Convey("basic SimNet channel based remote procedure call with rpc25519: register a callback on the server, and have the client call it.", t, func() {

		cfg := NewConfig()
		cfg.UseSimNet = true

		cfg.ServerAddr = "127.0.0.1:0"
		srv := NewServer("srv_test701", cfg)

		vv("about to srv.Start() in 701")
		t0 := time.Now()
		serverAddr, err := srv.Start()
		vv("back from srv.Start() in 701, elap = %v", time.Since(t0))

		panicOn(err)
		defer srv.Close()

		//vv("(SimNet) server Start() returned serverAddr = '%v'", serverAddr)

		// if the server is partitioned, the client should not
		// be able to make a call.
		simnet := cfg.GetSimnet()
		if simnet == nil {
			panic("no simnet??")
		}
		defer simnet.Close() // let shutdown happen.

		serviceName := "customEcho"
		srv.Register2Func(serviceName, customEcho)

		cfg.ClientDialToHostPort = serverAddr.String()
		cli, err := NewClient("cli_test701", cfg)
		panicOn(err)
		err = cli.Start()
		panicOn(err)

		defer cli.Close()

		req := NewMessage()
		req.HDR.ToServiceName = serviceName
		req.HDR.FromServiceName = serviceName // symmetric, right?
		req.JobSerz = []byte("Hello from client!")

		vv("about to call cli.SendAndGetReply") // seen at 00:00:00.000200023
		reply, err := cli.SendAndGetReply(req, nil, 0)
		vv("back from cli.SendAndGetReply; reply = %p; err='%v'", reply, err) // arg. timeout after 20s; we were blocked until 00:00:20.00040002. why?? no match-making happened, why not?
		// err is normal on shutdown...
		panicOn(err)

		vv("reply = %p", reply)
		if reply == nil {
			vv("arg. reply == nil. probably a panic error somewhere. bail.")
			panic("reply was nil")
			return
		} else {
			vv("cli sees reply (Seqno=%v) = '%v'", reply.HDR.Seqno, string(reply.JobSerz))
		}
		want := "Hello from client!"
		gotit := strings.HasPrefix(string(reply.JobSerz), want)
		if !gotit {
			t.Fatalf("expected JobSerz to start with '%v' but got '%v'", want, string(reply.JobSerz))
		}

		// set a timer
		t0 = time.Now()
		// default hop now 5 sec, so better be > 5 or deadlock in synctest.
		goalWait := 20 * time.Second
		timeout := cli.NewTimer(goalWait)

		//timerC := cli.TimeAfter(goalWait)
		t1 := <-timeout.C
		elap := time.Since(t0)
		timeout.Discard()
		if elap < goalWait {
			t.Fatalf("timer went off too early! elap(%v) < goalWait(%v)", elap, goalWait)
		}
		vv("good: finished timer (fired at %v) after %v >= goal %v", t1, elap, goalWait)

		dd := DropDeafSpec{
			UpdateDeafReads:  true,
			UpdateDropSends:  true,
			DeafReadsNewProb: 1,
			DropSendsNewProb: 1,
		}
		const deliverDroppedSends_NO = false
		err = simnet.FaultHost(srv.simnode.name, dd, deliverDroppedSends_NO)
		panicOn(err)
		vv("server partitioned, try cli call again. net: %v", simnet.GetSimnetSnapshot())

		waitFor := time.Second * 10

		// NOTE! must not re-use the req Message above!
		// its reply ready DoneCh is already closed, and
		// the nextOrReply field has the reply cached.
		// We won't even see any network activity if
		// we send the exact same req again.
		req2 := NewMessage()
		req2.HDR.ToServiceName = serviceName
		req2.HDR.FromServiceName = serviceName // symmetric, right?
		req2.JobSerz = []byte("Hello from client! 2nd time.")

		reply2, err := cli.SendAndGetReply(req2, nil, waitFor)
		// should hang here until timeout.

		if reply2 != nil {
			vv("reply2 = %p", reply2)
			vv("cli sees reply AFTER partition: (Seqno=%v) = '%v'", reply2.HDR.Seqno, string(reply2.JobSerz))
		}
		if err == nil {
			t.Fatalf("error: wanted timeout! server not partitioned??")
		} else {
			vv("good! got err = '%v'", err)
		}

		const deliverDroppedSends_YES = true
		const powerOnIfOff_YES = true
		// now reverse the fault, and get the third attempt through.
		err = simnet.AllHealthy(powerOnIfOff_YES, deliverDroppedSends_YES)
		panicOn(err)
		//vv("server un-partitioned, try cli call 3rd time.")
		vv("server un-partitioned, try cli call 3rd time. net: %v", simnet.GetSimnetSnapshot())
		req3 := NewMessage()
		req3.HDR.ToServiceName = serviceName
		req3.HDR.FromServiceName = serviceName
		req3.JobSerz = []byte("Hello from client! 3rd time.")

		reply3, err := cli.SendAndGetReply(req3, nil, waitFor)
		panicOn(err)
		want = "Hello from client! 3rd time."
		gotit = strings.HasPrefix(string(reply3.JobSerz), want)
		if !gotit {
			t.Fatalf("expected JobSerz to start with '%v' but got '%v'", want, string(reply3.JobSerz))
		}

		//})
	})
}

func Test604_rng_hops(t *testing.T) {
	// rng should respect minHop, maxHop,
	// and the tie breaker should return -1 or 1

	var tick, minHop, maxHop time.Duration
	var seed [32]byte

	minHop = time.Second
	maxHop = time.Second
	s := NewScenario(tick, minHop, maxHop, seed)

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
	s = NewScenario(tick, minHop, maxHop, seed)

	for range 1000 {
		hop := s.rngHop()
		if hop < minHop || hop > maxHop {
			t.Fatalf("got %v out of bounds [%v, %v]", hop, minHop, maxHop)
		}
	}
}

// simnet version of cli_test 006
func Test706_simnetonly_RoundTrip_Using_NetRPC(t *testing.T) {

	onlyBubbled(t, func() {

		// basic SimNet with rpc25519 using the net/rpc API: register a callback on the server, and have the client call it.
		cfg := NewConfig()
		//orig cfg.TCPonly_no_TLS = true
		cfg.UseSimNet = true
		//cfg.ServerSendKeepAlive = time.Second * 10

		path := GetPrivateCertificateAuthDir() + sep + "psk.binary"
		panicOn(setupPSK(path))
		cfg.PreSharedKeyPath = path
		//cfg.ReadTimeout = 2 * time.Second
		//cfg.WriteTimeout = 2 * time.Second

		cfg.ServerAddr = "127.0.0.1:0"
		srv := NewServer("srv_test706", cfg)

		vv("about to srv.Start() in 706")
		t0 := time.Now()
		serverAddr, err := srv.Start()
		vv("back from srv.Start() in 706, elap = %v", time.Since(t0))
		panicOn(err)
		defer srv.Close()

		simnet := srv.cfg.GetSimnet()
		defer simnet.Close()

		//vv("server Start() returned serverAddr = '%v'", serverAddr)

		// net/rpc API on server
		srv.Register(new(Arith))
		srv.Register(new(Embed))
		srv.RegisterName("net.rpc.Arith", new(Arith))
		srv.Register(&BuiltinTypes{})

		cfg.ClientDialToHostPort = serverAddr.String()
		client, err := NewClient("cli_test706", cfg)
		panicOn(err)
		err = client.Start()
		panicOn(err)
		defer client.Close()

		// net/rpc API on client, ported from attic/net_server_test.go
		var args *Args
		_ = args
		var reply *Reply

		// Synchronous calls
		args = &Args{7, 8}
		reply = new(Reply)
		err = client.Call("Arith.Add", args, reply, nil)
		if err != nil {
			t.Errorf("Add: expected no error but got string %q", err.Error())
		}
		if reply.C != args.A+args.B {
			t.Errorf("Add: expected %d got %d", reply.C, args.A+args.B)
		}
		//vv("good 006, got back reply '%#v'", reply)

		// Methods exported from unexported embedded structs
		args = &Args{7, 0}
		reply = new(Reply)
		err = client.Call("Embed.Exported", args, reply, nil)
		if err != nil {
			t.Errorf("Add: expected no error but got string %q", err.Error())
		}
		if reply.C != args.A+args.B {
			t.Errorf("Add: expected %d got %d", reply.C, args.A+args.B)
		}

		// Nonexistent method
		args = &Args{7, 0}
		reply = new(Reply)
		err = client.Call("Arith.BadOperation", args, reply, nil)
		// expect an error
		if err == nil {
			t.Error("BadOperation: expected error")
		} else if !strings.HasPrefix(err.Error(), "rpc: can't find method ") {
			t.Errorf("BadOperation: expected can't find method error; got %q", err)
		}
		//vv("good 006: past nonexistent method")

		// Unknown service
		args = &Args{7, 8}
		reply = new(Reply)
		err = client.Call("Arith.Unknown", args, reply, nil)
		if err == nil {
			t.Error("expected error calling unknown service")
		} else if !strings.Contains(err.Error(), "method") {
			t.Error("expected error about method; got", err)
		}
		//vv("good 006: past unknown service")

		// Out of order.
		args = &Args{7, 8}
		mulReply := new(Reply)
		mulCall := client.Go("Arith.Mul", args, mulReply, nil, nil)
		addReply := new(Reply)
		addCall := client.Go("Arith.Add", args, addReply, nil, nil)

		addCall = <-addCall.Done
		if addCall.Error != nil {
			t.Errorf("Add: expected no error but got string %q", addCall.Error.Error())
		}
		if addReply.C != args.A+args.B {
			t.Errorf("Add: expected %d got %d", addReply.C, args.A+args.B)
		}

		mulCall = <-mulCall.Done
		if mulCall.Error != nil {
			t.Errorf("Mul: expected no error but got string %q", mulCall.Error.Error())
		}
		if mulReply.C != args.A*args.B {
			t.Errorf("Mul: expected %d got %d", mulReply.C, args.A*args.B)
		}
		//vv("good 006: past out of order")

		// Error test
		args = &Args{7, 0}
		reply = new(Reply)
		err = client.Call("Arith.Div", args, reply, nil)
		// expect an error: zero divide
		if err == nil {
			t.Error("Div: expected error")
		} else if err.Error() != "divide by zero" {
			t.Error("Div: expected divide by zero error; got", err)
		}
		//vv("good 006: past error test")

		args = &Args{7, 8}
		reply = new(Reply)
		err = client.Call("Arith.Mul", args, reply, nil)
		if err != nil {
			t.Errorf("Mul: expected no error but got string %q", err.Error())
		}
		if reply.C != args.A*args.B {
			t.Errorf("Mul: expected %d got %d", reply.C, args.A*args.B)
		}
		//vv("good 006: past Arith.Mul test")

		// ServiceName contain "." character
		args = &Args{7, 8}
		reply = new(Reply)
		err = client.Call("net.rpc.Arith.Add", args, reply, nil)
		if err != nil {
			t.Errorf("Add: expected no error but got string %q", err.Error())
		}
		if reply.C != args.A+args.B {
			t.Errorf("Add: expected %d got %d", reply.C, args.A+args.B)
		}
		//vv("good 706: past ServiceName with dot . test")

		//vv("good: end of 706 test")
	})
}

// simnet version of 040 in cli_test.go
func Test740_simnetonly_remote_cancel_by_context(t *testing.T) {

	onlyBubbled(t, func() {

		cv.Convey("simnet remote cancellation", t, func() {

			cfg := NewConfig()
			//cfg.TCPonly_no_TLS = false
			cfg.UseSimNet = true
			//cfg.ServerSendKeepAlive = time.Second * 10

			cfg.ServerAddr = "127.0.0.1:0"
			srv := NewServer("srv_test740", cfg)

			serverAddr, err := srv.Start()
			panicOn(err)
			defer srv.Close()

			simnet := srv.cfg.GetSimnet()
			defer simnet.Close()

			//vv("server Start() returned serverAddr = '%v'", serverAddr)

			// net/rpc API on server
			mustCancelMe := NewMustBeCancelled()
			srv.Register(mustCancelMe)

			// and register early for 741 below
			serviceName741 := "test741_hang_until_cancel"
			srv.Register2Func(serviceName741, mustCancelMe.MessageAPI_HangUntilCancel)

			cfg.ClientDialToHostPort = serverAddr.String()
			client, err := NewClient("cli_test740", cfg)
			panicOn(err)
			err = client.Start()
			panicOn(err)

			defer client.Close()

			// using the net/rpc API
			args := &Args{7, 8}
			reply := new(Reply)

			var cliErr740 error
			cliErrIsSet740 := make(chan bool)
			ctx740, cancelFunc740 := context.WithCancel(context.Background())
			go func() {
				//vv("client.Call() goro top about to call over net/rpc: MustBeCancelled.WillHangUntilCancel()")

				cliErr740 = client.Call("MustBeCancelled.WillHangUntilCancel", args, reply, ctx740)
				//vv("client.Call() returned with cliErr = '%v'", cliErr740)
				close(cliErrIsSet740)
			}()

			// let the call get blocked.
			//vv("cli_test 740: about to block on test740callStarted")
			<-mustCancelMe.callStarted
			//vv("cli_test 740: we got past test740callStarted")

			// cancel it: transmit cancel request to server.
			cancelFunc740()
			//vv("past cancelFunc()")

			<-cliErrIsSet740
			//vv("past cliErrIsSet channel; cliErr740 = '%v'", cliErr740)

			if cliErr740 != ErrCancelReqSent {
				t.Errorf("Test740: expected ErrCancelReqSent but got %v", cliErr740)
			}

			// confirm that server side function is unblocked too
			//vv("about to verify that server side context was cancelled.")
			<-mustCancelMe.callFinished
			//vv("server side saw the cancellation request: confirmed.")

			// use Message []byte oriented API: test 741

			var cliErr741 error
			cliErrIsSet741 := make(chan bool)
			ctx741, cancelFunc741 := context.WithCancel(context.Background())
			req := NewMessage()
			req.HDR.Typ = CallRPC
			req.HDR.ToServiceName = serviceName741
			req.HDR.FromServiceName = serviceName741
			var reply741 *Message

			go func() {
				reply741, cliErr741 = client.SendAndGetReply(req, ctx741.Done(), 0)
				//vv("client.Call() returned with cliErr = '%v'", cliErr741)
				close(cliErrIsSet741)
			}()

			// let the call get blocked on the server (only works under test, of course).
			<-mustCancelMe.callStarted
			//vv("cli_test 741: we got past test741callStarted")

			// cancel it: transmit cancel request to server.
			cancelFunc741()
			//vv("past cancelFunc()")

			<-cliErrIsSet741
			//vv("past cliErrIsSet channel; cliErr = '%v'", cliErr741)

			if cliErr741 != ErrCancelReqSent {
				t.Errorf("Test741: expected ErrCancelReqSent but got %v", cliErr741)
			}

			if reply741 != nil {
				t.Errorf("Test741: expected reply741 to be nil, but got %v", reply741)
			}

			// confirm that server side function is unblocked too
			//vv("about to verify that server side context was cancelled.")
			<-mustCancelMe.callFinished
		})
	})
}

// add simnetonly

func Test745_simnetonly_upload(t *testing.T) {

	onlyBubbled(t, func() {
		cv.Convey("upload a large file in parts from client to server", t, func() {

			cfg := NewConfig()
			//cfg.TCPonly_no_TLS = false
			cfg.UseSimNet = true

			cfg.ServerAddr = "127.0.0.1:0"
			srv := NewServer("srv_test845", cfg)

			serverAddr, err := srv.Start()
			panicOn(err)
			defer srv.Close()

			simnet := srv.cfg.GetSimnet()
			defer simnet.Close()

			//vv("server Start() returned serverAddr = '%v'", serverAddr)

			// name must be "__fileUploader" for cli.go Uploader to work.
			uploaderName := "__fileUploader"
			streamer := NewServerSideUploadState()
			srv.RegisterUploadReaderFunc(uploaderName, streamer.ReceiveFileInParts)

			cfg.ClientDialToHostPort = serverAddr.String()
			client, err := NewClient("test845", cfg)
			panicOn(err)
			err = client.Start()
			panicOn(err)

			defer client.Close()

			// to read the final reply from the server,
			// use strm.ReadCh rather than client.GetReadIncomingCh(),
			// since strm.ReadCh is filtered for our CallID.

			ctx45, cancelFunc45 := context.WithCancel(context.Background())
			defer cancelFunc45()

			// be care not to re-use memory! the client
			// will not make a copy of the message
			// while waiting to send it, so you
			// must allocate new memory to send the next message
			// (and _not_ overwrite the first!)
			req := NewMessage()
			filename := "streams.all.together.txt"
			os.Remove(filename + ".servergot")
			req.HDR.ToServiceName = uploaderName
			req.HDR.FromServiceName = uploaderName
			req.HDR.Args = map[string]string{"readFile": filename}
			req.JobSerz = []byte("a=c(0")

			// start the call
			strm, err := client.UploadBegin(ctx45, uploaderName, req, 0)
			panicOn(err)

			originalStreamCallID := strm.CallID()
			//vv("strm started, with CallID = '%v'", originalStreamCallID)
			// then send N more parts

			var last bool
			N := 20
			for i := 1; i <= N; i++ {
				// good, allocating memory for new messages.
				streamMsg := NewMessage()
				streamMsg.JobSerz = []byte(fmt.Sprintf(",%v", i))
				if i == N {
					last = true
					streamMsg.JobSerz = append(streamMsg.JobSerz, []byte(")")...)
				}
				streamMsg.HDR.Args["blake3"] = blake3OfBytesString(streamMsg.JobSerz)
				err = strm.UploadMore(ctx45, streamMsg, last, 0)
				panicOn(err)
				//vv("client sent part %v, len %v : '%v'", i, len(streamMsg.JobSerz), string(streamMsg.JobSerz))
			}
			//vv("all N=%v parts sent", N)

			//vv("first call has returned; it got the reply that the server got the last part:'%v'", string(reply.JobSerz))

			timeout := client.NewTimer(time.Minute)
			select {
			case m := <-strm.ReadCh:
				report := string(m.JobSerz)
				//vv("got from readCh: '%v' with JobSerz: '%v'", m.HDR.String(), report)
				cv.So(strings.Contains(report, "bytesWrit"), cv.ShouldBeTrue)
				cv.So(m.HDR.CallID, cv.ShouldEqual, originalStreamCallID)
				cv.So(fileExists(filename+".servergot"), cv.ShouldBeTrue)

			case <-timeout.C:
				t.Fatalf("should have gotten a reply from the server finishing the stream.")
			}
			timeout.Discard()
			if fileExists(filename) && N == 20 {
				// verify the contents of the assembled file
				fileBytes, err := os.ReadFile(filename)
				panicOn(err)
				cv.So(string(fileBytes), cv.ShouldEqual, "a=c(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)")
			}
		})
	})
}

func Test755_simnetonly_simnet_download(t *testing.T) {

	onlyBubbled(t, func() {

		cv.Convey("download a large file in parts from server to client, the opposite direction of the previous test.", t, func() {

			cfg := NewConfig()
			//cfg.TCPonly_no_TLS = false
			cfg.UseSimNet = true

			// start server
			cfg.ServerAddr = "127.0.0.1:0"
			srv := NewServer("srv_test855", cfg)

			serverAddr, err := srv.Start()
			panicOn(err)
			defer srv.Close()

			simnet := srv.cfg.GetSimnet()
			defer simnet.Close()

			// register streamer func with server
			downloaderName := "downloaderName"
			ssss := &ServerSendsDownloadStateTest{}
			srv.RegisterServerSendsDownloadFunc(downloaderName, ssss.ServerSendsDownloadTest)

			// start client
			cfg.ClientDialToHostPort = serverAddr.String()
			client, err := NewClient("cli_test855", cfg)
			panicOn(err)
			err = client.Start()
			panicOn(err)
			defer client.Close()

			// ask server to send us the stream

			// use deadline so we can confirm it is transmitted back from server to client
			// in the stream.
			deadline := time.Now().Add(time.Hour)
			ctx55, cancelFunc55 := context.WithDeadline(context.Background(), deadline)
			defer cancelFunc55()

			// start the call
			downloader, err := client.RequestDownload(ctx55, downloaderName, "test855_not_real_download")
			panicOn(err)

			//vv("downloader requested, with CallID = '%v'", downloader.CallID)
			// then send N more parts

			done := false
			for i := 0; !done; i++ {
				timeout := client.NewTimer(time.Second * 10)
				select {
				case m := <-downloader.ReadDownloadsCh:
					//report := string(m.JobSerz)
					//vv("on i = %v; got from readCh: '%v' with JobSerz: '%v'", i, m.HDR.String(), report)

					if !m.HDR.Deadline.Equal(deadline) {
						t.Fatalf("deadline not preserved")
					}

					if m.HDR.Typ == CallDownloadEnd {
						//vv("good: we see CallDownloadEnd from server.")
						done = true
					}

					if i == 0 {
						cv.So(m.HDR.Typ == CallDownloadBegin, cv.ShouldBeTrue)
					} else if i == 19 {
						cv.So(m.HDR.Typ == CallDownloadEnd, cv.ShouldBeTrue)
					} else {
						cv.So(m.HDR.Typ == CallDownloadMore, cv.ShouldBeTrue)
					}

					if m.HDR.Seqno != downloader.Seqno() {
						t.Fatalf("Seqno not preserved/mismatch: m.HDR.Seqno = %v but "+
							"downloader.Seqno = %v", m.HDR.Seqno, downloader.Seqno())
					}

				case <-timeout.C:
					t.Fatalf("should have gotten a reply from the server finishing the stream.")
				}
				timeout.Discard()
			} // end for i

			// do we get the lastReply too then?

			timeout := client.NewTimer(time.Second * 10)
			select {
			case m := <-downloader.ReadDownloadsCh:
				//report := string(m.JobSerz)
				//vv("got from readCh: '%v' with JobSerz: '%v'", m.HDR.String(), report)

				if m.HDR.Subject != "This is end. My only friend, the end. - Jim Morrison, The Doors." {
					t.Fatalf("where did The Doors quote disappear to?")
				}

				if !m.HDR.Deadline.Equal(deadline) {
					t.Fatalf("deadline not preserved")
				}

				if m.HDR.Seqno != downloader.Seqno() {
					t.Fatalf("Seqno not preserved/mismatch: m.HDR.Seqno = %v but "+
						"downloader.Seqno = %v", m.HDR.Seqno, downloader.Seqno())
				}

			case <-timeout.C:
				t.Fatalf("should have gotten a lastReply from the server finishing the call.")
			}
			timeout.Discard()
		})
	})
}

func Test765_simnetonly_bidirectional_download_and_upload(t *testing.T) {

	onlyBubbled(t, func() {

		cv.Convey("we should be able to register a server func that does uploads and downloads sequentially or simultaneously.", t, func() {

			cfg := NewConfig()
			//cfg.TCPonly_no_TLS = false
			cfg.UseSimNet = true

			// start server
			cfg.ServerAddr = "127.0.0.1:0"
			srv := NewServer("srv_test865", cfg)

			serverAddr, err := srv.Start()
			panicOn(err)
			defer srv.Close()

			simnet := srv.cfg.GetSimnet()
			defer simnet.Close()

			// register streamer func with server
			streamerName := "bi-streamer Name"
			bi := &ServeBistreamState{}
			srv.RegisterBistreamFunc(streamerName, bi.ServeBistream)

			// start client
			cfg.ClientDialToHostPort = serverAddr.String()
			client, err := NewClient("cli_test865", cfg)
			panicOn(err)
			err = client.Start()
			panicOn(err)
			defer client.Close()

			// ask server to send us the bistream

			// use deadline so we can confirm it is transmitted back from server to client
			// in the stream.
			deadline := time.Now().Add(time.Hour)
			ctx65, cancelFunc65 := context.WithDeadline(context.Background(), deadline)
			defer cancelFunc65()

			// start the bistream

			req := NewMessage()
			filename := "bi.all.srv.read.streams.txt"
			os.Remove(filename)
			req.JobSerz = []byte("receiveFile:" + filename + "\na=c(0")

			bistream, err := client.RequestBistreaming(ctx65, streamerName, req)
			panicOn(err)

			//vv("bistream requested, with CallID = '%v'", bistream.CallID())
			// then send N more parts

			//vv("begin download part")

			done := false
			for i := 0; !done; i++ {

				timeout := client.NewTimer(time.Second * 10)
				select {
				case m := <-bistream.ReadDownloadsCh:
					//report := string(m.JobSerz)
					//vv("on i = %v; got from readCh: '%v' with JobSerz: '%v'", i, m.HDR.String(), report)

					if !m.HDR.Deadline.Equal(deadline) {
						t.Fatalf("deadline not preserved")
					}

					if m.HDR.Typ == CallDownloadEnd {
						//vv("good: we see CallDownloadEnd from server.")
						done = true
					}

					if i == 0 {
						cv.So(m.HDR.Typ == CallDownloadBegin, cv.ShouldBeTrue)
					} else if i == 19 {
						cv.So(m.HDR.Typ == CallDownloadEnd, cv.ShouldBeTrue)
					} else {
						cv.So(m.HDR.Typ == CallDownloadMore, cv.ShouldBeTrue)
					}

					if m.HDR.Seqno != bistream.Seqno() {
						t.Fatalf("Seqno not preserved/mismatch: m.HDR.Seqno = %v but "+
							"bistream.Seqno = %v", m.HDR.Seqno, bistream.Seqno())
					}

				case <-timeout.C:
					t.Fatalf("should have gotten a reply from the server finishing the stream.")
				}
				timeout.Discard()
			} // end for i

			//vv("done with download. begin upload part")

			// ============================================
			// ============================================
			//
			// next: test that the same server func can receive a stream.
			//
			// We now check that the client can upload (send a stream to the server).
			// While typically these are interleaved in real world usage,
			// here we start with simple and sequential use.
			// ============================================
			// ============================================

			// start upload to the server.

			// read the final reply from the server.

			originalStreamCallID := bistream.CallID()
			//vv("865 upload starting, with CallID = '%v'", originalStreamCallID)
			// then send N more parts

			var last bool
			N := 20
			for i := 1; i <= N; i++ {
				streamMsg := NewMessage()
				streamMsg.JobSerz = []byte(fmt.Sprintf(",%v", i))
				streamMsg.HDR.Subject = blake3OfBytesString(streamMsg.JobSerz)
				if i == N {
					last = true
					streamMsg.JobSerz = append(streamMsg.JobSerz, []byte(")")...)
				}
				err = bistream.UploadMore(ctx65, streamMsg, last, 0)
				panicOn(err)
				//vv("uploaded part %v", i)
			}
			//vv("all N=%v parts uploaded", N)

			//vv("first call has returned; it got the reply that the server got the last part:'%v'", string(reply.JobSerz))

			timeout := client.NewTimer(time.Minute)
			select {
			case m := <-bistream.ReadDownloadsCh:
				report := string(m.JobSerz)
				//vv("got from readCh: '%v' with JobSerz: '%v'", m.HDR.String(), report)
				cv.So(strings.Contains(report, "bytesWrit"), cv.ShouldBeTrue)
				cv.So(m.HDR.CallID, cv.ShouldEqual, originalStreamCallID)
				cv.So(fileExists(filename), cv.ShouldBeTrue)

			case <-timeout.C:
				t.Fatalf("should have gotten a reply from the server finishing the stream.")
			}
			timeout.Discard()

			if fileExists(filename) && N == 20 {
				// verify the contents of the assembled file
				fileBytes, err := os.ReadFile(filename)
				panicOn(err)
				cv.So(string(fileBytes), cv.ShouldEqual, "a=c(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)")
			}
		})
	})
}

// Back ported from gosimnet.
// basic gosimnet operations Listen/Accept, Dial, NewTimer
func Test101_gosimnet_basics(t *testing.T) {

	bubbleOrNot(func() {

		shutdown := make(chan struct{})
		defer close(shutdown)

		// gosimnet version of startup.
		//cfg := NewSimNetConfig()
		//network := NewSimNet(cfg)
		//defer network.Close()
		//srv := network.NewSimServer("srv_" + t.Name())
		//defer srv.Close()

		// standard rpc25519 startup.
		cfg := NewConfig()
		cfg.UseSimNet = true

		cfg.ServerAddr = "127.0.0.1:0"
		srv := NewServer("srv_"+t.Name(), cfg)

		//serverAddr, err := srv.Start()
		//vv("back from srv.Start() in 701, elap = %v", time.Since(t0))

		// but then diverge here
		lsn, err := srv.Listen("", "")
		panicOn(err)
		serverAddr := lsn.Addr()

		panicOn(err)
		defer srv.Close()

		simnet := srv.cfg.GetSimnet()
		defer simnet.Close()

		// we need the server's conn2 in order
		// to break it out of the Read by conn2.Close()
		// and shutdown all goro cleanly.

		var conn2mut sync.Mutex
		var conn2 []net.Conn
		var done bool
		defer func() {
			conn2mut.Lock()
			defer conn2mut.Unlock()
			done = true
			for _, c := range conn2 {
				c.Close()
			}
		}()

		// make an echo server for the client
		go func() {
			for {
				select {
				case <-shutdown:
					//vv("server exiting on shutdown")
					return
				default:
				}
				c2, err := lsn.Accept()
				if err != nil {
					//alwaysPrintf("lsn.Accept err: server exiting on '%v'", err)
					return
				}
				conn2mut.Lock()
				if done {
					conn2mut.Unlock()
					return
				}
				conn2 = append(conn2, c2)
				conn2mut.Unlock()

				//vv("Accept on conn: local %v <-> %v remote", c2.LocalAddr(), c2.RemoteAddr())
				// per-client connection.
				go func(c2 net.Conn) {
					readCount := 0
					by := make([]byte, 1000)
					for {
						select {
						case <-shutdown:
							vv("server conn exiting on shutdown")
							return
						default:
						}
						//vv("server about to read on conn")
						readCount++
						n, err := c2.Read(by)
						if readCount == 1 {
							// first read should not error
							panicOn(err)
						}
						if err != nil {
							if err != io.EOF {
								vv("server conn exiting on Read error '%v'", err)
								panic(err)
							}
							// expected EOF at end of test.
							return
						}
						by = by[:n]
						//vv("echo server got '%v'", string(by))
						// must end in \n or client will hang!
						_, err = fmt.Fprintf(c2,
							"hi back from echo server, I saw '%v'\n", string(by))
						if err != nil {
							vv("server conn exiting on Write error '%v'", err)
							return
						}
						// close the conn to test our EOF sending
						c2.Close()
					} // end for

				}(c2)
			} // end for
		}() // end server

		// gosimnet
		//cli := network.NewSimClient("cli_" + t.Name())
		//defer cli.Close()

		// stardard rpc25519
		cfg.ClientDialToHostPort = serverAddr.String()
		cli, err := NewClient("cli_"+t.Name(), cfg)
		panicOn(err)
		defer cli.Close()
		//err = cli.Start()
		//panicOn(err)

		//shared / similar
		//vv("cli about to Dial")
		conn, err := cli.Dial("gosimnet", serverAddr.String())
		//vv("err = '%v'", err) // simnet_test.go:82 2000-01-01 00:00:00.002 +0000 UTC err = 'this client is already connected. create a NewClient()'
		panicOn(err)
		defer conn.Close()

		//conn := cli.simconn

		// back to common code
		fmt.Fprintf(conn, "hello gosimnet")
		response, err := bufio.NewReader(conn).ReadString('\n')
		//vv("response = '%v', err = '%v'", string(response), err)
		panicOn(err) // EOF back here under synctest, so it is a timing thing
		// likely we have to now be checking the remotes close chan too.x

		//vv("client sees response: '%v'", string(response))
		if got, want := string(response), `hi back from echo server, I saw 'hello gosimnet'
`; got != want {
			t.Fatalf("error: want '%v' but got '%v'", want, got)
		}

		// reading more should get EOF, since server now closes the file.
		buf := make([]byte, 1)
		nr, err := conn.Read(buf)
		if err != io.EOF {
			panic(fmt.Sprintf("expected io.EOF, got nr=%v; err = '%v'", nr, err))
		}
		//vv("good: got EOF as we should, since server closes the conn.")

		// timer test
		t0 := time.Now()
		goalWait := time.Second

		// set a timer on the client side.
		timeout := cli.NewTimer(goalWait)
		t1 := <-timeout.C
		_ = t1
		elap := time.Since(t0)

		// must do this, since no automatic GC of gosimnet Timers.
		// note: also no timer Reset at the moment, just Discard
		// and make a new one.
		defer timeout.Discard()

		if elap < goalWait {
			t.Fatalf("timer went off too early! elap(%v) < goalWait(%v)", elap, goalWait)
		}
		//vv("good: finished timer (fired at %v) after %v >= goal %v", t1, elap, goalWait)
	})
}
func Test102_time_truncate_works_under_synctest(t *testing.T) {

	if !faketime {
		t.Skip("real time sensitive to slow machine: skipping test")
	}

	// truncate was giving unexpected
	// answers under synctest...
	// compare to real time. hmm. seems fine.

	tick := 100 * time.Millisecond
	//tick := time.Second

	net := &Simnet{
		scenario: &scenario{
			tick: tick,
		},
	}
	_ = net

	fake := false
	f := func() {

		//vv("\n\n running in a synctime bubble = %v; tick=%v;\n\n", fake, tick)
		for i := range 30 {

			now := time.Now()

			//dur, goal := net.durToGridPoint(now)
			// equivalently:
			goal := now.Add(tick).Truncate(tick)
			dur := goal.Sub(now)

			time.Sleep(dur)

			now2 := time.Now()
			offby := now2.Sub(goal)

			pct := 100 * math.Abs(float64(offby)) / float64(tick)

			//vv("i=%03d, dur=%v ; offby = %v (%0.2f %%); tick=%v; \n now=%v\n ->\n now2=%v\n goal=%v\n\n", i, dur, offby, pct, tick, now, now2, goal)

			if fake {
				if offby != 0 {
					panic(fmt.Sprintf("fake time, i=%v; expected offby of 0, got %v", i, offby))
				}
			} else {
				if pct >= 10 {
					panic(fmt.Sprintf("real time, i=%v; expected offby < 10%%, got %0.2f %% (%v)", i, pct, offby))
				}
			}
		}
	}
	// realtime
	if !faketime {
		f()
	}

	if faketime {
		fake = true
		bubbleOrNot(f) // calls synctest.Run(f) when faketime is true.
	}
}

func Test103_maskTime(t *testing.T) {
	return // userMaskTime changed and I don't want to update this test; probably delete it?

	bubbleOrNot(func() {
		now := time.Now()
		for range 100 {
			m := userMaskTime(now, 1)
			if m.Before(now) {
				panic(fmt.Sprintf("m(%v) < now(%v) wrong", m, now))
			}
			mod := m.UnixNano() % int64(timeMask0)
			if mod != int64(timeMask9) {
				panic(fmt.Sprintf("mod(%v) != timeMask9(%v)", mod, timeMask9))
			}
			//vv("%v -> %v", now, m)
			advance := time.Duration(cryptoRandPositiveInt64()) %
				(time.Millisecond * 10)
			now = now.Add(advance)
		}
	})
}

func Test771_simnetonly_client_dropped_sends(t *testing.T) {

	// onlyBubbled is fast. bubbleOrNot in realtime is very slow.
	//
	// to keep regular test runs fast, we use onlyBubbled.
	// It is legitimate to check on realtime too, but
	// only do that if you are prepared to wait minutes
	// for each test; under bubbleOrNot with realtime.
	onlyBubbled(t, func() {
		cv.Convey("simnet client dropped sends should appear in the senders dropped send Q", t, func() {

			simt, cfg := newSimnetTest(t, "test771")
			cli, srv, simnet, srvname, cliname := setupSimnetTest(simt, cfg)
			defer srv.Close()
			defer cli.Close()
			defer simnet.Close()

			serviceName := "customEcho"
			srv.Register2Func(serviceName, customEcho)

			//vv("simnet before cliDropSends %v", simnet.GetSimnetSnapshot())
			undoCliDrop := simt.clientDropsSends(1)
			//vv("simnet after cliDropsSends %v", simnet.GetSimnetSnapshot())

			req := NewMessage()
			req.HDR.ToServiceName = serviceName
			req.HDR.FromServiceName = serviceName
			req.JobSerz = []byte("Hello from client!")
			waitFor := time.Second
			reply, err := cli.SendAndGetReply(req, nil, waitFor)
			if err == nil {
				panic("wanted timeout could not see server")
			}
			if reply != nil {
				panic(fmt.Sprintf("expected nil reply on error, got '%v'", reply))
			}
			// is dropped send visible? both cli and srv
			stat := simnet.GetSimnetSnapshot()

			sps := stat.Peermap[srvname]
			sconn := sps.Connmap[srvname]
			cconn := stat.LoneCli[cliname].Conn[0]

			//vv("stat.Peermap = '%v'; cconn = '%v", stat.Peermap, cconn)

			// verify client is not faulty in sending, only server.
			ndrop := cconn.DroppedSendQ.Len()
			if ndrop == 0 {
				panic(fmt.Sprintf("expected cli ndrop(%v) > 0", ndrop))
			} else {
				//vv("good, saw cli ndrop(%v) > 0", ndrop)
			}

			ndrop = sconn.DroppedSendQ.Len()
			if ndrop != 0 {
				panic(fmt.Sprintf("expected srv ndrop(%v) == 0", ndrop))
			} else {
				//vv("good, saw srv ndrop(%v) == 0", ndrop)
			}
			//vv("err = '%v'; reply = %p", err, reply)

			// repair the network
			undoCliDrop()

			//vv("after cli repaired, re-attempt cli call with: %v", simnet.GetSimnetSnapshot())

			req2 := NewMessage()
			req2.HDR.ToServiceName = serviceName
			req2.HDR.FromServiceName = serviceName
			req2.JobSerz = []byte("Hello from client! 2nd time.")

			reply2, err := cli.SendAndGetReply(req2, nil, waitFor)
			panicOn(err)
			want := string(req2.JobSerz)
			gotit := strings.HasPrefix(string(reply2.JobSerz), want)
			if !gotit {
				t.Fatalf("expected JobSerz to start with '%v' but got '%v'", want, string(reply2.JobSerz))
			}

		})
	})
}

func Test772_simnetonly_server_dropped_sends(t *testing.T) {

	onlyBubbled(t, func() {
		cv.Convey("simnet server dropped sends should appear in the servers dropped send Q", t, func() {

			simt, cfg := newSimnetTest(t, "test772")
			cli, srv, simnet, srvname, cliname := setupSimnetTest(simt, cfg)
			defer srv.Close()
			defer cli.Close()
			defer simnet.Close()

			serviceName := "customEcho"
			srv.Register2Func(serviceName, customEcho)

			//vv("simnet before serverDropSends %v", simnet.GetSimnetSnapshot())
			undoServerDrop := simt.serverDropsSends(1)
			//vv("simnet after serverDropsSends %v", simnet.GetSimnetSnapshot())

			req := NewMessage()
			req.HDR.ToServiceName = serviceName
			req.HDR.FromServiceName = serviceName
			req.JobSerz = []byte("Hello from client!")
			waitFor := time.Second
			reply, err := cli.SendAndGetReply(req, nil, waitFor)
			if err == nil {
				panic("wanted timeout did not hear from server")
			}
			if reply != nil {
				panic(fmt.Sprintf("expected nil reply on error, got '%v'", reply))
			}
			// is dropped send visible? both cli and srv
			stat := simnet.GetSimnetSnapshot()

			sps := stat.Peermap[srvname]
			sconn := sps.Connmap[srvname]
			cconn := stat.LoneCli[cliname].Conn[0]

			//vv("stat.Peermap = '%v'; cconn = '%v", stat.Peermap, cconn)

			// verify only server is faulty in sending, not client.
			ndrop := cconn.DroppedSendQ.Len()
			if ndrop != 0 {
				panic(fmt.Sprintf("expected cli ndrop(%v) == 0", ndrop))
			} else {
				//vv("good, saw cli ndrop(%v) == 0", ndrop)
			}

			ndrop = sconn.DroppedSendQ.Len()
			if ndrop == 0 {
				panic(fmt.Sprintf("expected srv ndrop(%v) > 0", ndrop))
			} else {
				//vv("good, saw srv ndrop(%v) > 0", ndrop)
			}
			//vv("err = '%v'; reply = %p", err, reply)

			// repair the network
			undoServerDrop()

			//vv("after srv repaired, re-attempt cli call with: %v", simnet.GetSimnetSnapshot())

			req2 := NewMessage()
			req2.HDR.ToServiceName = serviceName
			req2.HDR.FromServiceName = serviceName
			req2.JobSerz = []byte("Hello from client! 2nd time.")

			reply2, err := cli.SendAndGetReply(req2, nil, waitFor)
			panicOn(err)
			want := string(req2.JobSerz)
			gotit := strings.HasPrefix(string(reply2.JobSerz), want)
			if !gotit {
				t.Fatalf("expected JobSerz to start with '%v' but got '%v'", want, string(reply2.JobSerz))
			}

		})
	})
}

//func (simt *simnetTest) Close() {
//	simt.simnet.Close()
//}

type simnetTest struct {
	cfg    *Config
	short  string // short test name
	simnet *Simnet
	t      *testing.T
	cli    *Client
	srv    *Server
}

func newSimnetTest(t *testing.T, shortTestName string) (simt *simnetTest, cfg *Config) {
	cfg = NewConfig()
	cfg.UseSimNet = true
	cfg.QuietTestMode = true

	return &simnetTest{
		cfg:   cfg,
		short: shortTestName,
		t:     t,
	}, cfg
}

// setupSimnetTest: encapsulate all the boilerplate to
// set up a 772 or 771 style test: run a single Server (not peer)
// plus a single Client that starts connected to that Server.
//
// For now we always assume that the client and server can
// initially talk to each other before introducing any faults.
//
// Otherwise the Client will error out with a
// "cannot Dial server" error before anything interesting
// can be tested with the simnet.
func setupSimnetTest(simt *simnetTest, cfg *Config) (
	// returned:
	cli *Client,
	srv *Server,
	simnet *Simnet,
	srvname string,
	cliname string,
) {

	cfg.ServerAddr = "127.0.0.1:0"
	srvname = "srv_" + simt.short
	srv = NewServer(srvname, cfg)
	//vv("about to srv.Start() srvname = %v", srvname)
	t0 := time.Now()
	_ = t0
	serverAddr, err := srv.Start()
	//vv("back from srv.Start() in 771, elap = %v", time.Since(t0))
	panicOn(err)
	simnet = cfg.GetSimnet()

	cliname = "cli_" + simt.short
	cfg.ClientDialToHostPort = serverAddr.String()
	cli, err = NewClient(cliname, cfg)
	panicOn(err)
	err = cli.Start()
	panicOn(err)

	simt.simnet = simnet
	simt.srv = srv
	simt.cli = cli
	return
}

func (t *simnetTest) setAllHealthy() {
	const deliverDroppedSends_YES = true
	const deliverDroppedSends_NO = false
	const powerOnIfOff_YES = true

	err := t.simnet.AllHealthy(powerOnIfOff_YES, deliverDroppedSends_NO)
	panicOn(err)
}

func (t *simnetTest) serverDropsSends(prob float64) (undo func()) {
	return t.nodeDropsSends(t.srv.simnode, prob)
}
func (t *simnetTest) clientDropsSends(prob float64) (undo func()) {
	return t.nodeDropsSends(t.cli.simnode, prob)
}

func (t *simnetTest) nodeDropsSends(node *simnode, prob float64) (undo func()) {

	dd := DropDeafSpec{
		UpdateDropSends:  true,
		DropSendsNewProb: prob,
	}

	// this is for recover really, it typically only
	// matters when faults are removed, but can
	// be used to time-warp packets while still faulted.
	const deliverDroppedSends_NO = false

	//err = simnet.FaultHost(srv.simnode.name, dd, deliverDroppedSends_NO)
	err := t.simnet.FaultHost(node.name, dd, deliverDroppedSends_NO)
	panicOn(err)
	undo = func() {
		dd := DropDeafSpec{
			UpdateDropSends:  true,
			DropSendsNewProb: 0,
		}
		err := t.simnet.FaultHost(node.name, dd, deliverDroppedSends_NO)
		panicOn(err)
	}

	//vv("nodeDropsSends done for '%v'", node.name)
	return
}

func (t *simnetTest) serverDeaf(prob float64) (undo func()) {
	return t.nodeDeaf(t.srv.simnode, prob)
}
func (t *simnetTest) clientDeaf(prob float64) (undo func()) {
	return t.nodeDeaf(t.cli.simnode, prob)
}

func (t *simnetTest) nodeDeaf(node *simnode, prob float64) (undo func()) {

	dd := DropDeafSpec{
		UpdateDeafReads:  true,
		DeafReadsNewProb: prob,
	}

	// this is for recover really, it typically only
	// matters when faults are removed, but can
	// be used to time-warp packets while still faulted.
	const deliverDroppedSends_NO = false

	err := t.simnet.FaultHost(node.name, dd, deliverDroppedSends_NO)
	panicOn(err)
	undo = func() {
		dd := DropDeafSpec{
			UpdateDeafReads:  true,
			DeafReadsNewProb: 0,
		}
		err := t.simnet.FaultHost(node.name, dd, deliverDroppedSends_NO)
		panicOn(err)
	}

	//vv("nodeDeaf done for '%v'", node.name)
	return
}

func (t *simnetTest) AlterServer(alt Alteration) (undo func()) {
	return t.alterNode(t.srv.simnode, alt)
}

func (t *simnetTest) AlterClient(alt Alteration) (undo func()) {
	return t.alterNode(t.cli.simnode, alt)
}

func (t *simnetTest) alterNode(node *simnode, alt Alteration) (undo func()) {

	undoAlter, err := t.simnet.AlterHost(node.name, alt)
	panicOn(err)
	undo = func() {
		_, err := t.simnet.AlterHost(node.name, undoAlter)
		panicOn(err)
	}

	//vv("alterNode done for '%v'", node.name)
	return
}

func Test781_simnetonly_client_isolated(t *testing.T) {

	// same as 771 but use AlterHost -> ISOLATED
	onlyBubbled(t, func() {
		cv.Convey("simnet ISOLATED client dropped sends should appear in the client's dropped send Q", t, func() {

			simt, cfg := newSimnetTest(t, "test781")
			cli, srv, simnet, srvname, cliname := setupSimnetTest(simt, cfg)
			defer srv.Close()
			defer cli.Close()
			defer simnet.Close()

			serviceName := "customEcho"
			srv.Register2Func(serviceName, customEcho)

			//vv("before simt.AlterClient(ISOLATE): %v", simnet.GetSimnetSnapshot())
			undoIsolated := simt.AlterClient(ISOLATE)
			vv("after simt.AlterClient(ISOLATE): %v", simnet.GetSimnetSnapshot())

			req := NewMessage()
			req.HDR.ToServiceName = serviceName
			req.HDR.FromServiceName = serviceName
			req.JobSerz = []byte("Hello from client!")
			waitFor := time.Second
			reply, err := cli.SendAndGetReply(req, nil, waitFor)
			if err == nil {
				panic("wanted timeout could not see server")
			}
			if reply != nil {
				panic(fmt.Sprintf("expected nil reply on error, got '%v'", reply))
			}
			// is dropped send visible? both cli and srv
			stat := simnet.GetSimnetSnapshot()

			sps := stat.Peermap[srvname]
			sconn := sps.Connmap[srvname]
			cconn := stat.LoneCli[cliname].Conn[0]

			//vv("stat.Peermap = '%v'; cconn = '%v", stat.Peermap, cconn)

			// verify client is not faulty in sending, only server.
			ndrop := cconn.DroppedSendQ.Len()
			if ndrop == 0 {
				panic(fmt.Sprintf("expected cli ndrop(%v) > 0", ndrop))
			} else {
				vv("good, saw cli ndrop(%v) > 0", ndrop)
			}

			ndrop = sconn.DroppedSendQ.Len()
			if ndrop != 0 {
				panic(fmt.Sprintf("expected srv ndrop(%v) == 0", ndrop))
			} else {
				vv("good, saw srv ndrop(%v) == 0", ndrop)
			}
			//vv("err = '%v'; reply = %p", err, reply)

			vv("cli still isolated, network after cli tried to send echo request: %v", simnet.GetSimnetSnapshot())

			// repair the network
			undoIsolated()

			vv("after cli repaired, re-attempt cli call with: %v", simnet.GetSimnetSnapshot())

			req2 := NewMessage()
			req2.HDR.ToServiceName = serviceName
			req2.HDR.FromServiceName = serviceName
			req2.JobSerz = []byte("Hello from client! 2nd time.")

			reply2, err := cli.SendAndGetReply(req2, nil, waitFor)
			panicOn(err)
			want := string(req2.JobSerz)
			gotit := strings.HasPrefix(string(reply2.JobSerz), want)
			if !gotit {
				t.Fatalf("expected JobSerz to start with '%v' but got '%v'", want, string(reply2.JobSerz))
			}

		})
	})
}

func Test782_simnetonly_server_isolated(t *testing.T) {

	// same as 781 but for server (isolate with AlterHost -> ISOLATED)
	onlyBubbled(t, func() {
		cv.Convey("simnet ISOLATED server dropped sends should appear in the server's dropped send Q", t, func() {

			simt, cfg := newSimnetTest(t, "test782")
			cli, srv, simnet, srvname, cliname := setupSimnetTest(simt, cfg)
			defer srv.Close()
			defer cli.Close()
			defer simnet.Close()

			serviceName := "customEcho"
			srv.Register2Func(serviceName, customEcho)

			//vv("before simt.AlterServer(ISOLATE): %v", simnet.GetSimnetSnapshot())
			undoIsolated := simt.AlterServer(ISOLATE)
			//vv("after simt.AlterServer(ISOLATE): %v", simnet.GetSimnetSnapshot())
			//vv("after simt.AlterServer(ISOLATE): %v", simnet.GetSimnetSnapshot().ShortString())

			req := NewMessage()
			req.HDR.ToServiceName = serviceName
			req.HDR.FromServiceName = serviceName
			req.JobSerz = []byte("Hello from client!")
			waitFor := time.Second
			reply, err := cli.SendAndGetReply(req, nil, waitFor)
			if err == nil {
				panic("wanted timeout could not see server")
			}
			if reply != nil {
				panic(fmt.Sprintf("expected nil reply on error, got '%v'", reply))
			}
			// is dropped send visible? both cli and srv
			stat := simnet.GetSimnetSnapshot()

			sps := stat.Peermap[srvname]
			sconn := sps.Connmap[srvname]
			cconn := stat.LoneCli[cliname].Conn[0]

			//vv("stat.Peermap = '%v'; cconn = '%v", stat.Peermap, cconn)

			// verify client is not faulty in sending, only server.
			ndrop := cconn.DroppedSendQ.Len()
			if ndrop == 0 {
				panic(fmt.Sprintf("expected cli ndrop(%v) > 0", ndrop))
			} else {
				vv("good, saw cli ndrop(%v) > 0", ndrop)
			}

			ndrop = sconn.DroppedSendQ.Len()
			if ndrop > 0 {
				panic(fmt.Sprintf("expected srv ndrop(%v) == 0", ndrop))
			} else {
				vv("good, saw srv ndrop(%v) == 0", ndrop)
			}
			//vv("err = '%v'; reply = %p", err, reply)

			//vv("srv still isolated, network after cli tried to send echo request: %v", simnet.GetSimnetSnapshot())

			// repair the network
			undoIsolated()

			vv("after srv repaired, re-attempt cli call with: %v", simnet.GetSimnetSnapshot())

			req2 := NewMessage()
			req2.HDR.ToServiceName = serviceName
			req2.HDR.FromServiceName = serviceName
			req2.JobSerz = []byte("Hello from client! 2nd time.")

			reply2, err := cli.SendAndGetReply(req2, nil, waitFor)
			panicOn(err)
			want := string(req2.JobSerz)
			gotit := strings.HasPrefix(string(reply2.JobSerz), want)
			if !gotit {
				t.Fatalf("expected JobSerz to start with '%v' but got '%v'", want, string(reply2.JobSerz))
			}

		})
	})
}
