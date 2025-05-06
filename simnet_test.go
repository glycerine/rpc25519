package rpc25519

// build/run with:
// GOTRACEBACK=all GOEXPERIMENT=synctest go test -v

import (
	//"context"
	//"encoding/base64"
	"fmt"
	"os"
	//"path/filepath"
	"context"
	"strings"
	"testing"
	"time"

	cv "github.com/glycerine/goconvey/convey"
)

func Test701_simnetonly_RoundTrip_SendAndGetReply_SimNet(t *testing.T) {

	cv.Convey("basic SimNet channel based remote procedure call with rpc25519: register a callback on the server, and have the client call it.", t, func() {

		cfg := NewConfig()
		cfg.UseSimNet = true

		cfg.ServerAddr = "127.0.0.1:0"
		srv := NewServer("srv_test701", cfg)

		serverAddr, err := srv.Start()
		panicOn(err)
		defer srv.Close()

		//vv("(SimNet) server Start() returned serverAddr = '%v'", serverAddr)

		serviceName := "customEcho"
		srv.Register2Func(serviceName, customEcho)

		cfg.ClientDialToHostPort = serverAddr.String()
		cli, err := NewClient("cli_test701", cfg)
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
		timeout := cli.NewTimer(goalWait)

		//timerC := cli.TimeAfter(goalWait)
		t1 := <-timeout.C
		elap := time.Since(t0)
		timeout.Discard()
		if elap < goalWait {
			t.Fatalf("timer went off too early! elap(%v) < goalWait(%v)", elap, goalWait)
		}
		vv("good: finished timer (fired at %v) after %v >= goal %v", t1, elap, goalWait)
	})
}

func Test704_rng_hops(t *testing.T) {
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

// simnet version of cli_test 006
func Test706_simnetonly_RoundTrip_Using_NetRPC(t *testing.T) {

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

	serverAddr, err := srv.Start()
	panicOn(err)
	defer srv.Close()

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
}

// simnet version of 040 in cli_test.go
func Test740_simnetonly_remote_cancel_by_context(t *testing.T) {

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
		req.HDR.ServiceName = serviceName741
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
}

// add simnetonly

func Test745_simnetonly_upload(t *testing.T) {

	cv.Convey("upload a large file in parts from client to server", t, func() {

		cfg := NewConfig()
		//cfg.TCPonly_no_TLS = false
		cfg.UseSimNet = true

		cfg.ServerAddr = "127.0.0.1:0"
		srv := NewServer("srv_test845", cfg)

		serverAddr, err := srv.Start()
		panicOn(err)
		defer srv.Close()

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
		req.HDR.ServiceName = uploaderName
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
}

func Test755_simnetonly_simnet_download(t *testing.T) {

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
}

func Test765_simnetonly_bidirectional_download_and_upload(t *testing.T) {

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
}
