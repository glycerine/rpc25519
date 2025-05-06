//go:build goexperiment.synctest

package rpc25519

import (
	//"context"
	//"encoding/base64"
	//"fmt"
	//"os"
	//"path/filepath"
	//"context"
	//"strings"
	"testing"
	//"testing/synctest"
	"time"

	cv "github.com/glycerine/goconvey/convey"
)

func Test800_SimNet_all_timers_dur_0_fire_now(t *testing.T) {

	if !globalUseSynctest {
		t.Skip("test only for synctest.") // see also build tag at top.
		return
	}

	bubbleOrNot(func() {
		cv.Convey("SimNet using synctest depends on all the times set to duration 0/now firing before we quiese to durable blocking. verify this assumption under synctest. yes: note the Go runtime implementation does a select with a default: so it will discard the timer alert rather than block.", t, func() {

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
			now := time.Now()
			if !t0.Equal(now) {
				t.Fatalf("we have a problem, Houston. t0=%v, but now=%v", t0, now)
			}
			//vv("end test800") // shows same time as start, good.
		})
	})
}

/*
func Test801_synctestonly_RoundTrip_SendAndGetReply_SimNet(t *testing.T) {

	cv.Convey("super basic synctest + SimNet send/read/timer test of channel based remote procedure call with rpc25519: register a callback on the server, and have the client call it. See a timer fire.", t, func() {

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
			timeout := cli.NewTimer(goalWait)

			//timerC := cli.TimeAfter(goalWait)
			t1 := <-timeout.C
			_ = t1
			elap := time.Since(t0)
			timeout.Discard()
			if elap < goalWait {
				t.Fatalf("timer went off too early! elap(%v) < goalWait(%v)", elap, goalWait)
			}
			//vv("good: finished timer (fired at %v) after %v >= goal %v", t1, elap, goalWait)
		})
	})
}

// synctest + simnet version of cli_test 006, simnet_test 706
func Test806__synctestonly_RoundTrip_Using_NetRPC(t *testing.T) {

	// basic SimNet with rpc25519 using the net/rpc API: register a callback on the server, and have the client call it.
	synctest.Run(func() {
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
		srv := NewServer("srv_test806", cfg)

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
		client, err := NewClient("cli_test806", cfg)
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
		//vv("good 806, got back reply '%#v'", reply)

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
		//vv("good 806: past nonexistent method")

		// Unknown service
		args = &Args{7, 8}
		reply = new(Reply)
		err = client.Call("Arith.Unknown", args, reply, nil)
		if err == nil {
			t.Error("expected error calling unknown service")
		} else if !strings.Contains(err.Error(), "method") {
			t.Error("expected error about method; got", err)
		}
		//vv("good 806: past unknown service")

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
		//vv("good 806: past out of order")

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
		//vv("good 806: past error test")

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
		//vv("good 806: past ServiceName with dot . test")

		//vv("good: end of 806 test")
	})
}

// synctest version of 040 in cli_test.go
func Test840_synctestonly_remote_cancel_by_context(t *testing.T) {

	synctest.Run(func() {

		cv.Convey("remote cancellation", t, func() {

			cfg := NewConfig()
			//cfg.TCPonly_no_TLS = false
			cfg.UseSimNet = true
			//cfg.ServerSendKeepAlive = time.Second * 10

			cfg.ServerAddr = "127.0.0.1:0"
			srv := NewServer("srv_test840", cfg)

			serverAddr, err := srv.Start()
			panicOn(err)
			defer srv.Close()

			//vv("server Start() returned serverAddr = '%v'", serverAddr)

			// net/rpc API on server
			mustCancelMe := NewMustBeCancelled()
			srv.Register(mustCancelMe)

			// and register early for 841 below
			serviceName041 := "test841_hang_until_cancel"
			srv.Register2Func(serviceName041, mustCancelMe.MessageAPI_HangUntilCancel)

			cfg.ClientDialToHostPort = serverAddr.String()
			client, err := NewClient("cli_test840", cfg)
			panicOn(err)
			err = client.Start()
			panicOn(err)

			defer client.Close()

			// using the net/rpc API
			args := &Args{7, 8}
			reply := new(Reply)

			var cliErr40 error
			cliErrIsSet40 := make(chan bool)
			ctx40, cancelFunc40 := context.WithCancel(context.Background())
			go func() {
				//vv("client.Call() goro top about to call over net/rpc: MustBeCancelled.WillHangUntilCancel()")

				cliErr40 = client.Call("MustBeCancelled.WillHangUntilCancel", args, reply, ctx40)
				//vv("client.Call() returned with cliErr = '%v'", cliErr40)
				close(cliErrIsSet40)
			}()

			// let the call get blocked.
			//vv("cli_test 040: about to block on test040callStarted")
			<-mustCancelMe.callStarted // synctest blocked here
			//vv("cli_test 040: we got past test040callStarted")

			// cancel it: transmit cancel request to server.
			cancelFunc40()
			//vv("past cancelFunc()")

			<-cliErrIsSet40
			//vv("past cliErrIsSet channel; cliErr40 = '%v'", cliErr40)

			if cliErr40 != ErrCancelReqSent {
				t.Errorf("Test040: expected ErrCancelReqSent but got %v", cliErr40)
			}

			// confirm that server side function is unblocked too
			//vv("about to verify that server side context was cancelled.")
			<-mustCancelMe.callFinished
			//vv("server side saw the cancellation request: confirmed.")

			// use Message []byte oriented API: test 841

			var cliErr41 error
			cliErrIsSet41 := make(chan bool)
			ctx41, cancelFunc41 := context.WithCancel(context.Background())
			req := NewMessage()
			req.HDR.Typ = CallRPC
			req.HDR.ServiceName = serviceName041
			var reply41 *Message

			go func() {
				reply41, cliErr41 = client.SendAndGetReply(req, ctx41.Done(), 0)
				//vv("client.Call() returned with cliErr = '%v'", cliErr41)
				close(cliErrIsSet41)
			}()

			// let the call get blocked on the server (only works under test, of course).
			<-mustCancelMe.callStarted
			//vv("cli_test 841: we got past test041callStarted")

			// cancel it: transmit cancel request to server.
			cancelFunc41()
			//vv("past cancelFunc()")

			<-cliErrIsSet41
			//vv("past cliErrIsSet channel; cliErr = '%v'", cliErr41)

			if cliErr41 != ErrCancelReqSent {
				t.Errorf("Test041: expected ErrCancelReqSent but got %v", cliErr41)
			}

			if reply41 != nil {
				t.Errorf("Test041: expected reply41 to be nil, but got %v", reply41)
			}

			// confirm that server side function is unblocked too
			//vv("about to verify that server side context was cancelled.")
			<-mustCancelMe.callFinished
		})
	})
}

func Test845_synctestonly_simnet_upload(t *testing.T) {

	synctest.Run(func() {

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
	})
}

func Test855_synctestonly_simnet_download(t *testing.T) {

	synctest.Run(func() {

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
	})
}

func Test865_synctestonly_simnet_bidirectional_download_and_upload(t *testing.T) {

	synctest.Run(func() {

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
	})
}
*/
