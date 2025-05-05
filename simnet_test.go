package rpc25519

import (
	//"context"
	//"encoding/base64"
	//"fmt"
	//"os"
	//"path/filepath"
	"context"
	"strings"
	"testing"
	"time"

	cv "github.com/glycerine/goconvey/convey"
)

func Test700_SimNet_all_timers_dur_0_fire_now(t *testing.T) {

	cv.Convey("SimNet depends on all the times set to duration 0/now firing before we quiese to durable blocking. verify this assumption. yes: note the Go runtime implementation does a select with a default: so it will discard the timer alert rather than block.", t, func() {

		t0 := time.Now()
		//vv("start test700")
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
		//vv("end test700") // shows same time as start, good.
		//})
	})
}

func Test701_RoundTrip_SendAndGetReply_SimNet(t *testing.T) {

	cv.Convey("basic SimNet channel based remote procedure call with rpc25519: register a callback on the server, and have the client call it.", t, func() {

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
		elap := time.Since(t0)
		timeout.Discard()
		if elap < goalWait {
			t.Fatalf("timer went off too early! elap(%v) < goalWait(%v)", elap, goalWait)
		}
		vv("good: finished timer (fired at %v) after %v >= goal %v", t1, elap, goalWait)
	})
}

func Test704_SimNet_rng_hops(t *testing.T) {
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
func Test706_SimNet_RoundTrip_Using_NetRPC(t *testing.T) {

	// basic SimNet with rpc25519 using the net/rpc API: register a callback on the server, and have the client call it.
	cfg := NewConfig()
	//orig cfg.TCPonly_no_TLS = true
	cfg.UseSimNet = true
	// ah, I think this made it work? hmm... not necessary, works without.
	// but I see the client has keep-alives on... hm, it does not...
	// where are these timers coming from then?
	// simnet.go:843 2000-01-01 00:00:48
	//  ------- CLIENT timerQ  completeTm PQ --------
	//pq[ 0] = mop{CLIENT TIMER init:-16s, arr:unk, complete:4s op.sn:33, msg.sn:0}
	//pq[ 1] = mop{CLIENT TIMER init:-10s, arr:unk, complete:10s op.sn:38, msg.sn:0}
	//pq[ 2] = mop{CLIENT TIMER init:-6s, arr:unk, complete:14s op.sn:43, msg.sn:0}

	//cfg.ServerSendKeepAlive = time.Second * 10

	path := GetPrivateCertificateAuthDir() + sep + "psk.binary"
	panicOn(setupPSK(path))
	cfg.PreSharedKeyPath = path
	//cfg.ReadTimeout = 2 * time.Second
	//cfg.WriteTimeout = 2 * time.Second

	cfg.ServerAddr = "127.0.0.1:0"
	srv := NewServer("srv_test001", cfg)

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
	client, err := NewClient("test006", cfg)
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

// synctest version of 040 in cli_test.go
func Test740_simnet_remote_cancel_by_context(t *testing.T) {

	cv.Convey("simnet remote cancellation", t, func() {

		cfg := NewConfig()
		//cfg.TCPonly_no_TLS = false
		cfg.UseSimNet = true
		//cfg.ServerSendKeepAlive = time.Second * 10 // does not stop hang on synctest

		cfg.ServerAddr = "127.0.0.1:0"
		srv := NewServer("srv_test040", cfg)

		serverAddr, err := srv.Start()
		panicOn(err)
		defer srv.Close()

		vv("server Start() returned serverAddr = '%v'", serverAddr)

		// net/rpc API on server
		mustCancelMe := NewMustBeCancelled()
		srv.Register(mustCancelMe)

		// and register early for 041 below
		serviceName041 := "test041_hang_until_cancel"
		srv.Register2Func(serviceName041, mustCancelMe.MessageAPI_HangUntilCancel)

		cfg.ClientDialToHostPort = serverAddr.String()
		client, err := NewClient("test040", cfg)
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
			vv("client.Call() goro top about to call over net/rpc: MustBeCancelled.WillHangUntilCancel()")

			cliErr40 = client.Call("MustBeCancelled.WillHangUntilCancel", args, reply, ctx40)
			vv("client.Call() returned with cliErr = '%v'", cliErr40)
			close(cliErrIsSet40)
		}()

		// let the call get blocked.
		vv("cli_test 040: about to block on test040callStarted")
		<-test040callStarted // synctest blocked here
		vv("cli_test 040: we got past test040callStarted")

		// cancel it: transmit cancel request to server.
		cancelFunc40()
		vv("past cancelFunc()")

		<-cliErrIsSet40
		vv("past cliErrIsSet channel; cliErr40 = '%v'", cliErr40)

		if cliErr40 != ErrCancelReqSent {
			t.Errorf("Test040: expected ErrCancelReqSent but got %v", cliErr40)
		}

		// confirm that server side function is unblocked too
		vv("about to verify that server side context was cancelled.")
		<-test040callFinished
		vv("server side saw the cancellation request: confirmed.")

		// use Message []byte oriented API: test 041

		var cliErr41 error
		cliErrIsSet41 := make(chan bool)
		ctx41, cancelFunc41 := context.WithCancel(context.Background())
		req := NewMessage()
		req.HDR.Typ = CallRPC
		req.HDR.ServiceName = serviceName041
		var reply41 *Message

		go func() {
			reply41, cliErr41 = client.SendAndGetReply(req, ctx41.Done(), 0)
			vv("client.Call() returned with cliErr = '%v'", cliErr41)
			close(cliErrIsSet41)
		}()

		// let the call get blocked on the server (only works under test, of course).
		<-test041callStarted
		vv("cli_test 041: we got past test041callStarted")

		// cancel it: transmit cancel request to server.
		cancelFunc41()
		vv("past cancelFunc()")

		<-cliErrIsSet41
		vv("past cliErrIsSet channel; cliErr = '%v'", cliErr41)

		if cliErr41 != ErrCancelReqSent {
			t.Errorf("Test041: expected ErrCancelReqSent but got %v", cliErr41)
		}

		if reply41 != nil {
			t.Errorf("Test041: expected reply41 to be nil, but got %v", reply41)
		}

		// confirm that server side function is unblocked too
		vv("about to verify that server side context was cancelled.")
		<-test041callFinished

	})
}
