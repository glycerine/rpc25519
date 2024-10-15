package rpc25519

import (
	"fmt"
	"testing"
	"time"

	cv "github.com/glycerine/goconvey/convey"
)

var _ = fmt.Printf
var _ = time.Now

func Test006_RoundTrip_Using_NetRPC_API_TCP(t *testing.T) {

	cv.Convey("basic TCP with rpc25519 using the net/rpc API: register a callback on the server, and have the client call it.", t, func() {

		cfg := NewConfig()
		cfg.TCPonly_no_TLS = true

		cfg.ServerAddr = "127.0.0.1:0"
		srv := NewServer("srv_test001", cfg)

		serverAddr, err := srv.Start()
		panicOn(err)
		defer srv.Close()

		vv("server Start() returned serverAddr = '%v'", serverAddr)

		// net/rpc API on server
		srv.Register(new(Arith))
		srv.Register(new(Embed))
		srv.RegisterName("net.rpc.Arith", new(Arith))
		srv.Register(BuiltinTypes{})

		cfg.ClientDialToHostPort = serverAddr.String()
		client, err := NewClient("test006", cfg)
		panicOn(err)
		defer client.Close()

		// net/rpc API on client, ported from net_server_test.go
		if false {
			// Synchronous calls
			args := &Args{7, 8}
			reply := new(Reply)
			err = client.Call("Arith.Add", args, reply)
			if err != nil {
				t.Errorf("Add: expected no error but got string %q", err.Error())
			}
			if reply.C != args.A+args.B {
				t.Errorf("Add: expected %d got %d", reply.C, args.A+args.B)
			}
			vv("Arith.Add got: %v + %v = %v", args.A, args.B, reply.C)
		}
		// Out of order.
		args := &Args{7, 8}
		mulReply := new(Reply)
		mulCall := client.Go("Arith.Mul", args, mulReply, nil)
		addReply := new(Reply)
		addCall := client.Go("Arith.Add", args, addReply, nil)
		_ = addCall
		//		addCall = <-addCall.Done
		//		if addCall.Error != nil {
		//			t.Errorf("Add: expected no error but got string %q", addCall.Error.Error())
		//		}
		//		if addReply.C != args.A+args.B {
		//			t.Errorf("Add: expected %d got %d", addReply.C, args.A+args.B)
		//		}

		mulCall = <-mulCall.Done
		if mulCall.Error != nil {
			t.Errorf("Mul: expected no error but got string %q", mulCall.Error.Error())
		}
		if mulReply.C != args.A*args.B {
			t.Errorf("Mul: expected %d got %d", mulReply.C, args.A*args.B)
		}

	})
}
