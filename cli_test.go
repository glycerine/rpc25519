package rpc25519

import (
	"fmt"
	"strings"
	"testing"
	"time"

	cv "github.com/glycerine/goconvey/convey"
)

var _ = fmt.Printf
var _ = time.Now

// This is a port of net/rpc server_test.go testRPC().
// See net_server_test.go:169 herein, which is a copy, with renaming
// to build here without conflict over who is the real Server{}.
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
		//var args *Args
		//var reply *Reply

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

		// Methods exported from unexported embedded structs
		args = &Args{7, 0}
		reply = new(Reply)
		err = client.Call("Embed.Exported", args, reply)
		if err != nil {
			t.Errorf("Add: expected no error but got string %q", err.Error())
		}
		if reply.C != args.A+args.B {
			t.Errorf("Add: expected %d got %d", reply.C, args.A+args.B)
		}

		// Nonexistent method
		args = &Args{7, 0}
		reply = new(Reply)
		err = client.Call("Arith.BadOperation", args, reply)
		// expect an error
		if err == nil {
			t.Error("BadOperation: expected error")
		} else if !strings.HasPrefix(err.Error(), "rpc: can't find method ") {
			t.Errorf("BadOperation: expected can't find method error; got %q", err)
		}

		// Unknown service
		args = &Args{7, 8}
		reply = new(Reply)
		err = client.Call("Arith.Unknown", args, reply)
		if err == nil {
			t.Error("expected error calling unknown service")
		} else if !strings.Contains(err.Error(), "method") {
			t.Error("expected error about method; got", err)
		}

		// Out of order.
		args = &Args{7, 8}
		mulReply := new(Reply)
		mulCall := client.Go("Arith.Mul", args, mulReply, nil)
		addReply := new(Reply)
		addCall := client.Go("Arith.Add", args, addReply, nil)

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

		// Error test
		args = &Args{7, 0}
		reply = new(Reply)
		err = client.Call("Arith.Div", args, reply)
		// expect an error: zero divide
		if err == nil {
			t.Error("Div: expected error")
		} else if err.Error() != "divide by zero" {
			t.Error("Div: expected divide by zero error; got", err)
		}

		// Bad type.
		reply = new(Reply)
		err = client.Call("Arith.Add", reply, reply) // args, reply would be the correct thing to use
		if err == nil {
			t.Error("expected error calling Arith.Add with wrong arg type")
		} else if !strings.Contains(err.Error(), "type") {
			t.Error("expected error about type; got", err)
		}

		// Non-struct argument
		const Val = 12345
		str := fmt.Sprint(Val)
		reply = new(Reply)
		err = client.Call("Arith.Scan", &str, reply)
		if err != nil {
			t.Errorf("Scan: expected no error but got string %q", err.Error())
		} else if reply.C != Val {
			t.Errorf("Scan: expected %d got %d", Val, reply.C)
		}

		// Non-struct reply
		args = &Args{27, 35}
		str = ""
		err = client.Call("Arith.String", args, &str)
		if err != nil {
			t.Errorf("String: expected no error but got string %q", err.Error())
		}
		expect := fmt.Sprintf("%d+%d=%d", args.A, args.B, args.A+args.B)
		if str != expect {
			t.Errorf("String: expected %s got %s", expect, str)
		}

		args = &Args{7, 8}
		reply = new(Reply)
		err = client.Call("Arith.Mul", args, reply)
		if err != nil {
			t.Errorf("Mul: expected no error but got string %q", err.Error())
		}
		if reply.C != args.A*args.B {
			t.Errorf("Mul: expected %d got %d", reply.C, args.A*args.B)
		}

		// ServiceName contain "." character
		args = &Args{7, 8}
		reply = new(Reply)
		err = client.Call("net.rpc.Arith.Add", args, reply)
		if err != nil {
			t.Errorf("Add: expected no error but got string %q", err.Error())
		}
		if reply.C != args.A+args.B {
			t.Errorf("Add: expected %d got %d", reply.C, args.A+args.B)
		}

	})
}

// and with TLS

func Test007_RoundTrip_Using_NetRPC_API_TLS(t *testing.T) {

	cv.Convey("TLS over TCP with rpc25519 using the net/rpc API: register a callback on the server, and have the client call it.", t, func() {

		cfg := NewConfig()
		cfg.TCPonly_no_TLS = false

		cfg.ServerAddr = "127.0.0.1:0"
		srv := NewServer("srv_test007", cfg)

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
		//var args *Args
		//var reply *Reply

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

		// Methods exported from unexported embedded structs
		args = &Args{7, 0}
		reply = new(Reply)
		err = client.Call("Embed.Exported", args, reply)
		if err != nil {
			t.Errorf("Add: expected no error but got string %q", err.Error())
		}
		if reply.C != args.A+args.B {
			t.Errorf("Add: expected %d got %d", reply.C, args.A+args.B)
		}

		// Nonexistent method
		args = &Args{7, 0}
		reply = new(Reply)
		err = client.Call("Arith.BadOperation", args, reply)
		// expect an error
		if err == nil {
			t.Error("BadOperation: expected error")
		} else if !strings.HasPrefix(err.Error(), "rpc: can't find method ") {
			t.Errorf("BadOperation: expected can't find method error; got %q", err)
		}

		// Unknown service
		args = &Args{7, 8}
		reply = new(Reply)
		err = client.Call("Arith.Unknown", args, reply)
		if err == nil {
			t.Error("expected error calling unknown service")
		} else if !strings.Contains(err.Error(), "method") {
			t.Error("expected error about method; got", err)
		}

		// Out of order.
		args = &Args{7, 8}
		mulReply := new(Reply)
		mulCall := client.Go("Arith.Mul", args, mulReply, nil)
		addReply := new(Reply)
		addCall := client.Go("Arith.Add", args, addReply, nil)

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

		// Error test
		args = &Args{7, 0}
		reply = new(Reply)
		err = client.Call("Arith.Div", args, reply)
		// expect an error: zero divide
		if err == nil {
			t.Error("Div: expected error")
		} else if err.Error() != "divide by zero" {
			t.Error("Div: expected divide by zero error; got", err)
		}

		// Bad type.
		reply = new(Reply)
		err = client.Call("Arith.Add", reply, reply) // args, reply would be the correct thing to use
		if err == nil {
			t.Error("expected error calling Arith.Add with wrong arg type")
		} else if !strings.Contains(err.Error(), "type") {
			t.Error("expected error about type; got", err)
		}

		// Non-struct argument
		const Val = 12345
		str := fmt.Sprint(Val)
		reply = new(Reply)
		err = client.Call("Arith.Scan", &str, reply)
		if err != nil {
			t.Errorf("Scan: expected no error but got string %q", err.Error())
		} else if reply.C != Val {
			t.Errorf("Scan: expected %d got %d", Val, reply.C)
		}

		// Non-struct reply
		args = &Args{27, 35}
		str = ""
		err = client.Call("Arith.String", args, &str)
		if err != nil {
			t.Errorf("String: expected no error but got string %q", err.Error())
		}
		expect := fmt.Sprintf("%d+%d=%d", args.A, args.B, args.A+args.B)
		if str != expect {
			t.Errorf("String: expected %s got %s", expect, str)
		}

		args = &Args{7, 8}
		reply = new(Reply)
		err = client.Call("Arith.Mul", args, reply)
		if err != nil {
			t.Errorf("Mul: expected no error but got string %q", err.Error())
		}
		if reply.C != args.A*args.B {
			t.Errorf("Mul: expected %d got %d", reply.C, args.A*args.B)
		}

		// ServiceName contain "." character
		args = &Args{7, 8}
		reply = new(Reply)
		err = client.Call("net.rpc.Arith.Add", args, reply)
		if err != nil {
			t.Errorf("Add: expected no error but got string %q", err.Error())
		}
		if reply.C != args.A+args.B {
			t.Errorf("Add: expected %d got %d", reply.C, args.A+args.B)
		}

	})
}

// and with QUIC

func Test008_RoundTrip_Using_NetRPC_API_QUIC(t *testing.T) {

	cv.Convey("QUIC with rpc25519 using the net/rpc API: register a callback on the server, and have the client call it.", t, func() {

		cfg := NewConfig()
		cfg.UseQUIC = true

		cfg.ServerAddr = "127.0.0.1:0"
		srv := NewServer("srv_test008", cfg)

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
		//var args *Args
		//var reply *Reply

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

		// Methods exported from unexported embedded structs
		args = &Args{7, 0}
		reply = new(Reply)
		err = client.Call("Embed.Exported", args, reply)
		if err != nil {
			t.Errorf("Add: expected no error but got string %q", err.Error())
		}
		if reply.C != args.A+args.B {
			t.Errorf("Add: expected %d got %d", reply.C, args.A+args.B)
		}

		// Nonexistent method
		args = &Args{7, 0}
		reply = new(Reply)
		err = client.Call("Arith.BadOperation", args, reply)
		// expect an error
		if err == nil {
			t.Error("BadOperation: expected error")
		} else if !strings.HasPrefix(err.Error(), "rpc: can't find method ") {
			t.Errorf("BadOperation: expected can't find method error; got %q", err)
		}

		// Unknown service
		args = &Args{7, 8}
		reply = new(Reply)
		err = client.Call("Arith.Unknown", args, reply)
		if err == nil {
			t.Error("expected error calling unknown service")
		} else if !strings.Contains(err.Error(), "method") {
			t.Error("expected error about method; got", err)
		}

		// Out of order.
		args = &Args{7, 8}
		mulReply := new(Reply)
		mulCall := client.Go("Arith.Mul", args, mulReply, nil)
		addReply := new(Reply)
		addCall := client.Go("Arith.Add", args, addReply, nil)

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

		// Error test
		args = &Args{7, 0}
		reply = new(Reply)
		err = client.Call("Arith.Div", args, reply)
		// expect an error: zero divide
		if err == nil {
			t.Error("Div: expected error")
		} else if err.Error() != "divide by zero" {
			t.Error("Div: expected divide by zero error; got", err)
		}

		// Bad type.
		reply = new(Reply)
		err = client.Call("Arith.Add", reply, reply) // args, reply would be the correct thing to use
		if err == nil {
			t.Error("expected error calling Arith.Add with wrong arg type")
		} else if !strings.Contains(err.Error(), "type") {
			t.Error("expected error about type; got", err)
		}

		// Non-struct argument
		const Val = 12345
		str := fmt.Sprint(Val)
		reply = new(Reply)
		err = client.Call("Arith.Scan", &str, reply)
		if err != nil {
			t.Errorf("Scan: expected no error but got string %q", err.Error())
		} else if reply.C != Val {
			t.Errorf("Scan: expected %d got %d", Val, reply.C)
		}

		// Non-struct reply
		args = &Args{27, 35}
		str = ""
		err = client.Call("Arith.String", args, &str)
		if err != nil {
			t.Errorf("String: expected no error but got string %q", err.Error())
		}
		expect := fmt.Sprintf("%d+%d=%d", args.A, args.B, args.A+args.B)
		if str != expect {
			t.Errorf("String: expected %s got %s", expect, str)
		}

		args = &Args{7, 8}
		reply = new(Reply)
		err = client.Call("Arith.Mul", args, reply)
		if err != nil {
			t.Errorf("Mul: expected no error but got string %q", err.Error())
		}
		if reply.C != args.A*args.B {
			t.Errorf("Mul: expected %d got %d", reply.C, args.A*args.B)
		}

		// ServiceName contain "." character
		args = &Args{7, 8}
		reply = new(Reply)
		err = client.Call("net.rpc.Arith.Add", args, reply)
		if err != nil {
			t.Errorf("Add: expected no error but got string %q", err.Error())
		}
		if reply.C != args.A+args.B {
			t.Errorf("Add: expected %d got %d", reply.C, args.A+args.B)
		}

	})
}