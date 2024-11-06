package rpc25519

import (
	"fmt"
	//"reflect"
	"strings"
	"testing"
	"time"

	cv "github.com/glycerine/goconvey/convey"
)

var _ = fmt.Printf
var _ = time.Now

/*
   Note: these testing types (originally from net/rpc)
   were moved to example.go because having
   generating greenapck on a test file makes cli_test_gen.go
   which does not look like a test file so a main build tries
   to use it and then cannot find these types.

type Args struct {
	A, B int
}

type Reply struct {
	C int
}

type Arith int

// Some of Arith's methods have value args, some have pointer args. That's deliberate.

func (t *Arith) Add(args Args, reply *Reply) error {
	reply.C = args.A + args.B
	//vv("Arith.Add(%v + %v) called.", args.A, args.B)
	return nil
}

func (t *Arith) Mul(args *Args, reply *Reply) error {
	reply.C = args.A * args.B
	//vv("Arith.Mul(%v * %v) called.", args.A, args.B)
	return nil
}

func (t *Arith) Div(args Args, reply *Reply) error {
	if args.B == 0 {
		return errors.New("divide by zero")
	}
	reply.C = args.A / args.B
	//vv("Arith.Div(%v / %v) called.", args.A, args.B)
	return nil
}

func (t *Arith) String(args *Args, reply *string) error {
	*reply = fmt.Sprintf("%d+%d=%d", args.A, args.B, args.A+args.B)
	//vv("Arith.Strings(%v, %v -> '%v') called.", args.A, args.B, *reply)
	return nil
}

func (t *Arith) Scan(args string, reply *Reply) (err error) {
	_, err = fmt.Sscan(args, &reply.C)
	return
}

func (t *Arith) Error(args *Args, reply *Reply) error {
	panic("ERROR")
}

func (t *Arith) SleepMilli(args *Args, reply *Reply) error {
	time.Sleep(time.Duration(args.A) * time.Millisecond)
	return nil
}

type hidden int

func (t *hidden) Exported(args Args, reply *Reply) error {
	reply.C = args.A + args.B
	return nil
}

type Embed struct {
	hidden
}

type BuiltinTypes struct {
}

func (BuiltinTypes) Map(args *Args, reply *map[int]int) error {
	(*reply)[args.A] = args.B
	return nil
}

func (BuiltinTypes) Slice(args *Args, reply *[]int) error {
	*reply = append(*reply, args.A, args.B)
	return nil
}

func (BuiltinTypes) Array(args *Args, reply *[2]int) error {
	(*reply)[0] = args.A
	(*reply)[1] = args.B
	return nil
}

// mimic Array's reply
func (BuiltinTypes) WantsContext(ctx context.Context, args *Args, reply *[2]int) error {
	if hdr := ctx.Value("HDR"); hdr != nil {
		h, ok := hdr.(*HDR)
		if ok {
			fmt.Printf("WantsContext called with HDR = '%v'; HDR.Nc.RemoteAddr() gives '%v'; HDR.Nc.LocalAddr() gives '%v'\n", h.String(), h.Nc.RemoteAddr(), h.Nc.LocalAddr())

			(*reply)[0] = args.A
			(*reply)[1] = args.B
		}
	} else {
		fmt.Println("HDR not found")
	}
	return nil
}
*/

// This is a port of net/rpc server_test.go testRPC().
// See attic/net_server_test.go:169 herein, which is a copy, with renaming
// to build here without conflict over who is the real Server{}.
func Test006_RoundTrip_Using_NetRPC_API_TCP(t *testing.T) {

	cv.Convey("basic TCP with rpc25519 using the net/rpc API: register a callback on the server, and have the client call it.", t, func() {

		cfg := NewConfig()
		cfg.TCPonly_no_TLS = true

		path := "my-keep-private-dir/psk.binary"
		panicOn(setupPSK(path))
		cfg.PreSharedKeyPath = path
		cfg.ReadTimeout = 2 * time.Second
		cfg.WriteTimeout = 2 * time.Second

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
		vv("good 006, got back reply '%#v'", reply)

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
		vv("good 006: past nonexistent method")

		// Unknown service
		args = &Args{7, 8}
		reply = new(Reply)
		err = client.Call("Arith.Unknown", args, reply, nil)
		if err == nil {
			t.Error("expected error calling unknown service")
		} else if !strings.Contains(err.Error(), "method") {
			t.Error("expected error about method; got", err)
		}
		vv("good 006: past unknown service")

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
		vv("good 006: past out of order")

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
		vv("good 006: past error test")

		args = &Args{7, 8}
		reply = new(Reply)
		err = client.Call("Arith.Mul", args, reply, nil)
		if err != nil {
			t.Errorf("Mul: expected no error but got string %q", err.Error())
		}
		if reply.C != args.A*args.B {
			t.Errorf("Mul: expected %d got %d", reply.C, args.A*args.B)
		}
		vv("good 006: past Arith.Mul test")

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
		vv("good 006: past ServiceName with dot . test")

		/*
			// actually we read what we can into it. Since greenpack is
			// designed to be evolvable; in other words,
			// to ignore unknown and missing fields, this is not a legit test
			// when using greenpack.

			// Bad type.
			reply = new(Reply)
			err = client.Call("Arith.Add", reply, reply) // args, reply would be the correct thing to use
			if err == nil {
				vv("err was nil but should not have been!")
				t.Error("expected error calling Arith.Add with wrong arg type")
			} else if !strings.Contains(err.Error(), "type") {
				t.Error("expected error about type; got", err)
			}
			vv("good 006: past bad type")

			// Non-struct argument: won't work with greenpack

			const Val = 12345
			str := fmt.Sprint(Val)
			reply = new(Reply)
			err = client.Call("Arith.Scan", &str, reply)
			if err != nil {
				t.Errorf("Scan: expected no error but got string %q", err.Error())
			} else if reply.C != Val {
				t.Errorf("Scan: expected %d got %d", Val, reply.C)
			}

			// Non-struct reply: won't work under greenpack

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

			// BuiltinTypes

			// Map
			args = &Args{7, 8}
			replyMap := map[int]int{}
			err = client.Call("BuiltinTypes.Map", args, &replyMap)
			if err != nil {
				t.Errorf("Map: expected no error but got string %q", err.Error())
			}
			if replyMap[args.A] != args.B {
				t.Errorf("Map: expected %d got %d", args.B, replyMap[args.A])
			}

			// Slice
			args = &Args{7, 8}
			replySlice := []int{}
			err = client.Call("BuiltinTypes.Slice", args, &replySlice)
			if err != nil {
				t.Errorf("Slice: expected no error but got string %q", err.Error())
			}
			if e := []int{args.A, args.B}; !reflect.DeepEqual(replySlice, e) {
				t.Errorf("Slice: expected %v got %v", e, replySlice)
			}

			// Array
			args = &Args{7, 8}
			replyArray := [2]int{}
			err = client.Call("BuiltinTypes.Array", args, &replyArray)
			if err != nil {
				t.Errorf("Array: expected no error but got string %q", err.Error())
			}
			if e := [2]int{args.A, args.B}; !reflect.DeepEqual(replyArray, e) {
				t.Errorf("Array: expected %v got %v", e, replyArray)
			}
		*/

		cv.So(true, cv.ShouldBeTrue)
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
		client, err := NewClient("test007", cfg)
		panicOn(err)
		err = client.Start()
		panicOn(err)

		defer client.Close()

		// net/rpc API on client, ported from net_server_test.go
		//var args *Args
		//var reply *Reply

		// Synchronous calls
		args := &Args{7, 8}
		reply := new(Reply)
		err = client.Call("Arith.Add", args, reply, nil)
		if err != nil {
			t.Errorf("Add: expected no error but got string %q", err.Error())
		}
		if reply.C != args.A+args.B {
			t.Errorf("Add: expected %d got %d", reply.C, args.A+args.B)
		}

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

		// Unknown service
		args = &Args{7, 8}
		reply = new(Reply)
		err = client.Call("Arith.Unknown", args, reply, nil)
		if err == nil {
			t.Error("expected error calling unknown service")
		} else if !strings.Contains(err.Error(), "method") {
			t.Error("expected error about method; got", err)
		}

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

		args = &Args{7, 8}
		reply = new(Reply)
		err = client.Call("Arith.Mul", args, reply, nil)
		if err != nil {
			t.Errorf("Mul: expected no error but got string %q", err.Error())
		}
		if reply.C != args.A*args.B {
			t.Errorf("Mul: expected %d got %d", reply.C, args.A*args.B)
		}

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

		/*
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

			// BuiltinTypes

			// Map
			args = &Args{7, 8}
			replyMap := map[int]int{}
			err = client.Call("BuiltinTypes.Map", args, &replyMap)
			if err != nil {
				t.Errorf("Map: expected no error but got string %q", err.Error())
			}
			if replyMap[args.A] != args.B {
				t.Errorf("Map: expected %d got %d", args.B, replyMap[args.A])
			}

			// Slice
			args = &Args{7, 8}
			replySlice := []int{}
			err = client.Call("BuiltinTypes.Slice", args, &replySlice)
			if err != nil {
				t.Errorf("Slice: expected no error but got string %q", err.Error())
			}
			if e := []int{args.A, args.B}; !reflect.DeepEqual(replySlice, e) {
				t.Errorf("Slice: expected %v got %v", e, replySlice)
			}

			// Array
			args = &Args{7, 8}
			replyArray := [2]int{}
			err = client.Call("BuiltinTypes.Array", args, &replyArray)
			if err != nil {
				t.Errorf("Array: expected no error but got string %q", err.Error())
			}
			if e := [2]int{args.A, args.B}; !reflect.DeepEqual(replyArray, e) {
				t.Errorf("Array: expected %v got %v", e, replyArray)
			}
		*/
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
		bit := &BuiltinTypes{}
		srv.Register(bit)

		cfg.ClientDialToHostPort = serverAddr.String()

		// The QUIC client will share the port with the QUIC server typically;
		// port sharing is on by default. So don't bother
		// setting the ClientHostPort. It is just a
		// suggestion anyway.
		//cfg.ClientHostPort = fmt.Sprintf("%v:%v", host, port+1)

		client, err := NewClient("test008", cfg)
		panicOn(err)
		err = client.Start()
		panicOn(err)

		defer client.Close()

		//vv("client local = '%v'", client.LocalAddr())

		// net/rpc API on client, ported from net_server_test.go
		//var args *Args
		//var reply *Reply

		// Synchronous calls
		args := &Args{7, 8}
		reply := new(Reply)
		err = client.Call("Arith.Add", args, reply, nil)
		if err != nil {
			t.Errorf("Add: expected no error but got string %q", err.Error())
		}
		if reply.C != args.A+args.B {
			t.Errorf("Add: expected %d got %d", reply.C, args.A+args.B)
		}

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

		// Unknown service
		args = &Args{7, 8}
		reply = new(Reply)
		err = client.Call("Arith.Unknown", args, reply, nil)
		if err == nil {
			t.Error("expected error calling unknown service")
		} else if !strings.Contains(err.Error(), "method") {
			t.Error("expected error about method; got", err)
		}

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

		args = &Args{7, 8}
		reply = new(Reply)
		err = client.Call("Arith.Mul", args, reply, nil)
		if err != nil {
			t.Errorf("Mul: expected no error but got string %q", err.Error())
		}
		if reply.C != args.A*args.B {
			t.Errorf("Mul: expected %d got %d", reply.C, args.A*args.B)
		}

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

		/*
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

			// BuiltinTypes

			// Map
			args = &Args{7, 8}
			replyMap := map[int]int{}
			err = client.Call("BuiltinTypes.Map", args, &replyMap)
			if err != nil {
				t.Errorf("Map: expected no error but got string %q", err.Error())
			}
			if replyMap[args.A] != args.B {
				t.Errorf("Map: expected %d got %d", args.B, replyMap[args.A])
			}

			// Slice
			args = &Args{7, 8}
			replySlice := []int{}
			err = client.Call("BuiltinTypes.Slice", args, &replySlice)
			if err != nil {
				t.Errorf("Slice: expected no error but got string %q", err.Error())
			}
			if e := []int{args.A, args.B}; !reflect.DeepEqual(replySlice, e) {
				t.Errorf("Slice: expected %v got %v", e, replySlice)
			}

			// Array
			args = &Args{7, 8}
			replyArray := [2]int{}
			err = client.Call("BuiltinTypes.Array", args, &replyArray)
			if err != nil {
				t.Errorf("Array: expected no error but got string %q", err.Error())
			}
			if e := [2]int{args.A, args.B}; !reflect.DeepEqual(replyArray, e) {
				t.Errorf("Array: expected %v got %v", e, replyArray)
			}

			err = client.Call("BuiltinTypes.WantsContext", args, &replyArray)
			if err != nil {
				t.Errorf("Array: expected no error but got string %q", err.Error())
			}
			if e := [2]int{args.A, args.B}; !reflect.DeepEqual(replyArray, e) {
				t.Errorf("Array: expected %v got %v", e, replyArray)
			}
		*/
	})
}

func (t *Hello) Say(args *BenchmarkMessage, reply *BenchmarkMessage) (err error) {
	s := "OK"
	var i int32 = 100
	*reply = *args
	reply.Field1 = s
	reply.Field2 = i

	//vv("Hello.Say has been called.")
	return nil
}

func BenchmarkHelloRpcxMessage(b *testing.B) {

	cfg := NewConfig()
	cfg.UseQUIC = true

	cfg.ServerAddr = "127.0.0.1:0"
	srv := NewServer("srv_bench", cfg)

	serverAddr, err := srv.Start()
	panicOn(err)
	defer srv.Close()

	vv("server Start() returned serverAddr = '%v'", serverAddr)

	// net/rpc API on server
	srv.Register(new(Hello))

	cfg.ClientDialToHostPort = serverAddr.String()

	// The QUIC client will share the port with the QUIC server typically;
	// port sharing is on by default. So don't bother
	// setting the ClientHostPort. It is just a
	// suggestion anyway.
	//cfg.ClientHostPort = fmt.Sprintf("%v:%v", host, port+1)

	client, err := NewClient("cli_bench", cfg)
	panicOn(err)
	err = client.Start()
	panicOn(err)
	defer client.Close()

	args := &BenchmarkMessage{}
	reply := &BenchmarkMessage{}

	sz := args.Msgsize() * 2
	b.SetBytes(int64(sz))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = client.Call("Hello.Say", args, reply, nil)
		panicOn(err)
		if reply.Field1 != "OK" || reply.Field2 != 100 {
			panic("Hello.Say not called?")
		}
	}
}
