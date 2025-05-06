package rpc25519

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	//"reflect"
	"path/filepath"
	"strings"
	"testing"
	"time"

	cv "github.com/glycerine/goconvey/convey"
	myblake3 "github.com/glycerine/rpc25519/hash"
)

var _ = ioutil.TempDir
var _ = filepath.Dir

func TestMain(m *testing.M) {

	var exitVal int
	synctestRun(func() {
		func() {
			//vv("TestMain running.")

			// create a temp dir for certs/ca,
			// and configure to uset it.
			tmp, err := ioutil.TempDir("", "rpc25519-tests-temp-dir")
			panicOn(err)
			//vv("running tests in tmp dir: %v", tmp)
			//vv("comment this next line to preserve the tmp dir for inspection.")
			defer os.RemoveAll(tmp)

			// write temp certs here
			os.Setenv("XDG_CONFIG_HOME", tmp)
			cwd, err := os.Getwd()
			_ = cwd
			panicOn(err)

			// make the directories
			dirCerts := GetCertsDir()
			_ = dirCerts
			dirCA := GetPrivateCertificateAuthDir()
			_ = dirCA
			//vv("dirCerts = '%v'", dirCerts)

			ca1path := filepath.Dir(dirCA)

			panicOn(SelfyNewKey("node", ca1path))
			panicOn(SelfyNewKey("client", ca1path))

			os.Chdir(tmp)
			defer os.Chdir(cwd)

			//vv("running in temp dir '%v'", tmp)

			exitVal = m.Run()
		}()
	})
	// Do stuff AFTER the tests

	// clean up the temp dir.

	os.Exit(exitVal)
}

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
    if hdr, ok := rpc25519.HDRFromContext(ctx); ok {
		if ok {
			fmt.Printf("WantsContext called with HDR = '%v'; HDR.Nc.RemoteAddr() gives '%v'; HDR.Nc.LocalAddr() gives '%v'\n", hdr.String(), hdr.Nc.RemoteAddr(), hdr.Nc.LocalAddr())

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
		//vv("good 006: past ServiceName with dot . test")

		/*
			// actually we read what we can into it. Since greenpack is
			// designed to be evolvable; in other words,
			// to ignore unknown and missing fields, this is not a legit test
			// when using greenpack.

			// Bad type.
			reply = new(Reply)
			err = client.Call("Arith.Add", reply, reply) // args, reply would be the correct thing to use
			if err == nil {
				//vv("err was nil but should not have been!")
				t.Error("expected error calling Arith.Add with wrong arg type")
			} else if !strings.Contains(err.Error(), "type") {
				t.Error("expected error about type; got", err)
			}
			//vv("good 006: past bad type")

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

		//cv.So(true, cv.ShouldBeTrue)
		//vv("good: end of 006 test")
	})
	//})
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

		//vv("server Start() returned serverAddr = '%v'", serverAddr)

		// net/rpc API on server
		srv.Register(new(Arith))
		srv.Register(new(Embed))
		srv.RegisterName("net.rpc.Arith", new(Arith))
		srv.Register(&BuiltinTypes{})

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

		//vv("server Start() returned serverAddr = '%v'", serverAddr)

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

	//vv("server Start() returned serverAddr = '%v'", serverAddr)

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

// this also has 041 in it.
func Test040_remote_cancel_by_context(t *testing.T) {

	cv.Convey("remote cancellation", t, func() {

		cfg := NewConfig()
		cfg.TCPonly_no_TLS = false
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
		<-mustCancelMe.callStarted // synctest blocked here
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
		<-mustCancelMe.callFinished
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
		<-mustCancelMe.callStarted
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
		<-mustCancelMe.callFinished

	})
}

func Test045_upload(t *testing.T) {

	cv.Convey("upload a large file in parts from client to server", t, func() {

		cfg := NewConfig()
		cfg.TCPonly_no_TLS = false

		cfg.ServerAddr = "127.0.0.1:0"
		srv := NewServer("srv_test045", cfg)

		serverAddr, err := srv.Start()
		panicOn(err)
		defer srv.Close()

		//vv("server Start() returned serverAddr = '%v'", serverAddr)

		// name must be "__fileUploader" for cli.go Uploader to work.
		uploaderName := "__fileUploader"
		streamer := NewServerSideUploadState()
		srv.RegisterUploadReaderFunc(uploaderName, streamer.ReceiveFileInParts)

		cfg.ClientDialToHostPort = serverAddr.String()
		client, err := NewClient("test045", cfg)
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

func Test055_download(t *testing.T) {

	cv.Convey("download a large file in parts from server to client, the opposite direction of the previous test.", t, func() {

		cfg := NewConfig()
		cfg.TCPonly_no_TLS = false

		// start server
		cfg.ServerAddr = "127.0.0.1:0"
		srv := NewServer("srv_test055", cfg)

		serverAddr, err := srv.Start()
		panicOn(err)
		defer srv.Close()

		// register streamer func with server
		downloaderName := "downloaderName"
		ssss := &ServerSendsDownloadStateTest{}
		srv.RegisterServerSendsDownloadFunc(downloaderName, ssss.ServerSendsDownloadTest)

		// start client
		cfg.ClientDialToHostPort = serverAddr.String()
		client, err := NewClient("test055", cfg)
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
		downloader, err := client.RequestDownload(ctx55, downloaderName, "test055_not_real_download")
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

func Test065_bidirectional_download_and_upload(t *testing.T) {

	cv.Convey("we should be able to register a server func that does uploads and downloads sequentially or simultaneously.", t, func() {

		cfg := NewConfig()
		cfg.TCPonly_no_TLS = false

		// start server
		cfg.ServerAddr = "127.0.0.1:0"
		srv := NewServer("srv_test065", cfg)

		serverAddr, err := srv.Start()
		panicOn(err)
		defer srv.Close()

		// register streamer func with server
		streamerName := "bi-streamer Name"
		bi := &ServeBistreamState{}
		srv.RegisterBistreamFunc(streamerName, bi.ServeBistream)

		// start client
		cfg.ClientDialToHostPort = serverAddr.String()
		client, err := NewClient("test065", cfg)
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
		//vv("065 upload starting, with CallID = '%v'", originalStreamCallID)
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

func Test100_BlakeHashesAgree(t *testing.T) {

	cv.Convey("blake3 hash strings should be consistent across all utility func", t, func() {

		h := myblake3.NewBlake3()
		data := []byte("hello world!")

		a := blake3OfBytesString(data)
		b := h.Hash32(data)
		c := myblake3.Blake3OfBytesString(data)
		vv(`blake3OfBytesString("hello world!") = '%v'`, a)
		vv(`myblake3.Hash32("hello world!")     = '%v'`, b)
		if a != b {
			panic("a != b, hashes are different!")
		}
		if c != b {
			panic("c != b, hashes are different!")
		}

		h.Reset()
		h.Write(data)
		d := h.SumString()
		if d != b {
			panic("d != b, hashes are different!")
		}

		h.Reset()
		h.Write(data[:3])
		h.Write(data[3:])
		e := h.SumString()
		if e != b {
			panic("e != b, hashes are different!")
		}
	})
}
