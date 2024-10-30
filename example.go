package rpc25519

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var _ = fmt.Printf
var _ = time.Now

//go:generate greenpack

// These example test structs and types were
// moved here (example.go) from cli_test.go so
// example_test_gen.go can build when not testing.
// This is to enable using/testing greenpack
// rather than gobs by default.

type Args struct {
	A int `zid:"0"`
	B int `zid:"1"`
}

type Reply struct {
	C int `zid:"0"`
}

type Arith int

// net/rpc comment:
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

type Simple int

func (t *Simple) Exported(args Args, reply *Reply) error {
	reply.C = args.A + args.B
	return nil
}

type Embed struct {
	Simple `zid:"0"`
}

type BuiltinTypes struct {
	Placeholder int `zid:"0"` // greenpack refuses to serialize an empty struct.
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

// these are placed here for greenpack, so generate will
// write greenpack serialization code for them.

// Request is part of the net/rpc API. Its docs:
//
// Request is a header written before every RPC call. It is used internally
// but documented here as an aid to debugging, such as when analyzing
// network traffic.
type Request struct {
	ServiceMethod string   `zid:"0"` // format: "Service.Method"
	Seq           uint64   `zid:"1"` // sequence number chosen by client
	next          *Request // for free list in Server
}

// InvalidRequest used instead of struct{} since greenpack needs one member element.
type InvalidRequest struct {
	Placeholder int `zid:"0"`
}

// Response is part of the net/rpc API. Its docs:
//
// Response is a header written before every RPC return. It is used internally
// but documented here as an aid to debugging, such as when analyzing
// network traffic.
type Response struct {
	ServiceMethod string    `zid:"0"` // echoes that of the Request
	Seq           uint64    `zid:"1"` // echoes that of the request
	Error         string    `zid:"2"` // error, if any.
	next          *Response // for free list in Server
}
