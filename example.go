package rpc25519

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var _ = fmt.Printf
var _ = time.Now

// we are 2298 bytes without the flags; try to get comparable
// to the 581 protoc size for BenchmarkMessage.
//go:generate greenpack -fast-strings -alltuple

// These example test structs and types were
// moved here (example.go) from cli_test.go so
// example_test_gen.go can build when not testing.
// This is to enable using/testing greenpack
// rather than the old serialization system by default.

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

// for the cli_test.go benchmark
// ala https://github.com/rpcx-ecosystem/rpcx-benchmark
// translation:
// https://github-com.translate.goog/rpcx-ecosystem/rpcx-benchmark?_x_tr_sl=auto&_x_tr_tl=en&_x_tr_hl=en-US&_x_tr_pto=wapp

type Hello struct {
	Placeholder int `zid:"0"` // must have public field or greenpack will ignore it.
}

type BenchmarkMessage struct {
	Field1   string   `zid:"0"`
	Field9   string   `zid:"1"`
	Field18  string   `zid:"2"`
	Field80  bool     `zid:"3"`
	Field81  bool     `zid:"4"`
	Field2   int32    `zid:"5"`
	Field3   int32    `zid:"6"`
	Field280 int32    `zid:"7"`
	Field6   int32    `zid:"8"`
	Field22  int64    `zid:"9"`
	Field4   string   `zid:"10"`
	Field5   []uint64 `zid:"11"`
	Field59  bool     `zid:"12"`
	Field7   string   `zid:"13"`
	Field16  int32    `zid:"14"`
	Field130 int32    `zid:"15"`
	Field12  bool     `zid:"16"`
	Field17  bool     `zid:"17"`
	Field13  bool     `zid:"18"`
	Field14  bool     `zid:"19"`
	Field104 int32    `zid:"20"`
	Field100 int32    `zid:"21"`
	Field101 int32    `zid:"22"`
	Field102 string   `zid:"23"`
	Field103 string   `zid:"24"`
	Field29  int32    `zid:"25"`
	Field30  bool     `zid:"26"`
	Field60  int32    `zid:"27"`
	Field271 int32    `zid:"28"`
	Field272 int32    `zid:"29"`
	Field150 int32    `zid:"30"`
	Field23  int32    `zid:"31"`
	Field24  bool     `zid:"32"`
	Field25  int32    `zid:"33"`
	Field78  bool     `zid:"34"`
	Field67  int32    `zid:"35"`
	Field68  int32    `zid:"36"`
	Field128 int32    `zid:"37"`
	Field129 string   `zid:"38"`
	Field131 int32    `zid:"39"`
}
