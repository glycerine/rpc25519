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

/* this is what the protobuf serializes;
   it takes advantage of default field values.
type BenchmarkMessage struct {
	Field1   string   `protobuf:"bytes,1,req,name=field1" json:"field1" zid:"0"`
	Field9   string   `protobuf:"bytes,9,opt,name=field9" json:"field9" zid:"1"`
	Field18  string   `protobuf:"bytes,18,opt,name=field18" json:"field18" zid:"2"`
	Field80  *bool    `protobuf:"varint,80,opt,name=field80,def=0" json:"field80,omitempty" zid:"3"`
	Field81  *bool    `protobuf:"varint,81,opt,name=field81,def=1" json:"field81,omitempty" zid:"4"`
	Field2   int32    `protobuf:"varint,2,req,name=field2" json:"field2" zid:"5"`
	Field3   int32    `protobuf:"varint,3,req,name=field3" json:"field3" zid:"6"`
	Field280 int32    `protobuf:"varint,280,opt,name=field280" json:"field280" zid:"7"`
	Field6   *int32   `protobuf:"varint,6,opt,name=field6,def=0" json:"field6,omitempty" zid:"8"`
	Field22  int64    `protobuf:"varint,22,opt,name=field22" json:"field22" zid:"9"`
	Field4   string   `protobuf:"bytes,4,opt,name=field4" json:"field4" zid:"10"`
	Field5   []uint64 `protobuf:"fixed64,5,rep,name=field5" json:"field5,omitempty" zid:"11"`
	Field59  *bool    `protobuf:"varint,59,opt,name=field59,def=0" json:"field59,omitempty" zid:"12"`
	Field7   string   `protobuf:"bytes,7,opt,name=field7" json:"field7" zid:"13"`
	Field16  int32    `protobuf:"varint,16,opt,name=field16" json:"field16" zid:"14"`
	Field130 *int32   `protobuf:"varint,130,opt,name=field130,def=0" json:"field130,omitempty" zid:"15"`
	Field12  *bool    `protobuf:"varint,12,opt,name=field12,def=1" json:"field12,omitempty" zid:"16"`
	Field17  *bool    `protobuf:"varint,17,opt,name=field17,def=1" json:"field17,omitempty" zid:"17"`
	Field13  *bool    `protobuf:"varint,13,opt,name=field13,def=1" json:"field13,omitempty" zid:"18"`
	Field14  *bool    `protobuf:"varint,14,opt,name=field14,def=1" json:"field14,omitempty" zid:"19"`
	Field104 *int32   `protobuf:"varint,104,opt,name=field104,def=0" json:"field104,omitempty" zid:"20"`
	Field100 *int32   `protobuf:"varint,100,opt,name=field100,def=0" json:"field100,omitempty" zid:"21"`
	Field101 *int32   `protobuf:"varint,101,opt,name=field101,def=0" json:"field101,omitempty" zid:"22"`
	Field102 string   `protobuf:"bytes,102,opt,name=field102" json:"field102" zid:"23"`
	Field103 string   `protobuf:"bytes,103,opt,name=field103" json:"field103" zid:"24"`
	Field29  *int32   `protobuf:"varint,29,opt,name=field29,def=0" json:"field29,omitempty" zid:"25"`
	Field30  *bool    `protobuf:"varint,30,opt,name=field30,def=0" json:"field30,omitempty" zid:"26"`
	Field60  *int32   `protobuf:"varint,60,opt,name=field60,def=-1" json:"field60,omitempty" zid:"27"`
	Field271 *int32   `protobuf:"varint,271,opt,name=field271,def=-1" json:"field271,omitempty" zid:"28"`
	Field272 *int32   `protobuf:"varint,272,opt,name=field272,def=-1" json:"field272,omitempty" zid:"29"`
	Field150 int32    `protobuf:"varint,150,opt,name=field150" json:"field150" zid:"30"`
	Field23  *int32   `protobuf:"varint,23,opt,name=field23,def=0" json:"field23,omitempty" zid:"31"`
	Field24  *bool    `protobuf:"varint,24,opt,name=field24,def=0" json:"field24,omitempty" zid:"32"`
	Field25  *int32   `protobuf:"varint,25,opt,name=field25,def=0" json:"field25,omitempty" zid:"33"`
	Field78  bool     `protobuf:"varint,78,opt,name=field78" json:"field78" zid:"34"`
	Field67  *int32   `protobuf:"varint,67,opt,name=field67,def=0" json:"field67,omitempty" zid:"35"`
	Field68  int32    `protobuf:"varint,68,opt,name=field68" json:"field68" zid:"36"`
	Field128 *int32   `protobuf:"varint,128,opt,name=field128,def=0" json:"field128,omitempty" zid:"37"`
	Field129 *string  `protobuf:"bytes,129,opt,name=field129,def=xxxxxxxxxxxxxxxxxxxxx" json:"field129,omitempty" zid:"38"`
	Field131 *int32   `protobuf:"varint,131,opt,name=field131,def=0" json:"field131,omitempty" zid:"39"`
}
*/

// for testing context cancellation

type MustBeCancelled struct {
	// as greenpack efficiently does nothing without any member elements.
	Placeholder int `zid:"0"`
}

func NewMustBeCancelled() *MustBeCancelled {
	return &MustBeCancelled{}
}

var test040callStarted = make(chan bool)
var test040callFinished = make(chan string, 1)

func (s *MustBeCancelled) WillHangUntilCancel(ctx context.Context, args *Args, reply *Reply) error {
	close(test040callStarted)
	fmt.Printf("example.go: server-side: WillHangUntilCancel() is running\n")
	select {
	case <-ctx.Done():
		msg := "example.go: MustBeCancelled.WillHangUntilCancel(): ctx.Done() was closed!"
		fmt.Printf("%v\n", msg)
		test040callFinished <- msg
	}
	return nil
}
