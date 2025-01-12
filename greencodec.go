package rpc25519

// modified from net/rpc client.go

// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"go/token"
	"io"
	"log"
	"os"
	"reflect"
	"sync"

	"github.com/glycerine/greenpack/msgp"
	"github.com/hexops/valast"
)

// A value sent as a placeholder for the server's response value when the server
// receives an invalid request. It is never decoded by the client since the Response
// contains an error when it is used.
var invalidRequest = &InvalidRequest{}

// logRegisterError specifies whether to log problems during method registration.
// To debug registration, recompile the package with this set to true.
const logRegisterError = false

// If set, print log statements for internal and I/O errors.
var debugLog = true

// ServerError represents an error that has been returned from
// the remote side of the RPC connection.
type ServerError string

func (e ServerError) Error() string {
	return string(e)
}

// ClientCodec is part of the net/rpc API. Its docs:
//
// A ClientCodec implements writing of RPC requests and
// reading of RPC responses for the client side of an RPC session.
// The client calls [ClientCodec.WriteRequest] to write a request to the connection
// and calls [ClientCodec.ReadResponseHeader] and [ClientCodec.ReadResponseBody] in pairs
// to read responses. The client calls [ClientCodec.Close] when finished with the
// connection. ReadResponseBody may be called with a nil
// argument to force the body of the response to be read and then
// discarded.
// See [NewClient]'s comment for information about concurrent access.
type ClientCodec interface {
	WriteRequest(*Request, msgp.Encodable) error
	ReadResponseHeader(*Response) error
	ReadResponseBody(msgp.Decodable) error

	Close() error
}

// ServerCodec is part of the net/rpc API. Its docs:
//
// A ServerCodec implements reading of RPC requests and writing of
// RPC responses for the server side of an RPC session.
// The server calls [ServerCodec.ReadRequestHeader] and [ServerCodec.ReadRequestBody] in pairs
// to read requests from the connection, and it calls [ServerCodec.WriteResponse] to
// write a response back. The server calls [ServerCodec.Close] when finished with the
// connection. ReadRequestBody may be called with a nil
// argument to force the body of the request to be read and discarded.
// See [NewClient]'s comment for information about concurrent access.
type ServerCodec interface {
	ReadRequestHeader(*Request) error
	ReadRequestBody(msgp.Decodable) error
	WriteResponse(*Response, msgp.Encodable) error

	// Close can be called multiple times and must be idempotent.
	Close() error
}

type greenpackClientCodec struct {
	cli          *Client
	rwc          io.ReadWriteCloser
	dec          *msgp.Reader
	enc          *msgp.Writer
	encBufWriter *bufio.Writer
	debugEncBuf  *bytes.Buffer // target of encBufWriter for debugging.
}

func (c *greenpackClientCodec) WriteRequest(r *Request, body msgp.Encodable) (err error) {
	if err = r.EncodeMsg(c.enc); err != nil {
		return
	}
	if err = body.EncodeMsg(c.enc); err != nil {
		return
	}
	err = c.enc.Flush() // flush the greenpack msgp.Writer
	if err != nil {
		return err
	}
	return c.encBufWriter.Flush() // flush the bufio.Writer
}

func (c *greenpackClientCodec) ReadResponseHeader(r *Response) error {
	return r.DecodeMsg(c.dec)
}

func (c *greenpackClientCodec) ReadResponseBody(body msgp.Decodable) (err error) {
	if body == nil {
		return nil
	}
	//vv("ReadResponseBody pre DecodeMsg: '%#v'", body)
	//vv("cli.decBuf has len %v: '%v'", len(c.cli.decBuf.Bytes()), string(c.cli.decBuf.Bytes()))
	err = body.DecodeMsg(c.dec)
	if err != nil {
		// greencodec.go:114 2025-01-12 09:15:13.501 -0600 CST error back from body.DecodeMsg: 'msgp: attempted to decode type "ext" with method for "map"'
		vv("error back from body.DecodeMsg: '%v' ", err) // here!
	}
	return
}

func (c *greenpackClientCodec) Close() error {
	return c.rwc.Close()
}

// ErrNetRpcShutdown is from net/rpc, and still
// distinct from ErrShutdown to help locate
// when and where the error was generated. It indicates the
// system, or at least the network connection or stream, is
// closed or shutting down.
var ErrNetRpcShutdown = errors.New("connection is shut down")

type Green interface {
	msgp.Encodable
	msgp.Decodable
}

// Call represents an active net/rpc RPC.
type Call struct {
	ServiceMethod string     // The name of the service and method to call.
	Args          Green      // The argument to the function (*struct).
	Reply         Green      // The reply from the function (*struct).
	Error         error      // After completion, the error status.
	Done          chan *Call // Receives *Call when Go is complete.
}

// Precompute the reflect type for error.
var typeOfError = reflect.TypeFor[error]()

type methodType struct {
	sync.Mutex // protects counters
	method     reflect.Method
	ArgType    reflect.Type
	ReplyType  reflect.Type
	numCalls   uint
}

type service struct {
	name      string                 // name of service
	rcvr      reflect.Value          // receiver of methods for the service
	typ       reflect.Type           // type of the receiver
	method    map[string]*methodType // registered methods
	ctxMethod map[string]*methodType // registered methods that start with ctx in callback

	rpcHDRavail bool
	rpcHDRfield reflect.Value
}

func (call *Call) done() {
	select {
	case call.Done <- call:
	default:
		// We don't want to block here. It is the caller's responsibility to make
		// sure the channel has enough buffer space. See comment in Go().
		if debugLog {
			log.Println("rpc: discarding Call reply due to insufficient Done chan capacity")
		}
	}
}

// suitableMethods returns suitable Rpc methods of typ. It will log
// errors if logErr is true.
func suitableMethods(typ reflect.Type, logErr bool) map[string]*methodType {
	methods := make(map[string]*methodType)
	for m := 0; m < typ.NumMethod(); m++ {
		method := typ.Method(m)
		mtype := method.Type
		mname := method.Name
		// Method must be exported.
		if !method.IsExported() {
			continue
		}
		// Method needs three ins: receiver, *args, *reply.
		if mtype.NumIn() != 3 {
			// We want to just ignore methods that are not registerable!

			//if logErr {
			//	alwaysPrintf("rpc.Register: method %q has %d input parameters; needs exactly three", mname, mtype.NumIn())
			//	panic("fix the above")
			//}
			continue
		}
		// First arg need not be a pointer.
		argType := mtype.In(1)
		if !isExportedOrBuiltinType(argType) {
			if logErr {
				log.Printf("rpc.Register: argument type of method %q is not exported: %q\n", mname, argType)
			}
			continue
		}
		// Second arg must be a pointer.
		replyType := mtype.In(2)
		if replyType.Kind() != reflect.Pointer {
			if logErr {
				log.Printf("rpc.Register: reply type of method %q is not a pointer: %q\n", mname, replyType)
			}
			continue
		}
		// Reply type must be exported.
		if !isExportedOrBuiltinType(replyType) {
			if logErr {
				log.Printf("rpc.Register: reply type of method %q is not exported: %q\n", mname, replyType)
			}
			continue
		}
		// Method needs one out.
		if mtype.NumOut() != 1 {
			if logErr {
				log.Printf("rpc.Register: method %q has %d output parameters; needs exactly one\n", mname, mtype.NumOut())
			}
			continue
		}
		// The return type of the method must be error.
		if returnType := mtype.Out(0); returnType != typeOfError {
			if logErr {
				log.Printf("rpc.Register: return type of method %q is %q, must be error\n", mname, returnType)
			}
			continue
		}
		methods[mname] = &methodType{method: method, ArgType: argType, ReplyType: replyType}
	}
	return methods
}

// contextFirstSuitableMethods returns suitable Rpc methods of typ. It will log
// errors if logErr is true.
func contextFirstSuitableMethods(typ reflect.Type, logErr bool) map[string]*methodType {
	methods := make(map[string]*methodType)
	for m := 0; m < typ.NumMethod(); m++ {
		method := typ.Method(m)
		mtype := method.Type
		mname := method.Name
		// Method must be exported.
		if !method.IsExported() {
			continue
		}
		// Method needs four ins: receiver, ctx, *args, *reply.
		if mtype.NumIn() != 4 {
			if logErr {
				log.Printf("rpc.Register: method %q has %d input parameters; needs exactly four\n", mname, mtype.NumIn())
			}
			continue
		}
		// First arg
		firstType := mtype.In(1)
		if firstType != reflect.TypeOf((*context.Context)(nil)).Elem() {
			continue
		}

		// Second arg need not be a pointer.
		argType := mtype.In(2)
		if !isExportedOrBuiltinType(argType) {
			if logErr {
				log.Printf("rpc.Register: argument type of method %q is not exported: %q\n", mname, argType)
			}
			continue
		}
		// third arg must be a pointer.
		replyType := mtype.In(3)
		if replyType.Kind() != reflect.Pointer {
			if logErr {
				log.Printf("rpc.Register: reply type of method %q is not a pointer: %q\n", mname, replyType)
			}
			continue
		}
		// Reply type must be exported.
		if !isExportedOrBuiltinType(replyType) {
			if logErr {
				log.Printf("rpc.Register: reply type of method %q is not exported: %q\n", mname, replyType)
			}
			continue
		}
		// Method needs one out.
		if mtype.NumOut() != 1 {
			if logErr {
				log.Printf("rpc.Register: method %q has %d output parameters; needs exactly one\n", mname, mtype.NumOut())
			}
			continue
		}
		// The return type of the method must be error.
		if returnType := mtype.Out(0); returnType != typeOfError {
			if logErr {
				log.Printf("rpc.Register: return type of method %q is %q, must be error\n", mname, returnType)
			}
			continue
		}
		methods[mname] = &methodType{method: method, ArgType: argType, ReplyType: replyType}
	}
	return methods
}

type greenpackServerCodec struct {
	pair         *rwPair
	rwc          io.ReadWriteCloser
	dec          *msgp.Reader
	enc          *msgp.Writer
	encBufWriter *bufio.Writer
	debugEncBuf  *bytes.Buffer // target of encBufWriter for debugging.
	closed       bool
}

func (c *greenpackServerCodec) ReadRequestHeader(r *Request) (err error) {
	//vv("ReadRequestHeader before DecodeMsg: '%#v'; avail=%v  decBuf='%v'", r, len(c.pair.decBuf.Bytes()), string(c.pair.decBuf.Bytes()))

	err = r.DecodeMsg(c.dec)
	//vv("ReadRequestHeader after fill in r='%#v'", r)
	return
}

func (c *greenpackServerCodec) ReadRequestBody(body msgp.Decodable) (err error) {
	if body == nil {
		// srv.go:671 readRequest() trying to discard body.
		// We should be able to no-op this because we'll
		// just ignore the remainder of the JobSerz bytes in our Message.
		return nil
	}
	//vv("server side is doing ReadRequestBody into '%#v'", body)
	//vv("server side decBuf = '%v'", string(c.pair.decBuf.Bytes()))
	err = body.DecodeMsg(c.dec)
	//vv("server side after fill in of body ='%#v'", body)
	return
}

func (c *greenpackServerCodec) WriteResponse(r *Response, body msgp.Encodable) (err error) {
	vv("top of WriteResponse, about to encode header; c.enc.Buffered() = %v", c.enc.Buffered())
	if err = r.EncodeMsg(c.enc); err != nil {
		vv("problem EncodeMsg on the header: '%v'", err)
		if c.enc.Flush() == nil {
			// couldn't encode the header. Should not happen, so if it does,
			// shut down the connection to signal that the connection is broken.
			log.Println("rpc: greenpack error encoding response:", err)
			c.Close()
		}
		return
	}

	vv("DEBUG! TODO undo check: was Response net/rpc header written?")
	panicOn(c.enc.Flush())
	panicOn(c.encBufWriter.Flush())
	headerLen := len(c.debugEncBuf.Bytes())
	vv("after writing net/rpc Response header, debugEncBuf len= %v", headerLen)

	vv("WriteResponse: about to encode body: body = '%#v'", body)
	if err = body.EncodeMsg(c.enc); err != nil {
		vv("problem EncodeMsg on body: '%v'", err)
		if c.enc.Flush() == nil {
			// Was a problem encoding the body but the header has been written.
			// Shut down the connection to signal that the connection is broken.
			log.Println("rpc: greenpack error encoding body:", err)
			c.Close()
		}
		return
	}
	vv("WriteResponse: got past encoding the body, about to Flush()")
	err = c.enc.Flush()
	if err != nil {
		return err
	}
	err = c.encBufWriter.Flush()
	// DEBUG adds:
	debugby := c.debugEncBuf.Bytes()
	tot := len(debugby)
	bodyLen := tot - headerLen
	vv("server side: response header is len %v; blake3= '%v'", headerLen, blake3OfBytesString(debugby[:headerLen]))
	vv("server side: response body   is len %v; blake3= '%v'", bodyLen, blake3OfBytesString(debugby[headerLen:]))
	// verify encoding

	vv("verify encoding for body: %T", body)
	switch x := body.(type) {
	case *RsyncStep3A_SenderProvidesDeltas:
		vv("compare Marshal to Encode")
		bts, err := x.MarshalMsg(nil)
		panicOn(err)
		if bytes.Equal(bts, debugby[headerLen:]) {
			vv("good: marshal and ecode the same")
		} else {
			// marshal len 1972483; encode len 959266
			vv("marshal len %v; ecode len %v", len(bts), bodyLen)
			panic("marshal and encode differ")
		}

		vv("test decoding RsyncStep3A_SenderProvidesDeltas")
		y := &RsyncStep3A_SenderProvidesDeltas{}
		left, err3 := y.UnmarshalMsg(debugby[headerLen:])

		// we have reproduced the error on the server side!
		panicOn(err3) // panic: msgp: attempted to decode type "ext" with method for "map"
		if len(left) > 0 {
			// getting here, nothing being decoded???
			panic(fmt.Sprintf("should have 0 left, but we have %v", len(left)))
		}
		if !reflect.DeepEqual(y, x) {
			fdx, err := os.Create("debug.x.orig")
			panicOn(err)
			fmt.Fprintf(fdx, "%v\n", valast.String(x))
			fdx.Close()

			fdy, err := os.Create("debug.y.test_unmarshal")
			panicOn(err)
			fmt.Fprintf(fdy, "%v\n", valast.String(y))
			fdy.Close()

			panic("not deep equal!")
		} else {
			vv("test Unmarshal good.")
		}
	}
	return
}

func (c *greenpackServerCodec) Close() error {
	if c.closed {
		// Only call c.rwc.Close once; otherwise the semantics are undefined.
		return nil
	}
	c.closed = true
	return c.rwc.Close()
}

// Is this type exported or a builtin?
func isExportedOrBuiltinType(t reflect.Type) bool {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	// PkgPath will be non-empty even for an exported type,
	// so we need to check the type name as well.
	return token.IsExported(t.Name()) || t.PkgPath() == ""
}
