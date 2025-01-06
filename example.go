package rpc25519

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	// check-summing utilities.
	cristalbase64 "github.com/cristalhq/base64"
	"lukechampine.com/blake3"
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

// Args in example.go is part of the tests.
type Args struct {
	A int `zid:"0"`
	B int `zid:"1"`
}

// Reply in example.go is part of the tests.
type Reply struct {
	C int `zid:"0"`
}

// Arith in example.go is part of the tests.
type Arith int

// net/rpc comment:
// Some of Arith's methods have value args, some have pointer args. That's deliberate.

// Arith.Add in example.go is part of the tests.
func (t *Arith) Add(args Args, reply *Reply) error {
	reply.C = args.A + args.B
	//vv("Arith.Add(%v + %v) called.", args.A, args.B)
	return nil
}

// Arith.Mul in example.go is part of the tests.
func (t *Arith) Mul(args *Args, reply *Reply) error {
	reply.C = args.A * args.B
	//vv("Arith.Mul(%v * %v) called.", args.A, args.B)
	return nil
}

// Arith.Div in example.go is part of the tests.
func (t *Arith) Div(args Args, reply *Reply) error {
	if args.B == 0 {
		return errors.New("divide by zero")
	}
	reply.C = args.A / args.B
	//vv("Arith.Div(%v / %v) called.", args.A, args.B)
	return nil
}

// Arith.String in example.go is part of the tests.
func (t *Arith) String(args *Args, reply *string) error {
	*reply = fmt.Sprintf("%d+%d=%d", args.A, args.B, args.A+args.B)
	//vv("Arith.Strings(%v, %v -> '%v') called.", args.A, args.B, *reply)
	return nil
}

// Arith.Scan in example.go is part of the tests.
func (t *Arith) Scan(args string, reply *Reply) (err error) {
	_, err = fmt.Sscan(args, &reply.C)
	return
}

// Arith.Error in example.go is part of the tests.
func (t *Arith) Error(args *Args, reply *Reply) error {
	panic("ERROR")
}

// Arith.SleepMilli in example.go is part of the tests.
func (t *Arith) SleepMilli(args *Args, reply *Reply) error {
	time.Sleep(time.Duration(args.A) * time.Millisecond)
	return nil
}

// Simple in example.go is part of the tests.
type Simple int

// Simple.Exported in example.go is part of the tests.
func (t *Simple) Exported(args Args, reply *Reply) error {
	reply.C = args.A + args.B
	return nil
}

// Embed in example.go is part of the tests.
type Embed struct {
	Simple `zid:"0"`
}

// BuiltinTypes in example.go is part of the tests.
type BuiltinTypes struct {
	Placeholder int `zid:"0"` // greenpack refuses to serialize an empty struct.
}

// BuiltinTypes.Map in example.go is part of the tests.
func (BuiltinTypes) Map(args *Args, reply *map[int]int) error {
	(*reply)[args.A] = args.B
	return nil
}

// BuiltinTypes.Slice in example.go is part of the tests.
func (BuiltinTypes) Slice(args *Args, reply *[]int) error {
	*reply = append(*reply, args.A, args.B)
	return nil
}

// BuiltinTypes.Array in example.go is part of the tests.
func (BuiltinTypes) Array(args *Args, reply *[2]int) error {
	(*reply)[0] = args.A
	(*reply)[1] = args.B
	return nil
}

// BuiltinTypes.WantsContext in example.go is part of the tests.
// Here, mimic Array's reply.
func (BuiltinTypes) WantsContext(ctx context.Context, args *Args, reply *[2]int) error {
	if h, ok := HDRFromContext(ctx); ok {
		fmt.Printf("WantsContext called with HDR = '%v'; HDR.Nc.RemoteAddr() gives '%v'; HDR.Nc.LocalAddr() gives '%v'\n", h.String(), h.Nc.RemoteAddr(), h.Nc.LocalAddr())

		(*reply)[0] = args.A
		(*reply)[1] = args.B
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

// Hello in example.go is part of the tests.
type Hello struct {
	Placeholder int `zid:"0"` // must have public field or greenpack will ignore it.
}

// BenchmarkMessage in example.go is part of the tests
// and benchmarks.
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

// The MustBeCancelled struct in example.go is part of the tests.
// See cli_test.go Test040 for details.
type MustBeCancelled struct {
	// as greenpack efficiently does nothing without any member elements.
	Placeholder int `zid:"0"`
}

// NewMustBeCancelled in example.go is part of the tests.
// See cli_test.go Test040 for details.
func NewMustBeCancelled() *MustBeCancelled {
	return &MustBeCancelled{}
}

var test040callStarted = make(chan bool, 1)
var test040callFinished = make(chan string, 1)

// WillHangUntilCancel in example.go is part of the tests.
// See cli_test.go Test040 for details.
func (s *MustBeCancelled) WillHangUntilCancel(ctx context.Context, args *Args, reply *Reply) error {
	test040callStarted <- true
	fmt.Printf("example.go: server-side: WillHangUntilCancel() is running\n")

	// demonstrate getting at the net.Conn in use.
	if hdr, ok := HDRFromContext(ctx); ok {
		fmt.Printf("example.go: net.rpc API: our net.Conn has local = '%v'; remote = '%v'\n",
			hdr.Nc.LocalAddr(), hdr.Nc.RemoteAddr())
	}

	select {
	case <-ctx.Done():
		msg := "example.go: MustBeCancelled.WillHangUntilCancel(): ctx.Done() was closed!"
		fmt.Printf("%v\n", msg)
		test040callFinished <- msg
	}
	return nil
}

var test041callStarted = make(chan bool, 1)
var test041callFinished = make(chan string, 1)

// MessageAPI_HangUntilCancel in example.go is part of the tests.
// See cli_test.go Test040 for details.
func (s *MustBeCancelled) MessageAPI_HangUntilCancel(req, reply *Message) error {
	test041callStarted <- true
	fmt.Printf("example.go: server-side: MessageAPI_HangUntilCancel() is running\n")
	// demonstrate net.Conn access:
	fmt.Printf("example.go: Message API: our net.Conn has local = '%v'; remote = '%v'\n",
		req.HDR.Nc.LocalAddr(), req.HDR.Nc.RemoteAddr())
	select {
	case <-req.HDR.Ctx.Done():
		msg := "example.go: MustBeCancelled.MessageAPI_HangUntilCancel(): ctx.Done() was closed!"
		fmt.Printf("%v\n", msg)
		test041callFinished <- msg
	}
	return nil
}

// ServerSideUploadState is used by
// Test045_streaming_client_to_server (upload) in cli_test.go
// and cmd/srv/server.go to demonstrate
// streaming a large (or infinite) file in small parts,
// from client to server, all while keeping FIFO
// message order.
type ServerSideUploadState struct {
	// key is CallID
	m map[string]*PerCallID_FileToDiskState
}

//msgp:ignore PerCallID_FileToDiskState
type PerCallID_FileToDiskState struct {
	CallID string
	T0     time.Time

	OverrideFilename string // if set, use instead of "readFile" in Args.
	FnameTmp         string
	FnameFinal       string
	Randomness       string

	Fd        *os.File
	BytesWrit int64

	Blake3hash *blake3.Hasher

	PartsSeen map[int64]bool
	SeenCount int
}

// NewServerSideUploadState returns a new
// ServerSideUploadState. This is part of
// the cli_test.go Test045 mechanics.
func NewServerSideUploadState() *ServerSideUploadState {
	return &ServerSideUploadState{
		m: make(map[string]*PerCallID_FileToDiskState),
	}
}

func NewPerCallID_FileToDiskState(callID string) *PerCallID_FileToDiskState {
	return &PerCallID_FileToDiskState{
		CallID:     callID,
		PartsSeen:  make(map[int64]bool),
		Blake3hash: blake3.New(64, nil),
		T0:         time.Now(),
	}
}

// ReceiveFileInParts is used by
// Test045_streaming_client_to_server in cli_test.go
// to demonstrate streaming from client to server.
//
// See the cmd/cli/client.go and cmd/srv/server.go
// and their (cli -sendfile) and (srv -readfile) flags
// for a full implementation of an scp-like utility
// that uses this method.
//
// ReceiveFileInParts is an UploadReaderFunc and is
// registered on the Server with
// the Server.RegisterUploadReaderFunc() call.
//
// Notice that since we get a callback-per-message,
// and these messages can be from different clients,
// we must track the state of each of them in their
// own PerCallIDUploadState. This is a counterpoint
// in the design space: compare the Bistream and Download
// versions that are invoked once per call initiation.
// We have to manage the state of all clients, whereas
// they have to handle channels.
//
// What happens if the client connection goes down?
// We still have state here. We'd like it to get
// cleaned up if the connection is lost. We use the
// deadCallID for that. It it is set, then clean up
// after that CallID and return. ctx, req, and lastReply
// will be nil, so no other work is possible.
func (st *ServerSideUploadState) ReceiveFileInParts(ctx context.Context, req *Message, lastReply *Message, deadCallID string) (err error) {

	if deadCallID != "" {
		//vv("cleaning up after dead callID '%v'", deadCallID)
		delete(st.m, deadCallID)
		return nil
	}

	callID := req.HDR.CallID
	s, ok := st.m[callID]
	if !ok {
		s = NewPerCallID_FileToDiskState(callID)
		st.m[callID] = s
	}

	if s.T0.IsZero() {
		s.T0 = req.HDR.Created
	}
	hdr1 := req.HDR

	select {
	case <-ctx.Done():
		delete(st.m, callID)
		return ErrContextCancelled
	default:
	}

	if hdr1.StreamPart != int64(s.SeenCount) {
		panic(fmt.Sprintf("%v = hdr1.StreamPart != s.seenCount = %v", hdr1.StreamPart, s.SeenCount))
	}

	err = s.WriteOneMsgToFile(req, "servergot", lastReply != nil)
	if err != nil {
		return
	}
	if lastReply != nil {

		totSum := "blake3-" + cristalbase64.URLEncoding.EncodeToString(s.Blake3hash.Sum(nil))
		//vv("ReceiveFileInParts sees last set!")
		//vv("bytesWrit=%v; \nserver totSum='%v'", s.bytesWrit, totSum)

		elap := time.Since(s.T0)
		mb := float64(s.BytesWrit) / float64(1<<20)
		seconds := (float64(elap) / float64(time.Second))
		rate := mb / seconds

		lastReply.HDR.Args["serverTotalBlake3sum"] = totSum

		// finally reply to the original caller.
		lastReply.JobSerz = []byte(fmt.Sprintf("got upcall at '%v' => "+
			"elap = %v\n (while mb=%v) => %v MB/sec. ; \n bytesWrit=%v;",
			s.T0, elap, mb, rate, s.BytesWrit))

		//vv("returning with lastReply = '%v'", string(lastReply.JobSerz))

		// cleanup
		delete(st.m, callID)
	}
	return
}

func (s *PerCallID_FileToDiskState) WriteOneMsgToFile(req *Message, suffix string, last bool) (err error) {

	s.SeenCount++
	hdr1 := &req.HDR
	if hdr1.StreamPart == 0 {
		if s.SeenCount != 1 {
			panic("we saw a part before 0!")
		}
		//vv("ServerSideUploadState.ReceiveFileInParts sees part 0: hdr1='%v'", hdr1.String())
		s.PartsSeen = make(map[int64]bool)
		s.Blake3hash = blake3.New(64, nil)

		filename := ""
		ok := false
		if s.OverrideFilename != "" {
			filename = s.OverrideFilename
		} else {
			filename, ok = hdr1.Args["readFile"]
			if !ok {
				panic("Args must contain readFile -> the file name !")
			}
		}

		// do an atomic rename from temp file to final name at the end
		s.FnameFinal = filename + "." + suffix // avoid clobbering origin file if same dir
		s.Randomness = cryRandBytesBase64(16)
		s.FnameTmp = s.FnameFinal + ".tmp_" + s.Randomness

		sum := blake3OfBytesString(req.JobSerz)
		blake3checksumBase64, ok := hdr1.Args["blake3"]
		if ok {
			if blake3checksumBase64 != sum {
				panic(fmt.Sprintf("checksum on first %v bytes disagree: client sent blake3sum='%v'; we computed = '%v'", len(req.JobSerz), blake3checksumBase64, sum))
			}
		}

		// save the file handle for the next callback too.
		s.Fd, err = os.Create(s.FnameTmp)
		if err != nil {
			return fmt.Errorf("error: server could not path '%v': '%v'", s.FnameTmp, err)
		}
		go s.Fd.Sync() // get the file showing on disk asap
	}

	part := req.HDR.StreamPart
	seen := s.PartsSeen[part]
	if seen {
		panic(fmt.Sprintf("part %v was already seen.", part))
	}
	s.PartsSeen[part] = true

	s.Blake3hash.Write(req.JobSerz)
	serverSum := blake3OfBytesString(req.JobSerz)
	clientSum := req.HDR.Args["blake3"]

	//vv("server part %v, len %v, server-sum='%v' \n        while Subject blake3    client-sum='%v'\n", req.HDR.StreamPart, len(req.JobSerz), serverSum, clientSum)

	if part > 0 && serverSum != clientSum {
		panic(fmt.Sprintf("checksum disagree on part %v; see above."+
			" server sees len %v req.JobSerz='%v'; serverSum='%v'; clientSum='%v'",
			part, len(req.JobSerz),
			string(req.JobSerz), serverSum, clientSum))
	}
	n := len(req.JobSerz)
	if n > 0 {
		nw, err := io.Copy(s.Fd, bytes.NewBuffer(req.JobSerz))
		s.BytesWrit += nw
		if err != nil {
			err = fmt.Errorf("ReceiveFileInParts: on "+
				"writing StreamPart 1 to path '%v', we got error: "+
				"'%v', after writing %v of %v", s.FnameTmp, err, nw, n)
			vv("problem: %v", err.Error())
			return err
		}
	}
	if last {
		//vv("WriteOneMsgToFile sees last, renaming '%v' -> '%v'", s.FnameTmp, s.FnameFinal)
		s.Fd.Close()
		err = os.Rename(s.FnameTmp, s.FnameFinal)
		panicOn(err)
	}
	return
}

type ServerSendsDownloadState struct{}

// ServerSendsDownload is used by Test055_streaming_server_to_client.
// It demonstrates how a registered server func can stream to the client.
// ServerSendStream has type ServerSendsDownloadFunc, and gets
// registered on the server with srv.RegisterServerSendsDownloadFunc().
func (ssss *ServerSendsDownloadState) ServerSendsDownload(srv *Server, ctx context.Context, req *Message, sendStreamPart func(ctx context.Context, msg *Message, last bool) error, lastReply *Message) (err error) {
	done := ctx.Done()
	for i := range 20 {
		msg := NewMessage()
		msg.JobSerz = []byte(fmt.Sprintf("part %v;", i))
		sendStreamPart(ctx, msg, i == 19)
		select {
		case <-done:
			vv("exiting early! we see done requested at i = %v", i)
			break
		default:
		}
	}

	lastReply.HDR.Subject = "This is end. My only friend, the end. - Jim Morrison, The Doors."
	return
}

// ServeBistreamState is used by Test065_bidirectional_streaming test.
type ServeBistreamState struct{}

// ServeBistream is an example of a BistreamFunc,
// a server side registered function for bi-streaming
// (doing both upload and download simultaneously).
// See cli_test.go Test065_bidirectional_download_and_upload
// for a test that uses this method.
func (bi *ServeBistreamState) ServeBistream(
	srv *Server,
	ctx context.Context,
	req *Message,
	uploadsFromClientCh <-chan *Message,
	sendDownloadPartToClient func(ctx context.Context, msg *Message, last bool) error,
	lastReply *Message,
) (err error) {

	t0 := time.Now()
	//vv("top of ServeBistream(). req.HDR = '%v'", req.HDR.String())

	if req.HDR.StreamPart != 0 {
		panic(fmt.Sprintf("req.StreamPart = %v but we expected 0", req.HDR.StreamPart))
	}

	// handle the first message body
	prefix := "receiveFile:"
	var filename string
	var initial string
	if bytes.HasPrefix(req.JobSerz, []byte(prefix)) {
		pay := string(req.JobSerz[len(prefix):])
		splt := strings.Split(pay, "\n")
		filename = splt[0]
		initial = splt[1]
		//vv("will save to filename: '%v'", filename)
	}
	var writeFD *os.File
	if filename != "" {
		writeFD, err = os.Create(filename)
		if err != nil {
			// terminate both upload and download
			return err
		}
		_, err = writeFD.WriteString(initial)
		if err != nil {
			return err
		}
	}

	done := ctx.Done()
	for i := range 20 {
		//vv("on i = %v", i)
		msg := NewMessage()
		msg.JobSerz = []byte(fmt.Sprintf("part %v;", i))
		err = sendDownloadPartToClient(ctx, msg, i == 19)
		if err != nil {
			return fmt.Errorf("error back from sendDownloadPartToClient: '%v'", err)
		}
		select {
		case <-done:
			vv("exiting early! we see done requested at i = %v", i)
			break
		//case msg := <-uploadsFromClientCh:
		//	_ = msg
		//	//vv("we got stream part from client: '%v'", msg.HDR.String())
		default:
		}
	}
	//vv("ServeBistream: done sending 20 messages")

	// upload handling
	bytesWrit := int64(0)
	// already saw 0 in first message, but it is repeated in the channel.
	totalParts := int64(0)
	for {
		select {
		case <-ctx.Done():
			// allow call cancellation.
			return fmt.Errorf("context cancelled")
		case msg := <-uploadsFromClientCh:
			//vv("ServeBistream sees upload part: '%v'", msg.HDR.String())
			if msg.HDR.StreamPart != totalParts {
				panic(fmt.Sprintf("%v = msg.HDR.StreamPart != totalParts = %v",
					msg.HDR.StreamPart, totalParts))
			}
			totalParts++
			switch msg.HDR.Typ {
			case CallUploadMore, CallUploadEnd:
				n := len(msg.JobSerz)
				nw, err := io.Copy(writeFD, bytes.NewBuffer(msg.JobSerz))
				bytesWrit += nw
				if err != nil {
					err = fmt.Errorf("ReceiveFileInParts: on "+
						"writing StreamPart %v to path '%v', we got error: "+
						"'%v', after writing %v of %v. So far total "+
						"from all messages was: %v",
						msg.HDR.StreamPart, filename, err, nw, n, bytesWrit)
					vv("problem: %v", err.Error())
					return err
				} else {
					//vv("succesfully wrote part %v to the file '%v': '%v'", part, s.fname, string(req.JobSerz))
				}
				if msg.HDR.Typ == CallUploadEnd {
					writeFD.Close()

					elap := time.Since(req.HDR.Created)
					mb := float64(bytesWrit) / float64(1<<20)
					seconds := (float64(elap) / float64(time.Second))
					rate := mb / seconds

					// finally reply to the original caller.
					lastReply.JobSerz = []byte(fmt.Sprintf(
						"totalParts:%v: got upcall at '%v' => elap = %v "+
							"(while mb=%v) => %v MB/sec. ; "+
							"bytesWrit=%v;", totalParts, t0, elap, mb, rate, bytesWrit))

					lastReply.HDR.Subject = "This is end. My only friend," +
						" the end. - Jim Morrison, The Doors."

					return nil
				}
			}
		}
	}

	return
}

// MinimalBistreamFunc stands in contract
// to the fleshed on ServeBistream example above.
// It attempts to illustrate only the bare minimal
// needed to implement a BistreamFunc, so
// the user can glimpse its conceptual elegance.
//
// It provides a starter template for writing your own.
//
// Typically you might want to make it a
// method on a struct that provides it with
// other application dependent state and helper methods.
// But that's not minimal.
func MinimalBistreamFunc(
	srv *Server,
	ctx context.Context,
	req *Message,
	uploadsFromClientCh <-chan *Message,
	sendDownloadPartToClient func(ctx context.Context, by []byte, last bool) error,
	lastReply *Message,
) (err error) {

	done := ctx.Done()
	for {
		select {
		// upload stream parts arrive here
		case msg := <-uploadsFromClientCh:

			// ... process this uploaded part ...

			// Note! the req pointer is supplied both by
			// the actual argument req in our call arguments,
			// *and* in the fromClientCh: it will
			// be the first msg *Message seen here.
			// Be sure to only process it once!
			// The req.HDR.StreamPart will be 0.

			// ...and, when we have a part for the download stream,
			// send it on to the client like this
			last := false // set to true if no more downloads to follow.
			err = sendDownloadPartToClient(ctx, msg.JobSerz, last)
			if err != nil {
				// connection to client may have been lost...
				return err
			}

			// ... more processing of msg if need be ...

		case <-done:
			// ...do any cleanup,
			// ...and decide what to do if len(uploadsFromClientCh) > 0 ...
			// ...then ship this lastReply to the client...
			lastReply.JobSerz = []byte("that's all folks!")
			return
		}
	}

	return
}

func blake3OfBytes(by []byte) []byte {
	h := blake3.New(64, nil)
	h.Write(by)
	return h.Sum(nil)
}

func blake3OfBytesString(by []byte) string {
	sum := blake3OfBytes(by)
	return "blake3.32B-" + cristalbase64.URLEncoding.EncodeToString(sum[:32])
}

// Echo anything we get from the client back. srv uses to test bistreaming.
func EchoBistreamFunc(
	srv *Server,
	ctx context.Context,
	req *Message,
	uploadsFromClientCh <-chan *Message,
	sendDownloadPartToClient func(ctx context.Context, msg *Message, last bool) error,
	lastReply *Message,
) (err error) {

	done := ctx.Done()
	//vv("top of EchoBistreamFunc; done = %p", done)
	for {
		//vv("EchoBistreamFunc; about to select (get upload from client or done)")
		select {
		// upload stream parts arrive here
		case msg := <-uploadsFromClientCh:
			//vv("got msg in EchoBistreamFunc: '%v'", msg.HDR.String())

			// simplest thing, echo back what we got.
			// but convert it down download
			last := false
			switch msg.HDR.Typ {
			case CallUploadBegin:
				msg.HDR.Typ = CallDownloadBegin
			case CallUploadMore:
				msg.HDR.Typ = CallDownloadMore
			case CallUploadEnd:
				msg.HDR.Typ = CallDownloadEnd
				last = true
			}
			err = sendDownloadPartToClient(ctx, msg, last)
			if err != nil {
				// connection to client may have been lost...
				return err
			}
			if last {
				//vv("EchoBistreamFunc saw CallUploadEnd, finishing.")
				return nil
			}
		case <-done:
			//vv("EchoBistreamFunc has been cancelled, sending CallDownloadEnd and that's all folks.")
			sendDownloadPartToClient(ctx, nil, true)
			lastReply.JobSerz = []byte("that's all folks!")
			return
		}
	}

	return
}
