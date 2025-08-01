package rpc25519

import (
	"bytes"
	//"encoding/hex"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	cryrand "crypto/rand"
	cristalbase64 "github.com/cristalhq/base64"
	"github.com/glycerine/greenpack/msgp"
	"github.com/glycerine/loquet"
	gjson "github.com/goccy/go-json"
	mathrand2 "math/rand/v2"
)

var _ = cristalbase64.URLEncoding

//go:generate greenpack

type CallType int

const (
	CallNone CallType = 0

	CallRPC    CallType = 1
	CallNetRPC CallType = 2

	CallRequestBistreaming CallType = 3

	// CallType numbers < 100 are reserved for future two-way
	// callling convention needs. All types
	// with number < 100 should be call+response
	// two way methods. number >= 100 are for one-way sends.

	// All type numbers >= 100 are one-way calls.
	CallOneWay         CallType = 100
	CallRPCReply       CallType = 101
	CallKeepAlive      CallType = 102
	CallCancelPrevious CallType = 103
	CallError          CallType = 104 // we could not complete a request

	// client sends a stream to the server, in an Upload:
	CallUploadBegin CallType = 105 // one of these; and
	CallUploadMore  CallType = 106 // possibly many of these; and
	CallUploadEnd   CallType = 107 // just one of these to finish.

	// the opposite: when client wants to get a stream
	// from the server.
	CallRequestDownload CallType = 108

	// The server responds to CallRequestDownload with
	CallDownloadBegin CallType = 109 // one of these to start;
	CallDownloadMore  CallType = 110 // possibly many of these;
	CallDownloadEnd   CallType = 111 // and one of these to finish.

	// try to keep all peer traffic isolated
	// and only using these:
	CallPeerStart        CallType = 112
	CallPeerStartCircuit CallType = 113

	// CallPeerCircuitEstablishedAck is sent to
	// ack all circuit creation so as to make fragRPC possible;
	// it was added later and is off the critical path so
	// most frag/ckt operations need not stall waiting for it.
	// But and RPC style frag op which wants to wait
	// for confirmation that the newly requested
	// circuit has been made, can wait.
	CallPeerCircuitEstablishedAck CallType = 114
	CallPeerStartCircuitTakeToID  CallType = 115

	// do not start a second peer, create ckt with existing.
	CallPeerStartCircuitAtMostOne CallType = 116

	CallPeerTraffic        CallType = 117
	CallPeerError          CallType = 118
	CallPeerFromIsShutdown CallType = 119
	CallPeerEndCircuit     CallType = 120
)

func (ct CallType) String() string {
	switch ct {
	case CallNone:
		return "CallNone"
	case CallRPC:
		return "CallRPC "
	case CallRPCReply:
		return "CallRPCReply"
	case CallOneWay:
		return "CallOneWay"
	case CallNetRPC:
		return "CallNetRPC"
	case CallRequestBistreaming:
		return "CallRequestBistreaming"

	case CallPeerStart:
		return "CallPeerStart"
	case CallPeerStartCircuit:
		return "CallPeerStartCircuit"
	case CallPeerStartCircuitAtMostOne:
		return "CallPeerStartCircuitAtMostOne"
	case CallPeerCircuitEstablishedAck:
		return "CallPeerCircuitEstablishedAck"

	case CallPeerTraffic:
		return "CallPeerTraffic"
	case CallPeerError:
		return "CallPeerError"
	case CallPeerFromIsShutdown:
		return "CallPeerFromIsShutdown"
	case CallPeerEndCircuit:
		return "CallPeerEndCircuit"
	case CallPeerStartCircuitTakeToID:
		return "CallPeerStartCircuitTakeToID"

	case CallKeepAlive:
		return "CallKeepAlive"
	case CallCancelPrevious:
		return "CallCancelPrevious"
	case CallUploadBegin:
		return "CallUploadBegin"
	case CallUploadMore:
		return "CallUploadMore"
	case CallUploadEnd:
		return "CallUploadEnd"

	case CallRequestDownload:
		return "CallRequestDownload"
	case CallDownloadBegin:
		return "CallDownloadBegin"
	case CallDownloadMore:
		return "CallDownloadMore"
	case CallDownloadEnd:
		return "CallDownloadEnd"
	case CallError:
		return "CallError"

	default:
		panic(fmt.Sprintf("need to update String() for CallType %v", int(ct)))
	}
}

const rfc3339NanoNumericTZ0pad = "2006-01-02T15:04:05.000000000-07:00"

var lastSerialPrivate int64

func issueSerial() (cur int64) {
	cur = atomic.AddInt64(&lastSerialPrivate, 1)
	//if cur == 10 {
	//	vv("here is serial 10 stack \n'%v'", stack())
	//}
	return
}

var myPID = int64(os.Getpid())

var chacha8randMut sync.Mutex
var chacha8rand *mathrand2.ChaCha8 = newCryrandSeededChaCha8()

func newCryrandSeededChaCha8() *mathrand2.ChaCha8 {
	var seed [32]byte
	_, err := cryrand.Read(seed[:])
	panicOn(err)
	return mathrand2.NewChaCha8(seed)
}

func cryRandBytesBase64(numBytes int) string {
	by := make([]byte, numBytes)
	_, err := cryrand.Read(by)
	panicOn(err)
	return cristalbase64.URLEncoding.EncodeToString(by)
}

// Message transports JobSerz []byte slices for
// the user, who can de-serialize them they wish.
// The HDR header field provides transport details.
type Message struct {

	// HDR contains header information.
	HDR HDR `zid:"0"`

	// JobSerz is the "body" of the message.
	// The user provides and interprets this.
	JobSerz []byte `zid:"1"`

	// JobErrs returns error information from the server-registered
	// user-defined callback functions.
	JobErrs string `zid:"2"`

	// LocalErr is not serialized on the wire by the server.
	// It communicates only local (client/server side) information.
	//
	// Callback functions convey
	// errors in JobErrs (by returning an error);
	// or in-band within JobSerz.
	LocalErr error `msg:"-"`

	// DoneCh.WhenClosed will be closed on the client when the one-way is
	// sent or the round-trip call completes.
	// NewMessage() automatically allocates DoneCh correctly and
	// should be used when creating a new Message (on the client to send).
	DoneCh *loquet.Chan[Message] `msg:"-"`

	// for emulating a socket connection,
	// after the JobSerz bytes are read that is the end-of-file.
	EOF bool `zid:"3"`

	nextOrReply *Message // free list on server, replies to round-trips in the client.
}

// CopyForSimNetSend is used by the simnet
// so that senders can overwrite their sent
// messages once they are "in the network", emulating
// the copy that the kernel does for socket writes.
// For safety, this cloning zeroes DoneCh, nextOrReply, ...
// to avoid false-sharing; anything marked `msg:"-"`
// would not be serialized by greenpack when
// sent over a network.
// In cl.HDR, we nil out Nc, Ctx, and streamCh
// -- they are marked `msg:"-"` and are expected
// to be set by the receiver when needed.
func (m *Message) CopyForSimNetSend() (c *Message) {
	cp := *m
	c = &cp
	c.nextOrReply = nil
	// make our own copy of these central/critical bytes.
	c.JobSerz = append([]byte{}, m.JobSerz...)
	c.LocalErr = nil // marked msg:"-"
	// like MessageFromGreenpack, DoneCh is
	// not needed for sends/reads.
	c.DoneCh = nil
	c.nextOrReply = nil

	c.HDR.Nc = nil
	c.HDR.Args = make(map[string]string)
	for k, v := range m.HDR.Args {
		c.HDR.Args[k] = v
	}
	c.HDR.Ctx = nil
	c.HDR.streamCh = nil
	return
}

func copyArgsMap(args map[string]string) (clone map[string]string) {
	if args == nil || len(args) == 0 {
		return
	}
	clone = make(map[string]string)
	for k, v := range args {
		clone[k] = v
	}
	return
}

// interface for goq

// NewMessage allocates a new Message with a DoneCh properly created.
func NewMessage() *Message {
	m := &Message{}
	m.DoneCh = loquet.NewChan(m)
	m.HDR.Args = make(map[string]string)
	return m
}

// String returns a string representation of msg.
func (msg *Message) String() string {
	return fmt.Sprintf("&Message{HDR:%v, LocalErr:'%v', len %v JobSerz}", msg.HDR.String(), msg.LocalErr, len(msg.JobSerz))
}

// NewMessageFromBytes calls NewMessage() and sets by as the JobSerz field.
func NewMessageFromBytes(by []byte) (msg *Message) {
	msg = NewMessage()
	msg.JobSerz = by
	return
}

// newServerMessage returns a Message without allocating a channel,
// since the server does not need it.
func newServerMessage() *Message {
	return &Message{}
}

// allocate this just once
var keepAliveMsg = &Message{
	HDR: HDR{Typ: CallKeepAlive},
}

// MessageFromGreenpack unmarshals the by slice
// into a Message and returns it.
// The [greenpack format](https://github.com/glycerine/greenpack) is expected.
func MessageFromGreenpack(by []byte) (*Message, error) {
	// client and server readers do not need a DoneCh,
	// and it is slow to allocate. Really only senders
	// need a DoneCh to track if the Message went or not.
	// Otherwise we are comming from a remote and we
	// handle and send back on errors on the one responding
	// goroutine, without needing a channel to report on.
	//msg := NewMessage()
	msg := &Message{}
	_, err := msg.UnmarshalMsg(by)
	return msg, err
}

var ErrTooLarge = fmt.Errorf("error: length of payload JobSerz is over maxMessage - 1024(for header) = %v bytes, which is the limit.", maxMessage-1024)

// AsGreenpack marshalls m into o.
// The scratch workspace can be nil or reused to avoid allocation.
// The [greenpack format](https://github.com/glycerine/greenpack) is used.
// The m.JobSerz payload must be <= maxMessage-1024, or we
// will return ErrTooLarge without trying to serialize it.
func (m *Message) AsGreenpack(scratch []byte) (o []byte, err error) {
	if len(m.JobSerz) > maxMessage-1024 {
		vv("ErrTooLarge! len(m.JobSerz)= %v > %v = maxMessage-1024; \n m=\n%v\n", len(m.JobSerz), maxMessage-1024, m.HDR.String())
		return nil, ErrTooLarge
	}
	return m.MarshalMsg(scratch[:0])
}

// AsJSON returns JSON bytes via msgp.CopyToJSON() or msgp.UnmarshalAsJSON()
func (m *Message) AsJSON(scratch []byte) (o []byte, err error) {
	o, err = m.MarshalMsg(scratch[:0])
	if err != nil {
		return
	}
	var jsonBuf bytes.Buffer
	o, err = msgp.UnmarshalAsJSON(&jsonBuf, o)
	if err != nil {
		return
	}
	o = jsonBuf.Bytes()
	return
}

// HDR provides header information and details
// about the transport. It is the first thing in every Message.
// It is public so that clients can understand the
// context of their calls. Traditional `net/rpc` API users
// can use the `ctx context.Context` first argument
// form of callback methods and get an *HDR with HDRFromContext()
// as in the README.md introduction. Reproduced here:
//
//	func (s *Service) GetsContext(ctx context.Context, args *Args, reply *Reply) error {
//	    if hdr, ok := HDRFromContext(ctx); ok {
//	       fmt.Printf("GetsContext called with HDR = '%v'; "+
//	          "HDR.Nc.RemoteAddr() gives '%v'; HDR.Nc.LocalAddr() gives '%v'\n",
//	          hdr.String(), hdr.Nc.RemoteAddr(), hdr.Nc.LocalAddr())
//	    }
//	}
type HDR struct {

	// Nc is supplied to reveal the LocalAddr() or RemoteAddr() end points.
	// Do not read from, or write to, this connection;
	// that will cause the RPC connection to fail.
	Nc net.Conn `msg:"-"`

	Created time.Time `zid:"0"` // HDR creation time stamp.
	From    string    `zid:"1"` // originator host:port address.
	To      string    `zid:"2"` // destination host:port address.

	ServiceName string `zid:"3"` // registered name to call.

	// arguments/parameters for the call. should be short to keep the HDR small.
	// big stuff should be serialized in JobSerz.
	Args map[string]string `zid:"4"`

	Subject string   `zid:"5"` // in net/rpc, the "Service.Method" ServiceName
	Seqno   uint64   `zid:"6"` // user (client) set sequence number for each call (same on response).
	Typ     CallType `zid:"7"` // see constants above.
	CallID  string   `zid:"8"` // 20 bytes pseudo random base-64 coded string (same on response).
	Serial  int64    `zid:"9"` // system serial number

	LocalRecvTm time.Time `zid:"10"`

	// allow standard []byte oriented message to cancel too.
	Ctx context.Context `msg:"-"`

	// Deadline is optional, but if it is set on the client,
	// the server side context.Context will honor it.
	Deadline time.Time `zid:"11"` // if non-zero, set this deadline in the remote Ctx

	// The CallID will be identical on
	// all parts of the same stream.
	StreamPart int64 `zid:"12"`

	// NoSystemCompression turns off any usual
	// compression that the rpc25519 system
	// applies, for just sending this one Message.
	//
	// Not normally a needed (or a good idea),
	// this flag is for efficiency when the
	// user has implemented their own custom compression
	// scheme for the JobSerz data payload.
	//
	// By checking this flag, the system can
	// avoid wasting time attempting
	// to compress a second time; since the
	// user has, hereby, marked this Message
	// as incompressible.
	//
	// Not matched in reply compression;
	// this flag will not affect the usual
	// compression-matching in responses.
	// For those purposes, it is ignored.
	NoSystemCompression bool `zid:"13"`

	// ToPeerID and FromPeerID help maintain stateful sub-calls
	// allowing client/server symmetry when
	// implementing complex stateful protocols.
	ToPeerID   string `zid:"14"`
	ToPeerName string `zid:"15"`

	FromPeerID   string `zid:"16"`
	FromPeerName string `zid:"17"`

	FragOp int `zid:"18"`

	// streamCh is internal; used for client -> server streaming on CallUploadBegin
	streamCh chan *Message `msg:"-" json:"-"`
}

// NewHDR creates a new HDR header.
func NewHDR(from, to, serviceName string, typ CallType, streamPart int64) (m *HDR) {
	t0 := time.Now()
	serial := issueSerial()

	callID := NewCallID(serviceName)

	m = &HDR{
		Created:     t0,
		From:        from,
		To:          to,
		ServiceName: serviceName,
		Typ:         typ,
		CallID:      callID,
		Serial:      serial,
		StreamPart:  streamPart,
	}

	return
}

func NewCallID(name string) (cid string) {
	var pseudo [21]byte // not cryptographically random.
	chacha8randMut.Lock()
	chacha8rand.Read(pseudo[:])
	chacha8randMut.Unlock()
	cid = cristalbase64.URLEncoding.EncodeToString(pseudo[:])
	if name != "" { // traditional CallID won't have.
		AliasRegister(cid, cid+" ("+name+")")
	}
	return
}

func NewCryRandCallID() (cid string) {
	var random [21]byte
	cryrand.Read(random[:])
	cid = cristalbase64.URLEncoding.EncodeToString(random[:])
	return
}

func NewCryRandSuffix() (cid string) {
	var random [21]byte
	cryrand.Read(random[:])
	cid = cristalbase64.URLEncoding.EncodeToString(random[:])
	return
}

// for when the server is just going to replace the CallID with
// the request CallID anyway.
func newHDRwithoutCallID(from, to, serviceName string, typ CallType, streamPart int64) (m *HDR) {
	t0 := time.Now()
	serial := issueSerial()

	m = &HDR{
		Created:     t0,
		From:        from,
		To:          to,
		ServiceName: serviceName,
		//Subject: subject,
		Typ: typ,
		//CallID:  rness,
		Serial:     serial,
		StreamPart: streamPart,
	}

	return
}

// Equal compares two *HDR structs field by field for structural equality
func (a *HDR) Equal(b *HDR) bool {
	if a == b {
		return true
	}

	if a == nil || b == nil {
		return false
	}

	if len(a.Args) != len(b.Args) {
		return false
	}
	for k, v := range a.Args {
		if b.Args[k] != v {
			return false
		}
	}

	return a.Created.Equal(b.Created) &&
		a.From == b.From &&
		a.To == b.To &&
		a.Serial == b.Serial &&
		a.ServiceName == b.ServiceName &&
		a.Subject == b.Subject &&
		a.Typ == b.Typ &&
		a.CallID == b.CallID &&
		a.Seqno == b.Seqno &&
		a.StreamPart == b.StreamPart &&
		a.ToPeerID == b.ToPeerID &&
		a.FromPeerID == b.FromPeerID
}

func (m *HDR) String() string {
	//return m.Pretty()
	return fmt.Sprintf(`&rpc25519.HDR{
         Created: %q
            From: %q
              To: %q
     ServiceName: %q
            Args: %#v
         Subject: %q
           Seqno: %v
             Typ: %s
          CallID: %q
          Serial: %v
     LocalRecvTm: %s
        Deadline: %s
      FromPeerID: "%v"
    FromPeerName: "%v"
        ToPeerID: "%v"
      ToPeerName: "%v"
      StreamPart: %v
          FragOp: %v
}`,
		m.Created,
		AliasDecode(m.From),
		AliasDecode(m.To),
		m.ServiceName,
		m.Args,
		m.Subject,
		m.Seqno,
		m.Typ,
		AliasDecode(m.CallID),
		m.Serial,
		m.LocalRecvTm,
		m.Deadline,
		AliasDecode(m.FromPeerID),
		m.FromPeerName,
		AliasDecode(m.ToPeerID),
		m.ToPeerName,
		m.StreamPart,
		FragOpDecode(m.FragOp),
	)
}

// Compact is all on one line.
func (m *HDR) Compact() string {
	return fmt.Sprintf("%#v", m)
}

// JSON serializes to JSON.
func (m *HDR) JSON() []byte {
	jsonData, err := json.Marshal(m)
	panicOn(err)
	return jsonData
}

// Bytes serializes to compact JSON formatted bytes.
func (m *HDR) Bytes() []byte {
	return m.JSON()
}

// Unbytes reverses Bytes.
func Unbytes(jsonData []byte) *HDR {
	var mid HDR
	err := gjson.Unmarshal(jsonData, &mid)
	panicOn(err)
	return &mid
}

func HDRFromBytes(jsonData []byte) (*HDR, error) {
	var mid HDR
	err := gjson.Unmarshal(jsonData, &mid)
	if err != nil {
		return nil, err
	}
	return &mid, nil
}

// Pretty shows in pretty-printed JSON format.
func (m *HDR) Pretty() string {
	by := m.JSON()
	var pretty bytes.Buffer
	err := json.Indent(&pretty, by, "", "    ")
	panicOn(err)
	return pretty.String()
}

func cryptoRandBytes(n int) []byte {
	b := make([]byte, n)
	_, err := cryrand.Read(b)
	if err != nil {
		panic(err)
	}
	return b
}

// HDRFromGreenpack will unmarshal the header
// into the returned struct.
// The [greenpack format](https://github.com/glycerine/greenpack) is expected.
func HDRFromGreenpack(header []byte) (*HDR, error) {
	var mid HDR

	// UnmarshalMsg unmarshals the object
	// from binary, returing any leftover
	// bytes and any errors encountered.
	_, err := mid.UnmarshalMsg(header)
	return &mid, err
}

// AsGreenpack will marshall hdr into the o output bytes.
// The scratch bytes can be nil or
// reused and returned to avoid allocation.
// The [greenpack format](https://github.com/glycerine/greenpack) is used.
func (hdr *HDR) AsGreenpack(scratch []byte) (o []byte, err error) {

	// MarshalMsg appends the marshalled
	// form of the object to the provided
	// byte slice, returning the extended
	// slice and any errors encountered.

	// We don't use a global scratchspace because we
	// don't want goroutines to collide over it.
	// For memory tuning,
	return hdr.MarshalMsg(scratch[:0])
}

// hdrKeyType is an unexported type for keys defined in this package.
// This prevents collisions with keys defined in other packages.
// This is the recommended method in the context.Context docs.
// See the public access func below for setting and getting.
type hdrKeyType int

// hdrKey is the key for *rpc25519.HDR values in Contexts. It is
// unexported; clients use user.NewContext and user.FromContext
// instead of using this key directly.
var hdrKey hdrKeyType = 43

// ContextWithHDR returns a new Context that carries value hdr.
func ContextWithHDR(ctx context.Context, hdr *HDR) context.Context {
	return context.WithValue(ctx, hdrKey, hdr)
}

// HDRFromContext returns the User value stored in ctx, if any.
func HDRFromContext(ctx context.Context) (*HDR, bool) {
	hdr, ok := ctx.Value(hdrKey).(*HDR)
	return hdr, ok
}

// print user legible names along side the host:port number.
var AliasMap sync.Map

func AliasDecode(addr string) string {
	v, ok := AliasMap.Load(addr)
	if ok {
		return v.(string)
	}
	return addr
}

func AliasRegister(addr, name string) {
	AliasMap.Store(addr, name)
}

var FragOpMap sync.Map

func FragOpDecode(op int) (name string) {
	v, ok := FragOpMap.Load(op)
	if ok {
		return v.(string)
	}
	return fmt.Sprintf("%v", op)
}
func FragOpRegister(op int, name string) {
	FragOpMap.Store(op, fmt.Sprintf("%v (%v)", op, name))
}
