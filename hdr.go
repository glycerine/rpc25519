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

	// CallType numbers 3-9 are reserved for future two-way
	// callling convention needs. All types
	// with number < 10 should be call+response
	// two way methods.

	// All type numbers >= 10 are one-way calls.
	CallOneWay   CallType = 10
	CallRPCReply CallType = 11

	CallKeepAlive      CallType = 12
	CallCancelPrevious CallType = 13

	// client sends a stream to the server, in an Upload:
	CallUploadBegin CallType = 14 // one of these; and
	CallUploadMore  CallType = 15 // possibly many of these; and
	CallUploadEnd   CallType = 16 // just one of these to finish.

	// the opposite: when client wants to get a stream
	// from the server.
	CallRequestDownload CallType = 17

	// The server responds to CallRequestDownload with
	CallDownloadBegin CallType = 18 // one of these to start;
	CallDownloadMore  CallType = 19 // possibly many of these;
	CallDownloadEnd   CallType = 20 // and one of these to finish.
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

	default:
		panic(fmt.Sprintf("need to update String() for CallType %v", int(ct)))
	}
}

const rfc3339NanoNumericTZ0pad = "2006-01-02T15:04:05.000000000-07:00"

var lastSerial int64

var myPID = int64(os.Getpid())

var chacha8randMut sync.Mutex
var chacha8rand *mathrand2.ChaCha8 = newCryrandSeededChaCha8()

func newCryrandSeededChaCha8() *mathrand2.ChaCha8 {
	var seed [32]byte
	_, err := cryrand.Read(seed[:])
	panicOn(err)
	return mathrand2.NewChaCha8(seed)
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

	// DoneCh will receive this Message itself when the call completes.
	// It must be buffered, with at least capacity 1.
	// NewMessage() automatically allocates DoneCh correctly and
	// should be used when creating a new Message.
	DoneCh chan *Message `msg:"-"`

	next *Message // free list on server
}

// interface for goq

// NewMessage allocates a new Message with a DoneCh properly created (buffered 1).
func NewMessage() *Message {
	return &Message{
		// NOTE: buffer size must be at least 1, so our Client.runSendLoop never blocks.
		// Thus we simplify the logic there, not requiring a ton of extra selects to
		// handle shutdown/timeout/etc.
		// Update: we make it capacity 2 here to avoid the race after a context cancelation
		// where both the cancel message and the original response come back,
		// which would cause us to hang in the send loop.
		DoneCh: make(chan *Message, 2),
	}
}

// String returns a string representation of msg.
func (msg *Message) String() string {
	return fmt.Sprintf("&Message{HDR:%v, LocalErr:'%v'}", msg.HDR.String(), msg.LocalErr)
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

// AsGreenpack marshalls m into o.
// The scratch workspace can be nil or reused to avoid allocation.
// The [greenpack format](https://github.com/glycerine/greenpack) is used.
func (m *Message) AsGreenpack(scratch []byte) (o []byte, err error) {
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
	Subject string    `zid:"3"` // in net/rpc, the "Service.Method" ServiceName
	Seqno   uint64    `zid:"4"` // user (client) set sequence number for each call (same on response).
	Typ     CallType  `zid:"5"` // see constants below.
	CallID  string    `zid:"6"` // 20 bytes pseudo random base-64 coded string (same on response).
	Serial  int64     `zid:"7"` // system serial number

	LocalRecvTm time.Time `zid:"8"`

	// allow standard []byte oriented message to cancel too.
	Ctx context.Context `msg:"-"`

	// Deadline is optional, but if it is set on the client,
	// the server side context.Context will honor it.
	Deadline time.Time `zid:"9"` // if non-zero, set this deadline in the remote Ctx

	// The CallID will be identical on
	// all parts of the same stream.
	StreamPart int64 `zid:"10"`

	// streamCh is internal; used for client -> server streaming on CallUploadBegin
	streamCh chan *Message `msg:"-"`
}

// NewHDR creates a new HDR header.
func NewHDR(from, to, subject string, typ CallType, streamPart int64) (m *HDR) {
	t0 := time.Now()
	serial := atomic.AddInt64(&lastSerial, 1)

	var pseudo [20]byte // not cryptographically random.
	chacha8randMut.Lock()
	chacha8rand.Read(pseudo[:])
	chacha8randMut.Unlock()
	rness := cristalbase64.URLEncoding.EncodeToString(pseudo[:])

	m = &HDR{
		Created:    t0,
		From:       from,
		To:         to,
		Subject:    subject,
		Typ:        typ,
		CallID:     rness,
		Serial:     serial,
		StreamPart: streamPart,
	}

	return
}

func NewCallID() string {
	var pseudo [20]byte // not cryptographically random.
	chacha8randMut.Lock()
	chacha8rand.Read(pseudo[:])
	chacha8randMut.Unlock()
	return cristalbase64.URLEncoding.EncodeToString(pseudo[:])
}

// for when the server is just going to replace the CallID with
// the request CallID anyway.
func newHDRwithoutCallID(from, to, subject string, typ CallType, streamPart int64) (m *HDR) {
	t0 := time.Now()
	serial := atomic.AddInt64(&lastSerial, 1)

	m = &HDR{
		Created: t0,
		From:    from,
		To:      to,
		Subject: subject,
		Typ:     typ,
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

	return a.Created.Equal(b.Created) &&
		a.From == b.From &&
		a.To == b.To &&
		a.Serial == b.Serial &&
		a.Subject == b.Subject &&
		a.Typ == b.Typ &&
		a.CallID == b.CallID &&
		a.Seqno == b.Seqno &&
		a.StreamPart == b.StreamPart
}

func (m *HDR) String() string { // has data race
	//return m.Pretty()
	return fmt.Sprintf(`&rpc25519.HDR{
    "Nc": %v,
    "Created": %q,
    "From": %q,
    "To": %q,
    "Subject": %q,
    "Seqno": %v,
    "Typ": %s,
    "CallID": %q,
    "Serial": %v,
    "LocalRecvTm": "%s",
    "Deadline": "%s",
    "StreamPart": %v
}`,
		m.Nc,
		m.Created,
		m.From,
		m.To,
		m.Subject,
		m.Seqno,
		m.Typ,
		m.CallID,
		m.Serial,
		m.LocalRecvTm,
		m.Deadline,
		m.StreamPart,
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
