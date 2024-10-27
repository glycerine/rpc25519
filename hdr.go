package rpc25519

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"

	crand "crypto/rand"
	"github.com/btcsuite/btcd/btcutil/base58"
	gjson "github.com/goccy/go-json"
)

//go:generate greenpack

const rfc3339NanoNumericTZ0pad = "2006-01-02T15:04:05.000000000-07:00"

var lastSerial int64

var myPID = int64(os.Getpid())

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

	// Err is not serialized on the wire by the server.
	// It communicates only local (client/server side) information. Callback
	// functions should convey errors in JobErrs or in-band within
	// JobSerz.
	Err error `msg:"-"`

	// DoneCh will receive this Message itself when the call completes.
	// It must be buffered, with at least capacity 1.
	// NewMessage() automatically allocates DoneCh correctly and
	// should be used when creating a new Message.
	DoneCh chan *Message `msg:"-"`
}

// allocate this just once
var keepAliveMsg = &Message{
	HDR: HDR{IsKeepAlive: true},
}

// MessageFromGreenpack unmarshals the by slice
// into a Message and returns it.
// The [greenpack format](https://github.com/glycerine/greenpack) is expected.
func MessageFromGreenpack(by []byte) (*Message, error) {
	msg := NewMessage()
	_, err := msg.UnmarshalMsg(by)
	return msg, err
}

// AsGreenpack marshalls m into o.
// The scratch workspace can be nil or reused to avoid allocation.
// The [greenpack format](https://github.com/glycerine/greenpack) is used.
func (m *Message) AsGreenpack(scratch []byte) (o []byte, err error) {
	return m.MarshalMsg(scratch[:0])
}

// HDR provides header information and details
// about the transport. It is the first thing in every Message.
// It is public so that clients can understand the
// context of their calls. Traditional `net/rpc` API users
// can use the `ctx context.Context` first argument
// form of callback methods and get an *HDR with ctx.Value("HDR")
// as in the README.md introduction. Reproduced here:
//
//	func (s *Service) GetsContext(ctx context.Context, args *Args, reply *Reply) error {
//	  if hdr := ctx.Value("HDR"); hdr != nil {
//	     h, ok := hdr.(*rpc25519.HDR)
//	     if ok {
//	       fmt.Printf("GetsContext called with HDR = '%v'; "+
//	          "HDR.Nc.RemoteAddr() gives '%v'; HDR.Nc.LocalAddr() gives '%v'\n",
//	          h.String(), h.Nc.RemoteAddr(), h.Nc.LocalAddr())
//	     }
//	  } else {
//	     fmt.Println("HDR not found")
//	  }
//	}
type HDR struct {

	// Nc is supplied to reveal the LocalAddr() or RemoteAddr() end points.
	// Do not read from, or write to, this connection;
	// that will cause the RPC connection to fail.
	Nc net.Conn `msg:"-"`

	Created     time.Time `zid:"0"`  // HDR creation time stamp.
	From        string    `zid:"1"`  // originator host:port address.
	To          string    `zid:"2"`  // destination host:port address.
	Subject     string    `zid:"3"`  // in net/rpc, the "Service.Method" ServiceName
	IsRPC       bool      `zid:"4"`  // in rpc25519 Message API, is this a TwoWayFunc call?
	IsLeg2      bool      `zid:"5"`  // in rpc25519 Message API, is TwoWayFunc reply?
	Serial      int64     `zid:"6"`  // serially incremented tracking number
	CallID      string    `zid:"7"`  // 40-byte crypto/rand base-58 coded string (same on response).
	PID         int64     `zid:"8"`  // Process ID of originator.
	Seqno       uint64    `zid:"9"`  // client set sequence number for each call (same on response).
	IsNetRPC    bool      `zid:"10"` // is net/rpc API in use for this request/response?
	IsKeepAlive bool      `zid:"11"` // keepalive message, no other fields expected.
}

// NewHDR creates a new HDR header.
func NewHDR(from, to, subject string, isRPC bool, isLeg2 bool) (m *HDR) {
	t0 := time.Now()
	//created := t0.In(chicago).Format(rfc3339NanoNumericTZ0pad)
	serial := atomic.AddInt64(&lastSerial, 1)
	// unchecked base58
	rness := toUncheckedBase58(cryptoRandBytes(40))
	m = &HDR{
		Created: t0,
		From:    from,
		To:      to,
		Subject: subject,
		IsRPC:   isRPC,
		IsLeg2:  isLeg2,
		Serial:  serial,
		CallID:  rness,
		PID:     myPID,
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
		a.Subject == b.Subject &&
		a.IsRPC == b.IsRPC &&
		a.IsLeg2 == b.IsLeg2 &&
		a.Serial == b.Serial &&
		a.CallID == b.CallID &&
		a.PID == b.PID &&
		a.Seqno == b.Seqno &&
		a.IsNetRPC == b.IsNetRPC
}

func (m *HDR) String() string {
	return m.Pretty()
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

func (m *HDR) OpaqueURLFriendly() string {
	j := m.JSON()
	s := toUncheckedBase58(j)
	return "mid2024-" + s
}

const prefix = "mid2024-"

func HDRFromOpaqueURLFriendly(s string) (*HDR, error) {
	if !strings.HasPrefix(s, prefix) {
		return nil, fmt.Errorf("did not begin with prefix 'mid2024-'")
	}
	jsonData := fromUncheckedBase58(s[len(prefix):])
	var mid HDR
	err := gjson.Unmarshal(jsonData, &mid)
	if err != nil {
		return nil, err
	}
	return &mid, nil
}

func cryptoRandBytes(n int) []byte {
	b := make([]byte, n)
	_, err := crand.Read(b)
	if err != nil {
		panic(err)
	}
	return b
}

func toUncheckedBase58(by []byte) string {
	return base58.Encode(by)
}
func fromUncheckedBase58(encodedStr string) []byte {
	return base58.Decode(encodedStr)
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
