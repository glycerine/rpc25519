package rpc25519

import (
	"bytes"
	//"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	cryrand "crypto/rand"
	cristalbase64 "github.com/cristalhq/base64"
	gjson "github.com/goccy/go-json"
	mathrand2 "math/rand/v2"
)

//go:generate greenpack

const rfc3339NanoNumericTZ0pad = "2006-01-02T15:04:05.000000000-07:00"

var lastSerial int64

var myPID = int64(os.Getpid())

var chacha8randMut sync.Mutex
var chacha8rand *mathrand2.ChaCha8

func init() {
	var seed [32]byte
	_, err := cryrand.Read(seed[:])
	panicOn(err)
	chacha8rand = mathrand2.NewChaCha8(seed)
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
}

// allocate this just once
var keepAliveMsg = &Message{
	HDR: HDR{Typ: CallKeepAlive},
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

	Created time.Time `zid:"0"` // HDR creation time stamp.
	From    string    `zid:"1"` // originator host:port address.
	To      string    `zid:"2"` // destination host:port address.
	Subject string    `zid:"3"` // in net/rpc, the "Service.Method" ServiceName
	Seqno   uint64    `zid:"4"` // user (client) set sequence number for each call (same on response).
	Typ     CallType  `zid:"5"` // see constants below.
	CallID  string    `zid:"6"` // 20 bytes pseudo random base-64 coded string (same on response).
	Serial  int64     `zid:"7"` // system serial number
}

type CallType int

const (
	CallNone      CallType = 0
	CallRPC       CallType = 1
	CallOneWay    CallType = 2
	CallNetRPC    CallType = 3
	CallKeepAlive CallType = 4
)

// NewHDR creates a new HDR header.
func NewHDR(from, to, subject string, typ CallType) (m *HDR) {
	t0 := time.Now()
	serial := atomic.AddInt64(&lastSerial, 1)

	//rness := cristalbase64.URLEncoding.EncodeToString(cryptoRandBytes(32))

	var pseudo [20]byte // not cryptographically random.
	chacha8randMut.Lock()
	chacha8rand.Read(pseudo[:])
	chacha8randMut.Unlock()
	rness := cristalbase64.URLEncoding.EncodeToString(pseudo[:])

	m = &HDR{
		Created: t0,
		From:    from,
		To:      to,
		Subject: subject,
		Typ:     typ,
		CallID:  rness,
		Serial:  serial,
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
		a.Seqno == b.Seqno
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
