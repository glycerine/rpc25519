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

const RFC3339NanoNumericTZ0pad = "2006-01-02T15:04:05.000000000-07:00"

var lastSerial int64

var myPID = int64(os.Getpid())

// Message basic substrate.
type Message struct {

	// HDR contains header information.
	HDR HDR `zid:"0"`

	JobSerz []byte `zid:"1"`

	JobErrs string `zid:"2"`

	// Err is not serialized on the wire by the server,
	// so communicates only local information. Callback
	// functions should convey errors in-band within
	// JobSerz.
	Err error `msg:"-"`

	DoneCh chan *Message `msg:"-"`
}

func MessageFromGreenpack(by []byte) (*Message, error) {
	msg := NewMessage()
	_, err := msg.UnmarshalMsg(by)
	return msg, err
}

// the scrach workspace can be nil or reused to avoid allocation.
func (m *Message) AsGreenpack(scratch []byte) (o []byte, err error) {
	return m.MarshalMsg(scratch[:0])
}

// The Multiverse Identitifer: for when there are
// multiple universes and so a UUID just won't do.
type HDR struct {
	Nc net.Conn `msg:"-"`

	Created  string `zid:"0"`
	From     string `zid:"1"`
	To       string `zid:"2"`
	Subject  string `zid:"3"`
	IsRPC    bool   `zid:"4"`
	IsLeg2   bool   `zid:"5"`
	Serial   int64  `zid:"6"`
	CallID   string `zid:"7"` // able to match call and response on this alone.
	PID      int64  `zid:"8"`
	Seqno    uint64 `zid:"9"`
	IsNetRPC bool   `zid:"10"`
}

func NewHDR(from, to, subject string, isRPC bool, isLeg2 bool) (m *HDR) {
	t0 := time.Now()
	created := t0.In(Chicago).Format(RFC3339NanoNumericTZ0pad)
	serial := atomic.AddInt64(&lastSerial, 1)
	// unchecked base58
	rness := toUncheckedBase58(cryptoRandBytes(40))
	m = &HDR{
		Created: created,
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

	return a.Created == b.Created &&
		a.From == b.From &&
		a.To == b.To &&
		a.Subject == b.Subject &&
		a.IsRPC == b.IsRPC &&
		a.IsLeg2 == b.IsLeg2 &&
		a.Serial == b.Serial &&
		a.CallID == b.CallID &&
		a.PID == b.PID &&
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

// workspace can be nil or reused to avoid allocation.
func HDRFromGreenpack(header []byte) (*HDR, error) {
	var mid HDR

	// UnmarshalMsg unmarshals the object
	// from binary, returing any leftover
	// bytes and any errors encountered.
	_, err := mid.UnmarshalMsg(header)
	return &mid, err
}

// the scrach workspace can be nil or reused to avoid allocation.
func (mid *HDR) AsGreenpack(scratch []byte) (o []byte, err error) {

	// MarshalMsg appends the marshalled
	// form of the object to the provided
	// byte slice, returning the extended
	// slice and any errors encountered.

	// We don't use a global scratchspace because we
	// don't want goroutines to collide over it.
	// For memory tuning,
	return mid.MarshalMsg(scratch[:0])
}