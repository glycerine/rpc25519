package rpc25519

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"time"
)

const (
	maxMessage = 20*1024*1024 - 64 // 20 MB max message size, prevents TLS clients from talking to TCP servers, as the random TLS data looks like very big message size.
)

var ErrTooLong = fmt.Errorf("message message too long:  over 2MB; encrypted client vs an un-encrypted server?")

var _ = io.EOF

// uConn hopefully works for both quic.Stream and net.Conn, universally.
type uConn interface {
	io.Writer
	SetWriteDeadline(t time.Time) error

	io.Reader
	SetReadDeadline(t time.Time) error
}

// =========================
//
// message structure
//
// 1. lenBody: next  8 bytes: *body_length*, big endian uint64.
//                The *body_length* says how many bytes are in the body.
//
// 2. body: next length bytes: *body*. The *body* is *body_length* bytes long.
//          These are greenpack encoded (msgpack) bytes.
// =========================

// a work (workspace) lets us re-use memory
// without constantly allocating.
// There should be one for reading, and
// a separate one for writing, so each
// goroutine needs its own so as to not
// colide with any other goroutine.
type workspace struct {
	maxMsgSize int
	buf        []byte

	readLenMessageBytes  []byte
	writeLenMessageBytes []byte

	name string

	// one writer at a time.
	wmut sync.Mutex
}

// currently only used for headers; but bodies may
// well benefit as well. In which case, bump up
// to maxMessage+1024 or so, rather than this 64KB.
func newWorkspace(name string, maxMsgSize int) *workspace {
	return &workspace{
		name:       name,
		maxMsgSize: maxMsgSize,
		// need at least len(msg) + 44; 44 because == msglen(8) + nonceX(24) + overhead(16)
		buf:                  make([]byte, maxMsgSize+64),
		readLenMessageBytes:  make([]byte, 8),
		writeLenMessageBytes: make([]byte, 8),
	}
}

// receiveMessage reads a framed message from conn
// nil or 0 timeout means no timeout.
func (w *workspace) readMessage(conn uConn, timeout *time.Duration) (msg *Message, err error) {
	// Read the first 8 bytes for the Message length
	_, err = readFull(conn, w.readLenMessageBytes, timeout)
	if err != nil {
		return nil, err
	}
	messageLen := binary.BigEndian.Uint64(w.readLenMessageBytes)

	// Read the message based on the messageLen
	if messageLen > maxMessage {
		// probably an encrypted client against an unencrypted server
		return nil, ErrTooLong
	}

	message := make([]byte, messageLen)
	_, err = readFull(conn, message, timeout)
	if err != nil {
		return nil, err
	}

	msg, err = MessageFromGreenpack(message)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

// sendMessage sends a framed message to conn
// nil or 0 timeout means no timeout.
func (w *workspace) sendMessage(conn uConn, msg *Message, timeout *time.Duration) error {
	w.wmut.Lock()
	defer w.wmut.Unlock()

	// serialize message to bytes
	bytesMsg, err := msg.AsGreenpack(w.buf)
	if err != nil {
		return err
	}
	nbytesMsg := len(bytesMsg)

	binary.BigEndian.PutUint64(w.writeLenMessageBytes, uint64(nbytesMsg))

	// Write Message length
	if err := writeFull(conn, w.writeLenMessageBytes, timeout); err != nil {
		return err
	}

	// Write Message
	return writeFull(conn, bytesMsg, timeout)
}

var zeroTime = time.Time{}

// readFull reads exactly len(buf) bytes from conn.
// The returned numRead can be less than
// len(buf) and and then err will be non-nil (on timeout for example).
//
// numRead must be returned, because it may be important
// to account for the partial read
// when numRead < len(buf), so as to discard a partially
// read message and get to the start of the next message.
func readFull(conn uConn, buf []byte, timeout *time.Duration) (numRead int, err error) {

	if timeout != nil && *timeout > 0 {
		conn.SetReadDeadline(time.Now().Add(*timeout))
	} else {
		// do not let previous deadlines contaminate this one.
		conn.SetReadDeadline(zeroTime)
	}

	need := len(buf)

	for numRead < len(buf) {
		n, err := conn.Read(buf[numRead:])
		numRead += n
		if numRead == need {
			// probably just EOF
			//panicOn(erf) goq will panic here.
			return numRead, nil
		}
		if err != nil {
			// can be a timeout, with partial read.
			return numRead, err
		}
	}

	return numRead, nil
	//return io.ReadFull(conn, buf)
}

// writeFull writes all bytes in buf to conn
func writeFull(conn uConn, buf []byte, timeout *time.Duration) error {

	if timeout != nil && *timeout > 0 {
		line := time.Now().Add(*timeout)
		//vv("writeFull setting deadline to '%v'", line)
		conn.SetWriteDeadline(line)
	} else {
		//vv("writeFull has no deadline")
		// do not let previous deadlines contaminate this one.
		conn.SetWriteDeadline(zeroTime)
	}

	need := len(buf)
	total := 0
	for total < len(buf) {
		n, err := conn.Write(buf[total:])
		total += n
		if total == need {
			//panicOn(err) // probably just EOF
			//vv("writeFull returning nil after seeing total == need = %v", total)
			return nil

		}
		if err != nil {
			//vv("writeFull returning err '%v'", err)
			return err
		}
	}
	return nil
}
