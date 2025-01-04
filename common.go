package rpc25519

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"time"
)

const (
	maxMessage = 64*1024*1024 - 80 // 64 MB max message size, prevents TLS clients from talking to TCP servers, as the random TLS data looks like very big message size.
)

var ErrTooLong = fmt.Errorf("message message too long: over 64MB; encrypted client vs an un-encrypted server?")

var _ = io.EOF

// magic is always the first 8 bytes of an
// unencrypted message on the wire. Thus
// magic lets us detect when message boundaries
// have been corrupted (also encrypted vs not mismatches).
// It was chosen randomly and should remain
// constant, or else clients and servers will not
// be able to talk to each other.
var magic = [8]byte{0xb3, 0x5d, 0x18, 0x39, 0xac, 0x8e, 0x1d, 0x80}

var ErrMagicWrong = fmt.Errorf("error: magic bytes not found at start of message")

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
// 0. magic: 8 bytes always the first thing. Simple, but
//           statistically has a high probability of detecting
//           partial message cutoff and resumption.
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
	magicCheck           []byte

	name string

	// one writer at a time.
	wmut sync.Mutex

	// Enforce one readMessage() on conn at a time,
	// at least within our workspace.
	// We do this because only
	// the single Read() operation
	// is considered atomic, but
	// a readFull() may do mulitple
	// Read() operations before
	// finishing, and we don't
	// want two goroutines interleaving
	// these. readMessage() certainly
	// does multiple Read()s as well.
	//
	// Update: technically we do not need this because
	// our use readMessage() is isolated to the
	// readLoop() functions.
	//rmut sync.Mutex
}

// currently only used for headers; but bodies may
// well benefit as well. In which case, bump up
// to maxMessage+1024 or so, rather than this 64KB.
//
// Update: no longer true!
// newWorkspace is used by the chacha.go encoder.sendMessage
// for example, as it uses buf for encoding and encrypting
// the full messages. Hence maxMsgSize *must* be
// at least maxMessage + magic(8 bytes) + msglen(8 bytes) +
// noncesize(24 bytes at most) + authTag(16 bytes) overhead
// or larger now. XChaCha has a 24 byte nonce.
// Poly1305 has authTag overhead of 16 bytes.
// Hence at least 56 more bytes beyond maxMessage
// are needed. We currently allocate 80 more bytes.
// This pre-allocation avoids all reallocation
// and memory churn during regular operation.
func newWorkspace(name string, maxMsgSize int) *workspace {
	return &workspace{
		name:       name,
		maxMsgSize: maxMsgSize,
		// need at least len(msg) + 56; 56 because ==
		// magic(8) + msglen(8) + nonceX(24) + overhead(16)
		buf:                  make([]byte, maxMsgSize+80),
		readLenMessageBytes:  make([]byte, 8),
		writeLenMessageBytes: make([]byte, 8),
		magicCheck:           make([]byte, 8),
	}
}

// receiveMessage reads a framed message from conn
// nil or 0 timeout means no timeout.
func (w *workspace) readMessage(conn uConn, timeout *time.Duration) (msg *Message, err error) {

	// our use of w.readMessage is isolated to the readLoop
	// on the server and in the client. They are the only
	// users within their processes. Hence this locking is
	// not needed. If that changes/if ever there are multiple goroutines
	// calling us, we would want to turn on read locking.
	//w.rmut.Lock()
	//defer w.rmut.Unlock()

	// Read the first 8 bytes for the magic check.
	_, err = readFull(conn, w.magicCheck, timeout)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(w.magicCheck, magic[:]) {
		return nil, ErrMagicWrong
	}
	// Read the next 8 bytes for the Message length.
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

	// Write magic
	if err := writeFull(conn, magic[:], timeout); err != nil {
		return err
	}

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
			return numRead, nil
		}
		if err != nil {
			// can be a timeout, with partial read.
			return numRead, err
		}
	}

	return numRead, nil
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
