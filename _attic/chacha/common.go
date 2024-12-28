package main

import (
	//"net"
	//"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	//"net"
	"time"
)

const (
	maxMessage = 2*1024*1024*1024 - 64 // 2GB max message size, prevents TLS clients from talking to TCP servers.
)

var ErrTooLong = fmt.Errorf("message message too long:  over 2GB; encrypted client vs an un-encrypted server?")

var _ = io.EOF

// uConn hopefully works for both quic.Stream and net.Conn, universally.
type uConn interface {
	io.Writer
	SetWriteDeadline(t time.Time) error

	io.Reader
	SetReadDeadline(t time.Time) error
}

var ErrNotEnoughSpace = fmt.Errorf("not enough space in buf to write message")

// a work (workspace) lets us re-use memory
// without constantly allocating.
// There should be one for reading, and
// a separate one for writing, so each
// goroutine needs its own so as to not
// colide with any other goroutine.
type workspace struct {
	buf []byte

	readLenMessageBytes  []byte
	writeLenMessageBytes []byte
}

// currently only used for headers; but bodies may
// well benefit as well. In which case, bump up
// to maxMessage+1024 or so, rather than this 64KB.
func newWorkspace(maxMsgSize int) *workspace {
	return &workspace{
		// need at least len(msg) + 44; 44 because == msglen(8) + nonceX(24) + overhead(16)
		buf:                  make([]byte, maxMsgSize+64),
		readLenMessageBytes:  make([]byte, 8),
		writeLenMessageBytes: make([]byte, 8),
	}
}

// receiveMessage reads a framed message from conn
// nil or 0 timeout means no timeout.
// buf provides re-usable space to write the message bytes into.
// We will grow the underlying backing array of buf if need be.
// The returned msg will share the backing array of buf.
func (w *workspace) readMessage(conn uConn, timeout *time.Duration) (msg []byte, err error) {

	// Read the first 8 bytes for the Message length
	err = readFull(conn, w.readLenMessageBytes, timeout)
	if err != nil {
		return
	}
	messageLen := int(binary.BigEndian.Uint64(w.readLenMessageBytes))

	// Read the message based on the messageLen
	if messageLen > maxMessage {
		// probably an encrypted client against an unencrypted server
		return nil, ErrTooLong
	}

	// only grow buf if we have to

	w.buf = w.buf[0:cap(w.buf)]
	if len(w.buf) < messageLen {
		w.buf = append(w.buf, make([]byte, messageLen-len(w.buf))...)
	}

	msg = w.buf[:messageLen]
	err = readFull(conn, msg, timeout)
	return msg, err
}

// sendMessage sends a framed message to conn
// nil or 0 timeout means no timeout.
func (w *workspace) sendMessage(conn uConn, bytesMsg []byte, timeout *time.Duration) error {

	nbytesMsg := len(bytesMsg)
	binary.BigEndian.PutUint64(w.writeLenMessageBytes, uint64(nbytesMsg))

	// Write Message length
	if err := writeFull(conn, w.writeLenMessageBytes, timeout); err != nil {
		return err
	}

	// Write Message
	return writeFull(conn, bytesMsg, timeout)
}

// readFull reads exactly len(buf) bytes from conn
func readFull(conn uConn, buf []byte, timeout *time.Duration) error {

	if timeout != nil && *timeout > 0 {
		conn.SetReadDeadline(time.Now().Add(*timeout))
	}

	need := len(buf)
	total := 0
	for total < len(buf) {
		n, err := conn.Read(buf[total:])
		total += n
		if total == need {
			// probably just EOF
			return nil
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// writeFull writes all bytes in buf to conn
func writeFull(conn uConn, buf []byte, timeout *time.Duration) error {

	if timeout != nil && *timeout > 0 {
		conn.SetWriteDeadline(time.Now().Add(*timeout))
	}

	need := len(buf)
	total := 0
	for total < len(buf) {
		n, err := conn.Write(buf[total:])
		total += n
		if total == need {
			return nil
		}
		if err != nil {
			return err
		}
	}
	return nil
}
