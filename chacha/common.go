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
	maxMessage = 2 * 1024 * 1024 * 1024 // 2GB max message size, prevents TLS clients from talking to TCP servers.
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

// receiveMessage reads a framed message from conn
// nil or 0 timeout means no timeout.
// buf provides re-usable space to write the message bytes into.
// We will grow the underlying backing array of buf if need be.
// The returned msg will share the backing array of buf.
func receiveMessage(conn uConn, buf []byte, timeout *time.Duration) (msg []byte, err error) {

	var lenBytes [8]byte
	// Read the first 8 bytes for the Message length
	err = readFull(conn, lenBytes[:], timeout)
	if err != nil {
		return
	}
	messageLen := int(binary.BigEndian.Uint64(lenBytes[:]))

	// Read the message based on the messageLen
	if messageLen > maxMessage {
		// probably an encrypted client against an unencrypted server
		return nil, ErrTooLong
	}

	// only grow buf if we have to
	buf = buf[0:cap(buf)]
	if len(buf) < messageLen {
		buf = append(buf, make([]byte, messageLen-len(buf))...)
	}

	msg = buf[:messageLen]
	err = readFull(conn, msg, timeout)
	return msg, err
}

// sendMessage sends a framed message to conn
// nil or 0 timeout means no timeout.
func sendMessage(conn uConn, bytesMsg []byte, timeout *time.Duration) error {

	nbytesMsg := len(bytesMsg)
	var lenBytes [8]byte
	binary.BigEndian.PutUint64(lenBytes[:], uint64(nbytesMsg))

	// Write Message length
	if err := writeFull(conn, lenBytes[:], timeout); err != nil {
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
