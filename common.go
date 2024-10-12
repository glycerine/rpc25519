package rpc25519

import (
	//"net"
	//"crypto/tls"
	//"encoding/binary"
	//"fmt"
	"io"
	//"net"
	"time"
)

const (
	maxMessage = 1024 * 1024 // 1MB max message size, prevents TLS clients from talking to TCP servers.
)

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
// 1. seqno: first 8 bytes: *sequenceNumber*, big endian uint64.
//                0 means no response needed/expected
//                odd means initiating; and expect +1 as the response.
//
// 2. lenHeader: next  8 bytes: *header_length*, big endian uint64.
//                The *header_length* says how many bytes are in the header.
//
// 3. header: next header_length bytes: *header*. The *header* is *header_length* bytes long.
//
// 4. lenBody: next  8 bytes: *body_length*, big endian uint64.
//                The *body_length* says how many bytes are in the body.
//
// 5. body: next length bytes: *body*. The *body* is *body_length* bytes long.
//
// =========================

// a work (workspace) lets us re-use memory
// without constantly allocating.
// There should be one for reading, and
// a separate one for writing, so each
// goroutine needs its own so as to not
// colide with any other goroutine.
type workspace struct {
	buf []byte
}

// currently only used for headers; but bodies may
// well benefit as well. In which case, bump up
// to maxMessage+1024 or so, rather than this 64KB.
func newWorkspace() *workspace {
	return &workspace{
		buf: make([]byte, 1<<16),
	}
}

// receiveMessage reads a framed message from conn
// nil or 0 timeout means no timeout.
func (w *workspace) receiveMessage(conn uConn, timeout *time.Duration) (seqno uint64, msg *Message, err error) {

	if timeout != nil && *timeout > 0 {
		conn.SetReadDeadline(time.Now().Add(*timeout))
	}

	msg, err = MessageFromGreenpack(conn)
	if err != nil {
		return 0, nil, err
	}

	return msg.Seqno, msg, nil
}

// sendMessage sends a framed message to conn
// nil or 0 timeout means no timeout.
func (w *workspace) sendMessage(seqno uint64, conn uConn, msg *Message, timeout *time.Duration) error {

	msg.Seqno = seqno
	msg.MID.Seqno = seqno

	if timeout != nil && *timeout > 0 {
		conn.SetWriteDeadline(time.Now().Add(*timeout))
	}

	return msg.WriteAsGreenpack(conn, w.buf[:0])
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
