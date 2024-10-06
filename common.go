package rpc25519

import (
	"net"
	//"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	//"net"
	"time"
)

const (
	maxMessage = 1024 * 1024 // 1MB max message size, prevents TLS clients from talking to TCP servers.
)

var _ = io.EOF

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
func (w *workspace) receiveMessage(conn net.Conn, timeout *time.Duration) (seqno uint64, msg *Message, err error) {

	// Read the first 8 bytes as the seqno
	seqnoBytes := make([]byte, 8)
	if err := readFull(conn, seqnoBytes, timeout); err != nil {
		return 0, nil, err
	}
	seqno = binary.BigEndian.Uint64(seqnoBytes)

	// Read the next 8 bytes for the MID header length
	lenHeaderBytes := make([]byte, 8)
	if err := readFull(conn, lenHeaderBytes, timeout); err != nil {
		return seqno, nil, err
	}
	headerLen := binary.BigEndian.Uint64(lenHeaderBytes)

	// Read the MID header based on the headerLen
	if headerLen > maxMessage {
		// probably an encrypted client against an unencrypted server
		return 0, nil, fmt.Errorf("message header too long: %v is over 1MB; encrypted client vs an un-encrypted server?", headerLen)
	}

	header := make([]byte, headerLen)
	if err := readFull(conn, header, timeout); err != nil {
		return seqno, nil, err
	}

	// Ugh. JSON can be so crippled for floating point NaN/Inf. Prefer greenpack.
	//mid, err := MIDFromBytes(header) // json
	mid, err := MIDFromGreenpack(header) // greenpack/msgpack
	if err != nil {
		return seqno, nil, err
	}

	msg = NewMessage()
	msg.MID = *mid
	msg.Seqno = seqno

	// Read the body len
	lenBodyBytes := make([]byte, 8)
	if err := readFull(conn, lenBodyBytes, timeout); err != nil {
		return seqno, nil, err
	}
	bodyLen := binary.BigEndian.Uint64(lenBodyBytes)
	if bodyLen > maxMessage {
		// probably an encrypted client against an unencrypted server
		return 0, nil, fmt.Errorf("message too long: %v is over 1MB; encrypted client vs an un-encrypted server?", bodyLen)
	}

	// Read the body
	body := make([]byte, bodyLen)
	if err := readFull(conn, body, timeout); err != nil {
		return seqno, nil, err
	}
	msg.JobSerz = body

	return seqno, msg, nil
}

// sendMessage sends a framed message to conn
// nil or 0 timeout means no timeout.
func (w *workspace) sendMessage(seqno uint64, conn net.Conn, msg *Message, timeout *time.Duration) error {

	msg.Seqno = seqno

	// write seqno
	seqnoBytes := make([]byte, 8)
	if seqno != 0 {
		binary.BigEndian.PutUint64(seqnoBytes, seqno)
	}
	if err := writeFull(conn, seqnoBytes, timeout); err != nil {
		return err
	}

	// Write MID header length
	//header := msg.MID.Bytes() // json
	header, err := msg.MID.AsGreenpack(w.buf) // greenpack/msgpack
	if err != nil {
		return err
	}
	nheader := len(header)

	nheaderBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(nheaderBytes, uint64(nheader))
	if err := writeFull(conn, nheaderBytes, timeout); err != nil {
		return err
	}

	// Write MID header
	if err := writeFull(conn, header, timeout); err != nil {
		return err
	}

	// Write len of body msg.JobSerz

	bodyLenBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bodyLenBytes, uint64(len(msg.JobSerz)))
	if err := writeFull(conn, bodyLenBytes, timeout); err != nil {
		return err
	}

	// Write body msg.JobSerz
	return writeFull(conn, msg.JobSerz, timeout)
}

// readFull reads exactly len(buf) bytes from conn
func readFull(conn net.Conn, buf []byte, timeout *time.Duration) error {

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
func writeFull(conn net.Conn, buf []byte, timeout *time.Duration) error {

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
