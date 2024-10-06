package edwardsrpc

import (
	"crypto/tls"
	"encoding/binary"
	"io"
	//"net"
	"time"
)

var _ = io.EOF

// =========================
//
// message structure
//
// first 8 bytes: *command*, big endian uint64.
//                *command* == 0 means FIN. All done. No length or message follows.
//                This stream can/should be torn down.
//                The other meanings of *command* are application specific.
//
// next  8 bytes: *length*, big endian uint64.
//                The *length* says how many bytes are in the message.
//
// next length bytes: *message*. The *message* is *length* bytes long.
//                The contents of the *message* should be determined
//                from the *command* and inspection of the *message* bytes.
//
// =========================

// receiveMessage reads a framed message from conn
// nil or 0 timeout means no timeout.
func receiveMessage(conn *tls.Conn, timeout *time.Duration) ([]byte, error) {

	// Read the first 8 bytes as the command.
	cmdBytes := make([]byte, 8)
	if err := readFull(conn, cmdBytes, timeout); err != nil {
		return nil, err
	}
	cmd := binary.BigEndian.Uint64(cmdBytes)

	// zero means no command, no message: time to shutdown. fin.
	if cmd == 0 {
		return nil, io.EOF
	}

	// Read the next 8 bytes for the message length
	lenBytes := make([]byte, 8)
	if err := readFull(conn, lenBytes, timeout); err != nil {
		return nil, err
	}
	msgLen := binary.BigEndian.Uint64(lenBytes)

	// Read the message based on the length
	msg := make([]byte, msgLen)
	if err := readFull(conn, msg, timeout); err != nil {
		return nil, err
	}

	return msg, nil
}

// sendMessage sends a framed message to conn
// nil or 0 timeout means no timeout.
func sendMessage(command int64, conn *tls.Conn, msg []byte, timeout *time.Duration) error {

	cmdBytes := make([]byte, 8)
	if command != 0 {
		binary.BigEndian.PutUint64(cmdBytes, uint64(command))
	}
	// Write command
	if err := writeFull(conn, cmdBytes, timeout); err != nil {
		return err
	}
	if command == 0 {
		return nil // because 0 means, EOF, all done. signing off.
	}
	// Otherwise assume a length word (8 bytes) then the message of that length.

	lenBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(lenBytes, uint64(len(msg)))

	// Write length
	if err := writeFull(conn, lenBytes, timeout); err != nil {
		return err
	}

	// Write message
	return writeFull(conn, msg, timeout)
}

// readFull reads exactly len(buf) bytes from conn
func readFull(conn *tls.Conn, buf []byte, timeout *time.Duration) error {

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
func writeFull(conn *tls.Conn, buf []byte, timeout *time.Duration) error {

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
