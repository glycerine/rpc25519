package rpc25519

import (
	//"bytes"
	//"encoding/binary"
	//"errors"
	//"fmt"
	"io"
	//"os"
	//"strings"
	//"sync"
	//"sync/atomic"
	"time"
)

var _ = io.EOF

// simtime implements the same workspace/blabber interface
// so we can plug in
// netsim and do comms via channels for testing/synctest
// based accelerated timeout testing.
//
// Note that uConn and its Write/Read are
// not actually used; channel sends/reads replace them.
// We still need a dummy uConn to pass to
// readMessage() and sendMessage() which are the
// interception points for the simulated network.
type simtime struct {
	netsim *netsim
}

// not actually used though
func (s *simtime) Write(p []byte) (n int, err error)   { return }
func (s *simtime) SetWriteDeadline(t time.Time) error  { return nil }
func (s *simtime) Read(data []byte) (n int, err error) { return }
func (s *simtime) SetReadDeadline(t time.Time) error   { return nil }

// receiveMessage reads a framed message from conn.
func (w *simtime) readMessage(conn uConn) (msg *Message, err error) {
	return
}

func (w *simtime) sendMessage(conn uConn, msg *Message, timeout *time.Duration) error {
	return nil
}
