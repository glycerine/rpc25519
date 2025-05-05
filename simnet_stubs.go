//go:build !goexperiment.synctest

package rpc25519

import (
	"net"
	"time"
)

// stubs to allow building NOT under synctest

type simnet struct{}
type simnode struct{}
type simnetConn struct{}
type SimNetConfig struct{}
type mop struct {
	timerC chan time.Time
}
type clientRegistration struct{}

func (s *simnet) createNewTimer(origin *simnode, dur time.Duration, begin time.Time, isCli bool) (timer *mop) {
	return
}

func (s *simnet) readMessage(conn uConn) (msg *Message, err error) {
	return
}
func (s *simnet) sendMessage(conn uConn, msg *Message, timeout *time.Duration) error {
	return nil
}
func (s *simnet) discardTimer(origin *simnode, origTimerMop *mop, discardTm time.Time) (wasArmed bool) {
	return
}
func (s *simnet) newClientRegistration(c *Client, localHostPort, serverAddr, dialTo string) *clientRegistration {
	return nil
}

func (s *Server) runSimNetServer(serverAddr string, boundCh chan net.Addr, simNetConfig *SimNetConfig) {
}

func (c *Client) runSimNetClient(localHostPort, serverAddr string) {}
