package rpc25519

// version of golang.org/x/net/netutil#LimitListener
// that supports tls.Conn

// This file derived from golang.org/x/net/netutil/listen.go
// which uses the same license as our LICENSE, so the license
// reporting obligation is met:
// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package netutil provides network utility functions, complementing the more
// common ones in the net package.

import (
	"crypto/tls"
	"net"
	"sync"
)

// newTlsLimitListener returns a Listener that accepts at most n simultaneous
// connections from the provided Listener. Its returned net.Conn embed *tls.Conn.
func newTlsLimitListener(l net.Listener, n int) net.Listener {
	return &tlsLimitListener{
		Listener: l,
		sem:      make(chan struct{}, n),
		done:     make(chan struct{}),
	}
}

type tlsLimitListener struct {
	net.Listener
	sem       chan struct{}
	closeOnce sync.Once     // ensures the done chan is only closed once
	done      chan struct{} // no values sent; closed when Close is called
}

// acquire acquires the limiting semaphore. Returns true if successfully
// acquired, false if the listener is closed and the semaphore is not
// acquired.
func (l *tlsLimitListener) acquire() bool {
	select {
	case <-l.done:
		return false
	case l.sem <- struct{}{}:
		return true
	}
}
func (l *tlsLimitListener) release() { <-l.sem }

func (l *tlsLimitListener) Accept() (net.Conn, error) {
	if !l.acquire() {
		// If the semaphore isn't acquired because the listener was closed, expect
		// that this call to accept won't block, but immediately return an error.
		// If it instead returns a spurious connection (due to a bug in the
		// Listener, such as https://golang.org/issue/50216), we immediately close
		// it and try again. Some buggy Listener implementations (like the one in
		// the aforementioned issue) seem to assume that Accept will be called to
		// completion, and may otherwise fail to clean up the client end of pending
		// connections.
		for {
			c, err := l.Listener.Accept()
			if err != nil {
				return nil, err
			}
			c.Close()
		}
	}

	c, err := l.Listener.Accept()
	if err != nil {
		l.release()
		return nil, err
	}
	return &tlsLimitListenerConn{Conn: c.(*tls.Conn), release: l.release}, nil
}

func (l *tlsLimitListener) Close() error {
	err := l.Listener.Close()
	l.closeOnce.Do(func() { close(l.done) })
	return err
}

type tlsLimitListenerConn struct {
	*tls.Conn
	releaseOnce sync.Once
	release     func()
}

func (l *tlsLimitListenerConn) Close() error {
	err := l.Conn.Close()
	l.releaseOnce.Do(l.release)
	return err
}
