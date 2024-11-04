package rpc25519

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"

	"github.com/quic-go/quic-go"
	//"github.com/quic-go/quic-go/qlog"
)

var _ = fmt.Printf

// Important machine settings in order to use QUIC:
/*
https://github.com/quic-go/quic-go/wiki/UDP-Buffer-Sizes

UDP Buffer Sizes
Marten Seemann edited this page on Apr 23 · 3 revisions
As of quic-go v0.19.x, you might see warnings about the receive and send buffer sizes.

Experiments have shown that QUIC transfers on high-bandwidth connections can be limited by the size of the UDP receive and send buffer. The receive buffer holds packets that have been received by the kernel, but not yet read by the application (quic-go in this case). The send buffer holds packets that have been sent by quic-go, but not sent out by the kernel. In both cases, once these buffers fill up, the kernel will drop any new incoming packet.

Therefore, quic-go tries to increase the buffer size. The way to do this is an OS-specific, and we currently have an implementation for linux, windows and darwin. However, an application is only allowed to do increase the buffer size up to a maximum value set in the kernel. Unfortunately, on Linux this value is rather small, too small for high-bandwidth QUIC transfers.

non-BSD
It is recommended to increase the maximum buffer size by running:

sysctl -w net.core.rmem_max=7500000
sysctl -w net.core.wmem_max=7500000
This command would increase the maximum send and the receive buffer size to roughly 7.5 MB. Note that these settings are not persisted across reboots.

BSD
Taken from: https://medium.com/@CameronSparr/increase-os-udp-buffers-to-improve-performance-51d167bb1360

On BSD/Darwin systems you need to add about a 15% padding to the kernel limit socket buffer. Meaning if you want a 25MB buffer (8388608 bytes) you need to set the kernel limit to 26214400*1.15 = 30146560. This is not documented anywhere but happens in the kernel here.

To update the value immediately to 7.5 MB, type the following commands as root:

sysctl -w kern.ipc.maxsockbuf=8441037
Add the following lines to the /etc/sysctl.conf file to keep this setting across reboots:

kern.ipc.maxsockbuf=8441037
*/

func (s *Server) runQUICServer(quicServerAddr string, tlsConfig *tls.Config, boundCh chan net.Addr) {
	defer func() {
		s.halt.Done.Close()
		//vv("exiting Server.runQUICServer('%v')", quicServerAddr) // seen, yes, on shutdown test.
	}()

	// Server address to connect to
	serverAddr, err := net.ResolveUDPAddr("udp", quicServerAddr)
	if err != nil {
		alwaysPrintf("Failed to resolve server address: %v", err)
		return
	}
	isIPv6 := serverAddr.IP.To4() == nil
	if isIPv6 {
		// don't presume.
		//panic(fmt.Sprintf("quic-go does not work well over IPv6 VPNs, so we reject this server address '%v'. Please use an IPv4 network with quic, or if IPv6 is required then use TCP/TLS over TCP.", serverAddr.String()))
		//alwaysPrintf("warning: quic serverAddr '%v' is IPv6.", serverAddr.String())
	}
	//vv("quicServerAddr '%v' -> '%v'", quicServerAddr, serverAddr)

	var transport *quic.Transport
	s.cfg.shared.mut.Lock()
	if !s.cfg.NoSharePortQUIC && s.cfg.shared.quicTransport != nil {
		transport = s.cfg.shared.quicTransport
		s.cfg.shared.shareCount++
		//vv("s.cfg.shared.shareCount is now %v on server '%v'", s.cfg.shared.shareCount, s.name)
		s.cfg.shared.mut.Unlock()
	} else {
		udpConn, err := net.ListenUDP("udp", serverAddr)
		if err != nil {
			alwaysPrintf("Failed to bind UPD server to '%v': '%v'\n", serverAddr, err)
			s.cfg.shared.mut.Unlock()
			return
		}
		transport = &quic.Transport{
			Conn:               udpConn,
			ConnectionIDLength: 20,
		}
		s.cfg.shared.quicTransport = transport
		s.cfg.shared.shareCount++
		//vv("s.cfg.shared.shareCount is now %v on server '%v'", s.cfg.shared.shareCount, s.name)
		s.cfg.shared.mut.Unlock()
	}
	// note: we do not defer updConn.Close() because it may be shared with other clients/servers.
	// Instead: reference count in cfg.shareCount and call in Close()

	// Create a QUIC listener

	quicConfig := &quic.Config{
		Allow0RTT:         true,
		InitialPacketSize: 1200, // needed to work over Tailscale that defaults to MTU 1280.

		// export QLOGDIR and set this for qlog tracing.
		//Tracer: qlog.DefaultConnectionTracer,

		// quic-go keep alives are unreliable, so do them ourselves
		// in the quic server send loop. See
		// https://github.com/quic-go/quic-go/issues/4710
		//KeepAlivePeriod: 5 * time.Second,
	}

	// "ListenEarly starts listening for incoming QUIC connections.
	//  There can only be a single listener on any net.PacketConn.
	//  Listen may only be called again after the current Listener was closed."
	//  -- https://pkg.go.dev/github.com/quic-go/quic-go#section-readme

	listener, err := transport.ListenEarly(tlsConfig, quicConfig)

	if err != nil {
		log.Fatalf("Error starting QUIC listener: %v", err)
	}
	defer listener.Close()

	addr := listener.Addr()
	//vv("QUIC Server listening on %v://%v", addr.Network(), addr.String())
	if boundCh != nil {
		select {
		case boundCh <- addr:
		case <-time.After(100 * time.Millisecond):
		}
	}

	s.mut.Lock()     // avoid data race
	s.lsn = listener // allow shutdown
	s.quicConfig = quicConfig
	s.mut.Unlock()

	ctx := context.Background()
	for {
		// Accept a QUIC connection
		conn, err := listener.Accept(ctx)
		if err != nil {
			//vv("Error accepting session: %v", err)
			r := err.Error()

			// Error accepting session: quic: server closed
			if strings.Contains(r, "quic: server closed") ||
				strings.Contains(r, "use of closed network connection") {
				return
			}
			continue
		}

		// wait for the handshake to complete so we are encrypted/can verify host keys.
		// see https://quic-go.net/docs/quic/server/
		select {
		case <-conn.HandshakeComplete():
			//vv("quic_server handshake completed")
		case <-conn.Context().Done():
			// connection closed before handshake completion, e.g. due to handshake (auth) failure
			alwaysPrintf("quic_server handshake failure on earlyListener.Accept()")
			continue
		}

		// client key verification.
		knownHostsPath := "known_client_keys"
		connState := conn.ConnectionState().TLS
		raddr := conn.RemoteAddr()
		remoteAddr := strings.TrimSpace(raddr.String())

		if !s.cfg.SkipVerifyKeys {
			// NB only ed25519 keys are permitted, any others will result
			// in an immediate error
			good, bad, wasNew, err := hostKeyVerifies(knownHostsPath, &connState, remoteAddr)
			_ = wasNew
			_ = bad
			if err != nil && len(good) == 0 {
				//fmt.Fprintf(os.Stderr, "key failed list:'%#v': '%v'\n", bad, err)
				return
			}
			if err != nil {
				//vv("hostKeyVerifies returned error '%v' for remote addr '%v'", err, remoteAddr)
				return
			}
			//for i := range good {
			//	vv("accepted identity for client: '%v' (was new: %v)\n", good[i], wasNew)
			//}
		}

		go func(conn quic.Connection) {
			defer func() {
				conn.CloseWithError(0, "")
				//vv("finished with this quic connection.")
			}()

			for {
				// Accept a stream
				stream, err := conn.AcceptStream(context.Background())
				//vv("quic server accepted a stream, err='%v'", err)
				if err != nil {
					if strings.Contains(err.Error(), "timeout: no recent network activity") {
						// Now that we have app-level keep-alives, this
						// really does mean the other end went away.
						// Hopefully we see "Application error 0x0 (remote)"
						// but if the client crashed before it could
						// send it, we may not.
						// vv("quic server read loop finishing quic session/connection: '%v'", err)
						return
					}
					if strings.Contains(err.Error(), "Application error 0x0 (remote)") {
						// normal shutdown.
						//vv("client finished.")
						return
					}
					if err == io.EOF {
						return
					}

					// Error accepting stream: INTERNAL_ERROR (local):
					//  write udp [::]:42677->[fdc8:... 61e]:59299: use of closed network connection
					// quic_server.go:164 quic_server: Error accepting stream: Application error 0x0 (local): server shutdown
					//vv("quic_server: Error accepting stream: %v", err)
					return
				}

				//vv("quic server: s.cfg.encryptPSK = %v", s.cfg.encryptPSK)
				if s.cfg.encryptPSK {
					wrap := &NetConnWrapper{Stream: stream, Connection: conn}

					//s.cfg.randomSymmetricSessKeyFromPreSharedKey, s.cfg.cliEphemPub, s.cfg.srvEphemPub, err =
					//	symmetricServerHandshake(wrap, s.cfg.preSharedKey)

					randomSymmetricSessKey, cliEphemPub, srvEphemPub, err :=
						symmetricServerVerifiedHandshake(wrap, s.cfg.preSharedKey, s.creds)

					if err != nil {
						alwaysPrintf("stream failed to athenticate: '%v'", err)
						return
					}

					// prevent data race
					s.cfg.mut.Lock()
					s.cfg.randomSymmetricSessKeyFromPreSharedKey = randomSymmetricSessKey
					s.cfg.cliEphemPub = cliEphemPub
					s.cfg.srvEphemPub = srvEphemPub
					s.cfg.mut.Unlock()

				}

				// each stream gets its own read/send pair.
				pair := s.newQUIC_RWPair(stream, conn)
				go pair.runSendLoop(stream, conn)
				go pair.runReadLoop(stream, conn)
			}
		}(conn)
	}
}

type quicRWPair struct {
	*rwPair
	stream quic.Stream
}

func (s *Server) newQUIC_RWPair(stream quic.Stream, conn quic.Connection) *quicRWPair {

	wrap := &NetConnWrapper{Stream: stream, Connection: conn}

	//vv("Server new quic pair: local = '%v'", local(wrap))
	//vv("Server new quic pair: remote = '%v'", remote(wrap))

	// No longer duplicate everything that newRWPair() does.
	// It was a burden to keep them in sync. Just point to it.
	return &quicRWPair{
		rwPair: s.newRWPair(wrap),
		stream: stream,
	}
}

func (s *quicRWPair) runSendLoop(stream quic.Stream, conn quic.Connection) {
	defer func() {

		//vv("quick server send loop shutting down for conn.Remote = '%v'", conn.RemoteAddr())
		s.Server.deletePair(s.rwPair)
		s.halt.ReqStop.Close()
		s.halt.Done.Close()
	}()

	symkey := s.Server.cfg.preSharedKey
	if s.cfg.encryptPSK {
		s.cfg.mut.Lock()
		symkey = s.cfg.randomSymmetricSessKeyFromPreSharedKey
		s.cfg.mut.Unlock()
	}

	w := newBlabber("quic server send loop", symkey, stream, s.Server.cfg.encryptPSK, maxMessage, true)

	// implement ServerSendKeepAlive
	var lastPing time.Time
	var doPing bool
	var pingEvery time.Duration
	var pingWakeCh <-chan time.Time

	if s.cfg.ServerSendKeepAlive > 0 {
		// quic-go keepalives are broken at v0.47.0 - v0.48.1 over tailscale;
		// https://github.com/quic-go/quic-go/issues/4710
		// Do them ourselves at application layer.
		doPing = true
		pingEvery = s.cfg.ServerSendKeepAlive
		lastPing = time.Now()
		pingWakeCh = time.After(pingEvery)
	}

	for {
		if doPing {
			now := time.Now()
			if time.Since(lastPing) > pingEvery {
				err := w.sendMessage(stream, keepAliveMsg, &s.cfg.WriteTimeout)
				//vv("quic server sent rpc25519 keep alive. err='%v'", err)
				_ = err
				lastPing = now
				pingWakeCh = time.After(pingEvery)
			} else {
				pingWakeCh = time.After(lastPing.Add(pingEvery).Sub(now))
			}
		}

		select {
		case <-pingWakeCh:
			// check and send above.
			continue
		case msg := <-s.SendCh:
			//vv("quic_server got from s.SendCh, sending msg.HDR = '%v'", msg.HDR.String())
			err := w.sendMessage(stream, msg, &s.cfg.WriteTimeout)
			if err != nil {
				//vv("quic_server sendMessage got err = '%v'; on trying to send Seqno=%v", err, msg.HDR.Subject)
				// notify any short-time-waiting server push user.
				// This is super useful to let goq retry jobs quickly.
				msg.Err = err
				select {
				case msg.DoneCh <- msg:
				default:
				}
				alwaysPrintf("sendMessage got err = '%v'; on trying to send Seqno=%v", err, msg.HDR.Seqno)
				// just let user try again?
			} else {
				// tell caller there was no error.
				select {
				case msg.DoneCh <- msg:
				default:
				}
				lastPing = time.Now() // no need for ping
			}
		case <-s.halt.ReqStop.Chan:
			return
		}
	}
}

func (s *quicRWPair) runReadLoop(stream quic.Stream, conn quic.Connection) {
	defer func() {
		//vv("quic server read loop shutting down for remote conn = '%v'", conn.RemoteAddr())

		s.halt.ReqStop.Close()
		s.halt.Done.Close()

		stream.Close()

		// hmm: can we let other streams continue?
		// Too hard to distinguish a stream vs conn error, for now.
		// We let other clients continue for sure.
		conn.CloseWithError(0, "server shutdown")
	}()

	symkey := s.Server.cfg.preSharedKey
	if s.cfg.encryptPSK {
		s.cfg.mut.Lock()
		symkey = s.cfg.randomSymmetricSessKeyFromPreSharedKey
		s.cfg.mut.Unlock()
	}
	w := newBlabber("quic server read loop", symkey, stream, s.Server.cfg.encryptPSK, maxMessage, true)

	wrap := &NetConnWrapper{Stream: stream, Connection: conn}

	var callme1 OneWayFunc
	var callme2 TwoWayFunc
	foundCallback1 := false
	foundCallback2 := false

	for {

		select {
		case <-s.halt.ReqStop.Chan:
			return
		default:
		}

		req, err := w.readMessage(stream, &s.cfg.ReadTimeout)
		if err == io.EOF {
			//vv("server sees io.EOF from receiveMessage")
			// close of socket before read of full message.
			// shutdown this connection or we'll just
			// spin here at 500% cpu.
			return
		}
		if err != nil {
			//vv("quic server read loop sees err = '%v'", err)
			r := err.Error()
			if strings.Contains(r, "remote error: tls: bad certificate") {
				//vv("ignoring client connection with bad TLS cert.")
				continue
			}
			if strings.Contains(r, "use of closed network connection") {
				return // shutting down
			}

			// "timeout: no recent network activity" should only
			// be seen on disconnection of a client because we
			// have a 5 second heartbeat going.
			if strings.Contains(r, "timeout: no recent network activity") {
				// We should never see this because of our app level keep-alives.
				// If we do, then it means the client really went down.
				//vv("quic server read loop exiting on '%v'", err)
				return
			}
			if strings.Contains(r, "Application error 0x0 (remote)") {
				// normal message from active client who knew they were
				// closing down and politely let us know too. Otherwise
				// we just have to time out.
				return
			}

			alwaysPrintf("ugh. error from remote %v: %v", conn.RemoteAddr(), err)
			//conn.Close()
			//s.halt.Done.Close()
			return
		}

		if req.HDR.IsKeepAlive {
			//vv("quic_server read loop got an rpc25519 keep alive.")
			continue

		}
		//vv("server received message with seqno=%v: %v", seqno, req)

		req.HDR.Nc = wrap

		if req.HDR.IsNetRPC {
			//vv("have IsNetRPC call: '%v'", req.HDR.Subject)
			err = s.callBridgeNetRpc(req)
			if err != nil {
				alwaysPrintf("callBridgeNetRpc errored out: '%v'", err)
			}
			continue
		}

		foundCallback1 = false
		foundCallback2 = false
		callme1 = nil
		callme2 = nil

		s.Server.mut.Lock()
		if req.HDR.IsRPC {
			if s.Server.callme2 != nil {
				callme2 = s.Server.callme2
				foundCallback2 = true
			}
		} else {
			//vv("not rpc, do we have a callme1: '%p'", s.Server.callme1)
			if s.Server.callme1 != nil {
				callme1 = s.Server.callme1
				foundCallback1 = true
			}
		}
		s.Server.mut.Unlock()

		if foundCallback1 {
			// run the callback in a goro, so we can keep doing reads.
			//vv("callme1 about to run req.HDR='%v'", req.HDR)
			go callme1(req)
		}

		if foundCallback2 {
			// run the callback in a goro, so we can keep doing reads.
			go func(req *Message, callme2 TwoWayFunc) {

				//vv("callme2 about to run, req.Nc local = '%v', remote = '%v'", local(req.HDR.Nc), remote(req.HDR.Nc))
				//vv("stream local = '%v', remote = '%v'", local(stream), remote(stream))
				//vv("conn   local = '%v', remote = '%v'", local(conn), remote(conn))

				if cap(req.DoneCh) < 1 || len(req.DoneCh) >= cap(req.DoneCh) {
					panic("req.DoneCh too small; fails the sanity check to be received on.")
				}

				reply := NewMessage()

				replySeqno := req.HDR.Seqno // just echo back same
				// allow user to change Subject
				reply.HDR.Subject = req.HDR.Subject
				reqCallID := req.HDR.CallID

				err := callme2(req, reply)
				if err != nil {
					reply.JobErrs = err.Error()
				}
				// don't read from req now, just in case callme2 messed with it.

				from := local(conn)
				to := remote(conn)
				isRPC := true
				isLeg2 := true

				mid := NewHDR(from, to, reply.HDR.Subject, isRPC, isLeg2)

				// We are able to match call and response rigourously on the CallID alone.
				mid.CallID = reqCallID
				mid.Seqno = replySeqno
				reply.HDR = *mid

				select {
				case s.SendCh <- reply:
					//vv("reply went over pair.SendCh to the send goro write loop")
				case <-s.halt.ReqStop.Chan:
					return
				}
			}(req, callme2)
		}
	}
}
