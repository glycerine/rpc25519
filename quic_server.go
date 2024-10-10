package rpc25519

import (
	"context"
	"crypto/tls"
	//"fmt"
	"log"
	//"os"
	"net"
	"time"

	"github.com/quic-go/quic-go"
	"io"
	"strings"

	"github.com/glycerine/idem"
)

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

func (s *Server) RunQUICServer(serverAddr string, tlsConfig *tls.Config, boundCh chan net.Addr) {
	// Load server certificate and private key

	// Create a QUIC listener
	listener, err := quic.ListenAddr(serverAddr, tlsConfig, nil)
	if err != nil {
		log.Fatalf("Error starting QUIC listener: %v", err)
	}
	defer listener.Close()

	addr := listener.Addr()
	log.Printf("QUIC Server listening on %v:%v", addr.Network(), addr.String())
	if boundCh != nil {
		select {
		case boundCh <- addr:
		case <-time.After(100 * time.Millisecond):
		}
	}

	s.lsn = listener // allow shutdown

	ctx := context.Background()
	for {
		// Accept a QUIC connection
		conn, err := listener.Accept(ctx)
		if err != nil {
			log.Printf("Error accepting session: %v", err)
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
			good, bad, wasNew, err := HostKeyVerifies(knownHostsPath, &connState, remoteAddr)
			_ = wasNew
			_ = bad
			if err != nil && len(good) == 0 {
				//fmt.Fprintf(os.Stderr, "key failed list:'%#v': '%v'\n", bad, err)
				return
			}
			if err != nil {
				vv("HostKeyVerifies returned error '%v' for remote addr '%v'", err, remoteAddr)
				return
			}
			//for i := range good {
			//	vv("accepted identity for client: '%v' (was new: %v)\n", good[i], wasNew)
			//}
		}

		go func(conn quic.Connection) {
			defer func() {
				conn.CloseWithError(0, "")
				vv("finished with this quic connection.")
			}()

			for {
				// Accept a stream
				stream, err := conn.AcceptStream(context.Background())

				if err != nil {
					if strings.Contains(err.Error(), "timeout: no recent network activity") {
						// ignore these, they happen every 30 seconds or so.
						//log.Printf("ignoring timeout")
						//continue
						// or does this means it is time to close up shop?
						vv("timeout, finishing quic session/connection")
						return
					}
					if strings.Contains(err.Error(), "Application error 0x0 (remote)") {
						// normal shutdown.
						log.Printf("client finished.")
						return
					}
					log.Printf("Error accepting stream: %v", err)
					return
				}

				pair := s.NewQUIC_RWPair(stream, conn)
				go pair.runSendLoop(stream, conn)
				go pair.runRecvLoop(stream, conn)
			}
		}(conn)
	}
}

type QUIC_RWPair struct {
	RWPair
	Stream quic.Stream
}

func (s *Server) NewQUIC_RWPair(stream quic.Stream, conn quic.Connection) *QUIC_RWPair {

	wrap := &NetConnWrapper{Stream: stream, Connection: conn}

	p := &QUIC_RWPair{
		RWPair: RWPair{
			cfg:    s.cfg,
			Server: s,
			Conn:   wrap,
			SendCh: make(chan *Message, 10),
			halt:   idem.NewHalter(),
		},
		Stream: stream,
	}
	key := remote(conn)

	s.mut.Lock()
	defer s.mut.Unlock()

	s.remote2pair[key] = &p.RWPair

	select {
	case s.RemoteConnectedCh <- key:
	default:
	}
	return p
}

func (s *QUIC_RWPair) runSendLoop(stream quic.Stream, conn quic.Connection) {
	defer func() {
		s.halt.ReqStop.Close()
		s.halt.Done.Close()
	}()

	w := newWorkspace()

	for {
		select {
		case msg := <-s.SendCh:
			err := w.sendMessage(msg.Seqno, stream, msg, &s.cfg.WriteTimeout)
			if err != nil {
				log.Printf("sendMessage got err = '%v'; on trying to send Seqno=%v", err, msg.Seqno)
			}
		case <-s.halt.ReqStop.Chan:
			return
		}
	}
}

func (s *QUIC_RWPair) runRecvLoop(stream quic.Stream, conn quic.Connection) {
	defer func() {
		//log.Printf("rpc25519.Server: runRecvLoop shutting down for local conn = '%v'", conn.LocalAddr())

		s.halt.ReqStop.Close()
		s.halt.Done.Close()
		conn.CloseWithError(0, "") // just the one, let other clients continue.
	}()

	w := newWorkspace()

	for {

		select {
		case <-s.halt.ReqStop.Chan:
			return
		default:
		}

		seqno, req, err := w.receiveMessage(stream, &s.cfg.ReadTimeout)
		if err == io.EOF {
			//log.Printf("server sees io.EOF from receiveMessage")
			continue // close of socket before read of full message.
		}
		if err != nil {
			r := err.Error()
			if strings.Contains(r, "remote error: tls: bad certificate") {
				vv("ignoring client connection with bad TLS cert.")
				continue
			}
			if strings.Contains(r, "use of closed network connection") {
				return // shutting down
			}

			log.Printf("ugh. error from remote %v: %v", conn.RemoteAddr(), err)
			//conn.Close()
			//s.halt.Done.Close()
			return
		}

		//vv("server received message with seqno=%v: %v", seqno, req)

		s.Server.mut.Lock()
		var callme CallbackFunc
		foundCallback := false
		if s.Server.callme != nil {
			callme = s.Server.callme
			foundCallback = true
		}
		s.Server.mut.Unlock()

		if foundCallback {
			// run the callback in a goto, so we can keep doing reads.
			go func(req *Message, callme CallbackFunc) {
				req.Nc = stream
				req.Seqno = seqno
				if cap(req.DoneCh) < 1 || len(req.DoneCh) >= cap(req.DoneCh) {
					panic("req.DoneCh too small; fails the sanity check to be received on.")
				}

				reply := callme(req)
				// <-req.DoneCh

				if reply != nil && reply.Err != nil {
					log.Printf("note: callback on seqno %v from '%v' got Err='%v", seqno, conn.RemoteAddr(), err)
				}
				// if reply is nil, then we return nothing; it was probably a OneWaySend() target.

				if reply == nil && seqno > 0 {
					//vv("back from Server.callme() callback: nil reply but calling seqno=%v. huh", seqno)
				}
				// Since seqno was >0, we know that
				// a reply is eventually, expected, even though this callme gave none.
				// That's okay, server might just respond to it later with a sendMessage().

				if reply != nil {
					// Seqno: increment by one; so request 3 return response 4.
					reply.Seqno = req.Seqno + 1

					from := local(conn)
					to := remote(conn)
					isRPC := true
					isLeg2 := true
					subject := req.Subject

					mid := NewMID(from, to, subject, isRPC, isLeg2)

					// We are able to match call and response rigourously on the CallID alone.
					mid.CallID = req.MID.CallID
					reply.MID = *mid

					select {
					case s.SendCh <- reply:
						//vv("reply went over pair.SendCh to the send goro write loop")
					case <-s.halt.ReqStop.Chan:
						return
					}
				}
			}(req, callme)
		}
	}
}

/*
func handleStream(stream quic.Stream, conn quic.Connection) {
	defer func() {
		stream.Close()
		vv("done with handleStream.")
	}()

	timeout := time.Minute
	for {

		msg, err := receiveMessage(stream, &timeout)
		if err == io.EOF {
			return // close of socket before read of full message.
		}
		if err != nil && strings.Contains(err.Error(), "remote error: tls: bad certificate") {
			vv("ignoring client connection with bad TLS cert.")
			return
		}
		if err != nil {
			if strings.Contains(err.Error(), "timeout: no recent network activity") {
				vv("timeout, finishing stream")
				return
			}
			if strings.Contains(err.Error(), "Application error 0x0 (remote)") {
				// normal shutdown.
				return
			}
		}
		panicOn(err)

		log.Printf("Received message from %v: '%v'", conn.RemoteAddr(), string(msg))

		// Echo the message back to the client
		//
		// command 0: nothing else, not even a message. We are shutting down, done. close the stream.
		// command 1: one-shot. Just this one message, then we are done.
		// command >=2: leave the stream open, many possible commands with messages arriving.
		if err := sendMessage(1, stream, msg, &timeout); err != nil {
			log.Printf("Error sending message to %v: %v", conn.RemoteAddr(), err)
			return
		}
	}

}
*/
