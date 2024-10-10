package rpc25519

import (
	//"context"
	"crypto/tls"
	//"fmt"
	"log"
	//"os"
	"net"
	"time"

	"github.com/quic-go/quic-go"
	//"io"
	//"strings"
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
	/*
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

	   			// return error on host-key change to detect MITM.
	   			good, bad, wasNew, err := HostKeyVerifies(knownHostsPath, &connState, remoteAddr)
	   			if err != nil && len(good) == 0 && len(bad) > 0 {
	   				fmt.Fprintf(os.Stderr, "key failed list:'%#v': '%v'\n", bad, err)
	   				continue
	   			}
	   			if err != nil {
	   				fmt.Fprintf(os.Stderr, "on checking for known identity, err = '%v' ", err)
	   				continue
	   			}
	   			for i := range good {
	   				vv("accepted identity for client: '%v' (was new: %v)\n", good[i], wasNew)
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

	   					go handleStream(stream, conn)
	   				}
	   			}(conn)
	   		}
	   	}

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
}
