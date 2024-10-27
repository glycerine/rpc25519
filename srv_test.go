package rpc25519

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	cv "github.com/glycerine/goconvey/convey"
)

func Test001_RoundTrip_SendAndGetReply_TCP(t *testing.T) {

	cv.Convey("basic TCP remote procedure call with rpc25519: register a callback on the server, and have the client call it.", t, func() {

		cfg := NewConfig()
		cfg.TCPonly_no_TLS = true

		cfg.ServerAddr = "127.0.0.1:0"
		srv := NewServer("srv_test001", cfg)

		serverAddr, err := srv.Start()
		panicOn(err)
		defer srv.Close()

		vv("server Start() returned serverAddr = '%v'", serverAddr)

		srv.Register2Func(customEcho)

		cfg.ClientDialToHostPort = serverAddr.String()
		cli, err := NewClient("test001", cfg)
		panicOn(err)
		defer cli.Close()

		req := NewMessage()
		req.JobSerz = []byte("Hello from client!")

		reply, err := cli.SendAndGetReply(req, nil)
		panicOn(err)

		vv("server sees reply (Seqno=%v) = '%v'", reply.HDR.Seqno, string(reply.JobSerz))

	})
}

func Test002_RoundTrip_SendAndGetReply_TLS(t *testing.T) {

	cv.Convey("basic TLS remote procedure call with rpc25519: register a callback on the server, and have the client call it.", t, func() {

		cfg := NewConfig()
		cfg.TCPonly_no_TLS = false

		cfg.ServerAddr = "127.0.0.1:0"
		srv := NewServer("srv_test002", cfg)

		serverAddr, err := srv.Start()
		panicOn(err)
		defer srv.Close()

		vv("server Start() returned serverAddr = '%v'", serverAddr)

		// Commands with odd numbers (1, 3, 5, 7, ...) are for starting an RPC call,
		// requesting an action, initiating a command.
		// The even numbered commands are the replies to those odds.
		// Think of "start counting at 1".
		srv.Register2Func(customEcho)

		cfg.ClientDialToHostPort = serverAddr.String()
		cli, err := NewClient("test002", cfg)
		panicOn(err)
		defer cli.Close()

		req := NewMessage()
		req.JobSerz = []byte("Hello from client!")

		reply, err := cli.SendAndGetReply(req, nil)
		panicOn(err)

		vv("srv_test sees reply (Seqno=%v) = '%v'", reply.HDR.Seqno, string(reply.JobSerz))

		srv.Register1Func(oneWayStreet)
		req = NewMessage()
		req.JobSerz = []byte("One-way Hello from client!")

		err = cli.OneWaySend(req, nil)
		panicOn(err)
		<-oneWayStreetChan
		cv.So(true, cv.ShouldEqual, true)
		vv("yay. we confirmed that oneWayStreen func has run")
		// sleep a little to avoid shutting down before server can decide
		// not to process/return a reply.
		time.Sleep(time.Millisecond * 50)
	})
}

// echo implements rpc25519.TwoWayFunc
func customEcho(in *Message, out *Message) error {
	vv("customEcho called, Seqno=%v, msg='%v'", in.HDR.Seqno, string(in.JobSerz))
	//vv("callback to echo: with msg='%#v'", in)
	out.JobSerz = append(in.JobSerz, []byte(fmt.Sprintf("\n with time customEcho sees this: '%v'", time.Now()))...)
	return nil
}

var oneWayStreetChan = make(chan bool, 10)

// oneWayStreet does not reply. for testing cli.OneWaySend(); the
// client will not wait for a reply, and we need not send one.
func oneWayStreet(in *Message) {
	vv("oneWayStreet() called. sending on oneWayStreetChan and returning nil. seqno=%v, msg='%v'", in.HDR.Seqno, string(in.JobSerz))
	oneWayStreetChan <- true
}

func Test003_client_notification_callbacks(t *testing.T) {

	cv.Convey("client.GetReads() and GetOneRead() can be used to monitor all messages or just the next one ", t, func() {

		cfg := NewConfig()
		cfg.TCPonly_no_TLS = true

		cfg.ServerAddr = "127.0.0.1:0"
		srv := NewServer("srv_test003", cfg)

		serverAddr, err := srv.Start()
		panicOn(err)
		defer srv.Close()

		vv("server Start() returned serverAddr = '%v'", serverAddr)

		srv.Register2Func(customEcho)

		cfg.ClientDialToHostPort = serverAddr.String()
		cli, err := NewClient("test003", cfg)
		panicOn(err)
		defer cli.Close()

		req := NewMessage()
		req.JobSerz = []byte("Hello from client!")

		incoming := cli.GetReadIncomingCh()
		done := make(chan bool)
		ackDone := make(chan bool)
		go func() {
			for {
				select {
				case <-done:
					close(ackDone)
					return
				case msg := <-incoming:
					vv("got incoming msg = '%v'", string(msg.JobSerz))
				}
			}
		}()

		for i := 0; i < 3; i++ {
			reply, err := cli.SendAndGetReply(req, nil)
			panicOn(err)
			vv("server sees reply (Seqno=%v) = '%v'", reply.HDR.Seqno, string(reply.JobSerz))
		}

		close(done)
		<-ackDone
	})
}

// see below Test014 for QUIC version
func Test004_server_push(t *testing.T) {

	cv.Convey("server.SendCh should push messages to the client", t, func() {

		cfg := NewConfig()
		cfg.TCPonly_no_TLS = true

		cfg.ServerAddr = "127.0.0.1:0"
		srv := NewServer("srv_test004", cfg)

		serverAddr, err := srv.Start()
		panicOn(err)
		defer srv.Close()

		vv("server Start() returned serverAddr = '%v'", serverAddr)

		//srv.RegisterFunc(5, customEcho)

		cfg.ClientDialToHostPort = serverAddr.String()
		cli, err := NewClient("test004", cfg)
		panicOn(err)
		defer cli.Close()

		incoming := cli.GetReadIncomingCh()
		done := make(chan bool)
		ackDone := make(chan bool)
		seqno := uint64(43)

		go func() {
			for {
				select {
				case <-done:
					close(ackDone)
					return
				case msg := <-incoming:
					vv("got incoming msg = '%v'", string(msg.JobSerz))
					if msg.HDR.Seqno != seqno {
						panic(fmt.Sprintf("expected seqno %v, but got %v", seqno, msg.HDR.Seqno))
					}
				}
			}
		}()

		req := NewMessage()
		req.JobSerz = []byte("Hello from server push.")

		// the new stuff under test

		vv("cli.Conn remote key = '%v'", remote(cli.conn))
		vv("cli.Conn local key = '%v'", local(cli.conn))
		destAddr := local(cli.conn)

		for rem := range srv.RemoteConnectedCh {
			if rem.Remote == destAddr {
				break // we should not encounter net.Conn not found now.
			}
		}

		callID := "callID_here"
		subject := "subject_here"
		err = srv.SendMessage(callID, subject, destAddr, req.JobSerz, seqno)
		panicOn(err) // net.Conn not found

		// does the client get it?

		time.Sleep(time.Millisecond * 50)
		close(done)
		<-ackDone

	})
}

func Test005_RoundTrip_SendAndGetReply_QUIC(t *testing.T) {

	cv.Convey("basic QUIC remote procedure call with rpc25519: register a callback on the server, and have the client call it.", t, func() {

		cfg := NewConfig()
		cfg.UseQUIC = true

		cfg.ServerAddr = "127.0.0.1:0"
		srv := NewServer("srv_test005", cfg)
		serverAddr, err := srv.Start()
		panicOn(err)
		defer srv.Close()

		vv("server Start() returned serverAddr = '%v'", serverAddr.String())

		// Commands with odd numbers (1, 3, 5, 7, ...) are for starting an RPC call,
		// requesting an action, initiating a command.
		// The even numbered commands are the replies to those odds.
		// Think of "start counting at 1".
		srv.Register2Func(customEcho)

		cfg.ClientDialToHostPort = serverAddr.String()
		cli, err := NewClient("test002", cfg)
		panicOn(err)
		defer cli.Close()

		req := NewMessage()
		req.JobSerz = []byte("Hello from client!")

		reply, err := cli.SendAndGetReply(req, nil)
		panicOn(err)

		vv("srv_test sees reply (Seqno=%v) = '%v'", reply.HDR.Seqno, string(reply.JobSerz))

		srv.Register1Func(oneWayStreet)
		req = NewMessage()
		req.JobSerz = []byte("One-way Hello from client!")

		err = cli.OneWaySend(req, nil)
		panicOn(err)
		<-oneWayStreetChan
		cv.So(true, cv.ShouldEqual, true)
		vv("yay. we confirmed that oneWayStreen func has run")
		// sleep a little to avoid shutting down before server can decide
		// not to process/return a reply.
		time.Sleep(time.Millisecond * 50)
	})
}

func setupPSK(path string) error {
	if !fileExists(path) {
		// Define a shared secret key (32 bytes for AES-256-GCM)
		key := NewChaCha20CryptoRandKey()
		odir := filepath.Dir(path)
		os.MkdirAll(odir, 0700)
		ownerOnly(odir)
		fd, err := os.Create(path)
		panicOn(err)
		_, err = fd.Write(key)
		panicOn(err)
		fd.Close()
		ownerOnly(path)
	} else {
		vv("using existing psk file '%v'", path)
	}
	return nil
}

func Test011_PreSharedKey_over_TCP(t *testing.T) {

	cv.Convey("If we enable pre-shared-key encryption, round trips should still work", t, func() {

		cfg := NewConfig()
		cfg.UseQUIC = true
		//cfg.TCPonly_no_TLS = true

		path := "my-keep-private-dir/psk.binary"
		panicOn(setupPSK(path))
		cfg.PreSharedKeyPath = path

		cfg.ServerAddr = "127.0.0.1:0"
		srv := NewServer("srv_test011", cfg)

		serverAddr, err := srv.Start()
		panicOn(err)
		defer srv.Close()

		vv("server Start() returned serverAddr = '%v'", serverAddr)

		srv.Register2Func(customEcho)

		cfg.ClientDialToHostPort = serverAddr.String()
		cli, err := NewClient("test011", cfg)
		panicOn(err)
		defer cli.Close()

		req := NewMessage()
		req.JobSerz = []byte("Hello from client!")

		reply, err := cli.SendAndGetReply(req, nil)
		panicOn(err)

		vv("server sees reply (Seqno=%v) = '%v'", reply.HDR.Seqno, string(reply.JobSerz))

	})
}

func Test012_PreSharedKey_must_agree(t *testing.T) {

	cv.Convey("If the pre-shared-keys disagree, we should not communicate", t, func() {

		ccfg := NewConfig()
		ccfg.UseQUIC = true
		//cfg.TCPonly_no_TLS = true

		scfg := NewConfig()
		scfg.UseQUIC = true

		path := "my-keep-private-dir/client_psk.binary"
		panicOn(setupPSK(path))
		ccfg.PreSharedKeyPath = path

		path2 := "my-keep-private-dir/server_psk2.binary"
		panicOn(setupPSK(path2))
		scfg.PreSharedKeyPath = path2

		scfg.ServerAddr = "127.0.0.1:0"
		srv := NewServer("srv_test011", scfg)

		serverAddr, err := srv.Start()
		panicOn(err)
		defer srv.Close()

		vv("server Start() returned serverAddr = '%v'", serverAddr)

		srv.Register2Func(customEcho)

		ccfg.ClientDialToHostPort = serverAddr.String()
		cli, err := NewClient("test011", ccfg)
		panicOn(err)
		defer cli.Close()

		req := NewMessage()
		req.JobSerz = []byte("Hello from client!")

		reply, err := cli.SendAndGetReply(req, nil)
		_ = reply
		// expect an error here
		if err == nil {
			panic("expected error from bad handshake!")
		} else {
			vv("good, got err = '%v'", err)
		}

		//vv("server sees reply (Seqno=%v) = '%v'", reply.HDR.Seqno, string(reply.JobSerz))

	})
}

func Test014_server_push_quic(t *testing.T) {

	cv.Convey("server.SendCh should push messages to the client under QUIC", t, func() {

		cfg := NewConfig()
		cfg.UseQUIC = true

		cfg.ServerAddr = "127.0.0.1:0"
		srv := NewServer("srv_test014", cfg)

		serverAddr, err := srv.Start()
		panicOn(err)
		defer srv.Close()

		vv("server Start() returned serverAddr = '%v'", serverAddr)

		//srv.RegisterFunc(5, customEcho)

		cfg.ClientDialToHostPort = serverAddr.String()
		cli, err := NewClient("test014", cfg)
		panicOn(err)
		defer cli.Close()

		incoming := cli.GetReadIncomingCh()
		done := make(chan bool)
		ackDone := make(chan bool)
		seqno := uint64(43)

		go func() {
			for {
				select {
				case <-done:
					close(ackDone)
					return
				case msg := <-incoming:
					vv("got incoming msg = '%v'", string(msg.JobSerz))
					if msg.HDR.Seqno != seqno {
						panic(fmt.Sprintf("expected seqno %v, but got %v", seqno, msg.HDR.Seqno))
					}
				}
			}
		}()

		req := NewMessage()
		req.JobSerz = []byte("Hello from server push.")

		// the new stuff under test

		vv("cli.Conn remote key = '%v'", remote(cli.quicConn))
		vv("cli.Conn local key = '%v'", local(cli.quicConn))
		destAddr := local(cli.quicConn)

		// client has to initiate to get a stream, otherwise
		// server will never know about them.
		clireq := NewMessage()
		clireq.HDR.Subject = "one way hello"
		clireq.JobSerz = []byte("one way Hello from client!")
		err = cli.OneWaySend(clireq, nil)
		panicOn(err)

		for rem := range srv.RemoteConnectedCh {
			if rem.Remote == destAddr {
				break // we should not encounter net.Conn not found now.
			}
			panic(fmt.Sprintf("who else is connecting to us??: '%v'", rem))
		}
		vv("srv is connected to client")

		callID := "callID_here"
		subject := "subject_here"
		err = srv.SendMessage(callID, subject, destAddr, req.JobSerz, seqno)
		panicOn(err) // net.Conn not found

		// does the client get it?

		time.Sleep(time.Millisecond * 50)
		close(done)
		<-ackDone

	})
}

func Test015_server_push_quic_notice_disco_quickly(t *testing.T) {

	cv.Convey("server.SendCh should push messages to the client under QUIC, and notice quickly if client has already disconnected.", t, func() {

		cfg := NewConfig()
		cfg.UseQUIC = true

		cfg.ServerAddr = "127.0.0.1:0"
		srv := NewServer("srv_test015", cfg)

		serverAddr, err := srv.Start()
		panicOn(err)
		defer srv.Close()

		vv("server Start() returned serverAddr = '%v'", serverAddr)

		//srv.RegisterFunc(5, customEcho)

		// NOTE: for QUIC and this test, we might not want
		// port sharing between the client and the server,
		// since we want to shut down the client and have
		// the server notice. Its hard to shut down the
		// client if the same udp listener stays up to
		// support the server, no?

		ccfg := NewConfig()
		ccfg.UseQUIC = true
		ccfg.ClientDialToHostPort = serverAddr.String()
		ccfg.NoSharePortQUIC = true

		cli, err := NewClient("test015", ccfg)
		panicOn(err)
		// we will manually Close below
		//defer cli.Close()

		if cli.cfg.shared.shareCount > 1 {
			panic("must set up this test with independent " +
				"client and server to be able to simulate the " +
				"client process vanishing on the other end")
		}

		incoming := cli.GetReadIncomingCh()
		done := make(chan bool)
		ackDone := make(chan bool)
		seqno := uint64(43)

		go func() {
			for {
				select {
				case <-done:
					close(ackDone)
					return
				case msg := <-incoming:
					vv("got incoming msg = '%v'", string(msg.JobSerz))
					if msg.HDR.Seqno != seqno {
						panic(fmt.Sprintf("expected seqno %v, but got %v", seqno, msg.HDR.Seqno))
					}
				}
			}
		}()

		// the new stuff under test

		vv("cli.Conn remote key = '%v'", remote(cli.quicConn))
		vv("cli.Conn local key = '%v'", local(cli.quicConn))
		destAddr := local(cli.quicConn)

		// client has to initiate to get a stream, otherwise
		// server will never know about them.
		clireq := NewMessage()
		clireq.HDR.Subject = "one way hello"
		clireq.JobSerz = []byte("one way Hello from client!")
		err = cli.OneWaySend(clireq, nil)
		panicOn(err)

		var rem *ServerClient
		for rem = range srv.RemoteConnectedCh {
			if rem.Remote == destAddr {
				break // we should not encounter net.Conn not found now.
			}
			panic(fmt.Sprintf("who else is connecting to us??: '%v'", rem))
		}
		vv("srv is connected to client")

		vv("shutting down client before server can send to us")
		cli.Close()

		// wait for server to notice the client's disconnect.
		<-rem.GoneCh
		vv("server has seen that rem '%v' is gone", rem.Remote)

		//select {}
		//time.Sleep(time.Millisecond * 1000)

		// good here; we see
		// quic_server.go:376 2024-10-26 17:00:48.716 -0500 CDT ugh. error from remote 127.0.0.1:64891: Application error 0x0 (remote)

		vv("now try having server push to disco client")

		req := NewMessage()
		req.JobSerz = []byte("Hello from server push.")

		callID := "server_push_callID_here"
		subject := "server push"
		err = srv.SendMessage(callID, subject, destAddr, req.JobSerz, seqno)

		// do we get an error since client is not there?
		if err == nil {
			panic("expected error from send message since client is gone")
		}
		if err == ErrNetConnectionNotFound {
			vv("good: got error back on SendMessage to shutdown client, like we want: '%v'", err)
			err = nil
		}
		panicOn(err) // unexpected error type!

		close(done)
		<-ackDone

	})
}
