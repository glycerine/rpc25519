package rpc25519

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
		err = cli.Start()
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
		err = cli.Start()
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
		err = cli.Start()
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
		err = cli.Start()
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

		pushMsg := NewMessage()
		pushMsg.JobSerz = req.JobSerz
		pushMsg.HDR.Typ = CallOneWay // or CallStreamBegin if starting a stream.
		pushMsg.HDR.Subject = subject
		pushMsg.HDR.To = destAddr
		pushMsg.HDR.Seqno = seqno
		pushMsg.HDR.CallID = callID

		err = srv.SendOneWayMessage(context.Background(), pushMsg, nil)
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
		err = cli.Start()
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
		err = cli.Start()
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
		err = cli.Start()
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
		err = cli.Start()
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

		// In QUIC, client has to initiate to get a stream, otherwise
		// server will never know about them/the stream.
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

		pushMsg := NewMessage()
		pushMsg.JobSerz = req.JobSerz
		pushMsg.HDR.Typ = CallOneWay // or CallStreamBegin if starting a stream.
		pushMsg.HDR.Subject = subject
		pushMsg.HDR.To = destAddr
		pushMsg.HDR.Seqno = seqno
		pushMsg.HDR.CallID = callID

		err = srv.SendOneWayMessage(context.Background(), pushMsg, nil)
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
		err = cli.Start()
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

		vv("now try having server push to disco client")

		req := NewMessage()
		req.JobSerz = []byte("Hello from server push.")

		callID := "server_push_callID_here"
		subject := "server push"

		pushMsg := NewMessage()
		pushMsg.JobSerz = req.JobSerz
		pushMsg.HDR.Typ = CallOneWay // or CallStreamBegin if starting a stream.
		pushMsg.HDR.Subject = subject
		pushMsg.HDR.To = destAddr
		pushMsg.HDR.Seqno = seqno
		pushMsg.HDR.CallID = callID

		err = srv.SendOneWayMessage(context.Background(), pushMsg, nil)

		// do we get an error since client is not there?
		if err == nil {
			panic("expected error from send message since client is gone")
		}
		if err == ErrNetConnectionNotFound {
			vv("good: got error back on SendMessage to shutdown client, like we want: '%v'", err)
			err = nil
		} else if strings.Contains(err.Error(), "Application error 0x0 (remote)") {
			// good. This is Server.SendMessage() bubbling up the error from
			// the quic client actively closing on us. Exactly what we want.
			vv("good! got expected error '%v'", err)
		} else {
			panicOn(err) // unexpected error type!
		}

		close(done)
		<-ackDone

	})
}

func Test016_WithPreSharedKey_inner_handshake_must_be_properly_signed(t *testing.T) {

	if !useVerifiedHandshake {
		return // currently pre-shared-key is super simple and not verified.
	}

	cv.Convey("Even if the pre-shared-keys agree, if the initial handshake ephemeral keys are not signed by a key that is signed by our CA, then we should error out on authentication failure.", t, func() {

		testServer := false

		for i := 0; i < 2; i++ {
			if i == 1 {
				testServer = true
			}

			// Don't copy over top level certs/ca.crt,
			// as that would mess up other tests/manual testing.
			// Instead, setup two entirely new CA:

			// set up 1st test CA
			ca1path := fmt.Sprintf("temp-1st-ca-for-testing-%v", i)
			panicOn(SelfyNewKey("node1", ca1path))
			panicOn(SelfyNewKey("client", ca1path))
			defer os.RemoveAll(ca1path)

			// set up 2nd, different CA
			ca2path := fmt.Sprintf("temp-2nd-ca-for-testing-%v", i)
			panicOn(SelfyNewKey("node2", ca2path))
			panicOn(SelfyNewKey("client", ca2path))
			defer os.RemoveAll(ca2path)

			ccfg := NewConfig()
			ccfg.UseQUIC = true
			//cfg.TCPonly_no_TLS = true
			ccfg.CertPath = ca2path + "/certs"
			ccfg.ClientKeyPairName = "client"

			scfg := NewConfig()
			scfg.UseQUIC = true
			scfg.ServerKeyPairName = "node1"
			scfg.CertPath = ca1path + "/certs"

			// let the client initially handshake so we can test the inner tunnel
			// symmetric.go rejection path, rather than the outer quic/tls handshake.
			scfg.SkipVerifyKeys = true
			ccfg.SkipVerifyKeys = true

			// even if same psk
			path := ca2path + "/test_client_psk.binary"
			panicOn(setupPSK(path))
			ccfg.PreSharedKeyPath = path
			scfg.PreSharedKeyPath = path

			// even if the CA is the same (but node2 from a different CA).
			if testServer {
				// should cause failure on the server
				copyFileDestSrc(ca2path+"/certs/ca.crt", ca1path+"/certs/ca.crt")
			} else {
				// and the reverse, should cause failure on the client
				copyFileDestSrc(ca1path+"/certs/ca.crt", ca2path+"/certs/ca.crt")
			}

			scfg.ServerAddr = "127.0.0.1:0"
			srv := NewServer("srv_test016", scfg)

			serverAddr, err := srv.Start()
			panicOn(err)
			defer srv.Close()

			vv("server Start() returned serverAddr = '%v'", serverAddr)

			srv.Register2Func(customEcho)

			ccfg.ClientDialToHostPort = serverAddr.String()
			cli, err := NewClient("test016", ccfg)
			panicOn(err)
			err = cli.Start()
			panicOn(err)

			defer cli.Close()

			// we want and expect to get here, because only on opening
			// a new quic stream does the inner symmetric crypto get
			// applied. We must create a new quic stream to test the inside crypto.

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
		}
	})
}

func Test030_RoundTrip_SendAndGetReply_then_JSON(t *testing.T) {

	cv.Convey("convert to JSON after basic TCP, see what we have.", t, func() {

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
		err = cli.Start()
		panicOn(err)

		defer cli.Close()

		req := NewMessage()
		req.JobSerz = []byte("Hello from client!")

		reply, err := cli.SendAndGetReply(req, nil)
		panicOn(err)

		j, err := reply.AsJSON(nil)
		_ = j
		panicOn(err)

		//vv("server sees reply '%v' ==> json:\n%v\n", reply, string(j))
		/*
			srv_test.go:767 2024-11-23 06:17:28.084 -0600 CST server sees reply '&Message{HDR:{
			    "Nc": null,
			    "Created": "2024-11-23T12:17:28.084799268Z",
			    "From": "tcp://127.0.0.1:41791",
			    "To": "tcp://127.0.0.1:56892",
			    "Subject": "",
			    "Seqno": 2,
			    "Typ": 1,
			    "CallID": "bmcmr1iak8Jb6ZGiFD1Ctm7u3CU=",
			    "Serial": 2,
			    "LocalRecvTm": "2024-11-23T12:17:28.084885833Z"
			}, LocalErr:'<nil>'}' ==> json:
			{"HDR_zid00_rct":{"Created_zid00_tim":"2024-11-23T12:17:28.084799268Z","From_zid01_str":"tcp://127.0.0.1:41791","To_zid02_str":"tcp://127.0.0.1:56892","Seqno_zid04_u64":2,"Typ_zid05_rct":1,"CallID_zid06_str":"bmcmr1iak8Jb6ZGiFD1Ctm7u3CU=","Serial_zid07_i64":2,"LocalRecvTm_zid08_tim":"2024-11-23T12:17:28.084885833Z"},"JobSerz_zid01_bin":"SGVsbG8gZnJvbSBjbGllbnQhCiB3aXRoIHRpbWUgY3VzdG9tRWNobyBzZWVzIHRoaXM6ICcyMDI0LTExLTIzIDEyOjE3OjI4LjA4NDY1NzcyICswMDAwIFVUQyBtPSswLjAwNzMyNTI5Myc="}
		*/
		// manually extracting the JobSerz_zid01_bin field from the example above:
		jobSerz := "SGVsbG8gZnJvbSBjbGllbnQhCiB3aXRoIHRpbWUgY3VzdG9tRWNobyBzZWVzIHRoaXM6ICcyMDI0LTExLTIzIDEyOjE3OjI4LjA4NDY1NzcyICswMDAwIFVUQyBtPSswLjAwNzMyNTI5Myc="
		// converting it to a string by base64 decoding.
		dst := make([]byte, base64.StdEncoding.DecodedLen(len(jobSerz)))
		n, err := base64.StdEncoding.Decode(dst, []byte(jobSerz))
		panicOn(err)
		dst = dst[:n]
		vv("jobSerz -> dst = '%v'", string(dst))
		// jobSerz -> dst = 'Hello from client!
		// with time customEcho sees this: '2024-11-23 12:12:21.045869908 +0000 UTC m=+0.007191979''

	})
}
