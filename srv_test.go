package rpc25519

import (
	"fmt"
	"testing"
	"time"

	cv "github.com/glycerine/goconvey/convey"
)

func Test001_RoundTrip_SendAndGetReply_TCP(t *testing.T) {

	cv.Convey("basic TCP remote procedure call with rpc25519: register a callback on the server, and have the client call it.", t, func() {

		cfg := NewConfig()
		cfg.TCPonly_no_TLS = true

		srv := NewServer(cfg)
		defer srv.Close()

		serverAddr, err := srv.Start()
		panicOn(err)

		cfg.ServerAddr = serverAddr.String()
		vv("server Start() returned serverAddr = '%v'", cfg.ServerAddr)

		srv.RegisterFunc(customEcho)

		cli, err := NewClient("test001", cfg)
		panicOn(err)
		defer cli.Close()

		req := NewMessage()
		req.JobSerz = []byte("Hello from client!")

		reply, err := cli.SendAndGetReply(req, nil)
		panicOn(err)

		vv("server sees reply (Seqno=%v) = '%v'", reply.Seqno, string(reply.JobSerz))

	})
}

func Test002_RoundTrip_SendAndGetReply_TLS(t *testing.T) {

	cv.Convey("basic TLS remote procedure call with rpc25519: register a callback on the server, and have the client call it.", t, func() {

		cfg := NewConfig()
		cfg.TCPonly_no_TLS = false

		srv := NewServer(cfg)
		defer srv.Close()

		serverAddr, err := srv.Start()
		panicOn(err)

		cfg.ServerAddr = serverAddr.String()
		vv("server Start() returned serverAddr = '%v'", cfg.ServerAddr)

		// Commands with odd numbers (1, 3, 5, 7, ...) are for starting an RPC call,
		// requesting an action, initiating a command.
		// The even numbered commands are the replies to those odds.
		// Think of "start counting at 1".
		srv.RegisterFunc(customEcho)

		cli, err := NewClient("test002", cfg)
		panicOn(err)
		defer cli.Close()

		req := NewMessage()
		req.JobSerz = []byte("Hello from client!")

		reply, err := cli.SendAndGetReply(req, nil)
		panicOn(err)

		vv("srv_test sees reply (Seqno=%v) = '%v'", reply.Seqno, string(reply.JobSerz))

		srv.RegisterFunc(oneWayStreet)
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

// echo implements rpc25519.CallbackFunc
func customEcho(in *Message) (out *Message) {
	vv("customEcho called, Seqno=%v, msg='%v'", in.Seqno, string(in.JobSerz))
	//vv("callback to echo: with msg='%#v'", in)
	in.JobSerz = append(in.JobSerz, []byte(fmt.Sprintf("\n with time customEcho sees this: '%v'", time.Now()))...)
	return in // echo
}

var oneWayStreetChan = make(chan bool, 10)

// oneWayStreet does not reply. for testing cli.OneWaySend(); the
// client will not wait for a reply, and we need not send one.
func oneWayStreet(in *Message) (out *Message) {
	vv("oneWayStreet() called. sending on oneWayStreetChan and returning nil. seqno=%v, msg='%v'", in.Seqno, string(in.JobSerz))
	oneWayStreetChan <- true
	return nil
}

func Test003_client_notification_callbacks(t *testing.T) {

	cv.Convey("client.GetReads() and GetOneRead() can be used to monitor all messages or just the next one ", t, func() {

		cfg := NewConfig()
		cfg.TCPonly_no_TLS = true

		srv := NewServer(cfg)
		defer srv.Close()

		serverAddr, err := srv.Start()
		panicOn(err)

		cfg.ServerAddr = serverAddr.String()
		vv("server Start() returned serverAddr = '%v'", cfg.ServerAddr)

		srv.RegisterFunc(customEcho)

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
			vv("server sees reply (Seqno=%v) = '%v'", reply.Seqno, string(reply.JobSerz))
		}

		close(done)
		<-ackDone
	})
}

func Test004_server_push(t *testing.T) {

	cv.Convey("server.SendCh should push messages to the client", t, func() {

		cfg := NewConfig()
		cfg.TCPonly_no_TLS = true

		srv := NewServer(cfg)
		defer srv.Close()

		serverAddr, err := srv.Start()
		panicOn(err)

		cfg.ServerAddr = serverAddr.String()
		vv("server Start() returned serverAddr = '%v'", cfg.ServerAddr)

		//srv.RegisterFunc(5, customEcho)

		cli, err := NewClient("test004", cfg)
		panicOn(err)
		defer cli.Close()

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

		req := NewMessage()
		req.JobSerz = []byte("Hello from server push.")

		// the new stuff under test

		vv("cli.Conn remote key = '%v'", remote(cli.Conn))
		vv("cli.Conn local key = '%v'", local(cli.Conn))
		destAddr := local(cli.Conn)

		for rem := range srv.RemoteConnectedCh {
			if rem == destAddr {
				break // we should not encounter net.Conn not found now.
			}
		}

		callID := "callID_here"
		subject := "subject_here"
		err = srv.SendMessage(callID, subject, destAddr, req.JobSerz, 0)
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

		srv := NewServer(cfg)
		defer srv.Close()

		serverAddr, err := srv.Start()
		panicOn(err)

		cfg.ServerAddr = serverAddr.String()
		vv("server Start() returned serverAddr = '%v'", cfg.ServerAddr)

		// Commands with odd numbers (1, 3, 5, 7, ...) are for starting an RPC call,
		// requesting an action, initiating a command.
		// The even numbered commands are the replies to those odds.
		// Think of "start counting at 1".
		srv.RegisterFunc(customEcho)

		cli, err := NewClient("test002", cfg)
		panicOn(err)
		defer cli.Close()

		req := NewMessage()
		req.JobSerz = []byte("Hello from client!")

		reply, err := cli.SendAndGetReply(req, nil)
		panicOn(err)

		vv("srv_test sees reply (Seqno=%v) = '%v'", reply.Seqno, string(reply.JobSerz))

		srv.RegisterFunc(oneWayStreet)
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
