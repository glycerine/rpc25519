package rpc25519

import (
	//"context"
	//"encoding/base64"
	//"fmt"
	//"os"
	//"path/filepath"
	"strings"
	"testing"
	//"time"

	cv "github.com/glycerine/goconvey/convey"
)

func Test801_RoundTrip_SendAndGetReply_SimNet(t *testing.T) {

	cv.Convey("basic SimNet channel based remote procedure call with rpc25519: register a callback on the server, and have the client call it.", t, func() {

		cfg := NewConfig()
		cfg.TCPonly_no_TLS = true
		cfg.UseSimNet = true

		cfg.ServerAddr = "127.0.0.1:0"
		srv := NewServer("srv_test001", cfg)

		serverAddr, err := srv.Start()
		panicOn(err)
		defer srv.Close()

		vv("server Start() returned serverAddr = '%v'", serverAddr)

		serviceName := "customEcho"
		srv.Register2Func(serviceName, customEcho)

		cfg.ClientDialToHostPort = serverAddr.String()
		cli, err := NewClient("test001", cfg)
		panicOn(err)
		err = cli.Start()
		panicOn(err)

		defer cli.Close()

		req := NewMessage()
		req.HDR.ServiceName = serviceName
		req.JobSerz = []byte("Hello from client!")

		reply, err := cli.SendAndGetReply(req, nil, 0)
		panicOn(err)

		vv("reply = %p", reply)
		vv("server sees reply (Seqno=%v) = '%v'", reply.HDR.Seqno, string(reply.JobSerz))
		want := "Hello from client!"
		gotit := strings.HasPrefix(string(reply.JobSerz), want)
		if !gotit {
			t.Fatalf("expected JobSerz to start with '%v' but got '%v'", want, string(reply.JobSerz))
		}

	})
}
