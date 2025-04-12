package jsync

import (
	//"bytes"
	//cryrand "crypto/rand"
	"fmt"
	//"path/filepath"
	//"io"
	"os"
	"testing"
	//"time"

	cv "github.com/glycerine/goconvey/convey"
	rpc "github.com/glycerine/rpc25519"
)

func Test1234_empty_files_sync(t *testing.T) {

	cv.Convey("a dir with empty files should still sync those files", t, func() {

		// set up two isolated test dirs, so we can
		// be sure exactly what is changing by inspection.

		cwd, err := os.Getwd()
		panicOn(err)
		_ = cwd

		localDir := cwd + sep + "local_cli_dir_test1234/"
		remoteDir := cwd + sep + "remote_srv_dir_test1234/"
		// remove any old leftover test files first.
		os.RemoveAll(localDir)
		os.RemoveAll(remoteDir)
		os.MkdirAll(localDir, 0755)
		os.MkdirAll(remoteDir, 0755)

		// create empty test file
		localBase := fmt.Sprintf("test1234_empty.dat")
		localPath := localDir + localBase
		vv("localPath = '%v'", localPath)

		// modify "local" target path so we don't overwrite our
		// source file when testing in one directory. Also to
		// check that we can.
		remoteBase := localBase + ".remote_rsync_out"
		remotePath := remoteDir + sep + remoteBase
		vv("remotePath = '%v'", remotePath)

		testfd, err := os.Create(localPath)
		panicOn(err)
		testfd.Close()
		vv("created empty test file in remotePath='%v'.", remotePath)

		// set up a server and a client.

		cfg := rpc.NewConfig()
		//cfg.TCPonly_no_TLS = true
		//cfg.CompressionOff = true

		cfg.ServerAddr = "127.0.0.1:0"
		srv := rpc.NewServer("srv_rsync_test1234", cfg)

		// ==== configure server's remote dir first, start server.
		os.Setenv("RPC25519_SERVER_DATA_DIR", remoteDir)
		// try not to impact other tests:
		defer os.Unsetenv("RPC25519_SERVER_DATA_DIR")

		// for this to work: the srv must note its home dir when
		// starting, just once, and keep referencing that, and
		// not the cwd, which we will change below for the cli
		// to also note its start dir.
		serverAddr, err := srv.Start()
		panicOn(err)
		defer srv.Close()

		srvDataDir := srv.DataDir()

		vv("server Start() returned serverAddr = '%v'; wd = '%v'; srv.DataDir() = '%v'",
			serverAddr, srv.ServerStartingDir(), srvDataDir)

		// JsyncServer and JsyncClient are re-prefixed to
		// tell them apart from "actual" rsync. Jsync is ours.
		jSyncSrv, err := NewJsyncServer(srv)
		panicOn(err)
		defer jSyncSrv.Close()

		// Note: avoid changing directories. It messes up other tests.

		cfg.ClientDialToHostPort = serverAddr.String()
		cli, err := rpc.NewClient("cli_rsync_test1234", cfg)
		panicOn(err)
		err = cli.Start()
		panicOn(err)

		vv("client Start() coming from cli.LocalAddr = '%v'; wd = '%v'",
			cli.LocalAddr(), cli.ClientStartingDir())

		defer cli.Close()

		// ============ actual test now:

		jSyncCli, err := NewJsyncClient(cli)
		panicOn(err)
		defer jSyncCli.Close()

		dataBytesMoved0, err := jSyncCli.PushFromTo(localPath, remotePath)
		panicOn(err)
		_ = dataBytesMoved0
		cv.So(dataBytesMoved0, cv.ShouldBeLessThan, 1000)

		// confirm it happened.
		difflen := compareFilesDiffLen(localPath, remotePath)
		cv.So(difflen, cv.ShouldEqual, 0)
	})
}
