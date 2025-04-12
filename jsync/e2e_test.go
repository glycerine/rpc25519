package jsync

import (
	//"bytes"
	//cryrand "crypto/rand"
	"fmt"
	//"path/filepath"
	//"io"
	mathrand2 "math/rand/v2"
	"os"
	"testing"
	"time"

	cv "github.com/glycerine/goconvey/convey"
	rpc "github.com/glycerine/rpc25519"
)

func Test220_push_then_pull_idempotent(t *testing.T) {

	cv.Convey("using our rsync-like-protocol, a client doing a push, then a pull of the same file should see the pull do no changes (no-op) since the push already synchronized the remote with the local copy of the file. Also make sure we can have local and remote file names different and still sync to each other.", t, func() {

		// set up two isolated test dirs, so we can
		// be sure exactly what is changing by inspection.

		cwd, err := os.Getwd()
		panicOn(err)
		_ = cwd

		localDir := cwd + sep + "local_cli_dir_test220/"
		remoteDir := cwd + sep + "remote_srv_dir_test220/"
		// remove any old leftover test files first.
		os.RemoveAll(localDir)
		os.RemoveAll(remoteDir)
		os.MkdirAll(localDir, 0755)
		os.MkdirAll(remoteDir, 0755)

		// create a test file
		N := 1
		localBase := fmt.Sprintf("test220_chacha_%vmb.dat", N)
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
		slc := make([]byte, 1<<20) // 1 MB slice

		// deterministic pseudo-random numbers as data.
		var seed [32]byte
		seed[1] = 2
		generator := mathrand2.NewChaCha8(seed)

		// random or zeros?
		allZeros := false
		if allZeros {
			// slc is already ready with all 0.
		} else {
			generator.Read(slc)
		}
		for range N {
			_, err = testfd.Write(slc)
			panicOn(err)
		}
		testfd.Close()
		vv("created N = %v MB test file in remotePath='%v'.", N, remotePath)

		// set up a server and a client.

		cfg := rpc.NewConfig()
		//cfg.TCPonly_no_TLS = true
		//cfg.CompressionOff = true

		cfg.ServerAddr = "127.0.0.1:0"
		srv := rpc.NewServer("srv_rsync_test220", cfg)

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
		cli, err := rpc.NewClient("cli_rsync_test220", cfg)
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
		cv.So(dataBytesMoved0, cv.ShouldBeGreaterThan, len(slc))

		// confirm it happened.
		difflen := compareFilesDiffLen(localPath, remotePath)
		cv.So(difflen, cv.ShouldEqual, 0)

		// check mod time being updated
		lsz, lmod, err := FileSizeModTime(localPath)
		panicOn(err)
		rsz, rmod, err := FileSizeModTime(remotePath)
		panicOn(err)
		if lsz != rsz {
			panic("lsz != rsz")
		}
		if !rmod.Equal(lmod) {
			t.Fatalf("error: lmod='%v' but lmod='%v'", lmod, rmod) // red.
		}

		vv("why is below getting into the convert dir to file path??")

		// test that a change in remote modtime, followed by
		// a pull from that remote, gets us the local having a matching mod time.

		pastModTime := time.Now().Add(-5 * time.Minute)
		err = os.Chtimes(remotePath, time.Time{}, pastModTime)
		panicOn(err)

		dataBytesMoved1, err := jSyncCli.PullToFrom(localPath, remotePath)
		panicOn(err)

		// yay. we efficiently don't send chunks when not needed.
		cv.So(dataBytesMoved1, cv.ShouldBeLessThan, 1000)

		// check mod time being updated for pull
		lsz, lmod, err = FileSizeModTime(localPath)
		panicOn(err)
		rsz, rmod, err = FileSizeModTime(remotePath)
		panicOn(err)
		if lsz != rsz {
			panic("lsz != rsz")
		}
		if !rmod.Equal(lmod) {
			t.Fatalf("error: lmod='%v' but lmod='%v'", lmod, rmod)
		}

	})
}
